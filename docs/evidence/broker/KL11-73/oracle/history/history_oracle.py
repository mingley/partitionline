#!/usr/bin/env python3
"""Check captured causal histories against independently decoded WAL bytes.

This is an offline fixed-member safety checker, not a model checker of all
possible executions. It reads bounded captures and imports only our independent
binary decoder. Neither emitted verdicts nor a source SHA alone establish proof.
"""
from __future__ import annotations

import argparse
from collections import Counter
import hashlib
import json
from pathlib import Path

from wal_oracle import MAX_ENTRIES, MAX_RECORD, MAX_TERM, read_journal, require
from election_oracle import read_election

MAX_TRACE_BYTES = 32 * 1024 * 1024
MAX_EVENTS = 10_000
KINDS = {"initial", "campaign", "vote_request", "vote_response", "activate",
         "propose", "prepare", "receive", "ack", "drop", "timeout", "poll",
         "crash", "fault", "reopen", "process_exit"}


def integer(value, low=0, high=(1 << 64) - 1):
    require(type(value) is int and low <= value <= high, "integer type/range")
    return value


def boolean(value):
    require(type(value) is bool, "boolean type")
    return value


def position(row):
    term, index = integer(row["term"], 0, MAX_TERM), integer(row["index"], 0, MAX_ENTRIES)
    require((term == 0) == (index == 0), "position empty/term")
    return term, index


def record(row):
    term, index = position(row)
    kind = integer(row["kind"], 0, 1)
    value = row["payload_hex"]
    require(type(value) is str and len(value) <= 2 * MAX_RECORD, "record payload bound")
    payload = bytes.fromhex(value)
    require((kind == 0 and payload) or (kind == 1 and not payload), "record kind/payload")
    if "sha256" in row:
        require(row["sha256"] == hashlib.sha256(payload).hexdigest(), "record digest")
    return term, index, kind, payload


def canonical_request(row):
    previous = position(row["previous"])
    entries = [record(x) for x in row["entries"]]
    require(len(entries) <= MAX_ENTRIES, "request entry bound")
    term = integer(row["term"], 1, MAX_TERM)
    last = previous
    for entry in entries:
        require(entry[1] == last[1] + 1 and last[0] <= entry[0] <= term,
                "request record continuity/term")
        last = entry[:2]
    require(previous[0] <= term, "request previous epoch")
    return {"leader": integer(row["leader"]), "peer": integer(row["peer"]),
            "sequence": integer(row["sequence"], 1), "term": term,
            "previous": previous, "leader_commit": integer(row["leader_commit"], 0, MAX_ENTRIES),
            "entries": entries, "target": last}


def canonical_response(row):
    return {"leader": integer(row["leader"]), "peer": integer(row["peer"]),
            "sequence": integer(row["sequence"], 1),
            "term": integer(row["term"], 1, MAX_TERM), "success": boolean(row["success"]),
            "matched": position(row["matched"]),
            "conflict_index": integer(row["conflict_index"], 0, MAX_ENTRIES + 1)}


def safe_file(base, name):
    require(type(name) is str, "artifact path type")
    path = (base / name).resolve()
    require(path.is_relative_to(base.resolve()) and path.is_file(), "artifact path/availability")
    return path


def verify(path: Path):
    require(path.stat().st_size <= MAX_TRACE_BYTES, "trace byte ceiling")
    trace_bytes = path.read_bytes()
    trace = json.loads(trace_bytes)
    group = trace["group"]
    voters = group["voters"]
    require(type(voters) is list and 1 <= len(voters) <= 64 and
            voters == sorted(set(voters)), "fixed group membership")
    for node in voters:
        integer(node, 0, 0x7fffffff)
    events = trace["events"]
    require(type(events) is list and 1 <= len(events) <= MAX_EVENTS, "event count ceiling")
    require(events[0]["kind"] == "initial", "initial event")
    copies = trace["final_journals"]
    require(type(copies) is list and 1 <= len(copies) <= MAX_EVENTS * len(voters),
            "journal receipt count ceiling")
    checkpoints = {}
    raw = []
    for copy in copies:
        node = integer(copy["node_id"])
        ordinal = integer(copy["event_ordinal"], 0, len(events) - 1)
        require(node in voters and copy["group"] == group, "checkpoint group")
        replay = read_journal(safe_file(path.parent, copy["wal_path"]), node, group)
        require(len(replay.operations) == copy["wal_confirmed_ops"], "checkpoint operation count")
        require(replay.journal_sha256 == copy["wal_sha256"] and
                replay.journal_bytes == copy["wal_bytes"], "checkpoint content digest/size")
        election = read_election(safe_file(path.parent, copy["election_path"]), node, voters)
        require(election["sha256"] == copy["election_sha256"] and
                election["bytes"] == copy["election_bytes"] and
                election["state_count"] == copy["election_confirmed_states"],
                "checkpoint election digest/size/count")
        checkpoint_key = ordinal, node
        require(checkpoint_key not in checkpoints, "duplicate journal checkpoint")
        checkpoints[checkpoint_key] = replay, election
        raw.append({"ordinal": ordinal, "node": node, "operations": len(replay.operations),
                    "committed_end": replay.committed, "sha256": replay.journal_sha256,
                    "election_sha256": election["sha256"],
                    "election_states": election["state_count"],
                    "election_reconciliations": election["reconciliation_records"]})
    logs, committed, previous = {}, {node: [] for node in voters}, {}
    prepared, responses, pending, progress = {}, {}, {}, {}
    globally_committed = {}
    leader_commits = follower_commits = 0
    now = 0
    for ordinal, event in enumerate(events):
        try:
            require(integer(event["ordinal"]) == ordinal, "event ordinal continuity")
            current_time = integer(event["now_ms"])
            require(current_time >= now, "clock reversal")
            now = current_time
            kind, node = event["kind"], integer(event["node_id"])
            require(kind in KINDS and (node in voters or node == 0), "event kind/node")
            after = {integer(s["node_id"]): s for s in event["after"]}
            require(len(event["after"]) == len(voters) and sorted(after) == voters,
                    "complete unique node states")
            args, result = event["args"], event["result"]
            if kind == "initial":
                for member in voters:
                    require((ordinal, member) in checkpoints, "initial raw journal missing")
                    replay, _ = checkpoints[ordinal, member]
                    logs[member] = [(r.term, r.index, r.kind, r.payload) for r in replay.records]
            elif kind in ("activate", "propose") and "error" not in result:
                before = previous[node]
                require(before["open"] and before["ready"] and not before["poisoned"] and
                        before["role"] == "Leader", "proposal/activation authority")
                if kind == "activate":
                    require(result["disposition"] == "accepted" and
                            result["barrier_kind"] == 1 and boolean(result["barrier_payload_empty"]),
                            "activation barrier shape")
                    index, payload, record_kind = integer(result["barrier_index"], 1), b"", 1
                else:
                    require(before["active_term"] == before["term"], "proposal active epoch")
                    payload = bytes.fromhex(args["payload_hex"])
                    require(0 < len(payload) <= MAX_RECORD, "proposal byte budget")
                    require(args["sha256"] == hashlib.sha256(payload).hexdigest(), "proposal digest")
                    index, record_kind = integer(result["result_index"], 1), 0
                require(index == len(logs[node]) + 1, "local append position")
                logs[node].append((before["term"], index, record_kind, payload))
            elif kind == "prepare" and "error" not in result:
                request = canonical_request(result)
                leader, peer = request["leader"], request["peer"]
                require(node == leader and args == {"leader": leader, "peer": peer} and
                        leader in voters and peer in voters and leader != peer, "request identities")
                before = previous[leader]
                require(before["role"] == "Leader" and before["active_term"] == request["term"] ==
                        before["term"] and before["open"] and before["ready"] and
                        not before["poisoned"], "prepare active leader")
                require(request["leader_commit"] == before["committed_end"], "advertised commit")
                pterm, pindex = request["previous"]
                require((pindex == 0 and pterm == 0) or
                        (pindex <= len(logs[leader]) and logs[leader][pindex - 1][0] == pterm),
                        "request previous durable leader position")
                require(request["entries"] == logs[leader][pindex:pindex + len(request["entries"])],
                        "request payload differs from durable leader content")
                key = leader, peer, request["sequence"], request["term"]
                require(key not in prepared and (leader, peer) not in pending, "duplicate/outstanding request")
                prepared[key] = request
                pending[leader, peer] = key
            elif kind == "receive" and "error" not in result:
                request, response = canonical_request(args), canonical_response(result)
                key = request["leader"], request["peer"], request["sequence"], request["term"]
                require(node == request["peer"], "receive target identity")
                if key not in prepared or prepared[key] != request:
                    origin = event.get("input_origin", args.get("input_origin", {}))
                    require(origin.get("type") == "mutated_prepared_request" and
                            origin.get("changed_fields") == ["peer"], "receive request provenance")
                    source_ordinal = integer(origin["source_ordinal"], 0, ordinal - 1)
                    source = events[source_ordinal]
                    require(source["kind"] == "prepare", "mutated input origin event")
                    original = canonical_request(source["result"])
                    original["peer"] = request["peer"]
                    require(original == request and not response["success"] and
                            previous[node]["term"] > request["term"],
                            "declared stale input mutation accepted or altered elsewhere")
                require((response["leader"], response["peer"], response["sequence"]) == key[:3],
                        "receive response correlation")
                require(previous[node]["open"] and previous[node]["ready"] and
                        not previous[node]["poisoned"], "receive readiness")
                if response["success"]:
                    require(response["term"] == request["term"] and
                            response["matched"] == request["target"] and response["conflict_index"] == 0,
                            "response exact durable target")
                    pterm, pindex = request["previous"]
                    require((pindex == 0 and pterm == 0) or
                            (pindex <= len(logs[node]) and logs[node][pindex - 1][0] == pterm),
                            "successful receive previous prefix")
                    for entry in request["entries"]:
                        index = entry[1]
                        if index <= len(logs[node]) and logs[node][index - 1] != entry:
                            require(index > len(committed[node]) and
                                    logs[node][index - 1][0] != entry[0], "committed/same-epoch repair")
                            logs[node] = logs[node][:index - 1]
                        if index > len(logs[node]):
                            require(index == len(logs[node]) + 1, "follower contiguous append")
                            logs[node].append(entry)
                    ceiling = min(request["leader_commit"], len(logs[node]), request["target"][1])
                    require(after[node]["committed_end"] == max(len(committed[node]), ceiling),
                            "follower commit beyond causally matched end")
                else:
                    require(response["matched"] == (0, 0) and response["conflict_index"] > 0,
                            "failure response shape")
                    require(after[node]["committed_end"] == len(committed[node]), "failed receive commit")
                responses[key] = response
            elif kind == "ack":
                response = canonical_response(args)
                peer, leader = response["peer"], response["leader"]
                if result["disposition"] == "accepted":
                    require(node == leader and (leader, peer) in pending, "ack outstanding provenance")
                    key = pending[leader, peer]
                    require(key in responses and responses[key] == response and
                            key[2] == response["sequence"], "ack fabricated/stale response")
                    before = previous[leader]
                    require(before["role"] == "Leader" and before["active_term"] == before["term"] == key[3],
                            "ack active leader epoch")
                    request = prepared[key]
                    if response["success"]:
                        require(response["term"] == key[3] and response["matched"] == request["target"],
                                "ack target/epoch mismatch")
                        progress[leader, peer] = max(progress.get((leader, peer), 0), response["matched"][1])
                    del pending[leader, peer]
                    require(result["committed_end"] == after[node]["committed_end"], "ack result commit")
                else:
                    require(result["disposition"] == "rejected" and
                            after[node]["committed_end"] == len(committed[node]), "rejected ack changed commit")
            elif kind == "timeout" and result.get("disposition") == "accepted":
                pair = node, integer(args["peer"])
                require(pair in pending and pending[pair][2] == args["sequence"], "timeout provenance")
                del pending[pair]
            elif kind == "fault":
                require(not previous[node]["open"] and not after[node]["open"], "fault requires closed owner")
                fault_file = safe_file(path.parent, args["file"])
                fault_bytes = fault_file.read_bytes()
                require(len(fault_bytes) <= 256 * 1024 * 1024 and
                        hashlib.sha256(fault_bytes).hexdigest() == args["after_sha256"], "fault artifact digest")
                tail = integer(args["bytecount"], 1, 31)
                require(fault_bytes[-tail:] == b"PLENTRY1"[:tail] and
                        hashlib.sha256(fault_bytes[:-tail]).hexdigest() == args["before_sha256"],
                        "recognized incomplete tail artifact")
            for member in voters:
                state = after[member]
                for flag in ("open", "poisoned", "ready"):
                    boolean(state[flag])
                term = integer(state["term"], 1, MAX_TERM)
                require(state["role"] in {"Follower", "Candidate", "Leader"}, "role")
                require(not state["ready"] or (state["open"] and not state["poisoned"]), "ready poison/open")
                require(state["active_term"] is None or
                        (state["role"] == "Leader" and state["active_term"] == term), "active epoch/role")
                last = position(state["last_position"])
                actual_last = logs[member][-1][:2] if logs[member] else (0, 0)
                require(last == actual_last, "reported durable log differs from causal content")
                end = integer(state["committed_end"], 0, len(logs[member]))
                rows = [record(row) for row in state["committed_records"]]
                require(rows == logs[member][:end] and rows[:len(committed[member])] == committed[member],
                        "committed prefix changed/lost or differs from causal content")
                if end > len(committed[member]):
                    if kind == "ack" and member == node:
                        require(rows[-1][0] == term and state["role"] == "Leader" and
                                state["active_term"] == term, "leader old-term commit")
                        supporters = {member} | {peer for peer in voters if peer != member and
                                                 progress.get((member, peer), 0) >= end}
                        require(len(supporters) >= len(voters) // 2 + 1, "commit lacks distinct causal majority")
                        leader_commits += 1
                    else:
                        require(kind == "receive" and member == node and result["success"],
                                "commit without causal receive/majority")
                        follower_commits += 1
                for entry in rows:
                    index = entry[1]
                    require(index not in globally_committed or globally_committed[index] == entry,
                            "cross-node committed byte-prefix disagreement")
                    globally_committed[index] = entry
                committed[member] = rows
                if (ordinal, member) in checkpoints:
                    replay, election = checkpoints[ordinal, member]
                    require([(r.term, r.index, r.kind, r.payload) for r in replay.records] == logs[member] and
                            replay.committed == end and len(replay.operations) == state["wal_durable_ops"],
                            "actual WAL disagrees with reported/causal state")
                    require(election["final"]["term"] == term and
                            election["final"]["voted_for"] == state["voted_for"] and
                            election["final"]["log"] == last and
                            election["state_count"] == state["election_durable_states"],
                            "actual election WAL disagrees with reported/reconciled state")
                before = previous.get(member)
                if before:
                    require(term >= before["term"], "term regression")
                if kind == "process_exit" or kind in ("crash", "reopen") and member == node or (
                        before and (term, state["role"], state["active_term"]) !=
                        (before["term"], before["role"], before["active_term"])):
                    pending = {key: value for key, value in pending.items() if key[0] != member}
                    progress = {key: value for key, value in progress.items() if key[0] != member}
            previous = after
        except (ValueError, KeyError, TypeError, IndexError) as error:
            raise ValueError(f"event {ordinal} {event.get('kind')}: {error}") from error
    require(all(any(copy["node_id"] == n and copy["phase"] == "parent-after-abrupt-exit"
                    for copy in copies) for n in voters), "post-process-exit raw journals missing")
    return {"schema_version": 1, "disposition": "accepted captured fixed-member safety history",
            "trace_sha256": hashlib.sha256(trace_bytes).hexdigest(),
            "source_sha_reported": trace.get("source_sha"), "source_binding_scope": "external immutable runner required",
            "voters": voters, "events": len(events), "event_kinds": dict(Counter(e["kind"] for e in events)),
            "raw_journal_checkpoints": raw, "leader_commit_advances": leader_commits,
            "follower_commit_advances": follower_commits, "agreed_committed_positions": len(globally_committed),
            "limitations": ["Captured histories only; no exhaustive model or production/wire qualification.",
                            "Apache election/component checks and immutable source binding are separate proofs."]}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("trace", type=Path)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    args.output.write_text(json.dumps(verify(args.trace), indent=2) + "\n")


if __name__ == "__main__":
    main()

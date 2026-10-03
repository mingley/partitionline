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

from wal_oracle import MAX_ENTRIES, MAX_RECORD, MAX_TERM, MAX_RECORD, Record, require
from snapshot_oracle import MAX_IMAGE, decode_image, read_image, read_journal, install_records
from election_oracle import read_election

MAX_TRACE_BYTES = 32 * 1024 * 1024
MAX_EVENTS = 10_000
KINDS = {"initial", "campaign", "vote_request", "vote_response", "activate",
         "propose", "prepare", "receive", "ack", "drop", "timeout", "poll",
         "crash", "fault", "reopen", "process_exit", "checkpoint", "prepare_snapshot",
         "begin_snapshot", "snapshot_chunk", "receive_snapshot_chunk", "partial_snapshot_chunk",
         "finish_incomplete_snapshot", "abort_snapshot", "finish_snapshot",
         "drop_snapshot_ack", "ack_snapshot"}


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


def descriptor(row):
    generation = row["generation"]
    require(type(generation) is str and len(generation) == 32 and
            bytes.fromhex(generation).hex() == generation and generation != "00" * 16,
            "snapshot generation")
    base = position(row["base"])
    count = integer(row["records"], 0, MAX_ENTRIES)
    require(count == base[1], "snapshot full prefix count")
    return {"generation": generation, "base": {"term": base[0], "index": base[1]},
            "records": count, "payload_bytes": integer(row["payload_bytes"], 0, 64 * 1024 * 1024),
            "bytes": integer(row["bytes"], 114, MAX_IMAGE),
            "checksum": integer(row["checksum"], 0, 0xffffffff)}


def snapshot_request(row):
    term = integer(row["term"], 1, MAX_TERM)
    d = descriptor(row["descriptor"])
    commit = integer(row["leader_commit"], 0, MAX_ENTRIES)
    require(d["base"]["term"] <= term and d["base"]["index"] <= commit,
            "snapshot request term/commit")
    return {"leader": integer(row["leader"]), "peer": integer(row["peer"]),
            "sequence": integer(row["sequence"], 1), "term": term,
            "leader_commit": commit, "descriptor": d}


def snapshot_key(request):
    return tuple(request[k] for k in ("leader", "peer", "sequence", "term"))


def snapshot_response(row):
    return {"response": canonical_response(row["response"]), "descriptor": descriptor(row["descriptor"])}


def as_tuples(rows):
    return [(r.term, r.index, r.kind, r.payload) for r in rows]


def as_records(rows):
    return [Record(index, term, kind, payload) for term, index, kind, payload in rows]


def verify(path: Path):
    require(path.stat().st_size <= MAX_TRACE_BYTES, "trace byte ceiling")
    trace_bytes = path.read_bytes()
    trace = json.loads(trace_bytes)
    require(trace["schema_version"] == 2 and trace["profile"] == "fixed-membership-full-prefix-snapshot",
            "snapshot trace schema/profile")
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
    raw, images = [], {}
    for copy in copies:
        node = integer(copy["node_id"])
        ordinal = integer(copy["event_ordinal"], 0, len(events) - 1)
        require(node in voters and copy["group"] == group, "checkpoint group")
        images_dir = (path.parent / copy["images_dir"]).resolve()
        require(images_dir.is_relative_to(path.parent.resolve()) and images_dir.is_dir(),
                "checkpoint image directory")
        replay = read_journal(safe_file(path.parent, copy["wal_path"]), node, group, images_dir)
        require(len(replay.operations) == copy["wal_confirmed_ops"], "checkpoint operation count")
        require(replay.selected == copy["selected_snapshot"], "checkpoint selected receipt")
        if "wal_sha256" in copy:
            require(replay.journal_sha256 == copy["wal_sha256"] and
                    replay.journal_bytes == copy["wal_bytes"], "checkpoint content digest/size")
        election = read_election(safe_file(path.parent, copy["election_path"]), node, voters)
        require(election["state_count"] == copy["election_confirmed_states"], "checkpoint election count")
        if "election_sha256" in copy:
            require(election["sha256"] == copy["election_sha256"] and
                    election["bytes"] == copy["election_bytes"], "checkpoint election digest/size")
        image_paths = sorted(images_dir.glob("snapshot-*.image"))
        require(len(image_paths) <= 64, "checkpoint generation ceiling")
        for image_path in image_paths:
            image = read_image(image_path, group)
            generation = image["descriptor"]["generation"]
            require(image_path.name == "snapshot-" + generation + ".image", "image filename binding")
            key = node, generation
            require(key not in images or images[key]["sha256"] == image["sha256"],
                    "immutable image generation changed")
            images[key] = image
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
    snapshot_prepared, snapshot_responses, assemblies, emitted_chunks = {}, {}, {}, {}
    selected = {node: None for node in voters}
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
                    selected[member] = replay.selected
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
            elif kind == "checkpoint" and "error" not in result:
                d = descriptor(result)
                before = previous[node]
                require(before["open"] and before["ready"] and not before["poisoned"], "checkpoint readiness")
                require(args == {"generation": d["generation"]} and
                        d["base"]["index"] == len(committed[node]), "checkpoint exact committed base")
                image = images.get((node, d["generation"]))
                require(image is not None and image["descriptor"] == d and
                        as_tuples(image["records"]) == committed[node], "checkpoint image/content binding")
                require((ordinal, node) in checkpoints and checkpoints[ordinal, node][0].selected == d and
                        checkpoints[ordinal, node][0].operations[-1]["opcode"] == "install" and
                        checkpoints[ordinal, node][0].operations[-1]["authority"] == 0,
                        "checkpoint missing local selecting WAL receipt")
                selected[node] = d
            elif kind == "prepare_snapshot" and "error" not in result:
                offer = snapshot_request(result)
                leader, peer = offer["leader"], offer["peer"]
                before = previous[node]
                require(node == leader and args == {"peer": peer} and leader in voters and
                        peer in voters and peer != leader and before["role"] == "Leader" and
                        before["active_term"] == before["term"] == offer["term"] and
                        before["open"] and before["ready"] and not before["poisoned"],
                        "snapshot prepare active leader")
                require(offer["leader_commit"] == len(committed[node]) and
                        offer["descriptor"] == selected[node], "snapshot offer selected commit binding")
                key = snapshot_key(offer)
                require(key not in snapshot_prepared and (leader, peer) not in pending,
                        "snapshot duplicate/outstanding correlation")
                snapshot_prepared[key] = offer
                pending[leader, peer] = key
            elif kind == "begin_snapshot" and result.get("disposition") == "accepted":
                offer = snapshot_request(args)
                key = snapshot_key(offer)
                require(snapshot_prepared.get(key) == offer and node == offer["peer"] and
                        node not in assemblies and previous[node]["open"] and previous[node]["ready"] and
                        not previous[node]["poisoned"] and previous[node]["term"] <= offer["term"],
                        "snapshot begin provenance/readiness")
                assemblies[node] = {"request": offer, "bytes": bytearray()}
            elif kind == "snapshot_chunk" and "error" not in result:
                offer = snapshot_request(args)
                key = snapshot_key(offer)
                require(snapshot_prepared.get(key) == offer and node == offer["leader"] and
                        pending.get((node, offer["peer"])) == key and
                        snapshot_request(result["request"]) == offer, "emitted chunk correlation")
                offset = integer(result["offset"], 0, MAX_IMAGE)
                value = result["bytes_hex"]
                require(type(value) is str and 2 <= len(value) <= 2 * MAX_RECORD, "chunk byte ceiling")
                data = bytes.fromhex(value)
                image = images[node, offer["descriptor"]["generation"]]
                image_path = next((path.parent / c["images_dir"] / ("snapshot-" + offer["descriptor"]["generation"] + ".image")
                                  for c in copies if c["node_id"] == node and
                                  (path.parent / c["images_dir"] / ("snapshot-" + offer["descriptor"]["generation"] + ".image")).is_file())
                whole = image_path.read_bytes()
                require(image["descriptor"] == offer["descriptor"] and data == whole[offset:offset + len(data)] and
                        offset + len(data) <= len(whole) and boolean(result["done"]) == (offset + len(data) == len(whole)),
                        "emitted chunk differs from selected durable image")
                emitted_chunks[ordinal] = {"request": offer, "offset": offset, "data": data,
                                           "done": result["done"]}
            elif kind in ("receive_snapshot_chunk", "partial_snapshot_chunk") and result.get("disposition") == "accepted":
                offer = snapshot_request(args["request"])
                offset = integer(args["offset"], 0, MAX_IMAGE)
                value = args["bytes_hex"]
                require(type(value) is str and 2 <= len(value) <= 2 * MAX_RECORD, "received chunk ceiling")
                data = bytes.fromhex(value)
                assembly = assemblies.get(node)
                require(assembly is not None and assembly["request"] == offer and node == offer["peer"] and
                        offset == len(assembly["bytes"]) and offset + len(data) <= offer["descriptor"]["bytes"],
                        "received chunk identity/order/budget")
                if kind == "partial_snapshot_chunk":
                    origin = args["input_origin"]
                    source = integer(origin["source_ordinal"], 0, ordinal - 1)
                    chunk = emitted_chunks[source]
                    require(origin["type"] == "prefix_of_emitted_chunk" and chunk["request"] == offer and
                            chunk["offset"] == offset and 0 < len(data) < len(chunk["data"]) and
                            data == chunk["data"][:len(data)], "partial chunk declared prefix provenance")
                else:
                    require(any(chunk["request"] == offer and chunk["offset"] == offset and
                                chunk["data"] == data and chunk["done"] == boolean(args["done"])
                                for chunk in emitted_chunks.values()), "received chunk emitted provenance")
                assembly["bytes"].extend(data)
            elif kind == "finish_incomplete_snapshot":
                offer = snapshot_request(args)
                assembly = assemblies.get(node)
                require(assembly is not None and assembly["request"] == offer and
                        len(assembly["bytes"]) < offer["descriptor"]["bytes"] and
                        result["disposition"] == "rejected", "incomplete image installation accepted")
            elif kind == "abort_snapshot" and result.get("disposition") == "accepted":
                require(node in assemblies, "abort without incoming snapshot")
                del assemblies[node]
            elif kind == "finish_snapshot" and "error" not in result:
                offer = snapshot_request(args)
                assembly = assemblies.get(node)
                require(assembly is not None and assembly["request"] == offer and node == offer["peer"],
                        "snapshot finish assembly provenance")
                image = decode_image(bytes(assembly["bytes"]), group)
                require(image["descriptor"] == offer["descriptor"], "assembled snapshot descriptor binding")
                response = snapshot_response(result)
                expected = {"leader": offer["leader"], "peer": node, "sequence": offer["sequence"],
                            "term": offer["term"], "success": True,
                            "matched": position(offer["descriptor"]["base"]), "conflict_index": 0}
                require(response["response"] == expected and response["descriptor"] == offer["descriptor"],
                        "snapshot success receipt correlation")
                require(previous[node]["term"] <= offer["term"] and
                        (selected[node] is None or offer["descriptor"]["base"]["index"] >= selected[node]["base"]["index"]),
                        "snapshot receiver term/base regression")
                logs[node] = as_tuples(install_records(as_records(logs[node]), len(committed[node]), image, offer["term"]))
                selected[node] = offer["descriptor"]
                require(after[node]["committed_end"] == len(image["records"]) and (ordinal, node) in checkpoints,
                        "snapshot commit or selecting raw checkpoint missing")
                replay = checkpoints[ordinal, node][0]
                require(replay.operations[-1]["opcode"] == "install" and replay.operations[-1]["authority"] == 1 and
                        replay.operations[-1]["sequence"] == offer["sequence"] and
                        replay.operations[-1]["leader"] == offer["leader"], "snapshot missing synchronized remote Install receipt")
                snapshot_responses[snapshot_key(offer)] = response
                del assemblies[node]
            elif kind == "drop_snapshot_ack":
                response = snapshot_response(args)
                r = response["response"]
                require(snapshot_responses.get((r["leader"], r["peer"], r["sequence"], r["term"])) == response,
                        "lost snapshot receipt not emitted")
            elif kind == "ack_snapshot":
                response = snapshot_response(args)
                r = response["response"]
                leader, peer = r["leader"], r["peer"]
                key = leader, peer, r["sequence"], r["term"]
                if result["disposition"] == "accepted":
                    require(node == leader and pending.get((leader, peer)) == key and
                            snapshot_responses.get(key) == response and
                            snapshot_prepared[key]["descriptor"] == response["descriptor"],
                            "snapshot ack fabricated/stale/descriptor mismatch")
                    before = previous[node]
                    require(before["role"] == "Leader" and before["active_term"] == before["term"] == r["term"] and
                            r["success"] and r["matched"] == position(response["descriptor"]["base"]),
                            "snapshot ack active epoch/exact target")
                    progress[leader, peer] = max(progress.get((leader, peer), 0), r["matched"][1])
                    del pending[leader, peer]
                    require(result["committed_end"] == after[node]["committed_end"], "snapshot ack reported commit")
                else:
                    require(result["disposition"] == "rejected" and after[node]["committed_end"] == len(committed[node]),
                            "rejected snapshot receipt changed commit")
                    if "input_origin" in args:
                        origin = args["input_origin"]
                        source = integer(origin["source_ordinal"], 0, ordinal - 1)
                        original = snapshot_response(events[source]["result"])
                        require(events[source]["kind"] == "finish_snapshot" and
                                origin["type"] == "mutated_emitted_response" and
                                origin["changed_fields"] == ["descriptor.checksum"] and
                                original["response"] == r and
                                {k: v for k, v in original["descriptor"].items() if k != "checksum"} ==
                                {k: v for k, v in response["descriptor"].items() if k != "checksum"} and
                                original["descriptor"]["checksum"] != response["descriptor"]["checksum"],
                                "forged snapshot receipt declared mutation provenance")
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
                    if kind in ("ack", "ack_snapshot") and member == node:
                        require(rows[-1][0] == term and state["role"] == "Leader" and
                                state["active_term"] == term, "leader old-term commit")
                        supporters = {member} | {peer for peer in voters if peer != member and
                                                 progress.get((member, peer), 0) >= end}
                        require(len(supporters) >= len(voters) // 2 + 1, "commit lacks distinct causal majority")
                        leader_commits += 1
                    else:
                        require(member == node and ((kind == "receive" and result["success"]) or
                                (kind == "finish_snapshot" and result["response"]["success"])),
                                "commit without causal receive/majority")
                        follower_commits += 1
                for entry in rows:
                    index = entry[1]
                    require(index not in globally_committed or globally_committed[index] == entry,
                            "cross-node committed byte-prefix disagreement")
                    globally_committed[index] = entry
                committed[member] = rows
                require(state["selected_snapshot"] == selected[member] and
                        position(state["base_position"]) == (position(selected[member]["base"]) if selected[member] else (0, 0)),
                        "reported snapshot selection differs from causal Install")
                if (ordinal, member) in checkpoints:
                    replay, election = checkpoints[ordinal, member]
                    require([(r.term, r.index, r.kind, r.payload) for r in replay.records] == logs[member] and
                            replay.committed == end and len(replay.operations) == state["wal_durable_ops"] and
                            replay.selected == selected[member],
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

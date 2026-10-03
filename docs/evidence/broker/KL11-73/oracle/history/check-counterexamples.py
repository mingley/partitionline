#!/usr/bin/env python3
"""Deliberate counterexamples to causal and durable-byte proof acceptance."""
from __future__ import annotations

import argparse
import copy
import hashlib
import json
from pathlib import Path
import shutil

from history_oracle import verify
from wal_oracle import crc32c, read_journal, require


def rewrite_operation(raw, select, change):
    """Preserve envelope and payload CRCs while changing one actual operation."""
    raw = bytearray(raw)
    offset = 24
    while offset < len(raw):
        length = int.from_bytes(raw[offset + 8:offset + 12], "big")
        start = offset + 32
        payload = bytearray(raw[start:start + length])
        if select(payload):
            change(payload)
            raw[start:start + length] = payload
            raw[offset + 24:offset + 28] = crc32c(payload).to_bytes(4, "big")
            raw[offset + 28:offset + 32] = crc32c(raw[offset:offset + 28]).to_bytes(4, "big")
            return bytes(raw)
        offset = start + length
    raise ValueError("counterexample operation unavailable in captured journal")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("trace", type=Path)
    parser.add_argument("--work", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    baseline = verify(args.trace)
    require(not args.work.exists(), "fresh counterexample work directory required")
    shutil.copytree(args.trace.parent, args.work)
    original = json.loads(args.trace.read_text())
    results = []

    def reject(name, action, expected, mode="trace", node=None):
        case = args.work / (name + (".json" if mode == "trace" else ".wal"))
        if mode == "trace":
            candidate = copy.deepcopy(original)
            action(candidate)
            case.write_text(json.dumps(candidate, indent=2) + "\n")
            check = lambda: verify(case)
        else:
            final = next(c for c in reversed(original["final_journals"])
                         if c["node_id"] == node and c["phase"] == "parent-after-abrupt-exit")
            raw = (args.trace.parent / final["wal_path"]).read_bytes()
            case.write_bytes(action(raw))
            check = lambda: read_journal(case, node, original["group"])
        try:
            check()
        except ValueError as error:
            require(expected in str(error), "counterexample rejected for unintended reason: " + str(error))
            results.append({"name": name, "disposition": "rejected", "expected_reason": expected,
                            "observed_reason": str(error), "input_sha256": hashlib.sha256(case.read_bytes()).hexdigest(),
                            "artifact": case.name, "checksum_valid_semantic_mutation": mode == "journal"})
        else:
            raise ValueError("counterexample accepted: " + name)

    accepted_ack = next(e["ordinal"] for e in original["events"]
                        if e["kind"] == "ack" and e["result"].get("disposition") == "accepted" and e["args"]["success"])
    receive = next(e["ordinal"] for e in original["events"]
                   if e["kind"] == "receive" and e["result"].get("success"))
    propose = next(e["ordinal"] for e in original["events"] if e["kind"] == "propose")
    prepare = next(e["ordinal"] for e in original["events"] if e["kind"] == "prepare")
    reject("stale-ack-sequence", lambda t: t["events"][accepted_ack]["args"].update(sequence=9999),
           "ack fabricated/stale response")
    reject("forged-ack-position", lambda t: t["events"][accepted_ack]["args"]["matched"].update(index=100),
           "ack fabricated/stale response")
    reject("fabricated-follower-match", lambda t: t["events"][receive]["result"]["matched"].update(index=100),
           "response exact durable target")
    reject("false-leader-commit-announcement", lambda t: t["events"][prepare]["result"].update(leader_commit=1),
           "advertised commit")
    reject("duplicate-voter-state", lambda t: t["events"][propose]["after"][1].update(node_id=t["events"][propose]["after"][0]["node_id"]),
           "complete unique node states")

    def invented_commit(t):
        event = t["events"][propose]
        node = next(s for s in event["after"] if s["node_id"] == event["node_id"])
        node["committed_end"] = 1
        node["committed_records"] = [{"term": node["term"], "index": 1, "kind": 1, "payload_hex": ""}]
    reject("unacknowledged-local-commit", invented_commit, "commit without causal receive/majority")
    node = original["group"]["voters"][0]
    reject("foreign-replica-identity", lambda raw: rewrite_operation(raw, lambda p: p[8] == 1,
           lambda p: p.__setitem__(slice(16, 20), original["group"]["voters"][1].to_bytes(4, "big"))),
           "foreign local ID", "journal", node)
    reject("old-term-follower-commit", lambda raw: rewrite_operation(raw,
           lambda p: p[8] == 4 and p[36] == 1,
           lambda p: p.__setitem__(slice(24, 32), (1).to_bytes(8, "big"))),
           "commit position/authority", "journal", node)
    reject("partial-final-operation", lambda raw: raw + b"PLENT", "partial journal entry header", "journal", node)
    if len(original["group"]["voters"]) == 5:
        reject("truncate-committed-prefix", lambda raw: rewrite_operation(raw, lambda p: p[8] == 3,
               lambda p: p.__setitem__(slice(16, 24), (0).to_bytes(8, "big"))),
               "committed or stale truncation", "journal", node)
    args.output.write_text(json.dumps({"schema_version": 1, "baseline": baseline,
                                      "all_counterexamples_rejected": True, "cases": results}, indent=2) + "\n")
    print(json.dumps({"baseline_events": baseline["events"], "counterexamples_rejected": len(results)}))


if __name__ == "__main__":
    main()

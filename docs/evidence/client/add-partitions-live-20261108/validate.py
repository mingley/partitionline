#!/usr/bin/env python3
"""Check that one matched AddPartitions v3 exchange covers both partitions."""
import json
import sys
from pathlib import Path

events = [json.loads(line) for line in Path(sys.argv[1]).read_text().splitlines()]
bad = [e for e in events if e["kind"] in {"decode_error", "relay_error", "connect_error"}]
if bad:
    raise SystemExit("proxy reported errors: " + json.dumps(bad))
requests = {(e["connection"], e["data"]["correlation"]): e
            for e in events if e["kind"] == "request"}
responses = {(e["connection"], e["data"]["correlation"]): e
             for e in events if e["kind"] == "response"}
matched = []
for key, req in requests.items():
    data = req["data"]
    response = responses.get(key)
    if req["version"] != 3 or not response or response["version"] != 3:
        continue
    topics = data["topics"]
    if data["transactional_id"] != "pl-add-coverage-391" or len(topics) != 1:
        continue
    topic = topics[0]
    if topic["topic"] != "pl-add-coverage" or sorted(topic["partitions"]) != [0, 1]:
        continue
    results = response["data"]["topics"]
    if len(results) != 1 or results[0]["topic"] != topic["topic"]:
        continue
    parts = results[0]["partitions"]
    if sorted((p["partition"], p["error"]) for p in parts) != [(0, 0), (1, 0)]:
        continue
    matched.append({"request": req, "response": response})
if not matched:
    raise SystemExit("no matched v3 exchange covering both partitions with zero errors: " + json.dumps(events))
print(json.dumps({"matched_count": len(matched), "matches": matched}, indent=2, sort_keys=True))

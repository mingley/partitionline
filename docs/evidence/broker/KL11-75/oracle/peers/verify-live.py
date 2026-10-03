#!/usr/bin/env python3
"""Independently check exact public peer records, holes, UUIDs and provenance."""
import argparse
import hashlib
import json
from pathlib import Path

PRODUCERS = ("j412", "j421", "j431", "native", "rust")
STAGES = ("initial", "first", "before-expiry", "expired", "restart", "appended")
SCENARIOS = {
    "mixed": [(1000,"a","o"),(1007,None,"v"),(1003,"a","n"),(1007,"b","o"),(1010,"b",None),(1011,"",""),(1012,None,None),(1013,"c",None),(1014,"a","p"),(1015,"a","q")],
    "removed": [(1000,"a","o"),(1001,"a","n"),(1002,"a",None),(1003,"a","p"),(1004,"a","q")],
    "nulls": [(1000,None,"v"),(1001,None,None),(1002,"z","p"),(1003,"z","q")],
}
FIRST = {"mixed": [2,4,5,7,8], "removed": [2,3], "nulls": [2]}
EXPIRED = {"mixed": [2,5,8], "removed": [3], "nulls": [2]}


def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def read(path, maximum=1024*1024):
    if path.is_symlink() or not path.is_file() or path.stat().st_size > maximum:
        raise ValueError("bounded regular receipt required")
    return json.loads(path.read_text())


def require(condition, label):
    if not condition:
        raise ValueError(label)


def literal_record(producer, scenario, offset):
    time, key, value = SCENARIOS[scenario][offset]
    return {"topic": f"cp-{producer}-{scenario}", "partition": 0, "offset": offset, "timestamp": time,
            "key_hex": None if key is None else key.encode().hex(),
            "value_hex": None if value is None else value.encode().hex(),
            "headers": [{"key": "d", "value_hex": "61"}, {"key": "d", "value_hex": None}, {"key": "e", "value_hex": ""}]}


def expected_read(producer, stage):
    records, positions = [], []
    for scenario, source in SCENARIOS.items():
        initial_end = len(source)-1
        end = initial_end + (stage == "appended")
        if stage == "initial":
            kept = list(range(end))
        elif stage in ("first", "before-expiry"):
            kept = FIRST[scenario]
        else:
            kept = list(EXPIRED[scenario]) + ([initial_end] if stage == "appended" else [])
        for seek in (0, 3 if scenario == "mixed" else 1, end):
            records.extend((seek, literal_record(producer,scenario,offset)) for offset in kept if offset >= seek)
            positions.append({"label": "public-consumer-position", "topic": f"cp-{producer}-{scenario}",
                              "seek": seek, "position": end, "beginning_offset": 0, "end_offset": end})
    return records, positions


def check_lane(path, source_sha):
    lane = read(path,16*1024*1024)
    require(lane.get("schema_version") == 1 and lane.get("source_sha") == source_sha and lane.get("passed") is True,
            "exact source/pass/schema lane receipt")
    require(lane.get("failure") is None and lane.get("actual_peer_jobs") == 160 and len(lane["results"]) == 160,
            "complete actual160-job lane")
    require(len(lane["servers"]) == 5 and all(row.get("exit_code") == 0 for row in lane["servers"]), "five successful actual server sessions")
    require([row["stage"] for row in lane["servers"]] == ["initial","first","before-expiry","expired","restart"], "actual source process restart sequence")
    require(len(lane["states"]) == 5 and [row["stage"] for row in lane["states"]] == ["initial","first","before-expiry","expired","appended"], "five durable bounded state snapshots")
    expected_jobs = {(reader, producer, "read", stage) for reader in PRODUCERS for producer in PRODUCERS for stage in STAGES}
    expected_jobs.update((peer,peer,"seed","initial") for peer in PRODUCERS)
    expected_jobs.update((peer,peer,"append","appended") for peer in PRODUCERS)
    actual_jobs, total_records, total_assertions, topic_ids = set(), 0, 0, {}
    for row in lane["results"]:
        key = (row["reader"], row["producer"], row["operation"], row["stage"])
        require(key in expected_jobs and key not in actual_jobs, "actual job coverage without duplicates")
        actual_jobs.add(key)
        require(row["exit_code"] == 0 and sha(Path(row["log"])) == row["log_sha256"], "actual command outcome/log binding")
        peer_path = Path(row["report"])
        require(sha(peer_path) == row["report_sha256"], "actual peer receipt byte binding")
        peer = read(peer_path)
        require(peer["passed"] is True and peer["operation"] == row["operation"] and peer["stage"] == row["stage"]
                and peer["prefix"] == "cp-" + row["producer"], "actual public peer phase/topic/pass binding")
        require(peer["assertions"] == row["assertions"] and peer["records"] == row["records"], "raw actual counts retained")
        observed = [(event["seek"],event["record"]) for event in peer["history"] if event["label"] == "public-consumer-record"]
        positions = [event for event in peer["history"] if event["label"] == "public-consumer-position"]
        deliveries = [event["record"] for event in peer["history"] if event["label"] == "public-producer-delivery"]
        if row["operation"] == "read":
            wanted, expected_positions = expected_read(row["producer"],row["stage"])
            require(observed == wanted and positions == expected_positions and not deliveries, "exact actual sparse/empty retained records and terminal positions")
            require(peer["records"] == len(wanted), "actual consumed record count")
        else:
            wanted = []
            for scenario, source in SCENARIOS.items():
                offsets = range(len(source)-1) if row["operation"] == "seed" else [len(source)-1]
                wanted.extend(literal_record(row["producer"],scenario,offset) for offset in offsets)
            require(deliveries == wanted and not observed and not positions and peer["records"] == len(wanted), "genuine public producer deliveries and offset continuation")
        for topic, identity in peer["identities"].items():
            require(topic in {f"cp-{row['producer']}-{scenario}" for scenario in SCENARIOS}
                    and len(identity) == 32 and all(c in "0123456789abcdef" for c in identity) and identity != "0"*32, "valid actual topic UUID")
            require(topic not in topic_ids or topic_ids[topic] == identity, "actual UUID persistence across compaction/restart")
            topic_ids[topic] = identity
        total_records += peer["records"]
        total_assertions += peer["assertions"]
    require(actual_jobs == expected_jobs and total_records == 2345, "full160 actual jobs and2345 actual record events")
    require(len(topic_ids) == 15 and topic_ids == lane["public_topic_identities"], "all15 public topic identities independently reassembled")
    wanted_ops = {(topic,stage,clock) for topic in topic_ids for stage,clock in (("first",2000),("before-expiry",2999),("expired",3000))}
    actual_ops = set()
    for row in lane["operator_calls"]:
        key = (row["topic"],row["stage"],row["clock_ms"])
        require(key in wanted_ops and key not in actual_ops, "exact explicit clock/operator coverage")
        actual_ops.add(key)
        payload = bytes.fromhex(row["operator_request_hex"])
        require(len(payload) == 28 and payload[:16].hex() == topic_ids[row["topic"]] and payload[16:20] == b"\0"*4
                and int.from_bytes(payload[20:],"big",signed=True) == row["clock_ms"], "exact actual28B local operator command")
    require(actual_ops == wanted_ops and len(actual_ops) == 45, "all45 durable operator receipts")
    for state in lane["states"]:
        root = path.parent / ("state-" + state["stage"])
        total = 0
        require(len(state["files"]) <= 2048, "bounded retained state file count")
        for row in state["files"]:
            file = root / row["path"]
            require(file.is_relative_to(root) and ".." not in Path(row["path"]).parts and not file.is_symlink()
                    and file.is_file() and file.stat().st_size == row["bytes"] and row["bytes"] <= 1024*1024
                    and sha(file) == row["sha256"], "retained durable state byte/hash/size binding")
            total += row["bytes"]
        require(total == state["bytes"] and total <= 32*1024*1024, "bounded complete state byte total")
    for server in lane["servers"]:
        require(sha(Path(server["log"])) == server["log_sha256"], "actual server log binding")
    for receipt in lane["receipts"].values():
        require(sha(Path(receipt["path"])) == receipt["sha256"], "actual source/compiler receipt binding")
    for binary in lane["binaries"].values():
        require(sha(Path(binary["path"])) == binary["sha256"], "actual executable identity binding")
    return {"path": str(path), "sha256": sha(path), "toolchain": lane["toolchain"], "features": lane["features"],
            "actual_peer_jobs": 160, "actual_record_events": total_records, "actual_assertions": total_assertions,
            "public_topics": 15, "operator_calls": 45, "state_snapshots": 5}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source-sha", required=True)
    parser.add_argument("--lane-report", action="append", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    rows = [check_lane(path,args.source_sha) for path in args.lane_report]
    require(len(rows) == 4 and {(row["toolchain"],row["features"]) for row in rows}
            == {(compiler,features) for compiler in ("stable","1.85.0") for features in ("default","all-features")},
            "four fresh actual toolchain/feature lanes")
    result = {"schema_version": 1, "source_sha": args.source_sha, "passed": True, "verifier_sha256": sha(Path(__file__)),
              "lanes": rows, "actual_peer_jobs": 640, "actual_record_events": 9380, "operator_calls": 180,
              "scope": "Actual public ordinary records/offsets/UUIDs and retained bytes; no inferred empty-header or transactional/replicated-state behavior."}
    args.output.parent.mkdir(parents=True,exist_ok=True)
    args.output.write_text(json.dumps(result,indent=2)+"\n")
    print(json.dumps({"passed":True,"actual_peer_jobs":640,"actual_record_events":9380}),flush=True)


if __name__ == "__main__":
    main()

#!/usr/bin/env python3
"""Validate one frozen live verifiable scenario, retaining every event and peer row."""
import argparse
import hashlib
import importlib.util
import json
import math
from pathlib import Path
import re
import sys

ROOT = Path(__file__).resolve().parents[1]
PROFILE = ROOT / "tests/conformance/verifiable-live-profile.json"


def require(condition, message):
    if not condition:
        raise ValueError(message)


def unique_object(pairs):
    result = {}
    for key, value in pairs:
        require(key not in result, f"duplicate JSON key: {key}")
        result[key] = value
    return result


def load_json(text):
    def invalid(value):
        raise ValueError(f"non-finite JSON value: {value}")
    return json.loads(text, object_pairs_hook=unique_object, parse_constant=invalid)


def integer(value):
    return type(value) is int


def partition_rows(actual, expected):
    """JSON booleans compare equal to integers in Python; reject that ambiguity."""
    return (isinstance(actual, list) and len(actual) == len(expected)
            and all(isinstance(row, dict) and set(row) == set(want)
                    and all(type(row[key]) is type(value) and row[key] == value
                            for key, value in want.items())
                    for row, want in zip(actual, expected)))


def events(text, schemas, started, ended):
    rows = []
    previous = started
    for line in text.splitlines():
        require(bool(line.strip()), "blank line in event stream")
        row = load_json(line)
        require(isinstance(row, dict), "event must be an object")
        name = row.get("name")
        require(name in schemas, f"unknown event: {name}")
        require(set(row) == schemas[name] | {"name", "timestamp"}, f"wrong fields: {name}")
        stamp = row["timestamp"]
        require(integer(stamp) and previous <= stamp <= ended, "invalid event timestamp/order")
        previous = stamp
        rows.append(row)
    require(bool(rows), "empty event stream")
    return rows


def record(row, topic, offset):
    return (row.get("topic") == topic and integer(row.get("partition"))
            and row["partition"] == 0 and integer(row.get("offset"))
            and row["offset"] == offset and row.get("key") is None
            and row.get("value") == str(offset))


def producer_history(text, topic, count, started, ended):
    rows = events(text, {
        "startup_complete": set(), "shutdown_complete": set(),
        "producer_send_success": {"topic", "partition", "offset", "key", "value"},
        "tool_data": {"sent", "acked", "target_throughput", "avg_throughput"},
    }, started, ended)
    require([r["name"] for r in rows] == ["startup_complete"]
            + ["producer_send_success"] * count + ["shutdown_complete", "tool_data"],
            "producer lifecycle, send count or error mismatch")
    for offset, row in enumerate(rows[1:-2]):
        require(record(row, topic, offset), "wrong producer identity/payload/offset")
    tool = rows[-1]
    require(all(integer(tool[k]) for k in ("sent", "acked", "target_throughput")),
            "producer tool counters must be integers")
    require((tool["sent"], tool["acked"], tool["target_throughput"]) == (count, count, -1),
            "producer totals mismatch")
    average = tool["avg_throughput"]
    require(type(average) in (float, int) and math.isfinite(average) and average >= 0,
            "invalid producer throughput summary")
    return {"events": len(rows), "acknowledged": count}


def consumer_history(text, topic, count, started, ended):
    rows = events(text, {
        "startup_complete": set(), "shutdown_complete": set(),
        "partitions_assigned": {"partitions"}, "partitions_revoked": {"partitions"},
        "record_data": {"topic", "partition", "offset", "key", "value"},
        "records_consumed": {"count", "partitions"},
        "offsets_committed": {"success", "offsets"},
    }, started, ended)
    require(rows[0]["name"] == "startup_complete" and rows[-1]["name"] == "shutdown_complete",
            "consumer lifecycle incomplete")
    assigned = False
    assignments = revocations = delivered = polls = commits = 0
    pending = []
    awaiting_commit = False
    last_committed = 0
    partition = {"topic": topic, "partition": 0}
    for index, row in enumerate(rows):
        name = row["name"]
        if name == "startup_complete":
            require(index == 0, "duplicate startup")
        elif name == "shutdown_complete":
            require(index == len(rows) - 1 and not assigned and not pending
                    and not awaiting_commit, "premature consumer shutdown")
        elif name == "partitions_assigned":
            require(not assigned and assignments == 0 and partition_rows(row["partitions"], [partition]),
                    "unexpected assignment")
            assigned = True
            assignments += 1
        elif name == "partitions_revoked":
            require(assigned and partition_rows(row["partitions"], [partition])
                    and delivered == count and not pending and not awaiting_commit,
                    "unexpected/premature revocation")
            assigned = False
            revocations += 1
        elif name == "record_data":
            require(assigned and not awaiting_commit and delivered < count
                    and record(row, topic, delivered), "consumer duplicate/missing/reordered record")
            pending.append(delivered)
            delivered += 1
        elif name == "records_consumed":
            require(assigned and bool(pending) and not awaiting_commit, "unexpected poll summary")
            require(integer(row["count"]) and row["count"] == len(pending), "poll count mismatch")
            summary = partition | {"count": len(pending), "minOffset": pending[0],
                                   "maxOffset": pending[-1]}
            require(partition_rows(row["partitions"], [summary]), "poll offset summary mismatch")
            polls += 1
            awaiting_commit = True
        elif name == "offsets_committed":
            require(assigned and awaiting_commit and row["success"] is True,
                    "missing/failed/unexpected commit")
            offsets = [partition | {"offset": pending[-1] + 1}]
            require(partition_rows(row["offsets"], offsets), "commit is not maxOffset + 1")
            last_committed = pending[-1] + 1
            pending.clear()
            awaiting_commit = False
            commits += 1
    require((assignments, revocations, delivered, last_committed) == (1, 1, count, count)
            and commits == polls and polls > 0, "consumer history incomplete")
    return {"events": len(rows), "delivered": delivered, "polls": polls,
            "commits": commits, "final_committed_offset": last_committed}


def java_history(records, offsets, topic, group, count):
    rows = records.splitlines()
    require(len(rows) == count, "Java record count mismatch")
    found = []
    for line in rows:
        match = re.fullmatch(r"Partition:0\tOffset:(\d+)\tnull\t(\d+)", line)
        require(match is not None, "unexpected Java record row")
        offset, value = map(int, match.groups())
        require(offset == value, "Java payload/offset mismatch")
        found.append(offset)
    require(found == list(range(count)), "Java missing/duplicate/reordered record")
    data = []
    for line in offsets.splitlines():
        fields = line.split()
        if not fields or line == f"Consumer group '{group}' has no active members.":
            continue
        if fields[:3] == ["GROUP", "TOPIC", "PARTITION"]:
            continue
        require(len(fields) == 9, "unexpected Java group row")
        require(fields[:3] == [group, topic, "0"], "Java group/topic/partition mismatch")
        require(fields[3:6] == [str(count), str(count), "0"], "Java committed/end/lag mismatch")
        require(fields[6:] == ["-", "-", "-"], "Java group has an unexpected active member")
        data.append(fields)
    require(len(data) == 1, "missing/duplicate Java group offset")
    return {"records": count, "committed_offset": count, "end_offset": count, "lag": 0}


def validate(root, expected_source, profile_path=PROFILE):
    profile_bytes = profile_path.read_bytes()
    profile = load_json(profile_bytes)
    identity = load_json((root / "identity.json").read_text())
    require(re.fullmatch(r"[0-9a-f]{40}", expected_source) is not None, "invalid candidate source")
    require(identity["candidate_source_sha"] == expected_source, "wrong candidate source")
    require(identity["profile_sha256"] == hashlib.sha256(profile_bytes).hexdigest(), "wrong frozen profile")
    require(identity["broker_reference"] == profile["broker_reference"]
            and identity["broker_version"] == profile["broker_version"], "wrong broker pin")
    image = identity["container_image_id"]
    require(isinstance(image, str) and re.fullmatch(r"sha256:[0-9a-f]{64}", image)
            and image == identity["inspected_image_id"], "wrong container/image identity")
    digest = profile["broker_reference"].split("@", 1)[1]
    require("apache/kafka@" + digest in identity["repo_digests"], "missing frozen repository digest")
    require(re.match(re.escape(profile["broker_version"]) + r"(?:\s|$)", identity["kafka_cli_version"]),
            "wrong actual Java CLI version")
    require(identity["prerequisite_exit_codes"] == {"build": 0, "create-topic": 0}
            and all(integer(v) for v in identity["prerequisite_exit_codes"].values()),
            "missing/failed build or fresh topic creation")
    topic, group = identity["topic"], identity["group"]
    require(re.fullmatch(r"plverifiable-4-1-2-[0-9a-f]{32}", topic) is not None
            and group == topic + "-group", "unowned scenario namespace")
    require(set(identity["exit_codes"]) == {"producer", "consumer", "java-records", "java-offsets"}
            and all(integer(v) and v == 0 for v in identity["exit_codes"].values()),
            "missing/failed required process")
    started, ended = identity["started_ms"], identity["ended_ms"]
    require(integer(started) and integer(ended) and 0 < started <= ended, "invalid execution clock")
    count = profile["records"]
    require(integer(count) and count == 25 and profile["partitions"] == 1, "changed scenario denominator")
    for name in ("producer", "consumer"):
        require((root / f"{name}.stderr.log").read_text() == "", f"{name} stderr is not empty")
    result = {
        "schema_version": 1, "scope": profile["scope"],
        "candidate_source_sha": expected_source, "identity": identity,
        "producer": producer_history((root / "producer.jsonl").read_text(), topic, count, started, ended),
        "consumer": consumer_history((root / "consumer.jsonl").read_text(), topic, count, started, ended),
        "java": java_history((root / "java-records.log").read_text(),
                             (root / "java-offsets.log").read_text(), topic, group, count),
        "cases": [{"id": profile["case_id"], "source_pin": profile["contract_source_pin"],
                   "peer_pin": profile["broker_version"], "peer_identity": "docker:" + profile["broker_reference"],
                   "status": "independent_pass", "artifacts": [str(root / name) for name in
                   ("identity.json", "producer.jsonl", "consumer.jsonl", "java-records.log", "java-offsets.log")]}],
    }
    spec = importlib.util.spec_from_file_location("conformance_report", ROOT / "scripts/conformance-report.py")
    reporter = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(reporter)
    summary = reporter.validate_and_aggregate_reports(profile["registry"], [(str(root), result)],
                                                     require_independent_pass=True, repo_root=ROOT)
    require(summary["success"] and summary["denominator_cases"] == 1, "frozen single-case report failed")
    result["case_summary"] = summary
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("report", type=Path)
    parser.add_argument("source_sha")
    args = parser.parse_args()
    try:
        result = validate(args.report.resolve(), args.source_sha)
    except (ValueError, KeyError, TypeError, OSError) as error:
        print(f"Live verifiable scenario rejected: {error}", file=sys.stderr)
        return 1
    (args.report / "report.json").write_text(json.dumps(result, indent=2) + "\n")
    print("Live verifiable scenario: all 25 acknowledgements, deliveries and Java-confirmed offsets passed")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

#!/usr/bin/env python3
"""Gate benchmark JSONL journals with the shared KL03-18 record-history checker.

Raw journals are read-only. Verdict and normalized history outputs are created
exclusively, so a failed attempt cannot be overwritten by a later invocation.
"""
from __future__ import annotations
import argparse
from collections import Counter
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import re
import sys

ROOT = Path(__file__).resolve().parents[1]
SPEC = importlib.util.spec_from_file_location("partitionline_record_history", ROOT / "scripts/check-record-history.py")
CHECKER = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = CHECKER
SPEC.loader.exec_module(CHECKER)


class InvalidJournal(ValueError):
    """A partial or malformed run cannot clear the correctness gate."""


def require(condition, message):
    if not condition:
        raise InvalidJournal(message)


def integer(value, name, minimum=0):
    require(type(value) is int and value >= minimum, f"{name} must be an integer >= {minimum}")
    return value


def mix(value):
    mask = (1 << 64) - 1
    value = (value + 0x9E3779B97F4A7C15) & mask
    value = ((value ^ (value >> 30)) * 0xBF58476D1CE4E5B9) & mask
    value = ((value ^ (value >> 27)) * 0x94D049BB133111EB) & mask
    return value ^ (value >> 31)


def payload(seed, identity, size):
    require(0 <= seed < 1 << 64 and 0 <= identity < 1 << 64, "seed/ID outside u64")
    require(size >= 24, "history payload must be at least 24 bytes")
    out = bytearray(b"PLBENCH1" + seed.to_bytes(8, "big") + identity.to_bytes(8, "big"))
    word = mix(seed ^ identity)
    while len(out) < size:
        out.extend(word.to_bytes(8, "big")[:min(8, size - len(out))])
        word = mix(word)
    return bytes(out)


def load_journal(path, role):
    rows = []
    for number, line in enumerate(Path(path).read_text().splitlines(), 1):
        require(bool(line.strip()), f"{role} blank journal line {number}")
        row = json.loads(line)
        require(isinstance(row, dict), f"{role} journal line {number} is not an object")
        rows.append(row)
    require(len(rows) >= 3, f"{role} journal is empty or incomplete")
    config, summary = rows[0], rows[-1]
    require(config.get("kind") == "config" and config.get("role") == role and type(config.get("schema_version")) is int and config["schema_version"] == 1, f"{role} config missing or unsupported")
    require(summary.get("kind") == "summary" and summary.get("role") == role, f"{role} completion summary missing")
    require(summary.get("completed") is True and summary.get("run_disposition") == "executed", f"{role} run failed or incomplete")
    records = rows[1:-1]
    for row in records:
        require(row.get("kind") == "record", f"{role} unexpected journal event")
        require(row.get("status") == "accepted", f"{role} raw record status must be accepted, never inferred acknowledgment")
        require(row.get("topic") == config.get("topic"), f"{role} topic disagrees with config")
        integer(row.get("partition"), f"{role} partition")
        require(isinstance(row.get("id"), str) and row["id"], f"{role} record ID missing")
        require(isinstance(row.get("payload_hash"), str) and re.fullmatch(r"[0-9a-f]{64}", row["payload_hash"]), f"{role} SHA-256 missing or malformed")
        integer(row.get("payload_bytes"), f"{role} payload_bytes", 24)
        require(isinstance(row.get("key"), str) and re.fullmatch(r"(?:[0-9a-f]{2})+", row["key"]), f"{role} key missing or malformed")
        if role == "consumer":
            integer(row.get("offset"), "consumer offset")
    return config, records, summary


def build_history(producer_path, consumer_path):
    pc, attempted, ps = load_journal(producer_path, "producer")
    cc, consumed, cs = load_journal(consumer_path, "consumer")
    acks = pc.get("acks")
    require(type(acks) is int and acks in (-1, 0, 1), "invalid producer acks")
    require(type(pc.get("idempotent")) is bool and type(pc.get("transactional")) is bool, "producer semantics missing")
    require(not pc["idempotent"] or acks == -1, "idempotent producer requires acks=-1")
    for key in ("topic", "seed", "payload_bytes", "partitions"):
        require(pc.get(key) == cc.get(key), f"producer/consumer {key} differs")
    require(cc.get("isolation_level") in ("read_committed", "read_uncommitted"), "invalid consumer isolation")
    count = integer(pc.get("count"), "producer count", 1)
    integer(pc.get("seed"), "seed")
    size = integer(pc.get("payload_bytes"), "payload size", 24)
    partitions = integer(pc.get("partitions"), "partitions", 1)
    total = integer(ps.get("accepted_total"), "accepted_total", 1)
    warmup = integer(ps.get("warmup_records"), "warmup_records")
    require(integer(ps.get("measured_records"), "measured_records", 1) == count, "measured count differs from requested count")
    require(total == count + warmup == len(attempted), "accepted count differs from complete producer record history")
    require(integer(ps.get("produce_errors"), "produce_errors") == 0, "producer errors invalidate the run")
    expected_acknowledged = total if acks != 0 else 0
    require(integer(ps.get("acknowledged_total"), "acknowledged_total") == expected_acknowledged, "acks0 cannot be acknowledged; acknowledged total did not settle all accepted records")
    require(integer(ps.get("locally_completed_total"), "locally_completed_total") == (total if acks == 0 else 0), "local completion count differs from acks semantics")
    require(integer(cc.get("count"), "consumer count", 1) == total, "consumer count must include warmup and measured application records")
    require(integer(cs.get("consumed"), "consumed", 1) == len(consumed), "consumer summary differs from raw application record count")
    require(integer(cs.get("verified"), "verified", 1) == len(consumed) and integer(cs.get("verify_mismatches"), "verify_mismatches") == 0, "consumer verification failed")
    require(integer(cs.get("unique_ids"), "unique_ids", 1) == len(set(r["id"] for r in consumed)), "consumer unique-ID summary differs from history")
    require(all(row["payload_bytes"] == size for row in consumed), "consumer payload size differs")
    offsets = cc.get("end_offsets")
    require(isinstance(offsets, list) and len(offsets) == partitions, "consumer offset fence missing")
    fence = {}
    for row in offsets:
        require(isinstance(row, dict), "invalid consumer offset fence")
        partition = integer(row.get("partition"), "fence partition")
        require(partition not in fence, "duplicate fence partition")
        fence[partition] = integer(row.get("offset"), "fence offset")
    require(set(fence) == set(range(partitions)), "incomplete consumer partition fence")
    require(all(row["partition"] in fence and row["offset"] < fence[row["partition"]] for row in consumed), "consumer row outside independent offset fence")
    # Independently derive every expected payload from the frozen generator,
    # rather than trusting either client's reported SHA-256.
    for identity, row in enumerate(attempted):
        require(row["id"] == f'{pc["seed"]:016x}:{identity}', "producer ID history is incomplete or reordered")
        require(row["partition"] == identity % partitions, "producer deterministic partition differs")
        require(row["payload_bytes"] == size, "producer payload size differs")
        require(row["payload_hash"] == hashlib.sha256(payload(pc["seed"], identity, size)).hexdigest(), "producer hash differs from independent deterministic payload")
        require(row["key"] == f'plbench-{pc["seed"]:016x}-{row["partition"]}'.encode().hex(), "producer deterministic key differs")
        require(row.get("phase") == ("warmup" if identity < warmup else "measure"), "producer phase differs from warmup boundary")
    history = {
        "history_id": "benchmark-record-history",
        "config": {"acks": acks, "idempotent": pc["idempotent"], "transactional": pc["transactional"], "isolation_level": cc["isolation_level"], "delivery": "partition"},
        "attempted": [{**r, "status": "acked" if acks else "accepted", "attempt_index": i} for i, r in enumerate(attempted)],
        "consumed": consumed,
        "transactions": pc.get("transactions", []),
    }
    return history


def verify(producer_path, consumer_path):
    history = build_history(producer_path, consumer_path)
    result = CHECKER.verify_history(history).to_dict()
    # The generic checker correctly permits acks0 data loss. This benchmark
    # gate additionally requires the frozen workload's complete ID set and
    # uniqueness to establish receipt integrity; it never promotes local
    # completion to broker acknowledgment.
    wanted = Counter(r["id"] for r in history["attempted"])
    actual = Counter(r["id"] for r in history["consumed"])
    if actual != wanted:
        result["valid"] = False
        result["violations"].append({"type": "BENCHMARK_ID_SET_MISMATCH", "message": "application ID multisets differ even if totals match"})
        result["minimal_counterexample"] = result["minimal_counterexample"] or result["violations"][-1]
    if history["config"]["acks"] == 0:
        produce_order = {row["id"]: i for i, row in enumerate(history["attempted"])}
        previous = {}
        for row in history["consumed"]:
            if row["id"] not in produce_order:
                continue
            order = produce_order[row["id"]]
            partition = (row["topic"], row["partition"])
            if partition in previous and order <= previous[partition]:
                result["valid"] = False
                violation = {"type": "BENCHMARK_ORDERING_VIOLATION", "message": "acks0 receipt history regressed in the deterministic partition order"}
                result["violations"].append(violation)
                result["minimal_counterexample"] = result["minimal_counterexample"] or violation
            previous[partition] = order
    if not result["valid"]:
        result["summary"] = f'FAIL: benchmark record-history gate rejected {len(result["violations"])} violation(s)'
    result.update(integrity_verified=result["valid"], performance_claims_invalidated=not result["valid"], qualification=False, suite_hold="active", acknowledged_throughput=history["config"]["acks"] != 0)
    return history, result


def write_new(path, value):
    with Path(path).open("x") as output:
        json.dump(value, output, indent=2)
        output.write("\n")


def validate_env():
    def setting(key, default, minimum):
        value = os.environ.get(key, str(default))
        require(re.fullmatch(r"[0-9]+", value) is not None, f"invalid {key}")
        number = int(value)
        require(str(number) == value and minimum <= number <= (1 << 64) - 1, f"invalid {key}")
        return number
    for key, default in (("COUNT", 5000), ("RUNS", 1), ("PARTITIONS", 1)):
        setting(key, default, 1)
    setting("PAYLOAD_BYTES", 100, 24)
    setting("SEED", 1592590337, 0)
    setting("LINGER_MS", 5, 0)
    setting("CONNECTIONS", 8, 1)
    setting("MAX_IN_FLIGHT", 16, 1)
    setting("MAX_WAIT_MS", 100, 0)
    max_bytes = setting("MAX_BYTES", 16777216, 1)
    min_bytes = setting("MIN_BYTES", 1, 1)
    require(min_bytes <= max_bytes, "MIN_BYTES exceeds MAX_BYTES")
    setting("WARMUP_SECS", 0, 0)
    require(os.environ.get("WARMUP_SECS", "0") == "0", "integrity harness requires WARMUP_SECS=0; warmup records need a distinct campaign phase")
    require(os.environ.get("ACKS", "1") in ("1", "-1"), "integrity harness requires ACKS=1 or -1; ACKS=0 is local completion")
    require(os.environ.get("IDEMPOTENT", "0") in ("0", "1"), "IDEMPOTENT must be 0 or 1")
    require(os.environ.get("IDEMPOTENT", "0") != "1" or os.environ.get("ACKS", "1") == "-1", "IDEMPOTENT=1 requires ACKS=-1")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--validate-env", action="store_true")
    parser.add_argument("--producer")
    parser.add_argument("--consumer")
    parser.add_argument("--output")
    parser.add_argument("--history")
    args = parser.parse_args()
    if args.validate_env:
        try:
            validate_env()
            return 0
        except InvalidJournal as error:
            print(str(error), file=sys.stderr)
            return 2
    if not all((args.producer, args.consumer, args.output)):
        parser.error("--producer, --consumer, and --output are required")
    status = 0
    try:
        history, result = verify(args.producer, args.consumer)
        if args.history:
            write_new(args.history, history)
        status = 0 if result["valid"] else 1
    except (OSError, ValueError, CHECKER.HistoryValidationError) as error:
        result = {"valid": False, "integrity_verified": False, "performance_claims_invalidated": True, "qualification": False, "suite_hold": "active", "error": str(error)}
        status = 2
    try:
        write_new(args.output, result)
    except OSError as error:
        print(f"cannot create verdict without overwriting evidence: {error}", file=sys.stderr)
        return 2
    print(json.dumps(result))
    return status


if __name__ == "__main__":
    sys.exit(main())

#!/usr/bin/env python3
"""Verify closed-loop diagnostic acknowledgements against independent Java receipts."""
import argparse
from collections import deque
import csv
import json
import math
from pathlib import Path

MASK = (1 << 64) - 1
SEED = 0x5EED0001

def require(condition, message):
    if not condition:
        raise ValueError(message)

def mix(value):
    value = (value + 0x9E3779B97F4A7C15) & MASK
    value = ((value ^ (value >> 30)) * 0xBF58476D1CE4E5B9) & MASK
    value = ((value ^ (value >> 27)) * 0x94D049BB133111EB) & MASK
    return value ^ (value >> 31)

def expected_record(index, payload_bytes):
    key = index.to_bytes(8, "big") + mix(SEED ^ index).to_bytes(8, "big")
    state = SEED ^ ((index * 0x9E3779B97F4A7C15) & MASK)
    payload = bytearray()
    while len(payload) < payload_bytes:
        state = mix(state)
        payload.extend(state.to_bytes(8, "big"))
    return key, bytes(payload[:payload_bytes])

def partition_count(count, partitions, partition):
    return 0 if partition >= count else 1 + (count - 1 - partition) // partitions

def expected_offset(phase, index, warmup, partitions):
    part = index % partitions
    start = partition_count(warmup, partitions, part) if phase == "measure" else 0
    return part, start + index // partitions

def read_csv(path):
    with Path(path).open(newline="", encoding="utf-8") as stream:
        return list(csv.reader(stream))

def parse_trace(path, kind, warmup, count, partitions, payload_bytes):
    rows = deque(read_csv(path))
    if kind == "rust":
        require(len(rows) >= 3, "truncated Rust trace")
        require(rows.popleft() == ["config", str(SEED), str(warmup), str(count), str(partitions), str(payload_bytes)],
                "Rust config mismatch")
        settings = rows.popleft()
        require(len(settings) == 12
                and settings[:11] == ["settings", "-1", "true", "0", "1000000", "1", "1", "1", "33554432", "30000", "1048576"]
                and 0 < int(settings[11]) <= 1_048_576 < 33_554_432,
                "Rust configured settings or record bound mismatch")
    result = {}
    for phase, total in [("warmup", warmup), ("measure", count)]:
        for index in range(total):
            require(rows, f"missing {phase} acknowledgement {index}")
            row = rows.popleft()
            require(len(row) == (7 if kind == "c" else 6) and row[0] == "record",
                    f"malformed {kind} acknowledgement")
            require(row[1] == phase and int(row[2]) == index, "acknowledgements reordered or repeated")
            partition, offset = expected_offset(phase, index, warmup, partitions)
            require(int(row[3]) == partition and int(row[4]) == offset, "acknowledgement partition/offset differs")
            if kind == "c":
                require(row[5:] == ["ack", "0"], "C delivery callback failed")
            else:
                require(int(row[5]) > 0, "invalid Rust latency sample")
            result[(partition, offset)] = (phase, index)
        require(rows, f"missing {phase} completion")
        row = rows.popleft()
        if kind == "c":
            require(row == ["phase_end", phase, str(total), "1"], "C phase not fully completed at occupancy one")
        else:
            require(len(row) == 5 and row[:3] == ["phase_end", phase, str(total)]
                    and int(row[3]) > 0 and row[4] == "1",
                    "Rust phase not fully completed at occupancy one")
    require(list(rows) == [["complete"]], "trace incomplete or has trailing records")
    return result

def verify_fences(path, warmup, count, partitions):
    rows = Path(path).read_text(encoding="utf-8").splitlines()
    require(len(rows) == partitions, "incomplete independent high-watermarks")
    fences = {}
    for row in rows:
        fields = row.split("\t")
        require(len(fields) == 3, "malformed independent fence")
        partition, start, end = map(int, fields)
        require(0 <= partition < partitions and partition not in fences and start == 0,
                "invalid or duplicate independent fence")
        expected = partition_count(warmup, partitions, partition) + partition_count(count, partitions, partition)
        require(end == expected, "independent end offset differs from acknowledged workload")
        fences[partition] = end
    return fences

def verify_java_receipts(path, ack_map, fences, warmup, count, partitions, payload_bytes):
    seen = set()
    lines = Path(path).read_text(encoding="utf-8").splitlines()
    require(len(lines) == warmup + count, "independent receipt count differs")
    for line in lines:
        row = json.loads(line)
        require(type(row.get("partition")) is int and type(row.get("offset")) is int,
                "invalid receipt coordinates")
        coordinate = (row["partition"], row["offset"])
        require(coordinate in ack_map and coordinate not in seen, "missing, unexpected, or duplicate receipt")
        require(row["partition"] in fences and 0 <= row["offset"] < fences[row["partition"]],
                "receipt outside independent fence")
        phase, index = ack_map[coordinate]
        key, payload = expected_record(index, payload_bytes)
        require(row.get("key") == key.hex() and row.get("value") == payload.hex(),
                f"{phase} record bytes differ")
        require(row.get("headers") == 0, "unexpected record headers")
        seen.add(coordinate)
    require(seen == set(ack_map), "not all acknowledgements were read independently")

def verify_c_raw(raw_path, samples_path, warmup, count, partitions):
    raw = json.loads(Path(raw_path).read_text(encoding="utf-8"))
    require(raw.get("client") == "librdkafka-native-c" and raw.get("version") == "2.15.0",
            "C library identity mismatch")
    effective = raw["effective_config"]
    for key, wanted in {
        "request.required.acks": "-1",
        "enable.idempotence": "true",
        "queue.buffering.max.ms": "0",
        "batch.size": "1000000",
        "batch.num.messages": "1",
        "max.in.flight.requests.per.connection": "1",
        "queue.buffering.max.messages": "1000000",
        "queue.buffering.max.kbytes": "32768",
        "compression.codec": "none",
        "message.timeout.ms": "30000",
        "message.max.bytes": "1048576",
        "security.protocol": "plaintext",
        "allow.auto.create.topics": "false",
        "socket.nagle.disable": "true",
    }.items():
        value = str(effective.get(key, "")).lower()
        if key == "queue.buffering.max.ms":
            require(float(value) == 0.0, "C linger mismatch")
        else:
            require(value == wanted, f"C effective setting mismatch: {key}={value!r}")
    for name, total in [("warmup", warmup), ("timed", count)]:
        phase = raw[name]
        require(all(type(phase.get(key)) is int and phase.get(key) == total for key in ("offered", "accepted", "acknowledged")),
                f"C {name} did not complete all outcomes")
        require(all(type(phase.get(key)) is int and phase.get(key) == 0 for key in ("rejected", "timed_out", "unknown", "callback_failures", "queue_full_retries")),
                f"C {name} has failed/unknown/queue-full outcomes")
        require(type(phase.get("max_outstanding")) is int and phase.get("max_outstanding") == 1
                and isinstance(phase.get("elapsed_s"), (int, float))
                and math.isfinite(phase["elapsed_s"]) and phase["elapsed_s"] > 0,
                f"C {name} occupancy or elapsed time invalid")
        require(not phase.get("errors"), f"C {name} has error events")
    durable = raw["durability"]
    require(durable.get("verified") and durable.get("replication_factor") == 1
            and durable.get("min_insync_replicas") == 1 and raw.get("broker_nodes") == 1,
            "broker replication/min-ISR shape differs")
    high = raw["high_watermarks"]
    require(high.get("queried") and high.get("total_offset_delta") == count,
            "C measured-phase high watermarks differ")
    found = set()
    for part in high["partitions"]:
        p = part["partition"]
        require(p not in found and 0 <= p < partitions, "invalid C watermark partition")
        found.add(p)
        start = partition_count(warmup, partitions, p)
        end = start + partition_count(count, partitions, p)
        require(part["start_offset"] == start and part["end_offset"] == end
                and part["offset_delta"] == end - start, "C high-watermark fence mismatch")
    require(len(found) == partitions, "incomplete C watermarks")
    check = raw["verification"]
    require(check.get("performed") and check.get("verified_ids") == count
            and not check.get("duplicate_ids") and not check.get("bad_records"),
            "native consumer receipt check failed")
    samples = read_csv(samples_path)
    require(samples and samples.pop(0) == ["record_id", "latency_us"], "C latency header differs")
    require(len(samples) == count, "C did not retain every timed callback")
    for index, row in enumerate(samples):
        require(len(row) == 2 and int(row[0]) == index and math.isfinite(float(row[1])) and float(row[1]) >= 0,
                "C callback IDs or samples differ")

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--kind", choices=["rust", "c"], required=True)
    parser.add_argument("--trace", required=True)
    parser.add_argument("--fences", required=True)
    parser.add_argument("--receipts", required=True)
    parser.add_argument("--c-raw")
    parser.add_argument("--c-samples")
    parser.add_argument("--warmup", type=int, required=True)
    parser.add_argument("--count", type=int, required=True)
    parser.add_argument("--partitions", type=int, required=True)
    parser.add_argument("--payload-bytes", type=int, required=True)
    args = parser.parse_args()
    require(args.warmup > 0 and args.count > 0 and args.partitions > 0
            and args.payload_bytes > 0, "nonpositive workload")
    acks = parse_trace(args.trace, args.kind, args.warmup, args.count, args.partitions, args.payload_bytes)
    fences = verify_fences(args.fences, args.warmup, args.count, args.partitions)
    verify_java_receipts(args.receipts, acks, fences, args.warmup, args.count, args.partitions, args.payload_bytes)
    if args.kind == "c":
        require(args.c_raw and args.c_samples, "C raw and samples are required")
        verify_c_raw(args.c_raw, args.c_samples, args.warmup, args.count, args.partitions)
    print(json.dumps({"kind": args.kind, "warmup_acknowledged": args.warmup,
          "measured_acknowledged": args.count, "independently_read": len(acks),
          "max_application_outstanding": 1, "isolation": "read_uncommitted",
          "disposition": "exploratory_noncomparable", "matrix_eligible": False},
          sort_keys=True))

if __name__ == "__main__":
    main()

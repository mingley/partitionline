#!/usr/bin/env python3
"""Independently check every acknowledged record; report closed-loop diagnostics."""
import argparse
from collections import deque
import csv
import json
from pathlib import Path
import statistics

MASK = (1 << 64) - 1
SEED = 0x5EED0001
GOLD = 0x9E3779B97F4A7C15

def require(ok, message):
    if not ok:
        raise ValueError(message)

def mix(value):
    value = (value + GOLD) & MASK
    value = ((value ^ (value >> 30)) * 0xBF58476D1CE4E5B9) & MASK
    value = ((value ^ (value >> 27)) * 0x94D049BB133111EB) & MASK
    return value ^ (value >> 31)

def expected_record(index, payload_bytes):
    key = index.to_bytes(8, "big") + mix(SEED ^ index).to_bytes(8, "big")
    state = SEED ^ ((index * GOLD) & MASK)
    value = bytearray()
    while len(value) < payload_bytes:
        state = mix(state)
        value.extend(state.to_bytes(8, "big"))
    return key, bytes(value[:payload_bytes])

def verify_trace(path, warmup, count, payload):
    with Path(path).open(newline="", encoding="utf-8") as f:
        rows = deque(csv.reader(f))
    require(len(rows) >= 5, "truncated trace")
    require(rows.popleft() == ["config", str(SEED), str(warmup), str(count), "1", str(payload)], "workload mismatch")
    require(rows.popleft() == ["settings", "-1", "true", "0", "1000000", "1", "1", "1", "33554432", "30000", "1048576", "540000"], "settings mismatch")
    samples = {}
    for phase, start, total in (("warmup", 0, warmup), ("measure", warmup, count)):
        times = []
        for index in range(start, start + total):
            require(rows, f"missing {phase} acknowledgement {index}")
            row = rows.popleft()
            require(len(row) == 6 and row[:5] == ["record", phase, str(index), "0", str(index)],
                    f"wrong {phase} delivery for record {index}")
            elapsed = int(row[5])
            require(elapsed > 0, f"nonpositive delivery latency {index}")
            times.append(elapsed)
        require(rows, f"missing {phase} completion")
        row = rows.popleft()
        require(len(row) == 5 and row[:3] == ["phase_end", phase, str(total)] and int(row[3]) > 0 and row[4] == "1",
                f"invalid {phase} completion or occupancy")
        samples[phase] = (times, int(row[3]))
    require(list(rows) == [["complete"]], "trace incomplete or trailing rows")
    return samples

def verify_receipts(path, fences, total, payload):
    require(Path(fences).read_text(encoding="utf-8") == f"0\t0\t{total}\n", "independent fence mismatch")
    seen = set()
    with Path(path).open(encoding="utf-8") as f:
        for line in f:
            row = json.loads(line)
            index = row.get("offset")
            require(type(index) is int and row.get("partition") == 0 and 0 <= index < total and index not in seen,
                    f"duplicate/unexpected independent coordinate {index}")
            key, value = expected_record(index, payload)
            require(row.get("key") == key.hex() and row.get("value") == value.hex(),
                    f"independent bytes differ for record {index}")
            require(row.get("headers") == 0, f"headers differ for record {index}")
            seen.add(index)
    require(len(seen) == total, f"independent receipts {len(seen)} != {total}")

def verify_c_config(path):
    actual = json.loads(Path(path).read_text(encoding="utf-8"))
    expected = {
        "request.required.acks": "-1", "enable.idempotence": "true",
        "queue.buffering.max.ms": "0", "batch.size": "1000000",
        "batch.num.messages": "1", "max.in.flight.requests.per.connection": "1",
        "queue.buffering.max.messages": "1000000", "queue.buffering.max.kbytes": "32768",
        "compression.codec": "none", "message.timeout.ms": "30000", "request.timeout.ms": "30000", "message.max.bytes": "1048576",
        "security.protocol": "plaintext", "allow.auto.create.topics": "false",
        "socket.nagle.disable": "true", "socket.timeout.ms": "30000",
        "socket.connection.setup.timeout.ms": "10000", "connections.max.idle.ms": "540000",
    }
    for key, wanted in expected.items():
        value = str(actual.get(key, "")).lower()
        if key == "queue.buffering.max.ms":
            require(value and float(value) == 0, f"config mismatch {key}={value!r}")
        else:
            require(value == wanted, f"config mismatch {key}={value!r}")

def percentile(sorted_values, p):
    return sorted_values[min(len(sorted_values) - 1, (len(sorted_values) * p + 999) // 1000 - 1)] / 1000

def metrics(times, elapsed_ns):
    values = sorted(times)
    return {"elapsed_ns": elapsed_ns, "samples": len(values),
            "records_per_s": len(values) * 1e9 / elapsed_ns,
            "value_bytes_per_s": len(values) * 100 * 1e9 / elapsed_ns,
            "min_us": values[0] / 1000, "mean_us": statistics.mean(values) / 1000,
            "p50_us": percentile(values, 500), "p95_us": percentile(values, 950),
            "p99_us": percentile(values, 990), "p999_us": percentile(values, 999),
            "max_us": values[-1] / 1000}

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--kind", required=True, choices=("rust", "c"))
    parser.add_argument("--trace", required=True)
    parser.add_argument("--fences", required=True)
    parser.add_argument("--receipts", required=True)
    parser.add_argument("--c-config")
    parser.add_argument("--warmup", type=int, required=True)
    parser.add_argument("--count", type=int, required=True)
    parser.add_argument("--payload", type=int, required=True)
    args = parser.parse_args()
    require(args.warmup > 0 and args.count > 0 and args.payload == 100, "invalid workload")
    samples = verify_trace(args.trace, args.warmup, args.count, args.payload)
    verify_receipts(args.receipts, args.fences, args.warmup + args.count, args.payload)
    if args.kind == "c":
        require(args.c_config is not None, "C config missing")
        verify_c_config(args.c_config)
    print(json.dumps({"kind": args.kind, "verified_acknowledgements": args.warmup + args.count,
                      "independent_receipts": args.warmup + args.count,
                      "max_application_outstanding": 1, "matrix_eligible": False,
                      "measurement": "exploratory closed-loop call-to-completion",
                      "warmup": metrics(*samples["warmup"]), "measure": metrics(*samples["measure"])},
                     indent=2, sort_keys=True))

if __name__ == "__main__":
    main()

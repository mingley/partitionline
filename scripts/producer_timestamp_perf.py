#!/usr/bin/env python3
"""Run paired, calibrated diagnostic samples on the same host."""
import hashlib
import json
import os
import re
import subprocess
import sys

BASE_JSON, FIX_JSON, REPORT = sys.argv[1:]

def artifacts(path):
    found = {}
    with open(path, encoding="utf-8") as source:
        for line in source:
            message = json.loads(line)
            if message.get("reason") != "compiler-artifact" or not message.get("executable"):
                continue
            name = message.get("target", {}).get("name")
            if name == "producer_timestamp_perf":
                kind = "producer"
            elif name == "partitionline" and message.get("profile", {}).get("test"):
                kind = "encode"
            else:
                continue
            if kind in found:
                raise RuntimeError(f"duplicate {kind} artifact in {path}")
            found[kind] = message["executable"]
    if set(found) != {"producer", "encode"}:
        raise RuntimeError(f"missing artifact in {path}: {found}")
    return found

base = artifacts(BASE_JSON)
fix = artifacts(FIX_JSON)
ARMS = {
    "producer": {"baseline": base["producer"], "candidate": fix["producer"]},
    "encode": {"baseline": base["encode"], "candidate": fix["encode"]},
}
binary_hashes = {}
for kind, arms in ARMS.items():
    binary_hashes[kind] = {}
    for arm, path in arms.items():
        with open(path, "rb") as binary:
            digest = hashlib.sha256(binary.read()).hexdigest()
        binary_hashes[kind][arm] = digest
        print(f"TIMESTAMP_BENCH_BINARY kind={kind} arm={arm} path={path} sha256={digest}", flush=True)
    if binary_hashes[kind]["baseline"] == binary_hashes[kind]["candidate"]:
        raise RuntimeError(f"same binary for both {kind} arms")
TESTS = {
    "producer": "timestamp_producer_sample",
    "encode": "producer::tests::timestamp_encode_sample",
}
def sample(kind, arm, mode, batch, rounds):
    env = dict(os.environ, PL_BENCH_MODE=mode, PL_BENCH_BATCH=str(batch), PL_BENCH_ROUNDS=str(rounds))
    cmd = [ARMS[kind][arm], "--ignored", "--exact", TESTS[kind], "--nocapture"]
    result = subprocess.run(cmd, env=env, text=True, capture_output=True, timeout=180)
    if result.returncode:
        print(result.stdout, result.stderr, file=sys.stderr)
        raise RuntimeError(f"sample failed: {kind} {arm} {mode} {batch} {rounds}")
    marker = f"TIMESTAMP_BENCH_{kind.upper()} "
    lines = [line[line.index(marker):] for line in result.stdout.splitlines() if marker in line]
    if len(lines) != 1 or "test result: ok. 1 passed" not in result.stdout:
        print(result.stdout, result.stderr, file=sys.stderr)
        raise RuntimeError("invalid sample format or test count")
    parts = lines[0].split()
    data = dict(part.split("=", 1) for part in parts[1:])
    if data["mode"] != mode or int(data["batch"]) != batch or int(data["rounds"]) != rounds:
        raise RuntimeError("wrong sample identity")
    records, elapsed = int(data["records"]), int(data["elapsed_ns"])
    if records != batch * rounds or elapsed <= 0 or data.get("timestamps_verified") != "1":
        raise RuntimeError("invalid sample quantities or timestamps")
    row = {"kind": kind, "arm": arm, "mode": mode, "batch": batch, "rounds": rounds,
           "records": records, "elapsed_ns": elapsed}
    if kind == "encode":
        if data.get("reset_implicit") != ("1" if mode == "implicit" else "0"):
            raise RuntimeError("implicit timestamp reset was not verified")
        row.update(bytes=int(data["bytes"]), crc=int(data["crc"]), reset_implicit=int(data["reset_implicit"]))
    elif data.get("new_records") != "1":
        raise RuntimeError("fresh producer records not verified")
    print(json.dumps(row, sort_keys=True), flush=True)
    return row

report = {"minimum_calibration_ns": 250_000_000, "binary_hashes": binary_hashes, "calibration": [], "samples": []}
for kind in ("producer", "encode"):
    for batch in (1, 4096):
        if kind == "producer":
            limit = 100_000 if batch == 1 else 256
        else:
            limit = 4_194_304 if batch == 1 else 4096
        required = 1
        for mode in ("implicit", "explicit"):
            for arm in ("baseline", "candidate"):
                rounds = 1
                while True:
                    row = sample(kind, arm, mode, batch, rounds)
                    report["calibration"].append(row)
                    if row["elapsed_ns"] >= report["minimum_calibration_ns"]:
                        required = max(required, rounds)
                        break
                    if rounds >= limit:
                        raise RuntimeError(f"calibration did not reach 250ms: {kind} {batch} {mode} {arm}")
                    rounds = min(rounds * 2, limit)
        for mode in ("implicit", "explicit"):
            for i in range(8):
                order = ("baseline", "candidate") if i % 2 == 0 else ("candidate", "baseline")
                for arm in order:
                    row = sample(kind, arm, mode, batch, required)
                    row["pair"] = i
                    report["samples"].append(row)
        if kind == "encode":
            for mode in ("implicit", "explicit"):
                identities = {(x["bytes"], x["crc"]) for x in report["samples"]
                              if x["kind"] == "encode" and x["mode"] == mode and x["batch"] == batch}
                if len(identities) != 1:
                    raise RuntimeError(f"encoded bodies differ: {mode} {batch}: {identities}")
with open(REPORT, "w", encoding="utf-8") as out:
    json.dump(report, out, sort_keys=True, indent=2)
    out.write("\n")
print(f"TIMESTAMP_BENCH_REPORT {REPORT}", flush=True)

#!/usr/bin/env python3
"""Compare real Kafka Produce wire create time with public receipt metadata."""
import glob
import hashlib
import json
import os
from pathlib import Path
import re
import subprocess
import sys

base_json, fix_json, classpath, capture_dir, proxy_log, report_path = sys.argv[1:]

def binary(path):
    found = []
    with open(path, encoding="utf-8") as src:
        for line in src:
            obj = json.loads(line)
            if (obj.get("reason") == "compiler-artifact"
                    and obj.get("target", {}).get("name") == "producer_timestamp_kafka_smoke"
                    and obj.get("executable")):
                found.append(obj["executable"])
    if len(found) != 1:
        raise RuntimeError(f"expected one smoke executable in {path}, found {found}")
    return found[0]

bins = {"baseline": binary(base_json), "candidate": binary(fix_json)}
hashes = {}
for arm, path in bins.items():
    with open(path, "rb") as binary_file:
        hashes[arm] = hashlib.sha256(binary_file.read()).hexdigest()
    print(f"KAFKA_BINARY arm={arm} sha256={hashes[arm]}", flush=True)
if hashes["baseline"] == hashes["candidate"]:
    raise RuntimeError("baseline and candidate executables are identical")

receipt_pattern = re.compile(
    r"KAFKA_RECEIPT marker=(baseline|candidate) offset=(-?\d+) timestamp=(-?\d+) elapsed_ms=(\d+)"
)
receipts = {}
for arm, topic in (("baseline", "pl-time-baseline"), ("candidate", "pl-time-candidate")):
    env = dict(os.environ, PL_LIVE_ENDPOINT="127.0.0.1:9092", PL_LIVE_TOPIC=topic, PL_LIVE_MARKER=arm)
    result = subprocess.run(
        [bins[arm], "--ignored", "--exact", "producer_timestamp_real_kafka", "--nocapture"],
        env=env, text=True, capture_output=True, timeout=90,
    )
    print(result.stdout, flush=True)
    if result.returncode:
        print(result.stderr, file=sys.stderr)
        raise RuntimeError(f"real broker {arm} producer failed")
    found = receipt_pattern.findall(result.stdout)
    if len(found) != 1 or found[0][0] != arm:
        raise RuntimeError(f"invalid receipt output: {found}")
    _, offset, timestamp, elapsed = found[0]
    receipts[arm] = {"offset": int(offset), "timestamp": int(timestamp), "elapsed_ms": int(elapsed)}

wire_pattern = re.compile(
    r"KAFKA_WIRE marker=(baseline|candidate) version=(\d+) correlation=(-?\d+) "
    r"timestamp=(-?\d+) base_timestamp=(-?\d+) error=(-?\d+) offset=(-?\d+) log_append=(-?\d+)"
)
wire = {}
for request in sorted(glob.glob(str(Path(capture_dir) / "*.req"))):
    response = request[:-4] + ".resp"
    if not Path(response).is_file():
        raise RuntimeError(f"missing response for {request}")
    result = subprocess.run(
        ["java", "-cp", classpath, "ObserveProduceTime", request, response],
        text=True, capture_output=True, timeout=30,
    )
    print(result.stdout, flush=True)
    if result.returncode:
        print(result.stderr, file=sys.stderr)
        raise RuntimeError(f"independent Java decoder failed for {request}")
    found = wire_pattern.findall(result.stdout)
    if len(found) != 1:
        raise RuntimeError(f"invalid wire output: {found}")
    marker, version, correlation, timestamp, base_time, error, offset, append = found[0]
    if marker in wire:
        raise RuntimeError(f"duplicate Produce attempt for {marker}")
    wire[marker] = {
        "version": int(version), "correlation": int(correlation),
        "timestamp": int(timestamp), "base_timestamp": int(base_time),
        "error": int(error), "offset": int(offset), "log_append": int(append),
    }
if set(wire) != {"baseline", "candidate"}:
    raise RuntimeError(f"unexpected captured markers: {wire}")
for arm in ("baseline", "candidate"):
    row = wire[arm]
    if row["timestamp"] != row["base_timestamp"] or row["error"] != 0 or row["log_append"] != -1:
        raise RuntimeError(f"unexpected real broker response or record: {arm}: {row}")
    if row["offset"] != receipts[arm]["offset"] or receipts[arm]["elapsed_ms"] < 150:
        raise RuntimeError(f"receipt mismatch or proxy delay absent: {arm}")
if receipts["baseline"]["timestamp"] == wire["baseline"]["timestamp"]:
    raise RuntimeError("baseline did not show delayed-ack timestamp difference")
if receipts["candidate"]["timestamp"] != wire["candidate"]["timestamp"]:
    raise RuntimeError("candidate receipt did not retain the Produce create time")
with open(proxy_log, encoding="utf-8") as src:
    delays = [int(x) for x in re.findall(r"PROXY_RESPONSE api=0 correlation=-?\d+ delay_ns=(\d+)", src.read())]
if len(delays) != 2 or any(delay < 150_000_000 for delay in delays):
    raise RuntimeError(f"missing delayed Produce responses: {delays}")
report = {"binary_hashes": hashes, "receipts": receipts, "wire": wire, "produce_response_delay_ns": delays}
with open(report_path, "w", encoding="utf-8") as out:
    json.dump(report, out, indent=2, sort_keys=True)
    out.write("\n")
print("KAFKA_TIMESTAMP_COMPARISON baseline=different candidate=equal broker_log_append=-1", flush=True)

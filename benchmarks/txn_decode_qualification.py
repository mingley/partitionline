#!/usr/bin/env python3
"""Paired exploratory loopback timings of the real transaction roundtrip."""
import json
import os
import platform
import random
import statistics
import subprocess
import sys

base, candidate = sys.argv[1:]
cpus = os.sched_getaffinity(0)
cpu = min(cpus)
os.sched_setaffinity(0, {cpu})
rng = random.Random(20261108)
print("ENVIRONMENT", json.dumps({
    "cpu": cpu, "original_affinity": sorted(cpus), "platform": platform.platform(),
    "machine": platform.machine(), "seed": 20261108, "warmup": 40, "samples": 300,
    "measurement": "sequential per-request wall time; socket, scheduler and decode",
}), flush=True)
if platform.machine() != "x86_64":
    raise RuntimeError("x86_64 required")

def run(kind, executable, version, count, pair):
    env = dict(os.environ, PL_TXN_PARTITIONS=str(count), PL_TXN_VERSION=str(version),
               PL_TXN_REUSE="1" if kind == "candidate" else "0")
    cmd = [executable, "producer::tests::txn_decode_loopback_benchmark", "--exact",
           "--ignored", "--nocapture", "--test-threads=1"]
    result = subprocess.run(cmd, env=env, text=True, capture_output=True, timeout=120)
    if result.returncode != 0:
        print("CHILD FAILURE", json.dumps({"kind": kind, "version": version,
              "partitions": count, "pair": pair, "returncode": result.returncode}), flush=True)
        print("CHILD STDOUT\n" + result.stdout, flush=True)
        print("CHILD STDERR\n" + result.stderr, flush=True)
        raise RuntimeError("benchmark child failed")
    marker = "PL_TXN_DECODE_BENCH "
    found = [line.split(marker, 1)[1] for line in result.stdout.splitlines() if marker in line]
    if len(found) != 1:
        raise RuntimeError(f"expected one benchmark record, got {found}: {result.stdout} {result.stderr}")
    data = json.loads(found[0])
    if (data["partitions"] != count or data["version"] != version
        or data["reuse"] != (kind == "candidate") or len(data["samples_ns"]) != 300
        or any(type(n) is not int or n <= 0 for n in data["samples_ns"])):
        raise RuntimeError(f"invalid samples: {data}")
    data.update({"kind": kind, "pair": pair})
    print("RAW", json.dumps(data, separators=(",", ":")), flush=True)
    values = sorted(data["samples_ns"])
    return {"median_ns": statistics.median(values), "p95_ns": values[284]}

for version in [0, 3]:
    for count in [1, 128, 8192]:
        order = [("base", "candidate")] * 3 + [("candidate", "base")] * 3
        rng.shuffle(order)
        pairs = []
        for i, sequence in enumerate(order):
            sample = {}
            for kind in sequence:
                exe = base if kind == "base" else candidate
                sample[kind] = run(kind, exe, version, count, i)
            sample["order"] = sequence
            sample["median_ratio"] = sample["candidate"]["median_ns"] / sample["base"]["median_ns"]
            pairs.append(sample)
        print("SUMMARY", json.dumps({"version": version, "partitions": count, "pairs": pairs,
               "median_of_pair_ratios": statistics.median(p["median_ratio"] for p in pairs)},
              separators=(",", ":")), flush=True)

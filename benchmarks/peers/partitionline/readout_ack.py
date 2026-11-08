#!/usr/bin/env python3
"""Read only the retained files from the explicitly pinned diagnostic run."""
import csv
import hashlib
import json
import math
from pathlib import Path
import re
import statistics
import subprocess
import sys

ROOT = Path(__file__).resolve().parents[3]
RUN = Path(sys.argv[1])
RUN_SHA = "c8a6c6fb84a27f224b37a2fce9cc8f4e443c9333"
BASE_SHA = "264322ff430b0594f4c3864e53dcd832de2a930b"
PIN = "9a94e11452cdeb0a844db44ee5dd01ccbe17d3ab"

def require(value, message):
    if not value:
        raise ValueError(message)

def digest(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()

def hash_entries(path):
    result = {}
    for line in path.read_text(encoding="utf-8").splitlines():
        parts = line.split(maxsplit=1)
        require(len(parts) == 2 and re.fullmatch("[0-9a-f]{64}", parts[0]) is not None,
                f"invalid hash line in {path.name}")
        require(parts[1] not in result, f"duplicate hash path in {path.name}")
        result[parts[1]] = parts[0]
    return result

commits = (RUN / "commits.txt").read_text(encoding="utf-8").splitlines()
require(commits == [RUN_SHA, BASE_SHA], "artifact source commits differ")
source = hash_entries(RUN / "source-sha256.txt")
for relative, expected in source.items():
    local = (ROOT / relative).resolve()
    require(local.is_relative_to(ROOT) and local.is_file() and digest(local) == expected,
            f"checked-out source hash differs: {relative}")
native_manifest = json.loads((RUN / "native-build-manifest.json").read_text())
require(native_manifest["commit"] == PIN, "librdkafka source commit differs")
binary_hashes = {}
for name in ["rust-binary-sha256.txt", "c-binary-sha256.txt", "java-audit-sha256.txt"]:
    for path, sha in hash_entries(RUN / name).items():
        label = Path(path).name
        require(label not in binary_hashes, "duplicate binary hash label")
        binary_hashes[label] = sha
require("bench_ack_closed_loop" in binary_hashes and "c-closed-loop" in binary_hashes
        and "librdkafka.so.1" in binary_hashes and "kafka-clients-3.9.1.jar" in binary_hashes,
        "missing recorded binary hashes")
require(binary_hashes["librdkafka.so.1"] == native_manifest["library_sha256"],
        "recorded library hashes differ")
order = (RUN / "order.txt").read_text(encoding="utf-8").splitlines()
require(sorted(order) == ["c rust", "rust c"], "unbalanced run order")
print("PROVENANCE", json.dumps({
    "diagnostic_run": 42444229431, "artifact_id": 13602847198,
    "source_commit": RUN_SHA, "base_commit": BASE_SHA,
    "source_sha256": source, "binary_sha256_recorded_at_run": binary_hashes,
    "librdkafka_commit": native_manifest["commit"],
    "librdkafka_tree": native_manifest["library_tree"],
    "broker_image": (RUN / "kafka-image.txt").read_text().strip(),
    "affinity": (RUN / "affinity.txt").read_text().strip(),
    "order": order,
}, sort_keys=True))
for pair, sequence in enumerate(order, start=1):
    for arm in sequence.split():
        directory = RUN / f"pair-{pair}-{arm}"
        cmd = [sys.executable, str(ROOT / "benchmarks/peers/partitionline/verify_ack_pair.py"),
               "--kind", arm, "--trace", str(directory / "ack.csv"),
               "--fences", str(directory / "fences.tsv"),
               "--receipts", str(directory / "java-receipts.jsonl"),
               "--warmup", "16", "--count", "64", "--partitions", "1", "--payload-bytes", "100"]
        if arm == "c":
            cmd += ["--c-raw", str(directory / "c-raw.json"),
                    "--c-samples", str(directory / "c-samples.csv")]
        result = subprocess.run(cmd, text=True, capture_output=True, check=True)
        verdict = json.loads(result.stdout)
        require(verdict["independently_read"] == 80 and not verdict["matrix_eligible"],
                "independent receipt verdict differs")
        if arm == "rust":
            with (directory / "ack.csv").open(newline="", encoding="utf-8") as stream:
                rows = list(csv.reader(stream))
            samples = [int(row[5]) / 1_000 for row in rows if row[0] == "record" and row[1] == "measure"]
            ends = [row for row in rows if row[:2] == ["phase_end", "measure"]]
            require(len(ends) == 1, "missing Rust phase time")
            elapsed_ms = int(ends[0][3]) / 1_000_000
            setting = next(row for row in rows if row[0] == "settings")
            config = {"configured_acks": setting[1], "idempotence": setting[2],
                      "linger_ms": setting[3], "batch_bytes": setting[4], "batch_records": setting[5],
                      "max_in_flight": setting[6], "requested_connections": setting[7],
                      "visible_buffer_bytes": setting[8], "delivery_timeout_ms": setting[9],
                      "max_request_size": setting[10], "single_record_upper_bound": setting[11]}
        else:
            raw = json.loads((directory / "c-raw.json").read_text())
            with (directory / "c-samples.csv").open(newline="", encoding="utf-8") as stream:
                samples = [float(row["latency_us"]) for row in csv.DictReader(stream)]
            elapsed_ms = raw["timed"]["elapsed_s"] * 1_000
            config = {"effective": raw["effective_config"], "cluster_id": raw["cluster_id"],
                      "queue_full_retries": raw["timed"]["queue_full_retries"]}
        require(len(samples) == 64 and all(math.isfinite(v) and v >= 0 for v in samples),
                "invalid timed samples")
        sorted_samples = sorted(samples)
        print("ARM", json.dumps({"pair": pair, "arm": arm, "warmup": 16,
              "measured": 64, "independent_receipts": 80,
              "phase_ms": elapsed_ms, "sample_unit": "microseconds",
              "p50": statistics.median(sorted_samples), "p95": sorted_samples[math.ceil(.95 * len(samples)) - 1],
              "min": sorted_samples[0], "max": sorted_samples[-1], "config": config},
              sort_keys=True))
print("LIMITS", json.dumps({
    "disposition": "exploratory_noncomparable", "matrix_eligible": False,
    "broker_shape": "one Kafka 3.9.1 broker; one fresh partition; RF=1; minISR=1",
    "workload": "nontransactional, idempotent, acks=all, plaintext, 100-byte seeded values, 16-byte IDs",
    "readback": "independent Kafka 3.9.1 Java consumer; read_uncommitted; both phases",
    "driver_limitations": "closed loop, one outstanding; unequal backend queue semantics and driver overhead; actual connection counts unmeasured; C phase includes an empty flush after callbacks; Rust phase includes receipt writing",
    "environment": "shared x86 host; two short pairs; no open-loop latency, sample floor or throughput qualification",
    "binary_hashes": "recorded by original job; binary files were not retained in artifact",
}, sort_keys=True))

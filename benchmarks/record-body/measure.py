#!/usr/bin/env python3
"""Paired process-level measurements; lower ns per whole encoded batch is better."""
import json
from pathlib import Path
import random
import statistics
import subprocess
import sys

names = ["one_empty", "one_100", "f01_8_plain", "f02_8_headers", "f03_500_plain",
         "f04_32_headers", "many_null_key", "many_empty_key", "header_heavy",
         "boundaries", "large_values", "gzip_128", "snappy_128", "lz4_128",
         "async_f01_8", "async_f03_500", "async_headers", "async_large",
         "varint_u32_1", "varint_u32_2", "varint_u32_5",
         "varint_u64_1", "varint_u64_2", "varint_u64_10"]
base, candidate, cpu, output = sys.argv[1:]
def call(binary, *arguments):
    command = ["taskset", "-c", cpu, binary, *map(str, arguments)]
    completed = subprocess.run(command, check=True, text=True, capture_output=True, timeout=180)
    return completed.stdout.strip()

iterations = {}
for name in names:
    iterations[name] = int(call(base, "calibrate", name))
    if not 1 <= iterations[name] <= 16_777_216:
        raise ValueError(f"invalid calibration for {name}")
print(json.dumps({"calibrated_iterations": iterations}, sort_keys=True), flush=True)
trials = []
for trial in range(1, 10):
    order = names.copy()
    random.Random(0x47C0 + trial).shuffle(order)
    for name in order:
        pair = {}
        for arm, binary in ([("base", base), ("candidate", candidate)] if trial % 2
                            else [("candidate", candidate), ("base", base)]):
            raw = call(binary, "bench", name, iterations[name])
            fields = raw.split(",")
            if len(fields) != 3 or fields[0] != name or int(fields[1]) != iterations[name]:
                raise ValueError(f"invalid output: {raw!r}")
            ns = int(fields[2])
            if ns <= 0:
                raise ValueError(f"invalid time: {raw!r}")
            pair[arm] = ns
        row = {"trial": trial, "shape": name, "iterations": iterations[name],
               "base_elapsed_ns": pair["base"], "candidate_elapsed_ns": pair["candidate"],
               "base_ns_per_batch": pair["base"] / iterations[name],
               "candidate_ns_per_batch": pair["candidate"] / iterations[name],
               "candidate_over_base": pair["candidate"] / pair["base"],
               "order": "base-candidate" if trial % 2 else "candidate-base"}
        trials.append(row)
        print(json.dumps(row, sort_keys=True), flush=True)
summaries = []
for name in names:
    rows = [r for r in trials if r["shape"] == name]
    ratios = [r["candidate_over_base"] for r in rows]
    med = statistics.median(ratios)
    summaries.append({"shape": name, "pairs": len(rows),
                      "base_median_ns_per_batch": statistics.median(r["base_ns_per_batch"] for r in rows),
                      "candidate_median_ns_per_batch": statistics.median(r["candidate_ns_per_batch"] for r in rows),
                      "median_paired_ratio_candidate_over_base": med,
                      "paired_ratio_min": min(ratios), "paired_ratio_max": max(ratios),
                      "paired_ratio_mad": statistics.median(abs(x - med) for x in ratios)})
result = {"qualification": "local paired x86 encode diagnostics, not end-to-end ranking",
          "trials": trials, "summary": summaries}
Path(output).write_text(json.dumps(result, indent=2, sort_keys=True) + "\n", encoding="utf-8")
print(json.dumps({"summary": summaries}, indent=2, sort_keys=True))

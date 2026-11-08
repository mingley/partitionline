#!/usr/bin/env python3
"""Balanced three-way paired x86 timings; lower nanoseconds per batch is better."""
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
base, current, outlined, cpu, output = sys.argv[1:]
binaries = {"base": base, "current": current, "outlined": outlined}

def call(binary, *arguments):
    command = ["taskset", "-c", cpu, binary, *map(str, arguments)]
    try:
        completed = subprocess.run(command, check=True, text=True, capture_output=True, timeout=180)
    except subprocess.CalledProcessError as error:
        print(json.dumps({"command": command, "status": error.returncode,
                          "stdout": error.stdout, "stderr": error.stderr}), file=sys.stderr)
        raise
    return completed.stdout.strip()

iterations = {}
for name in names:
    iterations[name] = int(call(base, "calibrate", name))
    if not 1 <= iterations[name] <= 67_108_864:
        raise ValueError(f"invalid calibration for {name}")
print(json.dumps({"calibrated_iterations": iterations}, sort_keys=True), flush=True)
orders = [("base", "current", "outlined"), ("current", "outlined", "base"),
          ("outlined", "base", "current"), ("outlined", "current", "base"),
          ("current", "base", "outlined"), ("base", "outlined", "current")]
trials = []
for trial in range(1, 13):
    shapes = names.copy()
    random.Random(0x47C0 + trial).shuffle(shapes)
    order = orders[(trial - 1) % len(orders)]
    for name in shapes:
        times = {}
        for arm in order:
            raw = call(binaries[arm], "bench", name, iterations[name])
            fields = raw.split(",")
            if len(fields) != 3 or fields[0] != name or int(fields[1]) != iterations[name]:
                raise ValueError(f"invalid output: {raw!r}")
            ns = int(fields[2])
            if ns <= 0:
                raise ValueError(f"invalid time: {raw!r}")
            times[arm] = ns
        row = {"trial": trial, "shape": name, "iterations": iterations[name], "order": order,
               "base_elapsed_ns": times["base"], "current_elapsed_ns": times["current"],
               "outlined_elapsed_ns": times["outlined"],
               "current_over_base": times["current"] / times["base"],
               "outlined_over_base": times["outlined"] / times["base"],
               "outlined_over_current": times["outlined"] / times["current"]}
        trials.append(row)
        print(json.dumps(row, sort_keys=True), flush=True)
summaries = []
for name in names:
    rows = [r for r in trials if r["shape"] == name]
    summary = {"shape": name, "trials": len(rows)}
    for arm in ("base", "current", "outlined"):
        summary[arm + "_median_ns_per_batch"] = statistics.median(
            r[arm + "_elapsed_ns"] / r["iterations"] for r in rows)
    for metric in ("current_over_base", "outlined_over_base", "outlined_over_current"):
        values = [r[metric] for r in rows]
        med = statistics.median(values)
        summary[metric] = {"median": med, "min": min(values), "max": max(values),
                           "mad": statistics.median(abs(x - med) for x in values)}
    summaries.append(summary)
result = {"qualification": "local matched x86 encoding diagnostics, not end-to-end ranking",
          "trials": trials, "summary": summaries}
Path(output).write_text(json.dumps(result, indent=2, sort_keys=True) + "\n", encoding="utf-8")
print(json.dumps({"summary": summaries}, indent=2, sort_keys=True))

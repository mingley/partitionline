#!/usr/bin/env python3
"""Paired, in-process comparison of the Registry JSON object index."""
import csv
import json
import os
from pathlib import Path
import re
import shutil
import statistics
import subprocess
import sys
import tempfile

ROOT = Path(__file__).resolve().parents[2]
SCHEMA = ROOT / "partitionline-schema"
QUALIFICATION = ROOT / "qualification" / "schema-json-avl"
WORK = Path(tempfile.gettempdir()) / ("schema-json-avl-" + str(os.getpid()))
MARKER = "#[cfg(test)]\nmod tests {\n"
BEGIN = "    // BEGIN_JSON_INDEX_CONTROLS\n"
END = "    // END_JSON_INDEX_CONTROLS\n\n"
BENCH = (QUALIFICATION / "bench.rs.txt").read_text()
PATTERN = re.compile(r"PL_TREE_SAMPLE shape=(\S+) iterations=(\d+) ns=(\d+) body_bytes=(\d+)")
SHAPES = {
    "typical", "versioned", "k5", "k16", "k32", "k33", "k34", "k64", "k65",
    "k96", "k128", "k129", "k256", "k512", "k4097", "k8193",
    "k8193_desc", "k8193_shuffled", "k512_prefix256",
}

def checked(command, **kwargs):
    proc = subprocess.run(command, text=True, capture_output=True, **kwargs)
    if proc.returncode:
        print(proc.stdout[-12000:], file=sys.stderr)
        print(proc.stderr[-12000:], file=sys.stderr)
        raise RuntimeError("command failed: " + repr(command))
    return proc.stdout

def source_without_controls(text):
    if text.count(BEGIN) != 1 or text.count(END) != 1:
        raise ValueError("candidate controls markers")
    left, rest = text.split(BEGIN, 1)
    _, right = rest.split(END, 1)
    return left + right

def compile_arm(name, source):
    root = WORK / name
    shutil.copytree(SCHEMA, root)
    path = root / "src" / "registry.rs"
    if source.count(MARKER) != 1:
        raise ValueError("benchmark injection point")
    path.write_text(source.replace(MARKER, MARKER + BENCH, 1))
    env = os.environ.copy()
    env["CARGO_TARGET_DIR"] = str(WORK / (name + "-target"))
    args = [
        "cargo", "test", "--manifest-path", str(root / "Cargo.toml"),
        "--locked", "--release", "--no-default-features", "--features", "registry",
        "--lib", "--no-run", "--message-format=json",
    ]
    messages = checked(args, cwd=ROOT, env=env, timeout=600)
    executables = []
    for line in messages.splitlines():
        item = json.loads(line)
        if (item.get("reason") == "compiler-artifact"
                and item.get("profile", {}).get("test")
                and item.get("executable")):
            executables.append(item["executable"])
    if len(executables) != 1:
        raise ValueError("expected one test executable: " + repr(executables))
    return executables[0]

def measure(executable):
    output = checked(
        [executable, "json_tree_qualification_bench", "--ignored", "--nocapture", "--test-threads=1"],
        cwd=ROOT, timeout=180,
    )
    if "1 passed" not in output:
        raise ValueError("benchmark test did not run exactly once")
    rows = PATTERN.findall(output)
    result = {}
    for shape, iterations, ns, body_bytes in rows:
        if shape in result:
            raise ValueError("duplicate shape")
        result[shape] = (int(iterations), int(ns), int(body_bytes))
    if set(result) != SHAPES:
        raise ValueError("incomplete shapes: " + repr(set(result) ^ SHAPES))
    return result

def main():
    if WORK.exists():
        raise ValueError("comparison directory already exists")
    allowed = os.sched_getaffinity(0)
    cpu = min(allowed)
    os.sched_setaffinity(0, {cpu})
    print("cpu=" + str(cpu), flush=True)
    WORK.mkdir(parents=True)
    baseline = (QUALIFICATION / "baseline.rs.txt").read_text()
    candidate = source_without_controls((SCHEMA / "src" / "registry.rs").read_text())
    if baseline[baseline.index(MARKER):] != candidate[candidate.index(MARKER):]:
        raise ValueError("benchmark test scaffolds differ")
    arms = {"baseline": compile_arm("baseline", baseline),
            "candidate": compile_arm("candidate", candidate)}
    for name, path in arms.items():
        digest = checked(["sha256sum", path], cwd=ROOT).strip()
        print("binary=" + name + " " + digest, flush=True)
        measure(path)
    values = {}
    writer = csv.writer(sys.stdout, lineterminator="\n")
    writer.writerow(["pair", "order", "arm", "shape", "iterations", "total_ns", "body_bytes"])
    for pair in range(12):
        order = ["baseline", "candidate"] if pair % 2 == 0 else ["candidate", "baseline"]
        for position, arm in enumerate(order):
            rows = measure(arms[arm])
            for shape in sorted(rows):
                iterations, ns, body_bytes = rows[shape]
                values[pair, arm, shape] = ns / iterations
                writer.writerow([pair, position, arm, shape, iterations, ns, body_bytes])
        sys.stdout.flush()
    for shape in sorted(SHAPES):
        ratios = [100 * (values[p, "candidate", shape] / values[p, "baseline", shape] - 1)
                  for p in range(12)]
        print("SUMMARY shape={} paired_median_pct={:.4f} min_pct={:.4f} max_pct={:.4f}".format(
            shape, statistics.median(ratios), min(ratios), max(ratios)), flush=True)

if __name__ == "__main__":
    main()

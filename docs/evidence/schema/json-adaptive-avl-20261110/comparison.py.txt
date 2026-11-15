#!/usr/bin/env python3
"""Paired, in-process comparison of the Registry JSON object index."""
import csv
import json
import hashlib
import os
from pathlib import Path
import re
import shutil
import statistics
import subprocess
import sys
import tempfile
import time

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
    path.write_text(source)
    production_sha = checked(["git", "hash-object", str(path)], cwd=ROOT).strip()
    path.write_text(source.replace(MARKER, MARKER + BENCH, 1))
    benchmark_sha = checked(["git", "hash-object", str(path)], cwd=ROOT).strip()
    print("source={} stripped={} instrumented={}".format(name, production_sha, benchmark_sha), flush=True)
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

def differential(executable):
    output = checked(
        [executable, "json_index_differential_corpus", "--ignored", "--nocapture", "--test-threads=1"],
        cwd=ROOT, timeout=120,
    )
    if "1 passed" not in output:
        raise ValueError("differential test did not run exactly once")
    records = []
    for line in output.splitlines():
        marker = line.find("PL_JSON_DIFF case=")
        if marker >= 0:
            records.append(line[marker + len("PL_JSON_DIFF "):])
    if len(records) != 104 or len(set(line.split(" result=", 1)[0] for line in records)) != 104:
        raise ValueError("differential corpus is incomplete or has duplicate labels")
    return records

def observe_resource(executable, arm, shape, trial, payload, keys):
    env = os.environ.copy()
    env["PL_JSON_PAYLOAD"] = str(payload)
    env["PL_JSON_KEYS"] = str(keys)
    path = WORK / ("resource-" + arm + "-" + shape + "-" + str(trial) + ".log")
    with path.open("wb") as output:
        child = subprocess.Popen(
            [executable, "json_index_resource_observation", "--ignored", "--nocapture", "--test-threads=1"],
            cwd=ROOT, env=env, stdout=output, stderr=subprocess.STDOUT,
        )
        deadline = time.monotonic() + 30
        try:
            while True:
                pid, status, usage = os.wait4(child.pid, os.WNOHANG)
                if pid:
                    child.returncode = os.waitstatus_to_exitcode(status)
                    break
                if time.monotonic() >= deadline:
                    child.kill()
                    _, status, _usage = os.wait4(child.pid, 0)
                    child.returncode = os.waitstatus_to_exitcode(status)
                    raise TimeoutError("resource observation timed out: " + arm + " " + shape)
                time.sleep(0.01)
        except BaseException:
            if child.returncode is None:
                child.kill()
                _, status, _usage = os.wait4(child.pid, 0)
                child.returncode = os.waitstatus_to_exitcode(status)
            raise
    output = path.read_text()
    found = re.findall(r"PL_JSON_RESOURCE body_bytes=(\d+) extra_keys=(\d+) payload=(\d+) elapsed_ns=(\d+)", output)
    if child.returncode != 0 or len(found) != 1 or "1 passed" not in output:
        print(output[-8192:], file=sys.stderr)
        raise RuntimeError("invalid resource observation")
    body_bytes, observed_keys, observed_payload, elapsed_ns = map(int, found[0])
    if observed_keys != keys or observed_payload != payload:
        raise ValueError("resource workload mismatch")
    if payload and body_bytes != payload + 13:
        raise ValueError("resource payload body mismatch")
    print("RESOURCE arm={} shape={} trial={} body_bytes={} extra_keys={} parse_ns={} peak_rss_kib={}".format(
        arm, shape, trial, body_bytes, observed_keys, elapsed_ns, usage.ru_maxrss), flush=True)

def main():
    if WORK.exists():
        raise ValueError("comparison directory already exists")
    allowed = os.sched_getaffinity(0)
    cpu = min(allowed)
    WORK.mkdir(parents=True)
    baseline = (QUALIFICATION / "baseline.rs.txt").read_text()
    adaptive64 = source_without_controls((SCHEMA / "src" / "registry.rs").read_text())
    promotion = """            entries.push((key, value));
            index.insert_next(&entries)?;
            // Catch up gradually, then keep at most 32 keys in the linear tail.
            if entries.len().saturating_sub(index.nodes.len()) > 32 {
                index.insert_next(&entries)?;
            }
            self.skip_ws();"""
    single = """            entries.push((key, value));
            index.insert_next(&entries)?;
            self.skip_ws();"""
    if adaptive64.count(promotion) != 1:
        raise ValueError("adaptive migration marker")
    split64 = adaptive64.replace(promotion, single, 1)
    threshold = "const JSON_LINEAR_TAIL: usize = 64;"
    if split64.count(threshold) != 1:
        raise ValueError("candidate threshold marker")
    split32 = split64.replace(threshold, "const JSON_LINEAR_TAIL: usize = 32;", 1)
    for candidate in (split32, split64, adaptive64):
        if baseline[baseline.index(MARKER):] != candidate[candidate.index(MARKER):]:
            raise ValueError("benchmark test scaffolds differ")
    arms = {"baseline": compile_arm("baseline", baseline),
            "split32": compile_arm("split32", split32),
            "split64": compile_arm("split64", split64),
            "adaptive64": compile_arm("adaptive64", adaptive64)}
    os.sched_setaffinity(0, {cpu})
    print("cpu=" + str(cpu), flush=True)
    transcripts = {name: differential(path) for name, path in arms.items()}
    expected = transcripts["baseline"]
    for name, transcript in transcripts.items():
        if transcript != expected:
            for index, (left, right) in enumerate(zip(expected, transcript)):
                if left != right:
                    print("DIFF_MISMATCH arm={} index={}".format(name, index), flush=True)
                    print("baseline=" + left[:4096], flush=True)
                    print("candidate=" + right[:4096], flush=True)
                    break
            raise ValueError("baseline and candidate JSON semantics differ: " + name)
    for record in expected:
        prefix, result = record.split(" result=", 1)
        if result.startswith("Err("):
            print("DIFF " + prefix + " result=" + result, flush=True)
        else:
            print("DIFF {} result=Ok bytes={} sha256={}".format(
                prefix, len(result.encode()), hashlib.sha256(result.encode()).hexdigest()), flush=True)
    print("differential_cases=" + str(len(expected)), flush=True)
    for name, path in arms.items():
        digest = checked(["sha256sum", path], cwd=ROOT).strip()
        print("binary=" + name + " " + digest, flush=True)
        measure(path)
    values = {}
    writer = csv.writer(sys.stdout, lineterminator="\n")
    writer.writerow(["pair", "order", "arm", "shape", "iterations", "total_ns", "body_bytes"])
    base_order = ("baseline", "split32", "split64", "adaptive64")
    orders = []
    for offset in range(len(base_order)):
        forward = base_order[offset:] + base_order[:offset]
        orders.extend((forward, tuple(reversed(forward))))
    for pair in range(16):
        order = orders[pair % len(orders)]
        for position, arm in enumerate(order):
            rows = measure(arms[arm])
            for shape in sorted(rows):
                iterations, ns, body_bytes = rows[shape]
                values[pair, arm, shape] = ns / iterations
                writer.writerow([pair, position, arm, shape, iterations, ns, body_bytes])
        sys.stdout.flush()
    for shape in sorted(SHAPES):
        for arm, reference in (("split32", "baseline"), ("split64", "baseline"),
                               ("adaptive64", "baseline"), ("adaptive64", "split64"),
                               ("adaptive64", "split32")):
            ratios = [100 * (values[p, arm, shape] / values[p, reference, shape] - 1)
                      for p in range(16)]
            print("SUMMARY arm={} reference={} shape={} paired_median_pct={:.4f} min_pct={:.4f} max_pct={:.4f}".format(
                arm, reference, shape, statistics.median(ratios), min(ratios), max(ratios)), flush=True)

    for shape, payload, keys, trials in [
        ("many_keys", 0, 16384, 5),
        ("default_limit", 1024 * 1024 - 13, 0, 3),
        ("max_limit", 64 * 1024 * 1024 - 13, 0, 3),
    ]:
        for trial in range(trials):
            order = orders[trial % len(orders)]
            for arm in order:
                observe_resource(arms[arm], arm, shape, trial, payload, keys)

if __name__ == "__main__":
    main()

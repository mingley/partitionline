#!/usr/bin/env python3
"""Run pinned encode controls and compare complete Produce bodies."""
import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys

base_json, fix_json, output = sys.argv[1:]
out = Path(output)
def executable(path):
    candidates = []
    with open(path, encoding="utf-8") as source:
        for line in source:
            msg = json.loads(line)
            if (msg.get("reason") == "compiler-artifact"
                    and msg.get("target", {}).get("name") == "partitionline"
                    and msg.get("profile", {}).get("test")
                    and msg.get("executable")):
                candidates.append(msg["executable"])
    if len(candidates) != 1:
        raise RuntimeError(f"expected one test binary in {path}: {candidates}")
    return candidates[0]

binaries = {"baseline": executable(base_json), "candidate": executable(fix_json)}
binary_hashes = {}
for arm, binary in binaries.items():
    binary_hashes[arm] = hashlib.sha256(Path(binary).read_bytes()).hexdigest()
    print(f"EXACT_WIRE_BINARY arm={arm} sha256={binary_hashes[arm]}", flush=True)
if binary_hashes["baseline"] == binary_hashes["candidate"]:
    raise RuntimeError("test binaries unexpectedly match")
for arm, binary in binaries.items():
    arm_dir = out / arm
    arm_dir.mkdir(parents=True, exist_ok=True)
    env = dict(os.environ, PL_TIMESTAMP_WIRE_OUT=str(arm_dir.resolve()))
    result = subprocess.run(
        [binary, "--ignored", "--exact", "producer::tests::timestamp_exact_wire_bodies", "--nocapture"],
        env=env, text=True, capture_output=True, timeout=120,
    )
    print(result.stdout, flush=True)
    if result.returncode or "test result: ok. 1 passed" not in result.stdout:
        print(result.stderr, file=sys.stderr)
        raise RuntimeError(f"wire control failed for {arm}")
expected = {
    f"v{version}-{codec}.bin"
    for version in (3, 9, 13)
    for codec in ("none", "gzip", "snappy", "lz4", "zstd")
    if not (version == 3 and codec == "zstd")
}
for arm in binaries:
    names = {path.name for path in (out / arm).glob("*.bin")}
    if names != expected:
        raise RuntimeError(f"wrong {arm} body set: {names ^ expected}")
cases = []
for name in sorted(expected):
    base = (out / "baseline" / name).read_bytes()
    fix = (out / "candidate" / name).read_bytes()
    if not base or base != fix:
        raise RuntimeError(f"complete Produce bodies differ: {name}")
    digest = hashlib.sha256(base).hexdigest()
    cases.append({"case": name, "bytes": len(base), "sha256": digest, "byte_equal": True})
    print(f"EXACT_WIRE_EQUAL case={name} bytes={len(base)} sha256={digest}", flush=True)
report = {"binary_sha256": binary_hashes, "cases": cases}
(out / "comparison.json").write_text(json.dumps(report, sort_keys=True, indent=2) + "\n", encoding="utf-8")
print(f"EXACT_WIRE_RESULT cases={len(cases)} byte_equal=1", flush=True)

#!/usr/bin/env python3
"""Retain sanitized focused transport checks for one installed Rust toolchain."""

import argparse
import hashlib
import json
import os
from pathlib import Path
import subprocess


parser = argparse.ArgumentParser()
parser.add_argument("toolchain", choices=["stable", "1.85.0"])
args = parser.parse_args()
root = Path(__file__).resolve().parents[4]
destination = Path(__file__).resolve().parent
sources = [
    "partitionline-broker/Cargo.toml",
    "partitionline-broker/Cargo.lock",
    "partitionline-broker/src/lib.rs",
    "partitionline-broker/src/transport.rs",
    "partitionline-broker/tests/transport.rs",
]


def hashes():
    return {
        name: hashlib.sha256((root / name).read_bytes()).hexdigest()
        for name in sources
    }


environment = os.environ.copy()
environment.update(CARGO_BUILD_JOBS="1", CARGO_INCREMENTAL="0", RUSTDOCFLAGS="-D warnings")
before = hashes()
result = {
    "task": "KL11-02",
    "toolchain": args.toolchain,
    "tested_base_sha": subprocess.check_output(
        ["git", "rev-parse", "HEAD"], cwd=root, text=True
    ).strip(),
    "source_sha256": before,
    "rustc": subprocess.check_output(
        ["rustup", "run", args.toolchain, "rustc", "--version"], text=True
    ).strip(),
    "cargo": subprocess.check_output(
        ["cargo", f"+{args.toolchain}", "--version"], text=True
    ).strip(),
    "environment": {
        "CARGO_BUILD_JOBS": "1",
        "CARGO_INCREMENTAL": "0",
        "RUSTDOCFLAGS": "-D warnings",
        "cpu_affinity": "externally restricted to 0-2,4 for this retained run",
    },
    "checks": [],
}
manifest = ["--locked", "--manifest-path", "partitionline-broker/Cargo.toml"]
commands = [
    ("format", ["rustup", "run", args.toolchain, "rustfmt", "--edition", "2021", "--check", *sources[-2:]]),
    ("transport-tests", ["cargo", f"+{args.toolchain}", "test", *manifest, "--test", "transport"]),
    ("strict-clippy", ["cargo", f"+{args.toolchain}", "clippy", *manifest, "--lib", "--test", "transport", "--", "-D", "warnings"]),
    ("strict-docs", ["cargo", f"+{args.toolchain}", "doc", *manifest, "--no-deps"]),
]
failed = False
for name, command in commands:
    completed = subprocess.run(command, cwd=root, env=environment, text=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    output = completed.stdout.replace(str(root), "<repository>")
    target = environment.get("CARGO_TARGET_DIR")
    if target:
        output = output.replace(target, "<target>")
    log = f"{args.toolchain}-{name}.log"
    (destination / log).write_text(output)
    result["checks"].append({"name": name, "command": command, "exit_code": completed.returncode, "log": log})
    print(f"{args.toolchain} {name}: exit {completed.returncode}", flush=True)
    if completed.returncode:
        print(output, flush=True)
        failed = True
        break
result["sources_unchanged_during_checks"] = before == hashes()
failed |= not result["sources_unchanged_during_checks"]
result["passed"] = not failed
(destination / f"{args.toolchain}-results.json").write_text(json.dumps(result, indent=2) + "\n")
raise SystemExit(1 if failed else 0)

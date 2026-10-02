#!/usr/bin/env python3
"""Run the broker CI gates against an immutable committed source archive."""

import argparse
import hashlib
import io
import json
import os
from pathlib import Path
import re
import subprocess
import tarfile


parser = argparse.ArgumentParser()
parser.add_argument("toolchain", choices=["stable", "1.85.0"])
parser.add_argument("--source-sha", required=True)
parser.add_argument("--snapshot-dir", type=Path, required=True)
arguments = parser.parse_args()
repository = Path(__file__).resolve().parents[4]
evidence = Path(__file__).resolve().parent
snapshot = arguments.snapshot_dir.resolve()
source_sha = subprocess.check_output(["git", "rev-parse", f"{arguments.source_sha}^{{commit}}"], cwd=repository, text=True).strip()
paths = subprocess.check_output(["git", "ls-tree", "-r", "--name-only", source_sha, "--", "partitionline-broker", "clippy.toml"], cwd=repository, text=True).splitlines()
pins = {name: hashlib.sha256(subprocess.check_output(["git", "show", f"{source_sha}:{name}"], cwd=repository)).hexdigest() for name in paths}
if not snapshot.exists():
    snapshot.mkdir(parents=True)
    archive = subprocess.check_output(["git", "archive", source_sha, "--", "partitionline-broker", "clippy.toml"], cwd=repository)
    with tarfile.open(fileobj=io.BytesIO(archive)) as stream:
        stream.extractall(snapshot, filter="data")


def snapshot_matches():
    actual = {str(path.relative_to(snapshot)): hashlib.sha256(path.read_bytes()).hexdigest() for path in snapshot.rglob("*") if path.is_file()}
    return actual == pins


assert snapshot_matches(), "archive does not match committed files or contains additional files"
environment = os.environ.copy()
environment.update(CARGO_INCREMENTAL="0", CARGO_BUILD_JOBS="1", RUSTDOCFLAGS="-D warnings")
assert environment.get("CARGO_TARGET_DIR"), "set an external dedicated CARGO_TARGET_DIR"
manifest = ["--locked", "--manifest-path", "partitionline-broker/Cargo.toml"]
commands = [
    ("format", ["cargo", f"+{arguments.toolchain}", "fmt", "--manifest-path", "partitionline-broker/Cargo.toml", "--", "--check"]),
    ("default-tests", ["cargo", f"+{arguments.toolchain}", "test", *manifest, "--all-targets"]),
    ("all-features-tests", ["cargo", f"+{arguments.toolchain}", "test", *manifest, "--all-targets", "--all-features"]),
    ("strict-clippy", ["cargo", f"+{arguments.toolchain}", "clippy", *manifest, "--all-targets", "--all-features", "--", "-D", "warnings"]),
    ("strict-docs", ["cargo", f"+{arguments.toolchain}", "doc", *manifest, "--all-features", "--no-deps"]),
    ("default-doctests", ["cargo", f"+{arguments.toolchain}", "test", *manifest, "--doc"]),
    ("all-features-doctests", ["cargo", f"+{arguments.toolchain}", "test", *manifest, "--doc", "--all-features"]),
]


def clean_output(output):
    for path, replacement in [(str(snapshot), "<snapshot>"), (str(repository), "<repository>"), (environment["CARGO_TARGET_DIR"], "<target>")]:
        output = output.replace(path, replacement)
    return output


result = {
    "task": "KL11-35",
    "source_sha": source_sha,
    "validation_context": "immutable git archive; no unrelated uncommitted sibling files",
    "toolchain": arguments.toolchain,
    "rustc": subprocess.check_output(["rustup", "run", arguments.toolchain, "rustc", "--version"], text=True).strip(),
    "cargo": subprocess.check_output(["cargo", f"+{arguments.toolchain}", "--version"], text=True).strip(),
    "openssl": subprocess.check_output(["openssl", "version"], text=True).strip(),
    "source_sha256": pins,
    "environment": {"CARGO_INCREMENTAL": "0", "CARGO_BUILD_JOBS": "1", "RUSTDOCFLAGS": "-D warnings", "cpu_affinity": sorted(os.sched_getaffinity(0))},
    "checks": [],
}
clean_command = ["cargo", f"+{arguments.toolchain}", "clean", "--manifest-path", "partitionline-broker/Cargo.toml", "-p", "partitionline-broker"]
clean = subprocess.run(clean_command, cwd=snapshot, env=environment, text=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
(evidence / f"{arguments.toolchain}-package-clean.log").write_text(clean_output(clean.stdout))
result["preparation"] = {"command": clean_command, "exit_code": clean.returncode, "log": f"{arguments.toolchain}-package-clean.log"}
failed = clean.returncode != 0
if not failed:
    for name, command in commands:
        completed = subprocess.run(command, cwd=snapshot, env=environment, text=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
        output = clean_output(completed.stdout)
        log = f"{arguments.toolchain}-{name}.log"
        (evidence / log).write_text(output)
        check = {"name": name, "command": command, "exit_code": completed.returncode, "log": log}
        if "tests" in name:
            check["passed_test_executions"] = sum(int(count) for count in re.findall(r"test result: ok\. (\d+) passed", output))
        result["checks"].append(check)
        print(f"{arguments.toolchain} {name}: exit {completed.returncode}", flush=True)
        if completed.returncode:
            print(output, flush=True)
            failed = True
            break
result["snapshot_unchanged_during_checks"] = snapshot_matches()
failed |= not result["snapshot_unchanged_during_checks"]
result["passed"] = not failed
(evidence / f"{arguments.toolchain}-results.json").write_text(json.dumps(result, indent=2) + "\n")
raise SystemExit(1 if failed else 0)

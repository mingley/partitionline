#!/usr/bin/env python3
"""Build the standalone public Rust peer from a caller-pinned source archive."""

import argparse
import hashlib
import json
import os
from pathlib import Path
import subprocess


def digest(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source-root", type=Path, required=True)
    parser.add_argument("--target", type=Path, required=True)
    parser.add_argument("--toolchain", choices=("stable", "1.85.0"), required=True)
    parser.add_argument("--evidence", type=Path, required=True)
    parser.add_argument("--binary-out", type=Path, required=True)
    parser.add_argument("--all-features", action="store_true")
    parser.add_argument("--generate-lock", action="store_true")
    parser.add_argument("--source-sha", required=True)
    args = parser.parse_args()
    if len(args.source_sha) != 40 or any(c not in "0123456789abcdef" for c in args.source_sha):
        parser.error("Full exact source SHA required")
    peer = args.source_root / "docs/evidence/broker/KL11-75/oracle/peers/rust-adopter"
    args.evidence.mkdir(parents=True, exist_ok=False)
    capture = args.evidence / "source"
    (capture / "src").mkdir(parents=True)
    before = {}
    for name in ("Cargo.toml", "Cargo.lock", "src/main.rs"):
        path = peer / name
        if path.exists():
            (capture / name).write_bytes(path.read_bytes())
            before[name] = digest(path)
    env = dict(os.environ)
    env.update(
        CARGO_HOME="/workspace/work/cargo",
        RUSTUP_HOME="/workspace/work/rustup",
        CARGO_TARGET_DIR=str(args.target),
        CARGO_BUILD_JOBS="1",
        CARGO_INCREMENTAL="0",
        CARGO_PROFILE_DEV_DEBUG="0",
        CARGO_PROFILE_TEST_DEBUG="0",
    )
    env["PATH"] = "/workspace/work/cargo/bin:" + env["PATH"]
    base = ["taskset", "-c", "2,4", "cargo", "+" + args.toolchain]
    commands = []

    def run(label, command):
        log = args.evidence / (label + ".log")
        with log.open("wb") as output:
            completed = subprocess.run(
                command, cwd=peer, env=env, stdout=output, stderr=subprocess.STDOUT,
                check=False,
            )
        item = {"label": label, "command": command, "exit_code": completed.returncode,
                "log": str(log), "log_sha256": digest(log)}
        commands.append(item)
        return completed.returncode == 0

    passed = run("rustc", ["taskset", "-c", "2,4", "rustc", "+" + args.toolchain, "-Vv"])
    if passed and args.generate_lock:
        passed = run("generate-lock", base + ["generate-lockfile", "--offline"])
    features = ["--all-features"] if args.all_features else []
    if passed:
        passed = run("build", base + ["build", "--locked", "--offline"] + features)
    if passed:
        passed = run("clippy", base + ["clippy", "--locked", "--offline", "--all-targets"]
                     + features + ["--", "-D", "warnings"])
    if passed:
        passed = run("fmt", base + ["fmt", "--all", "--", "--check"])
    after = {name: digest(peer / name) for name in ("Cargo.toml", "Cargo.lock", "src/main.rs")
             if (peer / name).exists()}
    binary = None
    if passed:
        produced = args.target / "debug/partitionline-compaction-peer"
        args.binary_out.parent.mkdir(parents=True, exist_ok=True)
        args.binary_out.write_bytes(produced.read_bytes())
        args.binary_out.chmod(0o755)
        binary = {"path": str(args.binary_out), "sha256": digest(args.binary_out),
                  "bytes": args.binary_out.stat().st_size}
    if (peer / "Cargo.lock").exists():
        (capture / "Cargo.lock").write_bytes((peer / "Cargo.lock").read_bytes())
    report = {"schema_version": 1, "passed": passed, "source_root": str(args.source_root), "source_sha": args.source_sha,
              "toolchain": args.toolchain, "all_features": args.all_features,
              "target": str(args.target), "before": before, "after": after,
              "commands": commands, "binary": binary,
              "scope": "Preparation only; actual broker interoperability requires fresh live receipts."}
    (args.evidence / "validation.json").write_text(json.dumps(report, indent=2) + "\n")
    print(json.dumps({"passed": passed, "evidence": str(args.evidence), "binary": binary}), flush=True)
    raise SystemExit(0 if passed else 1)


if __name__ == "__main__":
    main()

#!/usr/bin/env python3
"""Replay the SASL foundation's Rust QA from an immutable source archive."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import subprocess
import tarfile
import io


def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo", type=Path, required=True)
    parser.add_argument("--source-sha", required=True)
    parser.add_argument("--work", type=Path, required=True)
    parser.add_argument("--target", type=Path, required=True)
    parser.add_argument("--cpus", default="0,1")
    parser.add_argument("--toolchains", nargs="+", default=["stable", "1.85.0"])
    args = parser.parse_args()
    args.work.mkdir(parents=True, exist_ok=False)
    source = args.work / "source"
    source.mkdir()
    inputs = ["partitionline-broker", "clippy.toml",
              "tests/conformance/broker/implemented-api-versions.json",
              "docs/evidence/broker/KL11-59/topic-name-cases.tsv",
              "docs/evidence/broker/KL11-60",
              "docs/evidence/broker/KL11-57/upstream-java-oracle.json"]
    archive = subprocess.check_output(["git", "archive", args.source_sha, *inputs], cwd=args.repo)
    with tarfile.open(fileobj=io.BytesIO(archive)) as handle:
        handle.extractall(source, filter="data")
    env = dict(os.environ, CARGO_INCREMENTAL="0", CARGO_BUILD_JOBS="1",
               CARGO_PROFILE_DEV_DEBUG="0", CARGO_PROFILE_TEST_DEBUG="0")
    manifest = source / "partitionline-broker/Cargo.toml"
    results = {"source_sha": args.source_sha, "archive_inputs": inputs,
               "archive_sha256": hashlib.sha256(archive).hexdigest(),
               "cpu_affinity": args.cpus, "profile_debug": 0, "commands": [],
               "files": {str(path.relative_to(source)): sha(path)
                         for path in source.rglob("*") if path.is_file()}}
    for toolchain in args.toolchains:
        prefix = ["taskset", "-c", args.cpus, "cargo", f"+{toolchain}"]
        common = ["--offline", "--locked", "--manifest-path", str(manifest)]
        targets = ["--all-targets", "--target-dir", str(args.target), "-j1"]
        cells = [
            ("versions", ["rustc", f"+{toolchain}", "-vV"]),
            ("fmt", prefix + ["fmt", "--manifest-path", str(manifest), "--", "--check"]),
            ("default-tests", prefix + ["test", *common, *targets]),
            ("sasl-tests", prefix + ["test", *common, "--features", "sasl", *targets]),
            ("all-tests", prefix + ["test", *common, "--all-features", *targets]),
            ("default-clippy", prefix + ["clippy", *common, *targets, "--", "-D", "warnings"]),
            ("sasl-clippy", prefix + ["clippy", *common, "--features", "sasl", *targets, "--", "-D", "warnings"]),
            ("all-clippy", prefix + ["clippy", *common, "--all-features", *targets, "--", "-D", "warnings"]),
            ("docs", prefix + ["doc", *common, "--all-features", "--no-deps", "--target-dir", str(args.target), "-j1"]),
        ]
        for name, command in cells:
            log = args.work / f"{toolchain}-{name}.log"
            cell_env = dict(env, RUSTDOCFLAGS="-D warnings") if name == "docs" else env
            with log.open("wb") as output:
                result = subprocess.run(command, cwd=source, env=cell_env, stdout=output, stderr=subprocess.STDOUT)
            text = log.read_text()
            counts = [list(map(int, match)) for match in re.findall(
                r"test result: ok\. (\d+) passed; (\d+) failed; (\d+) ignored; (\d+) measured; (\d+) filtered out", text)]
            row = {"toolchain": toolchain, "cell": name, "argv": command,
                   "exit_code": result.returncode, "log": log.name,
                   "log_sha256": sha(log), "test_result_rows": counts}
            if counts:
                row["test_totals"] = dict(zip(["passed", "failed", "ignored", "measured", "filtered"],
                                              [sum(values) for values in zip(*counts)]))
            results["commands"].append(row)
            (args.work / "results.json").write_text(json.dumps(results, indent=2) + "\n")
            print(f"{toolchain} {name}: exit={result.returncode} {row.get('test_totals', '')}", flush=True)
            if result.returncode:
                raise SystemExit(result.returncode)
    print("All frozen-source Rust checks passed.", flush=True)


if __name__ == "__main__":
    main()

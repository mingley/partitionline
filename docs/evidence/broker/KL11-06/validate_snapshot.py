#!/usr/bin/env python3
"""Run exact-source broker gates on stable/MSRV with isolated targets and proofs."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import subprocess


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source", type=Path, required=True)
    parser.add_argument("--source-commit", required=True)
    parser.add_argument("--targets", type=Path, required=True)
    parser.add_argument("--evidence", type=Path, required=True)
    args = parser.parse_args()
    if not re.fullmatch(r"[0-9a-f]{40}", args.source_commit):
        raise ValueError("exact source commit required")
    args.evidence.mkdir(parents=True, exist_ok=True)
    if hasattr(os, "sched_setaffinity"):
        os.sched_setaffinity(0, {0, 1, 2, 4})
    manifest = args.source / "partitionline-broker/Cargo.toml"
    env = dict(os.environ, CARGO_INCREMENTAL="0")
    checks = []
    toolchains = {}
    before = {str(path.relative_to(args.source)): hashlib.sha256(path.read_bytes()).hexdigest()
              for path in sorted(args.source.rglob("*")) if path.is_file() and not path.is_symlink()}
    for toolchain in ("stable", "1.85.0"):
        versions = {}
        for executable in ("rustc", "cargo"):
            version_command = [executable, "+" + toolchain, "--version", "--verbose"]
            version_result = subprocess.run(version_command, cwd=args.source, env=env,
                                            stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                                            text=True, check=False, timeout=30)
            if version_result.returncode:
                raise ValueError("cannot identify pinned compiler/toolchain")
            versions[executable] = {"command": version_command, "output": version_result.stdout,
                                    "exit_code": version_result.returncode}
        toolchains[toolchain] = versions
        target = args.targets / toolchain
        for features in ("default", "all-features"):
            label = toolchain + "-" + features
            flags = [] if features == "default" else ["--all-features"]
            cell_env = dict(env, PL_PARTITION_PROOF_DIR=str(args.evidence / (label + "-partition-proof")),
                            PARTITIONLINE_WIRE_REPORT=str(args.evidence / (label + "-protocol-handler.json")),
                            PARTITIONLINE_METADATA_REPORT=str(args.evidence / (label + "-metadata-handler.json")),
                            PARTITIONLINE_PRODUCE_REPORT=str(args.evidence / (label + "-produce-handler.json")))
            for kind in ("test", "clippy", "doc", "doctest"):
                cmd = ["cargo", "+" + toolchain, "test" if kind == "doctest" else kind, "--manifest-path", str(manifest), "--locked",
                       "--target-dir", str(target), "-j", "2"] + flags
                if kind in ("test", "clippy"):
                    cmd += ["--all-targets"]
                if kind == "clippy":
                    cmd += ["--", "-D", "warnings"]
                if kind == "doctest":
                    cmd += ["--doc"]
                if kind == "doc":
                    cmd += ["--no-deps"]
                    cell_env["RUSTDOCFLAGS"] = "-D warnings"
                result = subprocess.run(cmd, cwd=args.source, env=cell_env, stdout=subprocess.PIPE,
                                        stderr=subprocess.STDOUT, text=True, check=False, timeout=300)
                log = f"{label}-{kind}.log"
                (args.evidence / log).write_text(result.stdout, encoding="utf-8")
                counts = [int(value) for value in re.findall(r"test result: ok\. ([0-9]+) passed", result.stdout)]
                ignored = [int(value) for value in re.findall(r"test result: ok\. [0-9]+ passed; [0-9]+ failed; ([0-9]+) ignored", result.stdout)]
                checks.append({"cell": label, "check": kind, "command": cmd, "exit_code": result.returncode,
                               "verdict": "passed" if result.returncode == 0 else "failed", "test_counts": counts,
                               "passed_tests": sum(counts), "ignored_tests": sum(ignored), "log": log})
                print(f"{label} {kind}: {result.returncode}; tests {sum(counts)}", flush=True)
        cmd = ["cargo", "+" + toolchain, "fmt", "--manifest-path", str(manifest), "--check"]
        result = subprocess.run(cmd, cwd=args.source, env=env, stdout=subprocess.PIPE,
                                stderr=subprocess.STDOUT, text=True, check=False, timeout=60)
        log = toolchain + "-fmt.log"
        (args.evidence / log).write_text(result.stdout, encoding="utf-8")
        checks.append({"cell": toolchain, "check": "fmt", "command": cmd, "exit_code": result.returncode,
                       "verdict": "passed" if result.returncode == 0 else "failed", "log": log})
        print(f"{toolchain} fmt: {result.returncode}", flush=True)
    after = {str(path.relative_to(args.source)): hashlib.sha256(path.read_bytes()).hexdigest()
             for path in sorted(args.source.rglob("*")) if path.is_file() and not path.is_symlink()}
    unchanged = before == after
    report = {"schema_version": 1, "source_commit": args.source_commit, "checks": checks,
              "toolchains": toolchains,
              "source_unchanged": unchanged, "source_file_count": len(before), "source_files_sha256": after,
              "verdict": "passed" if unchanged and all(row["exit_code"] == 0 for row in checks) else "failed",
              "qualification": "not_run"}
    (args.evidence / "validation.json").write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
    print("source unchanged:", unchanged, "verdict:", report["verdict"], flush=True)
    return 0 if report["verdict"] == "passed" else 1


if __name__ == "__main__":
    raise SystemExit(main())

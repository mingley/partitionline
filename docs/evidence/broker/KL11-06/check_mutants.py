#!/usr/bin/env python3
"""Run two deliberately broken handlers against immutable-source behavioral tests.

Copies are isolated; the source snapshot is never edited. Nonzero Rust behavioral
assertions are required, rather than treating compilation failures as a detection.
"""
import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import subprocess


def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source", type=Path, required=True)
    parser.add_argument("--source-commit", required=True)
    parser.add_argument("--scratch", type=Path, required=True)
    parser.add_argument("--evidence", type=Path, required=True)
    args = parser.parse_args()
    if not re.fullmatch(r"[0-9a-f]{40}", args.source_commit):
        raise ValueError("exact source commit required")
    args.scratch.mkdir(parents=True, exist_ok=True)
    args.evidence.mkdir(parents=True, exist_ok=True)
    env = dict(os.environ, CARGO_INCREMENTAL="0")
    if hasattr(os, "sched_setaffinity"):
        os.sched_setaffinity(0, {0, 1, 2, 4})
    baseline = args.source / "partitionline-broker/src/produce.rs"
    checks = []
    for label, test in (
        ("zero-response-base-offset", "offsets_replay_and_deleted_identity_never_reuses_data"),
        ("acks-zero-emits-frame", "independent_apache_produce_goldens"),
    ):
        target = args.scratch / label
        shutil.copytree(args.source, target)
        module = target / "partitionline-broker/src/produce.rs"
        text = module.read_text(encoding="utf-8")
        if label == "zero-response-base-offset":
            old = "part.base = result.base_offset;"
            if text.count(old) != 1:
                raise ValueError("reviewed Produce offset mutation site changed")
            text = text.replace(old, "part.base = 0;")
        else:
            start = text.index("pub(crate) fn process(")
            body = text[start:]
            if body.count("return Ok(None);") != 1:
                raise ValueError("reviewed acks0 mutation site changed")
            text = text[:start] + body.replace("return Ok(None);", "return Ok(Some(Vec::new()));")
        module.write_text(text, encoding="utf-8")
        cmd = ["cargo", "+stable", "test", "--manifest-path", str(target / "partitionline-broker/Cargo.toml"),
               "--test", "produce", test, "--locked", "--target-dir", str(args.scratch / (label + "-target")),
               "-j", "2", "--", "--exact"]
        result = subprocess.run(cmd, env=env, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                                text=True, check=False, timeout=240)
        (args.evidence / (label + ".log")).write_text(result.stdout, encoding="utf-8")
        behavior = "assertion `left == right` failed" in result.stdout if label == "zero-response-base-offset" else "no_response_keep_open observedOk(Some([]))" in result.stdout
        detected = result.returncode == 101 and f"test {test} ... FAILED" in result.stdout and behavior and "could not compile" not in result.stdout
        checks.append({"mutation": label, "source_commit": args.source_commit,
                       "baseline_produce_sha256": sha(baseline), "mutant_produce_sha256": sha(module),
                       "command": cmd, "observed_exit_code": result.returncode,
                       "verdict": "failed" if result.returncode else "passed",
                       "expected_behavioral_failure_verified": detected, "failed_test": test,
                       "log": label + ".log"})
    report = {"schema_version": 1, "source_commit": args.source_commit,
              "checks": checks, "all_expected_failures_verified": all(row["expected_behavioral_failure_verified"] for row in checks),
              "qualification": "not_run"}
    (args.evidence / "mutants.json").write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(report, indent=2))
    return 0 if report["all_expected_failures_verified"] else 1


if __name__ == "__main__":
    raise SystemExit(main())

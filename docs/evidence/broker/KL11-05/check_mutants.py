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
    baseline = args.source / "partitionline-broker/src/metadata.rs"
    checks = []
    for label, test in (
        ("zero-response-uuid", "independent_apache_metadata_admin_goldens"),
        ("validate-only-writes", "persistent_create_validate_delete_and_fresh_identity"),
    ):
        target = args.scratch / label
        shutil.copytree(args.source, target)
        module = target / "partitionline-broker/src/metadata.rs"
        text = module.read_text(encoding="utf-8")
        if label == "zero-response-uuid":
            start, end = text.index("fn metadata("), text.index("fn is_internal(")
            section = text[start:end]
            if section.count("writer.put(&result.id)?;") != 1:
                raise ValueError("reviewed Metadata UUID mutation site changed")
            text = text[:start] + section.replace("writer.put(&result.id)?;", "writer.put(&ZERO)?;") + text[end:]
        else:
            old = "Ok(_) if request.validate => OK,"
            if text.count(old) != 1:
                raise ValueError("reviewed validate-only mutation site changed")
            text = text.replace(old, "Ok(_) if request.validate && topic.name.is_empty() => OK,")
        module.write_text(text, encoding="utf-8")
        cmd = ["cargo", "+stable", "test", "--manifest-path", str(target / "partitionline-broker/Cargo.toml"),
               "--test", "metadata", test, "--locked", "--target-dir", str(args.scratch / (label + "-target")),
               "-j", "2", "--", "--exact"]
        result = subprocess.run(cmd, env=env, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                                text=True, check=False, timeout=240)
        (args.evidence / (label + ".log")).write_text(result.stdout, encoding="utf-8")
        detected = result.returncode == 101 and f"test {test} ... FAILED" in result.stdout and "assertion `left == right` failed" in result.stdout
        checks.append({"mutation": label, "source_commit": args.source_commit,
                       "baseline_metadata_sha256": sha(baseline), "mutant_metadata_sha256": sha(module),
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

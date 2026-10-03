#!/usr/bin/env python3
"""Validate a source-pinned SDK build and qualified broker receipt; print safe launch argv only."""
import argparse
import hashlib
import json
from pathlib import Path


def sha(path):
    value = hashlib.sha256()
    with path.open("rb") as source:
        while chunk := source.read(1024 * 1024):
            value.update(chunk)
    return value.hexdigest()


def require(value, label):
    if not value:
        raise ValueError(label)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--release", required=True, choices=("4.1.2", "4.2.1", "4.3.1"))
    parser.add_argument("--build-validation", required=True, type=Path)
    parser.add_argument("--broker-qualification", required=True, type=Path)
    parser.add_argument("--broker-source-sha", required=True)
    parser.add_argument("--config", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    args = parser.parse_args()
    base = Path(__file__).parent
    build = json.loads(args.build_validation.read_text())
    require(build.get("passed") is True and build.get("source_pins_sha256") == sha(base / "pins.json"),
            "current source-pinned successful SDK build required")
    rows = [row for row in build["rows"] if row["release"] == args.release]
    require(len(rows) == 1, "one actual SDK build cell required")
    row = rows[0]; source = base / "OAuthMetadataPeer.java"
    require(row["exit_code"] == 0 and row["source_sha256"] == sha(source), "actual peer source build binding required")
    classes = Path(row["classes_dir"])
    for item in row["classes"]:
        relative = Path(item["file"])
        require(not relative.is_absolute() and ".." not in relative.parts, "scoped class path required")
        path = classes / relative
        require(path.is_file() and not path.is_symlink() and sha(path) == item["sha256"]
                and path.stat().st_size == item["bytes"], "actual compiled class bytes changed")
    jars = []
    for pin in row["runtime_jars"]:
        path = Path(pin["path"])
        require(path.is_file() and not path.is_symlink() and sha(path) == pin["sha256"], "pinned runtime jar changed")
        jars.append(path)
    broker = json.loads(args.broker_qualification.read_text())
    require(broker.get("passed") is True and broker.get("source_commit") == args.broker_source_sha
            and broker.get("actual_full_broker_commands") == 51 and broker.get("actual_broker_lanes") == 12
            and broker.get("actual_cache_maintenance_commands") == 10 and broker.get("actual_total_executed_commands") == 61,
            "actual source-qualified51+10 broker proof required before runtime launch planning")
    require(args.config.is_file() and not args.config.is_symlink() and args.config.stat().st_size <= 8192,
            "bounded regular rendered config required; contents are never printed")
    java = Path(build["java"]["path"])
    require(sha(java) == build["java"]["sha256"], "actual compiled Java runtime binary changed")
    argv = ["taskset", "-c", "2,4", str(java), "-Xmx128m", "-cp", ":".join(map(str, [classes,*jars])),
            "OAuthMetadataPeer", str(args.config)]
    result = {"schema_version":1, "scope":"Validated launch plan only; no process or network action.",
              "actual_live_peers":0, "release":args.release, "broker_source_sha":args.broker_source_sha,
              "peer_source_sha256":sha(source), "build_validation":{"path":str(args.build_validation),"sha256":sha(args.build_validation)},
              "broker_qualification":{"path":str(args.broker_qualification),"sha256":sha(args.broker_qualification)},
              "argv":argv, "runtime_process_group_deadline_seconds":240,
              "redaction":"No configuration content, bearer/client secret/private key, exception messages, or stack traces in this plan."}
    args.output.write_text(json.dumps(result, indent=2) + "\n")
    print(json.dumps({"planned":True,"release":args.release,"actual_live_peers":0}))


if __name__ == "__main__":
    main()

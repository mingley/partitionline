#!/usr/bin/env python3
"""Verify retained source, sidecars, counts and final peer build artifacts."""
import hashlib
import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[5]
BASE = Path(__file__).resolve().parent.parent


def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def main():
    reports = []
    commands = 0
    paths = ["oracle/final-8cf2516e-01/results.json", "oracle/final-8cf2516e-02/results.json",
             "peers/final-db002076-compilation/results.json", "peers/final-db002076-admin-policy/results.json",
             "peers/final-db002076-native-fixture/results.json", "peers/final-db002076-live/results.json"]
    for relative in paths:
        path = BASE / relative
        report = json.loads(path.read_text())
        rows = list(report.get("commands", []))
        for release in report.get("releases", []):
            rows.extend(release.get("commands", []))
        for command in rows:
            for stream in ["stdout", "stderr"]:
                assert sha(path.parent / command[stream]) == command[stream + "_sha256"]
        commands += len(rows)
        reports.append({"path": relative, "sha256": sha(path), "commands": len(rows), "verdict": report["verdict"]})
    assert commands == 43
    result_rows = 0
    for version in ["4.1.2", "4.2.1", "4.3.1"]:
        path = BASE / "peers/final-db002076-admin-policy" / (version + "-outcomes.tsv")
        rows = [line.split("\t") for line in path.read_text().splitlines()[1:]]
        assert len(rows) == 16
        assert len({row[0] for row in rows}) == 14
        result_rows += len(rows)
    native = json.loads((BASE / "peers/final-db002076-compilation/results.json").read_text())
    assert sha(Path("/workspace/work/c-peer/lib/librdkafka.so.1")) == native["native_library_sha256"]
    build = Path("/workspace/work/broker-sasl/72-takeover/final-db002076-peer")
    assert sha(build / "sasl-native-peer") == native["native_binary_sha256"]
    for version, receipt in native["native_peer_build"].items():
        assert {path.name: sha(path) for path in sorted((build / version).glob("*.class"))} == receipt["classes_sha256"]
    before = json.loads((BASE / "peers/final-db002076-source-before.json").read_text())
    after = json.loads((BASE / "peers/final-db002076-source-after.json").read_text())
    assert before == after and before["mismatches"] == []
    live = json.loads((BASE / "peers/final-db002076-live/results.json").read_text())
    assert live["server_binary_sha256"] == sha(Path("/workspace/work/target-broker-codecs/debug/partitionline-sasl-wire-evidence"))
    report = {"source_sha": live["source_sha"],
              "scope": "Final immutable source, raw TSV case/result counts, binary/library/class and log validation. Two SDK cleanup completions remain failed.",
              "verdict": live["verdict"], "verified_git_blobs_before_after": before["checked_git_blobs"],
              "verified_final_command_sidecars": commands, "final_reports": reports,
              "actual_apache_object_policy_cases": 42, "actual_apache_object_policy_result_rows": result_rows,
              "all_final_native_library_binary_and_java_class_hashes_verified": True,
              "failed_external_sdk_completion_cells": 2, "unqualified_all_peer_pass": False,
              "final_server_binary_sha256": live["server_binary_sha256"],
              "live_validation_sha256": sha(BASE / "peers/final-db002076-live/validation.json"),
              "validation_producer_sha256": sha(Path(__file__)), "production_qualification": False,
              "retained_validator_draft_failure": "peers/final-validation-initial-failure.json"}
    (BASE / "peers/final-validation.json").write_text(json.dumps(report, indent=2) + "\n")
    print(json.dumps({"verified_commands": commands, "admin_request_cases": 42, "admin_result_rows": result_rows,
                      "failed_sdk_completion_cells": 2, "all_sidecars_verified": True}))


if __name__ == "__main__":
    main()

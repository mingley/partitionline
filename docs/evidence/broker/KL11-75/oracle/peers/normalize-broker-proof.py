#!/usr/bin/env python3
"""Admit only a complete immutable four-lane broker receipt and exact Fetch ELFs."""
import argparse
import hashlib
import json
from pathlib import Path


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--validation", required=True, type=Path)
    parser.add_argument("--source-sha", required=True)
    parser.add_argument("--binary-root", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    args = parser.parse_args()
    raw = json.loads(args.validation.read_text())
    if raw.get("source_commit") != args.source_sha or raw.get("schema") != 1:
        raise ValueError("Exact immutable broker source/schema required")
    final = raw["final_source"]
    if final.get("all_git_blobs_match") is not True or not final.get("file_count") or not final.get("set_and_blob_sha256"):
        raise ValueError("Complete final Git blob integrity proof required")
    expected = {"clean-before-source-switch", "format", "clean-between-toolchains"}
    for compiler in ("stable", "1.85.0"):
        for features in ("default", "all-features"):
            for gate in ("all-targets", "strict-clippy", "strict-doc", "strict-doctest"):
                expected.add(f"{compiler}-{features}-{gate}")
    commands = raw["commands"]
    if len(commands) != 19 or {row["name"] for row in commands} != expected:
        raise ValueError("Exactly19 actual full broker commands/four strict lanes required")
    for row in commands:
        if row.get("exit_code") != 0:
            raise ValueError("All actual broker gates must pass before public peers")
        for phase in ("source_before", "source_after"):
            proof = row[phase]
            if proof.get("all_git_blobs_match") is not True or proof.get("file_count") != final["file_count"] or proof.get("set_and_blob_sha256") != final["set_and_blob_sha256"]:
                raise ValueError("Exact full source bytes must survive every command")
    binaries = []
    for compiler in ("stable", "1.85.0"):
        for features in ("default", "all-features"):
            lane = f"{compiler}-{features}-all-targets"
            rows = [row for row in raw["retained_binaries"] if row.get("lane") == lane
                    and Path(row["path"]).name in ("fetch", "fetch-all-features")
                    and not row.get("compression")]
            if len(rows) != 1:
                raise ValueError("One genuine copied Fetch live ELF required per lane: " + lane)
            row = rows[0]
            relative = Path(row["path"])
            if relative.is_absolute() or ".." in relative.parts:
                raise ValueError("Copied broker ELF path must be scoped")
            path = args.binary_root / relative
            data = path.read_bytes()
            digest = hashlib.sha256(data).hexdigest()
            gate = next(command for command in commands if command["name"] == lane)
            if not data.startswith(b"\x7fELF") or digest != row["sha256"] or row.get("source_commit") != args.source_sha or row.get("build_command") != gate["argv"]:
                raise ValueError("Exact actual source/lane/test-build/ELF byte binding required")
            binaries.append(dict(row, actual_path=str(path), bytes=len(data)))
    compiler_versions = raw["toolchains"]
    if set(compiler_versions) != {"stable", "1.85.0"} or "release: 1.85.0" not in compiler_versions["1.85.0"]:
        raise ValueError("Actual stable/MSRV compiler receipts required")
    result = {"schema_version": 1, "schema": 1, "source_commit": args.source_sha, "passed": True,
              "actual_full_broker_commands": 19, "actual_broker_lanes": 4,
              "raw_validation": {"path": str(args.validation), "sha256": hashlib.sha256(args.validation.read_bytes()).hexdigest()},
              "normalizer_sha256": hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
              "retained_binaries": binaries, "commands": commands,
              "toolchains": compiler_versions, "final_source": final,
              "scope": "Admission input to genuine public peers; this normalization does not itself establish any client/runtime result."}
    args.output.parent.mkdir(parents=True,exist_ok=True)
    args.output.write_text(json.dumps(result,indent=2)+"\n")
    print(json.dumps({"passed": True, "source_sha":args.source_sha, "actual_full_broker_commands":19, "copied_fetch_binaries":4}),flush=True)


if __name__ == "__main__":
    main()

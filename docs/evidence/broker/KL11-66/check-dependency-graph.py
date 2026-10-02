#!/usr/bin/env python3
"""Compare active Linux default dependency graphs from two immutable SHAs."""
import argparse
import hashlib
import io
import json
import os
from pathlib import Path
import subprocess
import tarfile


def digest(data):
    return hashlib.sha256(data).hexdigest()


def graph(metadata):
    packages = {package["id"]: package for package in metadata["packages"]}
    result = []
    for node in metadata["resolve"]["nodes"]:
        package = packages[node["id"]]
        deps = [{"name": dependency["name"],
                 "package": packages[dependency["pkg"]]["name"],
                 "version": packages[dependency["pkg"]]["version"],
                 "kinds": dependency["dep_kinds"]} for dependency in node["deps"]]
        result.append({"name": package["name"], "version": package["version"],
                       "features": sorted(node["features"]),
                       "dependencies": sorted(deps, key=lambda row: row["name"])})
    return sorted(result, key=lambda row: (row["name"], row["version"]))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo", type=Path, required=True)
    parser.add_argument("--baseline", required=True)
    parser.add_argument("--candidate", required=True)
    parser.add_argument("--work", type=Path, required=True)
    args = parser.parse_args()
    args.work.mkdir(parents=True, exist_ok=False)
    result = {"baseline_source_sha": args.baseline, "candidate_source_sha": args.candidate,
              "platform": "x86_64-unknown-linux-gnu", "default_graphs": {},
              "commands": [], "manifest_hashes": {}}
    for label, source_sha in [("baseline", args.baseline), ("candidate", args.candidate)]:
        dest = args.work / label
        dest.mkdir()
        archive = subprocess.check_output(["git", "archive", source_sha, "Cargo.toml",
            "Cargo.lock", "src", "tests", "examples", "partitionline-broker"], cwd=args.repo)
        with tarfile.open(fileobj=io.BytesIO(archive)) as handle:
            handle.extractall(dest, filter="data")
        result["manifest_hashes"][label] = {str(path.relative_to(dest)): digest(path.read_bytes())
            for path in [dest / "Cargo.toml", dest / "Cargo.lock", dest / "partitionline-broker/Cargo.toml",
                         dest / "partitionline-broker/Cargo.lock"]}
        for crate, manifest in [("client", dest / "Cargo.toml"),
                                ("broker", dest / "partitionline-broker/Cargo.toml")]:
            command = ["cargo", "+stable", "metadata", "--offline", "--locked",
                "--format-version", "1", "--filter-platform", result["platform"],
                "--manifest-path", str(manifest)]
            run = subprocess.run(command, cwd=dest, env=os.environ, capture_output=True)
            output = args.work / f"{label}-{crate}-metadata.json"
            error = args.work / f"{label}-{crate}-metadata.stderr"
            output.write_bytes(run.stdout)
            error.write_bytes(run.stderr)
            result["commands"].append({"argv": command, "exit_code": run.returncode,
                "stdout": output.name, "stdout_sha256": digest(run.stdout),
                "stderr": error.name, "stderr_sha256": digest(run.stderr)})
            if run.returncode:
                (args.work / "results.json").write_text(json.dumps(result, indent=2) + "\n")
                raise SystemExit(run.returncode)
            result["default_graphs"][f"{label}-{crate}"] = graph(json.loads(run.stdout))
    result["unchanged"] = {crate: result["default_graphs"][f"baseline-{crate}"] ==
        result["default_graphs"][f"candidate-{crate}"] for crate in ["client", "broker"]}
    (args.work / "results.json").write_text(json.dumps(result, indent=2) + "\n")
    print(json.dumps(result["unchanged"], sort_keys=True))
    if not all(result["unchanged"].values()):
        raise SystemExit("Default dependency graph changed")


if __name__ == "__main__":
    main()

#!/usr/bin/env python3
"""Compare live and refused-first bootstrap dials on an unchanged baseline."""

import argparse
import hashlib
import json
from pathlib import Path
import re
import subprocess


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo-root", type=Path, default=Path(__file__).resolve().parents[4])
    parser.add_argument("--target-dir", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--cpu", type=int)
    parser.add_argument("--import-output", type=Path)
    parser.add_argument("--probe-binary", type=Path)
    args = parser.parse_args()
    repo = args.repo_root.resolve()
    target = args.target_dir.resolve()
    source = Path(__file__).with_name("bootstrap-probe.rs").resolve()
    binary = args.probe_binary or target / "bootstrap-probe"
    if args.import_output:
        if not args.probe_binary:
            parser.error("--import-output requires the original --probe-binary")
        raw = args.import_output.read_text()
    else:
        subprocess.run(["cargo", "build", "--locked", "--release", "--manifest-path",
                        str(repo / "benchmarks/runtime/Cargo.toml"), "--target-dir",
                        str(target)], cwd=repo, check=True)
        deps = target / "release/deps"
        libraries = {}
        for name in ["partitionline", "tokio"]:
            matches = sorted(deps.glob(f"lib{name}-*.rlib"), key=lambda p: p.stat().st_mtime)
            if not matches:
                raise SystemExit(f"missing built {name} library in {deps}")
            libraries[name] = matches[-1]
        subprocess.run(["rustc", "--edition=2021", "-O", str(source), "--extern",
                        f"partitionline={libraries['partitionline']}", "--extern",
                        f"tokio={libraries['tokio']}", "-L", f"dependency={deps}",
                        "-o", str(binary)], cwd=repo, check=True)
        command = [str(binary)]
        if args.cpu is not None:
            command = ["taskset", "-c", str(args.cpu)] + command
        raw = subprocess.check_output(command, text=True, cwd=repo)
    pattern = re.compile(r"repetition=(\d+) refused_first=(false|true) dials=(\d+) total_ns=(\d+)")
    rows = []
    for line in raw.splitlines():
        match = pattern.fullmatch(line)
        if not match:
            raise SystemExit(f"unexpected probe output: {line}")
        repetition, refused, dials, ns = match.groups()
        rows.append({"repetition": int(repetition), "refused_first": refused == "true",
                     "dials": int(dials), "total_ns": int(ns),
                     "mean_us_per_dial": int(ns) / int(dials) / 1000.0})
    if len(rows) != 10 or sum(row["dials"] for row in rows) != 1000:
        raise SystemExit("probe did not execute all five alternating 100-dial pairs")
    result = {
        "schema_version": 1,
        "baseline_sha": subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=repo,
                                                 text=True).strip(),
        "source_sha256": hashlib.sha256(source.read_bytes()).hexdigest(),
        "binary_sha256": hashlib.sha256(binary.read_bytes()).hexdigest(),
        "toolchain": subprocess.check_output(["rustc", "--version"], text=True).strip(),
        "profile": "release dependencies; rustc -O standalone probe; current-thread Tokio",
        "cpu_affinity": args.cpu,
        "connect_timeout_ms": 1000,
        "warmup_dials": 20,
        "pair_order": "live/refused-first, refused-first/live, alternating",
        "samples": rows,
        "limits": [
            "Both arms execute the baseline; this is a hotspot visibility probe, not a candidate A/B.",
            "Refused-first is 127.0.0.1:1, matching nb-connect; the probe first verifies it refuses connections.",
            "Only TCP/bootstrap selection is timed; ApiVersions/SASL/metadata/first ack are excluded.",
            "No blackholed SYN, stalled TLS handshake, DNS delay or multi-broker setup is exercised.",
        ],
    }
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(result, indent=2) + "\n")
    print(f"{len(rows)} baseline dial batches written to {args.output}")


if __name__ == "__main__":
    main()

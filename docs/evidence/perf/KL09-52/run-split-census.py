#!/usr/bin/env python3
"""Reproduce the bounded BytesMut request-split allocation hypothesis probe."""

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
    parser.add_argument("--repetitions", type=int, default=5)
    args = parser.parse_args()
    if args.repetitions < 1:
        parser.error("--repetitions must be positive")
    repo = args.repo_root.resolve()
    target = args.target_dir.resolve()
    source = Path(__file__).with_name("split-census.rs").resolve()
    build = ["cargo", "build", "--locked", "--release", "--manifest-path",
             str(repo / "benchmarks/codec/Cargo.toml"), "--target-dir", str(target)]
    subprocess.run(build, cwd=repo, check=True)
    deps = target / "release/deps"
    libraries = {}
    for name in ["codec", "bytes"]:
        matches = sorted(deps.glob(f"lib{name}-*.rlib"), key=lambda p: p.stat().st_mtime)
        if not matches:
            raise SystemExit(f"missing built {name} library in {deps}")
        libraries[name] = matches[-1]
    binary = target / "split-census"
    compile_cmd = ["rustc", "--edition=2021", "-O", str(source), "--extern",
                   f"codec={libraries['codec']}", "--extern", f"bytes={libraries['bytes']}",
                   "-L", f"dependency={deps}", "-o", str(binary)]
    subprocess.run(compile_cmd, cwd=repo, check=True)
    pattern = re.compile(r"size=(\d+) requests=(\d+) split_allocations=(\d+) "
                         r"split_bytes=(\d+) reuse_allocations=(\d+) reuse_bytes=(\d+)")
    samples = []
    for repetition in range(args.repetitions):
        output = subprocess.check_output([str(binary)], text=True, cwd=repo)
        for line in output.splitlines():
            match = pattern.fullmatch(line)
            if not match:
                raise SystemExit(f"unexpected probe output: {line}")
            values = list(map(int, match.groups()))
            samples.append(dict(zip(["request_bytes", "requests", "baseline_allocations",
                                     "baseline_allocated_bytes", "reuse_allocations",
                                     "reuse_allocated_bytes"], values), repetition=repetition))
    result = {
        "schema_version": 1,
        "baseline_sha": subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=repo,
                                                 text=True).strip(),
        "source_sha256": hashlib.sha256(source.read_bytes()).hexdigest(),
        "binary_sha256": hashlib.sha256(binary.read_bytes()).hexdigest(),
        "toolchain": subprocess.check_output(["rustc", "--version"], text=True).strip(),
        "profile": "release dependencies; rustc -O standalone probe",
        "allocator": "codec::CountingAlloc and codec::census from the baseline checkout",
        "repetitions": args.repetitions,
        "samples": samples,
        "limits": [
            "Standalone allocation mechanism probe; no runtime timing comparison.",
            "Input and initial 16 KiB request buffers are allocated outside each census.",
            "Each split payload is dropped before the next request, matching completed writes.",
            "The reuse arm isolates take/restore; it is not a production candidate or a proposed retention policy.",
        ],
    }
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(result, indent=2) + "\n")
    print(f"{len(samples)} samples written to {args.output}")


if __name__ == "__main__":
    main()

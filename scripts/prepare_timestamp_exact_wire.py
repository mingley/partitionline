#!/usr/bin/env python3
"""Inject identical test-only controls into two pinned producer sources."""
import hashlib
from pathlib import Path
import subprocess
import sys

baseline_commit, out_dir = sys.argv[1:]
candidate = Path("src/producer.rs").read_bytes()
baseline = subprocess.check_output(["git", "show", f"{baseline_commit}:src/producer.rs"])
for arm, data, expected in (
    ("baseline", baseline, "3d65fcceecbb43c1a974f7f6b7bd58c8a310930f6bcced9052a00e88700eb37f"),
    ("candidate", candidate, "60a3fa0985358198e81757e6080af8b58ab52cb00edc1f34e2c97ed033e90b78"),
):
    actual = hashlib.sha256(data).hexdigest()
    print(f"EXACT_WIRE_ORIGINAL arm={arm} sha256={actual}", flush=True)
    if actual != expected:
        raise RuntimeError(f"unexpected {arm} source: {actual}")
anchor = b"    #[test]\n    fn an_uncompressed_produce_sizes_its_batch_once() {"
fixture = Path("tests/fixtures/producer_timestamp_exact_wire.inc").read_bytes()
output = Path(out_dir)
output.mkdir(parents=True, exist_ok=True)
for arm, source in (("baseline", baseline), ("candidate", candidate)):
    if source.count(anchor) != 1:
        raise RuntimeError(f"missing unique insertion point in {arm}")
    instrumented = source.replace(anchor, fixture + anchor)
    path = output / f"{arm}.rs"
    path.write_bytes(instrumented)
    print(f"EXACT_WIRE_INSTRUMENTED arm={arm} sha256={hashlib.sha256(instrumented).hexdigest()}", flush=True)

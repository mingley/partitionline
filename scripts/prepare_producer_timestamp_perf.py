#!/usr/bin/env python3
"""Prepare matched test-only source for a paired timestamp timing."""
from pathlib import Path
import subprocess
import sys

baseline_commit, output_dir = sys.argv[1:]
candidate = Path("src/producer.rs").read_text(encoding="utf-8")
baseline = subprocess.check_output(
    ["git", "show", f"{baseline_commit}:src/producer.rs"], text=True
)
anchor = "    #[test]\n    fn an_uncompressed_produce_sizes_its_batch_once() {"
start = '    #[tokio::test(flavor = "current_thread")]\n    async fn timestamp_freezing_at_existing_encode_yields()'
if candidate.count(anchor) != 1 or baseline.count(anchor) != 1 or candidate.count(start) != 1:
    raise RuntimeError("test insertion anchor mismatch")
unit_start = candidate.index(start)
unit_end = candidate.index(anchor, unit_start)
partial_control = candidate[unit_start:unit_end]
benchmark = Path("tests/fixtures/producer_timestamp_encode_bench.inc").read_text(encoding="utf-8")
base_with_bench = baseline.replace(anchor, partial_control + benchmark + anchor)
fix_with_bench = candidate.replace(anchor, benchmark + anchor)
out = Path(output_dir)
out.mkdir(parents=True, exist_ok=True)
(out / "baseline.rs").write_text(base_with_bench, encoding="utf-8")
(out / "candidate.rs").write_text(fix_with_bench, encoding="utf-8")

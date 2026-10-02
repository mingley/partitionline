#!/usr/bin/env python3
"""Replay consistency checks over retained KL04-06 mock diagnostics."""
import collections
import json
from pathlib import Path

root = Path(__file__).resolve().parent
results = []
for path in sorted(root.glob("mock-*.jsonl")):
    rows = [json.loads(line) for line in path.read_text().splitlines()]
    samples, summary = rows[:-1], rows[-1]
    assert summary["kind"] == "open_loop_produce_ack"
    assert [s["id"] for s in samples] == list(range(len(samples)))
    observed = collections.Counter(s["outcome"] for s in samples)
    outcomes = summary["outcomes"]
    assert outcomes["offered"] == len(samples)
    assert outcomes["accepted"] == sum(s["accepted"] for s in samples)
    for key in ("acknowledged", "rejected", "timed_out", "unknown"):
        assert outcomes[key] == observed[key]
    assert outcomes["offered"] == sum(outcomes[k] for k in ("acknowledged", "rejected", "timed_out", "unknown"))
    assert outcomes["consumed"] == 0
    assert summary["max_pending_observed"] <= summary["max_pending_configured"]
    assert summary["qualification"] is False and summary["suite_hold"] == "active"
    assert summary["run_disposition"] == ("executed" if observed["acknowledged"] == len(samples) else "failed")
    for sample in samples:
        intended, offered = sample["intended_arrival_ns"], sample["actual_offer_ns"]
        assert intended == sample["id"] * 1_000_000_000 // summary["rate_per_second"]
        assert offered >= intended
        assert sample["schedule_lag_ns"] == offered - intended
        lower, upper = sample["actual_enqueue_lower_ns"], sample["actual_enqueue_upper_ns"]
        assert sample["accepted"] == (upper is not None)
        if upper is not None:
            assert offered <= lower <= upper <= sample["completed_ns"]
            assert sample["enqueue_wait_upper_ns"] == upper - offered
        if sample["outcome"] == "acknowledged":
            ack = sample["acknowledgment_observed_ns"]
            assert ack is not None and ack >= upper
            assert sample["end_to_end_ns"] == ack - intended
            assert sample["enqueue_to_ack_upper_ns"] == ack - lower
        else:
            assert sample["acknowledgment_observed_ns"] is None
            assert sample["end_to_end_ns"] is None
    for metric in ("end_to_end", "enqueue_to_ack_upper_bound", "schedule_lag", "enqueue_wait_upper_bound"):
        distribution = summary[metric]
        assert distribution["sample_count"] < summary["sample_floor"]
        assert distribution["sample_floor_met"] is False
        assert all(distribution[p] is None for p in ("p50_us", "p95_us", "p99_us", "p99_9_us"))
    if path.stem == "mock-stall":
        first_ack = min(s["acknowledgment_observed_ns"] for s in samples)
        assert all(s["actual_offer_ns"] < first_ack for s in samples)
    results.append({"artifact": path.name, "samples": len(samples), "outcomes": outcomes})
assert len(results) == 4
print(json.dumps({"status": "passed", "artifact_count": len(results), "runs": results}, indent=2))

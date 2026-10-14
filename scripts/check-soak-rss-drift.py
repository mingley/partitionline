#!/usr/bin/env python3
"""Compare first steady-state-hour and final-hour median RSS for a soak run.

Does not rebuild or mutate the frozen soak driver. A short or incomplete run
fails closed. Passing this check is not a performance claim.
"""
from __future__ import annotations

import argparse
import json
import statistics
import sys
from pathlib import Path


def rows(path: Path):
    with path.open() as stream:
        for number, line in enumerate(stream, 1):
            if not line.strip():
                continue
            try:
                yield json.loads(line)
            except (ValueError, TypeError) as error:
                raise ValueError(f"{path}:{number}: malformed row") from error


def load_host_samples(directory: Path) -> list[dict]:
    manifest = json.loads((directory / "manifest.json").read_text())
    samples = []
    for attempt in manifest.get("attempts") or []:
        host = directory / attempt["host"]
        if not host.is_file():
            raise ValueError(f"missing host telemetry: {host}")
        for row in rows(host):
            if any(type(row.get(name)) is not int or row[name] < 0 for name in ("rss_bytes", "monotonic_ns")):
                raise ValueError(f"{host}: missing RSS/monotonic sample")
            samples.append(row)
    if len(samples) < 4:
        raise ValueError("not enough host samples")
    return samples


def median_rss(samples: list[dict], start_ns: int, end_ns: int) -> tuple[int, int]:
    window = [row["rss_bytes"] for row in samples if start_ns <= row["monotonic_ns"] < end_ns]
    if len(window) < 2:
        raise ValueError("RSS window has fewer than two samples")
    return int(statistics.median(window)), len(window)


def last_workload(directory: Path) -> dict | None:
    manifest = json.loads((directory / "manifest.json").read_text())
    latest = None
    for attempt in manifest.get("attempts") or []:
        raw = directory / attempt.get("raw", "")
        if not raw.is_file():
            continue
        for row in rows(raw):
            if row.get("kind") == "sample":
                latest = row
    return latest


def progress(directory: Path, window_ms: int, startup_ms: int) -> dict:
    """Report live RSS/workload without claiming the 24h comparison.

    Exit status of this report is not a pass. `passed` stays false.
    """
    frozen = json.loads((directory / "frozen.json").read_text())
    samples = load_host_samples(directory)
    origin = samples[0]["monotonic_ns"]
    latest = samples[-1]
    elapsed_ms = (latest["monotonic_ns"] - origin) / 1_000_000
    rss = [row["rss_bytes"] for row in samples]
    first_start = origin + startup_ms * 1_000_000
    first_end = first_start + window_ms * 1_000_000
    first_hour: dict
    if latest["monotonic_ns"] + 1 >= first_end:
        median, count = median_rss(samples, first_start, first_end)
        first_hour = {
            "complete": True,
            "median_rss_bytes": median,
            "samples": count,
            "remaining_ms": 0.0,
        }
    else:
        first_hour = {
            "complete": False,
            "median_rss_bytes": None,
            "samples": sum(1 for row in samples if first_start <= row["monotonic_ns"] < first_end),
            "remaining_ms": max(0.0, (first_end - latest["monotonic_ns"]) / 1_000_000),
        }
    workload = last_workload(directory)
    counters = None
    if workload is not None:
        counters = {
            name: workload.get(name)
            for name in (
                "elapsed_ms",
                "offered",
                "accepted",
                "completed",
                "failed",
                "ambiguous",
                "pending",
                "producer_queue_bytes",
                "consumer_buffered_bytes",
                "delivered",
                "fetch_errors",
                "runtime_tasks",
            )
        }
    return {
        "schema_version": 1,
        "directory": str(directory),
        "mode": frozen.get("mode"),
        "report": "progress",
        "elapsed_ms": elapsed_ms,
        "startup_ms": startup_ms,
        "window_ms": window_ms,
        "rss_bytes": {
            "min": min(rss),
            "median": int(statistics.median(rss)),
            "max": max(rss),
            "latest": latest["rss_bytes"],
        },
        "threads_latest": latest.get("threads"),
        "socket_fds_latest": latest.get("socket_fds"),
        "first_hour": first_hour,
        "workload": counters,
        "passed": False,
        "complete_24h": False,
        "production_qualification": False,
        "performance_claims_valid": False,
        "rss_bound_claimed": False,
    }


def evaluate(directory: Path, target_pct: float, window_ms: int, startup_ms: int, require_ms: int) -> dict:
    frozen = json.loads((directory / "frozen.json").read_text())
    samples = load_host_samples(directory)
    origin = samples[0]["monotonic_ns"]
    elapsed_ms = (samples[-1]["monotonic_ns"] - origin) / 1_000_000
    required = max(require_ms, 2 * window_ms + startup_ms)
    if elapsed_ms + 1 < required:
        raise ValueError(
            f"elapsed {elapsed_ms:.0f} ms is short of the {required} ms RSS-drift window "
            f"(startup {startup_ms} ms + first {window_ms} ms + final {window_ms} ms)"
        )
    first_start = origin + startup_ms * 1_000_000
    first_end = first_start + window_ms * 1_000_000
    final_end = samples[-1]["monotonic_ns"] + 1
    final_start = final_end - window_ms * 1_000_000
    if final_start < first_end:
        raise ValueError("first and final RSS windows overlap")
    first_median, first_n = median_rss(samples, first_start, first_end)
    final_median, final_n = median_rss(samples, final_start, final_end)
    if first_median == 0:
        raise ValueError("first-hour median RSS is zero")
    drift_pct = (final_median - first_median) * 100.0 / first_median
    passed = abs(drift_pct) <= target_pct
    return {
        "schema_version": 1,
        "directory": str(directory),
        "mode": frozen.get("mode"),
        "elapsed_ms": elapsed_ms,
        "startup_ms": startup_ms,
        "window_ms": window_ms,
        "target_pct": target_pct,
        "first_hour_median_rss_bytes": first_median,
        "final_hour_median_rss_bytes": final_median,
        "first_hour_samples": first_n,
        "final_hour_samples": final_n,
        "drift_pct": drift_pct,
        "passed": passed,
        "production_qualification": False,
        "performance_claims_valid": False,
        "rss_bound_claimed": False,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("directory", type=Path)
    parser.add_argument("--target-pct", type=float, default=10.0)
    parser.add_argument("--window-ms", type=int, default=3_600_000)
    parser.add_argument("--startup-ms", type=int, default=60_000)
    parser.add_argument("--require-ms", type=int, default=86_400_000)
    parser.add_argument(
        "--progress",
        action="store_true",
        help="print a live RSS/workload report; never a 24h pass",
    )
    args = parser.parse_args()
    try:
        if not 0 < args.target_pct <= 100:
            raise ValueError("target-pct out of bounds")
        if min(args.window_ms, args.startup_ms, args.require_ms) < 0 or args.window_ms < 100:
            raise ValueError("window/startup/require out of bounds")
        if args.progress:
            result = progress(args.directory, args.window_ms, args.startup_ms)
            print(json.dumps(result, indent=2))
            return 0
        result = evaluate(args.directory, args.target_pct, args.window_ms, args.startup_ms, args.require_ms)
        print(json.dumps(result, indent=2))
        return 0 if result["passed"] else 1
    except (OSError, ValueError, KeyError, TypeError) as error:
        print(f"soak RSS drift failed: {error}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())

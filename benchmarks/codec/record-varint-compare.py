"""Pair two exact-source release builds on a single Linux x86 runner."""
import json
import os
from pathlib import Path
import platform
import statistics
import subprocess
import sys

PAIRS = 12
CASES = {
    "none-tiny", "none-100", "none-1024-h2", "none-large", "gzip-100",
    "snappy-100", "lz4-1024-h2", "zstd-1024-h2",
}


def run(executable, rotation):
    proc = subprocess.run(
        [executable, str(rotation)], check=True, capture_output=True,
        text=True, timeout=120,
    )
    rows = [json.loads(line) for line in proc.stdout.splitlines()]
    result = {row["case"]: row for row in rows}
    if len(rows) != len(CASES) or set(result) != CASES:
        raise ValueError("expected exactly one row per fixture")
    return result


def main():
    if platform.machine() != "x86_64":
        raise ValueError("run the comparison on x86_64")
    cpus = sorted(os.sched_getaffinity(0))
    cpu = cpus[-1]
    os.sched_setaffinity(0, {cpu})
    print(f"PINNED_CPU {cpu} AVAILABLE_CPUS {cpus}", flush=True)

    baseline, candidate, evidence = sys.argv[1:]
    signatures = {}
    samples = {case: [] for case in CASES}
    with Path(evidence).open("w", encoding="utf-8") as results:
        for pair in range(PAIRS):
            order = ("baseline", "candidate") if pair % 2 == 0 else ("candidate", "baseline")
            rows = {
                arm: run(baseline if arm == "baseline" else candidate, pair)
                for arm in order
            }
            for case in sorted(CASES):
                for arm in ("baseline", "candidate"):
                    row = rows[arm][case]
                    timing = row["elapsed_ns"]
                    if type(timing) is not int or timing <= 0:
                        raise ValueError("expected a positive integer elapsed time")
                    signature = (
                        row["records_per_batch"], row["iterations"],
                        row["wire_bytes_per_batch"], row["wire_sha256"],
                    )
                    if signatures.setdefault(case, signature) != signature:
                        raise ValueError(f"{case} changed fixture, work, or bytes")
                entry = {"pair": pair, "order": order, "case": case, **{
                    arm: rows[arm][case] for arm in ("baseline", "candidate")
                }}
                line = json.dumps(entry, separators=(",", ":"))
                print("VARINT_PAIR " + line, flush=True)
                results.write(line + "\n")
                base_ns = rows["baseline"][case]["elapsed_ns"]
                cand_ns = rows["candidate"][case]["elapsed_ns"]
                samples[case].append((base_ns, cand_ns))

    for case, times in sorted(samples.items()):
        speedup = sorted((baseline / candidate - 1) * 100 for baseline, candidate in times)
        records_per_trial = signatures[case][0] * signatures[case][1]
        print("VARINT_RESULT " + json.dumps({
            "case": case,
            "median_throughput_change_percent": statistics.median(speedup),
            # The 3rd and 10th ordered observations span a 96.14% sign interval
            # for the median at n=12, under independent continuous paired ratios.
            "sign_interval_percent": [speedup[2], speedup[9]],
            "median_baseline_records_per_second": statistics.median(
                records_per_trial * 1e9 / baseline for baseline, _ in times
            ),
            "median_candidate_records_per_second": statistics.median(
                records_per_trial * 1e9 / candidate for _, candidate in times
            ),
        }, separators=(",", ":")), flush=True)


if __name__ == "__main__":
    main()

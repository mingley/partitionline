#!/usr/bin/env python3
"""Summarize retained trials, preserving every missing/failing arm."""
import json
from pathlib import Path
import statistics
import sys

from check_sample import load_complete_sample

root = Path(sys.argv[1])
pairs = []
issues = []
for pair in range(1, 6):
    row = {"pair": pair}
    for arm in ("rust", "c"):
        directory = root / f"pair-{pair}-{arm}"
        try:
            result = json.loads((directory / "verdict.json").read_text(encoding="utf-8"))
            sockets = load_complete_sample(directory)
            status = int((directory / "driver-exit.txt").read_text(encoding="utf-8"))
            if status != 0 or result["verified_acknowledgements"] != 20000:
                raise ValueError("incomplete driver or verification")
            row[arm] = {"measure": result["measure"], "socket_samples": sockets["successful_samples"],
                        "socket_counts": sockets["counts"]}
        except (OSError, ValueError, KeyError, json.JSONDecodeError) as error:
            issues.append(f"pair {pair} {arm}: {error}")
    pairs.append(row)
if not issues:
    for row in pairs:
        r, c = row["rust"]["measure"], row["c"]["measure"]
        row["ratios_c_over_rust"] = {
            "records_per_s": c["records_per_s"] / r["records_per_s"],
            "p50_latency": c["p50_us"] / r["p50_us"],
            "p99_latency": c["p99_us"] / r["p99_us"],
        }
    summary = {key: statistics.median(row["ratios_c_over_rust"][key] for row in pairs)
               for key in ("records_per_s", "p50_latency", "p99_latency")}
else:
    summary = None
result = {"disposition": "exploratory closed-loop on a shared runner; not contract-qualified",
          "connection_sampling_limit": "50 ms intervals cannot exclude transient extra sockets",
          "issues": issues, "pairs": pairs, "median_ratios_c_over_rust": summary}
print(json.dumps(result, indent=2, sort_keys=True))
if issues:
    sys.exit(1)

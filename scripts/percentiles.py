#!/usr/bin/env python3
"""p50 / p99 / p999 from one-integer-per-line latency samples (microseconds)."""
import sys


def percentile(sorted_vals, p: float) -> int:
    if not sorted_vals:
        return 0
    idx = int(round((len(sorted_vals) - 1) * p))
    return sorted_vals[min(max(idx, 0), len(sorted_vals) - 1)]


def main() -> None:
    path = sys.argv[1]
    vals = []
    with open(path, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line:
                vals.append(int(line))
    vals.sort()
    print(
        f"n={len(vals)} p50_us={percentile(vals, 0.50)} "
        f"p99_us={percentile(vals, 0.99)} p999_us={percentile(vals, 0.999)}"
    )


if __name__ == "__main__":
    main()

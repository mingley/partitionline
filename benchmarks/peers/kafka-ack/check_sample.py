#!/usr/bin/env python3
"""Fail closed unless retained socket samples are complete and match the gate."""
from collections import Counter
import json
from pathlib import Path
import sys


def load_complete_sample(directory):
    directory = Path(directory)
    status = int((directory / "socket-sampler-exit.txt").read_text(encoding="utf-8").strip())
    report = json.loads((directory / "sockets.json").read_text(encoding="utf-8"))
    if not isinstance(report, dict):
        raise ValueError("invalid socket sample report")
    if status != 0 or report.get("complete") is not True or report.get("error") is not None:
        raise ValueError("socket sampler failed or reported incomplete evidence")
    samples = report.get("samples")
    count = report.get("successful_samples")
    if type(count) is not int or count < 2 or not isinstance(samples, list) or len(samples) != count:
        raise ValueError("socket sample count is missing or incomplete")
    previous = -1
    observed = []
    for sample in samples:
        if not isinstance(sample, list) or len(sample) != 2:
            raise ValueError("invalid socket sample")
        elapsed, connections = sample
        if type(elapsed) is not int or type(connections) is not int or elapsed < previous or connections < 0:
            raise ValueError("invalid socket sample")
        previous = elapsed
        observed.append(connections)
    counts = {str(key): value for key, value in sorted(Counter(observed).items())}
    if report.get("counts") != counts or report.get("max_established") != max(observed):
        raise ValueError("socket sample summary differs from raw samples")
    if report["max_established"] != 1:
        raise ValueError(f"observed sockets {report['max_established']} in {count} samples")
    return report


def main():
    try:
        load_complete_sample(sys.argv[1])
    except (OSError, ValueError, KeyError, json.JSONDecodeError) as error:
        print(f"socket gate failed: {error}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())

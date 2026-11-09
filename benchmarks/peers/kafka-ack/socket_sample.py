#!/usr/bin/env python3
"""Sample established TCP sockets owned by one benchmark process, at 50 ms intervals."""
import argparse
from collections import Counter
import json
import os
import sys
from pathlib import Path
import time

def sockets(pid, port):
    root = Path("/proc") / str(pid)
    inodes = set()
    for fd in (root / "fd").iterdir():
        try:
            link = os.readlink(fd)
        except FileNotFoundError:
            continue
        if link.startswith("socket:[") and link.endswith("]"):
            inodes.add(link[8:-1])
    count = 0
    for name in ("tcp", "tcp6"):
        for line in (root / "net" / name).read_text(encoding="ascii").splitlines()[1:]:
            fields = line.split()
            if len(fields) >= 10 and fields[3] == "01" and fields[9] in inodes:
                remote_port = int(fields[2].rsplit(":", 1)[1], 16)
                count += remote_port == port
    return count

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("pid", type=int)
    parser.add_argument("port", type=int)
    parser.add_argument("output")
    args = parser.parse_args()
    start = time.monotonic_ns()
    samples = []
    races = 0
    error = None
    while Path("/proc", str(args.pid)).exists():
        try:
            samples.append((time.monotonic_ns() - start, sockets(args.pid, args.port)))
        except (FileNotFoundError, ProcessLookupError):
            races += 1
        except OSError as exc:
            # Keep prior samples, but do not retry or skip a denied descriptor.
            error = {"type": type(exc).__name__, "message": str(exc)}
            break
        time.sleep(0.05)
    result = {"sample_period_ms": 50, "duration_ns": time.monotonic_ns() - start,
              "successful_samples": len(samples), "races": races,
              "complete": error is None, "error": error,
              "counts": dict(sorted(Counter(value for _, value in samples).items())),
              "max_established": max((value for _, value in samples), default=None),
              "samples": samples,
              "note": "Discrete samples can miss transient connections."}
    with Path(args.output).open("x", encoding="utf-8") as f:
        json.dump(result, f, indent=2, sort_keys=True)
        f.write("\n")
    if error is not None:
        print(f"socket sampling incomplete: {error['type']}: {error['message']}", file=sys.stderr)
        return 1
    return 0

if __name__ == "__main__":
    sys.exit(main())

#!/usr/bin/env python3
"""Record wait4 resource usage for one owned, parent-bound command."""
import ctypes
import json
import os
from pathlib import Path
import signal
import sys
import time


def main():
    if len(sys.argv) < 3:
        raise SystemExit("usage: measure-process.py new-result.json command [args...]")
    output = Path(sys.argv[1])
    if output.exists():
        raise SystemExit("resource output already exists")
    owner = os.getpid()
    start = time.monotonic_ns()
    child = os.fork()
    if child == 0:
        library = ctypes.CDLL(None, use_errno=True)
        if library.prctl(1, signal.SIGKILL, 0, 0, 0):
            os._exit(125)
        if os.getppid() != owner:
            os._exit(125)
        os.execvp(sys.argv[2], sys.argv[2:])
    def forward(signum, frame):
        try:
            os.kill(child, signum)
        except ProcessLookupError:
            pass
    for signum in (signal.SIGTERM, signal.SIGINT):
        signal.signal(signum, forward)
    waited, status, usage = os.wait4(child, 0)
    assert waited == child
    code = os.waitstatus_to_exitcode(status)
    result = dict(schema_version=1, scope="whole owned child lifetime, including setup, warmup and shutdown",
                  child_pid=child, parent_waited=True, exit_code=code,
                  user_cpu_seconds=usage.ru_utime, system_cpu_seconds=usage.ru_stime,
                  peak_rss_kbytes=usage.ru_maxrss,
                  wall_seconds=(time.monotonic_ns()-start)/1e9,
                  voluntary_context_switches=usage.ru_nvcsw,
                  involuntary_context_switches=usage.ru_nivcsw)
    with output.open("x") as f:
        json.dump(result, f, indent=2, allow_nan=False)
        f.write("\n")
        f.flush()
        os.fsync(f.fileno())
    raise SystemExit(code if code >= 0 else 128-code)


if __name__ == "__main__":
    main()

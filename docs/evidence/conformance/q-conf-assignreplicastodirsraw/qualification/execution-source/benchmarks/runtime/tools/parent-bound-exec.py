#!/usr/bin/env python3
"""Stop the executed process if its original Linux parent disappears."""
import ctypes
import os
import signal
import sys


def main():
    if len(sys.argv) < 3:
        raise SystemExit("usage: parent-bound-exec.py expected-parent-pid command [args...]")
    parent = int(sys.argv[1])
    if parent <= 1 or os.getppid() != parent:
        raise SystemExit("benchmark parent disappeared before launch")
    library = ctypes.CDLL(None, use_errno=True)
    if library.prctl(1, signal.SIGKILL, 0, 0, 0):
        raise OSError(ctypes.get_errno(), "PR_SET_PDEATHSIG")
    if os.getppid() != parent:
        raise SystemExit("benchmark parent disappeared during launch")
    os.execvp(sys.argv[2], sys.argv[2:])


if __name__ == "__main__":
    main()

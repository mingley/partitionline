#!/usr/bin/env python3
"""Operational CPU-mask adapter; retains requested and effective child argv."""
import json
import os
import sys
args = sys.argv[1:]
requested = ["taskset"] + args
if len(args) < 3 or args[:2] != ["-c", "0-2,4"]:
    raise SystemExit("Unexpected taskset invocation")
actual = ["/usr/bin/taskset", "-c", "2,4"] + args[2:]
with open(os.environ["PL_MEMBERSHIP_AFFINITY_LOG"], "a", encoding="utf-8") as stream:
    stream.write(json.dumps({"requested_argv": requested, "effective_argv": actual, "cwd": os.getcwd()}) + "\n")
os.execv(actual[0], actual)

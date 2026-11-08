#!/usr/bin/env python3
"""Require the intentional fault to fail at the target's byte assertion."""
import os
import pathlib
import re
import sys

fault = os.environ["FAULT"]
text = pathlib.Path(sys.argv[1]).read_text()
expected_left = {"omit": 32881, "double": 32655}[fault]
if "cancelled channel admission retained its unqueued reservation" not in text:
    raise SystemExit(f"{fault}: target assertion not reached")
match = re.search(r"left:\s*(\d+)\s+right:\s*(\d+)", text)
if match is None or tuple(map(int, match.groups())) != (expected_left, 32768):
    raise SystemExit(f"{fault}: wrong assertion values: {match.groups() if match else 'missing'}")
if "test result: FAILED. 0 passed; 1 failed" not in text:
    raise SystemExit(f"{fault}: wrong number of tests or failure location")
print(f"{fault}: expected target assertion and byte values verified")

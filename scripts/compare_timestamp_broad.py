#!/usr/bin/env python3
"""Summarize matched producer suite logs without attributing failures."""
import json
from pathlib import Path
import re
import sys

root = Path(sys.argv[1])
report = {}
for suite in ("lib", "mock"):
    report[suite] = {}
    for arm in ("baseline", "candidate"):
        log = (root / f"{arm}-{suite}.log").read_text(encoding="utf-8")
        status = int((root / f"{arm}-{suite}.status").read_text(encoding="utf-8"))
        summary = re.findall(
            r"test result: (ok|FAILED)\. (\d+) passed; (\d+) failed; (\d+) ignored; (\d+) measured; (\d+) filtered out;",
            log,
        )
        if len(summary) != 1:
            raise RuntimeError(f"missing or duplicate suite summary for {arm} {suite}: {summary}")
        result, passed, failed, ignored, measured, filtered = summary[0]
        names = sorted(re.findall(r"^test (.+?) \.\.\. FAILED$", log, re.MULTILINE))
        if len(names) != int(failed) or (status == 0) != (result == "ok"):
            raise RuntimeError(f"status or failure names disagree for {arm} {suite}: {status} {summary} {names}")
        report[suite][arm] = {
            "status": status, "result": result, "passed": int(passed), "failed": int(failed),
            "ignored": int(ignored), "measured": int(measured), "filtered_out": int(filtered),
            "failed_tests": names,
        }
    left = set(report[suite]["baseline"]["failed_tests"])
    right = set(report[suite]["candidate"]["failed_tests"])
    report[suite]["failure_comparison"] = {
        "both": sorted(left & right), "baseline_only": sorted(left - right),
        "candidate_only": sorted(right - left),
    }
print(json.dumps(report, indent=2, sort_keys=True))
(root / "comparison.json").write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")

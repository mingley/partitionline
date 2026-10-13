#!/usr/bin/env python3
"""Run seeded broker journal fault histories and record the observer counts.

The campaign opens the recovery, transaction-marker, and transactional-offset
journals. It checks crash reopen, repeated lost acknowledgements, fenced
epochs, and a follower that misses leader appends. It does not inject a
filesystem sync error and it does not create a multi-host cluster.
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
OUT = ROOT / "docs" / "evidence" / "broker" / "faults" / "campaign.json"


def main() -> int:
    OUT.parent.mkdir(parents=True, exist_ok=True)
    env = os.environ.copy()
    env["PL_FAULT_CAMPAIGN_OUT"] = str(OUT)
    completed = subprocess.run(
        [
            "cargo",
            "test",
            "--locked",
            "--offline",
            "--manifest-path",
            "partitionline-broker/Cargo.toml",
            "--test",
            "fault_campaign",
            "--",
            "--nocapture",
        ],
        cwd=ROOT,
        env=env,
        check=False,
        text=True,
    )
    if completed.returncode != 0:
        return completed.returncode
    report = json.loads(OUT.read_text())
    failures = []
    for profile in report["profiles"]:
        if profile["faults"] < report["faults_per_profile"] or profile["observer_failures"] != 0:
            failures.append(profile["profile"])
    if failures:
        print("observer failures: " + ", ".join(failures), file=sys.stderr)
        return 1
    print(json.dumps(report, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

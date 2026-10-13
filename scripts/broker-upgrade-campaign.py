#!/usr/bin/env python3
"""Pin source identity and run the mixed-version feature rollback check.

The check uses the local admin journal. Version 0 raises a feature, version 1
raises it again, and a downgrade or an unspoken version leaves that level and
the log directory in place. This is not a multi-broker rolling upgrade.
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
import tempfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
OUT = ROOT / "docs" / "evidence" / "broker" / "upgrade" / "campaign.json"


def run(args: list[str]) -> str:
    completed = subprocess.run(args, cwd=ROOT, check=True, text=True, capture_output=True)
    return completed.stdout.strip()


def main() -> int:
    OUT.parent.mkdir(parents=True, exist_ok=True)
    behavior_file = Path(tempfile.mkstemp(prefix="pl-upgrade-", suffix=".json")[1])
    env = os.environ.copy()
    env["PL_UPGRADE_CAMPAIGN_OUT"] = str(behavior_file)
    completed = subprocess.run(
        [
            "cargo",
            "test",
            "--locked",
            "--offline",
            "--manifest-path",
            "partitionline-broker/Cargo.toml",
            "--test",
            "upgrade_campaign",
            "--",
            "--nocapture",
        ],
        cwd=ROOT,
        env=env,
        check=False,
        text=True,
    )
    if completed.returncode != 0:
        sys.stderr.write(completed.stdout)
        sys.stderr.write(completed.stderr)
        return completed.returncode
    behavior = json.loads(behavior_file.read_text())
    behavior_file.unlink(missing_ok=True)
    report = {
        "source_sha": run(["git", "rev-parse", "HEAD"]),
        "rustc": run(["rustc", "--version"]),
        "cluster_id": "cluster-1",
        "config": {
            "feature": "metadata.version",
            "spoken_versions": [0, 1],
            "log_dir": "data-a",
        },
        "behavior": behavior,
    }
    if not behavior["downgrade_rejected"] or not behavior["unsupported_version_rejected"]:
        print("downgrade was not rejected", file=sys.stderr)
        return 1
    if behavior["rollback_level"] != behavior["proven_level"]:
        print("rollback level diverged", file=sys.stderr)
        return 1
    OUT.write_text(json.dumps(report, indent=2) + "\n")
    print(json.dumps(report, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

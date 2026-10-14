"""First-hour vs final-hour soak RSS drift checker."""
from __future__ import annotations

import json
import os
from pathlib import Path
import subprocess
import tempfile
import unittest

ROOT = Path(__file__).resolve().parents[2]
SCRIPT = ROOT / "scripts" / "check-soak-rss-drift.py"


def write_run(directory: Path, rss_series: list[int], step_ns: int = 50_000_000) -> None:
    directory.mkdir(parents=True, exist_ok=True)
    attempt = directory / "attempt-0001"
    attempt.mkdir()
    host = attempt / "host.jsonl"
    origin = 1_000_000_000
    with host.open("w") as stream:
        for index, rss in enumerate(rss_series):
            stream.write(json.dumps({
                "utc": "2026-10-14T00:00:00+00:00",
                "monotonic_ns": origin + index * step_ns,
                "pid": 1,
                "rss_bytes": rss,
                "threads": 2,
                "socket_fds": 4,
            }) + "\n")
    (directory / "frozen.json").write_text(json.dumps({"mode": "controlled", "duration_ms": 86_400_000}) + "\n")
    (directory / "manifest.json").write_text(json.dumps({
        "attempts": [{"number": 1, "host": "attempt-0001/host.jsonl", "status": "passed"}],
    }) + "\n")


def series(startup: int, first: int, middle: int, final: int) -> list[int]:
    # 80 samples: 10 startup + 20 first + 20 middle + 20 final + 10 tail
    return ([startup] * 10) + ([first] * 20) + ([middle] * 20) + ([final] * 20) + ([final] * 10)


class SoakRssDrift(unittest.TestCase):
    def run_checker(self, directory: Path, extra=(), expected=0):
        command = [
            "python3", "-B", str(SCRIPT), str(directory),
            "--window-ms", "1000",
            "--startup-ms", "500",
            "--require-ms", "3500",
            *extra,
        ]
        result = subprocess.run(command, capture_output=True, text=True, timeout=10)
        if result.returncode != expected:
            raise AssertionError(f"exit {result.returncode} != {expected}: {result.stdout}\n{result.stderr}")
        return result

    def test_within_target_passes(self):
        with tempfile.TemporaryDirectory() as tmp:
            directory = Path(tmp) / "ok"
            write_run(directory, series(8_000_000, 10_000_000, 10_200_000, 10_400_000))
            result = self.run_checker(directory)
            data = json.loads(result.stdout)
            self.assertTrue(data["passed"])
            self.assertFalse(data["production_qualification"])
            self.assertLessEqual(abs(data["drift_pct"]), 10)

    def test_above_target_fails(self):
        with tempfile.TemporaryDirectory() as tmp:
            directory = Path(tmp) / "drift"
            write_run(directory, series(8_000_000, 10_000_000, 12_000_000, 13_000_000))
            result = self.run_checker(directory, expected=1)
            data = json.loads(result.stdout)
            self.assertFalse(data["passed"])
            self.assertGreater(data["drift_pct"], 10)

    def test_short_run_fails_closed(self):
        with tempfile.TemporaryDirectory() as tmp:
            directory = Path(tmp) / "short"
            write_run(directory, [10_000_000] * 8)
            result = self.run_checker(directory, expected=1)
            self.assertEqual(result.stdout, "")
            self.assertIn("short of the", result.stderr)

    def test_live_kl02_10_launch_is_not_24h(self):
        live = Path("/tmp/kl02-10-soak")
        if not (live / "frozen.json").is_file():
            self.skipTest("live KL02-10 output is not on this host")
        result = subprocess.run(
            ["python3", "-B", str(SCRIPT), str(live)],
            capture_output=True, text=True, timeout=10,
        )
        self.assertEqual(result.returncode, 1)
        self.assertIn("short of the", result.stderr)
        self.assertEqual(result.stdout, "")

    def test_progress_reports_incomplete_first_hour(self):
        with tempfile.TemporaryDirectory() as tmp:
            directory = Path(tmp) / "early"
            write_run(directory, [10_000_000] * 20)
            result = self.run_checker(directory, extra=("--progress",))
            data = json.loads(result.stdout)
            self.assertEqual(data["report"], "progress")
            self.assertFalse(data["passed"])
            self.assertFalse(data["complete_24h"])
            self.assertFalse(data["first_hour"]["complete"])
            self.assertIsNone(data["first_hour"]["median_rss_bytes"])
            self.assertGreater(data["first_hour"]["remaining_ms"], 0)

    def test_progress_reports_first_hour_when_window_closes(self):
        with tempfile.TemporaryDirectory() as tmp:
            directory = Path(tmp) / "hour"
            write_run(directory, series(8_000_000, 10_000_000, 10_200_000, 10_400_000))
            result = self.run_checker(directory, extra=("--progress",))
            data = json.loads(result.stdout)
            self.assertTrue(data["first_hour"]["complete"])
            self.assertEqual(data["first_hour"]["median_rss_bytes"], 10_000_000)
            self.assertFalse(data["passed"])
            self.assertFalse(data["production_qualification"])

    def test_live_kl02_10_progress_is_not_a_pass(self):
        live = Path("/tmp/kl02-10-soak")
        if not (live / "frozen.json").is_file():
            self.skipTest("live KL02-10 output is not on this host")
        result = subprocess.run(
            ["python3", "-B", str(SCRIPT), str(live), "--progress"],
            capture_output=True, text=True, timeout=10,
        )
        self.assertEqual(result.returncode, 0, result.stderr)
        data = json.loads(result.stdout)
        self.assertEqual(data["report"], "progress")
        self.assertFalse(data["passed"])
        self.assertFalse(data["complete_24h"])
        self.assertGreater(data["elapsed_ms"], 0)
        self.assertGreater(data["rss_bytes"]["latest"], 0)


if __name__ == "__main__":
    unittest.main()

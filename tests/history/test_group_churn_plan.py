"""Offline KL03-21 group-protocol churn plan tests."""

import json
import sys
import unittest
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(REPO_ROOT / "scripts"))

from group_churn_cases import (  # noqa: E402
    DEFAULT_FREEZE,
    REQUIRED_CHURNS,
    REQUIRED_PROFILES,
    cell_by_id,
    cells,
    evaluate_cell,
    load_freeze,
    record_id,
    synthetic_history,
    validate_freeze,
)

CHECKER = REPO_ROOT / "scripts" / "check-record-history.py"


def _check(history: dict, path: Path):
    import subprocess

    path.write_text(json.dumps(history))
    return subprocess.run(
        [sys.executable, str(CHECKER), "--json", str(path)],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )


class TestGroupChurnPlan(unittest.TestCase):
    def setUp(self):
        self.freeze = load_freeze(DEFAULT_FREEZE)
        self.cells = cells(self.freeze)

    def test_freeze_has_four_profiles_and_five_churns(self):
        report = validate_freeze(self.freeze)
        self.assertTrue(report["ok"])
        self.assertEqual(tuple(report["profiles"]), REQUIRED_PROFILES)
        self.assertEqual(tuple(report["churns"]), REQUIRED_CHURNS)
        self.assertEqual(len(self.cells), 20)
        self.assertEqual(report["replay_cell"], "classic/member_loss")

    def test_timer_budgets_are_distinct(self):
        budgets = self.freeze["budgets"]
        values = [
            budgets["session_timeout_ms"],
            budgets["heartbeat_interval_ms"],
            budgets["max_poll_interval_ms"],
            budgets["share_lock_hold_ms"],
            budgets["pause_ms"],
            budgets["coordinator_recovery_ms"],
        ]
        self.assertEqual(len(values), len(set(values)))
        self.assertLess(budgets["heartbeat_interval_ms"], budgets["session_timeout_ms"])
        self.assertLess(budgets["max_poll_interval_ms"], budgets["session_timeout_ms"])

    def test_older_kafka_kip848_and_share_are_explicitly_unsupported(self):
        older = self.freeze["older_brokers"]["3.9.1"]
        self.assertIn("unsupported", older["kip848"].lower())
        self.assertIn("unsupported", older["share"].lower())

    def test_every_synthetic_history_passes_the_checker(self):
        for cell in self.cells:
            with self.subTest(cell=cell["id"]):
                proc = _check(
                    synthetic_history(cell),
                    Path(f"/tmp/kl03-21-plan-{cell['id'].replace('/', '-')}.json"),
                )
                self.assertEqual(proc.returncode, 0, proc.stderr)
                self.assertTrue(json.loads(proc.stdout).get("all_valid"))

    def test_duplicate_delivery_is_rejected(self):
        cell = cell_by_id("classic/member_loss", self.freeze)
        proc = _check(
            synthetic_history(cell, duplicate=True),
            Path("/tmp/kl03-21-plan-dup.json"),
        )
        self.assertNotEqual(proc.returncode, 0)
        self.assertFalse(json.loads(proc.stdout).get("all_valid", False))

    def test_evaluate_cell_requires_unique_complete_ids(self):
        cell = cell_by_id("classic/pause", self.freeze)
        ids = [record_id(i) for i in range(6)]
        ok = evaluate_cell(cell, ids, {"a": ["joined", "paused", "committed"]})
        self.assertTrue(ok["ok"])
        missing = evaluate_cell(cell, ids[:4], {"a": ["joined", "paused"]})
        self.assertFalse(missing["ok"])
        self.assertIn("missing_or_extra_committed_ids", missing["errors"])

    def test_classic_pass_is_not_a_share_cell(self):
        share = cell_by_id("share/member_loss", self.freeze)
        classic = cell_by_id("classic/member_loss", self.freeze)
        self.assertNotEqual(share["id"], classic["id"])
        self.assertEqual(share["protocol"], "share")
        self.assertEqual(classic["protocol"], "classic")


if __name__ == "__main__":
    unittest.main()

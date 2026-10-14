"""Offline KL03-19 campaign plan and history-contract tests."""

import json
import sys
import unittest
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(REPO_ROOT / "scripts"))

from data_plane_faults import (  # noqa: E402
    DEFAULT_FREEZE,
    FAULTS,
    fault_counts,
    load_freeze,
    plan_all,
    plan_seed,
    record_id,
    synthetic_history,
)

CHECKER = REPO_ROOT / "scripts" / "check-record-history.py"


class TestDataPlanePlan(unittest.TestCase):
    def setUp(self):
        self.freeze = load_freeze(DEFAULT_FREEZE)
        self.plans = plan_all(self.freeze)

    def test_freeze_declares_one_thousand_seeds(self):
        self.assertEqual(self.freeze["seeds"]["count"], 1000)
        self.assertEqual(len(self.plans), 1000)
        self.assertEqual(self.plans[0]["seed"], 0)
        self.assertEqual(self.plans[-1]["seed"], 999)

    def test_every_declared_fault_has_two_hundred_seeds(self):
        counts = fault_counts(self.plans)
        self.assertEqual(set(counts), set(FAULTS))
        for name, count in counts.items():
            self.assertEqual(count, 200, name)

    def test_fault_after_is_inside_the_record_window(self):
        for plan in self.plans:
            self.assertGreaterEqual(plan["fault_after"], 1)
            self.assertLess(plan["fault_after"], plan["records"])

    def test_replay_seed_is_inside_the_frozen_range(self):
        replay = int(self.freeze["replay_seed"])
        self.assertIn(replay, range(self.freeze["seeds"]["start"], self.freeze["seeds"]["end"] + 1))
        plan = plan_seed(replay, self.freeze)
        self.assertEqual(plan["seed"], replay)
        self.assertIn(plan["fault"], FAULTS)

    def test_same_seed_is_stable(self):
        first = plan_seed(42, self.freeze)
        second = plan_seed(42, self.freeze)
        self.assertEqual(first, second)

    def test_synthetic_history_passes_the_record_checker(self):
        import subprocess

        plan = plan_seed(7, self.freeze)
        history = synthetic_history(plan)
        path = Path("/tmp/kl03-19-plan-good.json")
        path.write_text(json.dumps(history))
        proc = subprocess.run(
            [sys.executable, str(CHECKER), "--json", str(path)],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
        self.assertEqual(proc.returncode, 0, proc.stderr)
        self.assertTrue(json.loads(proc.stdout).get("all_valid"))

    def test_swapped_history_is_rejected(self):
        import subprocess

        plan = plan_seed(7, self.freeze)
        history = synthetic_history(plan, swap=True)
        path = Path("/tmp/kl03-19-plan-swap.json")
        path.write_text(json.dumps(history))
        proc = subprocess.run(
            [sys.executable, str(CHECKER), "--json", str(path)],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
        self.assertNotEqual(proc.returncode, 0)
        body = json.loads(proc.stdout) if proc.stdout.strip() else {}
        self.assertFalse(body.get("all_valid", False))

    def test_record_ids_are_unique_across_one_seed(self):
        ids = [record_id(3, i) for i in range(3)]
        self.assertEqual(len(ids), len(set(ids)))
        self.assertTrue(all(i.startswith("s0003-r") for i in ids))


if __name__ == "__main__":
    unittest.main()

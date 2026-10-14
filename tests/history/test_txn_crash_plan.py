"""Offline KL03-20 transactional crash/fencing plan tests."""

import json
import sys
import unittest
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(REPO_ROOT / "scripts"))

from txn_crash_cases import (  # noqa: E402
    DEFAULT_FREEZE,
    REQUIRED_CRASHES,
    assemble_history,
    case_by_id,
    cases,
    evaluate_observation,
    load_freeze,
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


class TestTxnCrashPlan(unittest.TestCase):
    def setUp(self):
        self.freeze = load_freeze(DEFAULT_FREEZE)
        self.cases = cases(self.freeze)

    def test_freeze_names_required_crash_boundaries_and_fence(self):
        report = validate_freeze(self.freeze)
        self.assertTrue(report["ok"])
        self.assertEqual(set(report["crashes"]), REQUIRED_CRASHES)
        self.assertEqual(report["replay_case"], "fence_replacement")
        self.assertTrue(any(c.get("fence") for c in self.cases))
        self.assertTrue(any(c.get("abort") for c in self.cases))
        self.assertEqual(len(self.cases), 8)

    def test_transaction_timeout_is_below_leader_recovery(self):
        txn_ms = int(self.freeze["budgets"]["transaction_timeout_ms"])
        leader_ms = int(self.freeze["budgets"]["leader_recovery_ms"])
        self.assertLess(txn_ms, leader_ms)
        self.assertEqual(txn_ms, 15_000)
        self.assertEqual(leader_ms, 20_000)

    def test_replay_case_is_frozen(self):
        replay = case_by_id(self.freeze["replay_case"], self.freeze)
        self.assertTrue(replay.get("fence"))
        self.assertEqual(replay["expect_output"], "replacement_only")

    def test_every_synthetic_history_passes_the_checker(self):
        for case in self.cases:
            variants = [False]
            if case.get("expect_output") == "atomic":
                variants = [False, True]
            for committed in variants:
                with self.subTest(case=case["id"], atomic_committed=committed):
                    history = synthetic_history(case, atomic_committed=committed)
                    proc = _check(
                        history,
                        Path(f"/tmp/kl03-20-plan-{case['id']}-{int(committed)}.json"),
                    )
                    self.assertEqual(proc.returncode, 0, proc.stderr)
                    self.assertTrue(json.loads(proc.stdout).get("all_valid"))

    def test_leaked_aborted_output_is_rejected(self):
        case = case_by_id("abort_hides", self.freeze)
        history = synthetic_history(case, leak_aborted=True)
        proc = _check(history, Path("/tmp/kl03-20-plan-leak.json"))
        self.assertNotEqual(proc.returncode, 0)
        body = json.loads(proc.stdout) if proc.stdout.strip() else {}
        self.assertFalse(body.get("all_valid", False))

    def test_evaluate_observation_accepts_atomic_either_outcome(self):
        case = case_by_id("crash_during_endtxn", self.freeze)
        committed = evaluate_observation(case, ["anchor", "out-in-0", "out-in-1"], 2)
        aborted = evaluate_observation(case, ["anchor"], -1)
        split = evaluate_observation(case, ["anchor", "out-in-0", "out-in-1"], -1)
        self.assertTrue(committed["ok"])
        self.assertTrue(aborted["ok"])
        self.assertFalse(split["ok"])
        self.assertIn("output_without_offsets", split["errors"])

    def test_evaluate_observation_hides_aborted_and_fenced_records(self):
        abort = case_by_id("abort_hides", self.freeze)
        fence = case_by_id("fence_replacement", self.freeze)
        self.assertTrue(evaluate_observation(abort, ["anchor"], -1)["ok"])
        self.assertFalse(evaluate_observation(abort, ["anchor", "out-in-0"], -1)["ok"])
        self.assertTrue(evaluate_observation(fence, ["anchor", "fence-out"], -1)["ok"])
        self.assertFalse(evaluate_observation(fence, ["anchor", "out-fenced", "fence-out"], -1)["ok"])

    def test_assemble_history_keeps_committed_offsets_with_output(self):
        case = case_by_id("happy_commit", self.freeze)
        consumed = [
            {
                "id": "anchor",
                "topic": "out",
                "partition": 0,
                "offset": 0,
                "key": "anchor",
                "payload": "anchor",
                "status": "acked",
            },
            {
                "id": "out-in-0",
                "topic": "out",
                "partition": 0,
                "offset": 1,
                "key": "in-0",
                "payload": "out-in-0",
                "status": "acked",
            },
            {
                "id": "out-in-1",
                "topic": "out",
                "partition": 0,
                "offset": 2,
                "key": "in-1",
                "payload": "out-in-1",
                "status": "acked",
            },
        ]
        history = assemble_history(
            case,
            output_topic="out",
            input_topic="in",
            txn_id="txn",
            consumed=consumed,
            committed_offset=2,
        )
        self.assertEqual(history["transactions"][0]["status"], "committed")
        self.assertEqual(history["offset_commits"][0]["offset"], 2)
        proc = _check(history, Path("/tmp/kl03-20-plan-assemble.json"))
        self.assertEqual(proc.returncode, 0, proc.stderr)


if __name__ == "__main__":
    unittest.main()

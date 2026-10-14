"""Offline KL01-22 fetch delivery regression plan tests."""

import importlib.util
import json
import sys
import unittest
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent.parent
GENERATOR = REPO_ROOT / "tests" / "conformance" / "java" / "generate_fetch_delivery_regressions.py"
CASES = REPO_ROOT / "tests" / "conformance" / "cases.json"
AUDIT_PIN = "cb7e97d3b92a8555aea34d59266a2990c206395f"
REGISTERED = (
    "audit-regression-a01-offset-filter",
    "audit-regression-a02-abort-interval",
    "audit-regression-a03-partial-failure",
    "audit-regression-a04-offset-out-of-range",
    "audit-regression-a05-cursor-commit",
)


def load_generator():
    spec = importlib.util.spec_from_file_location("generate_fetch_delivery_regressions", GENERATOR)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class TestFetchDeliveryRegressionsPlan(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.gen = load_generator()

    def test_registered_cohort_is_exactly_the_declared_five(self):
        self.assertEqual(list(self.gen.REGISTERED_CASE_IDS), list(REGISTERED))
        cases = json.loads(CASES.read_text())
        ids = {row["id"] for row in cases["cases"]}
        for case_id in self.gen.REGISTERED_CASE_IDS:
            self.assertIn(case_id, ids, case_id)

    def test_original_failed_dispositions_are_not_rewritten(self):
        cases = json.loads(CASES.read_text())
        by_id = {row["id"]: row for row in cases["cases"]}
        for case_id in REGISTERED:
            row = by_id[case_id]
            self.assertEqual(row["disposition"], "failed", case_id)
            self.assertEqual(row["source_pin"], AUDIT_PIN, case_id)
            self.assertEqual(row["immutable_source_pin"], AUDIT_PIN, case_id)
            self.assertEqual(row["peer_version_pin"], "4.1.0", case_id)
            self.assertEqual(row["adapter"], "audit-probe", case_id)
            self.assertIn("KL01-22", row.get("backlog_tasks", []), case_id)

    def test_finite_named_cells_and_zero_unsupported(self):
        for release in self.gen.PINS:
            self.assertEqual(self.gen.expected_bodies(release), 15)
            self.assertEqual(self.gen.expected_unsupported(release), 0)
        cells = [row["cell"] for row in self.gen.expected_cells("4.1.0")]
        self.assertEqual(
            cells,
            [
                "a01-offset-filter",
                "a02-abort-interval",
                "a04-offset-out-of-range",
                "a05-cursor-commit",
                "a05-offset-commit",
                "a05-offset-fetch",
                "a03-partial-failure",
            ],
        )
        self.assertEqual(self.gen.FETCH_VERSION, 12)
        self.assertEqual(self.gen.OFFSET_COMMIT_VERSION, 8)
        self.assertEqual(self.gen.OFFSET_FETCH_VERSION, 8)

    def test_pins_and_source_commits_are_named(self):
        self.assertEqual(len(self.gen.PINS), 5)
        self.assertEqual(len(self.gen.SOURCE_COMMITS), 5)
        self.assertEqual(
            self.gen.PINS["4.1.0"],
            "eb74709dc3be35e06208827860c87a94872e8a4b00f466880670785c85a20627",
        )
        self.assertTrue(self.gen.SOURCE.exists())


if __name__ == "__main__":
    sys.exit(unittest.main())

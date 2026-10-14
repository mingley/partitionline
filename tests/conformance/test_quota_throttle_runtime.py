"""Offline KL01-25 Produce/Fetch quota throttle runtime plan tests."""

import importlib.util
import json
import sys
import unittest
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent.parent
GENERATOR = REPO_ROOT / "tests" / "conformance" / "java" / "generate_quota_throttle_runtime.py"
CASES = REPO_ROOT / "tests" / "conformance" / "cases.json"
AUDIT_PIN = "cb7e97d3b92a8555aea34d59266a2990c206395f"
REGISTERED = ("feature-produce-fetch-quota-throttle-runtime",)


def load_generator():
    spec = importlib.util.spec_from_file_location("generate_quota_throttle_runtime", GENERATOR)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class TestQuotaThrottleRuntimePlan(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.gen = load_generator()

    def test_registered_cohort_is_exactly_the_declared_cell(self):
        self.assertEqual(list(self.gen.REGISTERED_CASE_IDS), list(REGISTERED))
        cases = json.loads(CASES.read_text())
        ids = {row["id"] for row in cases["cases"]}
        for case_id in self.gen.REGISTERED_CASE_IDS:
            self.assertIn(case_id, ids, case_id)

    def test_original_not_run_disposition_is_not_rewritten(self):
        cases = json.loads(CASES.read_text())
        by_id = {row["id"]: row for row in cases["cases"]}
        row = by_id["feature-produce-fetch-quota-throttle-runtime"]
        self.assertEqual(row["disposition"], "not_run")
        self.assertEqual(row["source_pin"], AUDIT_PIN)
        self.assertEqual(row["immutable_source_pin"], AUDIT_PIN)
        self.assertEqual(row["peer_version_pin"], "4.1.0")
        self.assertEqual(row["adapter"], "none")
        self.assertIn("KL01-25", row.get("backlog_tasks", []))

    def test_finite_named_cells_and_zero_unsupported(self):
        for release in self.gen.PINS:
            self.assertEqual(self.gen.expected_bodies(release), 8)
            self.assertEqual(self.gen.expected_unsupported(release), 0)
        cells = [row["cell"] for row in self.gen.expected_cells("4.1.0")]
        self.assertEqual(
            cells,
            [
                "p01-produce-zero",
                "p02-produce-quota",
                "p03-produce-negative",
                "f01-fetch-zero",
                "f02-fetch-quota",
                "f03-empty-throttled-full",
                "produce-v0-v2-removed",
                "produce-v3-v5-server-side-throttle",
                "fetch-v1-v7-pre-client-quota",
                "share-group-share-fetch",
                "kip-848-consumer-group-heartbeat",
            ],
        )
        nonapplicable = [
            row["cell"] for row in self.gen.expected_cells("3.9.1") if row["disposition"] == "nonapplicable"
        ]
        self.assertEqual(
            nonapplicable,
            [
                "produce-v0-v2-removed",
                "produce-v3-v5-server-side-throttle",
                "fetch-v1-v7-pre-client-quota",
                "share-group-share-fetch",
                "kip-848-consumer-group-heartbeat",
            ],
        )
        self.assertEqual(self.gen.PRODUCE_VERSION, 9)
        self.assertEqual(self.gen.FETCH_VERSION, 12)
        self.assertEqual(self.gen.TOPIC, "pl-quota")
        self.assertEqual(self.gen.SESSION_ID, 91)
        self.assertEqual(self.gen.PRODUCE_QUOTA_MS, 250)
        self.assertEqual(self.gen.FETCH_QUOTA_MS, 80)

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

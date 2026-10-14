"""Offline KL01-29 share-offset alter/delete plan tests."""

import importlib.util
import json
import sys
import unittest
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent.parent
GENERATOR = REPO_ROOT / "tests" / "conformance" / "java" / "generate_share_offset_mutations.py"
CASES = REPO_ROOT / "tests" / "conformance" / "cases.json"
AUDIT_PIN = "f0ca05da9c04942b211e2f56ddc615b9860cdea2"
REGISTERED = (
    "current-public-api-091-v0-4-1-2",
    "current-public-api-091-v0-4-2-1",
    "current-public-api-091-v0-4-3-1",
    "current-public-api-092-v0-4-1-2",
    "current-public-api-092-v0-4-2-1",
    "current-public-api-092-v0-4-3-1",
)


def load_generator():
    spec = importlib.util.spec_from_file_location("generate_share_offset_mutations", GENERATOR)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class TestShareOffsetMutationsPlan(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.gen = load_generator()

    def test_registered_cohort_is_exactly_the_declared_cells(self):
        self.assertEqual(list(self.gen.REGISTERED_CASE_IDS), list(REGISTERED))
        cases = json.loads(CASES.read_text())
        ids = {row["id"] for row in cases["cases"]}
        for case_id in self.gen.REGISTERED_CASE_IDS:
            self.assertIn(case_id, ids, case_id)

    def test_original_not_run_dispositions_are_not_rewritten(self):
        cases = json.loads(CASES.read_text())
        by_id = {row["id"]: row for row in cases["cases"]}
        for case_id in REGISTERED:
            row = by_id[case_id]
            self.assertEqual(row["disposition"], "not_run", case_id)
            self.assertEqual(row["source_pin"], AUDIT_PIN, case_id)
            self.assertEqual(row["immutable_source_pin"], AUDIT_PIN, case_id)
            self.assertEqual(row["adapter"], "none", case_id)
            self.assertIn("KL01-29", row.get("backlog_tasks", []), case_id)
            self.assertEqual(row["api_version"], 0, case_id)

    def test_finite_named_cells_and_historical_unsupported(self):
        self.assertEqual(self.gen.expected_bodies("4.1.2"), 8)
        self.assertEqual(self.gen.expected_unsupported("4.1.2"), 0)
        self.assertEqual(self.gen.expected_bodies("3.9.1"), 0)
        self.assertEqual(self.gen.expected_unsupported("3.9.1"), 8)
        cells = [row["cell"] for row in self.gen.expected_cells("4.1.2")]
        self.assertEqual(
            cells,
            [
                "alter-ok",
                "alter-invalid",
                "delete-ok",
                "delete-unknown",
                "describe-share-group-offsets",
                "share-fetch-acknowledge",
                "share-group-heartbeat",
                "ducktape-live-broker",
                "not-coordinator-public-retry-loop",
            ],
        )
        unsupported = [
            row["cell"] for row in self.gen.expected_cells("3.9.1") if row["disposition"] == "unsupported"
        ]
        self.assertEqual(unsupported, list(self.gen.NAMED_PAIRS))
        self.assertEqual(self.gen.ALTER_VERSION, 0)
        self.assertEqual(self.gen.DELETE_VERSION, 0)
        self.assertEqual(self.gen.TOPIC, "pl-share")
        self.assertEqual(self.gen.GROUP, "pl-share")

    def test_pins_and_source_commits_are_named(self):
        self.assertEqual(len(self.gen.PINS), 5)
        self.assertEqual(len(self.gen.SOURCE_COMMITS), 5)
        self.assertEqual(list(self.gen.SPEAKING), ["4.1.0", "4.1.2", "4.2.1", "4.3.1"])
        self.assertTrue(self.gen.SOURCE.exists())


if __name__ == "__main__":
    sys.exit(unittest.main())

"""Offline KL01-24 Fetch session runtime plan tests."""

import importlib.util
import json
import sys
import unittest
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent.parent
GENERATOR = REPO_ROOT / "tests" / "conformance" / "java" / "generate_fetch_session_runtime.py"
CASES = REPO_ROOT / "tests" / "conformance" / "cases.json"
AUDIT_PIN = "cb7e97d3b92a8555aea34d59266a2990c206395f"
REGISTERED = ("feature-fetch-incremental-session-runtime",)


def load_generator():
    spec = importlib.util.spec_from_file_location("generate_fetch_session_runtime", GENERATOR)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class TestFetchSessionRuntimePlan(unittest.TestCase):
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
        row = by_id["feature-fetch-incremental-session-runtime"]
        self.assertEqual(row["disposition"], "not_run")
        self.assertEqual(row["source_pin"], AUDIT_PIN)
        self.assertEqual(row["immutable_source_pin"], AUDIT_PIN)
        self.assertEqual(row["peer_version_pin"], "4.1.0")
        self.assertEqual(row["adapter"], "none")
        self.assertIn("KL01-24", row.get("backlog_tasks", []))

    def test_finite_named_cells_and_zero_unsupported(self):
        for release in self.gen.PINS:
            self.assertEqual(self.gen.expected_bodies(release), 10)
            self.assertEqual(self.gen.expected_unsupported(release), 0)
        cells = [row["cell"] for row in self.gen.expected_cells("4.1.0")]
        self.assertEqual(
            cells,
            [
                "s01-full",
                "s02-incremental",
                "s03-forgotten",
                "s04-not-found",
                "s05-after-not-found",
                "s06-invalid-epoch",
                "s07-after-invalid-epoch",
                "fetch-v4-v6-legacy",
                "fetch-v13-topic-ids",
                "share-v2-fetch-session-reset",
            ],
        )
        nonapplicable = [
            row["cell"] for row in self.gen.expected_cells("3.9.1") if row["disposition"] == "nonapplicable"
        ]
        self.assertEqual(
            nonapplicable,
            ["fetch-v4-v6-legacy", "fetch-v13-topic-ids", "share-v2-fetch-session-reset"],
        )
        self.assertEqual(self.gen.FETCH_VERSION, 12)
        self.assertEqual(self.gen.TOPIC, "pl-session")
        self.assertEqual(self.gen.SESSION_ID, 91)

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

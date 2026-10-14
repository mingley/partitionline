"""Offline KL01-27 verifiable Consumer reset/autocommit plan tests."""

import importlib.util
import json
import sys
import unittest
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent.parent
GENERATOR = REPO_ROOT / "tests" / "conformance" / "java" / "generate_verifiable_consumer_offsets.py"
CASES = REPO_ROOT / "tests" / "conformance" / "cases.json"
AUDIT_PIN = "3e1803560e081d44f8e5ae4a3e4b449f69ff0a54"
REGISTERED = (
    "verifiable-consumer-reset-policies-3-9-1",
    "verifiable-consumer-autocommit-3-9-1",
)


def load_generator():
    spec = importlib.util.spec_from_file_location("generate_verifiable_consumer_offsets", GENERATOR)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class TestVerifiableConsumerOffsetsPlan(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.gen = load_generator()

    def test_registered_cohort_is_exactly_the_declared_cells(self):
        self.assertEqual(list(self.gen.REGISTERED_CASE_IDS), list(REGISTERED))
        cases = json.loads(CASES.read_text())
        ids = {row["id"] for row in cases["cases"]}
        for case_id in self.gen.REGISTERED_CASE_IDS:
            self.assertIn(case_id, ids, case_id)

    def test_original_local_consistency_dispositions_are_not_rewritten(self):
        cases = json.loads(CASES.read_text())
        by_id = {row["id"]: row for row in cases["cases"]}
        for case_id in REGISTERED:
            row = by_id[case_id]
            self.assertEqual(row["disposition"], "local_consistency", case_id)
            self.assertEqual(row["source_pin"], AUDIT_PIN, case_id)
            self.assertEqual(row["immutable_source_pin"], AUDIT_PIN, case_id)
            self.assertEqual(row["peer_version_pin"], "3.9.1", case_id)
            self.assertEqual(row["adapter"], "verifiable-scenario", case_id)
            self.assertIn("KL01-27", row.get("backlog_tasks", []), case_id)

    def test_finite_named_cells_and_zero_unsupported(self):
        for release in self.gen.PINS:
            self.assertEqual(self.gen.expected_bodies(release), 16)
            self.assertEqual(self.gen.expected_unsupported(release), 0)
        cells = [row["cell"] for row in self.gen.expected_cells("4.1.0")]
        self.assertEqual(
            cells,
            [
                "r01-latest-list-offsets",
                "r02-offset-fetch-empty",
                "r03-offset-fetch-stored",
                "r04-latest-fetch-new",
                "r05-none-resume-fetch",
                "a01-autocommit-fetch-first",
                "r06-latest-commit-next-2",
                "a02-autocommit-interval-commit-1",
                "e01-events",
                "earliest-reset-default-only",
                "message-create-time-elapsed-runtime",
                "command-config-4-2-plus",
                "broker-list-removed-4-2",
                "kip-848-consumer-protocol",
                "share-group",
                "verifiable-producer-options",
                "ducktape-live-broker",
            ],
        )
        nonapplicable = [
            row["cell"] for row in self.gen.expected_cells("3.9.1") if row["disposition"] == "nonapplicable"
        ]
        self.assertEqual(nonapplicable, list(self.gen.NONAPPLICABLE_CELLS))
        self.assertEqual(self.gen.FETCH_VERSION, 12)
        self.assertEqual(self.gen.LIST_OFFSETS_VERSION, 5)
        self.assertEqual(self.gen.OFFSET_COMMIT_VERSION, 8)
        self.assertEqual(self.gen.OFFSET_FETCH_VERSION, 8)
        self.assertEqual(self.gen.TOPIC, "pl-offsets")
        self.assertEqual(self.gen.GROUP, "pl-offsets")

    def test_pins_and_source_commits_are_named(self):
        self.assertEqual(len(self.gen.PINS), 5)
        self.assertEqual(len(self.gen.SOURCE_COMMITS), 5)
        self.assertEqual(
            self.gen.PINS["3.9.1"],
            "7568b998572d256f0b7bc0afdc1b7a2588b8b08415c62ce314c864a6851ae9d9",
        )
        self.assertTrue(self.gen.SOURCE.exists())


if __name__ == "__main__":
    sys.exit(unittest.main())

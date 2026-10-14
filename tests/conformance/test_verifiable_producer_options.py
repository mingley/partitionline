"""Offline KL01-26 verifiable Producer option-profile plan tests."""

import importlib.util
import json
import sys
import unittest
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent.parent
GENERATOR = REPO_ROOT / "tests" / "conformance" / "java" / "generate_verifiable_producer_options.py"
CASES = REPO_ROOT / "tests" / "conformance" / "cases.json"
AUDIT_PIN = "3e1803560e081d44f8e5ae4a3e4b449f69ff0a54"
REGISTERED = ("verifiable-producer-options-3-9-1",)


def load_generator():
    spec = importlib.util.spec_from_file_location("generate_verifiable_producer_options", GENERATOR)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class TestVerifiableProducerOptionsPlan(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.gen = load_generator()

    def test_registered_cohort_is_exactly_the_declared_cell(self):
        self.assertEqual(list(self.gen.REGISTERED_CASE_IDS), list(REGISTERED))
        cases = json.loads(CASES.read_text())
        ids = {row["id"] for row in cases["cases"]}
        for case_id in self.gen.REGISTERED_CASE_IDS:
            self.assertIn(case_id, ids, case_id)

    def test_original_local_consistency_disposition_is_not_rewritten(self):
        cases = json.loads(CASES.read_text())
        by_id = {row["id"]: row for row in cases["cases"]}
        row = by_id["verifiable-producer-options-3-9-1"]
        self.assertEqual(row["disposition"], "local_consistency")
        self.assertEqual(row["source_pin"], AUDIT_PIN)
        self.assertEqual(row["immutable_source_pin"], AUDIT_PIN)
        self.assertEqual(row["peer_version_pin"], "3.9.1")
        self.assertEqual(row["adapter"], "verifiable-scenario")
        self.assertIn("KL01-26", row.get("backlog_tasks", []))

    def test_finite_named_cells_and_zero_unsupported(self):
        for release in self.gen.PINS:
            self.assertEqual(self.gen.expected_bodies(release), 8)
            self.assertEqual(self.gen.expected_unsupported(release), 0)
        cells = [row["cell"] for row in self.gen.expected_cells("4.1.0")]
        self.assertEqual(
            cells,
            [
                "o01-profile-batch",
                "o02-per-send",
                "o03-events",
                "message-create-time-elapsed-runtime",
                "throughput-0-unthrottled",
                "max-messages-until-killed",
                "command-config-4-2-plus",
                "broker-list-removed-4-2",
                "verifiable-consumer-earliest",
                "ducktape-live-broker",
            ],
        )
        nonapplicable = [
            row["cell"] for row in self.gen.expected_cells("3.9.1") if row["disposition"] == "nonapplicable"
        ]
        self.assertEqual(nonapplicable, list(self.gen.NONAPPLICABLE_CELLS))
        self.assertEqual(self.gen.PRODUCE_VERSION, 9)
        self.assertEqual(self.gen.TOPIC, "pl-options")
        self.assertEqual(self.gen.RECORD_COUNT, 6)
        self.assertEqual(self.gen.THROUGHPUT, 5)
        self.assertEqual(self.gen.VALUE_PREFIX, 42)
        self.assertEqual(self.gen.REPEATING_KEYS, 2)
        self.assertEqual(self.gen.CREATE_TIME, 1_650_000_000_000)

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

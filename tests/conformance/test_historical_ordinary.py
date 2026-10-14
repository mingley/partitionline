"""Offline KL01-23 historical ordinary plan tests."""

import importlib.util
import json
import sys
import unittest
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent.parent
GENERATOR = REPO_ROOT / "tests" / "conformance" / "java" / "generate_historical_ordinary.py"
CASES = REPO_ROOT / "tests" / "conformance" / "cases.json"
AUDIT_PIN = "cb7e97d3b92a8555aea34d59266a2990c206395f"
REGISTERED = (
    "broker-cell-historical-apache-3-9-1",
    "broker-cell-historical-apache-4-1-0",
    "verifiable-produce-consume-3-9-1",
)
PEERS = {
    "broker-cell-historical-apache-3-9-1": ("3.9.1", "live-broker"),
    "broker-cell-historical-apache-4-1-0": ("4.1.0", "live-broker"),
    "verifiable-produce-consume-3-9-1": ("3.9.1", "verifiable-scenario"),
}


def load_generator():
    spec = importlib.util.spec_from_file_location("generate_historical_ordinary", GENERATOR)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class TestHistoricalOrdinaryPlan(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.gen = load_generator()

    def test_registered_cohort_is_exactly_the_declared_three(self):
        self.assertEqual(list(self.gen.REGISTERED_CASE_IDS), list(REGISTERED))
        cases = json.loads(CASES.read_text())
        ids = {row["id"] for row in cases["cases"]}
        for case_id in self.gen.REGISTERED_CASE_IDS:
            self.assertIn(case_id, ids, case_id)

    def test_original_local_consistency_dispositions_are_not_rewritten(self):
        cases = json.loads(CASES.read_text())
        by_id = {row["id"]: row for row in cases["cases"]}
        for case_id, (peer, adapter) in PEERS.items():
            row = by_id[case_id]
            self.assertEqual(row["disposition"], "local_consistency", case_id)
            self.assertEqual(row["source_pin"], AUDIT_PIN, case_id)
            self.assertEqual(row["immutable_source_pin"], AUDIT_PIN, case_id)
            self.assertEqual(row["peer_version_pin"], peer, case_id)
            self.assertEqual(row["adapter"], adapter, case_id)
            self.assertIn("KL01-23", row.get("backlog_tasks", []), case_id)

    def test_finite_named_cells_and_zero_unsupported(self):
        for release in self.gen.PINS:
            self.assertEqual(self.gen.expected_bodies(release), 8)
            self.assertEqual(self.gen.expected_unsupported(release), 0)
        cells = [row["cell"] for row in self.gen.expected_cells("4.1.0")]
        self.assertEqual(
            cells,
            [
                "produce",
                "fetch",
                "list-offsets",
                "metadata",
                "verifiable-events",
                "kip-848-consumer-group-heartbeat",
                "share-group-share-fetch",
            ],
        )
        nonapplicable = [row["cell"] for row in self.gen.expected_cells("3.9.1") if row["disposition"] == "nonapplicable"]
        self.assertEqual(nonapplicable, ["kip-848-consumer-group-heartbeat", "share-group-share-fetch"])
        self.assertEqual(self.gen.PRODUCE_VERSION, 9)
        self.assertEqual(self.gen.FETCH_VERSION, 12)
        self.assertEqual(self.gen.LIST_OFFSETS_VERSION, 5)
        self.assertEqual(self.gen.METADATA_VERSION, 9)
        self.assertEqual(self.gen.TOPIC, "pl-ordinary")
        self.assertEqual(self.gen.RECORD_COUNT, 25)

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

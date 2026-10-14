"""Offline KL01-18 Produce wire plan tests."""

import importlib.util
import json
import sys
import unittest
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent.parent
GENERATOR = REPO_ROOT / "tests" / "conformance" / "java" / "generate_produce_wire.py"
CASES = REPO_ROOT / "tests" / "conformance" / "cases.json"


def load_generator():
    spec = importlib.util.spec_from_file_location("generate_produce_wire", GENERATOR)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class TestProduceWirePlan(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.gen = load_generator()

    def test_registered_cohort_is_exactly_the_declared_fifteen(self):
        self.assertEqual(len(self.gen.REGISTERED_CASE_IDS), 15)
        cases = json.loads(CASES.read_text())
        ids = {row["id"] for row in cases["cases"]}
        for case_id in self.gen.REGISTERED_CASE_IDS:
            self.assertIn(case_id, ids, case_id)

    def test_original_dispositions_are_not_rewritten(self):
        cases = json.loads(CASES.read_text())
        by_id = {row["id"]: row for row in cases["cases"]}
        for case_id in (
            "proto-produce-v03-local",
            "proto-produce-v12-local",
            "matrix-cell-produce-3-9-1",
            "matrix-cell-produce-4-1-0",
        ):
            self.assertEqual(by_id[case_id]["disposition"], "local_consistency")
        self.assertEqual(by_id["proto-diff-produce-v12-apache-3-9-1"]["disposition"], "unsupported")
        self.assertEqual(by_id["proto-diff-produce-v13-apache-3-9-1"]["disposition"], "unsupported")
        self.assertEqual(by_id["proto-differential-produce-apache-java"]["disposition"], "blocked")

    def test_finite_named_cells_and_historical_refusals(self):
        self.assertEqual(self.gen.expected_bodies("3.9.1"), 60)
        self.assertEqual(self.gen.expected_unsupported("3.9.1"), 2)
        for release in ("4.1.0", "4.1.2", "4.2.1", "4.3.1"):
            self.assertEqual(self.gen.expected_bodies(release), 76)
            self.assertEqual(self.gen.expected_unsupported(release), 0)
        cells = {row["version"] for row in self.gen.expected_cells("3.9.1") if row["cell"] == "unsupported"}
        self.assertEqual(cells, {12, 13})
        tagged = [row for row in self.gen.expected_cells("4.1.2") if row["cell"] == "tagged"]
        self.assertEqual([row["version"] for row in tagged], [9, 10, 11, 12, 13])

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

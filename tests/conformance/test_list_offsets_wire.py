"""Offline KL01-21 ListOffsets wire plan tests."""

import importlib.util
import json
import sys
import unittest
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent.parent
GENERATOR = REPO_ROOT / "tests" / "conformance" / "java" / "generate_list_offsets_wire.py"
CASES = REPO_ROOT / "tests" / "conformance" / "cases.json"


def load_generator():
    spec = importlib.util.spec_from_file_location("generate_list_offsets_wire", GENERATOR)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class TestListOffsetsWirePlan(unittest.TestCase):
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
            "proto-list-offsets-v01-local",
            "proto-list-offsets-v09-local",
            "proto-list-offsets-v10-local",
            "matrix-cell-listoffsets-3-9-1",
            "matrix-cell-listoffsets-4-1-0",
        ):
            self.assertEqual(by_id[case_id]["disposition"], "local_consistency")
        self.assertEqual(by_id["proto-diff-list-offsets-v10-apache-3-9-1"]["disposition"], "unsupported")
        self.assertEqual(by_id["proto-unsupported-list-offsets-v11-client-cap"]["disposition"], "unsupported")
        self.assertEqual(by_id["proto-differential-list-offsets-apache-java"]["disposition"], "blocked")

    def test_finite_named_cells_and_historical_refusals(self):
        self.assertEqual(self.gen.expected_bodies("3.9.1"), 62)
        self.assertEqual(self.gen.expected_unsupported("3.9.1"), 2)
        self.assertEqual(self.gen.expected_bodies("4.1.0"), 70)
        self.assertEqual(self.gen.expected_unsupported("4.1.0"), 1)
        self.assertEqual(self.gen.expected_bodies("4.1.2"), 70)
        self.assertEqual(self.gen.expected_unsupported("4.1.2"), 1)
        for release in ("4.2.1", "4.3.1"):
            self.assertEqual(self.gen.expected_bodies(release), 78)
            self.assertEqual(self.gen.expected_unsupported(release), 0)
        cells = {row["version"] for row in self.gen.expected_cells("3.9.1") if row["cell"] == "unsupported"}
        self.assertEqual(cells, {10, 11})
        cells_410 = {row["version"] for row in self.gen.expected_cells("4.1.0") if row["cell"] == "unsupported"}
        self.assertEqual(cells_410, {11})
        tagged = [row for row in self.gen.expected_cells("4.3.1") if row["cell"] == "tagged"]
        self.assertEqual([row["version"] for row in tagged], [6, 7, 8, 9, 10, 11])

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

"""The broker compatibility matrix keeps every pinned cell."""

import importlib.util
import json
from pathlib import Path
import unittest

ROOT = Path(__file__).resolve().parents[2]
spec = importlib.util.spec_from_file_location(
    "broker_compatibility", ROOT / "scripts" / "check-broker-compatibility.py"
)
checker = importlib.util.module_from_spec(spec)
spec.loader.exec_module(checker)


class BrokerCompatibilityMatrix(unittest.TestCase):
    def test_committed_matrix_matches_the_full_denominator(self):
        document = json.loads(checker.OUT.read_text())
        checker.check(document)
        self.assertGreater(document["denominator"], document["broker_denominator"])
        self.assertTrue(all(cell["status"] != "passed" for cell in document["broker_cells"]))
        self.assertTrue(all(cell["blocking"] for cell in document["client_cells"]))

    def test_dropping_a_required_cell_fails(self):
        document = checker.build()
        document["broker_cells"] = document["broker_cells"][1:]
        document["broker_denominator"] -= 1
        document["denominator"] -= 1
        with self.assertRaises(ValueError):
            checker.check(document)


if __name__ == "__main__":
    unittest.main()

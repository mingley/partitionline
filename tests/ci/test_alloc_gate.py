"""The hosted census gate must preserve failures and reject incomplete runs."""
import os
from pathlib import Path
import subprocess
import tempfile
import unittest

ROOT = Path(__file__).resolve().parents[2]


class AllocationGate(unittest.TestCase):
    def run_gate(self, output, code):
        with tempfile.TemporaryDirectory() as temporary:
            task = Path(temporary)
            cargo = task / "cargo"
            cargo.write_text("#!/usr/bin/env python3\nimport sys\n"
                             "if sys.argv[1:] == ['-V']: print('fake unit-test cargo'); sys.exit(0)\n"
                             f"print({output!r})\nsys.exit({code})\n", encoding="utf-8")
            cargo.chmod(0o755)
            report = task / "report"
            env = dict(os.environ, PATH=str(task) + os.pathsep + os.environ["PATH"],
                       PL_ALLOC_REPORT_DIR=str(report))
            process = subprocess.run(["bash", "scripts/ci-alloc-budget.sh"], cwd=ROOT,
                                     env=env, capture_output=True, text=True)
            self.assertTrue((report / "identity.log").is_file())
            self.assertEqual((report / "tests.log").read_text(encoding="utf-8"), output + "\n")
            return process

    def test_failure_retains_status_and_escapes_annotation(self):
        result = self.run_gate("actual budget: 100%\nFAIL census", 42)
        self.assertEqual(result.returncode, 42)
        self.assertIn("::error title=Allocation gate::", result.stdout)
        self.assertIn("100%25%0AFAIL", result.stdout)

    def test_incomplete_success_fails(self):
        result = self.run_gate("test result: ok. 0 passed; 0 failed; 0 ignored;", 0)
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("Incomplete allocation-gate execution", result.stdout)

    def test_both_actual_tests_required(self):
        summary = "test result: ok. 1 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.01s"
        result = self.run_gate("test alloc_budgets ... ok\n" + summary + "\n"
                               "test seeded_json1k_payloads_and_allocation_baselines ... ok\n" + summary, 0)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertNotIn("::error", result.stdout)


if __name__ == "__main__":
    unittest.main()

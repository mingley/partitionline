"""Case identity guards and the distinction between Rust proof and peer servers."""
import copy
import importlib.util
import json
import os
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
SPEC = importlib.util.spec_from_file_location('capability_cases', ROOT / 'scripts/client-capability-cases.py')
CASES = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(CASES)
INVENTORY = ROOT / 'tests/conformance/client-capability-cases.json'


class CapabilityCaseTests(unittest.TestCase):
    def setUp(self):
        self.suites = CASES.load_suites(INVENTORY)

    def test_current_inventory_requires_all_93_semantic_cases_and_lists_both_external_servers(self):
        self.assertEqual(sum(len(s['required_cases']) for s in self.suites), 93)
        self.assertEqual(sum(len(s['external_probes']) for s in self.suites), 2)
        for suite in self.suites:
            discovered = sorted(suite['required_cases'] + suite['external_probes'])
            self.assertEqual(CASES.selected_names(suite, discovered), sorted(suite['required_cases']))

    def test_equal_count_renaming_does_not_satisfy_coverage(self):
        for suite in self.suites:
            discovered = suite['required_cases'] + suite['external_probes']
            changed = ['renamed_required_case'] + discovered[1:]
            with self.subTest(suite=suite['label']), self.assertRaises(CASES.CaseInventoryError):
                CASES.selected_names(suite, changed)

    def test_missing_extra_and_duplicate_cases_all_fail_discovery(self):
        suite = self.suites[0]
        original = suite['required_cases'] + suite['external_probes']
        for changed in (original[1:], original + ['new_unaudited_case'], original + original[:1]):
            with self.subTest(discovered=changed), self.assertRaises(CASES.CaseInventoryError):
                CASES.selected_names(suite, changed)

    def test_external_server_names_must_also_match_discovery(self):
        suite = self.suites[0]
        for changed in (suite['required_cases'],
                        suite['required_cases'] + suite['external_probes'] + ['unexpected_server']):
            with self.subTest(discovered=changed), self.assertRaises(CASES.CaseInventoryError):
                CASES.selected_names(suite, changed)

    def execution(self, lines, summary='ok. 2 passed; 0 failed; 0 ignored;', exit_code=0, errors=b''):
        text = '\n'.join(lines + ['test result: ' + summary]) + '\n'
        return CASES.verify_execution({'name': 'control', 'exit_code': exit_code},
                                      text.encode(), errors, ['case_a', 'case_b'])

    def test_execution_requires_each_selected_identity_once(self):
        original = ['test case_a ... ok', 'test case_b ... ok']
        self.assertTrue(self.execution(original)['passed'])
        for lines in (original[:1], original + original[:1],
                      ['test renamed_case ... ok', original[1]], [original[0], original[0]]):
            with self.subTest(lines=lines), self.assertRaises(CASES.CaseExecutionError):
                self.execution(lines)

    def test_required_ignored_failed_or_nonzero_exit_never_pass(self):
        self.assertFalse(self.execution(['test case_a ... ok', 'test case_b ... ignored'],
                                        'ok. 1 passed; 0 failed; 1 ignored;')['passed'])
        self.assertFalse(self.execution(['test case_a ... ok', 'test case_b ... FAILED'],
                                        'FAILED. 1 passed; 1 failed; 0 ignored;', exit_code=101)['passed'])
        self.assertFalse(self.execution(['test case_a ... ok', 'test case_b ... ok'], exit_code=101)['passed'])

    def test_execution_rejects_inconsistent_summary_and_compiler_errors(self):
        lines = ['test case_a ... ok', 'test case_b ... ok']
        with self.assertRaises(CASES.CaseExecutionError):
            self.execution(lines, 'ok. 1 passed; 0 failed; 0 ignored;')
        with self.assertRaises(CASES.CaseExecutionError):
            self.execution(lines + ['test result: ok. 2 passed; 0 failed; 0 ignored;'])
        with self.assertRaises(CASES.CaseExecutionError):
            self.execution(lines, errors=b'error[E0123]: could not compile the test')

    def test_required_case_cannot_be_reclassified_as_external(self):
        value = json.loads(INVENTORY.read_text())
        value['suites'][0]['external_probes'].append(value['suites'][0]['required_cases'].pop())
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / 'inventory.json'
            path.write_text(json.dumps(value))
            with self.assertRaises(CASES.CaseInventoryError):
                CASES.load_suites(path)

    def test_suite_removal_or_filter_changes_cannot_reduce_coverage(self):
        original = json.loads(INVENTORY.read_text())
        changes = [copy.deepcopy(original), copy.deepcopy(original)]
        changes[0]['suites'].pop()
        changes[1]['suites'][0]['selector'].append('one_case')
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / 'inventory.json'
            for value in changes:
                path.write_text(json.dumps(value))
                with self.assertRaises(CASES.CaseInventoryError):
                    CASES.load_suites(path)

    def test_optimized_python_fails_before_any_proof_command_and_retains_failure(self):
        with tempfile.TemporaryDirectory() as directory:
            result = subprocess.run(
                [sys.executable, '-O', '-B', str(ROOT / 'scripts/ci-client-capabilities.py'),
                 '--toolchain', 'stable', '--features', 'default'],
                cwd=directory, env={**os.environ, 'PYTHONDONTWRITEBYTECODE': '1'},
                capture_output=True, text=True, timeout=10)
            self.assertNotEqual(result.returncode, 0)
            report = json.loads((Path(directory) / 'target/client-capabilities/stable/default/evidence/validation.json').read_text())
            self.assertFalse(report['passed'])
            self.assertEqual(report['commands'], [])
            self.assertIn('assertions enabled', report['failure']['detail'])

    def test_helper_import_failure_retains_an_unpassed_report(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            script = root / 'ci-client-capabilities.py'
            script.write_bytes((ROOT / 'scripts/ci-client-capabilities.py').read_bytes())
            (root / 'client-capability-cases.py').write_text('raise RuntimeError("broken case helper")\n')
            result = subprocess.run(
                [sys.executable, '-B', str(script), '--toolchain', 'stable', '--features', 'default'],
                cwd=root, env={**os.environ, 'PYTHONDONTWRITEBYTECODE': '1'},
                capture_output=True, text=True, timeout=10)
            self.assertNotEqual(result.returncode, 0)
            report = json.loads((root / 'target/client-capabilities/stable/default/evidence/validation.json').read_text())
            self.assertFalse(report['passed'])
            self.assertEqual(report['commands'], [])
            self.assertEqual(report['failure']['detail'], 'broken case helper')


if __name__ == '__main__':
    unittest.main()

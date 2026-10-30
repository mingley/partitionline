"""Guard controls for candidate evidence; these do not invoke Rust or Cargo."""
import gzip
import hashlib
import importlib.util
import json
import os
import sys
from pathlib import Path
import tempfile
import unittest
from unittest import mock

REPOSITORY = Path(__file__).resolve().parents[2]
SPEC = importlib.util.spec_from_file_location('client_candidate_qualification', REPOSITORY / 'scripts/ci-qualify-client-candidate.py')
DRIVER = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(DRIVER)


class ClientCandidateGuards(unittest.TestCase):
    def test_unsafe_or_ambiguous_paths_are_rejected(self):
        for path in ['', '.', '../lib.rs', '/tmp/lib.rs', '.git/config', 'src//lib.rs', 'src\\lib.rs']:
            with self.subTest(path=path), self.assertRaises(RuntimeError):
                DRIVER.relative_path(path)
        self.assertEqual(str(DRIVER.relative_path('src/lib.rs')), 'src/lib.rs')

    def test_asset_hash_is_checked_before_source_mutation(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / 'patch').write_text('changed bytes')
            with self.assertRaisesRegex(RuntimeError, 'digest mismatch'):
                DRIVER.read_asset(root, {'path': 'patch', 'sha256': '0' * 64})

    def test_patch_cannot_touch_an_unlisted_path(self):
        with tempfile.TemporaryDirectory() as directory:
            patch = Path(directory) / 'change.patch'
            patch.write_text('--- a/src/other.rs\n+++ b/src/other.rs\n@@ -1 +1 @@\n-old\n+new\n')
            with mock.patch.object(DRIVER, 'manifest', return_value={'src/lib.rs': 'a'}), mock.patch.object(DRIVER.subprocess, 'run') as command:
                with self.assertRaisesRegex(RuntimeError, 'explicitly changed manifest paths'):
                    DRIVER.apply_patch('unlisted', patch, {'src/lib.rs': 'a'}, {'src/lib.rs': 'b'})
                command.assert_not_called()

    def test_source_drift_stops_before_starting_a_command(self):
        with tempfile.TemporaryDirectory() as directory:
            with mock.patch.object(DRIVER, 'ROOT', Path(directory)), mock.patch.object(DRIVER, 'OUT', Path(directory)), mock.patch.object(DRIVER, 'EXPECTED', {'src/lib.rs': 'expected'}), mock.patch.object(DRIVER, 'manifest', return_value={'src/lib.rs': 'changed'}), mock.patch.object(DRIVER.shutil, 'disk_usage', return_value=mock.Mock(free=10**12)), mock.patch.object(DRIVER.subprocess, 'Popen') as command:
                with self.assertRaisesRegex(RuntimeError, 'source does not match'):
                    DRIVER.run('drift', ['cargo', '--version'])
                command.assert_not_called()

    def test_ignored_required_control_cannot_pass(self):
        with mock.patch.object(DRIVER, 'listing', side_effect=[{'required'}, {'required'}]), mock.patch.object(DRIVER, 'execute') as execute:
            with self.assertRaisesRegex(RuntimeError, 'missing or ignored'):
                DRIVER.full_target('ignored', 'build', 'partitionline', {'required'})
            execute.assert_not_called()

    def test_zero_executed_tests_cannot_pass_a_full_run(self):
        row = {'exit': 0, 'test_results': ['test result: ok. 0 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.00s']}
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / 'zero.log').write_text('')
            with mock.patch.object(DRIVER, 'OUT', root), mock.patch.object(DRIVER, 'listing', side_effect=[{'required'}, set()]), mock.patch.object(DRIVER, 'execute', return_value=row):
                self.assertFalse(DRIVER.full_target('zero', 'build', 'partitionline', {'required'}))
            self.assertIn('full test count differs from compiled discovery', json.loads((root / 'zero-selection.json').read_text())['verification_errors'])

    def test_missing_required_identity_cannot_pass(self):
        with mock.patch.object(DRIVER, 'listing', side_effect=[{'different'}, set()]), mock.patch.object(DRIVER, 'execute') as execute:
            with self.assertRaisesRegex(RuntimeError, 'missing or ignored'):
                DRIVER.full_target('missing', 'build', 'partitionline', {'required'})
            execute.assert_not_called()

    def test_other_test_failure_still_fails_when_required_cases_pass(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / 'full.log').write_text('test required ... ok\ntest other ... FAILED\n')
            row = {'exit': 101, 'test_results': ['test result: FAILED. 1 passed; 1 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.00s']}
            with mock.patch.object(DRIVER, 'OUT', root), mock.patch.object(DRIVER, 'listing', side_effect=[{'required', 'other'}, set()]), mock.patch.object(DRIVER, 'execute', return_value=row):
                self.assertFalse(DRIVER.full_target('full', 'build', 'partitionline', {'required'}))
            report = json.loads((root / 'full-selection.json').read_text())
            self.assertEqual(report['required_results'], {'required': 'ok'})
            self.assertEqual(report['result_counts']['failed'], 1)


    def test_duplicate_actual_identities_cannot_replace_a_missing_case(self):
        with self.assertRaisesRegex(RuntimeError, 'duplicate actual test identity'):
            DRIVER.test_statuses('test required ... ok\ntest required ... ok\n')

    def test_ignored_reasons_preserve_the_actual_identity(self):
        self.assertEqual(DRIVER.test_statuses('test fixture ... ignored, requires generated fixtures\n'), {'fixture': 'ignored'})

    def test_substituted_actual_identity_cannot_pass_the_same_count(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / 'full.log').write_text('test different ... ok\n')
            row = {'exit': 0, 'test_results': ['test result: ok. 1 passed; 0 failed; 0 ignored;']}
            with mock.patch.object(DRIVER, 'OUT', root), mock.patch.object(DRIVER, 'listing', side_effect=[{'required'}, set()]), mock.patch.object(DRIVER, 'execute', return_value=row):
                self.assertFalse(DRIVER.full_target('full', 'build', 'partitionline', {'required'}))
            self.assertIn('actual test identities differ from compiled discovery', json.loads((root / 'full-selection.json').read_text())['verification_errors'])

    def test_actual_failed_status_cannot_hide_behind_a_passing_summary(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / 'full.log').write_text('test required ... FAILED\n')
            row = {'exit': 0, 'test_results': ['test result: ok. 1 passed; 0 failed; 0 ignored;']}
            with mock.patch.object(DRIVER, 'OUT', root), mock.patch.object(DRIVER, 'listing', side_effect=[{'required'}, set()]), mock.patch.object(DRIVER, 'execute', return_value=row):
                self.assertFalse(DRIVER.full_target('full', 'build', 'partitionline', {'required'}))
            self.assertIn('actual statuses differ from test result summary', json.loads((root / 'full-selection.json').read_text())['verification_errors'])

    def test_actual_ignored_identities_must_match_discovery(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / 'full.log').write_text('test required ... ok\ntest fixture ... ignored, needs Java\n')
            row = {'exit': 0, 'test_results': ['test result: ok. 1 passed; 0 failed; 1 ignored;']}
            with mock.patch.object(DRIVER, 'OUT', root), mock.patch.object(DRIVER, 'listing', side_effect=[{'required', 'fixture'}, {'fixture'}]), mock.patch.object(DRIVER, 'execute', return_value=row):
                self.assertTrue(DRIVER.full_target('full', 'build', 'partitionline', {'required'}))

    def test_gzip_patch_raw_bytes_are_independently_pinned(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            data = b'--- a/src/lib.rs\n+++ b/src/lib.rs\n@@ -1 +1 @@\n-old\n+new\n'
            asset = root / 'change.patch.gz'
            asset.write_bytes(gzip.compress(data, mtime=0))
            entry = {'path': asset.name, 'sha256': hashlib.sha256(asset.read_bytes()).hexdigest(), 'bytes': asset.stat().st_size,
                     'uncompressed_bytes': len(data), 'uncompressed_sha256': '0' * 64}
            with mock.patch.object(DRIVER, 'OUT', root):
                with self.assertRaisesRegex(RuntimeError, 'decompressed patch differs'):
                    DRIVER.materialize_patch(root, entry)
            self.assertFalse((root / 'assets' / 'change.patch').exists())

    def test_known_oom_increase_and_reset_are_visible(self):
        before = 'oom 4\noom_kill 2\noom_group_kill 1\n'
        status, delta = DRIVER.memory_delta(before, 'oom 5\noom_kill 2\noom_group_kill 1\n')
        self.assertEqual(status, 'known')
        self.assertEqual(delta['oom'], 1)
        self.assertEqual(DRIVER.memory_delta(before, 'oom 0\noom_kill 0\noom_group_kill 0\n')[0], 'reset')
        self.assertEqual(DRIVER.memory_delta(None, before), ('unknown', None))


    @unittest.skipUnless(sys.platform.startswith('linux'), 'Linux owned-command primitive')
    def test_timeout_retains_failed_receipt_after_waiting_for_its_leader(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            env = os.environ.copy()
            with mock.patch.object(DRIVER, 'ROOT', root), mock.patch.object(DRIVER, 'OUT', root), mock.patch.object(DRIVER, 'ENV', env), mock.patch.object(DRIVER, 'EXPECTED', {}), mock.patch.object(DRIVER, 'manifest', return_value={}), mock.patch.object(DRIVER.shutil, 'disk_usage', return_value=mock.Mock(free=10**12)):
                with self.assertRaisesRegex(RuntimeError, 'incomplete run'):
                    DRIVER.run('timeout', [sys.executable, '-c', 'import time;time.sleep(30)'], timeout=.1)
            receipt = json.loads((root / 'timeout.json').read_text())
            self.assertEqual(receipt['stop_reason'], 'timeout')
            self.assertTrue(receipt['owned_command_cleanup']['leader_waited'])
            self.assertFalse(receipt['owned_command_cleanup']['remaining_descendants'])
            self.assertIsNotNone(receipt['exit'])

    def test_missing_cgroup_metadata_is_unknown(self):
        with mock.patch.object(Path, 'read_text', side_effect=FileNotFoundError):
            self.assertIsNone(DRIVER.memory())


if __name__ == '__main__':
    unittest.main()

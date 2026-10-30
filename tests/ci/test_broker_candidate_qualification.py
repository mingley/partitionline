"""Pure archive, source and execution-accounting controls; no Kafka/Cargo run."""
import gzip
from contextlib import nullcontext
import hashlib
import importlib.util
import io
import json
import os
from pathlib import Path
import stat
import subprocess
import sys
import tarfile
import tempfile
import unittest
from types import SimpleNamespace
from unittest.mock import Mock, patch

ROOT = Path(__file__).resolve().parents[2]
SPEC = importlib.util.spec_from_file_location('broker_candidate', ROOT/'scripts/ci-qualify-broker-candidate.py')
DRIVER = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(DRIVER)


def entry(data):
    return {'bytes': len(data), 'sha256': hashlib.sha256(data).hexdigest(), 'mode': 0o644}


def archive(path, rows):
    raw = io.BytesIO()
    with tarfile.open(fileobj=raw, mode='w') as bundle:
        for name, data, kind in rows:
            info = tarfile.TarInfo(name)
            info.type = kind
            info.size = len(data) if kind == tarfile.REGTYPE else 0
            bundle.addfile(info, io.BytesIO(data) if info.size else None)
    path.write_bytes(gzip.compress(raw.getvalue(), mtime=0))


def apply_private_source_patch(root):
    source = root/'source'
    source.mkdir()
    (source/'file.rs').write_bytes(b'old\n')
    (source/'file.rs').chmod(0o600)
    (source/'fixture.bin').write_bytes(b'unchanged fixture')
    (source/'fixture.bin').chmod(0o400)
    fixed = {'file.rs': dict(entry(b'fixed\n'), mode=0o600),
        'fixture.bin': dict(entry(b'unchanged fixture'), mode=0o400)}
    patch = root/'source.patch'
    patch.write_text('diff --git a/file.rs b/file.rs\n--- a/file.rs\n+++ b/file.rs\n'
        '@@ -1 +1 @@\n-old\n+fixed\n')
    old_mask = os.umask(0o022)
    try:
        subprocess.run(['git', 'apply', str(patch)], cwd=source, check=True,
            capture_output=True, text=True)
    finally:
        os.umask(old_mask)
    return source, fixed


class BrokerCandidateQualification(unittest.TestCase):
    def test_real_git_patch_restores_only_pinned_private_source_mode(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            source, fixed = apply_private_source_patch(root)
            self.assertEqual(stat.S_IMODE((source/'file.rs').stat().st_mode), 0o644)
            receipt = DRIVER.restore_patched_source_modes(
                source, fixed, {'file.rs'}, root/'source-modes.json')
            self.assertTrue(receipt['passed'])
            self.assertTrue(receipt['byte_inventory_verified_before_restoration'])
            self.assertEqual(receipt['observed_modes_before'], {'file.rs': 0o644})
            self.assertEqual(receipt['observed_modes_after'], {'file.rs': 0o600})
            self.assertEqual((source/'file.rs').read_bytes(), b'fixed\n')
            self.assertEqual(stat.S_IMODE((source/'fixture.bin').stat().st_mode), 0o400)
            DRIVER.verify_source(source, fixed)

    def test_source_mode_restoration_cannot_hide_byte_inventory_or_unlisted_mode_drift(self):
        for kind in ('bytes', 'inventory', 'unlisted-mode'):
            with self.subTest(kind=kind), tempfile.TemporaryDirectory() as temporary:
                root = Path(temporary)
                source, fixed = apply_private_source_patch(root)
                if kind == 'bytes':
                    (source/'file.rs').write_bytes(b'forged\n')
                elif kind == 'inventory':
                    (source/'extra.rs').write_bytes(b'not pinned')
                else:
                    (source/'fixture.bin').chmod(0o600)
                with self.assertRaises(ValueError):
                    DRIVER.restore_patched_source_modes(source, fixed, {'file.rs'}, root/'source-modes.json')
                self.assertEqual(stat.S_IMODE((source/'file.rs').stat().st_mode), 0o644)
                receipt = json.loads((root/'source-modes.json').read_text())
                self.assertFalse(receipt['passed'])
                self.assertEqual(receipt['observed_modes_after'], {})
                self.assertEqual(receipt['byte_inventory_verified_before_restoration'], kind == 'unlisted-mode')

    def test_optimized_python_retains_hash_failure_before_running_commands(self):
        with tempfile.TemporaryDirectory(dir=ROOT) as spec_temporary, tempfile.TemporaryDirectory() as temporary:
            spec_root = Path(spec_temporary)
            (spec_root/'bad.tar.gz').write_bytes(b'wrong archive hash')
            spec = {'schema_version': 1, 'kind': 'broker',
                'baseline_cases': sorted(DRIVER.BASELINE), 'fixed_cases': sorted(DRIVER.FIXED),
                'baseline_archive': {'path': 'bad.tar.gz', 'sha256': '0'*64}}
            path = spec_root/'spec.json'
            path.write_text(json.dumps(spec))
            output = Path(temporary)/'retained-failure'
            process = subprocess.run([sys.executable, '-O', '-B',
                str(ROOT/'scripts/ci-qualify-broker-candidate.py'), '--spec',
                str(path.relative_to(ROOT)), '--output', str(output)], capture_output=True, text=True)
            self.assertEqual(process.returncode, 1)
            report = json.loads((output/'qualification.json').read_text())
            self.assertFalse(report['passed'])
            self.assertIn('asset hash mismatch', report['error'])
            self.assertEqual(report['commands'], [])

    def test_duplicate_json_fields_are_rejected(self):
        with self.assertRaisesRegex(ValueError, 'duplicate JSON'):
            DRIVER.json.loads('{"passed":true,"passed":false}', object_pairs_hook=DRIVER.unique_object)

    def test_source_archive_roundtrip_and_changed_bytes(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            packed, source = root/'source.gz', root/'source'
            source.mkdir()
            archive(packed, [('nested/file.rs', b'owned source', tarfile.REGTYPE)])
            expected = {'nested/file.rs': entry(b'owned source')}
            DRIVER.extract_source(packed, source, expected)
            DRIVER.verify_source(source, expected)
            (source/'nested/file.rs').chmod(0o600)
            with self.assertRaisesRegex(ValueError, 'source mode changed'):
                DRIVER.verify_source(source, expected)
            (source/'nested/file.rs').chmod(0o644)
            (source/'alias').symlink_to(source/'nested', target_is_directory=True)
            with self.assertRaisesRegex(ValueError, 'source contains a symlink'):
                DRIVER.verify_source(source, expected)
            (source/'alias').unlink()
            (source/'nested/file.rs').write_bytes(b'changed')
            with self.assertRaisesRegex(ValueError, 'source bytes changed'):
                DRIVER.verify_source(source, expected)
            (source/'nested/file.rs').write_bytes(b'owned source')
            (source/'extra').write_bytes(b'untracked')
            with self.assertRaisesRegex(ValueError, 'inventory changed'):
                DRIVER.verify_source(source, expected)

    def test_archive_duplicate_symlink_and_traversal_are_rejected(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            for index, (rows, expected) in enumerate([
                ([('x', b'a', tarfile.REGTYPE), ('x', b'a', tarfile.REGTYPE)], {'x': entry(b'a')}),
                ([('x', b'', tarfile.SYMTYPE)], {'x': entry(b'')}),
                ([('../escape', b'a', tarfile.REGTYPE)], {'../escape': entry(b'a')}),
            ]):
                with self.subTest(index=index), self.assertRaises(ValueError):
                    packed, source = root/f'{index}.gz', root/f'{index}-source'
                    source.mkdir()
                    archive(packed, rows)
                    DRIVER.extract_source(packed, source, expected)

    def test_archive_cannot_substitute_content_or_missing_members(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            packed, source = root/'source.gz', root/'source'
            source.mkdir()
            archive(packed, [('x', b'forged', tarfile.REGTYPE)])
            with self.assertRaisesRegex(ValueError, 'hash/size mismatch'):
                DRIVER.extract_source(packed, source, {'x': entry(b'actual')})
            with self.assertRaisesRegex(ValueError, 'file inventory'):
                DRIVER.extract_source(packed, source, {'missing': entry(b'forged')})

    def test_patch_decompression_keeps_exact_bytes_and_rejects_substitution(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            raw = b'context line with a trailing space \n'
            packed = root/'patch.gz'
            packed.write_bytes(gzip.compress(raw, mtime=0))
            expected = {'expanded_bytes': len(raw), 'expanded_sha256': hashlib.sha256(raw).hexdigest()}
            target = DRIVER.materialize_patch(packed, root/'exact.patch', expected)
            self.assertEqual(target.read_bytes(), raw)
            with self.assertRaisesRegex(ValueError, 'hash/size mismatch'):
                DRIVER.materialize_patch(packed, root/'invalid.patch', dict(expected, expanded_sha256='0'*64))
            self.assertFalse((root/'invalid.patch').exists())

    def test_partial_and_reset_oom_counters_are_reported_without_inventing_values(self):
        self.assertEqual(DRIVER.oom_delta(None, {'oom': 0}), ('unavailable', None))
        self.assertEqual(DRIVER.oom_delta({'oom': 2}, {'oom': 2}), ('partial', {'oom': 0}))
        self.assertEqual(DRIVER.oom_delta({'oom': 2}, {'oom': 1}), ('reset', {'oom': -1}))

    def test_fast_completed_command_with_unknown_oom_does_not_invent_zero_events(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            source, output = root/'source', root/'output'
            source.mkdir()
            output.mkdir()
            (source/'x').write_bytes(b'a')
            (source/'x').chmod(0o644)
            qualification = DRIVER.Qualification(output, source, {'x': entry(b'a')})
            # A mocked instant command can finish before the first monitor sample.
            process = Mock()
            process.returncode = 0
            owned = SimpleNamespace(process=process, finished=lambda: True,
                cleanup_receipt={'leader_waited': True, 'remaining_descendants': []})
            with patch.object(DRIVER.OWNERSHIP, 'owned_command', return_value=nullcontext(owned)), \
                    patch.object(DRIVER, 'oom_events', return_value=None):
                qualification.run('instant', ['synthetic-no-native-work'])
            receipt = qualification.records[0]
            self.assertEqual(receipt['disk_samples'], [])
            self.assertIsNone(receipt['oom_event_deltas'])
            self.assertEqual(receipt['oom_counter_status'], 'unavailable')
            self.assertTrue(receipt['source_manifest_unchanged'])

    def test_fast_command_final_disk_floor_and_oom_events_retain_failed_receipts(self):
        for kind in ('disk', 'oom'):
            with self.subTest(kind=kind), tempfile.TemporaryDirectory() as temporary:
                root = Path(temporary)
                source, output = root/'source', root/'output'
                source.mkdir()
                output.mkdir()
                qualification = DRIVER.Qualification(output, source, {})
                owned = SimpleNamespace(process=SimpleNamespace(returncode=0), finished=lambda: True,
                    cleanup_receipt={'leader_waited': True, 'remaining_descendants': []})
                final_free = DRIVER.FLOOR if kind == 'disk' else DRIVER.FLOOR+1
                events = [None, None] if kind == 'disk' else [{'oom': 0}, {'oom': 1}]
                with patch.object(DRIVER.OWNERSHIP, 'owned_command', return_value=nullcontext(owned)), \
                        patch.object(DRIVER.shutil, 'disk_usage', side_effect=[
                            SimpleNamespace(free=DRIVER.FLOOR+1), SimpleNamespace(free=final_free)]), \
                        patch.object(DRIVER, 'oom_events', side_effect=events):
                    with self.assertRaisesRegex(ValueError, 'resource guard failed'):
                        qualification.run('instant-failure', ['synthetic-no-native-work'])
                receipt = json.loads((output/'instant-failure.json').read_text())
                self.assertFalse(receipt['passed'])
                self.assertTrue(receipt['ownership_complete'])
                self.assertEqual(receipt['disk_samples'], [])
                if kind == 'disk':
                    self.assertEqual(receipt['stop_reason'], 'disk-floor')
                else:
                    self.assertEqual(receipt['oom_counter_status'], 'partial')
                    self.assertEqual(receipt['oom_event_deltas'], {'oom': 1})

    def test_exact_negative_control_requires_every_real_failure_and_no_ignored_case(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            source, output = root/'source', root/'output'
            source.mkdir()
            output.mkdir()
            binary = root/'fake-artifact'
            binary.write_bytes(b'synthetic parser control, not a Rust executable')
            qualification = DRIVER.Qualification(output, source, {})
            artifact = {'execution_path': str(binary), 'sha256': DRIVER.sha(binary)}
            listing = ''.join(name+': test\n' for name in sorted(DRIVER.BASELINE))
            qualification.run = Mock(side_effect=[listing,
                'test result: FAILED. 0 passed; 6 failed; 1 ignored;\n'])
            with self.assertRaisesRegex(ValueError, 'count mismatch'):
                qualification.exact('partial', artifact, DRIVER.BASELINE, negative=True)
            wrong_names = sorted(DRIVER.BASELINE)[:-1]+['unexpected_failure']
            failures = ''.join('test '+name+' ... FAILED\n' for name in wrong_names)
            qualification.run = Mock(side_effect=[listing, failures+
                'test result: FAILED. 0 passed; 7 failed; 0 ignored;\n'])
            with self.assertRaisesRegex(ValueError, 'different test inventory'):
                qualification.exact('wrong-failures', artifact, DRIVER.BASELINE, negative=True)

    def test_positive_control_rejects_duplicate_or_unrelated_success_identities(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            source, output = root/'source', root/'output'
            source.mkdir()
            output.mkdir()
            binary = root/'fake-artifact'
            binary.write_bytes(b'synthetic positive identity parser control')
            qualification = DRIVER.Qualification(output, source, {})
            artifact = {'execution_path': str(binary), 'sha256': DRIVER.sha(binary)}
            names = sorted(DRIVER.FIXED)
            listing = ''.join(name+': test\n' for name in names)
            for replacement in [names[0], 'unrelated_success']:
                statuses = ''.join('test '+name+' ... ok\n' for name in names[:-1]+[replacement])
                qualification.run = Mock(side_effect=[listing, statuses+
                    'test result: ok. 12 passed; 0 failed; 0 ignored;\n'])
                with self.subTest(replacement=replacement), self.assertRaisesRegex(ValueError, 'different test inventory'):
                    qualification.exact('wrong-success', artifact, DRIVER.FIXED)


if __name__ == '__main__':
    unittest.main()

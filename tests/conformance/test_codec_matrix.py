"""Negative controls for actual codec matrix artifacts, not simulated SDK passes."""
import copy
import importlib.util
import json
import os
from pathlib import Path
import shutil
import tempfile
import unittest

ROOT = Path(__file__).resolve().parents[2]
SPEC = importlib.util.spec_from_file_location('codec_matrix', Path(__file__).with_name('run-codec-matrix.py'))
MODULE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(MODULE)


class CodecMatrixControls(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.original = Path(os.environ.get('PL_CODEC_NATIVE_ARTIFACTS',
                                           ROOT / 'docs/evidence/codecs/codec-matrix/native'))
        if not (cls.original / 'summary.json').is_file():
            raise RuntimeError('complete native codec evidence required for negative controls')

    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory(dir=self.original.parent)
        self.addCleanup(self.temporary.cleanup)
        self.directory = Path(self.temporary.name) / 'matrix'
        # Read-only segment/log links save space. Every mutation replaces its link.
        shutil.copytree(self.original, self.directory, copy_function=os.link,
                        ignore=shutil.ignore_patterns('data'))

    def mutate_json(self, name, edit):
        path = self.directory / name
        value = json.loads(path.read_text())
        edit(value)
        path.unlink()
        path.write_text(json.dumps(value))

    def rejected(self):
        with self.assertRaises(ValueError):
            MODULE.audit_native(self.directory)

    def test_actual_native_matrix_passes(self):
        report = MODULE.audit_native(self.directory)
        expected = 30 * len(report['broker_versions'])
        self.assertEqual((report['cells'], report['cross_client_readbacks']), (expected, expected * 2))
        self.assertEqual(len(report['stored_batches']), expected)

    def test_missing_codec_cell_rejected(self):
        self.mutate_json('cells.json', lambda rows: rows.pop())
        self.rejected()

    def test_duplicate_cell_cannot_replace_missing_peer(self):
        self.mutate_json('cells.json', lambda rows: rows.__setitem__(1, copy.deepcopy(rows[0])))
        self.rejected()

    def test_performance_claim_rejected(self):
        self.mutate_json('summary.json', lambda row: row.update(performance_claims_valid=True))
        self.rejected()

    def test_failed_child_rejected(self):
        self.mutate_json('commands.json', lambda rows: rows[0].update(exit_code=1))
        self.rejected()

    def test_unjoined_child_rejected(self):
        self.mutate_json('commands.json', lambda rows: rows[0].update(parent_waited=False))
        self.rejected()

    def test_timeout_rejected_even_with_success_exit(self):
        self.mutate_json('commands.json', lambda rows: rows[0].update(deadline_expired=True))
        self.rejected()

    def test_missing_cross_client_reader_rejected(self):
        self.mutate_json('cells.json', lambda rows: rows[0].update(readers=['java']))
        self.rejected()

    def test_success_log_substitution_rejected(self):
        row = json.loads((self.directory / 'commands.json').read_text())[0]
        path = self.directory / (row['name'] + '.log')
        path.unlink()
        path.write_text('{"status":"pass"}\n')
        self.rejected()

    def test_wrong_stored_codec_rejected(self):
        cells = json.loads((self.directory / 'cells.json').read_text())
        first = cells[0]
        segment = next((self.directory / first['broker'] / 'batches' / first['topic']).glob('*.log'))
        with self.assertRaises(ValueError):
            MODULE.batches(segment, 4)  # First producer/topic policy is uncompressed.

    def test_inner_payload_change_rejected_by_outer_crc(self):
        cells = json.loads((self.directory / 'cells.json').read_text())
        first = cells[0]
        segment = next((self.directory / first['broker'] / 'batches' / first['topic']).glob('*.log'))
        data = bytearray(segment.read_bytes())
        data[61] ^= 1
        segment.unlink()
        segment.write_bytes(data)
        with self.assertRaises(ValueError):
            MODULE.batches(segment, 0)

    def test_truncated_segment_rejected(self):
        cells = json.loads((self.directory / 'cells.json').read_text())
        first = cells[0]
        segment = next((self.directory / first['broker'] / 'batches' / first['topic']).glob('*.log'))
        data = segment.read_bytes()
        segment.unlink()
        segment.write_bytes(data[:-1])
        with self.assertRaises(ValueError):
            MODULE.batches(segment, 0)


if __name__ == '__main__':
    unittest.main()

"""Real Python validator controls over copied actual first-generation artifacts.

No JVM runs and no fixture bytes are synthesized as accepted evidence. Only
independent synthetic mutation copies are deleted after rejection controls.
"""
import sys
sys.dont_write_bytecode=True
import hashlib
from pathlib import Path
import shutil
import tempfile
import unittest
from test_prelaunch import runner

PARTIAL=Path('/workspace/work/streams-codecs/oracle-final-04f6bc29-overlay848-attempt-02/4.1.2/generation-1')

class ActualPartialFixtureControls(unittest.TestCase):
    def setUp(self):
        self.temp=tempfile.TemporaryDirectory(prefix='streams-actual-fixture-validator-control-')
        self.root=Path(self.temp.name)/'copied-fixtures'
        shutil.copytree(PARTIAL,self.root)
        self.before={p.name:hashlib.sha256(p.read_bytes()).hexdigest() for p in PARTIAL.iterdir()}

    def tearDown(self):
        self.assertEqual(self.before,{p.name:hashlib.sha256(p.read_bytes()).hexdigest() for p in PARTIAL.iterdir()})
        self.temp.cleanup()

    def test_actual_frozen_source_utf8_names_and_all53_hashes_are_admitted(self):
        rows=runner.fixture_tree(self.root)
        self.assertEqual(len(rows),55)
        self.assertEqual(sum(p['path'].endswith('.bin') for p in rows),53)
        names={p['path'] for p in rows}
        self.assertTrue({'describe-request-invalid-utf8.bin',
                         'heartbeat-request-header-utf8.bin',
                         'describe-request-header-utf8.bin'}<=names)

    def mutate_first_name(self,name):
        p=self.root/'cases.tsv';rows=p.read_text().splitlines()
        cells=rows[1].split('\t');cells[0]=name;rows[1]='\t'.join(cells)
        p.write_text('\n'.join(rows)+'\n')

    def test_unreviewed_digit_is_still_rejected(self):
        self.mutate_first_name('heartbeat-request-default7')
        with self.assertRaisesRegex(ValueError,'shape/name'):
            runner.fixture_tree(self.root)

    def test_path_traversal_is_still_rejected_before_fixture_access(self):
        self.mutate_first_name('../escape')
        with self.assertRaisesRegex(ValueError,'shape/name'):
            runner.fixture_tree(self.root)

    def test_duplicate_name_is_still_rejected(self):
        p=self.root/'cases.tsv';rows=p.read_text().splitlines()
        a=rows[1].split('\t');b=rows[2].split('\t');b[0]=a[0];b[-1]=a[-1]
        rows[2]='\t'.join(b);p.write_text('\n'.join(rows)+'\n')
        with self.assertRaisesRegex(ValueError,'identity/hash'):
            runner.fixture_tree(self.root)

    def test_actual_vector_hash_mutation_is_still_rejected(self):
        p=self.root/'heartbeat-request-default.bin';data=p.read_bytes()
        p.write_bytes(bytes([data[0]^1])+data[1:])
        with self.assertRaisesRegex(ValueError,'identity/hash'):
            runner.fixture_tree(self.root)

    def test_missing_actual_vector_is_still_rejected(self):
        (self.root/'heartbeat-request-default.bin').unlink()
        with self.assertRaisesRegex(ValueError,'expected53'):
            runner.fixture_tree(self.root)

    def test_missing_actual_table_row_is_still_rejected(self):
        p=self.root/'cases.tsv';rows=p.read_text().splitlines()
        p.write_text('\n'.join(rows[:-1])+'\n')
        with self.assertRaisesRegex(ValueError,'row count'):
            runner.fixture_tree(self.root)

    def test_table_column_mutation_is_still_rejected(self):
        p=self.root/'cases.tsv';rows=p.read_text().splitlines()
        rows[1]+='\textra';p.write_text('\n'.join(rows)+'\n')
        with self.assertRaisesRegex(ValueError,'shape/name'):
            runner.fixture_tree(self.root)

if __name__=='__main__':
    unittest.main(verbosity=2)

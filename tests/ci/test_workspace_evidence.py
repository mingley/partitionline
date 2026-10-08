"""Check restoration boundaries using independent, small publication archives."""
import gzip
import hashlib
import importlib.util
import io
import json
from pathlib import Path
import tarfile
import tempfile
import unittest

MODULE = Path(__file__).resolve().parents[2] / 'scripts/restore-workspace-evidence.py'
spec = importlib.util.spec_from_file_location('workspace_evidence', MODULE)
restore = importlib.util.module_from_spec(spec)
spec.loader.exec_module(restore)


class PublicationArchive(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.archive = self.root / 'archive'
        self.archive.mkdir()
        (self.archive / 'chunks').mkdir()
        self.payload = b'actual retained fixture bytes\n'
        self.sha = hashlib.sha256(self.payload).hexdigest()

    def fixture(self, names=('docs/evidence/one/data', 'docs/evidence/two/data'), member=None):
        raw = io.BytesIO()
        with tarfile.open(fileobj=raw, mode='w') as archive:
            info = tarfile.TarInfo(member or self.sha)
            info.size = len(self.payload)
            archive.addfile(info, io.BytesIO(self.payload))
        encoded = gzip.compress(raw.getvalue(), mtime=0)
        chunks = []
        # Deliberately split inside gzip headers and tar records.
        for index, offset in enumerate(range(0, len(encoded), 7), 1):
            name = f'chunks/part-{index:04d}.gz'
            data = encoded[offset:offset+7]
            (self.archive / name).write_bytes(data)
            chunks.append({'path':name,'bytes':len(data),'sha256':hashlib.sha256(data).hexdigest()})
        manifest = {'schema_version':1,'chunks':chunks,'files':[
            {'path':name,'bytes':len(self.payload),'sha256':self.sha,'mode':0o600} for name in names]}
        encoded_manifest = gzip.compress(json.dumps(manifest).encode(), mtime=0)
        (self.archive / 'manifest.json.gz').write_bytes(encoded_manifest)
        (self.archive / 'summary.json').write_text(json.dumps({
            'manifest_sha256':hashlib.sha256(encoded_manifest).hexdigest(),'files':len(names),'objects':1}))

    def test_deduplicated_content_and_prefix_restore(self):
        self.fixture()
        destination = self.root / 'restored'
        result = restore.verify(self.archive, destination, 'docs/evidence/two')
        self.assertEqual(result['verified_files'], 2)
        self.assertEqual(result['verified_objects'], 1)
        self.assertEqual(result['restored_files'], 1)
        self.assertEqual((destination / 'docs/evidence/two/data').read_bytes(), self.payload)
        self.assertFalse((destination / 'docs/evidence/one').exists())

    def test_corrupted_chunk_refused_before_restoration(self):
        self.fixture()
        (self.archive / 'chunks/part-0001.gz').write_bytes(b'corrupt')
        destination = self.root / 'restored'
        with self.assertRaisesRegex(ValueError, 'chunk checksum'):
            restore.verify(self.archive, destination)
        self.assertFalse(destination.exists())

    def test_path_traversal_refused(self):
        self.fixture(names=('../outside',))
        with self.assertRaisesRegex(ValueError, 'relative path'):
            restore.verify(self.archive, self.root / 'restored')
        self.assertFalse((self.root / 'outside').exists())

    def test_existing_file_never_overwritten(self):
        self.fixture()
        destination = self.root / 'restored'
        path = destination / 'docs/evidence/one/data'
        path.parent.mkdir(parents=True)
        path.write_bytes(b'keep these bytes')
        with self.assertRaises(FileExistsError):
            restore.verify(self.archive, destination)
        self.assertEqual(path.read_bytes(), b'keep these bytes')

    def test_destination_symlink_escape_refused(self):
        self.fixture()
        destination = self.root / 'restored'
        destination.mkdir()
        (destination / 'docs').symlink_to(self.root / 'outside', target_is_directory=True)
        with self.assertRaisesRegex(ValueError, 'escapes restoration root'):
            restore.verify(self.archive, destination)

    def test_unexpected_tar_member_refused(self):
        self.fixture(member='../../outside')
        with self.assertRaisesRegex(ValueError, 'archive member'):
            restore.verify(self.archive)


if __name__ == '__main__':
    unittest.main()

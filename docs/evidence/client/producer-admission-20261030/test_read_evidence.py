"""An alias cannot override metadata just because its object was checked earlier."""
import gzip
import hashlib
import io
import json
from pathlib import Path
import subprocess
import sys
import tarfile
import tempfile
import unittest

READER = Path(__file__).with_name('read-evidence.py')


def archive(path, changes=None):
    payload = b'verified payload\n'
    digest = hashlib.sha256(payload).hexdigest()
    good = dict(object='objects/' + digest, sha256=digest, bytes=len(payload))
    alias = dict(good)
    alias.update(changes or {})
    mapping = json.dumps(dict(files={'first.log': good, 'duplicate.log': alias})).encode()
    with tarfile.open(path, 'w:gz') as output:
        for name, data in [('files.json', mapping), (good['object'], payload)]:
            entry = tarfile.TarInfo(name)
            entry.size = len(data)
            output.addfile(entry, io.BytesIO(data))


def invoke(reader, changes=None):
    with tempfile.TemporaryDirectory() as directory:
        path = Path(directory) / 'alias.tar.gz'
        archive(path, changes)
        return subprocess.run([sys.executable, str(reader), str(path), '--verify'],
                              capture_output=True, text=True)


class ReaderControls(unittest.TestCase):
    def test_consistent_aliases_share_one_verified_object(self):
        result = invoke(READER)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(json.loads(result.stdout), dict(virtual_files=2, verified_objects=1))

    def test_repeated_object_must_check_every_virtual_length(self):
        result = invoke(READER, {'bytes': 1})
        self.assertNotEqual(result.returncode, 0)
        self.assertIn('evidence bytes differ: duplicate.log', result.stderr)

    def test_repeated_object_must_check_every_virtual_digest(self):
        result = invoke(READER, {'sha256': '0' * 64})
        self.assertNotEqual(result.returncode, 0)
        self.assertIn('evidence bytes differ: duplicate.log', result.stderr)


if __name__ == '__main__':
    unittest.main()

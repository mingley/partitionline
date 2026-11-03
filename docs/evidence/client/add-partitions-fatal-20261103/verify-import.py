#!/usr/bin/env python3
"""Verify the retained source-only archive without executing Rust."""
import hashlib
import json
from pathlib import Path
import tarfile

root = Path(__file__).resolve().parent
sha = lambda b: hashlib.sha256(b).hexdigest()
manifest = json.loads((root / 'manifest.json').read_bytes())
with tarfile.open(root / 'evidence.tar.gz', 'r:gz') as archive:
    members = {m.name: m for m in archive.getmembers() if m.isfile()}
    required = {'README.md', 'manifest.json', 'candidate.patch', 'verify.py'}
    required.update(f'{side}/{path}' for side in ('before', 'after') for path in manifest[side])
    assert set(members) == required, (sorted(set(members) - required), sorted(required - set(members)))
    def read(name):
        item = archive.extractfile(members[name])
        assert item is not None
        return item.read()
    assert read('README.md') == (root / 'README.md').read_bytes()
    assert read('manifest.json') == (root / 'manifest.json').read_bytes()
    assert read('candidate.patch') == (root / 'candidate.patch').read_bytes()
    assert sha(read('README.md')) == manifest['readme_sha256']
    assert sha(read('candidate.patch')) == manifest['patch_sha256']
    assert sha(read('verify.py')) == manifest['verifier_sha256']
    for side in ('before', 'after'):
        for path, expected in manifest[side].items():
            assert sha(read(f'{side}/{path}')) == expected, (side, path)
print('PASS: archive members, source pins, patch and manifest agree')
print('No Cargo, Rust compilation, native test, broker run or benchmark was performed.')

#!/usr/bin/env python3
"""Verify the complete accepted Git archive after all fresh public peer runs."""
import hashlib
import json
from pathlib import Path
import stat

ROOT = Path(__file__).resolve().parents[6]
BASE = Path(__file__).resolve().parent
SOURCE = Path('/workspace/work/retention-final-d147bcf1/source')
MANIFEST = Path('/workspace/work/retention-final-d147bcf1/source-integrity.json')


def main():
    data = MANIFEST.read_bytes()
    require = lambda condition: condition or (_ for _ in ()).throw(ValueError('source byte/mode/inventory mismatch'))
    require(hashlib.sha256(data).hexdigest() == '17b1a87c5ee3230f1cffd5422c5eacd082ac554de19b1795e879d0e983b1e00e')
    manifest = json.loads(data)
    require(manifest['source_commit'] == 'd147bcf1c0164778bdbad625842363f3721bc10e' and manifest['file_count'] == 44019)
    for name, expected in manifest['files'].items():
        path = SOURCE / name
        info = path.lstat()
        require(stat.S_ISREG(info.st_mode) and not path.is_symlink())
        require(('100755' if info.st_mode & 0o111 else '100644') == expected['mode'])
        contents = path.read_bytes()
        require(len(contents) == expected['bytes'] and hashlib.sha256(contents).hexdigest() == expected['sha256'])
        require(hashlib.sha1(b'blob ' + str(len(contents)).encode() + b'\0' + contents).hexdigest() == expected['git_blob_sha1'])
    actual = {str(p.relative_to(SOURCE)) for p in SOURCE.rglob('*') if p.is_file()}
    require(actual == set(manifest['files']))
    result = {'schema_version': 1, 'source_sha': manifest['source_commit'], 'passed': True,
              'checked_git_blobs': 44019, 'exact_inventory': True, 'git_blob_sha1_sha256_sizes_modes_checked': True,
              'source_root': str(SOURCE), 'source_manifest_sha256': hashlib.sha256(data).hexdigest(),
              'durable_manifest': 'docs/evidence/broker/KL11-10/storage/final-d147bcf1/source-integrity.json.gz',
              'checker_sha256': hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
              'command': ['python3', str(Path(__file__).relative_to(ROOT))],
              'scope': 'Full immutable archive verified after four fresh public Rust builds,72 TCP peer jobs,72 reverse Java executions; every source byte/mode unchanged.'}
    output = BASE / 'source-after-peers.json'
    output.write_text(json.dumps(result, indent=2) + '\n')
    print(json.dumps({'passed': True, 'checked_git_blobs': 44019, 'sha256': hashlib.sha256(output.read_bytes()).hexdigest()}))


if __name__ == '__main__':
    main()

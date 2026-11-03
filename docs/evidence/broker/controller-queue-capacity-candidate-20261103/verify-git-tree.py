#!/usr/bin/env python3
"""Compare local pinned source archive bytes to Git tree object IDs."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import subprocess
import tarfile

BASE = '45cbff24de76490594970fc962cd04e7be9fafb1'
PATHS = ['partitionline-broker', 'tests/conformance/broker', 'clippy.toml', 'rust-toolchain.toml',
         'docs/evidence/broker/KL11-59/topic-name-cases.tsv',
         'docs/evidence/broker/KL11-68/live-produce-api-versions/validation.json',
         'scripts/check-broker-api-matrix.py', 'tests/ci/test_broker_api_matrix.py']


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--checkout', type=Path, required=True)
    parser.add_argument('--manifest', type=Path, required=True)
    parser.add_argument('--archive', type=Path, required=True)
    args = parser.parse_args()
    expected = json.loads(args.manifest.read_text())
    env = {key: value for key, value in os.environ.items() if not key.startswith('GIT_')}
    env['LC_ALL'] = 'C'
    def git(*args_):
        return subprocess.check_output(['git', '-C', str(args.checkout), *args_], timeout=120, env=env)
    if git('rev-parse', BASE+'^{commit}').decode().strip() != BASE:
        raise ValueError('base commit mismatch')
    if git('rev-parse', '--show-object-format').decode().strip() != 'sha1':
        raise ValueError('unexpected Git object format')
    raw = git('ls-tree', '-r', '-z', BASE, '--', *PATHS)
    tree = {}
    for item in raw.split(b'\0'):
        if not item:
            continue
        header, encoded_name = item.split(b'\t', 1)
        mode, kind, object_id = header.split(b' ')
        name = encoded_name.decode('utf-8')
        if name in tree or name not in expected or kind != b'blob' or mode not in (b'100644', b'100755'):
            raise ValueError('unexpected Git tree entry: '+name)
        if int(mode, 8) & 0o777 != expected[name]['mode']:
            raise ValueError('Git source mode mismatch: '+name)
        tree[name] = object_id.decode('ascii')
    if set(tree) != set(expected):
        raise ValueError('Git tree inventory mismatch')
    if args.archive.stat().st_size > 16 * 1024**2:
        raise ValueError('source archive exceeds bound')
    seen = set()
    total = 0
    with tarfile.open(args.archive, mode='r:gz') as bundle:
        for member in bundle:
            name = member.name
            if not member.isfile() or name in seen or name not in expected or member.size > 8 * 1024**2:
                raise ValueError('unexpected archive member: '+name)
            entry = expected[name]
            if member.size != entry['bytes'] or member.mode != entry['mode']:
                raise ValueError('archive size/mode mismatch: '+name)
            total += member.size
            if total > 48 * 1024**2:
                raise ValueError('expanded source exceeds bound')
            data = bundle.extractfile(member).read()
            if hashlib.sha256(data).hexdigest() != entry['sha256']:
                raise ValueError('archive SHA-256 mismatch: '+name)
            git_blob = hashlib.sha1(b'blob ' + str(len(data)).encode('ascii') + b'\0' + data,
                                   usedforsecurity=False).hexdigest()
            if git_blob != tree[name]:
                raise ValueError('Git blob ID mismatch: '+name)
            seen.add(name)
    if seen != set(expected):
        raise ValueError('archive inventory mismatch')
    print(f'PASS: {len(seen)} source files match Git tree {BASE} without fetching blobs')


if __name__ == '__main__':
    main()

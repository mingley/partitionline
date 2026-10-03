#!/usr/bin/env python3
"""Check every archived Git blob against a retained source-integrity manifest."""
import argparse
import hashlib
import json
from pathlib import Path


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--source', type=Path, required=True)
    parser.add_argument('--manifest', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    manifest_bytes = args.manifest.read_bytes()
    manifest = json.loads(manifest_bytes)
    checked = 0
    for row in manifest['files']:
        path = args.source / row['path']
        data = path.read_bytes()
        assert len(data) == row['bytes'], row['path']
        assert hashlib.sha256(data).hexdigest() == row['sha256'], row['path']
        blob = b'blob ' + str(len(data)).encode('ascii') + b'\0' + data
        assert hashlib.sha1(blob).hexdigest() == row['git_blob'], row['path']
        if row['mode'] in ('100644', '100755'):
            executable = bool(path.stat().st_mode & 0o111)
            assert executable == (row['mode'] == '100755'), row['path']
        else:
            raise AssertionError('unsupported manifest mode: ' + row['path'])
        checked += 1
    assert checked == manifest['file_count']
    result = {
        'schema_version': 1,
        'source_sha': manifest['source_sha'],
        'git_tree': manifest['git_tree'],
        'file_count': checked,
        'all_git_blobs_sha256_and_modes_unchanged': True,
        'before_manifest_sha256': hashlib.sha256(manifest_bytes).hexdigest(),
        'scope': 'Tracked Git files only; compiler output is stored outside this archive.',
    }
    args.output.write_text(json.dumps(result, indent=2) + '\n')
    print(json.dumps(result))


if __name__ == '__main__':
    main()

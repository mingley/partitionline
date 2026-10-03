#!/usr/bin/env python3
"""Archive a pushed Git source and verify every extracted file against its blob ID."""
import argparse
import hashlib
import json
from pathlib import Path
import subprocess
import tarfile
import tempfile


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--repo', type=Path, default=Path('/workspace/partitionline'))
    parser.add_argument('--commit', required=True)
    parser.add_argument('--destination', type=Path, required=True)
    parser.add_argument('--evidence', type=Path, required=True)
    args = parser.parse_args()
    source = subprocess.check_output(['git', '-C', str(args.repo), 'rev-parse', args.commit + '^{commit}'], text=True).strip()
    assert len(source) == 40
    assert not args.destination.exists() and not args.evidence.exists()
    args.destination.parent.mkdir(parents=True, exist_ok=True)
    args.destination.mkdir()
    records = subprocess.check_output(['git', '-C', str(args.repo), 'ls-tree', '-rz', '--full-tree', source])
    expected = {}
    for record in records.split(b'\0'):
        if not record:
            continue
        identity, name = record.split(b'\t', 1)
        mode, kind, blob = identity.split(b' ')
        assert kind == b'blob' and mode in (b'100644', b'100755', b'120000')
        path = name.decode('utf-8')
        assert not path.startswith('/') and '..' not in Path(path).parts
        expected[path] = (mode.decode(), blob.decode())
    with tempfile.TemporaryDirectory(prefix='archive-read-', dir=args.destination.parent) as scratch:
        archive = Path(scratch) / 'source.tar'
        with archive.open('wb') as output:
            subprocess.run(['git', '-C', str(args.repo), 'archive', '--format=tar', source], stdout=output, check=True)
        with tarfile.open(archive) as bundle:
            bundle.extractall(args.destination, filter='data')
    identities = []
    for name, (mode, blob) in sorted(expected.items()):
        path = args.destination / name
        if mode == '120000':
            assert path.is_symlink()
            data = str(path.readlink()).encode('utf-8')
        else:
            assert path.is_file() and not path.is_symlink()
            data = path.read_bytes()
        actual = hashlib.sha1(b'blob ' + str(len(data)).encode('ascii') + b'\0' + data).hexdigest()
        assert actual == blob, name
        identities.append({'path': name, 'git_blob': blob, 'bytes': len(data), 'sha256': hashlib.sha256(data).hexdigest()})
    actual_paths = {str(path.relative_to(args.destination)) for path in args.destination.rglob('*')
                    if path.is_file() or path.is_symlink()}
    assert actual_paths == set(expected)
    args.evidence.parent.mkdir(parents=True, exist_ok=True)
    args.evidence.write_text(json.dumps({'source_sha': source, 'source_archive': str(args.destination),
                                        'verified_file_count': len(identities), 'every_git_blob_matches': True,
                                        'files': identities}, indent=2) + '\n')
    print(json.dumps({'source_sha': source, 'verified_file_count': len(identities), 'every_git_blob_matches': True}))


if __name__ == '__main__':
    main()

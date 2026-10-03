#!/usr/bin/env python3
"""Verify the packet, or restore its exact raw proof beneath an empty directory."""
import argparse
import gzip
import hashlib
import json
import os
from pathlib import Path
import stat


def digest(data):
    return hashlib.sha256(data).hexdigest()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--destination', type=Path,
                        help='Optional absent/empty directory; restores raw proof namespaces there')
    args = parser.parse_args()
    packet = Path(__file__).resolve().parent
    receipt = json.loads((packet / 'receipt.json').read_bytes())
    compressed_map = (packet / 'restore-map.json.gz').read_bytes()
    assert digest(compressed_map) == receipt['restore_map_gzip_sha256']
    raw_map = gzip.decompress(compressed_map)
    assert len(raw_map) == receipt['restore_map_raw_bytes']
    assert digest(raw_map) == receipt['restore_map_raw_sha256']
    mapping = json.loads(raw_map)
    cache = {}
    for obj in mapping['objects']:
        relative = Path(obj['path'])
        assert not relative.is_absolute() and '..' not in relative.parts
        packed = (packet / relative).read_bytes()
        assert len(packed) == obj['gzip_bytes'] and digest(packed) == obj['gzip_sha256']
        data = gzip.decompress(packed)
        assert len(data) == obj['raw_bytes'] and digest(data) == obj['raw_sha256']
        assert not data.startswith(b'\x7fELF')
        cache[obj['path']] = data
    dest = args.destination
    if dest is not None:
        assert not dest.is_symlink()
        assert not dest.exists() or not any(dest.iterdir()), 'destination must be empty'
        dest.mkdir(parents=True, exist_ok=True)
    seen = set()
    for f in mapping['files']:
        rel = Path(f['path'])
        assert not rel.is_absolute() and '..' not in rel.parts
        assert f['root'] in mapping['original_roots']
        key = (f['root'], f['path'])
        assert key not in seen
        seen.add(key)
        data = cache[f['object']]
        assert len(data) == f['bytes'] and digest(data) == f['sha256']
        if dest is not None:
            path = dest / f['root'] / rel
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_bytes(data)
            path.chmod(f['full_mode'])
            assert path.read_bytes() == data
            assert stat.S_IMODE(path.stat().st_mode) == f['full_mode']
    if dest is not None:
        for d in sorted(mapping['directories'], key=lambda d: len(Path(d['path']).parts), reverse=True):
            rel = Path(d['path'])
            assert not rel.is_absolute() and '..' not in rel.parts
            path = dest / d['root'] / rel
            path.mkdir(parents=True, exist_ok=True)
            path.chmod(d['full_mode'])
            assert stat.S_IMODE(path.stat().st_mode) == d['full_mode']
        actual_files = {(p.relative_to(dest).parts[0], str(Path(*p.relative_to(dest).parts[1:])))
                        for p in dest.rglob('*') if p.is_file()}
        assert actual_files == seen
    print(json.dumps({'passed': True, 'files': len(seen), 'objects': len(cache),
                      'restored': dest is not None,
                      'compiled_artifacts_restored': 0}))


if __name__ == '__main__':
    main()

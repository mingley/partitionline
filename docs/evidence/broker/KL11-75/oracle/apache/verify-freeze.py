#!/usr/bin/env python3
"""Verify frozen independent oracle and fixture bytes, modes and corpus inventory."""
import argparse
import hashlib
import json
from pathlib import Path


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--root', type=Path, default=Path(__file__).resolve().parents[6])
    args = parser.parse_args()
    path = args.root / 'docs/evidence/broker/KL11-75/oracle/apache/source-freeze.json'
    manifest = json.loads(path.read_text())
    for name, expected in manifest['inputs'].items():
        relative = Path(name)
        assert not relative.is_absolute() and '..' not in relative.parts
        actual = args.root / relative
        assert actual.is_file() and not actual.is_symlink(), name
        raw = actual.read_bytes()
        assert len(raw) == expected['bytes'] and hashlib.sha256(raw).hexdigest() == expected['sha256'], name
        assert ('100755' if actual.stat().st_mode & 0o111 else '100644') == expected['mode'], name
    prefix = 'partitionline-broker/tests/fixtures/records-compacted/'
    expected = {name for name in manifest['inputs'] if name.startswith(prefix)}
    actual = {str(p.relative_to(args.root)) for p in (args.root / prefix).rglob('*') if p.is_file()}
    assert expected == actual and len(expected) == 114
    print(json.dumps({'passed': True, 'checked_inputs': len(manifest['inputs']), 'fixture_files': 114,
                      'manifest_sha256': hashlib.sha256(path.read_bytes()).hexdigest()}))


if __name__ == '__main__':
    main()

#!/usr/bin/env python3
"""Verify the additive native seek input freeze, preserving the original freeze."""
import hashlib
import json
import stat
from pathlib import Path


def main():
    root = Path(__file__).resolve().parent
    manifest_path = root / 'source-freeze-native-seek-v3.json'
    manifest = json.loads(manifest_path.read_text())
    total = 0
    names = set()
    for row in manifest['files']:
        relative = Path(row['path'])
        if relative.is_absolute() or '..' in relative.parts or row['path'] in names:
            raise ValueError('Unique scoped relative input required')
        names.add(row['path'])
        path = root / relative
        if path.is_symlink() or not path.is_file():
            raise ValueError('Regular frozen input required')
        raw = path.read_bytes()
        if len(raw) != row['bytes'] or hashlib.sha256(raw).hexdigest() != row['sha256']:
            raise ValueError('Frozen input bytes changed: ' + row['path'])
        if stat.S_IMODE(path.stat().st_mode) != row['full_mode']:
            raise ValueError('Frozen full07777 mode changed: ' + row['path'])
        if path.suffix in ('.class', '.jar', '.pyc') or raw.startswith(b'\x7fELF'):
            raise ValueError('Compiled artifacts belong outside Git')
        total += len(raw)
    if total != manifest['input_bytes'] or len(names) != manifest['input_files']:
        raise ValueError('Complete input count and size required')
    print(json.dumps({'passed': True, 'input_files': len(names), 'input_bytes': total,
                      'source_freeze_sha256': hashlib.sha256(manifest_path.read_bytes()).hexdigest(),
                      'actual_live_jobs': 0}), flush=True)


if __name__ == '__main__':
    main()

#!/usr/bin/env python3
"""Verify stored and reconstructed bytes in a runtime evidence archive."""
import argparse
import gzip
import hashlib
import json
from pathlib import Path


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('directory', type=Path)
    args = parser.parse_args()
    root = args.directory.resolve()
    inventory = json.loads((root/'archive-inventory.json').read_text())
    for row in inventory['files']:
        path = (root/row['stored_path']).resolve()
        if not path.is_relative_to(root) or not path.is_file():
            raise ValueError('missing file or invalid archive path')
        with path.open('rb') as stream:
            digest = hashlib.file_digest(stream, 'sha256').hexdigest()
        if digest != row['stored_sha256'] or path.stat().st_size != row['stored_bytes']:
            raise ValueError('stored bytes differ: '+str(path))
        opener = gzip.open if row['encoding'] == 'gzip' else open
        if row['encoding'] not in ('gzip', 'identity'):
            raise ValueError('unsupported encoding')
        digest = hashlib.sha256(); count = 0
        with opener(path, 'rb') as stream:
            while chunk := stream.read(1024*1024):
                digest.update(chunk); count += len(chunk)
        if digest.hexdigest() != row['original_sha256'] or count != row['original_bytes']:
            raise ValueError('reconstructed bytes differ: '+str(path))
    print(len(inventory['files']), 'stored and reconstructed files verified')


if __name__ == '__main__':
    main()

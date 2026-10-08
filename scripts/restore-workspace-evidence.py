#!/usr/bin/env python3
"""Verify the publication archive and optionally restore selected evidence paths."""
import argparse
import collections
import gzip
import hashlib
import io
import json
from pathlib import Path, PurePosixPath
import re
import shutil
import tarfile
import tempfile


def digest(path):
    with path.open('rb') as stream:
        return hashlib.file_digest(stream, 'sha256').hexdigest()


def relative_path(name):
    path = PurePosixPath(name)
    if not isinstance(name, str) or not name or '\x00' in name or path.is_absolute() or '..' in path.parts or str(path) != name:
        raise ValueError('invalid relative path')
    return path


class Parts(io.RawIOBase):
    """Read consecutive bounded files as one stream without a temporary archive."""
    def __init__(self, paths):
        super().__init__()
        self.paths = iter(paths)
        self.stream = None

    def readable(self):
        return True

    def readinto(self, buffer):
        while True:
            if self.stream is None:
                path = next(self.paths, None)
                if path is None:
                    return 0
                self.stream = path.open('rb')
            count = self.stream.readinto(buffer)
            if count:
                return count
            self.stream.close()
            self.stream = None

    def close(self):
        if self.stream is not None:
            self.stream.close()
        super().close()


def verify(root, destination=None, prefix=None):
    root = root.resolve()
    summary = json.loads((root / 'summary.json').read_text())
    manifest_path = root / 'manifest.json.gz'
    if digest(manifest_path) != summary['manifest_sha256']:
        raise ValueError('manifest checksum mismatch')
    with gzip.open(manifest_path, 'rt') as stream:
        manifest = json.load(stream)
    if manifest['schema_version'] != 1:
        raise ValueError('unsupported manifest version')
    paths = []
    for index, row in enumerate(manifest['chunks'], 1):
        if row['path'] != f'chunks/part-{index:04d}.gz':
            raise ValueError('invalid chunk sequence')
        path = root / row['path']
        if not path.resolve().is_relative_to(root) or path.stat().st_size != row['bytes'] or digest(path) != row['sha256']:
            raise ValueError('chunk checksum mismatch')
        paths.append(path)
    if not paths:
        raise ValueError('empty chunk list')
    expected = {}
    selected = collections.defaultdict(list)
    names = set()
    selected_count = 0
    if destination is not None:
        destination = destination.resolve()
    if prefix is not None:
        prefix = str(relative_path(prefix)).rstrip('/')
    for row in manifest['files']:
        name = str(relative_path(row['path']))
        sha = row['sha256']
        if name in names or re.fullmatch('[0-9a-f]{64}', sha) is None or type(row['bytes']) is not int or row['bytes'] < 0 or type(row['mode']) is not int or not 0 <= row['mode'] <= 0o777:
            raise ValueError('invalid or repeated manifest entry')
        names.add(name)
        if expected.setdefault(sha, row['bytes']) != row['bytes']:
            raise ValueError('inconsistent object size')
        if destination is not None and (prefix is None or name == prefix or name.startswith(prefix + '/')):
            path = destination / name
            if not path.resolve().is_relative_to(destination):
                raise ValueError('destination escapes restoration root')
            if path.exists():
                raise FileExistsError(path)
            selected[sha].append((path, row['mode']))
            selected_count += 1
    if len(names) != summary['files'] or len(expected) != summary['objects']:
        raise ValueError('summary counts differ from manifest')
    seen = set()
    restored = 0
    with io.BufferedReader(Parts(paths)) as stream, tarfile.open(fileobj=stream, mode='r|gz') as archive:
        for member in archive:
            sha = member.name
            if not member.isfile() or sha not in expected or sha in seen or member.size != expected[sha]:
                raise ValueError('invalid, unexpected or repeated archive member')
            seen.add(sha)
            source = archive.extractfile(member)
            if source is None:
                raise ValueError('missing archive member')
            hasher = hashlib.sha256()
            count = 0
            # Spool only requested objects; verification itself needs no disk space.
            with tempfile.TemporaryFile() if sha in selected else io.BytesIO() as spool:
                while chunk := source.read(1024 * 1024):
                    hasher.update(chunk)
                    count += len(chunk)
                    if sha in selected:
                        spool.write(chunk)
                if hasher.hexdigest() != sha or count != member.size:
                    raise ValueError('reconstructed object checksum mismatch')
                for path, mode in selected[sha]:
                    path.parent.mkdir(parents=True, exist_ok=True)
                    if not path.resolve().is_relative_to(destination):
                        raise ValueError('destination escapes restoration root')
                    spool.seek(0)
                    with path.open('xb') as output:
                        shutil.copyfileobj(spool, output)
                    path.chmod(mode)
                    restored += 1
    if seen != set(expected):
        raise ValueError('missing archive objects')
    if restored != selected_count:
        raise ValueError('restoration count mismatch')
    return {'verified_files': len(names), 'verified_objects': len(seen), 'verified_chunks': len(paths), 'restored_files': restored}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('archive', type=Path)
    parser.add_argument('--destination', type=Path, help='Restore below this directory; existing files are refused')
    parser.add_argument('--prefix', help='Restore only this original path or directory; all archive bytes are verified')
    args = parser.parse_args()
    if args.prefix and args.destination is None:
        parser.error('--prefix requires --destination')
    print(json.dumps(verify(args.archive, args.destination, args.prefix), sort_keys=True))


if __name__ == '__main__':
    main()

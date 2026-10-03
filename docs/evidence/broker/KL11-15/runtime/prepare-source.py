#!/usr/bin/env python3
"""Extract one exact Git source and prove every tracked blob/mode before QA."""
import argparse
import hashlib
import json
from pathlib import Path
import re
import subprocess


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--repository', type=Path, required=True)
    parser.add_argument('--source-sha', required=True)
    parser.add_argument('--source', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    assert re.fullmatch('[0-9a-f]{40}', args.source_sha)
    args.source.mkdir(parents=True, exist_ok=False)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    archive = subprocess.Popen(['git', 'archive', args.source_sha],
                               cwd=args.repository, stdout=subprocess.PIPE)
    extracted = subprocess.run(['tar', '-x', '-C', str(args.source.resolve())],
                               stdin=archive.stdout)
    archive.stdout.close()
    assert archive.wait() == 0 and extracted.returncode == 0
    tree = subprocess.check_output(['git', 'rev-parse', args.source_sha + '^{tree}'],
                                   cwd=args.repository, text=True).strip()
    entries = subprocess.check_output(['git', 'ls-tree', '-r', '-z', args.source_sha],
                                      cwd=args.repository)
    files = []
    for entry in entries.rstrip(b'\0').split(b'\0'):
        meta, name = entry.split(b'\t', 1)
        mode, kind, expected = meta.decode('ascii').split()
        name = name.decode('utf-8')
        assert kind == 'blob' and mode in ('100644', '100755')
        path = args.source / name
        assert not path.is_symlink()
        data = path.read_bytes()
        blob = b'blob ' + str(len(data)).encode('ascii') + b'\0' + data
        assert hashlib.sha1(blob).hexdigest() == expected, name
        assert bool(path.stat().st_mode & 0o111) == (mode == '100755'), name
        files.append({'path': name, 'mode': mode, 'git_blob': expected,
                      'sha256': hashlib.sha256(data).hexdigest(), 'bytes': len(data)})
    args.output.write_text(json.dumps({'schema_version': 1,
        'source_sha': args.source_sha, 'git_tree': tree, 'file_count': len(files),
        'files': files, 'all_git_blobs_and_modes_match': True}, indent=2) + '\n')
    print(json.dumps({'source_sha': args.source_sha, 'git_tree': tree,
                      'verified_files': len(files), 'verified_bytes': sum(f['bytes'] for f in files)}))


if __name__ == '__main__':
    main()

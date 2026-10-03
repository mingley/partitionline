#!/usr/bin/env python3
"""Archive a pushed commit and bind every extracted file to its git blob."""
from pathlib import Path
import hashlib
import json
import os
import shutil
import subprocess
import sys

repo = Path('/workspace/partitionline')
sha, destination, receipt = sys.argv[1:4]
base = sys.argv[4:]
assert len(base) in (0, 2)
assert len(sha) == 40 and all(c in '0123456789abcdef' for c in sha)
source = Path(destination)
assert not source.exists(), f'fresh archive required: {source}'
remote = subprocess.check_output(['git', 'ls-remote', 'origin', 'refs/heads/main'], cwd=repo, text=True).split()[0]
subprocess.run(['git', 'merge-base', '--is-ancestor', sha, remote], cwd=repo, check=True)
archive_command = ['git', 'archive', sha]
if base:
    base_sha, base_directory = base
    shutil.copytree(base_directory, source, copy_function=os.link, symlinks=True)
    changed = subprocess.check_output(['git', 'diff', '--name-only', '-z', base_sha, sha], cwd=repo).split(b'\0')
    present = set(subprocess.check_output(['git', 'ls-tree', '-rz', '--name-only', sha], cwd=repo).split(b'\0'))
    additions = []
    for name in changed:
        if not name:
            continue
        path = source / name.decode()
        if path.is_file() or path.is_symlink():
            path.unlink()  # Break the old hardlink before extracting changed data.
        if name in present:
            additions.append(name.decode())
    archive_command.extend(['--', *additions])
else:
    source.mkdir(parents=True)
if not base:
    commands = [['git', 'archive', sha]]
else:
    commands = []
    chunk = []
    chunk_bytes = 0
    for addition in additions:
        size = len(os.fsencode(addition)) + 1
        if chunk and chunk_bytes + size > 32768:
            commands.append(['git', '--literal-pathspecs', 'archive', sha, '--', *chunk])
            chunk = []
            chunk_bytes = 0
        chunk.append(addition)
        chunk_bytes += size
    if chunk:
        commands.append(['git', '--literal-pathspecs', 'archive', sha, '--', *chunk])
for command in commands:
    archive = subprocess.Popen(command, cwd=repo, stdout=subprocess.PIPE)
    subprocess.run(['tar', '-x', '-C', str(source)], stdin=archive.stdout, check=True)
    archive.stdout.close()
    assert archive.wait() == 0
rows = subprocess.check_output(['git', 'ls-tree', '-rz', sha], cwd=repo).split(b'\0')
files = {}
for row in rows:
    if not row:
        continue
    metadata, name = row.split(b'\t', 1)
    mode, kind, blob = metadata.decode().split()
    assert kind == 'blob', (kind, name)
    relative = name.decode()
    path = source / relative
    data = path.readlink().as_posix().encode() if path.is_symlink() else path.read_bytes()
    digest = hashlib.sha1(b'blob ' + str(len(data)).encode() + b'\0' + data).hexdigest()
    assert digest == blob, relative
    files[relative] = {'mode': mode, 'git_blob': blob, 'sha256': hashlib.sha256(data).hexdigest(), 'bytes': len(data)}
output = Path(receipt)
output.parent.mkdir(parents=True, exist_ok=True)
output.write_text(json.dumps({'schema_version': 1, 'source_sha': sha, 'remote_main_at_archive': remote, 'source_directory': str(source), 'verified_files': len(files), 'verified_bytes': sum(f['bytes'] for f in files.values()), 'files': files}, indent=2) + '\n')
print(f'PASS: {len(files)} exact git blobs from pushed {sha}')

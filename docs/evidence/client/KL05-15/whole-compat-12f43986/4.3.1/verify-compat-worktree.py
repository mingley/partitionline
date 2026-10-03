from pathlib import Path
import hashlib
import json
import os
import subprocess
import sys

receipt, worktree_name, output_name, phase = sys.argv[1:]
worktree = Path(worktree_name)
output = Path(output_name)
subprocess.run([
    sys.executable,
    '/workspace/work/client-share-assessment/verify-existing-source.py',
    receipt, str(worktree), str(output), phase,
], check=True)
result = json.loads(output.read_text())
assert result['source_sha'] == '12f4398662044947ae653f07923290127457f2ef'
assert result['verified_git_blobs_and_modes'] == 69144
assert len(result['all_frozen_source_sha256']) == 12
expected = json.loads(Path(receipt).read_text())['files']
actual = set()
for directory, dirs, files in os.walk(worktree, followlinks=False):
    for name in files:
        relative = str((Path(directory) / name).relative_to(worktree))
        if relative != '.git':
            actual.add(relative)
    for name in list(dirs):
        path = Path(directory) / name
        if path.is_symlink():
            actual.add(str(path.relative_to(worktree)))
            dirs.remove(name)
assert actual == set(expected), {
    'extra': sorted(actual - set(expected)),
    'missing': sorted(set(expected) - actual),
}
shared_work = []
for name in actual:
    info = (worktree / name).lstat()
    try:
        original = (Path('/workspace/partitionline') / name).lstat()
    except FileNotFoundError:
        continue
    if info.st_dev == original.st_dev and info.st_ino == original.st_ino:
        shared_work.append(name)
assert not shared_work, shared_work
git = lambda *args: subprocess.check_output(
    ['git', '-C', str(worktree), *args], text=True
).strip()
assert git('rev-parse', 'HEAD') == result['source_sha']
assert git('status', '--porcelain', '--untracked-files=normal') == ''
detached = subprocess.run(
    ['git', '-C', str(worktree), 'symbolic-ref', '-q', 'HEAD'],
    capture_output=True, text=True,
)
assert detached.returncode == 1 and not detached.stdout
result['execution_worktree'] = {
    'path': str(worktree),
    'head': git('rev-parse', 'HEAD'),
    'clean_status': git('status', '--porcelain', '--untracked-files=normal'),
    'detached_head': True,
    'git_common_dir': git('rev-parse', '--git-common-dir'),
    'git_metadata_excluded_from_codepath_guard': ['.git'],
    'git_metadata_file_sha256': hashlib.sha256((worktree / '.git').read_bytes()).hexdigest(),
    'exact_canonical_codepath_set': True,
    'repo_work_shared_file_inodes': shared_work,
}
output.write_text(json.dumps(result, indent=2) + '\n')
print('PASS: detached clean 12f4 Git execution worktree, all 69,144 canonical paths/bytes/modes and no REPO WORK source inode sharing')

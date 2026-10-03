from pathlib import Path
import hashlib
import datetime
import json
import os
import stat
import subprocess
import sys

receipt, worktree_name, output_name, phase = sys.argv[1:]
worktree = Path(worktree_name)
output = Path(output_name)
raw_receipt = Path(receipt).read_bytes()
assert hashlib.sha256(raw_receipt).hexdigest() == 'afb3408c23640591767c3110112f993bdb5597401af99c8a75001fd30985c564'
audit = json.loads(raw_receipt)
assert audit['source_sha'] == '12f4398662044947ae653f07923290127457f2ef'
expected = audit['files']
assert len(expected) == 69144
tree = subprocess.check_output(['git', '-C', str(worktree), 'ls-tree', '-rz', 'HEAD']).split(b'\0')
git_rows = {}
for row in tree:
    if not row:
        continue
    metadata, name = row.split(b'\t', 1)
    mode, kind, blob = metadata.decode().split()
    assert kind == 'blob'
    git_rows[name.decode()] = (mode, blob)
assert set(git_rows) == set(expected)
mode_counts = {}
for name, row in expected.items():
    assert git_rows[name] == (row['mode'], row['git_blob']), name
    path = worktree / name
    info = path.lstat()
    assert stat.S_ISLNK(info.st_mode) == (row['mode'] == '120000'), name
    if row['mode'] != '120000':
        assert stat.S_ISREG(info.st_mode) and bool(info.st_mode & 0o111) == (row['mode'] == '100755'), name
    data = path.readlink().as_posix().encode() if row['mode'] == '120000' else path.read_bytes()
    assert hashlib.sha256(data).hexdigest() == row['sha256'], name
    assert hashlib.sha1(b'blob ' + str(len(data)).encode() + b'\0' + data).hexdigest() == row['git_blob'], name
    mode_counts[row['mode']] = mode_counts.get(row['mode'], 0) + 1
base = json.loads((worktree / 'docs/evidence/client/KL05-15/msrv-lint-correction/source-freeze-msrv-lint-correction.json').read_text())
frozen = {**base['source_sha256'], **base['unchanged_held_source_sha256']}
for name in [
    'docs/evidence/client/KL05-15/share-heartbeat-scheduling-correction/source-freeze-share-heartbeat-scheduling-correction.json',
    'docs/evidence/client/KL05-14/msrv-oracle-lint-correction/source-freeze-msrv-oracle-lint-correction.json',
]:
    frozen.update(json.loads((worktree / name).read_text())['source_sha256'])
assert len(frozen) == 12
for name, digest in frozen.items():
    assert hashlib.sha256((worktree / name).read_bytes()).hexdigest() == digest, name
result = {
    'schema_version': 2, 'phase': phase, 'source_sha': audit['source_sha'],
    'source_path': str(worktree), 'captured_at': datetime.datetime.now(datetime.timezone.utc).isoformat(),
    'verified_git_blobs_and_modes': len(expected), 'git_modes': mode_counts,
    'archive_receipt_sha256': hashlib.sha256(raw_receipt).hexdigest(),
    'all_frozen_source_sha256': frozen,
    'actual_detached_git_tree_rows_verified': len(git_rows),
    'remote_query_metadata': {
        'available': False, 'query_not_repeated': True,
        'known_first_exit_code': 128,
        'classification': 'HTTPS Git credential expiry prevented optional remote-main metadata. This does not replace any local source, Git tree, index, mode, frozen-hash, or image guard. No remote HEAD equality is claimed.',
    },
}
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

#!/usr/bin/env python3
"""Materialize exact Git files from an explicitly pinned immutable baseline.

This additive driver never changes baseline/origin/worktree or cleans partial outputs.
Hardlinks share only verified bytes/modes; new blobs use owned0600/0700 files.
"""
import argparse
import gzip
import hashlib
import json
import os
from pathlib import Path
import re
import stat
import subprocess

p = argparse.ArgumentParser()
p.add_argument('--source', required=True)
p.add_argument('--destination', required=True)
p.add_argument('--receipt-directory', required=True)
p.add_argument('--baseline-commit', required=True)
p.add_argument('--baseline-root', required=True)
p.add_argument('--baseline-origin', required=True)
p.add_argument('--baseline-origin-sha256', required=True)
a = p.parse_args()
repo = Path('/workspace/partitionline').resolve()
work = Path('/workspace/work').resolve()
base = Path(a.baseline_root).resolve()
origin_path = Path(a.baseline_origin).resolve()
destination = Path(a.destination).resolve()
out = Path(a.receipt_directory).resolve()
FLOOR = 350 * 1024 * 1024
MAX_BLOB = 32 * 1024 * 1024
MAX_MANIFEST = 128 * 1024 * 1024
MAX_ENTRIES = 1_000_000
disk = {'floor_bytes': FLOOR, 'guard_samples': 0, 'minimum_sampled_free_bytes': None,
        'action': 'refuse the next write at reserve; retain partial outputs, never clean sources'}
sha256 = lambda data: hashlib.sha256(data).hexdigest()
driver_hash = sha256(Path(__file__).read_bytes())
assert re.fullmatch('[0-9a-f]{40}', a.source)
assert re.fullmatch('[0-9a-f]{40}', a.baseline_commit)
assert re.fullmatch('[0-9a-f]{64}', a.baseline_origin_sha256)
for target in (base, origin_path, destination, out):
    assert target.is_relative_to(work) and not target.is_relative_to(repo), target
for left, right in ((destination, base), (out, base), (destination, out)):
    assert not left.is_relative_to(right) and not right.is_relative_to(left), (left, right)
assert base.is_dir() and origin_path.is_file()
assert not destination.exists() and not out.exists()
assert destination.parent.is_dir() and out.parent.is_dir()
assert base.stat().st_dev == destination.parent.stat().st_dev == out.parent.stat().st_dev == work.stat().st_dev


def guard_write(upper_bound=0):
    """Check shared free space before each allocation/write and after completion."""
    v = os.statvfs(work)
    free = v.f_bavail * v.f_frsize
    disk['guard_samples'] += 1
    old = disk['minimum_sampled_free_bytes']
    disk['minimum_sampled_free_bytes'] = free if old is None else min(old, free)
    assert free >= FLOOR + upper_bound, ('350MiB materialization reserve', free, upper_bound)
    return free


def git_blob(data):
    return hashlib.sha1(b'blob ' + str(len(data)).encode() + b'\0' + data).hexdigest()


def git_tree(commit):
    resolved = subprocess.check_output(['git', 'rev-parse', '--verify', commit + '^{commit}'],
                                       cwd=repo, text=True).strip()
    assert resolved == commit
    raw = subprocess.check_output(['git', 'ls-tree', '-r', '-z', commit], cwd=repo)
    entries = {}
    for row in raw.split(b'\0'):
        if not row:
            continue
        meta, encoded_name = row.split(b'\t', 1)
        mode, kind, blob = meta.decode().split()
        name = encoded_name.decode()
        assert kind == 'blob' and mode in ('100644', '100755'), (name, mode, kind)
        assert not Path(name).is_absolute() and all(part not in ('', '.', '..') for part in name.split('/'))
        assert name not in entries and len(entries) < MAX_ENTRIES
        entries[name] = (mode, blob)
    return entries


def worktree_inodes():
    """Detect same-file or cross-path aliasing with tracked/untracked WORK files."""
    inodes = set()
    for directory, dirs, files in os.walk(repo, followlinks=False):
        if Path(directory) == repo:
            dirs[:] = [name for name in dirs if name != '.git']
        for name in files:
            path = Path(directory) / name
            try:
                s = path.lstat()
            except FileNotFoundError:
                continue
            if stat.S_ISREG(s.st_mode):
                inodes.add((s.st_dev, s.st_ino))
    return inodes


origin_bytes = origin_path.read_bytes()
origin_mode = stat.S_IMODE(origin_path.stat().st_mode)
assert sha256(origin_bytes) == a.baseline_origin_sha256
origin = json.loads(origin_bytes)
assert origin['source_commit'] == a.baseline_commit
assert Path(origin['source_directory']).resolve() == base
manifest_pin = origin['source_manifest']
manifest_path = Path(manifest_pin['path']).resolve()
assert manifest_path.is_relative_to(work) and manifest_path.is_file()
assert not manifest_path.is_relative_to(destination) and not manifest_path.is_relative_to(out)
compressed = manifest_path.read_bytes()
manifest_mode = stat.S_IMODE(manifest_path.stat().st_mode)
assert sha256(compressed) == manifest_pin['compressed_sha256']
assert manifest_mode == manifest_pin['compressed_full_mode']
with gzip.open(manifest_path, 'rb') as f:
    encoded_base = f.read(MAX_MANIFEST + 1)
assert len(encoded_base) <= MAX_MANIFEST
assert len(encoded_base) == manifest_pin['uncompressed_bytes']
assert sha256(encoded_base) == manifest_pin['uncompressed_sha256']
base_pin = json.loads(encoded_base)
base_entries = git_tree(a.baseline_commit)
assert len(base_pin) == origin['verified_files']
assert set(base_pin) == set(base_entries)
for name, pin in base_pin.items():
    assert (pin['mode'], pin['git_blob_sha1']) == base_entries[name], name
    assert 0 <= pin['bytes'] <= MAX_BLOB
    assert pin['full_permission_mode'] & 0o7777 == pin['full_permission_mode']
assert sum(pin['bytes'] for pin in base_pin.values()) == origin['verified_bytes']


def verify_base():
    actual = {str(f.relative_to(base)) for f in base.rglob('*') if f.is_file() or f.is_symlink()}
    assert actual == set(base_entries)
    work_inodes = worktree_inodes()
    for name, pin in base_pin.items():
        f = base / name
        s = f.lstat()
        assert stat.S_ISREG(s.st_mode) and not f.is_symlink(), name
        actual_git_mode = '100755' if s.st_mode & 0o111 else '100644'
        assert actual_git_mode == pin['mode'], (name, actual_git_mode, pin['mode'])
        data = f.read_bytes()
        assert sha256(data) == pin['sha256'] and git_blob(data) == pin['git_blob_sha1'], name
        assert len(data) == pin['bytes'] and stat.S_IMODE(s.st_mode) == pin['full_permission_mode'], name
        assert (s.st_dev, s.st_ino) not in work_inodes, ('baseline aliases live WORK', name)
    assert origin_path.read_bytes() == origin_bytes
    assert stat.S_IMODE(origin_path.stat().st_mode) == origin_mode
    assert manifest_path.read_bytes() == compressed
    assert stat.S_IMODE(manifest_path.stat().st_mode) == manifest_mode
    return {'files': len(base_pin), 'git_path_set_and_every_git_blob_verified': True,
            'all_origin_full_permission_modes_verified': True,
            'tracked_and_untracked_worktree_inode_disjoint': True,
            'origin_and_manifest_bytes_and_modes_unchanged': True}


baseline_before = verify_base()
entries = git_tree(a.source)
# Metadata-only preflight fetches no file bytes and writes no output tree.
unique_blobs = sorted({blob for _, blob in entries.values()})
checked = subprocess.check_output(['git', 'cat-file', '--batch-check=%(objectname) %(objecttype) %(objectsize)'],
                                 input=''.join(blob + '\n' for blob in unique_blobs).encode(), cwd=repo)
sizes = {}
for line in checked.decode().splitlines():
    blob, kind, value = line.split()
    size = int(value)
    assert kind == 'blob' and 0 <= size <= MAX_BLOB
    assert blob not in sizes
    sizes[blob] = size
assert set(sizes) == set(unique_blobs)
owned_mode = lambda mode: 0o700 if mode == '100755' else 0o600
reusable = {name for name, (mode, blob) in entries.items()
            if (old := base_pin.get(name)) and old['mode'] == mode
            and old['git_blob_sha1'] == blob and old['full_permission_mode'] == owned_mode(mode)}
new_bytes = sum(sizes[blob] for name, (_, blob) in entries.items() if name not in reusable)
directory_names = {str(parent) for name in entries for parent in Path(name).parents if str(parent) != '.'}
v = os.statvfs(work)
block = max(v.f_frsize, 4096)
round_block = lambda size: ((size + block - 1) // block) * block
new_allocated = sum(round_block(sizes[blob]) for name, (_, blob) in entries.items() if name not in reusable)
# Conservatively charge one filesystem block per entry plus every directory.
entry_reserve = (len(entries) + len(directory_names) + 2) * block
def exact_audit_length(tree_entries, blob_sizes, mode_for):
    # Metadata-only: every actual SHA256 is exactly 64 unescaped ASCII hex
    # characters. Exact names (including JSON escaping), integer digit counts,
    # mode/blob fields, sorting and separators match the final inventory dump.
    skeleton = {name: {'mode': mode, 'git_blob_sha1': blob, 'sha256': '0' * 64,
                       'bytes': blob_sizes[blob], 'full_permission_mode': mode_for(mode)}
                for name, (mode, blob) in tree_entries.items()}
    return len(json.dumps(skeleton, sort_keys=True, separators=(',', ':')).encode())


def stored_gzip_upper(size):
    assert 0 <= size <= MAX_MANIFEST
    # <=16383 payload bytes +5 bytes per stored block, with 64 bytes for
    # wrapper/final-block/slack. This also dominates default zlib deflateBound
    # plus the 18-byte gzip wrapper throughout the admitted <=128MiB domain:
    # ceil(n/16383)*5 >= floor(n/4096)+floor(n/16384), and
    # floor(n/33554432)+25 <=29 <64. gzip.compress uses its unchanged defaults.
    upper = size + ((size + 16382) // 16383) * 5 + 64
    default_zlib_gzip_bound = size + (size >> 12) + (size >> 14) + (size >> 25) + 25
    assert upper >= default_zlib_gzip_bound
    return upper


audit_upper = exact_audit_length(entries, sizes, owned_mode)
assert audit_upper <= MAX_MANIFEST
raw_audit_allocated = round_block(audit_upper)
gzip_upper = stored_gzip_upper(audit_upper)
gzip_allocated = round_block(gzip_upper)
# Exactly one retained raw JSON and one retained gzip are written. The same
# 1MiB reserve covers the bounded receipt and minimal extra file metadata;
# entry/directory reserves and the 350MiB floor are unchanged.
metadata_reserve = 1_048_576
forecast_bytes = new_allocated + entry_reserve + raw_audit_allocated + gzip_allocated + metadata_reserve
forecast = {'new_blob_bytes': new_bytes, 'new_allocated_file_upper_bound_bytes': new_allocated,
            'hardlink_and_directory_entry_reserve_bytes': entry_reserve,
            'raw_audit_upper_bound_bytes': audit_upper, 'raw_audit_exact_encoded_bytes': audit_upper,
            'raw_audit_allocated_upper_bound_bytes': raw_audit_allocated,
            'gzip_upper_bound_bytes': gzip_upper, 'gzip_allocated_upper_bound_bytes': gzip_allocated,
            'receipt_and_minimal_metadata_reserve_bytes': metadata_reserve,
            'metadata_and_verification_reserve_bytes': raw_audit_allocated + gzip_allocated + metadata_reserve,
            'forecast_output_bytes': forecast_bytes, 'required_free_bytes': FLOOR + forecast_bytes,
            'sampled_free_bytes': guard_write(forecast_bytes), 'minimum_remaining_free_bytes': FLOOR}
guard_write(2 * block)
destination.mkdir()
out.mkdir()
batch = subprocess.Popen(['git', 'cat-file', '--batch'], cwd=repo,
                         stdin=subprocess.PIPE, stdout=subprocess.PIPE)
inventory = {}
copied_bytes = 0
work_inodes_before = worktree_inodes()
try:
    for name, (mode, blob) in entries.items():
        target = destination / name
        full_mode = owned_mode(mode)
        guard_write((len(Path(name).parents) + 1) * block)
        target.parent.mkdir(parents=True, exist_ok=True)
        if name in reusable:
            source_file = base / name
            # Recheck the live row directly before linking; never chmod a link.
            s = source_file.lstat()
            assert stat.S_ISREG(s.st_mode) and stat.S_IMODE(s.st_mode) == full_mode
            assert (s.st_dev, s.st_ino) not in work_inodes_before
            assert git_blob(source_file.read_bytes()) == blob
            guard_write(block)
            os.link(source_file, target)
        else:
            batch.stdin.write((blob + '\n').encode())
            batch.stdin.flush()
            header = batch.stdout.readline().decode().split()
            assert header == [blob, 'blob', str(sizes[blob])], header
            data = batch.stdout.read(sizes[blob])
            assert len(data) == sizes[blob] and batch.stdout.read(1) == b'\n' and git_blob(data) == blob
            guard_write(round_block(len(data)) + block)
            with target.open('xb') as f:
                assert os.fstat(f.fileno()).st_nlink == 1
                os.fchmod(f.fileno(), full_mode)  # Fresh owned inode only.
                for offset in range(0, len(data), 1_048_576):
                    chunk = data[offset:offset + 1_048_576]
                    guard_write(round_block(len(chunk)) + block)
                    assert f.write(chunk) == len(chunk)
                    guard_write()
            copied_bytes += len(data)
        data = target.read_bytes()
        s = target.lstat()
        assert stat.S_ISREG(s.st_mode) and git_blob(data) == blob
        assert stat.S_IMODE(s.st_mode) == full_mode
        assert (s.st_dev, s.st_ino) not in work_inodes_before
        inventory[name] = {'mode': mode, 'git_blob_sha1': blob, 'sha256': sha256(data),
                           'bytes': len(data), 'full_permission_mode': full_mode}
        guard_write()
finally:
    batch.stdin.close()
    batch.stdout.close()
    assert batch.wait() == 0
baseline_after = verify_base()
actual = {str(f.relative_to(destination)) for f in destination.rglob('*') if f.is_file() or f.is_symlink()}
assert actual == set(entries) and copied_bytes == new_bytes
work_inodes_after = worktree_inodes()
for name, pin in inventory.items():
    f = destination / name
    s = f.lstat()
    assert stat.S_ISREG(s.st_mode) and (s.st_dev, s.st_ino) not in work_inodes_after
    assert stat.S_IMODE(s.st_mode) == pin['full_permission_mode']
    assert git_blob(f.read_bytes()) == pin['git_blob_sha1']
encoded = json.dumps(inventory, sort_keys=True, separators=(',', ':')).encode()
assert len(encoded) == audit_upper
raw_audit = out / 'complete-source.json'
guard_write(round_block(len(encoded)) + block)
with raw_audit.open('xb') as f:
    os.fchmod(f.fileno(), 0o600)
    assert f.write(encoded) == len(encoded)
raw_mode = stat.S_IMODE(raw_audit.stat().st_mode)
encoded_gzip = gzip.compress(encoded, mtime=0)
assert len(encoded_gzip) <= gzip_upper
audit = out / 'complete-source.json.gz'
guard_write(round_block(len(encoded_gzip)) + block)
with audit.open('xb') as f:
    os.fchmod(f.fileno(), 0o600)
    assert f.write(encoded_gzip) == len(encoded_gzip)
assert gzip.decompress(audit.read_bytes()) == encoded
assert raw_audit.read_bytes() == encoded and stat.S_IMODE(raw_audit.stat().st_mode) == raw_mode
# Unlike the old driver, retain the raw audit too: this additive driver deletes nothing.
assert sha256(Path(__file__).read_bytes()) == driver_hash
receipt = {'source_commit': a.source, 'source_directory': str(destination),
           'verified_files': len(inventory), 'verified_bytes': sum(pin['bytes'] for pin in inventory.values()),
           'source_origin': 'exact Git tree; explicit origin/Git/fullmode-verified immutable baseline links plus owned new blobs',
           'baseline_source': a.baseline_commit, 'baseline_origin_receipt': str(origin_path),
           'baseline_origin_receipt_sha256': a.baseline_origin_sha256,
           'baseline_root': str(base), 'baseline_manifest_sha256': manifest_pin['compressed_sha256'],
           'unchanged_files_hardlinked': len(reusable), 'changed_new_blob_bytes': copied_bytes,
           'complete_path_set_and_git_blobs_verified': True, 'all_full_modes_verified': True,
           'no_inode_shared_with_repository_worktree': True, 'baseline_before_after_verified': True,
           'baseline_before': baseline_before, 'baseline_after': baseline_after,
           'source_manifest': {'path': str(audit), 'compressed_sha256': sha256(audit.read_bytes()),
                               'uncompressed_sha256': sha256(encoded), 'uncompressed_bytes': len(encoded),
                               'compression': 'gzip', 'decompression_verified': True,
                               'restore_path': 'complete-source.json', 'original_full_mode': raw_mode,
                               'compressed_full_mode': stat.S_IMODE(audit.stat().st_mode),
                               'uncompressed_retained_path': str(raw_audit)},
           'write_forecast': forecast, 'disk_guard': disk, 'driver_sha256': driver_hash}
receipt_data = (json.dumps(receipt, indent=2) + '\n').encode()
guard_write(round_block(len(receipt_data)) + block)
with (out / 'receipt.json').open('xb') as f:
    os.fchmod(f.fileno(), 0o600)
    assert f.write(receipt_data) == len(receipt_data)
print(json.dumps(receipt), flush=True)

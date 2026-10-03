#!/usr/bin/env python3
"""Independent read-only exact-tree and full file-mode verification."""
import gzip
import hashlib
import json
import os
from pathlib import Path
import stat
import subprocess
import time

REPO = Path('/workspace/partitionline')
OUT = Path('/workspace/work/client-capability-qa-preparation-e90efb49')
RECEIPT = Path('/workspace/work/integration/client-capabilities-source-e90efb49/receipt.json')

def sha(data):
    return hashlib.sha256(data).hexdigest()

def pathset(root):
    result = {}
    for directory, dirs, files in os.walk(root, followlinks=False):
        dirs[:] = [name for name in dirs if name != '.git']
        for name in list(dirs):
            p = Path(directory) / name
            if p.is_symlink():
                files.append(name)
                dirs.remove(name)
        for name in files:
            p = Path(directory) / name
            result[p.relative_to(root).as_posix()] = p.lstat()
    return result

def gitset(commit):
    raw = subprocess.check_output(['git', '-C', str(REPO), 'ls-tree', '-r', '-z', commit])
    result = {}
    for row in raw.split(b'\0'):
        if row:
            prefix, path = row.split(b'\t', 1)
            mode, kind, blob = prefix.decode().split()
            assert kind == 'blob', (commit, path, kind)
            result[path.decode()] = (mode, blob)
    return result

def manifest(receipt):
    meta = receipt['source_manifest']
    p = Path(meta['path'])
    zipped = p.read_bytes()
    assert sha(zipped) == meta['compressed_sha256'], str(p)
    assert stat.S_IMODE(p.stat().st_mode) == meta['compressed_full_mode']
    raw = gzip.decompress(zipped)
    assert sha(raw) == meta['uncompressed_sha256']
    assert len(raw) == meta['uncompressed_bytes']
    if 'uncompressed_retained_path' in meta:
        q = Path(meta['uncompressed_retained_path'])
        assert q.read_bytes() == raw
        assert stat.S_IMODE(q.stat().st_mode) == meta['original_full_mode']
    return json.loads(raw), {'compressed_sha256': sha(zipped), 'uncompressed_sha256': sha(raw),
                             'uncompressed_bytes': len(raw), 'compressed_full_mode': stat.S_IMODE(p.stat().st_mode)}

def verify(receipt, expected, work_inodes):
    root = Path(receipt['source_directory'])
    actual = pathset(root)
    upstream = gitset(receipt['source_commit'])
    assert actual.keys() == expected.keys() == upstream.keys(), (
        sorted(actual.keys() - expected.keys())[:10], sorted(expected.keys() - actual.keys())[:10])
    digest = hashlib.sha256()
    size = 0
    inodes = {}
    for name in sorted(expected):
        info = expected[name]
        st = actual[name]
        assert stat.S_IMODE(st.st_mode) == info['full_permission_mode'], (name, 'fullmode')
        assert (info['mode'], info['git_blob_sha1']) == upstream[name], (name, 'manifest/git')
        inode = (st.st_dev, st.st_ino)
        assert inode not in work_inodes, (name, 'shares repository WORK inode')
        p = root / name
        if info['mode'] == '120000':
            assert stat.S_ISLNK(st.st_mode)
            data = os.fsencode(os.readlink(p))
        else:
            assert stat.S_ISREG(st.st_mode)
            assert info['mode'] in ('100644', '100755')
            assert bool(st.st_mode & 0o111) == (info['mode'] == '100755'), (name, 'Git executable class')
            data = p.read_bytes()
        assert len(data) == info['bytes']
        assert sha(data) == info['sha256'], (name, 'sha256')
        blob = hashlib.sha1(b'blob ' + str(len(data)).encode() + b'\0' + data).hexdigest()
        assert blob == upstream[name][1], (name, 'Git blob')
        digest.update(name.encode() + b'\0' + blob.encode() + b'\0' + str(stat.S_IMODE(st.st_mode)).encode() + b'\0')
        size += len(data)
        inodes[name] = inode
    assert len(actual) == receipt['verified_files']
    assert size == receipt['verified_bytes']
    return {'files': len(actual), 'bytes': size, 'complete_path_set_matches_git': True,
            'all_git_blobs_sha256_and_lengths_match': True, 'all_full_file_permission_modes_match': True,
            'all_git_executable_classes_match': True, 'no_repository_worktree_inodes_shared': True,
            'set_blob_fullmode_sha256': digest.hexdigest()}, inodes

started = time.time()
receipt_bytes = RECEIPT.read_bytes()
receipt = json.loads(receipt_bytes)
assert receipt['source_commit'] == 'e90efb494401fbf6c52998501e2d4b5a42471a73'
origin_path = Path(receipt['baseline_origin_receipt'])
origin_bytes = origin_path.read_bytes()
assert sha(origin_bytes) == receipt['baseline_origin_receipt_sha256']
origin = json.loads(origin_bytes)
assert origin['source_commit'] == receipt['baseline_source']
assert origin['source_directory'] == receipt['baseline_root']
assert origin['source_manifest']['compressed_sha256'] == receipt['baseline_manifest_sha256']
expected, manifest_report = manifest(receipt)
baseline_expected, baseline_manifest_report = manifest(origin)
work_inodes = {(s.st_dev, s.st_ino) for s in pathset(REPO).values()}
baseline_report, baseline_inodes = verify(origin, baseline_expected, work_inodes)
source_report, source_inodes = verify(receipt, expected, work_inodes)
hardlinked = sum(source_inodes.get(name) == inode for name, inode in baseline_inodes.items())
assert hardlinked == receipt['unchanged_files_hardlinked']
new_bytes = sum(info['bytes'] for name, info in expected.items()
                if source_inodes[name] != baseline_inodes.get(name))
assert new_bytes == receipt['changed_new_blob_bytes']
stage_path = Path('/workspace/work/conformance-reconciliation-14/capability-source-stage-1a3a91c6/stage-manifest.json')
stage_bytes = stage_path.read_bytes()
assert sha(stage_bytes) == 'b4c102dc8edfdeb511ce3980bd432c6c21fd7d7a60e8775a2ec7cef3b433cb53'
report = {'schema_version': 1, 'source_sha': receipt['source_commit'], 'passed': True,
          'review_scope': 'read-only independent exact Git path/blob/length/SHA-256/full-file-mode and inode review; no compiler or runtime execution',
          'receipt_path': str(RECEIPT), 'receipt_sha256': sha(receipt_bytes),
          'origin_receipt_sha256': sha(origin_bytes), 'source_manifest': manifest_report,
          'baseline_manifest': baseline_manifest_report, 'baseline_current_verification': baseline_report,
          'source_verification': source_report, 'verified_hardlinked_files': hardlinked,
          'verified_new_blob_bytes': new_bytes, 'owned_stage_manifest_sha256': sha(stage_bytes),
          'limitations': ['Historical materializer before/after timing is a hash-bound origin claim; this review independently checks their present exact bytes and modes.',
                          'File full modes are verified; directory permission modes are outside the materializer manifest.',
                          'No Cargo/JVM/Kafka/issuer launch or product mutation performed.'],
          'elapsed_seconds': time.time() - started,
          'reviewer_script_sha256': sha(Path(__file__).read_bytes()),
          'command': ['taskset', '-c', '2,4', 'python3', str(Path(__file__))]}
(OUT / 'source-review.json').write_text(json.dumps(report, indent=2) + '\n')
print(json.dumps({'passed': True, 'files': source_report['files'], 'bytes': source_report['bytes'],
                  'receipt_sha256': report['receipt_sha256'], 'source_set_digest': source_report['set_blob_fullmode_sha256'],
                  'elapsed_seconds': report['elapsed_seconds']}))

#!/usr/bin/env python3
"""Qualify a complete immutable Git snapshot; keep every gate/source receipt."""
import argparse
import hashlib
import io
import json
import os
from pathlib import Path
import re
import shutil
import subprocess
import tarfile
import time

P = argparse.ArgumentParser()
P.add_argument('--source', required=True)
P.add_argument('--repository', default='/workspace/partitionline')
P.add_argument('--scratch', required=True)
A = P.parse_args()
os.sched_setaffinity(0, {0, 1, 2, 4})
REPO = Path(A.repository)
OUT = Path(A.scratch)
OUT.mkdir(parents=True, exist_ok=False)
TREE = OUT / 'source'
TREE.mkdir()
ENV = dict(os.environ, CARGO_HOME='/workspace/work/cargo', RUSTUP_HOME='/workspace/work/rustup',
           PATH='/workspace/work/cargo/bin:' + os.environ['PATH'], CARGO_INCREMENTAL='0',
           CARGO_BUILD_JOBS='1', CARGO_PROFILE_DEV_DEBUG='0', CARGO_PROFILE_TEST_DEBUG='0',
           CARGO_TARGET_DIR='/workspace/work/target-broker-segments')
sha = subprocess.check_output(['git', 'rev-parse', A.source + '^{commit}'], cwd=REPO, text=True).strip()
archive = subprocess.check_output(['git', 'archive', '--format=tar', sha], cwd=REPO)
with tarfile.open(fileobj=io.BytesIO(archive)) as tar:
    tar.extractall(TREE, filter='data')
expected = {}
raw = subprocess.check_output(['git', 'ls-tree', '-r', '-z', sha], cwd=REPO)
for item in raw.split(b'\0'):
    if not item:
        continue
    meta, name = item.split(b'\t', 1)
    mode, kind, blob = meta.decode().split()
    assert kind == 'blob', (mode, kind, name)
    path = name.decode()
    file = TREE / path
    data = os.readlink(file).encode() if mode == '120000' else file.read_bytes()
    actual = hashlib.sha1(b'blob ' + str(len(data)).encode() + b'\0' + data).hexdigest()
    assert actual == blob, path
    expected[path] = {'mode': mode, 'git_blob_sha1': blob, 'sha256': hashlib.sha256(data).hexdigest(), 'bytes': len(data)}
(OUT / 'source-integrity.json').write_text(json.dumps({'source_commit': sha, 'file_count': len(expected), 'files': expected}, indent=2) + '\n')

def verify():
    actual_paths = {str(p.relative_to(TREE)) for p in TREE.rglob('*') if p.is_file() or p.is_symlink()}
    assert actual_paths == set(expected), {'missing': sorted(set(expected) - actual_paths), 'extra': sorted(actual_paths - set(expected))}
    hashes = {}
    for name, pin in expected.items():
        path = TREE / name
        data = os.readlink(path).encode() if pin['mode'] == '120000' else path.read_bytes()
        blob = hashlib.sha1(b'blob ' + str(len(data)).encode() + b'\0' + data).hexdigest()
        assert blob == pin['git_blob_sha1'], name
        hashes[name] = blob
    encoded = json.dumps(hashes, sort_keys=True, separators=(',', ':')).encode()
    return {'file_count': len(hashes), 'all_git_blobs_match': True, 'set_and_blob_sha256': hashlib.sha256(encoded).hexdigest()}

receipt = {'schema': 1, 'source_commit': sha, 'archive_sha256': hashlib.sha256(archive).hexdigest(),
           'source_manifest_sha256': hashlib.sha256((OUT / 'source-integrity.json').read_bytes()).hexdigest(),
           'scope': 'complete immutable source; real ordinary rolling serving and process/IO histories, not physical power loss or full Kafka qualification',
           'environment': {k: ENV[k] for k in ('CARGO_HOME', 'RUSTUP_HOME', 'CARGO_TARGET_DIR', 'CARGO_INCREMENTAL', 'CARGO_BUILD_JOBS', 'CARGO_PROFILE_DEV_DEBUG', 'CARGO_PROFILE_TEST_DEBUG')},
           'commands': [], 'toolchains': {}, 'retained_binaries': []}
for tc in ('stable', '1.85.0'):
    receipt['toolchains'][tc] = subprocess.check_output(['rustc', '+' + tc, '-Vv'], env=ENV, text=True)
commands = [('clean-before-source-switch', ['cargo', '+stable', 'clean', '--manifest-path', 'partitionline-broker/Cargo.toml', '--target-dir', ENV['CARGO_TARGET_DIR']], {})]
commands += [('format', ['cargo', '+stable', 'fmt', '--manifest-path', 'partitionline-broker/Cargo.toml', '--check'], {})]
for tc in ('stable', '1.85.0'):
    if tc == '1.85.0':
        commands.append(('clean-between-toolchains', ['cargo', '+stable', 'clean', '--manifest-path', 'partitionline-broker/Cargo.toml', '--target-dir', ENV['CARGO_TARGET_DIR']], {}))
    prefix = ['cargo', '+' + tc]
    manifest = ['--locked', '--manifest-path', 'partitionline-broker/Cargo.toml']
    for feature, flags in (('default', []), ('all-features', ['--all-features'])):
        commands += [
            (tc + '-' + feature + '-all-targets', [*prefix, 'test', *manifest, *flags, '--all-targets', '--', '--test-threads=1', '--nocapture'], {'capture': True}),
            (tc + '-' + feature + '-strict-clippy', [*prefix, 'clippy', *manifest, *flags, '--all-targets', '--', '-D', 'warnings'], {}),
            (tc + '-' + feature + '-strict-doc', [*prefix, 'doc', *manifest, *flags, '--no-deps'], {'RUSTDOCFLAGS': '-D warnings'}),
            (tc + '-' + feature + '-strict-doctest', [*prefix, 'test', *manifest, *flags, '--doc'], {'RUSTDOCFLAGS': '-D warnings'})]
for name, argv, extra in commands:
    before = verify()
    env = dict(ENV)
    env.update({k: v for k, v in extra.items() if k != 'capture'})
    lane = OUT / name
    lane.mkdir()
    if extra.get('capture'):
        mappings = {'PARTITIONLINE_WIRE_REPORT': 'wire-report.json', 'PARTITIONLINE_METADATA_REPORT': 'metadata-report.json',
                    'PARTITIONLINE_PRODUCE_REPORT': 'produce-report.json', 'PARTITIONLINE_FETCH_REPORT': 'fetch-report.json',
                    'PARTITIONLINE_SEGMENTS_REPORT': 'rolling-report.json', 'PARTITIONLINE_SEGMENTS_ORACLE_REPORT': 'apache-router-report.json',
                    'PARTITIONLINE_SEGMENTS_RESPONSE_DIR': 'responses', 'PARTITIONLINE_SEGMENTS_PROOF_DIR': 'proof',
                    'PARTITIONLINE_SEGMENTS_FAULT_DIR': 'fault-histories'}
        env.update({k: str(lane / v) for k, v in mappings.items()})
    started = time.time()
    with (lane / 'command.log').open('w') as log:
        code = subprocess.call(['taskset', '-c', '0-2,4', *argv], cwd=TREE, env=env, stdout=log, stderr=subprocess.STDOUT)
    after = verify()
    item = {'name': name, 'argv': ['taskset', '-c', '0-2,4', *argv], 'environment_additions': {k: v for k, v in env.items() if ENV.get(k) != v},
            'exit_code': code, 'elapsed_seconds': round(time.time() - started, 3), 'source_before': before,
            'source_after': after, 'log_sha256': hashlib.sha256((lane / 'command.log').read_bytes()).hexdigest()}
    receipt['commands'].append(item)
    if code == 0 and name.endswith('default-all-targets'):
        output = (lane / 'command.log').read_text()
        matches = re.findall(r'Running tests/fetch.rs \(([^)]+)\)', output)
        assert len(matches) == 1, matches
        binary = Path(matches[0])
        if not binary.is_absolute():
            binary = TREE / binary
        destination = OUT / 'bin' / name.split('-default-')[0] / 'fetch'
        destination.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(binary, destination)
        receipt['retained_binaries'].append({'lane': name, 'path': str(destination.relative_to(OUT)), 'sha256': hashlib.sha256(destination.read_bytes()).hexdigest(), 'source_commit': sha, 'build_command': item['argv']})
    (OUT / 'validation.json').write_text(json.dumps(receipt, indent=2) + '\n')
    print(name, code, flush=True)
    if code:
        raise SystemExit(code)
receipt['final_source'] = verify()
(OUT / 'validation.json').write_text(json.dumps(receipt, indent=2) + '\n')

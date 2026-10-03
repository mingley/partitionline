#!/usr/bin/env python3
"""Record dirty-development checks; these are not immutable release qualification."""
import hashlib
import json
import os
from pathlib import Path
import subprocess
import time

ROOT = Path('/workspace/partitionline')
OUT = Path('/workspace/work/segments-development/focused-oracle-candidate')
OWNED = [f'partitionline-broker/{p}' for p in (
    'src/segments.rs', 'src/journal.rs', 'src/partition.rs', 'src/produce.rs', 'src/fetch.rs',
    'tests/segments.rs', 'tests/partition.rs', 'tests/produce.rs', 'tests/fetch.rs')]
OUT.mkdir(parents=True, exist_ok=True)
ENV = dict(os.environ, CARGO_HOME='/workspace/work/cargo', RUSTUP_HOME='/workspace/work/rustup',
           PATH='/workspace/work/cargo/bin:' + os.environ['PATH'], CARGO_INCREMENTAL='0',
           CARGO_BUILD_JOBS='1', CARGO_PROFILE_DEV_DEBUG='0', CARGO_PROFILE_TEST_DEBUG='0',
           CARGO_TARGET_DIR='/workspace/work/target-broker-segments')

def owned():
    return {p: hashlib.sha256((ROOT / p).read_bytes()).hexdigest() for p in OWNED}

baseline = owned()
receipt = {'schema_version': 1, 'qualification': 'dirty-development-only',
           'head': subprocess.check_output(['git', 'rev-parse', 'HEAD'], cwd=ROOT, text=True).strip(),
           'dirty_status': subprocess.check_output(['git', 'status', '--porcelain'], cwd=ROOT, text=True),
           'owned_sources': baseline, 'commands': [], 'environment': {k: ENV[k] for k in (
               'CARGO_HOME', 'RUSTUP_HOME', 'CARGO_INCREMENTAL', 'CARGO_BUILD_JOBS',
               'CARGO_PROFILE_DEV_DEBUG', 'CARGO_PROFILE_TEST_DEBUG', 'CARGO_TARGET_DIR')}}
for toolchain in ('stable', '1.85.0'):
    receipt.setdefault('toolchains', {})[toolchain] = subprocess.check_output(
        ['rustc', '+' + toolchain, '-Vv'], env=ENV, text=True)
commands = [('format-owned', ['rustfmt', '--edition', '2021', '--check', *OWNED])]
for tc in ('stable', '1.85.0'):
    if tc == '1.85.0':
        commands.append(('clean-between-toolchains', ['cargo', '+stable', 'clean', '--manifest-path', 'partitionline-broker/Cargo.toml', '--target-dir', ENV['CARGO_TARGET_DIR']]))
    prefix = ['cargo', '+' + tc]
    manifest = ['--locked', '--manifest-path', 'partitionline-broker/Cargo.toml']
    commands += [
        (tc + '-fault-histories', [*prefix, 'test', *manifest, '--lib', 'segments::tests', '--', '--test-threads=1', '--nocapture']),
        (tc + '-segments', [*prefix, 'test', *manifest, '--test', 'segments', '--', '--test-threads=1', '--nocapture']),
        (tc + '-apache-router', [*prefix, 'test', *manifest, '--test', 'fetch', 'authentic_apache_rolled_records', '--', '--test-threads=1', '--nocapture']),
        (tc + '-strict-clippy-default', [*prefix, 'clippy', *manifest, '--all-targets', '--', '-D', 'warnings']),
        (tc + '-strict-clippy-all', [*prefix, 'clippy', *manifest, '--all-targets', '--all-features', '--', '-D', 'warnings']),
        (tc + '-strict-doc-all', [*prefix, 'doc', *manifest, '--all-features', '--no-deps'])]
for name, argv in commands:
    assert owned() == baseline, 'owned source changed before ' + name
    before = owned()
    started = time.time()
    env = dict(ENV)
    if 'strict-doc' in name:
        env['RUSTDOCFLAGS'] = '-D warnings'
    if name.startswith('stable-') and any(key in name for key in ('fault-histories', 'segments', 'apache-router')):
        env['PARTITIONLINE_SEGMENTS_FAULT_DIR'] = str(OUT / 'fault-histories')
        env['PARTITIONLINE_SEGMENTS_ORACLE_REPORT'] = str(OUT / 'apache-router-report.json')
        env['PARTITIONLINE_SEGMENTS_REPORT'] = str(OUT / 'compiled-report.json')
        env['PARTITIONLINE_SEGMENTS_RESPONSE_DIR'] = str(OUT / 'responses')
        env['PARTITIONLINE_SEGMENTS_PROOF_DIR'] = str(OUT / 'proof')
    with (OUT / (name + '.log')).open('w') as log:
        code = subprocess.call(['taskset', '-c', '0-2,4', *argv], cwd=ROOT, env=env, stdout=log, stderr=subprocess.STDOUT)
    after = owned()
    item = {'name': name, 'argv': ['taskset', '-c', '0-2,4', *argv], 'exit_code': code,
            'elapsed_seconds': round(time.time() - started, 3), 'sources_before': before,
            'sources_after': after, 'log_sha256': hashlib.sha256((OUT / (name + '.log')).read_bytes()).hexdigest()}
    receipt['commands'].append(item)
    (OUT / 'validation.json').write_text(json.dumps(receipt, indent=2) + '\n')
    print(name, code, flush=True)
    assert after == baseline, 'owned source changed during ' + name
    if code:
        raise SystemExit(code)

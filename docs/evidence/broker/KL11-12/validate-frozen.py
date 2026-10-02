#!/usr/bin/env python3
"""Run and retain exact immutable election-card gates; no shared target."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import platform
import subprocess
import time

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument('snapshot', type=Path)
parser.add_argument('source_sha')
parser.add_argument('output', type=Path)
parser.add_argument('toolchain', choices=['stable', '1.85.0'])
parser.add_argument('target', type=Path)
args = parser.parse_args()
assert len(args.source_sha) == 40 and all(c in '0123456789abcdef' for c in args.source_sha)
args.snapshot = args.snapshot.resolve()
args.output = args.output.resolve()
args.target = args.target.resolve()
args.output.mkdir(exist_ok=True, parents=True)
manifest = args.snapshot / 'partitionline-broker/Cargo.toml'
env = dict(os.environ)
env.update(CARGO_HOME='/workspace/work/cargo', RUSTUP_HOME='/workspace/work/rustup',
           PATH='/workspace/work/cargo/bin:' + env['PATH'], CARGO_INCREMENTAL='0',
           CARGO_BUILD_JOBS='2', CARGO_PROFILE_DEV_DEBUG='0', CARGO_PROFILE_TEST_DEBUG='0',
           CARGO_TARGET_DIR=str(args.target), RUSTDOCFLAGS='-D warnings', PYTHONDONTWRITEBYTECODE='1')
env.pop('PL_RAFT_HISTORY_DIR', None)
prefix = ['taskset', '-c', '0-2,4', 'cargo', '+' + args.toolchain]
common = ['--locked', '--manifest-path', str(manifest)]
commands = [
    ('clean-broker', ['clean', '--manifest-path', str(manifest), '-p', 'partitionline-broker']),
    ('default-election-tests', ['test'] + common + ['--lib', '--test', 'raft_election']),
    ('allfeature-election-tests', ['test'] + common + ['--all-features', '--lib', '--test', 'raft_election']),
    ('fmt', ['fmt', '--manifest-path', str(manifest), '--all', '--', '--check']),
    ('strict-clippy', ['clippy'] + common + ['--all-features', '--all-targets', '--', '-D', 'warnings']),
    ('strict-rustdoc', ['doc'] + common + ['--all-features', '--no-deps']),
    ('strict-default-doctests', ['test'] + common + ['--doc']),
    ('strict-allfeature-doctests', ['test'] + common + ['--all-features', '--doc']),
]
paths = list((args.snapshot / 'partitionline-broker').rglob('*'))
paths += [args.snapshot / 'clippy.toml', args.snapshot / 'docs/evidence/broker/KL11-03/verify-format.py']
paths += list((args.snapshot / 'docs/evidence/broker/KL11-12').rglob('*'))
paths = sorted(p for p in paths if p.is_file() and '__pycache__' not in p.parts)

def fingerprints():
    return {str(p.relative_to(args.snapshot)): hashlib.sha256(p.read_bytes()).hexdigest() for p in paths}

before = fingerprints()
toolchain = subprocess.run(['rustc', '+' + args.toolchain, '-Vv'], env=env, capture_output=True, text=True, check=True).stdout
report = {'source_sha': args.source_sha, 'kind': 'exact git archive; no draft overlay',
          'snapshot': str(args.snapshot), 'target': str(args.target), 'python': platform.python_version(),
          'rustc_verbose': toolchain, 'environment': {k: env[k] for k in ['CARGO_INCREMENTAL', 'CARGO_BUILD_JOBS',
          'CARGO_PROFILE_DEV_DEBUG', 'CARGO_PROFILE_TEST_DEBUG', 'RUSTDOCFLAGS']},
          'source_files_sha256': before, 'commands': [], 'production_qualification': False}
destination = args.output / (args.toolchain + '-results.json')
assert not destination.exists()
for name, command in commands:
    full = prefix + command
    log = args.output / (args.toolchain + '-' + name + '.log')
    step_env = dict(env)
    if name == 'default-election-tests':
        step_env['PL_RAFT_HISTORY_DIR'] = str(args.output / (args.toolchain + '-histories'))
    start = time.monotonic()
    with log.open('xb') as output:
        result = subprocess.run(full, cwd=args.snapshot, env=step_env, stdout=output, stderr=subprocess.STDOUT)
    row = {'name': name, 'argv': full, 'exit_code': result.returncode, 'seconds': round(time.monotonic() - start, 3),
           'log': log.name, 'log_sha256': hashlib.sha256(log.read_bytes()).hexdigest()}
    if name == 'default-election-tests':
        row['PL_RAFT_HISTORY_DIR'] = step_env['PL_RAFT_HISTORY_DIR']
    report['commands'].append(row)
    report['source_unchanged'] = fingerprints() == before
    destination.write_text(json.dumps(report, indent=2) + '\n')
    print(json.dumps(row), flush=True)
    assert report['source_unchanged'], 'immutable source modified'
    if result.returncode:
        raise SystemExit(result.returncode)
report['verdict'] = 'passed'
destination.write_text(json.dumps(report, indent=2) + '\n')

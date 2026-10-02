#!/usr/bin/env python3
"""Validate an immutable git-archive checkout; keep each command and raw result."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import subprocess

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument('--source', required=True, type=Path)
parser.add_argument('--source-sha', required=True)
parser.add_argument('--target', required=True, type=Path)
parser.add_argument('--out', required=True, type=Path)
args = parser.parse_args()
args.out.mkdir(parents=True, exist_ok=True)
env = dict(os.environ, CARGO_INCREMENTAL='0', CARGO_TARGET_DIR=str(args.target))
results = []


def run(name, argv, extra=None):
    process = subprocess.run(['taskset', '-c', '0-2,4'] + argv, cwd=args.source,
                             env=dict(env, **(extra or {})), capture_output=True, text=True)
    path = args.out / (name + '.log')
    path.write_text(process.stdout + process.stderr)
    row = {'name': name, 'argv': ['taskset', '-c', '0-2,4'] + argv,
           'cwd': str(args.source), 'env_overrides': extra or {}, 'exit': process.returncode,
           'log': path.name, 'log_sha256': hashlib.sha256(path.read_bytes()).hexdigest()}
    row['test_results'] = [dict(zip(('passed', 'failed', 'ignored'), map(int, values)))
                           for values in re.findall(r'test result: ok\. (\d+) passed; (\d+) failed; (\d+) ignored', path.read_text())]
    results.append(row)
    (args.out / 'results.json').write_text(json.dumps({'source_sha': args.source_sha,
        'source': str(args.source), 'commands': results, 'qualification': 'not_run'}, indent=2) + '\n')
    if process.returncode:
        raise SystemExit(f'{name} failed; retained {path}')
    print(name + ': passed', flush=True)


manifest = 'partitionline-broker/Cargo.toml'
for toolchain in ('stable', '1.85.0'):
    run(toolchain + '-format', ['cargo', '+' + toolchain, 'fmt', '--manifest-path', manifest, '--check'])
    for features in ('default', 'all-features'):
        feature_args = [] if features == 'default' else ['--all-features']
        report = args.out / (toolchain + '-' + features + '-handler.json')
        run(toolchain + '-' + features + '-tests', ['cargo', '+' + toolchain, 'test',
            '--manifest-path', manifest, '--all-targets', '--jobs', '2'] + feature_args,
            {'PARTITIONLINE_WIRE_REPORT': str(report)})
        run(toolchain + '-' + features + '-registry', ['python3', '-B',
            'scripts/check-broker-api-matrix.py', '--handler-report', str(report),
            '--report', str(args.out / (toolchain + '-' + features + '-gate.json'))])
    run(toolchain + '-strict-clippy', ['cargo', '+' + toolchain, 'clippy', '--manifest-path', manifest,
        '--all-features', '--all-targets', '--jobs', '2', '--', '-D', 'warnings'])
    run(toolchain + '-strict-docs', ['cargo', '+' + toolchain, 'doc', '--manifest-path', manifest,
        '--all-features', '--no-deps', '--jobs', '2'], {'RUSTDOCFLAGS': '-D warnings'})
run('python-guards', ['python3', '-B', '-m', 'unittest', 'tests.ci.test_broker_api_matrix', '-v'])

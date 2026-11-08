#!/usr/bin/env python3
"""Exploratory paired same-client timing against this repository's mock."""
import hashlib
import json
import os
import pathlib
import shutil
import statistics
import subprocess
import sys
import time

ROOT = pathlib.Path(os.environ['GITHUB_WORKSPACE'])
OUT = ROOT / 'completion-timing-results'
OUT.mkdir(exist_ok=True)
EXPECTED = {
    'baseline': '4409633fa989effe1083e623f6e59fcc0d179eee',
    'candidate': 'bc45ba4f65f126c406ff88a57fcfe08007d35b29',
    'fixture': '8371f09924118dd8c3a8098d4b0f7ee8b6c66e6c',
}
TEST = 'timed_admission_and_flush_with_mock_readback'

def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()

def call(args, cwd, env=None):
    result = subprocess.run(args, cwd=cwd, env=env, text=True, capture_output=True)
    if result.returncode:
        print('FAILED COMMAND', args, flush=True)
        print(result.stdout, flush=True)
        print(result.stderr, file=sys.stderr, flush=True)
        raise SystemExit(result.returncode)
    return result

for arm, commit in EXPECTED.items():
    observed = call(['git', 'rev-parse', 'HEAD'], ROOT / arm).stdout.strip()
    if observed != commit:
        raise SystemExit(f'{arm} head mismatch: {observed}')

driver = ROOT / 'control/docs/evidence/client/producer-completion-draft-20261108/completion_timing.rs'
fixture = ROOT / 'fixture/tests/common/mod.rs'
fingerprints = {'driver_sha256': sha(driver), 'fixture_sha256': sha(fixture)}
for arm in ['baseline', 'candidate']:
    worktree = ROOT / arm
    fingerprints[arm] = {
        'commit': EXPECTED[arm],
        'producer_sha256': sha(worktree / 'src/producer.rs'),
        'lock_sha256': sha(worktree / 'Cargo.lock'),
    }
    shutil.copyfile(driver, worktree / 'tests/completion_timing.rs')
    shutil.copyfile(fixture, worktree / 'tests/common/mod.rs')
    if sha(worktree / 'tests/completion_timing.rs') != fingerprints['driver_sha256']:
        raise SystemExit(f'{arm}: driver copy mismatch')
    if sha(worktree / 'tests/common/mod.rs') != fingerprints['fixture_sha256']:
        raise SystemExit(f'{arm}: fixture copy mismatch')
if fingerprints['baseline']['producer_sha256'] == fingerprints['candidate']['producer_sha256']:
    raise SystemExit('baseline and candidate producer sources are identical')
if fingerprints['baseline']['lock_sha256'] != fingerprints['candidate']['lock_sha256']:
    raise SystemExit('lockfiles differ')
print('PROVENANCE', json.dumps(fingerprints, sort_keys=True), flush=True)
print('RUSTFLAGS', repr(os.environ.get('RUSTFLAGS')), flush=True)
print('CARGO_ENCODED_RUSTFLAGS', repr(os.environ.get('CARGO_ENCODED_RUSTFLAGS')), flush=True)
print(call(['rustc', '+stable', '-Vv'], ROOT).stdout, flush=True)
print(call(['cargo', '+stable', '-V'], ROOT).stdout, flush=True)

binaries = {}
for arm in ['baseline', 'candidate']:
    worktree = ROOT / arm
    target = pathlib.Path(os.environ['RUNNER_TEMP']) / f'completion-timing-{arm}'
    env = os.environ.copy()
    env['CARGO_TARGET_DIR'] = str(target)
    command = ['cargo', '+stable', 'test', '--locked', '--release', '--test', 'completion_timing', '--no-run', '--message-format=json']
    built = call(command, worktree, env)
    (OUT / f'{arm}-build.stderr').write_text(built.stderr)
    executable = []
    for line in built.stdout.splitlines():
        try:
            item = json.loads(line)
        except json.JSONDecodeError:
            continue
        if item.get('reason') == 'compiler-artifact' and item.get('target', {}).get('name') == 'completion_timing' and item.get('executable'):
            executable.append(item['executable'])
    if len(executable) != 1:
        print(built.stdout, flush=True)
        print(built.stderr, file=sys.stderr, flush=True)
        raise SystemExit(f'{arm}: expected one test binary, got {len(executable)}')
    binary = pathlib.Path(executable[0])
    if not binary.is_file():
        raise SystemExit(f'missing test binary: {binary}')
    binaries[arm] = binary
    print('BINARY', json.dumps({'arm': arm, 'path': str(binary), 'sha256': sha(binary)}, sort_keys=True), flush=True)
if sha(binaries['baseline']) == sha(binaries['candidate']):
    raise SystemExit('baseline and candidate binaries are identical')

results = []
for round_number in range(8):
    order = ['baseline', 'candidate'] if round_number % 2 == 0 else ['candidate', 'baseline']
    for arm in order:
        command = [str(binaries[arm]), '--exact', TEST, '--nocapture', '--test-threads=1']
        started = time.monotonic()
        try:
            run = subprocess.run(command, cwd=ROOT / arm, text=True, capture_output=True, timeout=90)
            stdout, stderr, code = run.stdout, run.stderr, run.returncode
        except subprocess.TimeoutExpired as error:
            stdout = error.stdout.decode(errors='replace') if isinstance(error.stdout, bytes) else error.stdout or ''
            stderr = error.stderr.decode(errors='replace') if isinstance(error.stderr, bytes) else error.stderr or ''
            code = None
        elapsed = time.monotonic() - started
        (OUT / f'{arm}-{round_number}.stdout').write_text(stdout)
        (OUT / f'{arm}-{round_number}.stderr').write_text(stderr)
        matches = [line.split('TIMING_JSON ', 1)[1] for line in stdout.splitlines() if 'TIMING_JSON ' in line]
        valid = code == 0 and 'running 1 test' in stdout and len(matches) == 1
        timing = json.loads(matches[0]) if valid else None
        if timing is not None and timing.get('records') != 4096:
            valid = False
        observation = {'arm': arm, 'round': round_number, 'exit_code': code, 'elapsed_s': round(elapsed, 3), 'valid': valid, 'timing': timing}
        results.append(observation)
        print('OBSERVATION', json.dumps(observation, sort_keys=True), flush=True)
        if not valid:
            print(stdout, flush=True)
            print(stderr, file=sys.stderr, flush=True)

(OUT / 'results.json').write_text(json.dumps({'sources': fingerprints, 'results': results}, indent=2) + '\n')
if not all(result['valid'] for result in results):
    raise SystemExit('one or more samples failed correctness/readback; no timing comparison is valid')
for arm in ['baseline', 'candidate']:
    samples = [row['timing'] for row in results if row['arm'] == arm]
    for field in ['admission_ns', 'flush_ns']:
        values = sorted(row[field] for row in samples)
        print('DISTRIBUTION', json.dumps({'arm': arm, 'field': field, 'values_ns': values, 'median_ns': statistics.median(values)}, sort_keys=True), flush=True)
print('Exploratory same-client mock diagnostic; no Kafka implementation ranking.', flush=True)

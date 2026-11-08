#!/usr/bin/env python3
"""Matched diagnostic for one retained-backing control; not a benchmark."""
import hashlib
import json
import os
import pathlib
import subprocess
import sys
import time

ROOT = pathlib.Path(os.environ['GITHUB_WORKSPACE'])
OUT = ROOT / 'matched-retained-results'
OUT.mkdir(exist_ok=True)
EXPECTED = {
    'baseline': '4409633fa989effe1083e623f6e59fcc0d179eee',
    'candidate': 'bc45ba4f65f126c406ff88a57fcfe08007d35b29',
}
TEST = 'cancelled_channel_admission_releases_only_the_unenqueued_reservation'

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

fingerprints = {}
for arm, commit in EXPECTED.items():
    worktree = ROOT / arm
    observed = call(['git', 'rev-parse', 'HEAD'], worktree).stdout.strip()
    if observed != commit:
        raise SystemExit(f'{arm} head mismatch: {observed}')
    fingerprints[arm] = {
        'commit': observed,
        'producer_sha256': sha(worktree / 'src/producer.rs'),
        'test_sha256': sha(worktree / 'tests/producer_retained_backing.rs'),
        'lock_sha256': sha(worktree / 'Cargo.lock'),
    }
if fingerprints['baseline']['producer_sha256'] == fingerprints['candidate']['producer_sha256']:
    raise SystemExit('baseline and candidate producer sources are identical')
for field in ['test_sha256', 'lock_sha256']:
    if fingerprints['baseline'][field] != fingerprints['candidate'][field]:
        raise SystemExit(f'{field} differs across arms')
print('PROVENANCE', json.dumps(fingerprints, sort_keys=True), flush=True)
print(call(['rustc', '+stable', '-Vv'], ROOT).stdout, flush=True)

results = []
for mode in ['default', 'all']:
    binaries = {}
    for arm in ['baseline', 'candidate']:
        worktree = ROOT / arm
        target = pathlib.Path(os.environ['RUNNER_TEMP']) / f'retained-{arm}-{mode}'
        env = os.environ.copy()
        env['CARGO_TARGET_DIR'] = str(target)
        command = ['cargo', '+stable', 'test', '--locked']
        if mode == 'all':
            command.append('--all-features')
        command += ['--test', 'producer_retained_backing', '--no-run', '--message-format=json']
        built = call(command, worktree, env)
        (OUT / f'{arm}-{mode}-build.stderr').write_text(built.stderr)
        executable = []
        for line in built.stdout.splitlines():
            try:
                item = json.loads(line)
            except json.JSONDecodeError:
                continue
            if item.get('reason') == 'compiler-artifact' and item.get('target', {}).get('name') == 'producer_retained_backing' and item.get('executable'):
                executable.append(item['executable'])
        if len(executable) != 1:
            print(built.stdout, flush=True)
            print(built.stderr, file=sys.stderr, flush=True)
            raise SystemExit(f'{arm} {mode}: expected one test binary, got {len(executable)}')
        binary = pathlib.Path(executable[0])
        if not binary.is_file():
            raise SystemExit(f'missing test binary: {binary}')
        binaries[arm] = binary
        print('BINARY', json.dumps({'arm': arm, 'mode': mode, 'path': str(binary), 'sha256': sha(binary)}, sort_keys=True), flush=True)

    for round_number in range(3):
        order = ['baseline', 'candidate'] if round_number % 2 == 0 else ['candidate', 'baseline']
        for arm in order:
            command = [str(binaries[arm]), '--exact', TEST, '--nocapture', '--test-threads=1']
            started = time.monotonic()
            try:
                run = subprocess.run(command, cwd=ROOT / arm, text=True, capture_output=True, timeout=45)
                stdout, stderr, code = run.stdout, run.stderr, run.returncode
            except subprocess.TimeoutExpired as error:
                stdout = error.stdout.decode(errors='replace') if isinstance(error.stdout, bytes) else error.stdout or ''
                stderr = error.stderr.decode(errors='replace') if isinstance(error.stderr, bytes) else error.stderr or ''
                code = None
            elapsed = time.monotonic() - started
            (OUT / f'{arm}-{mode}-{round_number}.stdout').write_text(stdout)
            (OUT / f'{arm}-{mode}-{round_number}.stderr').write_text(stderr)
            if 'running 1 test' not in stdout:
                print(stdout, flush=True)
                print(stderr, file=sys.stderr, flush=True)
                raise SystemExit(f'{arm} {mode} round {round_number}: expected one test')
            observation = {'arm': arm, 'mode': mode, 'round': round_number, 'exit_code': code, 'elapsed_s': round(elapsed, 3)}
            results.append(observation)
            print('OBSERVATION', json.dumps(observation, sort_keys=True), flush=True)
            print(stdout, flush=True)
            print(stderr, file=sys.stderr, flush=True)

(OUT / 'results.json').write_text(json.dumps({'sources': fingerprints, 'results': results}, indent=2) + '\n')
print('Diagnostic complete. Job success means observations were collected, not that every test passed.', flush=True)

import gzip
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import subprocess
import time

repo = Path('/workspace/partitionline')
base = '0b9797ef166a5d067869be1fe988ee21051c86ad'
snapshot = Path('/workspace/work/broker-oidc/jwks-media-overlay-01')
out = repo / 'docs/evidence/broker/KL11-37/production/development/jwks-media-01'
expected = json.loads(gzip.decompress((out / 'source-before.json.gz').read_bytes()))['files']
env = os.environ.copy()
env.update(CARGO_HOME='/workspace/work/cargo', RUSTUP_HOME='/workspace/work/rustup', PATH='/workspace/work/cargo/bin:' + env['PATH'], CARGO_INCREMENTAL='0', CARGO_BUILD_JOBS='1', CARGO_PROFILE_DEV_DEBUG='0', CARGO_PROFILE_TEST_DEBUG='0', CARGO_TARGET_DIR='/workspace/work/broker-oidc/target')
results = []
checks = []
restored = {}

def verify(label, phase, overrides=None):
    overrides = overrides or {}
    aggregate = hashlib.sha256()
    for row in expected:
        path = snapshot / row['path']
        digest = hashlib.sha256(path.read_bytes()).hexdigest()
        mode = path.stat().st_mode & 0o777
        if digest != overrides.get(row['path'], row['sha256']) or mode != row['mode']:
            raise AssertionError('isolated source changed: ' + row['path'])
        aggregate.update(row['path'].encode() + b'\0' + digest.encode() + b'\0' + str(mode).encode() + b'\n')
    checks.append(dict(command=label, phase=phase, files=len(expected), bytes_and_full_modes_identical=True, aggregate_sha256=aggregate.hexdigest(), intentional_baseline_overrides=sorted(overrides)))
    (out / 'source-per-command.json').write_text(json.dumps(dict(base_source_sha=base, checks=checks), indent=2) + '\n')

def run(label, argv, expected_exit=0, overrides=None):
    if shutil.disk_usage('/workspace').free < 350 * 1024 * 1024:
        raise RuntimeError('HOLD before new command: less than 350 MiB free')
    verify(label, 'before', overrides)
    print('start ' + label, flush=True)
    log = out / (label + '.log')
    started = time.monotonic()
    with log.open('wb') as stream:
        result = subprocess.run(argv, cwd=snapshot, env=env, stdout=stream, stderr=subprocess.STDOUT)
    data = log.read_bytes()
    totals = re.findall(rb'test result: (?:ok|FAILED)\. (\d+) passed; (\d+) failed; (\d+) ignored; (\d+) measured; (\d+) filtered out', data)
    row = dict(label=label, argv=argv, exit_code=result.returncode, expected_exit=expected_exit, duration_s=time.monotonic() - started, log=log.name, sha256=hashlib.sha256(data).hexdigest(), free_bytes_after=shutil.disk_usage('/workspace').free)
    if totals:
        row['test_totals'] = dict(zip(['passed', 'failed', 'ignored', 'measured', 'filtered'], [sum(int(t[k]) for t in totals) for k in range(5)]))
    verify(label, 'after', overrides)
    results.append(row)
    (out / 'commands.json').write_text(json.dumps(dict(base_source_sha=base, qualification='development only: exactly four isolated HTTP media followup paths; no OAuth socket drafts or official peer claim', commands=results), indent=2) + '\n')
    print('end ' + label + ' exit=' + str(result.returncode), flush=True)
    if result.returncode != expected_exit:
        raise RuntimeError('unexpected command exit: ' + label)
    if expected_exit == 101 and not (row.get('test_totals', {}).get('failed') == 1 and b'registered_jwks_media_is_accepted_only_for_signing_keys ... FAILED' in data):
        raise RuntimeError('baseline control did not fail in the intended regression')
    return row

manifest = ['--manifest-path', str(snapshot / 'partitionline-broker/Cargo.toml')]
common = manifest + ['--locked', '--offline', '--no-default-features', '--features', 'oidc']
regression = ['--test', 'oidc_http', 'registered_jwks_media_is_accepted_only_for_signing_keys', '--', '--exact']
overrides = {}
try:
    for name in ['partitionline-broker/src/security/oidc/http.rs', 'partitionline-broker/src/security/oidc/cache.rs']:
        path = snapshot / name
        restored[name] = path.read_bytes()
        old = subprocess.check_output(['git', 'show', base + ':' + name], cwd=repo)
        path.write_bytes(old)
        overrides[name] = hashlib.sha256(old).hexdigest()
    run('baseline-media-regression', ['taskset', '-c', '2,4', 'cargo', '+stable', 'test'] + common + regression, 101, overrides)
finally:
    for name, data in restored.items():
        (snapshot / name).write_bytes(data)

for toolchain in ['stable', '1.85.0']:
    cargo = ['taskset', '-c', '2,4', 'cargo', '+' + toolchain]
    commands = [
        (toolchain + '-rustc', ['taskset', '-c', '2,4', 'rustc', '+' + toolchain, '-vV']),
        (toolchain + '-fmt', cargo + ['fmt'] + manifest + ['--', '--check']),
        (toolchain + '-oidc-tests', cargo + ['test'] + common + ['--lib', '--test', 'oidc_http', '--test', 'oidc_validation']),
        (toolchain + '-oidc-clippy', cargo + ['clippy'] + common + ['--all-targets', '--', '-Dwarnings']),
    ]
    for label, argv in commands:
        run(label, argv)
run('restored-media-regression', ['taskset', '-c', '2,4', 'cargo', '+stable', 'test'] + common + regression)
print(json.dumps(dict(commands=len(results), expected_baseline_failure_retained=True, candidate_commands_passed=True)), flush=True)

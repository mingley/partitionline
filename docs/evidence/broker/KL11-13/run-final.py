"""Run only after coordinator pins implementation plus authentic fixtures."""
import hashlib
import json
import os
import pathlib
import re
import subprocess
import sys
import time

source = pathlib.Path(sys.argv[1]).resolve()
source_sha = sys.argv[2]
output = pathlib.Path(sys.argv[3]).resolve()
label = sys.argv[4]
toolchain = '1.85.0' if label == 'msrv' else 'stable'
assert re.fullmatch('[0-9a-f]{40}', source_sha)
output.mkdir(parents=True, exist_ok=False)
integrity = json.loads((source.parent / 'source-integrity.json').read_text())
assert integrity['source_sha'] == source_sha
def verify():
    for name, expected in integrity['files_sha256'].items():
        assert hashlib.sha256((source / name).read_bytes()).hexdigest() == expected, name
verify()
env = os.environ.copy()
env.update(CARGO_HOME='/workspace/work/cargo', RUSTUP_HOME='/workspace/work/rustup',
           CARGO_INCREMENTAL='0', CARGO_PROFILE_DEV_DEBUG='0', CARGO_PROFILE_TEST_DEBUG='0',
           CARGO_TARGET_DIR='/workspace/work/target-raft-wire-msrv' if label == 'msrv' else '/workspace/work/target-raft-wire')
env['PATH'] = '/workspace/work/cargo/bin:' + env['PATH']
receipt = {'source_sha': source_sha, 'toolchain': toolchain, 'cwd': str(source),
           'source_integrity': str(source.parent / 'source-integrity.json'), 'commands': []}
def retain():
    (output / 'summary.json').write_text(json.dumps(receipt, indent=2) + '\n')
def run(name, args, extra=None):
    local = env.copy()
    if extra:
        local.update(extra)
    command = ['taskset', '-c', '0-2,4'] + args
    log = output / (name + '.log')
    row = {'name': name, 'command': command, 'env': {k: local[k] for k in
           ('CARGO_HOME', 'RUSTUP_HOME', 'CARGO_TARGET_DIR', 'CARGO_INCREMENTAL', 'CARGO_PROFILE_DEV_DEBUG', 'CARGO_PROFILE_TEST_DEBUG')}, 'status': 'running'}
    if extra:
        row['env'].update(extra)
    receipt['commands'].append(row)
    retain()
    started = time.monotonic()
    with log.open('w') as stream:
        result = subprocess.run(command, cwd=source, env=local, stdout=stream, stderr=subprocess.STDOUT)
    text = log.read_text()
    row.update(exit_code=result.returncode, status='passed' if result.returncode == 0 else 'failed',
               elapsed_seconds=time.monotonic() - started, log=log.name, log_sha256=hashlib.sha256(log.read_bytes()).hexdigest())
    row['suite_counts'] = [dict(zip(('passed', 'failed', 'ignored', 'measured', 'filtered'), map(int, match)))
                           for match in re.findall(r'test result: (?:ok|FAILED)\. (\d+) passed; (\d+) failed; (\d+) ignored; (\d+) measured; (\d+) filtered out', text)]
    row['golden_case_totals'] = list(map(int, re.findall(r'Independent Apache controller fixture cases passed: (\d+)', text)))
    verify()
    row['all_tracked_source_files_unchanged'] = True
    retain()
    print(json.dumps({'toolchain': toolchain, 'name': name, 'exit_code': result.returncode}), flush=True)
    if result.returncode:
        raise SystemExit(result.returncode)

prefix = ['cargo', '+' + toolchain]
manifest = ['--manifest-path', 'partitionline-broker/Cargo.toml']
run('rustc-version', ['rustc', '+' + toolchain, '--version', '--verbose'])
run('clean-broker', prefix + ['clean', *manifest, '-p', 'partitionline-broker'])
for features in ('default', 'all-features'):
    evidence = output / features
    evidence.mkdir()
    extra = {'PL_CONTROLLER_RESPONSE_DIR': str(evidence / 'responses'),
             'PL_BROKER_CONTROLLER_REPORT': str(evidence / 'compiled-report.json')}
    run(features + '-all-targets', prefix + ['test', '--locked', *manifest, '--all-targets',
        *(['--all-features'] if features == 'all-features' else []), '--jobs', '1', '--', '--nocapture'], extra)
run('fmt', prefix + ['fmt', *manifest, '--all', '--', '--check'])
run('strict-clippy-default', prefix + ['clippy', '--locked', *manifest, '--all-targets', '--jobs', '1', '--', '-D', 'warnings'])
run('strict-clippy-all-features', prefix + ['clippy', '--locked', *manifest, '--all-targets', '--all-features', '--jobs', '1', '--', '-D', 'warnings'])
run('strict-docs', prefix + ['doc', '--locked', *manifest, '--no-deps', '--all-features', '--jobs', '1'], {'RUSTDOCFLAGS': '-D warnings'})
run('doctests', prefix + ['test', '--locked', *manifest, '--doc', '--all-features', '--jobs', '1'])
receipt['status'] = 'passed'
retain()

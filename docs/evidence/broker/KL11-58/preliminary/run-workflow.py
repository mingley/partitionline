#!/usr/bin/env python3
"""Execute the broker workflow shell steps against one immutable git snapshot."""
import datetime
import hashlib
import io
import json
import os
from pathlib import Path
import re
import shutil
import subprocess
import sys
import tarfile
import time

import yaml

repo = Path('/workspace/partitionline')
source_sha = sys.argv[1]
base = Path('/workspace/work/broker-ci/snapshots') / source_sha
base.mkdir(parents=True, exist_ok=False)
source = base / 'source'
source.mkdir()
archive = subprocess.check_output(['git', 'archive', '--format=tar', source_sha], cwd=repo)
with tarfile.open(fileobj=io.BytesIO(archive), mode='r:') as stream:
    stream.extractall(source, filter='data')
workflow_path = source / '.github/workflows/broker.yml'
workflow = yaml.load(workflow_path.read_text(), Loader=yaml.BaseLoader)
hashes = {}
for path in source.rglob('*'):
    if path.is_file() and (
        'partitionline-broker' in path.parts or
        str(path.relative_to(source)).startswith('tests/conformance/broker/') or
        str(path.relative_to(source)) in ['.github/workflows/broker.yml', 'scripts/check-broker-api-matrix.py',
                                        'tests/ci/test_broker_api_matrix.py', 'Cargo.toml']
    ):
        hashes[str(path.relative_to(source))] = hashlib.sha256(path.read_bytes()).hexdigest()
(base / 'source-hashes.json').write_text(json.dumps(hashes, indent=2) + '\n')
shutil.copyfile(workflow_path, base / 'broker.yml')
env_base = os.environ.copy()
env_base.update(CARGO_HOME='/workspace/work/cargo', RUSTUP_HOME='/workspace/work/rustup',
                PATH='/workspace/work/cargo/bin:' + os.environ['PATH'])
results = []

def run_steps(job, toolchain=None):
    lane = toolchain or 'api-inventory'
    output = base / lane
    output.mkdir()
    env = env_base | workflow['jobs'][job].get('env', {})
    if toolchain:
        env['RUSTUP_TOOLCHAIN'] = toolchain
        env['CARGO_TARGET_DIR'] = str(source / 'target/broker-ci' / toolchain)
    for step in workflow['jobs'][job]['steps']:
        if 'run' not in step:
            continue
        command = step['run']
        slug = re.sub(r'[^a-z0-9]+', '-', step['name'].lower()).strip('-')
        started = datetime.datetime.now(datetime.timezone.utc).isoformat()
        before = time.monotonic()
        with (output / (slug + '.stdout.log')).open('w') as stdout, (output / (slug + '.stderr.log')).open('w') as stderr:
            process = subprocess.run(['taskset', '-c', '0-2,4', 'bash', '--noprofile', '--norc', '-eo', 'pipefail', '-c', command],
                                     cwd=source, env=env | step.get('env', {}), stdout=stdout, stderr=stderr, timeout=900)
        row = {'lane': lane, 'step': step['name'], 'command': command, 'exit_code': process.returncode,
               'started_at_utc': started, 'seconds': round(time.monotonic() - before, 3),
               'stdout': lane + '/' + slug + '.stdout.log', 'stderr': lane + '/' + slug + '.stderr.log'}
        results.append(row)
        (base / 'commands.json').write_text(json.dumps(results, indent=2) + '\n')
        print(json.dumps(row), flush=True)
        if process.returncode:
            raise RuntimeError(f'{lane}: {step["name"]} failed; raw logs retained')

run_steps('api-inventory')
shutil.copyfile(source / 'target/broker-ci/api-matrix.json', base / 'api-matrix-verification.json')
for toolchain in workflow['jobs']['broker']['strategy']['matrix']['rust']:
    run_steps('broker', toolchain)

def version(*args):
    process = subprocess.run(args, env=env_base, text=True, capture_output=True, check=True)
    return (process.stdout + process.stderr).strip()

report = {'source_sha': source_sha, 'snapshot': str(source),
          'source_snapshot_method': 'git archive of exact pushed commit; no dirty worktree files copied',
          'source_hashes': 'source-hashes.json', 'workflow_sha256': hashlib.sha256(workflow_path.read_bytes()).hexdigest(),
          'commands_passed': len(results), 'commands_failed': 0,
          'toolchains': {tc: version('rustc', '+' + tc, '-Vv') for tc in ['stable', '1.85.0']},
          'python': version('python3', '--version'), 'yaml': 'PyYAML ' + yaml.__version__,
          'openssl': version('openssl', 'version'), 'uname': version('uname', '-a'),
          'hosted_setup_python': '3.13 requested by workflow; local version explicitly recorded above',
          'cpu_affinity': '0-2,4', 'shared_runner': True,
          'isolated_build_targets': {tc: str(source / 'target/broker-ci' / tc) for tc in ['stable', '1.85.0']},
          'hosted_actions_executed_locally': False, 'production_qualification': False}
(base / 'results.json').write_text(json.dumps(report, indent=2) + '\n')
print(json.dumps(report), flush=True)

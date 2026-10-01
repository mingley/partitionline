#!/usr/bin/env bash
# KL01-17: one live adapter scenario on the caller's owned, pinned broker.
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"
[[ "${REQUIRE_BROKER:-}" == 1 ]] || { echo 'REQUIRE_BROKER=1 required' >&2; exit 1; }
python3 -B - <<'PY'
import hashlib
import json
import os
from pathlib import Path
import re
import subprocess
import sys
import time
import uuid

root = Path.cwd()
profile_path = root / 'tests/conformance/verifiable-live-profile.json'
profile_bytes = profile_path.read_bytes()
profile = json.loads(profile_bytes)

def command(*args):
    return subprocess.check_output(args, text=True).strip()

def clean_source():
    if command('git', 'status', '--porcelain', '--untracked-files=normal'):
        raise ValueError('clean committed candidate source required')
    return command('git', 'rev-parse', 'HEAD')

source = clean_source()
name = os.environ.get('PL_VERIFIABLE_CONTAINER', '')
if not re.fullmatch(r'pl-compat-4-1-2-\d+', name):
    raise ValueError('caller-owned current broker container required')
reference = command('docker', 'inspect', '--format', '{{.Config.Image}}', name)
if reference != profile['broker_reference']:
    raise ValueError('container differs from frozen broker digest')
image = command('docker', 'inspect', '--format', '{{.Image}}', name)
if image != command('docker', 'image', 'inspect', '--format', '{{.Id}}', reference):
    raise ValueError('container/image identity mismatch')
bootstrap = command('docker', 'port', name, '9092/tcp')
if not re.fullmatch(r'127\.0\.0\.1:\d+', bootstrap):
    raise ValueError('owned container loopback mapping required')
if os.environ.get('KAFKA_BOOTSTRAP') != bootstrap:
    raise ValueError('bootstrap differs from owned container mapping')
base = Path(os.environ.get('PL_VERIFIABLE_REPORT_DIR', root / 'target/verifiable-scenario'))
attempt = uuid.uuid4().hex
report = base / attempt
report.mkdir(parents=True, exist_ok=False)
topic = 'plverifiable-4-1-2-' + attempt
group = topic + '-group'
identity = {'candidate_source_sha': source, 'profile_sha256': hashlib.sha256(profile_bytes).hexdigest(),
            'broker_reference': reference, 'broker_version': profile['broker_version'],
            'container': name, 'container_image_id': image, 'bootstrap': bootstrap,
            'inspected_image_id': image,
            'repo_digests': json.loads(command('docker', 'image', 'inspect', '--format',
                                              '{{json .RepoDigests}}', reference)),
            'kafka_cli_version': command('docker', 'exec', name,
                                         '/opt/kafka/bin/kafka-topics.sh', '--version'),
            'topic': topic, 'group': group, 'started_ms': time.time_ns() // 1_000_000,
            'exit_codes': {}, 'phase': 'build'}

def retain():
    identity['ended_ms'] = time.time_ns() // 1_000_000
    (report / 'identity.json').write_text(json.dumps(identity, indent=2) + '\n')

def run(phase, args, stdout, stderr, timeout):
    identity['phase'] = phase
    retain()
    with (report / stdout).open('wb') as out, (report / stderr).open('wb') as err:
        try:
            result = subprocess.run(args, stdout=out, stderr=err, timeout=timeout, check=False)
            code = result.returncode
        except subprocess.TimeoutExpired:
            code = 124
    identity['exit_codes'][phase] = code
    retain()
    if code:
        raise ValueError(f'{phase} exited {code}; full logs retained in {report}')

try:
    if not identity['kafka_cli_version'].startswith(profile['broker_version'] + ' '):
        raise ValueError('actual Java CLI version differs from frozen broker cell')
    (report / 'rustc.log').write_text(command('rustc', '-Vv') + '\n')
    (report / 'cargo.log').write_text(command('cargo', '-V') + '\n')
    run('build', ['cargo', 'build', '--locked', '--example', 'verifiable_producer', '--example',
                 'verifiable_consumer'], 'build.stdout.log', 'build.log', 600)
    run('create-topic', ['docker', 'exec', name, '/opt/kafka/bin/kafka-topics.sh',
                         '--bootstrap-server', 'localhost:9094', '--create', '--topic', topic,
                         '--partitions', '1', '--replication-factor', '1'],
        'create-topic.log', 'create-topic.stderr.log', 30)
    target = Path(os.environ.get('CARGO_TARGET_DIR', root / 'target')).resolve()
    run('producer', [str(target / 'debug/examples/verifiable_producer'), '--bootstrap-server',
                     bootstrap, '--topic', topic, '--max-messages', '25', '--acks', '-1'],
        'producer.jsonl', 'producer.stderr.log', 40)
    run('consumer', [str(target / 'debug/examples/verifiable_consumer'), '--bootstrap-server',
                     bootstrap, '--topic', topic, '--group-id', group, '--max-messages', '25',
                     '--verbose'], 'consumer.jsonl', 'consumer.stderr.log', 40)
    run('java-records', ['docker', 'exec', name, '/opt/kafka/bin/kafka-console-consumer.sh',
                         '--bootstrap-server', 'localhost:9094', '--topic', topic,
                         '--from-beginning', '--max-messages', '25', '--timeout-ms', '20000',
                         '--property', 'print.key=true', '--property', 'print.partition=true',
                         '--property', 'print.offset=true'], 'java-records.log',
        'java-records.stderr.log', 40)
    run('java-offsets', ['docker', 'exec', name, '/opt/kafka/bin/kafka-consumer-groups.sh',
                         '--bootstrap-server', 'localhost:9094', '--describe', '--group', group],
        'java-offsets.log', 'java-offsets.stderr.log', 30)
    if clean_source() != source:
        raise ValueError('candidate source changed during execution')
    identity['prerequisite_exit_codes'] = {key: identity['exit_codes'].pop(key)
                                           for key in ('build', 'create-topic')}
    identity['phase'] = 'validate-complete-history'
    retain()
    with (report / 'report-validation.log').open('wb') as log:
        result = subprocess.run(['python3', '-B', 'scripts/report-verifiable-scenario.py',
                                  str(report), source], stdout=log, stderr=log, check=False)
    print((report / 'report-validation.log').read_text())
    if result.returncode:
        raise ValueError('complete-history validator rejected the scenario')
    print(f'Full live verifiable report retained: {report}')
except Exception as error:
    identity['failure'] = str(error)
    retain()
    message = f'{identity["phase"]}: {error}'
    for filename in ('report-validation.log', 'producer.stderr.log', 'consumer.stderr.log',
                     'build.log', 'java-records.stderr.log', 'java-offsets.stderr.log'):
        path = report / filename
        if path.is_file() and path.stat().st_size:
            message += '\n' + filename + ':\n' + path.read_text(errors='replace')[-1200:]
    print(message, file=sys.stderr)
    if os.environ.get('GITHUB_ACTIONS') == 'true':
        message = message.encode('utf-8')[-2000:].decode('utf-8', errors='ignore')
        print('::error title=Required verifiable scenario::' + message.replace('%', '%25')
              .replace('\r', '%0D').replace('\n', '%0A'))
    raise SystemExit(1)
PY

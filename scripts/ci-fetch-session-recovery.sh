#!/usr/bin/env bash
# KL05-07: an owned broker advertises only the local wire observer to Rust clients.
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"
python3 -B - <<'PY'
import json
import os
from pathlib import Path
import platform
import re
import subprocess
import sys
import time
import uuid

reference = 'apache/kafka:4.1.2@sha256:5cc2a2fd93fa2687b44015eee04fb2c3edd9e526bd64bf8bec5ff1e268772e0e'

def command(*args):
    return subprocess.check_output(args, text=True).strip()

def clean_source():
    if command('git', 'status', '--porcelain', '--untracked-files=normal'):
        raise ValueError('clean committed candidate source required')
    return command('git', 'rev-parse', 'HEAD')

source = clean_source()
if (platform.system(), platform.machine()) != ('Linux', 'x86_64'):
    raise ValueError('native Linux x86_64 broker cell required')
attempt = uuid.uuid4().hex
name = 'pl-fetch-recovery-' + attempt
topic = 'plfetch-recovery-' + attempt
backend_port = os.environ.get('PL_FETCH_BACKEND_PORT', '19192')
proxy_port = os.environ.get('PL_FETCH_PROXY_PORT', '19193')
if not all(re.fullmatch(r'\d+', p) and 1024 <= int(p) <= 65535 for p in (backend_port, proxy_port)) or backend_port == proxy_port:
    raise ValueError('distinct unprivileged owned loopback ports required')
backend, proxy = '127.0.0.1:' + backend_port, '127.0.0.1:' + proxy_port
report = Path(os.environ.get('PL_FETCH_REPORT_DIR', 'target/fetch-session-recovery')) / attempt
report.mkdir(parents=True, exist_ok=False)
identity = {'source_sha': source, 'broker_reference': reference, 'container': name, 'topic': topic,
            'backend': backend, 'proxy': proxy, 'host_os': platform.system(), 'host_arch': platform.machine(),
            'exit_codes': {}, 'phase': 'prerequisites', 'started_ms': time.time_ns() // 1_000_000}
owned = False

def retain():
    identity['ended_ms'] = time.time_ns() // 1_000_000
    (report / 'identity.json').write_text(json.dumps(identity, indent=2) + '\n')

def run(phase, args, timeout=30, required=True, env=None):
    identity['phase'] = phase
    retain()
    with (report / (phase + '.stdout.log')).open('wb') as out, (report / (phase + '.stderr.log')).open('wb') as err:
        try:
            code = subprocess.run(args, stdout=out, stderr=err, timeout=timeout, env=env, check=False).returncode
        except subprocess.TimeoutExpired:
            code = 124
    identity['exit_codes'][phase] = code
    retain()
    if required and code:
        raise ValueError(f'{phase} exited {code}')
    return code

try:
    (report / 'rustc.log').write_text(command('rustc', '-Vv') + '\n')
    (report / 'cargo.log').write_text(command('cargo', '-V') + '\n')
    if subprocess.run(['docker', 'image', 'inspect', reference], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, check=False).returncode:
        run('pull', ['docker', 'pull', reference], 180)
    run('create', ['docker', 'create', '--name', name, '-p', '127.0.0.1:' + backend_port + ':9092',
        '-e', 'KAFKA_NODE_ID=1', '-e', 'KAFKA_PROCESS_ROLES=broker,controller',
        '-e', 'KAFKA_LISTENERS=PLAINTEXT://:9092,INTERNAL://:9094,CONTROLLER://:9093',
        '-e', 'KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://' + proxy + ',INTERNAL://localhost:9094',
        '-e', 'KAFKA_LISTENER_SECURITY_PROTOCOL_MAP=PLAINTEXT:PLAINTEXT,INTERNAL:PLAINTEXT,CONTROLLER:PLAINTEXT',
        '-e', 'KAFKA_INTER_BROKER_LISTENER_NAME=INTERNAL', '-e', 'KAFKA_CONTROLLER_LISTENER_NAMES=CONTROLLER',
        '-e', 'KAFKA_CONTROLLER_QUORUM_VOTERS=1@localhost:9093', '-e', 'KAFKA_AUTO_CREATE_TOPICS_ENABLE=false',
        '-e', 'KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=1', reference])
    owned = True
    run('start', ['docker', 'start', name])
    ready = False
    for number in range(1, 46):
        step = 'readiness-' + str(number)
        if run(step, ['docker', 'exec', name, '/opt/kafka/bin/kafka-broker-api-versions.sh',
                      '--bootstrap-server', 'localhost:9094'], 10, required=False) == 0:
            identity['exit_codes']['readiness'] = 0
            ready = True
            break
        time.sleep(1)
    if not ready:
        raise ValueError('owned broker readiness deadline exceeded')
    identity.update({'actual_reference': command('docker', 'inspect', '--format', '{{.Config.Image}}', name),
        'container_image_id': command('docker', 'inspect', '--format', '{{.Image}}', name),
        'inspected_image_id': command('docker', 'image', 'inspect', '--format', '{{.Id}}', reference),
        'repo_digests': json.loads(command('docker', 'image', 'inspect', '--format', '{{json .RepoDigests}}', reference)),
        'mapped_backend': command('docker', 'port', name, '9092/tcp'),
        'advertised_listeners': next(s.removeprefix('KAFKA_ADVERTISED_LISTENERS=') for s in json.loads(
            command('docker', 'inspect', '--format', '{{json .Config.Env}}', name)) if s.startswith('KAFKA_ADVERTISED_LISTENERS=')),
        'java_cli_version': command('docker', 'exec', name, '/opt/kafka/bin/kafka-topics.sh', '--version')})
    retain()
    run('create-topic', ['docker', 'exec', name, '/opt/kafka/bin/kafka-topics.sh', '--bootstrap-server',
                        'localhost:9094', '--create', '--topic', topic, '--partitions', '32', '--replication-factor', '1'])
    run('build', ['cargo', 'test', '--locked', '--test', 'consumer_fetch_semantics', '--no-run'], 600)
    env = dict(os.environ, REQUIRE_BROKER='1', PL_FETCH_TOPIC=topic, PL_FETCH_SOURCE_SHA=source,
               PL_FETCH_PROXY=proxy, PL_FETCH_BACKEND=backend)
    run('runtime', ['cargo', 'test', '--locked', '--test', 'consumer_fetch_semantics', '--', '--ignored',
                    '--exact', 'live_fetch_session_recovery_required', '--nocapture'], 90, env=env)
    run('java-records', ['docker', 'exec', name, '/opt/kafka/bin/kafka-console-consumer.sh', '--bootstrap-server',
                        'localhost:9094', '--topic', topic, '--from-beginning', '--max-messages', '65',
                        '--timeout-ms', '20000', '--property', 'print.partition=true', '--property', 'print.offset=true',
                        '--property', 'print.key=true'], 40)
    if clean_source() != source:
        raise ValueError('candidate source changed during execution')
except Exception as error:
    identity['failure'] = str(error)
finally:
    if owned:
        for phase, args in [('broker-logs', ['docker', 'logs', name]), ('cleanup', ['docker', 'rm', '-f', name])]:
            try:
                run(phase, args)
            except Exception as error:
                identity['failure'] = identity.get('failure', '') + '; ' + str(error)
    retain()

if 'failure' not in identity:
    try:
        with (report / 'report-validation.stdout.log').open('wb') as out, (report / 'report-validation.stderr.log').open('wb') as err:
            result = subprocess.run(['python3', '-B', 'scripts/report-fetch-session-recovery.py', str(report), source],
                                    stdout=out, stderr=err, timeout=30, check=False)
        (report / 'validation-exit.json').write_text(json.dumps({'report-validation': result.returncode}) + '\n')
        if result.returncode:
            raise ValueError('complete-history validator exited ' + str(result.returncode))
        print((report / 'report-validation.stdout.log').read_text())
    except Exception as error:
        identity['failure'] = str(error)
        retain()
if 'failure' in identity:
    message = identity['failure'] + '\nFull attempt: ' + str(report)
    for phase in ('runtime', 'build', 'report-validation'):
        path = report / (phase + '.stderr.log')
        if path.is_file() and path.stat().st_size:
            message += '\n' + phase + ':\n' + path.read_text(errors='replace')[-1200:]
    print(message, file=sys.stderr)
    if os.environ.get('GITHUB_ACTIONS') == 'true':
        message = message.encode()[-2000:].decode(errors='ignore')
        print('::error title=Required Fetch session recovery::' + message.replace('%', '%25').replace('\r', '%0D').replace('\n', '%0A'))
    raise SystemExit(1)
print('Complete observed live Fetch session recovery retained: ' + str(report))
PY

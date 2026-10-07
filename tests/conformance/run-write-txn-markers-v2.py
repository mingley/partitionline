#!/usr/bin/env python3
"""Run genuine public Java abortTransaction calls against the owned Rust wire peer."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import signal
import struct
import subprocess
import time

REPO = Path(__file__).resolve().parents[2]
ORACLE = REPO / 'docs/evidence/client/write-txn-markers-v2/oracle'
PINS = {
    '4.1.2': 'afe861c05067f4018a3148d73c1ed1e5fc90808757c15b043527d7e535a5d431',
    '4.2.1': '6a281026416938a53c105f2d91d2807fdc83d5658452abf0ad1b6d8ab8a553c8',
    '4.3.1': '52501b7b47510c66f898871adaf6d2968ab7246561d44ced43643a8a587f0b36',
}
SLF4J = 'd3ef575e3e4979678dc01bf1dcce51021493b4d11fb7f1be8ad982877c16a1c0'

def sha(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()

def save(path, value):
    with Path(path).open('x') as file:
        json.dump(value, file, indent=2); file.write('\n')

def body(frame):
    client = struct.unpack_from('>h', frame, 8)[0]
    start = 10 + max(0, client)
    if frame[start] != 0:
        raise ValueError('unexpected nonempty request-header tags')
    return frame[start + 1:]

def environment():
    env = os.environ.copy()
    for key in ('JAVA_TOOL_OPTIONS', 'JDK_JAVA_OPTIONS', '_JAVA_OPTIONS', 'CLASSPATH', 'LD_PRELOAD', 'LD_LIBRARY_PATH'):
        env.pop(key, None)
    return env

def execute(args, release, name, minimum, maximum, response, expected, marker_version, retries=0, omitted=False):
    directory = args.output / (release + '-' + name)
    directory.mkdir()
    save(directory / 'declared.json', dict(release=release, range=[minimum, maximum], omitted=omitted,
        expected=expected, expected_marker_version=marker_version, expected_retries=retries,
        scope='Genuine SDK public call against a scripted wire peer; no transaction-state broker qualification'))
    response_body = response[5:]  # All selected successful serializer cells are flexible.
    normal = args.fixtures / release / ('api-27-v' + str(1 if release == '4.1.2' else 2) + '-tv-0.response.bin')
    payloads = [response_body] + ([normal.read_bytes()[5:]] if retries else [])
    with (directory / 'responses.bin').open('xb') as file:
        for payload in payloads:
            file.write(struct.pack('>I', len(payload)) + payload)
    env = environment()
    env.update(PARTITIONLINE_CAPABILITY_PEER_DIR=str(directory), PARTITIONLINE_CAPABILITY_PEER_MODE='abort',
        PARTITIONLINE_CAPABILITY_PEER_VERSION=str(minimum), PARTITIONLINE_CAPABILITY_PEER_MAX=str(maximum),
        PARTITIONLINE_CAPABILITY_PEER_OMIT='1' if omitted else '0')
    command = [str(args.binary), 'serve_public_admin_probe', '--ignored', '--exact', '--nocapture']
    jvm_command = None; jvm_code = None; failure = None
    with (directory / 'peer.log').open('x') as log:
        peer = subprocess.Popen(command, env=env, stdout=log, stderr=subprocess.STDOUT, start_new_session=True)
        try:
            deadline = time.monotonic() + 10
            while not (directory / 'ready').exists():
                if peer.poll() is not None or time.monotonic() > deadline:
                    raise RuntimeError('Rust wire peer did not become ready')
                time.sleep(.01)
            bootstrap = (directory / 'ready').read_text()
            classpath = str(args.jars / ('kafka-clients-' + release + '.jar')) + os.pathsep + str(args.slf4j)
            jvm_command = ['java', '--class-path', classpath, str(ORACLE / 'PublicAdminProbe.java'),
                bootstrap, 'abort', expected, str(directory / 'public-result.json')]
            with (directory / 'java.log').open('x') as jvm_log:
                run = subprocess.run(jvm_command, env=environment(), stdout=jvm_log, stderr=subprocess.STDOUT, timeout=15)
                jvm_code = run.returncode
            if jvm_code:
                raise RuntimeError('Genuine Java public outcome differed from declaration')
        except Exception as error:
            failure = repr(error)
            raise
        finally:
            (directory / 'stop').touch(exist_ok=False)
            expired = False
            try:
                peer.wait(timeout=8)
            except subprocess.TimeoutExpired:
                expired = True; os.killpg(peer.pid, signal.SIGKILL); peer.wait(timeout=5)
            save(directory / 'process.json', dict(peer_command=command, peer_pid=peer.pid, peer_exit=peer.returncode,
                peer_deadline_expired=expired, parent_waited=True, java_command=jvm_command, java_exit=jvm_code,
                failure=failure))
    if peer.returncode or expired:
        raise RuntimeError('Rust wire peer did not join cleanly')
    closure = json.loads((directory / 'closure.json').read_text())
    if not closure['listeners_joined'] or not closure['socket_workers_joined'] or closure['runtime_tasks'] != 0:
        raise ValueError('missing explicit socket/task closure')
    rows = [line.split('\t') for line in (directory / 'actual-requests.tsv').read_text().splitlines()]
    markers = [row for row in rows if int(row[1]) == 27]
    if marker_version is None:
        if markers: raise ValueError('unsupported public cell sent marker data')
    else:
        if len(markers) != 1 + retries: raise ValueError('unexpected marker request count')
        expected_body = body((args.fixtures / release / f'api-27-v{marker_version}-tv-0.request.bin').read_bytes())
        for row in markers:
            if int(row[0]) != 2 or int(row[2]) != marker_version or bytes.fromhex(row[4]) != expected_body:
                raise ValueError('wrong leader, negotiated version or default TransactionVersion/body')
            reply = bytes.fromhex(row[6])
            if row[7] != 'true' or struct.unpack_from('>i', reply)[0] != int(row[3]) or reply[4] != 0:
                raise ValueError('missing complete correlated flexible response')
        if retries and len([row for row in rows if int(row[1]) == 3]) < 2:
            raise ValueError('retry did not refresh partition leader Metadata')
    return dict(release=release, name=name, outcome=expected, marker_version=marker_version,
                marker_requests=len(markers), runtime_tasks=0, clients_and_listeners_closed=True)

def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ('binary', 'binding', 'fixtures', 'jars', 'slf4j', 'output'):
        parser.add_argument('--' + name, type=Path, required=True)
    args = parser.parse_args()
    args.binary = args.binary.resolve(); args.output = args.output.resolve(); args.output.mkdir(parents=True, exist_ok=False)
    binding = json.loads(args.binding.read_text())
    if sha(args.binary) != binding['binary_sha256']:
        raise ValueError('Rust peer differs from source-bound executable')
    for name, digest in binding['sources'].items():
        if sha(REPO / name) != digest: raise ValueError('source differs: ' + name)
    for release, digest in PINS.items():
        if sha(args.jars / ('kafka-clients-' + release + '.jar')) != digest: raise ValueError('SDK pin differs')
    if sha(args.slf4j) != SLF4J: raise ValueError('SLF4J pin differs')
    save(args.output / 'source-binding.json', binding)
    results = []
    unsupported = 'org.apache.kafka.common.errors.UnsupportedVersionException'
    for release in PINS:
        version = 1 if release == '4.1.2' else 2
        fixtures = {row['name']: row for row in json.loads((args.fixtures / release / 'goldens.json').read_text())['cases']}
        labels = ['tv-0', 'empty', 'wrong-producer', 'wrong-topic', 'wrong-partition', 'no-topic', 'no-partition',
            'duplicate-producer', 'duplicate-topic', 'duplicate-partition', 'unrelated-zero', 'unrelated-error',
            'error-3', 'error-6', 'error-8', 'error-9', 'error-31', 'error-47', 'error-52']
        for label in labels:
            row = fixtures[f'api-27-v{version}-{label}']
            handler = row['official_handler']; retry = int(handler['category'] == 'unmapped')
            expected = 'completed' if retry or handler['category'] == 'completed' else handler['failure_type']
            response = bytes.fromhex(row['response_hex'])
            results.append(execute(args, release, label, version, version, response, expected, version, retry))
        response = bytes.fromhex(fixtures[f'api-27-v{version}-tv-0']['response_hex'])
        for minimum, maximum, omitted, label in [(0, 0, False, 'removed-v0'), (3, 3, False, 'future-v3'),
                                                 (1, 1, True, 'absent-api')]:
            results.append(execute(args, release, label, minimum, maximum, response, unsupported, None, omitted=omitted))
        results.append(execute(args, release, 'range1to2', 1, 2, response, 'completed', version))
        if release != '4.1.2':
            results.append(execute(args, release, 'downgrade-v1', 1, 1, response, 'completed', 1))
        else:
            results.append(execute(args, release, 'unsupported-v2', 2, 2, response, unsupported, None))
        save(args.output / (release + '-summary.json'), [row for row in results if row['release'] == release])
    if sha(args.binary) != binding['binary_sha256']: raise ValueError('Rust executable changed during run')
    for name, digest in binding['sources'].items():
        if sha(REPO / name) != digest: raise ValueError('source changed during run: ' + name)
    save(args.output / 'summary.json', dict(status='pass', profiles=len(results), results=results,
        genuine_public_sdk=True, broker_transaction_state_qualified=False, performance_claims_valid=False))
    print(json.dumps(dict(status='pass', profiles=len(results))))

if __name__ == '__main__':
    main()

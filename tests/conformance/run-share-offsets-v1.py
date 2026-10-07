#!/usr/bin/env python3
"""Run genuine public Java listShareGroupOffsets calls against the owned Rust wire peer."""
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
ORACLE = REPO / 'docs/evidence/client/share-offsets-v1/oracle'
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

def execute(args, release, name, minimum, maximum, response, expected, share_version, retries=0, omitted=False, handler=None):
    directory = args.output / (release + '-' + name)
    directory.mkdir()
    save(directory / 'declared.json', dict(release=release, range=[minimum, maximum], omitted=omitted,
        expected=expected, expected_share_version=share_version, expected_retries=retries,
        scope='Genuine public share-offset call against a scripted wire peer; no share-state broker qualification'))
    response_body = response[5:]  # All selected successful serializer cells are flexible.
    normal = args.fixtures / release / ('api-90-v' + str(share_version or 0) + '-lag-1.response.bin')
    payloads = [response_body] + ([normal.read_bytes()[5:]] if retries else [])
    with (directory / 'responses.bin').open('xb') as file:
        for payload in payloads:
            file.write(struct.pack('>I', len(payload)) + payload)
    env = environment()
    env.update(PARTITIONLINE_CAPABILITY_PEER_DIR=str(directory), PARTITIONLINE_CAPABILITY_PEER_MODE='share',
        PARTITIONLINE_CAPABILITY_PEER_VERSION=str(minimum), PARTITIONLINE_CAPABILITY_PEER_MAX=str(maximum),
        PARTITIONLINE_CAPABILITY_PEER_OMIT='1' if omitted else '0')
    command = [str(args.binary), 'serve_share_admin_probe', '--ignored', '--exact', '--nocapture']
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
            jvm_command = ['java', '--class-path', classpath, str(ORACLE / 'PublicShareAdminProbe.java'),
                bootstrap, 'share', expected, str(directory / 'public-result.json')]
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
    shares = [row for row in rows if int(row[1]) == 90]
    if share_version is None:
        if shares: raise ValueError('unsupported public cell sent share data')
    else:
        if len(shares) != 1 + retries: raise ValueError('unexpected share request count')
        expected_body = body((args.fixtures / release / f'api-90-v{share_version}-lag-1.request.bin').read_bytes())
        for row in shares:
            if int(row[0]) != 2 or int(row[2]) != share_version or bytes.fromhex(row[4]) != expected_body:
                raise ValueError('wrong leader, negotiated version or nullable all-topics request body')
            reply = bytes.fromhex(row[6])
            if row[7] != 'true' or struct.unpack_from('>i', reply)[0] != int(row[3]) or reply[4] != 0:
                raise ValueError('missing complete correlated flexible response')
        if retries and name in ('group-error-15', 'group-error-16') and len([row for row in rows if int(row[1]) == 10]) < 2:
            raise ValueError('unmapped group did not refresh its coordinator')
    public = json.loads((directory / 'public-result.json').read_text())
    if public['outcome'] == 'completed' and handler is not None:
        for field in ('returned_partition_count', 'start_offset', 'lag', 'public_result_type', 'offset_getter', 'leader_epoch'):
            if public[field] != handler[field]:
                raise ValueError('actual public result differs from actual SDK handler: ' + field)
    return dict(release=release, name=name, outcome=expected, share_version=share_version,
                share_requests=len(shares), runtime_tasks=0, clients_and_listeners_closed=True)

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
        version = 0 if release == '4.1.2' else 1
        fixtures = {row['name']: row for row in json.loads((args.fixtures / release / 'goldens.json').read_text())['cases']}
        supported = [row for row in fixtures.values() if row['api_version'] == version and row['status'] == 'actual-official-serialized-and-parsed']
        assert len(supported) == 20
        normal = fixtures[f'api-90-v{version}-lag-1']
        for row in supported:
            label = row['name'].removeprefix(f'api-90-v{version}-')
            handler = row['official_handler']
            retry = int(handler['completed_groups'] == 0 and handler['failed_groups'] == 0)
            expected = 'completed' if retry or handler['completed_groups'] else handler['failure_type']
            expected_handler = normal['official_handler'] if retry else handler
            results.append(execute(args, release, label, version, version, bytes.fromhex(row['response_hex']), expected, version, retry, handler=expected_handler))
        response = bytes.fromhex(normal['response_hex'])
        results.append(execute(args, release, 'range0to1', 0, 1, response, 'completed', version, handler=normal['official_handler']))
        for omitted, label in [(False, 'future-v2'), (True, 'absent-api')]:
            results.append(execute(args, release, label, 2, 2, response, unsupported, None, omitted=omitted))
        if release != '4.1.2':
            old = fixtures['api-90-v0-lag-1']
            results.append(execute(args, release, 'downgrade-v0', 0, 0, bytes.fromhex(old['response_hex']), 'completed', 0, handler=old['official_handler']))
        else:
            results.append(execute(args, release, 'unsupported-v1', 1, 1, response, unsupported, None))
        save(args.output / (release + '-summary.json'), [row for row in results if row['release'] == release])
    if sha(args.binary) != binding['binary_sha256']: raise ValueError('Rust executable changed during run')
    for name, digest in binding['sources'].items():
        if sha(REPO / name) != digest: raise ValueError('source changed during run: ' + name)
    save(args.output / 'summary.json', dict(status='pass', profiles=len(results), results=results,
        genuine_public_sdk=True, broker_transaction_state_qualified=False, performance_claims_valid=False))
    print(json.dumps(dict(status='pass', profiles=len(results))))

if __name__ == '__main__':
    main()

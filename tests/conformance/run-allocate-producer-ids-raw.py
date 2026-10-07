#!/usr/bin/env python3
"""Qualify API67 codecs and raw dispatch with pinned SDKs and owned Kafka controllers."""
import argparse
import ctypes
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import re
import shutil
import socket
import struct
import subprocess
import time

REPO = Path(__file__).resolve().parents[2]
spec = importlib.util.spec_from_file_location('allocate_generator', REPO / 'tests/conformance/java/generate_allocate_producer_ids_raw.py')
gen = importlib.util.module_from_spec(spec)
spec.loader.exec_module(gen)
owner = gen.owner


def save(path, value):
    with path.open('x') as file:
        json.dump(value, file, indent=2)
        file.write('\n')


def port():
    with socket.socket() as stream:
        stream.bind(('127.0.0.1', 0))
        return stream.getsockname()[1]


def native(args, release, sdk):
    directory = args.output / (release + '-native')
    directory.mkdir()
    home = args.brokers / ('kafka_2.13-' + release)
    ports = [port(), port()]
    if ports[0] == ports[1]:
        raise ValueError('port reservation collided')
    broker_address, controller_address = [f'127.0.0.1:{value}' for value in ports]
    properties = directory / 'server.properties'
    properties.write_text(f'''process.roles=broker,controller
node.id=1
controller.quorum.voters=1@{controller_address}
listeners=PLAINTEXT://{broker_address},CONTROLLER://{controller_address}
advertised.listeners=PLAINTEXT://{broker_address}
controller.listener.names=CONTROLLER
inter.broker.listener.name=PLAINTEXT
listener.security.protocol.map=CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT
log.dirs={directory / 'data'}
num.network.threads=2
num.io.threads=4
num.partitions=1
offsets.topic.replication.factor=1
transaction.state.log.replication.factor=1
transaction.state.log.min.isr=1
group.initial.rebalance.delay.ms=0
unstable.api.versions.enable=true
auto.create.topics.enable=false
''')
    identity = {str(p.relative_to(home)): gen.sha(p) for name in ['bin', 'libs', 'config'] for p in sorted((home / name).rglob('*')) if p.is_file()}
    save(directory / 'native-source-bindings.json', identity)
    environment = owner.base_env() | {'KAFKA_HEAP_OPTS': '-Xms64m -Xmx128m', 'LOG_DIR': str(directory / 'logs')}
    cluster = owner.execute([str(home / 'bin/kafka-storage.sh'), 'random-uuid'], environment, directory, 'cluster-id', 15).read_text().strip()
    owner.execute([str(home / 'bin/kafka-storage.sh'), 'format', '-t', cluster, '-c', str(properties)], environment, directory, 'format', 30)
    environment['KAFKA_HEAP_OPTS'] = '-Xms128m -Xmx256m'
    broker = None
    receipt = dict(release=release, cluster_id=cluster, parent_waited=False, controller=controller_address, broker=broker_address)
    try:
        with (directory / 'server.log').open('x') as log:
            command = [str(home / 'bin/kafka-server-start.sh'), str(properties)]
            broker = subprocess.Popen(command, env=environment, stdout=log, stderr=subprocess.STDOUT, start_new_session=True)
            receipt.update(pid=broker.pid, command=command)
            deadline = time.monotonic() + 60
            while True:
                text = (directory / 'server.log').read_text()
                epochs = re.findall(r'Successfully registered broker 1 with broker epoch (\d+)', text)
                if 'Kafka Server started' in text and epochs:
                    epoch = int(epochs[-1])
                    break
                if broker.poll() is not None or time.monotonic() > deadline:
                    raise RuntimeError('native broker startup failed')
                time.sleep(.1)
            receipt['registered_broker_epoch'] = epoch
            rust_directory = directory / 'rust'
            rust_env = owner.base_env() | {'ALLOCATE_IDS_CONTROLLER': controller_address, 'ALLOCATE_IDS_BROKER': broker_address,
                'ALLOCATE_IDS_BROKER_EPOCH': str(epoch), 'ALLOCATE_IDS_DIRECTORY': str(rust_directory)}
            owner.execute([str(args.binary), 'native_allocate_producer_ids_raw_history', '--ignored', '--exact', '--nocapture'],
                          rust_env, directory, 'rust-live', 15)
            owner.execute(sdk['java'] + ['parse-live', str(rust_directory)], owner.base_env(), directory, 'SDK-parse-Rust-live', 8)
            java_directory = directory / 'java'
            owner.execute(sdk['java'] + ['live', controller_address, str(epoch), str(java_directory)], owner.base_env(), directory, 'Java-live', 15)
            owner.execute(sdk['java'] + ['parse-live', str(java_directory)], owner.base_env(), directory, 'SDK-parse-Java-live', 8)
            blocks = []
            for caller in ['rust', 'java']:
                for index in range(5):
                    request = (directory / caller / f'live-{index}-request.bin').read_bytes()
                    response = (directory / caller / f'live-{index}-response.bin').read_bytes()
                    if len(request) != 13 or request[-1] != 0 or len(response) != 19 or response[-1] != 0:
                        raise ValueError('native canonical body bounds')
                    broker_id, broker_epoch = struct.unpack('>iq', request[:12])
                    if (broker_id, broker_epoch) != ((99, 0) if index == 3 else (1, -1) if index == 4 else (1, epoch)):
                        raise ValueError('native requested identity differs')
                    throttle, error, start, length = struct.unpack('>ihqi', response[:18])
                    if index < 3:
                        if error != 0 or length != 1000 or start < 0:
                            raise ValueError('native allocated block fields')
                        blocks.append(dict(caller=caller, index=index, start=start, length=length, throttle_ms=throttle))
                    elif (error, start, length) != (77, 0, 0):
                        raise ValueError('native stale/unknown epoch fields')
                if not json.loads((directory / caller / 'receipt.json').read_text())['socket_closed']:
                    raise ValueError('caller socket closure missing')
            if any(right['start'] < left['start'] + left['length'] for left, right in zip(blocks, blocks[1:])):
                raise ValueError('controller allocated overlapping blocks across callers')
            save(directory / 'block-history.json', blocks)
            receipt.update(actual_raw_requests=10, successful_blocks=6, stale_epoch_errors=4,
                           independent_live_body_parses=20, ordinary_broker_Admin_policy='Unsupported')
    finally:
        if broker:
            if broker.poll() is None:
                os.killpg(broker.pid, 15)
            try:
                broker.wait(timeout=20)
            except subprocess.TimeoutExpired:
                receipt['forced_shutdown'] = True
                owner.stop_group(broker)
                broker.wait(timeout=5)
            receipt.update(exit_code=broker.returncode, parent_waited=True)
            if owner.group_members(broker.pid):
                raise RuntimeError('owned broker left processes')
        closed = {}
        for name, value in zip(['broker', 'controller'], ports):
            with socket.socket() as stream:
                stream.settimeout(1)
                closed[name] = stream.connect_ex(('127.0.0.1', value)) != 0
            with socket.socket() as stream:
                stream.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
                stream.bind(('127.0.0.1', value))
        receipt['ports_closed_and_reusable'] = closed
        save(directory / 'process.json', receipt)
    if receipt.get('forced_shutdown') or not all(receipt['ports_closed_and_reusable'].values()):
        raise ValueError('native owned shutdown failed')
    for name, digest in identity.items():
        if gen.sha(home / name) != digest:
            raise ValueError('native executable input changed')
    return receipt


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ['binary', 'binding', 'jars', 'slf4j', 'brokers', 'sources', 'applicability', 'output']:
        parser.add_argument('--' + name, type=Path, required=True)
    args = parser.parse_args()
    args.output = args.output.resolve()
    args.output.mkdir(parents=True, exist_ok=False)
    if ctypes.CDLL(None, use_errno=True).prctl(36, 1, 0, 0, 0) != 0:
        raise OSError('subreaper unavailable')
    binding = json.loads(args.binding.read_text())
    if gen.sha(args.binary) != binding['binary_sha256']:
        raise ValueError('binary binding differs')
    for name, digest in binding['sources'].items():
        if gen.sha(REPO / name) != digest:
            raise ValueError('source binding differs ' + name)
    inputs = [args.binary, args.binding, args.slf4j, args.applicability, Path(__file__).resolve(),
              REPO / 'tests/conformance/java/generate_allocate_producer_ids_raw.py', REPO / 'scripts/run-benchmark-matrix.py']
    inputs += [REPO / name for name in binding['sources']]
    inputs += [p for p in sorted(args.sources.rglob('*')) if p.is_file()]
    inputs += [args.jars / ('kafka-clients-' + release + '.jar') for release in gen.PINS]
    guard = {str(path): gen.sha(path) for path in inputs}
    save(args.output / 'source-bindings.json', guard)
    for path in inputs:
        if path.is_relative_to(REPO):
            target = args.output / 'executed-source' / path.relative_to(REPO)
            target.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(path, target)
    listing = owner.execute([str(args.binary), '--list'], owner.base_env(), args.output, 'required-lanes', 8).read_text()
    for name in ['actual_sdk_allocate_producer_ids_bodies', 'native_allocate_producer_ids_raw_history']:
        if name + ': test' not in listing:
            raise ValueError('missing mandatory lane')
    sdks = gen.generate(args.jars, args.slf4j, args.output)
    results = []
    for sdk in sdks:
        release = sdk['release']
        reverse = args.output / (release + '-reverse')
        env = owner.base_env() | {'ALLOCATE_IDS_FIXTURES': sdk['fixtures'], 'ALLOCATE_IDS_REVERSE': str(reverse)}
        owner.execute([str(args.binary), 'actual_sdk_allocate_producer_ids_bodies', '--ignored', '--exact', '--nocapture'], env, args.output, release + '-Rust-wire', 8)
        result = owner.execute(sdk['java'] + ['verify', str(reverse), sdk['fixtures']], owner.base_env(), args.output, release + '-SDK-parse-Rust', 8).read_text()
        if json.loads(result.splitlines()[-1])['independently_parsed_Rust_bodies'] != 27:
            raise ValueError('missing reverse body')
        results.append(native(args, release, sdk))
    if any(gen.sha(name) != digest for name, digest in guard.items()):
        raise ValueError('executed input changed')
    save(args.output / 'summary.json', dict(status='pass', actual_sdk=True, actual_native_controllers=True,
        source_bound=True, all_processes_parent_waited=True, independent_wire_bodies=81, independent_live_body_parses=60,
        raw_controller_requests=30, successful_blocks=18, stale_epoch_errors=12, results=results,
        scope='API67 v0 raw-extension cohort only; no Java Admin method, live allocator exhaustion, failover or production/performance qualification.'))


if __name__ == '__main__':
    main()

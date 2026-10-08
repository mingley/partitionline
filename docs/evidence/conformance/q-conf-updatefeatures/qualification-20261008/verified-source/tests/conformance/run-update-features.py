#!/usr/bin/env python3
"""Check bounded API57 fixtures and public callers against pinned Apache peers."""
import argparse
import ctypes
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import signal
import socket
import subprocess
import time

REPO = Path(__file__).resolve().parents[2]
spec = importlib.util.spec_from_file_location('update_generator', REPO / 'tests/conformance/java/generate_update_features.py')
gen = importlib.util.module_from_spec(spec)
spec.loader.exec_module(gen)
owner = gen.owner
GUARD = {}
END = None
original_execute = owner.execute


def sha(path):
    with Path(path).open('rb') as stream:
        return hashlib.file_digest(stream, 'sha256').hexdigest()


def save(path, data):
    with path.open('x') as stream:
        json.dump(data, stream, indent=2)
        stream.write('\n')
        stream.flush()
        os.fsync(stream.fileno())


def guard():
    if time.monotonic() >= END:
        raise TimeoutError('overall API57 deadline')
    for path, digest in GUARD.items():
        if sha(path) != digest:
            raise ValueError('executed input changed: ' + path)


def execute(command, env, directory, label, timeout):
    guard()
    result = original_execute(command, env, directory, label, min(timeout, END - time.monotonic()))
    guard()
    return result


owner.execute = execute


def port():
    with socket.socket() as stream:
        stream.bind(('127.0.0.1', 0))
        return stream.getsockname()[1]


def native(args, sdk):
    release = sdk['release']
    root = args.output / (release + '-native')
    root.mkdir()
    home = args.brokers / ('kafka_2.13-' + release)
    ports = [port(), port()]
    if ports[0] == ports[1]:
        raise ValueError('port collision')
    broker, controller = [f'127.0.0.1:{value}' for value in ports]
    properties = root / 'server.properties'
    properties.write_text(f'''process.roles=broker,controller
node.id=1
controller.quorum.voters=1@{controller}
listeners=PLAINTEXT://{broker},CONTROLLER://{controller}
advertised.listeners=PLAINTEXT://{broker}
controller.listener.names=CONTROLLER
inter.broker.listener.name=PLAINTEXT
listener.security.protocol.map=CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT
log.dirs={root/'data'}
num.network.threads=2
num.io.threads=4
num.partitions=1
offsets.topic.replication.factor=1
transaction.state.log.replication.factor=1
transaction.state.log.min.isr=1
auto.create.topics.enable=false
log.segment.bytes=1048576
log.index.size.max.bytes=1048576
metadata.log.segment.bytes=8388608
''')
    native_inputs = {str(path): sha(path) for name in ('bin', 'libs', 'config')
                     for path in sorted((home / name).rglob('*')) if path.is_file()}
    save(root / 'native-source-bindings.json', native_inputs)
    GUARD.update(native_inputs)
    env = owner.base_env() | {'KAFKA_HEAP_OPTS': '-Xms64m -Xmx128m', 'LOG_DIR': str(root / 'logs')}
    cluster = execute([str(home / 'bin/kafka-storage.sh'), 'random-uuid'], env, root, 'cluster-id', 15).read_text().strip()
    execute([str(home / 'bin/kafka-storage.sh'), 'format', '-t', cluster, '-c', str(properties)], env, root, 'format', 30)
    env['KAFKA_HEAP_OPTS'] = '-Xms128m -Xmx256m'
    child = None
    receipt = dict(release=release, cluster_id=cluster, broker=broker, controller=controller, parent_waited=False)
    try:
        with (root / 'server.log').open('x') as log:
            command = ['python3', '-B', str(REPO / 'benchmarks/runtime/tools/parent-bound-exec.py'), str(os.getpid()),
                       str(home / 'bin/kafka-server-start.sh'), str(properties)]
            child = subprocess.Popen(command, env=env, stdout=log, stderr=subprocess.STDOUT, start_new_session=True)
            receipt.update(pid=child.pid, command=command)
            startup = min(END, time.monotonic() + 60)
            while True:
                if (root / 'server.log').stat().st_size > 64 * 1024 * 1024:
                    raise ValueError('broker log budget')
                if 'Kafka Server started' in (root / 'server.log').read_text():
                    break
                if child.poll() is not None or time.monotonic() >= startup:
                    raise RuntimeError('native startup failed')
                time.sleep(.1)
            java_output = root / 'java.json'
            execute(sdk['java'] + ['live', broker, str(java_output)], owner.base_env(), root, 'Java-live', 35)
            rust_output = root / 'rust.json'
            rust_env = owner.base_env() | {'UPDATE_FEATURES_BROKER': broker, 'UPDATE_FEATURES_OUTPUT': str(rust_output)}
            execute([str(args.binary), 'native_public_update_features_history', '--ignored', '--exact', '--nocapture'],
                    rust_env, root, 'Rust-live', 35)
            a, b = [json.loads(path.read_text()) for path in (java_output, rust_output)]
            if a != b:
                save(root / 'caller-differences.json', dict(java=a, rust=b))
                raise ValueError('actual public caller outcomes differ')
            receipt.update(actual_public_cases_per_caller=7, actual_public_outcomes_match=True,
                           finalized_features_unchanged=True)
    finally:
        if child is not None:
            if child.poll() is None:
                os.killpg(child.pid, signal.SIGTERM)
            try:
                child.wait(timeout=20)
            except subprocess.TimeoutExpired:
                receipt['forced_shutdown'] = True
            receipt['adopted_children'] = owner.stop_group(child)
            receipt.update(exit_code=child.returncode, parent_waited=True,
                           process_group_empty=not owner.group_members(child.pid))
        closed = []
        for value in ports:
            with socket.socket() as stream:
                stream.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
                stream.bind(('127.0.0.1', value))
            closed.append(dict(port=value, reusable=True))
        receipt['ports'] = closed
        save(root / 'process.json', receipt)
        for path in native_inputs:
            GUARD.pop(path)
    if receipt.get('forced_shutdown') or not receipt.get('process_group_empty'):
        raise ValueError('owned topology shutdown failed')
    if any(sha(path) != digest for path, digest in native_inputs.items()):
        raise ValueError('native input changed')
    return receipt


def main():
    global END
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ('binary', 'binding', 'jars', 'slf4j', 'brokers', 'output'):
        parser.add_argument('--' + name, type=Path, required=True)
    args = parser.parse_args()
    args.output = args.output.resolve()
    args.output.mkdir(parents=True, exist_ok=False)
    END = time.monotonic() + 600
    gen.deadline = END
    if ctypes.CDLL(None, use_errno=True).prctl(36, 1, 0, 0, 0):
        raise OSError('subreaper unavailable')
    binding = json.loads(args.binding.read_text())
    if sha(args.binary) != binding['binary_sha256']:
        raise ValueError('binary binding differs')
    for name, digest in binding['sources'].items():
        if sha(REPO / name) != digest:
            raise ValueError('compiled source differs: ' + name)
    inputs = [args.binary, args.binding, args.slf4j, Path(__file__).resolve(),
              REPO / 'tests/conformance/java/generate_update_features.py',
              REPO / 'tests/conformance/java/ConformanceUpdateFeatures.java']
    inputs += [REPO / name for name in binding['sources']]
    inputs += [args.jars / ('kafka-clients-' + tag + '.jar') for tag in gen.PINS]
    GUARD.update({str(path): sha(path) for path in inputs})
    save(args.output / 'source-bindings.json', GUARD)
    listing = execute([str(args.binary), '--list'], owner.base_env(), args.output, 'required-lanes', 8).read_text()
    for lane in ('actual_sdk_update_features_bodies', 'native_public_update_features_history'):
        if lane + ': test' not in listing:
            raise ValueError('missing mandatory lane')
    sdks = gen.generate(args.jars.resolve(), args.slf4j.resolve(), args.output)
    GUARD.update(gen.input_pins)
    results = []
    for sdk in sdks:
        release = sdk['release']
        reverse = args.output / (release + '-reverse')
        env = owner.base_env() | {'UPDATE_FEATURES_FIXTURES': sdk['fixtures'], 'UPDATE_FEATURES_REVERSE': str(reverse)}
        execute([str(args.binary), 'actual_sdk_update_features_bodies', '--ignored', '--exact', '--nocapture'],
                env, args.output, release + '-Rust-wire', 15)
        result = execute(sdk['java'] + ['verify', str(reverse), sdk['fixtures']], owner.base_env(), args.output,
                         release + '-SDK-reverse', 8).read_text()
        if json.loads(result.splitlines()[-1])['independently_parsed_Rust_bodies'] != 101:
            raise ValueError('reverse corpus incomplete')
        results.append(native(args, sdk))
    guard()
    save(args.output / 'summary.json', dict(actual_sdk_tags=list(gen.PINS),
        reverse_bodies=303, public_native_cases=42, results=results,
        qualification_limit='Finite wire and native public cases. Controller migration and original-deadline retries require separate qualification.'))
    save(args.output / 'final-source-bindings.json', GUARD)


if __name__ == '__main__':
    try:
        main()
    except BaseException:
        raise

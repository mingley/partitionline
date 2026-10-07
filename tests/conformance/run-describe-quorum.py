#!/usr/bin/env python3
"""Check DescribeQuorum with pinned SDKs, owned socket peers and fresh Kafka brokers."""
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
import threading
import time

REPO = Path(__file__).resolve().parents[2]
spec = importlib.util.spec_from_file_location('quorum_generator_pins', REPO / 'tests/conformance/java/generate_allocate_producer_ids_raw.py')
gen = importlib.util.module_from_spec(spec)
spec.loader.exec_module(gen)
owner = gen.owner
SOURCE = REPO / 'tests/conformance/java/ConformanceDescribeQuorum.java'
MODES = ['full-v0', 'full-v1', 'full-v2', 'empty-optionals', 'retry-partition6', 'retry-controller41', 'disconnect', 'top42', 'partition42', 'top31', 'partition31', 'bad-topic-count', 'bad-topic-name', 'bad-partition-count', 'bad-partition-index', 'all-malformed', 'all-malformed-errors', 'absent-api', 'future-minimum', 'deadline', 'retry-exhaustion']
SUCCESS = {'full-v0', 'full-v1', 'full-v2', 'empty-optionals', 'retry-partition6', 'retry-controller41', 'disconnect'}


def save(path, value):
    with path.open('x') as file:
        json.dump(value, file, indent=2)
        file.write('\n')


def uvar(value):
    result = bytearray()
    while value >= 128:
        result.append((value & 127) | 128)
        value >>= 7
    result.append(value)
    return bytes(result)


def compact(value):
    data = value.encode()
    return uvar(len(data) + 1) + data


def metadata(port):
    # API3v12: empty topic selection, one registered broker and controller.
    return struct.pack('>i', 0) + b'\x02' + struct.pack('>i', 1) + compact('127.0.0.1') + struct.pack('>i', port) + b'\x00\x00' + compact('quorum-fixture') + struct.pack('>i', 1) + b'\x01\x00'


def versions(version, mode):
    keys = [(18, 0, 0), (3, 12, 12), (19, 0, 0), (20, 0, 0)]
    if mode != 'absent-api':
        keys.append((55, 3 if mode == 'future-minimum' else 0, 3 if mode == 'future-minimum' else version))
    return struct.pack('>hi', 0, len(keys)) + b''.join(struct.pack('>hhh', *key) for key in keys)


class Peer:
    """At most16 live clients,64 accepts,512 frames and16MiB retained data; joins every worker."""
    def __init__(self, directory, mode, fixtures=None, upstream=None):
        self.directory, self.mode, self.fixtures, self.upstream = directory, mode, fixtures, upstream
        self.version = int(mode[-1]) if mode.startswith('full-v') else 2
        self.stop = threading.Event()
        self.lock = threading.Lock()
        self.listener = socket.socket()
        self.listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.listener.bind(('127.0.0.1', 0))
        self.listener.listen(16)
        self.listener.settimeout(.1)
        self.port = self.listener.getsockname()[1]
        self.address = f'127.0.0.1:{self.port}'
        self.sockets, self.workers, self.frames, self.errors = set(), [], [], []
        self.accepts = self.attempts = self.retained = self.peak_sockets = 0
        self.deadline = time.monotonic() + 120
        self.supervisor = threading.Thread(target=self.accept)
        self.supervisor.start()

    def read(self, stream, count):
        result = bytearray()
        while len(result) < count:
            if self.stop.is_set() or time.monotonic() >= self.deadline:
                raise EOFError('owner stopped or original peer deadline')
            try:
                part = stream.recv(count - len(result))
            except socket.timeout:
                continue
            if not part:
                raise EOFError('caller closed')
            result.extend(part)
        return bytes(result)

    def frame(self, stream):
        length = struct.unpack('>i', self.read(stream, 4))[0]
        if not 0 < length <= 1048576:
            raise ValueError('frame length exceeds admission')
        return self.read(stream, length)

    def accept(self):
        try:
            while not self.stop.is_set():
                try:
                    stream, _ = self.listener.accept()
                except socket.timeout:
                    continue
                stream.settimeout(.1)
                stream.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
                with self.lock:
                    self.accepts += 1
                    if len(self.sockets) >= 16 or self.accepts > 64:
                        stream.close()
                        raise ValueError('socket owner bound')
                    self.sockets.add(stream)
                    self.peak_sockets = max(self.peak_sockets, len(self.sockets))
                    worker = threading.Thread(target=self.worker, args=(stream,))
                    self.workers.append(worker)
                    worker.start()
        except OSError:
            if not self.stop.is_set():
                self.errors.append('listener failure')
        except BaseException as error:
            self.errors.append(repr(error))

    def worker(self, stream):
        upstream = None
        try:
            if self.upstream:
                upstream = socket.create_connection(self.upstream, timeout=2)
                upstream.settimeout(.1)
                upstream.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
                with self.lock:
                    self.sockets.add(upstream)
                    self.peak_sockets = max(self.peak_sockets, len(self.sockets))
                    if len(self.sockets) > 32:
                        raise ValueError('proxy socket owner bound')
            while not self.stop.is_set():
                request = self.frame(stream)
                if len(request) < 10:
                    raise ValueError('incomplete request header')
                api, version, correlation, client_len = struct.unpack('>hhih', request[:10])
                if client_len < -1 or client_len > len(request) - 10:
                    raise ValueError('client header length')
                client = request[10:10 + max(client_len, 0)].decode()
                with self.lock:
                    if len(self.frames) >= 512 or self.retained + len(request) > 16 * 1048576:
                        raise ValueError('capture owner bound')
                    index = len(self.frames)
                    if api == 55:
                        self.attempts += 1
                    attempt = self.attempts
                    row = dict(index=index, api=api, version=version, correlation=correlation, client=client, received_ns=time.monotonic_ns())
                    self.frames.append(row)
                    self.retained += len(request)
                directory = self.directory
                if self.upstream:
                    directory = self.directory / client
                    directory.mkdir(exist_ok=True)
                prefix = f'frame-{index:04d}-{api}-{version}'
                (directory / (prefix + '-request.frame')).write_bytes(request)
                if self.upstream:
                    upstream.sendall(struct.pack('>i', len(request)) + request)
                    response = self.frame(upstream)
                elif api == 18:
                    body = versions(self.version, self.mode)
                    if version != 0:
                        body = struct.pack('>h', 35) + body[2:]
                    response = struct.pack('>i', correlation) + body
                elif api == 3:
                    if version != 12:
                        raise ValueError('unexpected Metadata version')
                    response = struct.pack('>i', correlation) + b'\x00' + metadata(self.port)
                elif api == 55:
                    if not 0 <= version <= self.version or self.mode in ('absent-api', 'future-minimum'):
                        raise ValueError('unsupported API55 dispatched')
                    if self.mode == 'disconnect' and attempt == 1:
                        row['disposition'] = 'controlled-disconnect'
                        return
                    if self.mode == 'deadline' and attempt > 1:
                        if self.stop.wait(1):
                            return
                    mode = self.mode
                    if mode.startswith('full-v') or mode in ('deadline', 'disconnect'):
                        mode = 'full'
                    if mode == 'retry-exhaustion':
                        mode = 'full' if attempt == 1 else 'retry-partition6'
                    if mode in ('retry-partition6', 'retry-controller41') and attempt > 1 and self.mode != 'retry-exhaustion':
                        mode = 'full'
                    body = (self.fixtures / f'public-{version}-{mode}.response').read_bytes()
                    response = struct.pack('>i', correlation) + b'\x00' + body
                else:
                    raise ValueError(f'unexpected API {api}')
                with self.lock:
                    self.retained += len(response)
                    if self.retained > 16 * 1048576:
                        raise ValueError('capture byte bound')
                (directory / (prefix + '-response.frame')).write_bytes(response)
                row.update(response_ns=time.monotonic_ns(), disposition='reply')
                stream.sendall(struct.pack('>i', len(response)) + response)
        except (EOFError, ConnectionError):
            pass
        except OSError as error:
            if not self.stop.is_set():
                self.errors.append(repr(error))
        except BaseException as error:
            self.errors.append(repr(error))
        finally:
            for value in (stream, upstream):
                if value:
                    value.close()
                    with self.lock:
                        self.sockets.discard(value)

    def close(self):
        self.stop.set()
        self.listener.close()
        with self.lock:
            for stream in self.sockets:
                try:
                    stream.shutdown(socket.SHUT_RDWR)
                except OSError:
                    pass
        self.supervisor.join(timeout=3)
        for worker in self.workers:
            worker.join(timeout=3)
        if self.supervisor.is_alive() or any(worker.is_alive() for worker in self.workers) or self.sockets:
            raise ValueError('peer owner did not join')
        with socket.socket() as stream:
            stream.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            stream.bind(('127.0.0.1', self.port))
        save(self.directory / 'frames.json', self.frames)
        receipt = dict(workers_spawned=len(self.workers), workers_joined=len(self.workers), supervisor_joined=True, live_sockets=0, peak_sockets=self.peak_sockets, listener_port_rebound=self.port, accepted=self.accepts, frames=len(self.frames), retained_bytes=self.retained, errors=self.errors)
        save(self.directory / 'closure.json', receipt)
        if self.errors:
            raise ValueError('peer failed ' + str(self.errors))
        return receipt


def rust_call(args, directory, mode, address):
    env = owner.base_env() | {'QUORUM_ADDRESS':address, 'QUORUM_MODE':mode, 'QUORUM_OUTCOME':str(directory / 'rust-outcome.json')}
    owner.execute([str(args.binary), 'public_describe_quorum_probe', '--ignored', '--exact', '--nocapture'], env, directory, 'rust', 8)
    return json.loads((directory / 'rust-outcome.json').read_text())


def java_call(java, directory, mode, address):
    expected = 'success' if mode in SUCCESS else mode
    owner.execute(java + ['public', address, expected, str(directory / 'java-outcome.json')], owner.base_env(), directory, 'java', 8)
    return json.loads((directory / 'java-outcome.json').read_text())


def expected_failure(mode):
    if mode in SUCCESS:
        return None
    if mode in ('top42', 'partition42', 'all-malformed-errors'):
        return 'InvalidRequestException'
    if mode in ('top31', 'partition31'):
        return 'ClusterAuthorizationException'
    if mode in ('absent-api', 'future-minimum'):
        return 'UnsupportedVersionException'
    if mode in ('deadline', 'retry-exhaustion'):
        return 'TimeoutException'
    return 'UnknownServerException'


def run_public(args, release, java, fixtures, mode, driver):
    directory = args.output / (release + '-' + mode + '-' + driver)
    directory.mkdir()
    peer = Peer(directory, mode, fixtures)
    try:
        outcome = rust_call(args, directory, mode, peer.address) if driver == 'rust' else java_call(java, directory, mode, peer.address)
    finally:
        closure = peer.close()
    owner.execute(java + ['parse-frames', str(directory)], owner.base_env(), directory, 'SDK-parse-frames', 8)
    warmup = 1 if mode in ('deadline','retry-exhaustion') else 0
    attempts = peer.attempts - warmup
    failure = expected_failure(mode)
    if mode == 'retry-exhaustion' and driver == 'rust':
        failure = 'NotLeaderOrFollowerException'
        if attempts != 8 or outcome['broker_code'] != 6:
            raise ValueError('local retry cap/exhausted last error differs')
    if outcome['failure'] != failure:
        raise ValueError('wrong public outcome ' + str(outcome))
    if mode in SUCCESS:
        infos = sorted(directory.glob('*.frame.info.json'))
        if not infos or json.loads(infos[-1].read_text()) != outcome['result']:
            raise ValueError('SDK-decoded response fields differ from public result')
    if mode in ('absent-api', 'future-minimum') and peer.attempts != 0:
        raise ValueError('unsupported API dispatched')
    if mode not in ('absent-api', 'future-minimum') and attempts < 1:
        raise ValueError('required API55 call missing')
    if mode in ('retry-partition6', 'retry-controller41', 'disconnect') and peer.attempts != 2:
        raise ValueError('missing exact successful retry')
    if driver == 'rust' and attempts > 8:
        raise ValueError('Rust public attempt bound exceeded')
    if mode in ('deadline', 'retry-exhaustion') and outcome['elapsed_ns'] > 1500000000:
        raise ValueError('original public deadline excessive')
    if any(row['api'] == 55 and row['version'] != peer.version for row in peer.frames):
        raise ValueError('highest shared version differs')
    return dict(release=release, mode=mode, driver=driver, attempts=attempts, warmup_calls=warmup, captured_API55_frames=peer.attempts, outcome=outcome, closure=closure)


def native(args, release, java):
    directory = args.output / (release + '-native')
    directory.mkdir()
    home = args.brokers / ('kafka_2.13-' + release)
    def port():
        with socket.socket() as stream:
            stream.bind(('127.0.0.1', 0))
            return stream.getsockname()[1]
    broker_port, controller_port = port(), port()
    if broker_port == controller_port:
        raise ValueError('port reservation collision')
    proxy_directory = directory / 'proxy'
    proxy_directory.mkdir()
    proxy = Peer(proxy_directory, 'native', upstream=('127.0.0.1', broker_port))
    address, controller = f'127.0.0.1:{broker_port}', f'127.0.0.1:{controller_port}'
    properties = directory / 'server.properties'
    properties.write_text(f'''process.roles=broker,controller
node.id=1
controller.quorum.voters=1@{controller}
listeners=PLAINTEXT://{address},CONTROLLER://{controller}
advertised.listeners=PLAINTEXT://{proxy.address}
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
    identity = {str(p.relative_to(home)):gen.sha(p) for name in ['bin','libs','config'] for p in sorted((home / name).rglob('*')) if p.is_file()}
    save(directory / 'native-source-bindings.json', identity)
    env = owner.base_env() | {'KAFKA_HEAP_OPTS':'-Xms64m -Xmx128m', 'LOG_DIR':str(directory / 'logs')}
    broker = None
    receipt = dict(release=release, parent_waited=False, broker=address, controller=controller, proxy=proxy.address)
    try:
        cluster = owner.execute([str(home / 'bin/kafka-storage.sh'),'random-uuid'], env, directory, 'cluster-id', 15).read_text().strip()
        owner.execute([str(home / 'bin/kafka-storage.sh'),'format','-t',cluster,'-c',str(properties)], env, directory, 'format', 30)
        env['KAFKA_HEAP_OPTS'] = '-Xms128m -Xmx256m'
        with (directory / 'server.log').open('x') as log:
            broker = subprocess.Popen([str(home / 'bin/kafka-server-start.sh'),str(properties)], env=env, stdout=log, stderr=subprocess.STDOUT, start_new_session=True)
            receipt['pid'] = broker.pid
            deadline = time.monotonic() + 60
            while True:
                text = (directory / 'server.log').read_text()
                if 'Kafka Server started' in text and re.search(r'Successfully registered broker 1 with broker epoch \d+', text):
                    break
                if broker.poll() is not None or time.monotonic() >= deadline:
                    raise RuntimeError('native startup failed')
                time.sleep(.1)
            owner.execute([str(args.binary), 'native_describe_quorum_raw_history','--ignored','--exact','--nocapture'],owner.base_env() | {'QUORUM_ADDRESS':proxy.address,'QUORUM_NATIVE_DIRECTORY':str(directory / 'rust-raw')},directory,'rust-raw',10)
            owner.execute(java + ['native-raw',proxy.address,str(directory / 'java-raw')],owner.base_env(),directory,'java-raw',10)
            for driver in ['rust','java']:
                owner.execute(java + ['parse-native',str(directory / (driver + '-raw'))],owner.base_env(),directory,'SDK-parse-' + driver + '-raw',8)
                outcome = rust_call(args,directory,'full-v2',proxy.address) if driver == 'rust' else java_call(java,directory,'full-v2',proxy.address)
                if outcome['failure'] is not None or outcome['result']['leader'] != 1 or len(outcome['result']['voters']) != 1:
                    raise ValueError('native public quorum result differs')
                client = 'partitionline' if driver == 'rust' else 'describe-quorum-java'
                captured = proxy_directory / client
                owner.execute(java + ['parse-frames',str(captured)],owner.base_env(),directory,'SDK-parse-native-public-' + driver,8)
                infos = sorted(captured.glob('*.frame.info.json'))
                if len(infos) != 1 or json.loads(infos[0].read_text()) != outcome['result']:
                    raise ValueError('native public result does not match captured SDK parse')
                rows = [row for row in proxy.frames if row['api'] == 55 and row['client'] == client]
                if len(rows) != 1 or rows[0]['version'] != 2:
                    raise ValueError('native public negotiated version differs')
            receipt.update(actual_raw_requests=6, public_calls=2, independent_live_body_parses=12, independent_public_frame_parses=4)
    finally:
        receipt['proxy_closure'] = proxy.close()
        if broker:
            if broker.poll() is None:
                os.killpg(broker.pid,15)
            try:
                broker.wait(timeout=20)
            except subprocess.TimeoutExpired:
                receipt['forced_shutdown'] = True
                owner.stop_group(broker)
            receipt.update(exit_code=broker.returncode,parent_waited=True)
            if owner.group_members(broker.pid):
                raise RuntimeError('native process group leaked')
        closed = {}
        for name,value in [('broker',broker_port),('controller',controller_port)]:
            with socket.socket() as stream:
                stream.settimeout(1)
                closed[name] = stream.connect_ex(('127.0.0.1',value)) != 0
            with socket.socket() as stream:
                stream.setsockopt(socket.SOL_SOCKET,socket.SO_REUSEADDR,1)
                stream.bind(('127.0.0.1',value))
        receipt['ports_closed_and_reusable'] = closed
        save(directory / 'process.json',receipt)
    if receipt.get('forced_shutdown') or not all(receipt['ports_closed_and_reusable'].values()):
        raise ValueError('native shutdown failed')
    if any(gen.sha(home / name) != digest for name,digest in identity.items()):
        raise ValueError('native executed source changed')
    return receipt


def decoder_controls(java, reverse, fixtures, output, release):
    controls = []
    for mode in ['changed-leader-epoch', 'missing-directory-field', 'changed-partition-selection']:
        directory = output / (release + '-control-' + mode)
        shutil.copytree(reverse,directory)
        name = 'case-2-13-request.bin' if mode == 'changed-partition-selection' else 'case-2-11-response.bin'
        body = bytearray((directory / name).read_bytes())
        if mode == 'changed-partition-selection':
            # Root count/name/partition count followed by singleton index0/tag.
            body[-4] = 1
        else:
            position = body.index(bytes.fromhex('0000000100000007'))
            if mode == 'changed-leader-epoch':
                body[position + 7] = 8
            else:
                # Locate the first genuine SDK directory UUID2:7 and remove it.
                directory_id = struct.pack('>qq',2,7)
                location = body.index(directory_id)
                del body[location:location + 16]
        (directory / name).write_bytes(body)
        rejected = False
        try:
            owner.execute(java + ['verify',str(directory),str(fixtures)],owner.base_env(),output,release + '-control-' + mode,8)
        except ValueError:
            receipt = json.loads((output / (release + '-control-' + mode + '.process.json')).read_text())
            rejected = receipt.get('parent_waited') is True and receipt.get('exit_code') == 1 and 'failure' not in receipt
        if not rejected:
            raise ValueError('SDK accepted corrupted Rust fields ' + mode)
        controls.append(dict(mode=mode,SDK_rejected=True,parent_waited=True))
    save(output / (release + '-decoder-controls.json'),controls)
    return controls


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ['binary','binding','jars','slf4j','brokers','sources','applicability','output']:
        parser.add_argument('--' + name,type=Path,required=True)
    parser.add_argument('--prototype',action='store_true',help='Diagnostic first-peer subset; cannot qualify the card.')
    args = parser.parse_args()
    args.output = args.output.resolve()
    args.output.mkdir(parents=True,exist_ok=False)
    if ctypes.CDLL(None,use_errno=True).prctl(36,1,0,0,0) != 0:
        raise OSError('subreaper unavailable')
    binding = json.loads(args.binding.read_text())
    if gen.sha(args.binary) != binding['binary_sha256']:
        raise ValueError('binary binding differs')
    for name,digest in binding['sources'].items():
        if gen.sha(REPO / name) != digest:
            raise ValueError('Rust source binding differs ' + name)
    inputs = [args.binary,args.binding,args.slf4j,args.applicability,SOURCE,Path(__file__).resolve(),REPO / 'tests/conformance/java/generate_allocate_producer_ids_raw.py',REPO / 'tests/conformance/java/generate_describe_quorum.py',REPO / 'scripts/run-benchmark-matrix.py']
    inputs += [REPO / name for name in binding['sources']]
    inputs += [p for p in sorted(args.sources.rglob('*')) if p.is_file()]
    inputs += [p for p in sorted((args.sources.parent / 'additional-source').rglob('*')) if p.is_file()]
    inputs += [args.sources.parent / 'upstream-source-fetch.json', args.sources.parent / 'additional-source-fetch.json']
    for release,digest in gen.PINS.items():
        jar = args.jars / ('kafka-clients-' + release + '.jar')
        if gen.sha(jar) != digest:
            raise ValueError('SDK source pin differs')
        inputs.append(jar)
    if gen.sha(args.slf4j) != gen.SLF4J:
        raise ValueError('logging SDK pin differs')
    guard = {str(path):gen.sha(path) for path in inputs}
    save(args.output / 'source-bindings.json',guard)
    save(args.output / 'rust-binary-binding.json',binding)
    for path in inputs:
        if path.is_relative_to(REPO):
            target = args.output / 'executed-source' / path.relative_to(REPO)
            target.parent.mkdir(parents=True,exist_ok=True)
            shutil.copy2(path,target)
    results,natives = [],[]
    for release in list(gen.PINS)[:1] if args.prototype else gen.PINS:
        classes = args.output / (release + '-classes')
        classes.mkdir()
        cp = str(args.jars / ('kafka-clients-' + release + '.jar')) + os.pathsep + str(args.slf4j)
        owner.execute(['java','--add-modules','jdk.compiler','com.sun.tools.javac.Main','-source','21','-target','21','-Xlint:all','-Werror','-cp',cp,'-d',str(classes),str(SOURCE)],owner.base_env(),args.output,release + '-compile',30)
        java = ['java','-Xmx128m','-cp',str(classes) + os.pathsep + cp,'ConformanceDescribeQuorum']
        fixtures = args.output / (release + '-fixtures-1')
        second = args.output / (release + '-fixtures-2')
        for index,path in [(1,fixtures),(2,second)]:
            owner.execute(java + ['generate',str(path)],owner.base_env(),args.output,release + '-generate-' + str(index),8)
        if gen.tree(fixtures) != gen.tree(second):
            raise ValueError('SDK generations differ')
        reverse = args.output / (release + '-reverse')
        owner.execute([str(args.binary),'actual_sdk_describe_quorum_bodies','--ignored','--exact','--nocapture'],owner.base_env() | {'QUORUM_FIXTURES':str(fixtures),'QUORUM_REVERSE':str(reverse)},args.output,release + '-Rust-SDK-bodies',8)
        owner.execute(java + ['verify',str(reverse),str(fixtures)],owner.base_env(),args.output,release + '-SDK-verify-Rust',8)
        decoder_controls(java,reverse,fixtures,args.output,release)
        public = args.output / (release + '-public-1')
        public_second = args.output / (release + '-public-2')
        for index,path in [(1,public),(2,public_second)]:
            owner.execute(java + ['generate-public',str(path)],owner.base_env(),args.output,release + '-generate-public-' + str(index),8)
        if gen.tree(public) != gen.tree(public_second):
            raise ValueError('public SDK responses differ')
        for mode in MODES:
            pair = [run_public(args,release,java,public,mode,driver) for driver in ['java','rust']]
            if mode in SUCCESS and pair[0]['outcome']['result'] != pair[1]['outcome']['result']:
                raise ValueError('public Java/Rust fields differ')
            results.extend(pair)
        natives.append(native(args,release,java))
    if any(gen.sha(name) != digest for name,digest in guard.items()):
        raise ValueError('executed source changed')
    save(args.output / 'summary.json',dict(status='diagnostic_pass' if args.prototype else 'pass',results=results,native=natives,actual_sdk=True,actual_native_brokers=True,source_bound=True,all_processes_parent_waited=True,independent_wire_bodies=45*len(natives),public_socket_profiles=len(results),raw_native_requests=6*len(natives),native_public_calls=2*len(natives),independent_live_body_parses=12*len(natives),independent_public_native_frame_parses=4*len(natives),scope='Selected current0..2 codec and public broker-bootstrap client cohort; finite one-voter native handlers. No internal Kafka replay, secure transport or throughput qualification.'))


if __name__ == '__main__':
    main()

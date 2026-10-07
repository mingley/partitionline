#!/usr/bin/env python3
"""Run the finite codec correctness profile. No throughput or latency claims."""
import argparse
import ctypes
import hashlib
import itertools
import json
import os
from pathlib import Path
import shutil
import signal
import socket
import struct
import subprocess
import tarfile
import time

REPO = Path(__file__).resolve().parents[2]
FIXTURES = REPO / 'tests/fixtures/codec-matrix'
CODECS = ('none', 'gzip', 'snappy', 'lz4', 'zstd')
CLIENTS = ('rust', 'java', 'c')
BROKERS = ('3.9.1', '4.1.0')
CURRENT_BROKERS = ('4.1.2', '4.2.1', '4.3.1')
KNOWN_BROKERS = BROKERS + CURRENT_BROKERS
ARCHIVES = {
    '3.9.1': 'dd4399816e678946cab76e3bd1686103555e69bc8f2ab8686cda71aa15bc31a3',
    '4.1.0': '85b4538470d1dcb98d0273286bfab8717065522e597ecfffcd4db83a3021758e',
    '4.1.2': '18795e0b9a89de0ed8443ad83365e26f6407813afec4fe139c1acb0c3364e6dc',
    '4.2.1': 'cff839a5994a8fd9b026b554d09852807a6ef9fd4d8246ef151732631da49dfa',
    '4.3.1': 'f118328b2d053497350d5befd82c08db7ffd710327ff52943dd5caaa1b25db21',
}
JAR_SHA = '52501b7b47510c66f898871adaf6d2968ab7246561d44ced43643a8a587f0b36'
SDK_COMMIT = '9a94e11452cdeb0a844db44ee5dd01ccbe17d3ab'
SDK_LIBRARY_SHA = '23698e1d7ec1f3f496133b946474f997ed690fa57222d3785c3a176ed57563ca'


def sha(path):
    with Path(path).open('rb') as stream:
        return hashlib.file_digest(stream, 'sha256').hexdigest()


def save(path, value):
    Path(path).write_text(json.dumps(value, indent=2) + '\n')


def crc32c(data):
    crc = 0xffffffff
    for byte in data:
        crc ^= byte
        for _ in range(8):
            crc = (crc >> 1) ^ (0x82f63b78 if crc & 1 else 0)
    return crc ^ 0xffffffff


def batches(path, expected_codec=None):
    if path.stat().st_size > 16 * 1024 * 1024:
        raise ValueError('segment exceeds finite fixture budget')
    data = path.read_bytes()
    at, counts = 0, []
    while at < len(data):
        if len(data) - at < 61:
            raise ValueError('short batch header')
        size = struct.unpack_from('>i', data, at + 8)[0] + 12
        if size < 61 or size > len(data) - at:
            raise ValueError('invalid batch length')
        batch = data[at:at + size]
        codec = struct.unpack_from('>h', batch, 21)[0] & 7
        if batch[16] != 2 or codec > 4:
            raise ValueError('unexpected magic or codec')
        if expected_codec is not None and codec != expected_codec:
            raise ValueError('stored compression differs from topic policy')
        if struct.unpack_from('>I', batch, 17)[0] != crc32c(batch[21:]):
            raise ValueError('stored batch CRC mismatch')
        count = struct.unpack_from('>i', batch, 57)[0]
        if count < 1 or count > 9:
            raise ValueError('unexpected batch record count')
        counts.append(count)
        at += size
    return counts


def audit_native(directory):
    summary = json.loads((directory / 'summary.json').read_text())
    cells = json.loads((directory / 'cells.json').read_text())
    commands = json.loads((directory / 'commands.json').read_text())
    closures = json.loads((directory / 'closure.json').read_text())
    versions = tuple(summary['broker_versions'])
    if versions not in (BROKERS, CURRENT_BROKERS, KNOWN_BROKERS):
        raise ValueError('undeclared broker version profile')
    count = 30 * len(versions)
    expected = set(itertools.product(versions, ('producer', 'gzip'), CODECS, CLIENTS))
    actual = {(c['broker'], c['topic_compression'], c['producer_codec'], c['writer']) for c in cells}
    if len(cells) != count or actual != expected or summary.get('cells') != count or summary.get('cross_client_readbacks') != count * 2 or summary.get('performance_claims_valid') is not False:
        raise ValueError('incomplete or misclassified matrix')
    indexed = {c['name']: c for c in commands}
    if len(indexed) != len(commands):
        raise ValueError('duplicate command identity')
    for command in commands:
        if command['exit_code'] or command['deadline_expired'] or not command['parent_waited']:
            raise ValueError('failed or unjoined command')
        log = directory / (command['name'] + '.log')
        if sha(log) != command['log_sha256']:
            raise ValueError('command log changed')
    stored = []
    for cell in cells:
        readers = [c for c in CLIENTS if c != cell['writer']]
        if cell['readers'] != readers or cell['records'] != 9 or cell['status'] != 'pass':
            raise ValueError('incorrect reader or record accounting')
        prefix = cell['broker'] + '/' + cell['topic']
        for suffix in ('-create', '-config', '-' + cell['writer'] + '-produce',
                       *('-' + reader + '-consume' for reader in readers)):
            if prefix + suffix not in indexed:
                raise ValueError('missing native command')
        for peer, mode in [(cell['writer'], 'produce'), *((r, 'consume') for r in readers)]:
            log = (directory / (prefix + '-' + peer + '-' + mode + '.log')).read_text()
            if peer == 'rust':
                if 'test result: ok. 1 passed; 0 failed; 0 ignored; 0 measured;' not in log:
                    raise ValueError('Rust native test did not execute exactly one case')
            else:
                lines = [json.loads(line) for line in log.splitlines() if line.startswith('{')]
                if len(lines) != 1 or lines[0].get('status') != 'pass' or lines[0].get('mode') != mode:
                    raise ValueError('missing actual SDK completion')
                row = lines[0]
                if peer == 'c':
                    if row.get('records') != 9 or row.get('closed_and_joined') is not True:
                        raise ValueError('C SDK incomplete or unjoined')
                elif row.get('records' if mode == 'produce' else 'verified') != 9 or row.get('producer_closed' if mode == 'produce' else 'consumer_closed') is not True:
                    raise ValueError('Java SDK incomplete or unclosed')
        config = (directory / (prefix + '-config.log')).read_text()
        if 'compression.type=' + cell['topic_compression'] not in config or 'retention.ms=-1' not in config:
            raise ValueError('topic policy not verified')
        segments = sorted((directory / cell['broker'] / 'batches' / cell['topic']).glob('*.log'))
        if not segments:
            raise ValueError('missing native topic segment')
        codec = 1 if cell['topic_compression'] == 'gzip' else CODECS.index(cell['producer_codec'])
        counts = [n for segment in segments for n in batches(segment, codec)]
        if sum(counts) != 9:
            raise ValueError('stored record count differs')
        stored.append(dict(broker=cell['broker'], topic=cell['topic'], codec_id=codec, timestamp=cell['timestamp'],
                           records=9, batches=len(counts),
                           segments=[dict(path=str(p.relative_to(directory)), sha256=sha(p)) for p in segments]))
    if {c['broker'] for c in closures} != set(versions) or len(closures) != len(versions):
        raise ValueError('missing broker closure')
    for closure in closures:
        if not closure['parent_waited'] or len(closure['ports']) != 2 or not all(p['closed'] and p['reusable'] for p in closure['ports']):
            raise ValueError('broker or listener not closed')
    return dict(status='pass', cells=count, cross_client_readbacks=count * 2, broker_versions=list(versions),
                stored_batches=stored, performance_claims_valid=False)


def write_native_audit(directory):
    audit = audit_native(directory)
    save(directory / 'stored-batch-audit.json', audit)
    (directory / 'stored-cells.tsv').write_text(''.join(
        f"{c['broker']}\t{c['topic']}\t{CODECS[c['codec_id']]}\t{c['timestamp']}\n"
        for c in audit['stored_batches']))


class Runner:
    def __init__(self, output):
        self.output = output
        self.commands = []

    def run(self, name, args, env=None, timeout=60, failure=False):
        path = self.output / (name + '.log')
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open('w') as log:
            proc = subprocess.Popen([str(a) for a in args], cwd=REPO, env=env,
                                    stdout=log, stderr=subprocess.STDOUT, start_new_session=True)
            expired = False
            try:
                code = proc.wait(timeout=timeout)
            except subprocess.TimeoutExpired:
                expired = True
                os.killpg(proc.pid, signal.SIGKILL)
                code = proc.wait(timeout=10)
        self.commands.append(dict(name=name, argv=[str(a) for a in args], exit_code=code,
                                  timeout_seconds=timeout, deadline_expired=expired,
                                  parent_waited=True, expected_failure=failure, log_sha256=sha(path)))
        save(self.output / 'commands.json', self.commands)
        if expired or (code == 0 if failure else code != 0):
            raise RuntimeError(name + ' did not meet its exit/deadline contract')
        return path


def archive_home(homes, version):
    archive = homes / ('kafka_2.13-' + version + '.tgz')
    home = homes / ('kafka_2.13-' + version)
    if sha(archive) != ARCHIVES[version]:
        raise ValueError('Apache distribution archive pin changed')
    # Compare the executed scripts and jars with the officially pinned archive.
    with tarfile.open(archive) as tar:
        for member in tar:
            relative = Path(member.name).relative_to(home.name)
            if member.isfile() and relative.parts[0] in ('bin', 'libs'):
                with tar.extractfile(member) as stream:
                    digest = hashlib.file_digest(stream, 'sha256').hexdigest()
                if digest != sha(home / relative):
                    raise ValueError('extracted distribution differs: ' + str(relative))
    return home


def free_ports():
    # Hold both reservations together so the controller cannot receive the broker port.
    with socket.socket() as first, socket.socket() as second:
        first.bind(('127.0.0.1', 0))
        second.bind(('127.0.0.1', 0))
        return first.getsockname()[1], second.getsockname()[1]


def native(args, java):
    args.output.mkdir(parents=True, exist_ok=False)
    runner = Runner(args.output)
    env = dict(os.environ)
    env.pop('LD_PRELOAD', None)
    env.pop('LD_LIBRARY_PATH', None)
    sdk = args.rdkafka_root
    library = sdk / 'lib/librdkafka.so.1'
    if sha(library) != SDK_LIBRARY_SHA:
        raise ValueError('native SDK library pin changed')
    commit = subprocess.check_output(['git', '-C', str(sdk / 'source'), 'rev-parse', 'HEAD'], text=True, timeout=10).strip()
    if commit != SDK_COMMIT:
        raise ValueError('native SDK source commit changed')
    c = args.output / 'codec-c-peer'
    runner.run('build-c', ['cc', '-O2', '-Wall', '-Wextra', '-Werror', '-isystem', sdk / 'source/src',
                          REPO / 'tests/conformance/librdkafka/codec_matrix.c', '-L' + str(sdk / 'lib'),
                          '-Wl,-rpath,' + str(sdk / 'lib'), '-lrdkafka', '-o', c], env)
    build = runner.run('build-rust', ['cargo', 'test', '--all-features', '--test', 'codec_matrix',
                                     '--no-run', '--message-format=json'], env, timeout=180)
    rows = [json.loads(line) for line in build.read_text().splitlines() if line.startswith('{')]
    executables = [r['executable'] for r in rows if r.get('executable')]
    if len(executables) != 1:
        raise ValueError('ambiguous native Rust executable')
    rust = Path(executables[0])
    bindings = {str(p.relative_to(REPO)): sha(p) for p in
                [Path(__file__), REPO / 'src/protocol/records.rs', REPO / 'src/producer.rs',
                 REPO / 'tests/codec_matrix.rs', REPO / 'tests/conformance/java/CodecMatrix.java',
                 REPO / 'tests/conformance/librdkafka/codec_matrix.c', REPO / 'Cargo.lock']}
    save(args.output / 'source-bindings.json', dict(sources=bindings, rust_executable_sha256=sha(rust),
                                                 c_executable_sha256=sha(c), native_library_sha256=sha(library),
                                                 sdk_commit=commit, kafka_client_jar_sha256=sha(args.jar)))
    cells, closures = [], []
    for version in args.broker_versions:
        home = archive_home(args.homes, version)
        directory = args.output / version
        directory.mkdir()
        broker_port, controller_port = free_ports()
        props = directory / 'server.properties'
        props.write_text(f'''process.roles=broker,controller
node.id=1
controller.quorum.voters=1@127.0.0.1:{controller_port}
listeners=PLAINTEXT://127.0.0.1:{broker_port},CONTROLLER://127.0.0.1:{controller_port}
advertised.listeners=PLAINTEXT://127.0.0.1:{broker_port}
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
offsets.topic.num.partitions=3
transaction.state.log.num.partitions=3
''')
        broker_env = dict(env, KAFKA_HEAP_OPTS='-Xms64m -Xmx128m')
        cluster = runner.run(version + '/uuid', [home / 'bin/kafka-storage.sh', 'random-uuid'], broker_env, 30).read_text().strip()
        runner.run(version + '/format', [home / 'bin/kafka-storage.sh', 'format', '-t', cluster, '-c', props], broker_env, 30)
        broker_env['KAFKA_HEAP_OPTS'] = '-Xms128m -Xmx384m'
        with (directory / 'server.log').open('w') as log:
            broker = subprocess.Popen([str(home / 'bin/kafka-server-start.sh'), str(props)], env=broker_env,
                                      stdout=log, stderr=subprocess.STDOUT, start_new_session=True)
            try:
                deadline = time.monotonic() + 60
                while time.monotonic() < deadline:
                    if broker.poll() is not None:
                        raise RuntimeError('broker startup exited')
                    if 'Kafka Server started' in (directory / 'server.log').read_text():
                        break
                    time.sleep(.2)
                else:
                    raise RuntimeError('broker startup deadline')
                bootstrap = '127.0.0.1:' + str(broker_port)
                for override, codec, writer in itertools.product(('producer', 'gzip'), CODECS, CLIENTS):
                    topic = f'codec-{writer}-{codec}-{override}'
                    prefix = version + '/' + topic
                    timestamp = str(int(time.time() * 1000))
                    runner.run(prefix + '-create', [home / 'bin/kafka-topics.sh', '--bootstrap-server', bootstrap,
                               '--create', '--topic', topic, '--partitions', '1', '--replication-factor', '1',
                               '--config', 'compression.type=' + override, '--config', 'retention.ms=-1'], broker_env, 30)
                    runner.run(prefix + '-config', [home / 'bin/kafka-configs.sh', '--bootstrap-server', bootstrap,
                               '--entity-type', 'topics', '--entity-name', topic, '--describe'], broker_env, 30)
                    def client(peer, mode):
                        settings = dict(env, PL_CODEC_BOOTSTRAP=bootstrap, PL_CODEC_TOPIC=topic,
                                        PL_CODEC_MODE=mode, PL_CODEC=codec, PL_CODEC_TIMESTAMP=timestamp)
                        command = ([rust, 'native_codec_matrix_peer', '--ignored', '--exact'] if peer == 'rust'
                                   else java + [mode, bootstrap, topic, codec, timestamp] if peer == 'java'
                                   else [c, mode, bootstrap, topic, codec, timestamp])
                        runner.run(prefix + '-' + peer + '-' + mode, command, settings)
                    client(writer, 'produce')
                    readers = [r for r in CLIENTS if r != writer]
                    for reader in readers:
                        client(reader, 'consume')
                    cells.append(dict(broker=version, topic=topic, writer=writer, readers=readers,
                                      producer_codec=codec, topic_compression=override, records=9,
                                      timestamp=int(timestamp), status='pass'))
                    save(args.output / 'cells.json', cells)
                    print(json.dumps(cells[-1]), flush=True)
            finally:
                if broker.poll() is None:
                    os.killpg(broker.pid, signal.SIGTERM)
                    try:
                        broker.wait(timeout=30)
                    except subprocess.TimeoutExpired:
                        os.killpg(broker.pid, signal.SIGKILL)
                        broker.wait(timeout=10)
                else:
                    broker.wait(timeout=10)
                checks = []
                for port in (broker_port, controller_port):
                    with socket.socket() as s:
                        closed = s.connect_ex(('127.0.0.1', port)) != 0
                    with socket.socket() as s:
                        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
                        s.bind(('127.0.0.1', port))
                    checks.append(dict(port=port, closed=closed, reusable=True))
                closures.append(dict(broker=version, exit_code=broker.returncode, parent_waited=True, ports=checks))
                save(args.output / 'closure.json', closures)
        for cell in (c for c in cells if c['broker'] == version):
            target = directory / 'batches' / cell['topic']
            target.mkdir(parents=True)
            for path in (directory / 'data' / (cell['topic'] + '-0')).glob('*.log'):
                shutil.copy2(path, target / path.name)
    if any(sha(REPO / p) != digest for p, digest in bindings.items()):
        raise ValueError('executed sources changed during native matrix')
    count = 30 * len(args.broker_versions)
    save(args.output / 'summary.json', dict(status='pass', cells=count, records_per_cell=9,
                                          cross_client_readbacks=count * 2, broker_versions=args.broker_versions,
                                          performance_claims_valid=False))
    write_native_audit(args.output)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('mode', choices=('generate', 'verify', 'native', 'audit-native'))
    parser.add_argument('--jar', type=Path)
    parser.add_argument('--libs', type=Path)
    parser.add_argument('--homes', type=Path)
    parser.add_argument('--broker-versions', nargs='+', choices=KNOWN_BROKERS, default=list(CURRENT_BROKERS))
    parser.add_argument('--rdkafka-root', type=Path)
    parser.add_argument('--rust-output', type=Path)
    parser.add_argument('--fixtures', type=Path, default=FIXTURES)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    args.output = args.output.resolve()
    if args.mode == 'audit-native':
        write_native_audit(args.output)
        return
    if not args.jar or not args.libs or sha(args.jar) != JAR_SHA:
        parser.error('the pinned Apache client jar and codec dependency directory are required')
    java = ['java', '-Xmx128m', '--class-path', str(args.jar.resolve()) + ':' + str(args.libs.resolve() / '*'),
            str(REPO / 'tests/conformance/java/CodecMatrix.java')]
    if args.mode == 'native':
        if tuple(args.broker_versions) not in (BROKERS, CURRENT_BROKERS, KNOWN_BROKERS):
            parser.error('select the complete current, historical or combined broker profile')
        if not args.homes or not args.rdkafka_root:
            parser.error('native mode requires --homes and --rdkafka-root')
        args.homes, args.rdkafka_root = args.homes.resolve(), args.rdkafka_root.resolve()
        native(args, java)
        return
    args.output.mkdir(parents=True, exist_ok=False)
    runner = Runner(args.output)
    manifest = args.fixtures / 'manifest.json'
    if args.mode == 'generate':
        runner.run('java-fixtures', java + ['fixtures', str(args.fixtures.resolve())])
        plain = (args.fixtures / 'java-none.batch').read_bytes()
        library = Path('/lib/x86_64-linux-gnu/libsnappy.so.1').resolve()
        lib = ctypes.CDLL(str(library))
        lib.snappy_max_compressed_length.argtypes = [ctypes.c_size_t]
        lib.snappy_max_compressed_length.restype = ctypes.c_size_t
        lib.snappy_compress.argtypes = [ctypes.c_void_p, ctypes.c_size_t, ctypes.c_void_p, ctypes.POINTER(ctypes.c_size_t)]
        capacity = lib.snappy_max_compressed_length(len(plain) - 61)
        if capacity > 1024 * 1024:
            raise ValueError('raw Snappy fixture budget')
        packed = ctypes.create_string_buffer(capacity)
        size = ctypes.c_size_t(capacity)
        if lib.snappy_compress(plain[61:], len(plain) - 61, packed, ctypes.byref(size)):
            raise ValueError('native Snappy compression failed')
        raw = bytearray(plain[:61] + packed.raw[:size.value])
        struct.pack_into('>i', raw, 8, len(raw) - 12)
        struct.pack_into('>h', raw, 21, 2)
        struct.pack_into('>I', raw, 17, crc32c(raw[21:]))
        (args.fixtures / 'native-raw-snappy.batch').write_bytes(raw)
        save(args.fixtures / 'native-snappy-pin.json', dict(library=str(library), sha256=sha(library),
                                                          source='Debian libsnappy1v5 1.2.2-1',
                                                          decoded_record_section_bytes=len(plain) - 61))
        save(manifest, dict(schema_version=1, apache_jar_sha256=JAR_SHA,
                            java_source_sha256=sha(REPO / 'tests/conformance/java/CodecMatrix.java'),
                            generator_sha256=sha(Path(__file__)),
                            files=[dict(path=p.name, sha256=sha(p), size_bytes=p.stat().st_size)
                                   for p in sorted(args.fixtures.iterdir()) if p.is_file() and p.name != 'manifest.json']))
    else:
        m = json.loads(manifest.read_text())
        if m['apache_jar_sha256'] != JAR_SHA or m['java_source_sha256'] != sha(REPO / 'tests/conformance/java/CodecMatrix.java') or m['generator_sha256'] != sha(Path(__file__)):
            raise ValueError('fixture source or peer pin changed')
        for item in m['files']:
            path = args.fixtures / item['path']
            if sha(path) != item['sha256'] or path.stat().st_size != item['size_bytes']:
                raise ValueError('fixture pin changed: ' + item['path'])
    if args.rust_output:
        runner.run('java-read-rust', java + ['verify-fixtures', str(args.fixtures.resolve()), str(args.rust_output.resolve())])
    save(args.output / 'summary.json', dict(status='pass', mode=args.mode, manifest_sha256=sha(manifest),
                                          performance_claims_valid=False, rust_reverse_checked=bool(args.rust_output)))


if __name__ == '__main__':
    main()

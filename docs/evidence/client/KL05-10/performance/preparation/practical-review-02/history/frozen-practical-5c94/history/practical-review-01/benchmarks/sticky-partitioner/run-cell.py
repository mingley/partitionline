"""Prepared bounded host runner. Source preparation does not execute this runner."""
import argparse
import gzip
import hashlib
import json
import os
from pathlib import Path
import resource
import shutil
import signal
import stat
import subprocess
import threading
import time

FLOOR = 350 * 1024 * 1024
STOP_MARGIN = 16 * 1024 * 1024
CAPTURE = 1024 * 1024
CELL_SECONDS = 2400
PROFILES = ('rust-rr-keyed', 'rust-uniform-keyed', 'java-uniform-keyed',
            'rust-rr-null', 'rust-uniform-null', 'java-uniform-null')
JAR_SHA = 'dc3d65e3ac811a446184ea1dca0fe9cf957c2d8984dcb4668d01f4b77fc8f50e'
SLF4J_SHA = 'd3ef575e3e4979678dc01bf1dcce51021493b4d11fb7f1be8ad982877c16a1c0'


def sha(path):
    digest = hashlib.sha256()
    with path.open('rb') as stream:
        while data := stream.read(1024 * 1024):
            digest.update(data)
    return digest.hexdigest()


def identity(path):
    return {'sha256': sha(path), 'bytes': path.stat().st_size,
            'full_mode': stat.S_IMODE(path.stat().st_mode)}


def write(path, value):
    data = (json.dumps(value, indent=2, sort_keys=True) + '\n').encode()
    assert shutil.disk_usage('/workspace').free >= FLOOR + STOP_MARGIN + len(data)
    with path.open('xb') as stream:
        stream.write(data)


def members(pgid):
    result = []
    for directory in Path('/proc').iterdir():
        if not directory.name.isdigit():
            continue
        try:
            raw = (directory / 'stat').read_text()
            fields = raw[raw.rfind(')') + 2:].split()
            if int(fields[2]) == pgid:
                result.append({'pid': int(directory.name), 'group': int(fields[2]),
                               'session': int(fields[3]), 'state': fields[0]})
        except (FileNotFoundError, PermissionError, ProcessLookupError):
            pass
    return result


def stop(pgid):
    current = members(pgid)
    assert all(member['session'] == pgid for member in current), 'Do not signal other ownership'
    if current:
        try:
            os.killpg(pgid, signal.SIGKILL)
        except ProcessLookupError:
            pass


def source_guard(source, expected, deadline):
    observed = set()
    total = 0
    for directory, dirs, files in os.walk(source):
        assert time.monotonic() < deadline
        assert shutil.disk_usage('/workspace').free >= FLOOR
        directory = Path(directory)
        if directory == source and '.git' in dirs:
            dirs.remove('.git')
        assert not any((directory / name).is_symlink() for name in dirs)
        for name in files:
            path = directory / name
            relative = str(path.relative_to(source))
            if relative == '.git':
                continue
            assert relative in expected and not path.is_symlink()
            entry = expected[relative]
            assert stat.S_IMODE(path.stat().st_mode) == entry['full_permission_mode']
            assert path.stat().st_size == entry['bytes']
            assert sha(path) == entry['sha256']
            observed.add(relative)
            total += entry['bytes']
    assert observed == set(expected), 'Exact immutable source path set'
    return {'files': len(observed), 'bytes': total, 'all_expected_paths_SHA256_bytes_full_modes': True}


def main():
    parser = argparse.ArgumentParser()
    for name in ('source', 'pin', 'origin-receipt', 'origin-sha256', 'out', 'profile',
                 'bootstrap', 'topic', 'rust-driver', 'rust-build-receipt', 'broker-receipt'):
        parser.add_argument('--' + name, required=True)
    parser.add_argument('--seed', type=int, required=True)
    parser.add_argument('--java-classes')
    parser.add_argument('--java-build-receipt')
    parser.add_argument('--kafka-jar', default='/workspace/work/broker-wire/jars/kafka-clients-4.3.1.jar')
    parser.add_argument('--slf4j-jar', default='/workspace/work/broker-wire/jars/slf4j-api-1.7.36.jar')
    parser.add_argument('--qualification', action='store_true', help='small delivery/correctness scope; cannot qualify ranking')
    parser.add_argument('--execute', action='store_true', help='Root must grant the future exclusive CPU3/resource lease first')
    args = parser.parse_args()
    assert args.execute, 'PREPARED only; future root CPU3/resource lease required'
    assert args.profile in PROFILES and 0 <= args.seed < 2**64
    assert len(args.pin) == 40 and all(c in '0123456789abcdef' for c in args.pin)
    os.sched_setaffinity(0, {0, 1})
    deadline = time.monotonic() + (1000 if args.qualification else CELL_SECONDS)
    source = Path(args.source).resolve()
    assert source != Path('/workspace/partitionline')
    out = Path(args.out).resolve()
    assert not out.exists() and not out.is_relative_to(source)
    forecast = json.loads((Path(__file__).parent / ('qualification-resource-forecast.json' if args.qualification else 'resource-forecast.json')).read_text())
    required = forecast['per_cell_conservative_free_requirement_bytes']
    assert shutil.disk_usage('/workspace').free >= required
    origin_path = Path(args.origin_receipt).resolve()
    assert sha(origin_path) == args.origin_sha256
    origin = json.loads(origin_path.read_text())
    assert origin['source_commit'] == args.pin and Path(origin['source_directory']).resolve() == source
    manifest = origin_path.parent / 'complete-source.json.gz'
    assert sha(manifest) == origin['source_manifest']['compressed_sha256']
    expected = json.loads(gzip.decompress(manifest.read_bytes()))
    assert len(expected) <= 100_000 and sum(entry['bytes'] for entry in expected.values()) <= 1024**3
    own_root = source / 'benchmarks/sticky-partitioner'
    assert Path(__file__).resolve() == own_root / 'run-cell.py'
    broker = json.loads(Path(args.broker_receipt).read_text())
    assert 3 not in broker['cpu_set'] and broker['replication_factor'] == broker['min_isr'] == 1
    assert broker['partitions'] == 6 and broker['topic'] == args.topic and broker['retention_ms'] == -1
    assert broker['security'] == 'PLAINTEXT' and broker['ready'] is True
    assert broker['image_digest'].startswith('sha256:') and len(broker['image_digest']) == 71
    if args.qualification:
        assert broker['topic_creation_completed'] is True
        assert broker['physical_free_bytes_after_topic_ready'] >= FLOOR + STOP_MARGIN
        assert broker['no_new_image_pull_or_broker_initialization_in_cell'] is True
    rust = Path(args.rust_driver).resolve()
    rust_build_path = Path(args.rust_build_receipt).resolve()
    rust_build = json.loads(rust_build_path.read_text())
    assert rust_build['source_commit'] == args.pin
    assert type(rust_build['release']) is bool
    assert args.qualification or rust_build['release'] is True, 'Ranking requires actual release build; qualification may use verified debug build'
    assert identity(rust) == rust_build['driver_binary_identity']
    with rust.open('rb') as stream:
        assert stream.read(4) == b'\x7fELF', 'Actual executable ELF; no fake driver shim'
    assert rust.stat().st_mode & 0o111
    assert rust_build['driver_source_sha256'] == sha(own_root / 'src/main.rs')
    assert rust_build['package_recompiled_for_exact_source_pin'] is True
    inputs = {str(path): identity(path) for path in (rust, rust_build_path, Path(args.broker_receipt), origin_path, manifest)}
    if args.profile.startswith('java-'):
        assert args.java_classes and args.java_build_receipt
        classes = Path(args.java_classes).resolve()
        java_build = json.loads(Path(args.java_build_receipt).read_text())
        assert java_build['source_commit'] == args.pin and java_build['caller_sha256'] == sha(own_root / 'StickyBenchmark.java')
        expected_classes = {'StickyBenchmark.class', 'StickyBenchmark$Ack.class', 'StickyBenchmark$Phase.class'}
        actual_classes = {str(path.relative_to(classes)) for path in classes.rglob('*') if path.is_file()}
        assert actual_classes == expected_classes, 'No SDK shadow or unexpected generated classes'
        for relative in expected_classes:
            assert identity(classes / relative) == java_build['compiled_classes'][relative]
            inputs[str(classes / relative)] = identity(classes / relative)
        jar, slf4j = Path(args.kafka_jar).resolve(), Path(args.slf4j_jar).resolve()
        assert sha(jar) == JAR_SHA and sha(slf4j) == SLF4J_SHA
        inputs[str(jar)], inputs[str(slf4j)] = identity(jar), identity(slf4j)
        inputs[str(Path(args.java_build_receipt).resolve())] = identity(Path(args.java_build_receipt))
        java = str(Path(shutil.which('java')).resolve())
        inputs[java] = identity(Path(java))
    before = source_guard(source, expected, deadline)
    out.mkdir(mode=0o700)
    write(out / 'before.json', {'pin': args.pin, 'origin_identity': identity(origin_path), 'source': before,
                               'physical_free_bytes': shutil.disk_usage('/workspace').free, 'inputs': inputs,
                               'clock_ticks_per_second': os.sysconf('SC_CLK_TCK'),
                               'measurement_CPU3': not args.qualification, 'purpose': 'qualification' if args.qualification else 'ranking', 'performance_qualified':False, 'broker_CPU3_excluded_receipt': broker,
                               'source_preparation_is_not_measurement': False})
    records = out / 'records'
    environment = dict(os.environ)
    for name in ('CLASSPATH', 'JAVA_TOOL_OPTIONS', 'JDK_JAVA_OPTIONS', '_JAVA_OPTIONS'):
        environment.pop(name, None)

    def run(label, command, cpus, seconds):
        before = source_guard(source, expected, deadline)
        assert all(identity(Path(path)) == old for path, old in inputs.items())
        assert shutil.disk_usage('/workspace').free >= FLOOR + STOP_MARGIN
        command = ['taskset', '-c', cpus, *command]
        write(out / (label + '.command.json'), {'command': command, 'source_before': before})
        process = subprocess.Popen(command, env=environment, cwd=own_root, stdout=subprocess.PIPE,
                                   stderr=subprocess.PIPE, start_new_session=True)
        history = [{'event': 'started', 'owned_pgid': process.pid, 'members': members(process.pid)}]
        reasons = []
        samples = []
        finished = threading.Event()
        # Pipes bound captured output immediately without limiting legitimate journal/database files.
        def capture(pipe, path):
            written = 0
            with path.open('xb') as destination:
                while data := pipe.read(65536):
                    remaining = CAPTURE - written
                    destination.write(data[:max(0, remaining)])
                    written += len(data)
                    if written > CAPTURE:
                        reasons.append('captured stream1MiB cap; prefix retained, overflow caused failure')
                        stop(process.pid)
                        return
            pipe.close()
        readers = [threading.Thread(target=capture, args=(pipe, out / (label + suffix)), daemon=True)
                   for pipe, suffix in [(process.stdout, '.stdout'), (process.stderr, '.stderr')]]
        for reader in readers:
            reader.start()
        def monitor():
            while not finished.wait(0.2):
                free = shutil.disk_usage('/workspace').free
                sample = {'monotonic_ns': time.monotonic_ns(), 'free_bytes': free}
                try:
                    status = Path('/proc') / str(process.pid) / 'status'
                    sample['RSS_status_kib'] = [line for line in status.read_text().splitlines()
                                               if line.startswith(('VmRSS:', 'VmHWM:'))]
                except FileNotFoundError:
                    pass
                samples.append(sample)
                if free < FLOOR + STOP_MARGIN or time.monotonic() >= deadline:
                    reasons.append('physical350MiB floor+16MiB stop margin or whole-cell absolute deadline')
                    stop(process.pid)
                    return
        watcher = threading.Thread(target=monitor, daemon=True)
        watcher.start()
        try:
            code = process.wait(timeout=max(0.1, min(seconds, deadline-time.monotonic())))
        except subprocess.TimeoutExpired:
            reasons.append('per-command/whole-cell timeout')
            stop(process.pid)
            code = process.wait(timeout=10)
        finally:
            finished.set()
            watcher.join()
        current = members(process.pid)
        if current:
            stop(process.pid)
            closure = time.monotonic() + 5
            while current and time.monotonic() < closure:
                time.sleep(0.1)
                current = members(process.pid)
        for reader in readers:
            reader.join(timeout=5)
        history.append({'event': 'closed', 'owned_pgid': process.pid, 'members': current})
        write(out / (label + '.exit.json'), {'exit': code, 'stop_reasons': reasons,
              'period_seconds': 0.2, 'physical_floor_bytes': FLOOR, 'stop_margin_bytes': STOP_MARGIN,
              'resource_samples': samples, 'owned_process_history': history})
        after = source_guard(source, expected, deadline)
        inputs_unchanged = all(identity(Path(path)) == old for path, old in inputs.items())
        write(out / (label + '.after.json'), {'source': after, 'input_identities_unchanged': inputs_unchanged})
        assert inputs_unchanged
        assert all(not reader.is_alive() for reader in readers) and not current
        assert not reasons and code == 0, 'Retain this actual failed cell; never count it qualified'
    if args.profile.startswith('java-'):
        run('producer', [java, '-Xms64m' if args.qualification else '-Xms128m', '-Xmx256m' if args.qualification else '-Xmx512m', '-XX:MaxMetaspaceSize=128m',
                        '-cp', str(classes)+':'+str(jar)+':'+str(slf4j), 'StickyBenchmark', 'qualify' if args.qualification else 'rank',
                        args.profile, args.bootstrap, args.topic, str(records), str(args.seed)], '0,1' if args.qualification else '3', 360 if args.qualification else 900)
    else:
        run('producer', [str(rust), 'qualify-produce' if args.qualification else 'produce', args.profile, args.bootstrap, args.topic, str(records), str(args.seed)], '0,1' if args.qualification else '3', 360 if args.qualification else 900)
    run('independent-consumer', [str(rust), 'qualify-verify' if args.qualification else 'verify', args.profile, args.bootstrap, args.topic, str(records), str(args.seed)], '0,1', 360)
    run('offline-audit', [shutil.which('python3'), str(own_root / 'audit.py'), str(records), *(['--qualification'] if args.qualification else [])], '0,1', 180 if args.qualification else 1250)
    write(out / 'cell-complete.json', {'profile': args.profile, 'source_commit': args.pin,
                                     'inputs_before_after_exact_path_sha256_bytes_fullmode': True,
                                     'actual_cell_passed': True, 'purpose': 'qualification' if args.qualification else 'ranking', 'performance_qualified':False, 'paired_five_run_matrix_still_required': True})


if __name__ == '__main__':
    main()

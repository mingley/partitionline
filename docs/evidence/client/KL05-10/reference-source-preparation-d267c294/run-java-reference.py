"""Prepared genuine SDK runner. No JVM is launched unless root executes this file."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import shutil
import signal
import stat
import subprocess
import threading
import time
import gzip
import resource
import zipfile

ROOT = Path(__file__).resolve().parent
JAR = Path('/workspace/work/broker-wire/jars/kafka-clients-4.3.1.jar')
SLF4J = Path('/workspace/work/broker-wire/jars/slf4j-api-1.7.36.jar')
JAR_SHA = 'dc3d65e3ac811a446184ea1dca0fe9cf957c2d8984dcb4668d01f4b77fc8f50e'
SLF4J_SHA = 'd3ef575e3e4979678dc01bf1dcce51021493b4d11fb7f1be8ad982877c16a1c0'
DISK_FLOOR = 350 * 1024 * 1024
TOTAL_RUNTIME_SECONDS = 300
FILE_OUTPUT_LIMIT = 256 * 1024


def source_inventory(source, deadline):
    files = {}
    directories = {}
    for directory, subdirectories, filenames in os.walk(source):
        assert time.monotonic() < deadline, 'Whole-source scan exceeded total runtime'
        directory = Path(directory)
        directories[str(directory.relative_to(source))] = stat.S_IMODE(directory.stat().st_mode)
        if directory == source and '.git' in subdirectories:
            subdirectories.remove('.git')
        for name in subdirectories:
            assert not (directory / name).is_symlink(), 'No code directory symlinks'
        for name in filenames:
            assert time.monotonic() < deadline, 'Whole-source scan exceeded total runtime'
            path = directory / name
            relative = str(path.relative_to(source))
            if relative == '.git':
                continue
            assert not path.is_symlink(), relative
            data = path.read_bytes()
            files[relative] = {'sha256': digest(data), 'bytes': len(data),
                               'mode': oct(stat.S_IMODE(path.stat().st_mode)),
                               'git_blob': hashlib.sha1(b'blob ' + str(len(data)).encode() + b'\0' + data).hexdigest()}
    return {'files': files, 'directories_full_modes': directories}


def group_members(pgid):
    members = []
    for directory in Path('/proc').iterdir():
        if not directory.name.isdigit():
            continue
        try:
            raw = (directory / 'stat').read_text()
            tail = raw[raw.rfind(')') + 2:].split()
            if int(tail[2]) == pgid:
                members.append({'pid': int(directory.name), 'state': tail[0],
                                'parent': int(tail[1]), 'group': int(tail[2]),
                                'session': int(tail[3])})
        except (FileNotFoundError, ProcessLookupError, PermissionError):
            continue
    return members


def stop_owned_group(pgid, sig):
    members = group_members(pgid)
    assert all(member['session'] == pgid for member in members), 'Never signal unrelated process ownership'
    if members:
        try:
            os.killpg(pgid, sig)
        except ProcessLookupError:
            pass


def child_limits():
    # Bound each captured file at the kernel, and reject reference stdout above32KiB.
    resource.setrlimit(resource.RLIMIT_FSIZE, (FILE_OUTPUT_LIMIT, FILE_OUTPUT_LIMIT))


def digest(data):
    return hashlib.sha256(data).hexdigest()


def info(path):
    data = path.read_bytes()
    return {'sha256': digest(data), 'bytes': len(data), 'mode': oct(stat.S_IMODE(path.stat().st_mode))}


def write(path, value):
    path.write_text(json.dumps(value, indent=2, sort_keys=True) + '\n')


def main():
    global JAR, SLF4J
    parser = argparse.ArgumentParser()
    parser.add_argument('--out', required=True)
    parser.add_argument('--pin', required=True)
    parser.add_argument('--source', required=True)
    parser.add_argument('--origin-receipt', required=True)
    parser.add_argument('--origin-sha256', required=True)
    parser.add_argument('--kafka-jar', default=str(JAR))
    parser.add_argument('--slf4j-jar', default=str(SLF4J))
    args = parser.parse_args()
    os.sched_setaffinity(0, {0, 1})
    started = time.monotonic()
    deadline = started + TOTAL_RUNTIME_SECONDS
    JAR = Path(args.kafka_jar).resolve()
    SLF4J = Path(args.slf4j_jar).resolve()
    assert len(args.pin) == 40 and all(c in '0123456789abcdef' for c in args.pin)
    out = Path(args.out).resolve()
    assert not out.exists(), 'Use a fresh empty result directory; preserve failures'
    assert shutil.disk_usage('/workspace').free >= 400 * 1024 * 1024
    source = Path(args.source).resolve()
    assert source != Path('/workspace/partitionline'), 'Use the root-verified immutable actual pin'
    assert not out.is_relative_to(source)
    assert digest(Path(args.origin_receipt).read_bytes()) == args.origin_sha256
    fixture_root = source / 'tests/fixtures/sticky-partitioner'
    caller = fixture_root / 'GenerateUniformStickyFixture.java'
    input_path = fixture_root / 'uniform-input.tsv'
    for path in [caller, input_path]:
        relative = str(path.relative_to(source))
        expected = subprocess.check_output(['git', 'show', args.pin + ':' + relative], cwd='/workspace/partitionline', timeout=30)
        assert path.read_bytes() == expected and stat.S_IMODE(path.stat().st_mode) == 0o600
    observed = {str(path): info(path) for path in [JAR, SLF4J, caller, input_path]}
    assert observed[str(JAR)]['sha256'] == JAR_SHA and observed[str(JAR)]['mode'] == '0o600'
    assert observed[str(SLF4J)]['sha256'] == SLF4J_SHA and observed[str(SLF4J)]['mode'] == '0o600'
    with zipfile.ZipFile(JAR) as archive:
        version = archive.read('kafka/kafka-version.properties').decode()
        assert 'version=4.3.1' in version and 'commitId=26b251a451ce941d' in version
    out.mkdir(mode=0o700, parents=True)
    tree = subprocess.check_output(['git', 'ls-tree', '-rz', '--full-tree', args.pin], cwd='/workspace/partitionline', timeout=30)
    expected = {}
    for entry in tree.split(b'\0'):
        if not entry:
            continue
        header, path = entry.split(b'\t', 1)
        mode, kind, blob = header.decode().split()
        assert kind == 'blob' and mode in ['100644', '100755']
        expected[path.decode()] = {'blob': blob, 'mode': '0o700' if mode == '100755' else '0o600'}
    initial_source = source_inventory(source, deadline)
    assert set(initial_source['files']) == set(expected)
    for path, entry in expected.items():
        assert initial_source['files'][path]['git_blob'] == entry['blob']
        assert initial_source['files'][path]['mode'] == entry['mode']
    audit = (json.dumps(initial_source, indent=2, sort_keys=True) + '\n').encode()
    with (out / 'complete-immutable-source.json.gz').open('wb') as destination:
        with gzip.GzipFile(fileobj=destination, mode='wb', filename='', mtime=0) as gz:
            gz.write(audit)
    write(out / 'complete-source-provenance.json', {'pin': args.pin, 'git_files': len(expected),
                                                  'raw_audit_sha256': digest(audit), 'raw_audit_bytes': len(audit),
                                                  'Git_metadata_only_excluded': True, 'full_directory_modes_observed': True,
                                                  'origin_receipt': args.origin_receipt, 'origin_sha256': args.origin_sha256})

    def source_guard(label):
        current = source_inventory(source, deadline)
        unchanged = current == initial_source
        write(out / (label + '.whole-source-guard.json'), {'pin': args.pin, 'checked_Git_files': len(expected),
                                                         'initial_raw_audit_sha256': digest(audit),
                                                         'exact_code_path_set_bytes_full_modes_and_directory_modes_unchanged': unchanged})
        assert unchanged, 'Whole immutable source changed'
        assert time.monotonic() < deadline, 'Bounded total reference runtime'
    classes = out / 'classes'
    classes.mkdir(mode=0o700)
    environment = dict(os.environ)
    for key in ['CLASSPATH', 'JAVA_TOOL_OPTIONS', 'JDK_JAVA_OPTIONS', '_JAVA_OPTIONS']:
        environment.pop(key, None)
    javac = Path(shutil.which('javac')).resolve()
    java = Path(shutil.which('java')).resolve()
    classpath = str(JAR) + ':' + str(SLF4J)

    def run(label, executable, arguments, expected_exit=0):
        source_guard(label + '-before')
        assert shutil.disk_usage('/workspace').free >= DISK_FLOOR
        for path, original in observed.items():
            assert info(Path(path)) == original
        command = ['taskset', '-c', '0,1', str(executable), *arguments]
        write(out / (label + '.command.json'), {'command': command, 'pin': args.pin,
                                              'cwd': str(ROOT), 'cleared_JVM_injection_variables': ['CLASSPATH', 'JAVA_TOOL_OPTIONS', 'JDK_JAVA_OPTIONS', '_JAVA_OPTIONS']})
        samples = []
        stop_reason = []
        finished = threading.Event()
        history = []
        with (out / (label + '.stdout')).open('wb') as stdout, (out / (label + '.stderr')).open('wb') as stderr:
            process = subprocess.Popen(command, cwd=ROOT, env=environment, stdout=stdout, stderr=stderr,
                                       start_new_session=True, preexec_fn=child_limits)
            history.append({'event': 'started', 'owned_pgid': process.pid, 'members': group_members(process.pid)})

            def monitor():
                while not finished.wait(0.2):
                    free = shutil.disk_usage('/workspace').free
                    stdout_bytes = (out / (label + '.stdout')).stat().st_size
                    stderr_bytes = (out / (label + '.stderr')).stat().st_size
                    samples.append({'monotonic_ns': time.monotonic_ns(), 'free_bytes': free,
                                    'stdout_bytes': stdout_bytes, 'stderr_bytes': stderr_bytes})
                    reasons = []
                    if free < DISK_FLOOR:
                        reasons.append('350MiB physical disk floor')
                    if time.monotonic() >= deadline:
                        reasons.append('300second total runtime')
                    if stdout_bytes > 32768 or stderr_bytes > FILE_OUTPUT_LIMIT:
                        reasons.append('bounded captured output')
                    if reasons:
                        stop_reason.extend(reasons)
                        stop_owned_group(process.pid, signal.SIGKILL)
                        return

            watcher = threading.Thread(target=monitor, daemon=True)
            watcher.start()
            try:
                exit_code = process.wait(timeout=max(0.1, min(120, deadline - time.monotonic())))
            except subprocess.TimeoutExpired:
                stop_reason.append('per-command/total timeout')
                stop_owned_group(process.pid, signal.SIGKILL)
                exit_code = process.wait(timeout=10)
            finally:
                finished.set()
                watcher.join()
        (out / (label + '.exit')).write_text(str(exit_code) + '\n')
        members = group_members(process.pid)
        history.append({'event': 'parent_reaped', 'owned_pgid': process.pid, 'members': members})
        if members:
            stop_owned_group(process.pid, signal.SIGKILL)
            closure_deadline = min(deadline, time.monotonic() + 5)
            while members and time.monotonic() < closure_deadline:
                time.sleep(0.1)
                members = group_members(process.pid)
        history.append({'event': 'closed', 'owned_pgid': process.pid, 'members': members})
        write(out / (label + '.process-membership.json'), history)
        write(out / (label + '.resource-monitor.json'), {'period_seconds': 0.2, 'samples': samples,
                                                       'physical_floor_bytes': DISK_FLOOR,
                                                       'kernel_per_file_output_limit': FILE_OUTPUT_LIMIT,
                                                       'stop_reasons': stop_reason})
        source_guard(label + '-after')
        assert not members, 'Owned process group not closed'
        assert not stop_reason, 'Resource stop is not a successful/expected control result'
        assert (out / (label + '.stdout')).stat().st_size <= 32768
        assert (out / (label + '.stderr')).stat().st_size <= FILE_OUTPUT_LIMIT
        for path, original in observed.items():
            assert info(Path(path)) == original
        assert exit_code == expected_exit, label
        if expected_exit == 1:
            assert b'java.lang.IllegalArgumentException' in (out / (label + '.stderr')).read_bytes(), label
        return (out / (label + '.stdout')).read_bytes()

    run('javac-identity', javac, ['-version'])
    run('java-identity', java, ['-version'])
    run('compile-caller', javac, ['-J-Xms16m', '-J-Xmx128m', '-J-XX:MaxMetaspaceSize=128m',
                                '-cp', classpath, '-d', str(classes), str(caller)])
    expected_classes = {
        'org/apache/kafka/clients/producer/internals/GenerateUniformStickyFixture.class',
        'org/apache/kafka/clients/producer/internals/GenerateUniformStickyFixture$Controlled.class',
    }
    compiled = {str(path.relative_to(classes)) for path in classes.rglob('*') if path.is_file()}
    assert compiled == expected_classes, 'No shadow SDK or additional compiled classes permitted'
    compiled_identity = {str(path.relative_to(classes)): info(path) for path in classes.rglob('*') if path.is_file()}
    prefix = ['-Xms16m', '-Xmx128m', '-XX:MaxMetaspaceSize=128m', '-XX:ReservedCodeCacheSize=64m',
              '-cp', str(classes) + ':' + classpath,
              'org.apache.kafka.clients.producer.internals.GenerateUniformStickyFixture']

    def check_rows(stdout, input_bytes):
        assert len(stdout) <= 32768
        expected_rows = []
        name = None
        event = 0
        partitions = 0
        for line in input_bytes.decode().splitlines():
            if line.startswith('#') or not line:
                continue
            fields = line.split('\t')
            if fields[0] == 'CASE':
                name, partitions, event = fields[1], int(fields[4]), 0
            else:
                expected_rows.append((name, event, partitions))
                event += 1
        lines = stdout.decode().splitlines()
        assert lines[0] == '# official-kafka-4.3.1-uniform-rust-packed-events-v1'
        assert len(lines[1:]) == len(expected_rows)
        for line, (name, event, partitions) in zip(lines[1:], expected_rows):
            fields = line.split('\t')
            assert len(fields) == 6 and fields[0] == name and int(fields[1]) == event
            assert 0 <= int(fields[2]) < partitions and 0 <= int(fields[3]) < partitions
            assert int(fields[4]) >= 0 and int(fields[5]) in [int(fields[4]), int(fields[4]) + 1]

    original_input = input_path.read_bytes()
    positive = run('genuine-positive', java, prefix + [str(input_path)])
    check_rows(positive, original_input)
    (out / 'uniform-java-4.3.1.tsv').write_bytes(positive)  # Exact real JVM stdout only.
    input_lines = original_input.decode().splitlines()
    first_case = next(i for i, line in enumerate(input_lines) if line.startswith('CASE\t'))
    fields = input_lines[first_case].split('\t')
    variants = {}
    changed = input_lines.copy()
    changed_fields = fields.copy()
    changed_fields[2] = str(int(changed_fields[2]) + 1)
    changed[first_case] = '\t'.join(changed_fields)
    variants['negative-batch'] = ('\n'.join(changed) + '\n').encode()
    changed = input_lines.copy()
    changed_fields = fields.copy()
    draws = changed_fields[7].split(',')
    draws[0] = str((int(draws[0]) + 1) % (1 << 31))
    changed_fields[7] = ','.join(draws)
    changed[first_case] = '\t'.join(changed_fields)
    variants['negative-draw'] = ('\n'.join(changed) + '\n').encode()
    for label, data in variants.items():
        path = out / (label + '-input.tsv')
        path.write_bytes(data)
        stdout = run('genuine-' + label, java, prefix + [str(path)])
        check_rows(stdout, data)
        assert stdout != positive, 'Independent genuine control must differ'
        (out / ('uniform-java-4.3.1-' + label + '.tsv')).write_bytes(stdout)
    invalid = {'oversized': b'#' + b'x' * 32768}
    changed = input_lines.copy()
    changed_fields = fields.copy()
    changed_fields[5] = str(1 << int(fields[4]))
    changed[first_case] = '\t'.join(changed_fields)
    invalid['availability-mask'] = ('\n'.join(changed) + '\n').encode()
    first_append = next(i for i, line in enumerate(input_lines) if line.startswith('U\t'))
    changed = input_lines.copy()
    changed.insert(first_append + 1, changed[first_append])
    invalid['duplicate-record-alias'] = ('\n'.join(changed) + '\n').encode()
    first_drain = next(i for i, line in enumerate(input_lines) if line.startswith('D\t'))
    changed = input_lines.copy()
    changed[first_drain] = 'D\tunknown_record\t1'
    invalid['unknown-drain-alias'] = ('\n'.join(changed) + '\n').encode()
    for label, data in invalid.items():
        path = out / ('invalid-' + label + '-input.tsv')
        path.write_bytes(data)
        run('invalid-' + label, java, prefix + [str(path)], expected_exit=1)
    write(out / 'receipt.json', {'pin': args.pin, 'scope': 'Genuine SDK state-machine reference and independent negative/bounds controls only; Rust tests and performance still unqualified',
                               'immutable_source': str(source), 'origin_receipt': args.origin_receipt,
                               'origin_sha256': args.origin_sha256,
                               'caller_and_input_actual_Git_blobs_full600_before_after': True,
                               'source_artifact_inputs_before_and_after': observed,
                               'compiled_caller_classes_WORK_only': compiled_identity,
                               'positive_stdout_verbatim_fixture': info(out / 'uniform-java-4.3.1.tsv'),
                               'negative_transitions_genuine_JVM_stdout': {label: info(out / ('uniform-java-4.3.1-' + label + '.tsv')) for label in variants},
                               'invalid_input_controls_nonzero': list(invalid),
                               'no_compiled_artifact_publication': True})
    source_guard('final')


if __name__ == '__main__':
    main()

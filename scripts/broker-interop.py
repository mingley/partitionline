#!/usr/bin/env python3
"""Run bounded public Rust/official Java/native ordinary broker histories.

Inputs are trusted local configuration. Every attempt has a fresh evidence directory;
source trees, peer binaries, record receipts, journals and failed processes are retained.
"""
import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import subprocess
import time

RELEASES = ('4.1.2', '4.2.1', '4.3.1')
SDK_HASHES = {
    '4.1.2': '33b4d9f24ba793ce0ed06607aa92b61d764015d8a0ef72d2558dbb81def4b3ed',
    '4.2.1': '9eb0bcd658da6623b62c01a551f584d0dbed7222d930ec977e51160f55385159',
    '4.3.1': 'dc3d65e3ac811a446184ea1dca0fe9cf957c2d8984dcb4668d01f4b77fc8f50e',
}
SLF_HASH = 'd3ef575e3e4979678dc01bf1dcce51021493b4d11fb7f1be8ad982877c16a1c0'


def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def write(path, value):
    path.write_text(json.dumps(value, indent=2) + '\n')


def verify_source(source, integrity):
    rows = json.loads(integrity.read_text())
    assert re.fullmatch('[0-9a-f]{40}', rows['source_sha'])
    declared = {row['path'] for row in rows['files']}
    actual = {path.relative_to(source).as_posix() for path in source.rglob('*')
              if path.is_file() or path.is_symlink()}
    assert actual == declared, 'full archive file set differs from Git integrity receipt'
    for row in rows['files']:
        path = source / row['path']
        data = str(path.readlink()).encode() if path.is_symlink() else path.read_bytes()
        assert sha_bytes(data) == row['sha256'], row['path']
        assert hashlib.sha1(b'blob ' + str(len(data)).encode() + b'\0' + data).hexdigest() == row['git_blob']
    return rows['source_sha'], len(rows['files'])


def sha_bytes(data):
    return hashlib.sha256(data).hexdigest()


def environment(target):
    env = os.environ.copy()
    env.update(CARGO_HOME='/workspace/work/cargo', RUSTUP_HOME='/workspace/work/rustup',
               CARGO_TARGET_DIR=str(target), CARGO_BUILD_JOBS='1', CARGO_INCREMENTAL='0',
               CARGO_PROFILE_DEV_DEBUG='0', CARGO_PROFILE_TEST_DEBUG='0',
               PATH='/workspace/work/cargo/bin:' + env['PATH'])
    return env


def process(command, cwd, env, log, timeout=300):
    start = time.monotonic()
    timed_out = False
    try:
        result = subprocess.run(['taskset', '-c', '0-2,4', *command], cwd=cwd, env=env,
                                capture_output=True, text=True, timeout=timeout)
        stdout, stderr, status = result.stdout, result.stderr, result.returncode
    except subprocess.TimeoutExpired as error:
        def decoded(value):
            return value.decode('utf-8', errors='replace') if isinstance(value, bytes) else (value or '')
        stdout, stderr, status = decoded(error.stdout), decoded(error.stderr), 124
        stderr += '\nPROCESS_TIMEOUT after ' + str(timeout) + ' seconds\n'
        timed_out = True
    log.write_text('$ ' + ' '.join(command) + '\n' + stdout + stderr)
    return {'command': ['taskset', '-c', '0-2,4', *command], 'exit_code': status,
            'timed_out': timed_out, 'seconds': time.monotonic() - start, 'log_sha256': sha(log)}, stdout


def prepare(args):
    args.evidence.mkdir(parents=True, exist_ok=False)
    args.scratch.mkdir(parents=True, exist_ok=True)
    source_sha, files = verify_source(args.source, args.integrity)
    env = environment(args.target)
    report = {'source_sha': source_sha, 'verified_git_files': files, 'target': str(args.target), 'passed': False,
              'rust': {}, 'server': {}, 'java': [], 'native': {}, 'commands': []}
    peer = args.source / 'tests/conformance/broker/interop'
    oracle = args.source / 'docs/evidence/broker/KL11-68'
    pins = json.loads((peer / 'pins.json').read_text())
    try:
        for relative, expected in pins['immutable_repository_inputs'].items():
            assert sha(args.source / relative) == expected, relative
        assert {row['release']: row['kafka_clients_jar_sha256'] for row in pins['official_apache_releases']} == SDK_HASHES
        assert pins['slf4j_api_1_7_36_sha256'] == SLF_HASH
        for release in RELEASES:
            directory = args.scratch / 'java' / release
            directory.mkdir(parents=True, exist_ok=True)
            jar = args.jars / ('kafka-clients-' + release + '.jar')
            slf = args.jars / 'slf4j-api-1.7.36.jar'
            assert sha(jar) == SDK_HASHES[release] and sha(slf) == SLF_HASH
            for name, original in [('OrdinaryPeer', oracle / 'OrdinaryPeer.java'),
                                   ('JournalPeer', oracle / 'JournalPeer.java'),
                                   ('CrossReadPeer', peer / 'CrossReadPeer.java')]:
                text = original.read_text()
                if release != '4.3.1':
                    text = text.replace('org.apache.kafka.common.record.internal.', 'org.apache.kafka.common.record.')
                adapted = directory / (name + '.java')
                adapted.write_text(text)
                command = ['java', '--add-modules', 'jdk.compiler', 'com.sun.tools.javac.Main',
                           '-Xlint:all', '-Werror', '-cp', str(jar) + ':' + str(slf),
                           '-d', str(directory), str(adapted)]
                receipt, _ = process(command, args.source, env, args.evidence / (release + '-' + name + '.log'), 30)
                report['commands'].append(receipt)
                assert receipt['exit_code'] == 0
                report['java'].append({'release': release, 'name': name, 'directory': str(directory),
                                       'source_sha256': sha(original), 'adapted_source_sha256': sha(adapted),
                                       'class_sha256': sha(directory / (name + '.class')),
                                       'jar': str(jar), 'jar_sha256': sha(jar), 'slf': str(slf), 'slf_sha256': sha(slf)})
        native = json.loads((oracle / 'native-peer-build/validation.json').read_text())
        for name in ('source_sha', 'binary_sha256', 'header_sha256', 'library_sha256'):
            assert native[name] == pins['librdkafka'][name]
        assert sha(oracle / 'ordinary-peer.c') == native['source_sha256']
        assert sha(args.native_binary) == native['binary_sha256']
        assert sha(args.native_header) == native['header_sha256']
        assert sha(args.native_library) == native['library_sha256']
        report['native'] = {'binary': str(args.native_binary), 'binary_sha256': sha(args.native_binary),
                            'pins': native, 'source_sha256': sha(oracle / 'ordinary-peer.c'),
                            'header': str(args.native_header), 'library': str(args.native_library)}
        for toolchain in ('stable',):
            report['commands'].append({'rustc': subprocess.check_output(['rustc', '+' + toolchain, '-Vv'], env=env, text=True)})
            for name, manifest, package in [('peer', peer / 'Cargo.toml', 'partitionline-broker-interop-peer'),
                                             ('client', peer / 'Cargo.toml', 'partitionline'),
                                             ('broker', args.source / 'partitionline-broker/Cargo.toml', 'partitionline-broker')]:
                command = ['cargo', '+' + toolchain, 'clean', '--manifest-path', str(manifest), '-p', package]
                receipt, _ = process(command, args.source, env, args.evidence / (toolchain + '-' + name + '-package-clean.log'))
                report['commands'].append(receipt)
                assert receipt['exit_code'] == 0
            command = ['cargo', '+' + toolchain, 'fmt', '--check', '--manifest-path', str(peer / 'Cargo.toml')]
            receipt, _ = process(command, args.source, env, args.evidence / (toolchain + '-rust-format.log'))
            report['commands'].append(receipt)
            assert receipt['exit_code'] == 0
            for features, flags in [('default', []), ('all-features', ['--all-features'])]:
                manifest = str(peer / 'Cargo.toml')
                command = ['cargo', '+' + toolchain, 'build', '--locked', '--manifest-path', manifest, *flags]
                receipt, _ = process(command, args.source, env, args.evidence / (toolchain + '-' + features + '-rust-build.log'))
                report['commands'].append(receipt)
                assert receipt['exit_code'] == 0
                binary = args.target / 'debug/partitionline-broker-interop-peer'
                saved = args.scratch / 'bin' / toolchain / features / 'rust-peer'
                saved.parent.mkdir(parents=True, exist_ok=True)
                shutil.copy2(binary, saved)
                report['rust'][toolchain + '/' + features] = {'binary': str(saved), 'binary_sha256': sha(saved)}
                command = ['cargo', '+' + toolchain, 'clippy', '--locked', '--manifest-path', manifest, *flags, '--', '-D', 'warnings']
                receipt, _ = process(command, args.source, env, args.evidence / (toolchain + '-' + features + '-rust-clippy.log'))
                report['commands'].append(receipt)
                assert receipt['exit_code'] == 0
            command = ['cargo', '+' + toolchain, 'test', '--locked', '--manifest-path',
                       str(args.source / 'partitionline-broker/Cargo.toml'), '--test', 'fetch',
                       '--no-run', '--message-format=json']
            receipt, stdout = process(command, args.source, env, args.evidence / (toolchain + '-server-build.log'))
            report['commands'].append(receipt)
            assert receipt['exit_code'] == 0
            artifacts = [json.loads(line) for line in stdout.splitlines() if line.startswith('{')]
            executables = [row['executable'] for row in artifacts if row.get('reason') == 'compiler-artifact'
                           and row['target']['name'] == 'fetch' and row.get('executable')]
            assert len(executables) == 1
            saved = args.scratch / 'bin' / toolchain / 'fetch-server'
            shutil.copy2(executables[0], saved)
            report['server'][toolchain] = {'binary': str(saved), 'binary_sha256': sha(saved)}
        report['verified_git_files_after'] = verify_source(args.source, args.integrity)[1]
        report['passed'] = True
    finally:
        write(args.evidence / 'preparation.json', report)
    print(json.dumps({'preparation': str(args.evidence / 'preparation.json'), 'passed': report['passed']}))


def journal_hashes(state):
    return {str(path.relative_to(state)): sha(path) for path in sorted(state.rglob('*.journal'))}


def java_command(build, name, release, port, phase, clients, topic=None, count=None):
    row = next(row for row in build['java'] if row['release'] == release and row['name'] == name)
    assert sha(Path(row['directory']) / (name + '.class')) == row['class_sha256']
    assert sha(Path(row['jar'])) == row['jar_sha256'] and sha(Path(row['slf'])) == row['slf_sha256']
    command = ['java', '-Xms16m', '-Xmx128m', '-cp', row['directory'] + ':' + row['jar'] + ':' + row['slf'],
               name, release, str(port), phase, str(clients)]
    if topic is not None:
        command += [topic, str(count)]
    return command


def run(args):
    args.evidence.mkdir(parents=True, exist_ok=False)
    source_sha, files = verify_source(args.source, args.integrity)
    build = json.loads(args.preparation.read_text())
    assert build['passed'] and build['source_sha'] == source_sha
    lane = args.toolchain + '/' + args.features
    rust = build['rust'][lane]
    server = build['server'][args.toolchain]
    assert sha(Path(rust['binary'])) == rust['binary_sha256']
    assert sha(Path(server['binary'])) == server['binary_sha256']
    native = Path(build['native']['binary'])
    assert sha(native) == build['native']['binary_sha256']
    assert sha(Path(build['native']['header'])) == build['native']['pins']['header_sha256']
    assert sha(Path(build['native']['library'])) == build['native']['pins']['library_sha256']
    env = environment(args.target)
    if args.restart:
        assert args.state.is_dir()
        for name in ('ready', 'stop'):
            (args.state / name).unlink(missing_ok=True)
    else:
        args.state.mkdir(parents=True, exist_ok=False)
    args.clients.mkdir(parents=True, exist_ok=True)
    env.update(PARTITIONLINE_FETCH_LIVE_PORT=str(args.port), PARTITIONLINE_FETCH_LIVE_DIR=str(args.state))
    report = {'source_sha': source_sha, 'toolchain': args.toolchain, 'features': args.features,
              'restart': args.restart, 'verified_git_files': files, 'server': server,
              'rust': rust, 'preparation_sha256': sha(args.preparation), 'passed': False, 'peers': []}
    before = journal_hashes(args.state)
    server_process = None
    server_log = (args.evidence / 'server.log').open('w')
    try:
        command = ['taskset', '-c', '0-2,4', server['binary'], 'serve_live_probe', '--exact', '--nocapture']
        report['server_command'] = command
        server_process = subprocess.Popen(command, cwd=args.source, env=env, stdout=server_log, stderr=subprocess.STDOUT)
        ready_deadline = time.monotonic() + 20
        while not (args.state / 'ready').exists():
            assert server_process.poll() is None, 'server exited before readiness'
            assert time.monotonic() < ready_deadline, 'server readiness deadline'
            time.sleep(0.05)
        jobs = []
        tag = args.toolchain.replace('.', '-') + '-' + args.features
        bootstrap = '127.0.0.1:' + str(args.port)
        if args.restart:
            assert journal_hashes(args.state) == before, 'recovery changed complete journal bytes'
            report['complete_journal_hashes_unchanged_on_recovery'] = True
            jobs.append(('rust-recovery-read', [rust['binary'], bootstrap, 'read', str(args.clients), str(args.clients / 'rust-recovery-read.json'), tag]))
        else:
            for release in RELEASES:
                jobs.append(('java-' + release + '-append', java_command(build, 'OrdinaryPeer', release, args.port, 'append-all', args.clients)))
            jobs.append(('native-append', [str(native), bootstrap, 'append']))
        phase = 'restart' if args.restart else 'seed'
        jobs.append(('rust-' + phase, [rust['binary'], bootstrap, phase, str(args.clients), str(args.clients / ('rust-' + phase + '.json')), tag]))
        for release in RELEASES:
            jobs.append(('java-' + release + '-own-read', java_command(build, 'OrdinaryPeer', release, args.port, 'restart' if args.restart else 'fetch', args.clients)))
            for ack in ('1', '-1', '0'):
                topic = 'ordinary-rust-acks' + ack
                jobs.append(('java-' + release + '-rust-acks' + ack, java_command(build, 'CrossReadPeer', release, args.port, 'restart' if args.restart else 'initial', args.clients, topic, 13 if args.restart else 12)))
            jobs.append(('java-' + release + '-native-read', java_command(build, 'CrossReadPeer', release, args.port, 'restart' if args.restart else 'initial', args.clients, 'ordinary-native', 12)))
        jobs.append(('native-own-read', [str(native), bootstrap, 'restart' if args.restart else 'fetch']))
        for release in RELEASES:
            jobs.append(('native-java-' + release, [str(native), bootstrap, 'fetch', 'ordinary-' + release.replace('.', '-') + '-acks1', '12']))
        for ack in ('1', '-1', '0'):
            jobs.append(('native-rust-acks' + ack, [str(native), bootstrap, 'fetch', 'ordinary-rust-acks' + ack, '13' if args.restart else '12']))
        assert 0 < len(jobs) <= 32
        report['planned_peer_count'] = len(jobs)
        for index, (name, command) in enumerate(jobs):
            assert server_process.poll() is None, 'server stopped during actual peers'
            print('PEER', lane, phase, name, flush=True)
            log = args.evidence / (f'{index:02d}-' + name + '.log')
            receipt, _ = process(command, args.source, env, log, 95)
            report['peers'].append({'name': name, **receipt})
            write(args.evidence / 'validation.json', report)
            assert receipt['exit_code'] == 0, name
            if name == 'rust-recovery-read':
                assert journal_hashes(args.state) == before, 'read-only recovery changed journal bytes'
                report['complete_journal_hashes_unchanged_after_recovery_read'] = True
        report['journal_hashes'] = journal_hashes(args.state)
        shutil.copytree(args.state, args.evidence / 'journal-snapshot')
        shutil.copytree(args.clients, args.evidence / 'client-snapshot')
        (args.state / 'stop').write_text('stop')
        server_process.wait(timeout=15)
        report['server_exit_code'] = server_process.returncode
        assert server_process.returncode == 0
        report['verified_git_files_after'] = verify_source(args.source, args.integrity)[1]
        report['passed'] = True
    finally:
        if server_process is not None and server_process.poll() is None:
            (args.state / 'stop').write_text('stop')
            try:
                server_process.wait(timeout=15)
            except subprocess.TimeoutExpired:
                server_process.kill(); server_process.wait(timeout=5)
        if server_process is not None:
            report['server_exit_code'] = server_process.returncode
        server_log.close()
        if not report['passed']:
            if args.state.exists():
                shutil.copytree(args.state, args.evidence / 'failed-journal-snapshot')
            if args.clients.exists():
                shutil.copytree(args.clients, args.evidence / 'failed-client-snapshot')
        write(args.evidence / 'validation.json', report)
    print(json.dumps({'passed': report['passed'], 'actual_peer_executions': len(report['peers'])}))


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('mode', choices=['prepare', 'run'])
    for name in ('source', 'integrity', 'scratch', 'target', 'evidence'):
        parser.add_argument('--' + name, type=Path, required=True)
    parser.add_argument('--jars', type=Path, default=Path('/workspace/work/broker-wire/jars'))
    parser.add_argument('--native-binary', type=Path, default=Path('/workspace/work/broker-log-oracle/live-peer/ordinary-native'))
    parser.add_argument('--native-header', type=Path, default=Path('/workspace/work/c-peer/source/src/rdkafka.h'))
    parser.add_argument('--native-library', type=Path, default=Path('/workspace/work/c-peer/lib/librdkafka.so.1'))
    parser.add_argument('--preparation', type=Path)
    parser.add_argument('--toolchain', choices=['stable'])
    parser.add_argument('--features', choices=['default', 'all-features'])
    parser.add_argument('--port', type=int, default=19135)
    parser.add_argument('--state', type=Path)
    parser.add_argument('--clients', type=Path)
    parser.add_argument('--restart', action='store_true')
    args = parser.parse_args()
    assert 1024 <= args.port <= 65535
    if args.mode == 'prepare':
        prepare(args)
    else:
        assert all([args.preparation, args.toolchain, args.features, args.state, args.clients])
        run(args)


if __name__ == '__main__':
    main()

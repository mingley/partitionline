#!/usr/bin/env python3
"""Build/run an exact pushed Rust snapshot against pinned independent Java/C peers."""
import argparse
import hashlib
import io
import json
import os
from pathlib import Path
import shutil
import subprocess
import tarfile
import time

EVIDENCE = Path(__file__).resolve().parent
REPO = EVIDENCE.parents[3]
JARS = Path('/workspace/work/broker-wire/jars')
NATIVE = Path('/workspace/work/c-peer')
ENV = dict(os.environ, CARGO_HOME='/workspace/work/cargo', RUSTUP_HOME='/workspace/work/rustup', CARGO_INCREMENTAL='0', CARGO_BUILD_JOBS='1', PATH='/workspace/work/cargo/bin:' + os.environ['PATH'])
JAVA_PINS = {'4.1.2': '33b4d9f24ba793ce0ed06607aa92b61d764015d8a0ef72d2558dbb81def4b3ed', '4.2.1': '9eb0bcd658da6623b62c01a551f584d0dbed7222d930ec977e51160f55385159', '4.3.1': 'dc3d65e3ac811a446184ea1dca0fe9cf957c2d8984dcb4668d01f4b77fc8f50e'}
def sha(path): return hashlib.sha256(path.read_bytes()).hexdigest()
def command(args, cwd, log, env=ENV):
    result = subprocess.run(args, cwd=cwd, env=env, capture_output=True, text=True, timeout=180, check=False)
    log.write_text('$ ' + ' '.join(map(str, args)) + '\n' + result.stdout + result.stderr + f'\nexit_code={result.returncode}\n')
    item = {'command': list(map(str, args)), 'exit_code': result.returncode, 'log': str(log.relative_to(EVIDENCE))}
    return result, item

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--source-sha', required=True)
    parser.add_argument('--scratch', type=Path, required=True)
    parser.add_argument('--toolchain', choices=['stable', '1.85.0'], required=True)
    parser.add_argument('--port', type=int, default=19125)
    parser.add_argument('--attempt', type=int, default=1)
    args = parser.parse_args()
    source = subprocess.check_output(['git', 'rev-parse', args.source_sha + '^{commit}'], cwd=REPO, text=True).strip()
    label = args.toolchain.replace('.', '-')
    assert args.attempt > 0
    evidence = EVIDENCE / 'live' / label / f'attempt-{args.attempt}'
    evidence.mkdir(parents=True, exist_ok=True)
    scratch = args.scratch / label / f'attempt-{args.attempt}'
    snapshot = scratch / 'source'
    snapshot.mkdir(parents=True, exist_ok=False)
    paths = ['partitionline-broker', 'clippy.toml', 'tests/conformance/broker/implemented-api-versions.json']
    archive = subprocess.check_output(['git', 'archive', source, '--', *paths], cwd=REPO)
    with tarfile.open(fileobj=io.BytesIO(archive)) as tar: tar.extractall(snapshot, filter='data')
    sources = {str(p.relative_to(snapshot)): sha(p) for p in snapshot.rglob('*') if p.is_file()}
    for path, digest in sources.items():
        data = subprocess.check_output(['git', 'show', f'{source}:{path}'], cwd=REPO)
        assert hashlib.sha256(data).hexdigest() == digest
    report = {'source_sha': source, 'toolchain': args.toolchain, 'attempt': args.attempt, 'port': args.port, 'snapshot_file_count': len(sources), 'snapshot_hashes': sources, 'commands': [], 'status': 'in_progress', 'limitations': ['No Kafka broker/controller runtime used to assemble golden responses.', 'Runtime probes cover the advertised local single-node metadata/admin subset, not Produce or replication.']}
    report_path = evidence / 'validation.json'
    def save(): report_path.write_text(json.dumps(report, indent=2) + '\n')
    save()
    for version, expected in JAVA_PINS.items(): assert sha(JARS / f'kafka-clients-{version}.jar') == expected
    assert sha(JARS / 'slf4j-api-1.7.36.jar') == 'd3ef575e3e4979678dc01bf1dcce51021493b4d11fb7f1be8ad982877c16a1c0'
    assert sha(NATIVE / 'lib/librdkafka.so.1') == '8083662863399e55acd8ac411ec7b81b6da707974111591182f1105db967adbc'
    assert sha(NATIVE / 'source/src/rdkafka.h') == '33cca14d9fc87117100ce90bd84f2006b8ba521c788055db6af614d0f2f7ca2f'
    report['peers'] = {'java_jars': JAVA_PINS, 'c_source_commit': '9a94e11452cdeb0a844db44ee5dd01ccbe17d3ab', 'c_library_sha256': sha(NATIVE / 'lib/librdkafka.so.1'), 'c_header_sha256': sha(NATIVE / 'source/src/rdkafka.h'), 'java_peer_sha256': sha(EVIDENCE / 'MetadataPeer.java'), 'c_peer_sha256': sha(EVIDENCE / 'metadata-peer.c')}
    target = scratch / 'target'
    buildenv = dict(ENV, CARGO_TARGET_DIR=str(target))
    server = None
    serverlog = None
    live = scratch / 'catalog'
    live.mkdir()
    classes = scratch / 'classes'; classes.mkdir()
    cpeer = scratch / 'metadata-c-peer'
    state = evidence / 'histories'; state.mkdir()
    def run(command_args, filename, cwd=REPO, env=ENV):
        result, item = command(command_args, cwd, evidence / filename, env)
        report['commands'].append(item); save()
        if result.returncode: raise RuntimeError(f'command failed: {filename}')
        return result
    def start(phase):
        nonlocal server, serverlog
        (live / 'ready').unlink(missing_ok=True); (live / 'stop').unlink(missing_ok=True)
        serverlog = (evidence / f'server-{phase}.txt').open('w')
        serverenv = dict(ENV, PARTITIONLINE_METADATA_LIVE_PORT=str(args.port), PARTITIONLINE_METADATA_LIVE_DIR=str(live))
        server = subprocess.Popen(['taskset', '-c', '0-2,4', str(binary), '--exact', 'serve_live_probe', '--nocapture'], cwd=snapshot, env=serverenv, stdout=serverlog, stderr=subprocess.STDOUT)
        deadline = time.monotonic() + 30
        while not (live / 'ready').exists():
            if server.poll() is not None or time.monotonic() > deadline: raise RuntimeError('server failed before ready')
            time.sleep(0.1)
        report.setdefault('server_runs', []).append({'phase': phase, 'pid': server.pid, 'command': ['taskset', '-c', '0-2,4', str(binary), '--exact', 'serve_live_probe', '--nocapture'], 'port': args.port}); save()
    def stop(phase):
        nonlocal server, serverlog
        (live / 'stop').write_text('stop\n')
        code = server.wait(timeout=30)
        serverlog.close()
        report['server_runs'][-1]['exit_code'] = code
        server = None
        journal = live / 'catalog.journal'
        dest = evidence / f'catalog-{phase}.journal'
        shutil.copyfile(journal, dest)
        report['server_runs'][-1]['journal_sha256'] = sha(dest)
        save()
        if code: raise RuntimeError('server exit failed')
    try:
        compiled = run(['taskset', '-c', '0-2,4', 'cargo', '+' + args.toolchain, 'test', '--locked', '--manifest-path', 'partitionline-broker/Cargo.toml', '--test', 'metadata', '--no-run', '--message-format=json'], 'rust-build.txt', snapshot, buildenv)
        artifacts = [json.loads(line) for line in compiled.stdout.splitlines() if line.startswith('{')]
        binaries = [item['executable'] for item in artifacts if item.get('reason') == 'compiler-artifact' and item.get('executable') and item.get('target', {}).get('name') == 'metadata']
        assert len(binaries) == 1
        binary = Path(binaries[0]); report['rust_test_binary_sha256'] = sha(binary)
        run(['gcc', '-std=c11', '-Wall', '-Wextra', '-Werror', '-pedantic', '-isystem', str(NATIVE / 'source/src'), str(EVIDENCE / 'metadata-peer.c'), str(NATIVE / 'lib/librdkafka.so.1'), '-Wl,-rpath,' + str(NATIVE / 'lib'), '-o', str(cpeer)], 'c-compile.txt')
        report['peers']['c_binary_sha256'] = sha(cpeer)
        for version in JAVA_PINS:
            run(['taskset', '-c', '0-2,4', 'java', '--add-modules', 'jdk.compiler', 'com.sun.tools.javac.Main', '-Xlint:all', '-Werror', '-cp', str(JARS / f'kafka-clients-{version}.jar'), '-d', str(classes), str(EVIDENCE / 'MetadataPeer.java')], f'java-compile-{version}.txt')
        report['peers']['java_class_sha256'] = sha(classes / 'MetadataPeer.class'); save()
        for phase in ['create', 'restart']:
            start(phase)
            for version in JAVA_PINS:
                classpath = f'{classes}:{JARS / ("kafka-clients-" + version + ".jar")}:{JARS / "slf4j-api-1.7.36.jar"}'
                run(['taskset', '-c', '0-2,4', 'java', '-cp', classpath, 'MetadataPeer', version, str(args.port), phase, str(state)], f'java-{version}-{phase}.txt')
            run(['taskset', '-c', '0-2,4', str(cpeer), f'127.0.0.1:{args.port}', phase], f'c-{phase}.txt')
            stop(phase)
        report['status'] = 'pass'
    except Exception as error:
        report['status'] = 'failed'; report['failure'] = repr(error)
        raise
    finally:
        if server is not None:
            try:
                (live / 'stop').write_text('stop\n'); code = server.wait(timeout=30)
                report['server_runs'][-1]['cleanup_exit_code'] = code
            except Exception:
                server.terminate(); server.wait(timeout=10)
            if serverlog is not None: serverlog.close()
        report['snapshot_unchanged'] = sources == {str(p.relative_to(snapshot)): sha(p) for p in snapshot.rglob('*') if p.is_file()}
        save()
    print(json.dumps({'source_sha': source, 'toolchain': args.toolchain, 'status': report['status'], 'commands': len(report['commands']), 'server_runs': len(report['server_runs'])}))
if __name__ == '__main__': main()

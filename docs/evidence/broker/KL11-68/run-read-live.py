#!/usr/bin/env python3
"""Run pinned Java/native ordinary readers against an immutable Rust source archive."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess
import time

ROOT = Path(__file__).resolve().parent
RELEASES = ('4.1.2', '4.2.1', '4.3.1')


def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def snapshot(state, destination):
    shutil.copytree(state, destination)
    return {str(path.relative_to(state)): sha(path)
            for path in sorted(state.rglob('*')) if path.is_file()}


def main():
    parser = argparse.ArgumentParser()
    for name in ('source', 'target', 'evidence', 'state', 'jobs'):
        parser.add_argument('--' + name, type=Path, required=True)
    parser.add_argument('--source-sha', required=True)
    parser.add_argument('--toolchain', required=True)
    parser.add_argument('--port', type=int, default=19135)
    parser.add_argument('--features', default='')
    parser.add_argument('--test-name', default='fetch')
    parser.add_argument('--harness-prefix', default='PARTITIONLINE_FETCH_LIVE')
    parser.add_argument('--read-build', type=Path, default=ROOT / 'read-control-build/validation-attempt-5.json')
    parser.add_argument('--binary', type=Path)
    parser.add_argument('--binary-provenance', type=Path)
    parser.add_argument('--restart', action='store_true')
    args = parser.parse_args()
    args.evidence.mkdir(parents=True, exist_ok=False)
    jobs = json.loads(args.jobs.read_text())
    assert isinstance(jobs, list) and 0 < len(jobs) <= 24
    if args.restart:
        assert args.state.is_dir()
        for name in ('ready', 'stop'):
            (args.state / name).unlink(missing_ok=True)
    else:
        args.state.mkdir(parents=True, exist_ok=False)
    env = os.environ.copy()
    env.update(CARGO_HOME='/workspace/work/cargo', RUSTUP_HOME='/workspace/work/rustup',
               CARGO_TARGET_DIR=str(args.target), CARGO_BUILD_JOBS='1', CARGO_INCREMENTAL='0',
               CARGO_PROFILE_DEV_DEBUG='0', CARGO_PROFILE_TEST_DEBUG='0',
               PATH='/workspace/work/cargo/bin:' + env['PATH'],
               **{args.harness_prefix + '_PORT': str(args.port),
                  args.harness_prefix + '_DIR': str(args.state)})
    report = {'source_sha': args.source_sha, 'toolchain': args.toolchain,
              'source_archive': str(args.source), 'features': args.features,
              'port': args.port, 'restart': args.restart,
              'jobs_sha256': sha(args.jobs), 'jobs': jobs, 'peers': [], 'passed': False}
    server = None
    try:
        if args.binary:
            assert args.binary_provenance
            provenance = json.loads(args.binary_provenance.read_text())
            assert provenance['source_sha'] == args.source_sha and provenance['toolchain'] == args.toolchain
            assert provenance['features'] == args.features and provenance['binary_sha256'] == sha(args.binary)
            binary = args.binary
            report['build'] = {'reused_exact_source_binary': True, 'provenance': provenance,
                               'provenance_sha256': sha(args.binary_provenance)}
        else:
            command = ['taskset', '-c', '0-2,4', 'cargo', '+' + args.toolchain, 'test', '--locked',
                       '--manifest-path', str(args.source / 'partitionline-broker/Cargo.toml'),
                       '--test', args.test_name, '--no-run', '--message-format=json']
            if args.features:
                command += ['--features', args.features]
            result = subprocess.run(command, env=env, capture_output=True, text=True, timeout=240)
            (args.evidence / 'build.log').write_text('$ ' + ' '.join(command) + '\n' + result.stdout + result.stderr
                                                  + f'\nexit_code={result.returncode}\n')
            report['build'] = {'command': command, 'exit_code': result.returncode}
            assert result.returncode == 0
            artifacts = [json.loads(line) for line in result.stdout.splitlines() if line.startswith('{')]
            executables = [row['executable'] for row in artifacts if row.get('reason') == 'compiler-artifact'
                           and row['target']['name'] == args.test_name and row.get('executable')]
            assert len(executables) == 1
            binary = Path(executables[0])
        report['binary_sha256'] = sha(binary)
        report['compiler'] = subprocess.check_output(['rustc', '+' + args.toolchain, '-Vv'], env=env, text=True)
        command = ['taskset', '-c', '0-2,4', str(binary), 'serve_live_probe', '--exact', '--nocapture']
        report['server_command'] = command
        with (args.evidence / 'server.log').open('w') as output:
            server = subprocess.Popen(command, cwd=args.source, env=env, stdout=output, stderr=subprocess.STDOUT)
            report['server_pid'] = server.pid
            deadline = time.monotonic() + 15
            while not (args.state / 'ready').exists():
                if server.poll() is not None:
                    raise RuntimeError('server exited before ready')
                if time.monotonic() >= deadline:
                    raise TimeoutError('ready deadline')
                time.sleep(.1)
            client_pins = {row['version']: row['jar_sha256'] for row in json.loads((ROOT / 'upstream-pins.json').read_text())['releases']}
            codec_pins = json.loads((ROOT.parent / 'KL11-67/apache-oracle.json').read_text())['classpath']
            extra = []
            for row in codec_pins:
                assert sha(Path(row['path'])) == row['sha256']
                if row['name'] not in ('kafka-clients-4.3.1.jar', 'slf4j-api-1.7.36.jar'):
                    extra.append(row['path'])
            for ordinal, job in enumerate(jobs, 1):
                kind = job['peer']
                state = Path(job['state']) if 'state' in job else None
                phase = job['phase']
                item = {'job': job}
                if kind == 'native':
                    native = Path('/workspace/work/broker-log-oracle/live-peer/ordinary-native')
                    pins = json.loads((ROOT / 'native-peer-build/validation.json').read_text())
                    assert sha(native) == pins['binary_sha256']
                    assert sha(ROOT / 'ordinary-peer.c') == pins['source_sha256']
                    assert sha(Path('/workspace/work/c-peer/lib/librdkafka.so.1')) == pins['library_sha256']
                    command = ['taskset', '-c', '0-2,4', str(native), '127.0.0.1:' + str(args.port), phase]
                    if 'topic' in job:
                        command += [job['topic'], str(job['count'])]
                    item['pins'] = pins
                else:
                    release = job['release']
                    assert release in RELEASES
                    jars = [f'/workspace/work/broker-wire/jars/kafka-clients-{release}.jar',
                            '/workspace/work/broker-wire/jars/slf4j-api-1.7.36.jar'] + extra
                    assert sha(Path(jars[0])) == client_pins[release]
                    assert sha(Path(jars[1])) == 'd3ef575e3e4979678dc01bf1dcce51021493b4d11fb7f1be8ad982877c16a1c0'
                    if kind == 'ordinary':
                        classes = Path('/workspace/work/broker-log-oracle/live-peer') / release / 'classes'
                        peer = 'OrdinaryPeer'
                        arguments = [release, str(args.port), phase, str(state)]
                        build = json.loads((ROOT / 'live-peer-build/ordinary-java-codec.json').read_text())
                        assert sha(ROOT / 'OrdinaryPeer.java') == build['source_sha256']
                        identity = next(row for row in build['releases'] if row['release'] == release)
                    elif kind == 'read_control':
                        classes = Path('/workspace/work/broker-log-oracle/read-control-peer') / release
                        peer = 'ReadControlPeer'
                        arguments = [release, str(args.port), phase, str(state),
                                     str(args.source / 'partitionline-broker/tests/fixtures/fetch' / release)]
                        build = json.loads(args.read_build.read_text())
                        assert sha(ROOT / 'ReadControlPeer.java') == build['source_sha256']
                        identity = next(row for row in build['results'] if row['release'] == release)
                    elif kind == 'codec_read':
                        classes = Path('/workspace/work/broker-log-oracle/read-control-peer') / release
                        peer = 'CodecReadPeer'
                        arguments = [release, str(args.port), str(state),
                                     str(args.evidence / f'codec-peer-{ordinal:02}-{phase}.json'),
                                     str(args.source / 'partitionline-broker/tests/fixtures/codecs/plain.bin')]
                        build = json.loads(args.read_build.read_text())
                        assert sha(ROOT / 'CodecReadPeer.java') == build['codec_source_sha256']
                        identity = next(row for row in build['codec_results'] if row['release'] == release)
                    else:
                        assert kind == 'native_read'
                        classes = Path('/workspace/work/broker-log-oracle/read-control-peer') / release
                        peer = 'NativeReadPeer'
                        arguments = [release, str(args.port), phase, str(state)]
                        build = json.loads(args.read_build.read_text())
                        assert sha(ROOT / 'NativeReadPeer.java') == build['native_read_source_sha256']
                        identity = next(row for row in build['native_read_results'] if row['release'] == release)
                    assert identity['exit_code'] == 0 and sha(classes / (peer + '.class')) == identity['class_sha256']
                    command = ['taskset', '-c', '0-2,4', 'java', '-Xmx128m', '-cp', ':'.join([str(classes)] + jars), peer] + arguments
                    item.update(peer_source_sha256=sha(ROOT / (peer + '.java')),
                                class_sha256=sha(classes / (peer + '.class')),
                                classpath=[{'path': path, 'sha256': sha(Path(path))} for path in jars])
                started = time.monotonic()
                result = subprocess.run(command, env=env, capture_output=True, text=True, timeout=120)
                log = args.evidence / f'peer-{ordinal:02}-{kind}-{phase}.log'
                log.write_text('$ ' + ' '.join(command) + '\n' + result.stdout + result.stderr + f'\nexit_code={result.returncode}\n')
                item.update(command=command, exit_code=result.returncode, stdout=result.stdout,
                            elapsed_seconds=time.monotonic() - started, log=str(log))
                report['peers'].append(item)
                print(kind, job.get('release', '2.15.0'), phase, result.returncode, result.stdout[-200:], flush=True)
                if result.returncode:
                    raise RuntimeError(f'peer failed: {log}')
            report['passed'] = True
    except Exception as error:
        report['failure'] = repr(error)
        raise
    finally:
        if server is not None:
            (args.state / 'stop').write_text('stop after bounded read peer attempt\n')
            try:
                report['server_exit_code'] = server.wait(timeout=15)
            except subprocess.TimeoutExpired:
                server.terminate()
                report['server_exit_code'] = server.wait(timeout=5)
                report['forced_server_termination'] = True
            report['passed'] = report['passed'] and report['server_exit_code'] == 0
        report['journal_hashes'] = snapshot(args.state, args.evidence / 'journal-snapshot')
        peer_states = dict.fromkeys(job['state'] for job in jobs if 'state' in job)
        report['peer_state_snapshots'] = {}
        for ordinal, state in enumerate(peer_states, 1):
            if Path(state).is_dir():
                destination = args.evidence / f'peer-state-{ordinal:02}'
                shutil.copytree(state, destination)
                report['peer_state_snapshots'][state] = str(destination)
        (args.evidence / 'validation.json').write_text(json.dumps(report, indent=2) + '\n')
    assert report['passed']


if __name__ == '__main__':
    main()

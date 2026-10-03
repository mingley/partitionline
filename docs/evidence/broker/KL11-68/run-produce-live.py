#!/usr/bin/env python3
"""Run bounded ordinary Apache peers against an exact archived Rust Produce source."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess
import time

ROOT = Path(__file__).resolve().parent

def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()

def main():
    parser = argparse.ArgumentParser()
    for name in ['source', 'target', 'evidence', 'state', 'peer-state']:
        parser.add_argument('--' + name, type=Path, required=True)
    parser.add_argument('--source-sha', required=True)
    parser.add_argument('--toolchain', required=True)
    parser.add_argument('--features', default='')
    parser.add_argument('--port', type=int, default=19135)
    parser.add_argument('--phases', required=True)
    parser.add_argument('--restart', action='store_true')
    args = parser.parse_args()
    args.evidence.mkdir(parents=True, exist_ok=False)
    if args.restart:
        assert args.state.is_dir()
        for name in ['ready', 'stop']:
            (args.state / name).unlink(missing_ok=True)
    else:
        args.state.mkdir(parents=True, exist_ok=False)
    args.peer_state.mkdir(parents=True, exist_ok=True)
    env = os.environ.copy()
    env.update(CARGO_HOME='/workspace/work/cargo', RUSTUP_HOME='/workspace/work/rustup',
               CARGO_TARGET_DIR=str(args.target), CARGO_BUILD_JOBS='1', CARGO_INCREMENTAL='0',
               CARGO_PROFILE_DEV_DEBUG='0', CARGO_PROFILE_TEST_DEBUG='0',
               PATH='/workspace/work/cargo/bin:' + env['PATH'],
               PARTITIONLINE_PRODUCE_LIVE_PORT=str(args.port), PARTITIONLINE_PRODUCE_LIVE_DIR=str(args.state),
               PARTITIONLINE_CODEC_FIXTURES=str(args.source / 'partitionline-broker/tests/fixtures/codecs'))
    report = {'source_sha': args.source_sha, 'source_archive': str(args.source), 'toolchain': args.toolchain,
              'features': args.features, 'port': args.port, 'state': str(args.state),
              'peer_state': str(args.peer_state), 'restart': args.restart,
              'peer_source_sha256': sha(ROOT / 'OrdinaryPeer.java'), 'peers': []}
    command = ['taskset', '-c', '0-2,4', 'cargo', '+' + args.toolchain, 'test', '--locked',
               '--manifest-path', str(args.source / 'partitionline-broker/Cargo.toml'),
               '--test', 'produce', '--no-run', '--message-format=json']
    if args.features:
        command += ['--features', args.features]
    built = subprocess.run(command, env=env, capture_output=True, text=True, timeout=180)
    (args.evidence / 'build.log').write_text('$ ' + ' '.join(command) + '\n' + built.stdout + built.stderr + f'\nexit_code={built.returncode}\n')
    report['build'] = {'command': command, 'exit_code': built.returncode}
    assert built.returncode == 0
    artifacts = [json.loads(line) for line in built.stdout.splitlines() if line.startswith('{')]
    executables = [row['executable'] for row in artifacts if row.get('reason') == 'compiler-artifact'
                   and row['target']['name'] == 'produce' and row.get('executable')]
    assert len(executables) == 1
    binary = Path(executables[0])
    report['binary_sha256'] = sha(binary)
    report['compiler'] = subprocess.check_output(['rustc', '+' + args.toolchain, '-Vv'], env=env, text=True)
    command = ['taskset', '-c', '0-2,4', str(binary), 'serve_live_probe', '--exact', '--nocapture']
    report['server_command'] = command
    fixture_pins = json.loads((ROOT.parent / 'KL11-67/apache-oracle.json').read_text())['classpath']
    client_pins = {row['version']: row['jar_sha256'] for row in json.loads((ROOT / 'upstream-pins.json').read_text())['releases']}
    extra = []
    for row in fixture_pins:
        assert sha(Path(row['path'])) == row['sha256']
        if row['name'] not in ['kafka-clients-4.3.1.jar', 'slf4j-api-1.7.36.jar']:
            extra.append(row['path'])
    passed = False
    with (args.evidence / 'server.log').open('w') as output:
        server = subprocess.Popen(command, cwd=args.source, env=env, stdout=output, stderr=subprocess.STDOUT)
        report['server_pid'] = server.pid
        try:
            deadline = time.monotonic() + 10
            while not (args.state / 'ready').exists():
                if server.poll() is not None:
                    raise RuntimeError('server exited before ready')
                if time.monotonic() >= deadline:
                    raise TimeoutError('ready deadline')
                time.sleep(.1)
            for phase in args.phases.split(','):
                for version in ['4.1.2', '4.2.1', '4.3.1']:
                    jars = [f'/workspace/work/broker-wire/jars/kafka-clients-{version}.jar',
                            '/workspace/work/broker-wire/jars/slf4j-api-1.7.36.jar'] + extra
                    assert sha(Path(jars[0])) == client_pins[version]
                    classes = f'/workspace/work/broker-log-oracle/live-peer/{version}/classes'
                    command = ['taskset', '-c', '0-2,4', 'java', '-Xmx128m', '-cp', ':'.join([classes] + jars),
                               'OrdinaryPeer', version, str(args.port), phase, str(args.peer_state)]
                    started = time.monotonic()
                    result = subprocess.run(command, env=env, capture_output=True, text=True, timeout=120)
                    log = args.evidence / f'{version}-{phase}.log'
                    log.write_text('$ ' + ' '.join(command) + '\n' + result.stdout + result.stderr + f'\nexit_code={result.returncode}\n')
                    report['peers'].append({'release': version, 'phase': phase, 'command': command,
                                            'exit_code': result.returncode, 'stdout': result.stdout,
                                            'elapsed_seconds': time.monotonic() - started,
                                            'classpath': [{'path': path, 'sha256': sha(Path(path))} for path in jars],
                                            'class_sha256': sha(Path(classes) / 'OrdinaryPeer.class')})
                    print(version, phase, result.returncode, result.stdout, flush=True)
                    if result.returncode:
                        raise RuntimeError(f'peer failed: {log}')
            passed = True
        finally:
            (args.state / 'stop').write_text('stop after bounded peer attempt\n')
            try:
                report['server_exit_code'] = server.wait(timeout=15)
            except subprocess.TimeoutExpired:
                server.terminate()
                report['server_exit_code'] = server.wait(timeout=5)
                report['forced_server_termination'] = True
            report['passed'] = passed and report['server_exit_code'] == 0
            shutil.copytree(args.state, args.evidence / 'journal-snapshot')
            report['journal_hashes'] = {str(path.relative_to(args.state)): sha(path)
                                        for path in sorted(args.state.rglob('*')) if path.is_file()}
            (args.evidence / 'validation.json').write_text(json.dumps(report, indent=2) + '\n')
    assert report['passed']

if __name__ == '__main__':
    main()

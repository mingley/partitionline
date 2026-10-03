#!/usr/bin/env python3
"""Run broker validation from a supplied immutable archive; retain every exit/result."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import subprocess
import time


def digest(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--source', type=Path, required=True)
    parser.add_argument('--source-sha', required=True)
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--target', type=Path, required=True)
    args = parser.parse_args()
    assert re.fullmatch('[0-9a-f]{40}', args.source_sha)
    source = args.source.resolve()
    output = args.output.resolve()
    output.mkdir(parents=True, exist_ok=False)
    freeze = json.loads((Path(__file__).parent / 'source-freeze.json').read_text())
    for name, value in freeze['source_sha256'].items():
        assert digest(source / name) == value, f'frozen source mismatch: {name}'
    base = os.environ.copy()
    base.update(CARGO_HOME='/workspace/work/cargo', RUSTUP_HOME='/workspace/work/rustup',
                CARGO_INCREMENTAL='0', CARGO_PROFILE_DEV_DEBUG='0', CARGO_PROFILE_TEST_DEBUG='0')
    base['PATH'] = '/workspace/work/cargo/bin:' + base.get('PATH', '')
    commands = []
    results = []

    def run(argv, log, env):
        started = time.monotonic()
        with log.open('wb') as stream:
            result = subprocess.run(['taskset', '-c', '0-2,4'] + argv, cwd=source,
                                    env=env, stdout=stream, stderr=subprocess.STDOUT)
        row = {'argv': ['taskset', '-c', '0-2,4'] + argv, 'cwd': str(source),
               'log': str(log.relative_to(output)), 'exit_code': result.returncode,
               'elapsed_seconds': time.monotonic() - started, 'sha256': digest(log)}
        commands.append(row)
        (output / 'commands.json').write_text(json.dumps(commands, indent=2) + '\n')
        if result.returncode:
            raise SystemExit(f'gate failed; retained {log} (exit {result.returncode})')
        return log.read_text()

    manifest = ['--manifest-path', 'partitionline-broker/Cargo.toml']
    for toolchain in ['stable', '1.85.0']:
        env = base.copy()
        env['CARGO_TARGET_DIR'] = str(args.target.resolve() / toolchain)
        run(['cargo', '+' + toolchain, 'clean'] + manifest + ['-p', 'partitionline-broker'],
            output / f'{toolchain}-clean.log', env)
        for features in ['default', 'all-features']:
            cell = output / (toolchain + '-' + features)
            cell.mkdir()
            selected = ['--all-features'] if features == 'all-features' else []
            testenv = env.copy()
            testenv.update(PL_REPLICATION_SOURCE_SHA=args.source_sha,
                           PL_REPLICATION_RESPONSE_DIR=str(cell / 'replication'),
                           PARTITIONLINE_WIRE_REPORT=str(cell / 'protocol-report.json'),
                           PARTITIONLINE_METADATA_REPORT=str(cell / 'metadata-report.json'),
                           PARTITIONLINE_PRODUCE_REPORT=str(cell / 'produce-report.json'),
                           PARTITIONLINE_FETCH_REPORT=str(cell / 'fetch-report.json'),
                           PL_BROKER_CONTROLLER_REPORT=str(cell / 'controller-report.json'),
                           PL_CONTROLLER_RESPONSE_DIR=str(cell / 'controller-responses'),
                           PARTITIONLINE_FETCH_RESPONSE_DIR=str(cell / 'fetch-responses'))
            # No optional external live-port driver is silently inherited.
            for key in ['PARTITIONLINE_METADATA_LIVE_PORT', 'PARTITIONLINE_PRODUCE_LIVE_PORT',
                        'PARTITIONLINE_FETCH_LIVE_PORT']:
                testenv.pop(key, None)
            text = run(['cargo', '+' + toolchain, 'test'] + manifest + ['--all-targets', '-j1'] + selected,
                       cell / 'tests.log', testenv)
            suites = [tuple(map(int, match)) for match in re.findall(
                r'test result: ok\. (\d+) passed; (\d+) failed; (\d+) ignored', text)]
            assert suites and all(failed == 0 for _, failed, _ in suites)
            results.append({'toolchain': toolchain, 'features': features,
                            'passed': sum(p for p, _, _ in suites),
                            'failed': sum(f for _, f, _ in suites),
                            'ignored': sum(i for _, _, i in suites), 'suite_results': len(suites),
                            'replication_events': [73, 121], 'independent_oracle_status': 'pending separate receipt'})
            (output / 'results-partial.json').write_text(json.dumps(results, indent=2) + '\n')
            run(['python3', str(Path(__file__).resolve().parent / 'seal-traces.py'), str(cell / 'replication'),
                 '--source-root', str(source), '--source-sha', args.source_sha], cell / 'seal.log', env)
            run(['cargo', '+' + toolchain, 'clippy'] + manifest + ['--all-targets', '-j1'] + selected + ['--', '-D', 'warnings'],
                cell / 'clippy.log', env)
            docenv = env.copy()
            docenv['RUSTDOCFLAGS'] = '-D warnings'
            run(['cargo', '+' + toolchain, 'doc'] + manifest + ['--no-deps', '-j1'] + selected,
                cell / 'docs.log', docenv)
            run(['cargo', '+' + toolchain, 'test'] + manifest + ['--doc', '-j1'] + selected,
                cell / 'doctests.log', docenv)
    run(['cargo', '+stable', 'fmt'] + manifest + ['--', '--check'], output / 'fmt.log', base)
    (output / 'results.json').write_text(json.dumps({
        'schema_version': 1, 'source_sha': args.source_sha, 'source_sha256': freeze['source_sha256'],
        'cells': results, 'commands': commands,
        'scope': 'Compiled broker behavior/strict/docs/capture gates; independent Apache/WAL validation and qualification disposition are separate.',
        'limits': ['Typed replication is caller-driven and has no Kafka replication codec or autonomous peer runtime.',
                   'Optional external live-port helper tests are not an external Kafka session proof.',
                   'Dynamic membership, snapshots and production qualification remain separate tasks.']}, indent=2) + '\n')
    print(json.dumps(results))


if __name__ == '__main__':
    main()

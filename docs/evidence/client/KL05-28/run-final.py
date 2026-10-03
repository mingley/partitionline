#!/usr/bin/env python3
"""Qualify the exact pushed capability source and retain every process result."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import subprocess
import time


def verify(source, integrity):
    for row in integrity['files']:
        path = source / row['path']
        data = (str(path.readlink()).encode() if path.is_symlink() else path.read_bytes())
        assert len(data) == row['bytes']
        assert hashlib.sha256(data).hexdigest() == row['sha256'], row['path']
        assert hashlib.sha1(b'blob ' + str(len(data)).encode() + b'\0' + data).hexdigest() == row['git_blob']
    return len(integrity['files'])


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--source', type=Path, required=True)
    parser.add_argument('--target', type=Path, required=True)
    parser.add_argument('--evidence', type=Path, required=True)
    parser.add_argument('--integrity', type=Path, required=True)
    parser.add_argument('--reuse-first-toolchain', action='store_true')
    args = parser.parse_args()
    args.evidence.mkdir(exist_ok=False, parents=True)
    integrity = json.loads(args.integrity.read_text())
    env = os.environ.copy()
    env.update(CARGO_HOME='/workspace/work/cargo', RUSTUP_HOME='/workspace/work/rustup',
               CARGO_TARGET_DIR=str(args.target), CARGO_BUILD_JOBS='1', CARGO_INCREMENTAL='0',
               CARGO_PROFILE_DEV_DEBUG='0', CARGO_PROFILE_TEST_DEBUG='0',
               PATH='/workspace/work/cargo/bin:' + env['PATH'])
    report = {'source_sha': integrity['source_sha'], 'source_archive': str(args.source),
              'environment': {key: env[key] for key in ['CARGO_TARGET_DIR', 'CARGO_BUILD_JOBS', 'CARGO_INCREMENTAL', 'CARGO_PROFILE_DEV_DEBUG', 'CARGO_PROFILE_TEST_DEBUG']},
              'steps': [], 'passed': False}
    steps = [('format', ['cargo', '+stable', 'fmt', '--check'], {})]
    for toolchain in ['stable', '1.85.0']:
        (args.evidence / (toolchain + '-rustc.txt')).write_text(subprocess.check_output(['rustc', '+' + toolchain, '-Vv'], env=env, text=True))
        # Avoid retaining duplicate package binaries for both compilers.
        if toolchain != 'stable' or not args.reuse_first_toolchain:
            steps.append((toolchain + '-package-clean', ['cargo', '+' + toolchain, 'clean', '-p', 'partitionline'], {}))
        for label, flags in [('default', []), ('all-features', ['--all-features'])]:
            cargo = ['cargo', '+' + toolchain]
            steps.extend([
                (toolchain + '-' + label + '-cli-examples', [*cargo, 'build', '--locked', '--example', 'verifiable_producer', '--example', 'verifiable_consumer', *flags], {}),
                (toolchain + '-' + label + '-behavior', [*cargo, 'test', '--locked', '--all-targets', *flags], {}),
                (toolchain + '-' + label + '-clippy', [*cargo, 'clippy', '--locked', '--all-targets', *flags, '--', '-D', 'warnings'], {}),
                (toolchain + '-' + label + '-docs', [*cargo, 'doc', '--locked', '--no-deps', *flags], {'RUSTDOCFLAGS': '-D warnings'}),
                (toolchain + '-' + label + '-doctests', [*cargo, 'test', '--locked', '--doc', *flags], {'RUSTDOCFLAGS': '-D warnings'}),
            ])
    for name, command, extra in steps:
        print('START', name, flush=True)
        before = verify(args.source, integrity)
        actual = ['taskset', '-c', '0-2,4', *command]
        start = time.monotonic()
        with (args.evidence / (name + '.log')).open('w') as log:
            log.write('$ ' + ' '.join(actual) + '\n'); log.flush()
            result = subprocess.run(actual, cwd=args.source, env={**env, **extra}, stdout=log,
                                    stderr=subprocess.STDOUT, timeout=900)
        after = verify(args.source, integrity)
        report['steps'].append({'name': name, 'command': actual, 'exit_code': result.returncode,
                                'seconds': time.monotonic() - start, 'git_files_verified_before': before,
                                'git_files_verified_after': after, 'extra_environment': extra})
        (args.evidence / 'results.json').write_text(json.dumps(report, indent=2) + '\n')
        print('EXIT', name, result.returncode, flush=True)
        if result.returncode:
            raise SystemExit(result.returncode)
    report['passed'] = True
    (args.evidence / 'results.json').write_text(json.dumps(report, indent=2) + '\n')
    print('COMPLETE_EXACT_SOURCE_MATRIX', flush=True)


if __name__ == '__main__':
    main()

#!/usr/bin/env python3
"""Validate an immutable broker codec source snapshot on stable and Rust 1.85."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import subprocess


def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def main():
    if not __debug__:
        raise SystemExit('assertions must be enabled')
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--source', type=Path, required=True)
    parser.add_argument('--source-sha', required=True)
    parser.add_argument('--repo', type=Path, default=Path.cwd())
    parser.add_argument('--target', type=Path, required=True)
    args = parser.parse_args()
    source, repo = args.source.resolve(), args.repo.resolve()
    evidence = repo / 'docs/evidence/broker/KL11-67'
    final = evidence / ('final-' + args.source_sha[:8])
    final.mkdir(exist_ok=False)
    manifest = source / 'partitionline-broker/Cargo.toml'
    environment = dict(os.environ, CARGO_HOME='/workspace/work/cargo',
                       RUSTUP_HOME='/workspace/work/rustup',
                       PATH='/workspace/work/cargo/bin:' + os.environ['PATH'],
                       CARGO_INCREMENTAL='0', CARGO_BUILD_JOBS='1',
                       CARGO_PROFILE_DEV_DEBUG='0', CARGO_PROFILE_TEST_DEBUG='0',
                       RUSTDOCFLAGS='-D warnings')
    commands = []
    for lane in ['stable', '1.85.0']:
        base = ['cargo', '+' + lane]
        gates = [
            ('package-clean', base + ['clean', '--manifest-path', str(manifest),
                                     '--target-dir', str(args.target), '-p', 'partitionline-broker']),
            ('fmt', base + ['fmt', '--manifest-path', str(manifest), '--', '--check']),
            ('default', base + ['test', '--locked', '--manifest-path', str(manifest),
                                '--target-dir', str(args.target), '--all-targets']),
            ('codecs', base + ['test', '--locked', '--manifest-path', str(manifest),
                               '--target-dir', str(args.target), '--all-targets', '--features', 'codecs']),
            ('all-features', base + ['test', '--locked', '--manifest-path', str(manifest),
                                     '--target-dir', str(args.target), '--all-targets', '--all-features']),
            ('clippy', base + ['clippy', '--locked', '--manifest-path', str(manifest),
                               '--target-dir', str(args.target), '--all-targets', '--all-features', '--', '-D', 'warnings']),
            ('rustdoc', base + ['doc', '--locked', '--manifest-path', str(manifest),
                                '--target-dir', str(args.target), '--all-features', '--no-deps']),
            ('doctest-default', base + ['test', '--locked', '--manifest-path', str(manifest),
                                        '--target-dir', str(args.target), '--doc']),
            ('doctest-all-features', base + ['test', '--locked', '--manifest-path', str(manifest),
                                             '--target-dir', str(args.target), '--doc', '--all-features']),
        ]
        for name, command in gates:
            log = final / (lane + '-' + name + '.log')
            with log.open('w') as output:
                result = subprocess.run(['taskset', '-c', '0-2,4'] + command,
                                        env=environment, cwd=source, stdout=output,
                                        stderr=subprocess.STDOUT, timeout=1200)
            contents = log.read_text()
            counts = [int(n) for n in re.findall(r'test result: ok\. (\d+) passed', contents)]
            commands.append(dict(lane=lane, gate=name, command=['taskset', '-c', '0-2,4'] + command,
                                 exit_code=result.returncode, log=log.name,
                                 log_sha256=sha(log), passed_test_count=sum(counts)))
            (final / 'commands.json').write_text(json.dumps(commands, indent=2) + '\n')
            print(lane, name, 'exit', result.returncode, 'tests', sum(counts), flush=True)
            if result.returncode:
                raise SystemExit('Failed gate; retained exact command/status/log: ' + str(log))
    tracked = subprocess.run(['git', 'ls-tree', '-r', '--name-only', args.source_sha,
                              'partitionline-broker', 'Cargo.toml', 'Cargo.lock'],
                             cwd=repo, check=True, capture_output=True, text=True).stdout.splitlines()
    hashes = {name: sha(source / name) for name in tracked}
    for name in tracked:
        exact = subprocess.run(['git', 'show', args.source_sha + ':' + name], cwd=repo,
                               capture_output=True, check=True).stdout
        assert hashlib.sha256(exact).hexdigest() == hashes[name], name
    result = dict(source_sha=args.source_sha, tested_base_sha='37d72a18eef933f9005b9f7453763945882f104e',
                  source_sha256=hashes, command_count=len(commands), commands=commands,
                  environment={k: environment[k] for k in ['CARGO_INCREMENTAL', 'CARGO_BUILD_JOBS',
                               'CARGO_PROFILE_DEV_DEBUG', 'CARGO_PROFILE_TEST_DEBUG', 'RUSTDOCFLAGS']},
                  affinity='0-2,4', source_matches_committed_bytes=True,
                  rustc={lane: subprocess.run(['rustc', '+' + lane, '--version', '--verbose'],
                        env=environment, check=True, capture_output=True, text=True).stdout
                        for lane in ['stable', '1.85.0']})
    (final / 'validation.json').write_text(json.dumps(result, indent=2) + '\n')


if __name__ == '__main__':
    main()

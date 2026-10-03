#!/usr/bin/env python3
"""Prepare the owned adopter against immutable committed client6d, excluding sibling WIP."""
import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess
import time

ROOT = Path('/workspace/partitionline')
OUT = ROOT / 'docs/evidence/broker/KL11-08/preparation/focused-attempt-1'
BASE = Path('/workspace/work/client-capabilities-final/source')
PEER = Path('/workspace/work/broker-interop-focused-peer')
TARGET = Path('/workspace/work/target-client-capabilities')


def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def main():
    OUT.mkdir(parents=True, exist_ok=False)
    PEER.mkdir(parents=True, exist_ok=False)
    original = ROOT / 'tests/conformance/broker/interop'
    (PEER / 'src').mkdir()
    for name in ('Cargo.lock', 'src/main.rs'):
        shutil.copy2(original / name, PEER / name)
    manifest = (original / 'Cargo.toml').read_text().replace('path = "../../../.."', 'path = "' + str(BASE) + '"')
    (PEER / 'Cargo.toml').write_text(manifest)
    shutil.copy2(BASE / 'clippy.toml', PEER / 'clippy.toml')
    integrity = json.loads((ROOT / 'docs/evidence/client/KL05-28/final/source-integrity.json').read_text())
    assert integrity['source_sha'] == '6d6ca9aeed263f24870c4b06531d801be655294e'
    def verify():
        for row in integrity['files']:
            path = BASE / row['path']
            data = str(path.readlink()).encode() if path.is_symlink() else path.read_bytes()
            assert hashlib.sha256(data).hexdigest() == row['sha256']
        return len(integrity['files'])
    env = os.environ.copy()
    env.update(CARGO_HOME='/workspace/work/cargo', RUSTUP_HOME='/workspace/work/rustup',
               CARGO_TARGET_DIR=str(TARGET), CARGO_BUILD_JOBS='1', CARGO_INCREMENTAL='0',
               CARGO_PROFILE_DEV_DEBUG='0', CARGO_PROFILE_TEST_DEBUG='0',
               PATH='/workspace/work/cargo/bin:' + env['PATH'])
    report = {'client_source_sha': integrity['source_sha'], 'status': 'focused strict compile only; no broker runtime',
              'owned_source_files': {name: sha(original / name) for name in ('Cargo.toml', 'Cargo.lock', 'src/main.rs')},
              'adapted_manifest_sha256': sha(PEER / 'Cargo.toml'),
              'adaptation': 'Only the path dependency is redirected to the unchanged full committed6d client archive; no WIP client source or product edits.',
              'clippy_config_sha256': sha(PEER / 'clippy.toml'), 'steps': [], 'passed': False}
    try:
        for tc in ('stable', '1.85.0'):
            commands = [('client-clean', ['clean', '-p', 'partitionline']),
                        ('format', ['fmt', '--check']),
                        ('default-clippy', ['clippy', '--locked', '--', '-D', 'warnings']),
                        ('all-features-clippy', ['clippy', '--locked', '--all-features', '--', '-D', 'warnings'])]
            (OUT / (tc + '-rustc.txt')).write_text(subprocess.check_output(['rustc', '+' + tc, '-Vv'], env=env, text=True))
            for name, tail in commands:
                before = verify()
                command = ['taskset', '-c', '0-2,4', 'cargo', '+' + tc, *tail]
                start = time.monotonic()
                result = subprocess.run(command, cwd=PEER, env=env, capture_output=True, text=True, timeout=180)
                log = OUT / (tc + '-' + name + '.log')
                log.write_text('$ ' + ' '.join(command) + '\n' + result.stdout + result.stderr)
                report['steps'].append({'toolchain': tc, 'name': name, 'command': command, 'exit_code': result.returncode,
                                        'seconds': time.monotonic() - start, 'log_sha256': sha(log),
                                        'client_git_files_verified_before': before, 'client_git_files_verified_after': verify()})
                print(tc, name, result.returncode, flush=True)
                assert result.returncode == 0
        report['passed'] = True
    finally:
        (OUT / 'validation.json').write_text(json.dumps(report, indent=2) + '\n')


if __name__ == '__main__':
    main()

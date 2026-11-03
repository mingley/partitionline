#!/usr/bin/env python3
"""Collect one classic-join stack diagnostic in a dedicated GitHub runner.

The test and harness contain an explicit early stop. These observations are not
full compatibility, production-readiness, or performance qualification.
"""
import argparse
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import platform
import re
import resource
import secrets
import shutil
import subprocess
import sys
import time

COMMIT = '45cbff24de76490594970fc962cd04e7be9fafb1'
FLOOR = 1536 * 1024 * 1024
WALL_SECONDS = 900
CASE_PATCHES = {
    'normal': ['classic-only.patch'],
    'stack_2m': ['classic-only.patch'],
    'stack_8m': ['classic-only.patch'],
    'caller_box': ['classic-caller-box.patch'],
    'library_box': ['classic-only.patch', 'library-box-experiment.patch'],
    'phase_trace': ['classic-only.patch', 'phase-trace-diagnostic.patch'],
}
STACKS = {'stack_2m': '2097152', 'stack_8m': '8388608'}
THIS = Path(__file__).resolve().parent


def hash_file(path):
    digest = hashlib.sha256()
    size = 0
    with path.open('rb') as source:
        while True:
            chunk = source.read(1024 * 1024)
            if not chunk:
                break
            digest.update(chunk)
            size += len(chunk)
    return {'sha256': digest.hexdigest(), 'bytes': size}


def run(argv, cwd=None, timeout=30):
    return subprocess.run(argv, cwd=cwd, text=True, stdout=subprocess.PIPE,
                          stderr=subprocess.PIPE, timeout=timeout, check=True)


def git(subject, *args):
    return run(['git', '-C', str(subject), *args]).stdout.strip()


def check_space(paths):
    for path in paths:
        if shutil.disk_usage(path).free < FLOOR:
            raise RuntimeError('free space is below 1.5 GiB: ' + str(path))


def validate_paths(subject, output):
    if os.environ.get('GITHUB_ACTIONS') != 'true' or platform.system() != 'Linux' or platform.machine() != 'x86_64':
        raise RuntimeError('requires an isolated GitHub Actions Linux x86_64 job')
    workspace = Path(os.environ['GITHUB_WORKSPACE']).resolve(strict=True)
    runner_temp = Path(os.environ['RUNNER_TEMP']).resolve(strict=True)
    if subject.is_symlink() or subject.resolve(strict=True) != workspace / 'subject':
        raise RuntimeError('subject must be the dedicated workspace/subject checkout')
    if output.is_symlink() or output.resolve(strict=True).parent != runner_temp:
        raise RuntimeError('output must be an existing direct child of RUNNER_TEMP')
    if output.name != 'pl-stack-' + os.environ['STACK_CASE']:
        raise RuntimeError('unexpected case output directory')
    git_dir = subject / '.git'
    if git_dir.is_symlink() or not git_dir.is_dir():
        raise RuntimeError('subject must be an independent checkout')
    if Path(git(subject, 'rev-parse', '--show-toplevel')).resolve() != subject.resolve():
        raise RuntimeError('unexpected git worktree root')
    if git(subject, 'rev-parse', 'HEAD') != COMMIT:
        raise RuntimeError('unexpected subject commit')
    if git(subject, 'status', '--porcelain=v1', '--untracked-files=normal'):
        raise RuntimeError('subject checkout must be clean')


def verify_files(subject, pins, overrides=None):
    overrides = overrides or {}
    for name, expected in pins['files'].items():
        actual = hash_file(subject / name)
        if actual != overrides.get(name, expected):
            raise RuntimeError('source hash mismatch: ' + name)


def verify_patches(pins, names):
    for name in names:
        if hash_file(THIS / name) != pins['patches'][name]:
            raise RuntimeError('patch hash mismatch: ' + name)


def prepare(subject, output, case, pins):
    verify_files(subject, pins)
    names = ['harness-classic-only.patch', *CASE_PATCHES[case]]
    verify_patches(pins, names)
    for name in names:
        git(subject, 'apply', '--check', str(THIS / name))
        git(subject, 'apply', str(THIS / name))
    postcase = case if case in ('caller_box', 'library_box', 'phase_trace') else 'normal'
    postimages = pins['known_postimages'][postcase]
    verify_files(subject, pins, postimages)
    changed = set(git(subject, 'diff', '--name-only').splitlines())
    if changed != set(postimages):
        raise RuntimeError('unexpected modified paths: ' + repr(sorted(changed)))
    (output / 'subject.diff').write_text(git(subject, 'diff', '--no-ext-diff') + '\n')
    git(subject, 'add', '--', *sorted(changed))
    git(subject, '-c', 'user.name=stack-diagnostic', '-c', 'user.email=stack-diagnostic@invalid',
        '-c', 'core.hooksPath=/dev/null', 'commit', '--no-gpg-sign', '-qm', 'Local classic-join diagnostic')
    if git(subject, 'status', '--porcelain=v1', '--untracked-files=normal'):
        raise RuntimeError('diagnostic checkout is not clean after local commit')
    return git(subject, 'rev-parse', 'HEAD')


def list_owned(token):
    if re.fullmatch(r'[0-9a-f]{32}', token) is None:
        raise RuntimeError('invalid Docker ownership token')
    output = run(['docker', 'ps', '-aq', '--no-trunc', '--filter',
                  'label=partitionline.stack.run=' + token]).stdout.splitlines()
    result = []
    for ident in output:
        ident = ident.strip()
        if len(ident) != 64 or any(c not in '0123456789abcdef' for c in ident):
            raise RuntimeError('unexpected Docker container id')
        info = json.loads(run(['docker', 'inspect', ident]).stdout)
        if len(info) != 1:
            raise RuntimeError('unexpected Docker inspect result')
        item = info[0]
        labels = item.get('Config', {}).get('Labels') or {}
        if item.get('Id') != ident or item.get('Name') != '/pl-stack-' + token or labels.get('partitionline.stack.run') != token:
            raise RuntimeError('refusing to remove a container without an exact ownership match')
        result.append(ident)
    if len(result) > 1:
        raise RuntimeError('multiple containers claim one stack diagnostic token')
    return result


def cleanup_owned(token):
    removed = []
    for ident in list_owned(token):
        run(['docker', 'rm', '-f', '--', ident], timeout=30)
        removed.append(ident)
    if list_owned(token):
        raise RuntimeError('owned container remains after cleanup')
    return removed


def child_environment(case, output, token):
    # Do not inherit feature overrides, external broker settings, test filters,
    # Rust flags/wrappers, or GitHub output files from the calling shell.
    keep = ('PATH', 'HOME', 'USER', 'LANG', 'LC_ALL', 'TMPDIR', 'TMP', 'TEMP',
            'RUSTUP_HOME', 'CARGO_HOME', 'SSL_CERT_FILE', 'SSL_CERT_DIR',
            'HTTP_PROXY', 'HTTPS_PROXY', 'NO_PROXY', 'http_proxy', 'https_proxy', 'no_proxy')
    env = {name: os.environ[name] for name in keep if name in os.environ}
    env.update(CARGO_BUILD_JOBS='2', CARGO_PROFILE_DEV_DEBUG='0', CARGO_PROFILE_TEST_DEBUG='0',
               CARGO_INCREMENTAL='0', CARGO_TERM_COLOR='never',
               CARGO_TARGET_DIR=str(output.parent / ('pl-stack-target-' + case)),
               PL_COMPAT_REPORT_DIR=str(output / 'report'),
               PL_COMPAT_RUN_VERIFIABLE='0', PL_STACK_RUN_TOKEN=token)
    if case in STACKS:
        env['RUST_MIN_STACK'] = STACKS[case]
    return env


def limits():
    resource.setrlimit(resource.RLIMIT_CORE, (0, 0))
    resource.setrlimit(resource.RLIMIT_FSIZE, (512 * 1024 * 1024, 512 * 1024 * 1024))


def load_owner(subject):
    # The pinned helper is from the immutable subject, not the candidate patch.
    sys.dont_write_bytecode = True
    spec = importlib.util.spec_from_file_location('stack_owner', subject / 'scripts/candidate-command-ownership.py')
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module.owned_command


def classify(code, reason, runtime):
    if reason != 'completed':
        return 'interrupted_or_inconclusive'
    if code == 0 and 'PL_STACK_CLASSIC_JOIN_COMPLETE' in runtime and 'test result: ok. 1 passed; 0 failed' in runtime:
        return 'classic_join_returned'
    if 'has overflowed its stack' in runtime:
        return 'stack_overflow_reported'
    return 'failed_or_inconclusive'


def execute(subject, output, case, pins):
    receipt = {'source_commit': COMMIT, 'case': case, 'diagnostic_only': True,
               'original_live_test_modified_to_stop_after_classic_join': True,
               'stack_environment': STACKS.get(case), 'outcome': 'not_started'}
    # Validate before writing anything to a caller-supplied path.
    validate_paths(subject, output)
    receipt_path = output / 'receipt.json'
    receipt_path.write_text(json.dumps(receipt, indent=2) + '\n')
    token = secrets.token_hex(16)
    cleanup_error = None
    try:
        check_space([subject, output])
        if pins['commit'] != COMMIT:
            raise RuntimeError('unexpected manifest commit')
        if list_owned(token):
            raise RuntimeError('diagnostic container token already in use')
        receipt['manifest'] = hash_file(THIS / 'subject-pins.json')
        receipt['patches'] = {name: pins['patches'][name] for name in
                              ['harness-classic-only.patch', *CASE_PATCHES[case]]}
        receipt['local_diagnostic_commit'] = prepare(subject, output, case, pins)
        owned_command = load_owner(subject)
        env = child_environment(case, output, token)
        started = time.monotonic()
        reason = 'completed'
        owner = None
        try:
            with (output / 'harness.log').open('wb') as log:
                with owned_command(['bash', 'scripts/ci-broker-compatibility.sh', '4.1.2'],
                                   cwd=subject, env=env, stdout=log, stderr=subprocess.STDOUT,
                                   preexec_fn=limits) as owner:
                    while not owner.finished():
                        try:
                            check_space([subject, output])
                        except RuntimeError:
                            reason = 'disk_floor'
                            break
                        if time.monotonic() - started >= WALL_SECONDS:
                            reason = 'wall_timeout'
                            break
                        time.sleep(0.5)
        finally:
            if owner is not None:
                receipt['process_cleanup'] = owner.cleanup_receipt
            try:
                receipt['containers_removed_by_supervisor'] = cleanup_owned(token)
            except Exception as exc:
                cleanup_error = str(exc)
                receipt['container_cleanup_error'] = cleanup_error
        if cleanup_error:
            raise RuntimeError('Docker cleanup unconfirmed: ' + cleanup_error)
        code = owner.process.returncode
        runtime_path = output / 'report/runtime.log'
        runtime = runtime_path.read_text(errors='replace') if runtime_path.is_file() else ''
        binaries = [p for p in (output.parent / ('pl-stack-target-' + case) / 'debug/deps').glob('broker_compatibility-*')
                    if p.is_file() and not p.is_symlink() and os.access(p, os.X_OK)
                    and re.fullmatch(r'broker_compatibility-[0-9a-f]{16}', p.name)]
        receipt['test_binary_after_exit'] = ({'name': binaries[0].name, **hash_file(binaries[0])}
                                              if len(binaries) == 1 else {'matching_count': len(binaries)})
        receipt.update(exit_code=code, stop_reason=reason,
                       wall_seconds=round(time.monotonic() - started, 2),
                       outcome=classify(code, reason, runtime),
                       qualifying_full_compatibility=False, performance_measurement=False)
        verify_files(subject, pins, pins['known_postimages'][
            case if case in ('caller_box', 'library_box', 'phase_trace') else 'normal'])
    except BaseException as exc:
        receipt['outcome'] = 'collector_error'
        receipt['error'] = type(exc).__name__ + ': ' + str(exc)
    finally:
        receipt_path.write_text(json.dumps(receipt, indent=2) + '\n')
    return receipt


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--subject', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--case', required=True, choices=CASE_PATCHES)
    args = parser.parse_args()
    if os.environ.get('STACK_CASE') != args.case:
        parser.error('STACK_CASE and --case must match')
    pins = json.loads((THIS / 'subject-pins.json').read_text())
    receipt = execute(args.subject, args.output, args.case, pins)
    print(json.dumps(receipt, indent=2))
    return 0 if receipt['outcome'] == 'classic_join_returned' else 1


if __name__ == '__main__':
    sys.exit(main())

#!/usr/bin/env python3
"""Run one classic-join witness retaining the original caller source.

The helper unconditionally exits after classic join. This is diagnostic only.
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
CASE_PATCHES = {'baseline': [], 'library_box': ['library-box-experiment.patch']}
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


def run(argv, cwd=None, timeout=30, env=None):
    return subprocess.run(argv, cwd=cwd, env=env, text=True, stdout=subprocess.PIPE,
                          stderr=subprocess.PIPE, timeout=timeout, check=True)


def git(subject, *args):
    return run(['git', '-C', str(subject), *args]).stdout.strip()


def check_space(paths):
    for path in paths:
        if shutil.disk_usage(path).free < FLOOR:
            raise RuntimeError('free space is below 1.5 GiB: ' + str(path))


def validate_paths(subject, output):
    if (os.environ.get('GITHUB_ACTIONS') != 'true'
            or os.environ.get('RUNNER_ENVIRONMENT') != 'github-hosted'
            or platform.system() != 'Linux' or platform.machine() != 'x86_64'):
        raise RuntimeError('requires an isolated GitHub-hosted Linux x86_64 job')
    workspace = Path(os.environ['GITHUB_WORKSPACE'])
    runner_temp = Path(os.environ['RUNNER_TEMP'])
    if (not workspace.is_absolute() or workspace.resolve(strict=True) != workspace
            or not runner_temp.is_absolute() or runner_temp.resolve(strict=True) != runner_temp
            or workspace.stat().st_dev != runner_temp.stat().st_dev
            or workspace.is_relative_to(runner_temp) or runner_temp.is_relative_to(workspace)):
        raise RuntimeError('workspace and runner temp must be canonical on the same filesystem')
    if subject.is_symlink() or subject.resolve(strict=True) != workspace / 'subject':
        raise RuntimeError('subject must be the dedicated workspace/subject checkout')
    if (not output.is_absolute() or output.parent != runner_temp
            or output.exists() or output.is_symlink()):
        raise RuntimeError('output must be a new direct child of RUNNER_TEMP')
    if output.name != 'pl-caller-' + os.environ['STACK_CASE']:
        raise RuntimeError('unexpected case output directory')
    target = runner_temp / ('pl-caller-target-' + os.environ['STACK_CASE'])
    if target.exists() or target.is_symlink():
        raise RuntimeError('build target must be new')
    git_dir = subject / '.git'
    if git_dir.is_symlink() or not git_dir.is_dir():
        raise RuntimeError('subject must be an independent checkout')


def validate_checkout(subject):
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


def verify_caller(subject, pins):
    data = (subject / 'tests/broker_compatibility.rs').read_bytes()
    anchor = b'async fn run() -> partitionline::Result<()> {'
    if data.count(anchor) != 1:
        raise RuntimeError('caller anchor changed')
    start = data.index(anchor)
    expected = pins['original_caller_and_live_test_suffix']
    suffix = data[start:start + expected['bytes']]
    if len(suffix) != expected['bytes'] or hashlib.sha256(suffix).hexdigest() != expected['sha256']:
        raise RuntimeError('original caller or live test source changed')


def commit_changes(subject, output, label, expected_paths):
    changed = set(git(subject, 'diff', '--name-only').splitlines())
    if changed != set(expected_paths):
        raise RuntimeError('unexpected modified paths: ' + repr(sorted(changed)))
    (output / (label + '.diff')).write_text(git(subject, 'diff', '--no-ext-diff') + '\n')
    git(subject, 'add', '--', *sorted(changed))
    git(subject, '-c', 'user.name=caller-witness', '-c', 'user.email=caller-witness@invalid',
        '-c', 'core.hooksPath=/dev/null', 'commit', '--no-gpg-sign', '-qm', label)
    if git(subject, 'status', '--porcelain=v1', '--untracked-files=normal'):
        raise RuntimeError('diagnostic checkout is not clean after local commit')
    return git(subject, 'rev-parse', 'HEAD')


def prepare(subject, output, case, pins):
    verify_files(subject, pins)
    names = ['harness-classic-only.patch', 'future-state-diagnostic.patch', *CASE_PATCHES[case]]
    verify_patches(pins, [*names, 'classic-helper-stop.patch'])
    for name in names:
        git(subject, 'apply', '--check', str(THIS / name))
        git(subject, 'apply', str(THIS / name))
    postimages = pins['known_postimages'][case + '_original_helper']
    verify_files(subject, pins, postimages)
    verify_caller(subject, pins)
    return commit_changes(subject, output, 'original-helper', postimages)


def guard(subject, output, case, pins):
    name = 'classic-helper-stop.patch'
    verify_patches(pins, [name])
    git(subject, 'apply', '--check', str(THIS / name))
    git(subject, 'apply', str(THIS / name))
    postimages = pins['known_postimages'][case + '_guarded_helper']
    verify_files(subject, pins, postimages)
    verify_caller(subject, pins)
    return commit_changes(subject, output, 'guarded-helper', {'tests/broker_compatibility.rs'})


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
               CARGO_TARGET_DIR=str(output.parent / ('pl-caller-target-' + case)),
               PL_COMPAT_REPORT_DIR=str(output / 'report'),
               PL_COMPAT_RUN_VERIFIABLE='0', PL_STACK_RUN_TOKEN=token)
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


def memory_events():
    try:
        fields = dict(line.split() for line in Path('/sys/fs/cgroup/memory.events').read_text().splitlines())
        return {key: int(fields[key]) for key in ('oom', 'oom_kill', 'oom_group_kill') if key in fields}
    except (OSError, ValueError):
        return None


def command(subject, output, name, argv, env, owner_factory):
    check_space([subject, output])
    started = time.monotonic()
    before = memory_events()
    owner = None
    reason = 'completed'
    try:
        with (output / (name + '.log')).open('wb') as log:
            with owner_factory(argv, cwd=subject, env=env, stdout=log,
                               stderr=subprocess.STDOUT, preexec_fn=limits) as owner:
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
        cleanup = owner.cleanup_receipt if owner is not None else None
    after = memory_events()
    try:
        check_space([subject, output])
    except RuntimeError:
        reason = 'disk_floor'
    row = {'command': argv, 'exit_code': owner.process.returncode if owner else None,
           'stop_reason': reason, 'wall_seconds': round(time.monotonic() - started, 2),
           'process_cleanup': cleanup, 'memory_events_before': before, 'memory_events_after': after}
    (output / (name + '.json')).write_text(json.dumps(row, indent=2) + '\n')
    if (owner is None or not cleanup.get('leader_waited') or cleanup.get('remaining_descendants')
            or cleanup.get('descendants_present_at_close') or reason != 'completed'):
        raise RuntimeError('owned command did not complete cleanly: ' + name)
    if before is not None and after is not None and any(
            after.get(key, 0) != value for key, value in before.items()):
        raise RuntimeError('memory event counter changed: ' + name)
    return row


def parse_future_states(text):
    if 'PL_CLASSIC_FUTURE_STATES_COMPLETE' not in text or 'test result: ok. 1 passed; 0 failed' not in text:
        raise RuntimeError('non-polling future state diagnostic did not complete')
    matches = re.findall(r'PL_CLASSIC_FUTURE_STATE\t(caller|join_helper)\t(\d+)\t(\d+)', text)
    if len(matches) != 2 or {item[0] for item in matches} != {'caller', 'join_helper'}:
        raise RuntimeError('future state output is missing or ambiguous')
    states = {name: {'size': int(size), 'align': int(align)} for name, size, align in matches}
    if any(value['size'] <= 0 or value['align'] <= 0 for value in states.values()):
        raise RuntimeError('invalid future state measurement')
    return states


def test_binary(target):
    candidates = [p for p in (target / 'debug/deps').glob('broker_compatibility-*')
                  if p.is_file() and not p.is_symlink() and os.access(p, os.X_OK)
                  and re.fullmatch(r'broker_compatibility-[0-9a-f]{16}', p.name)]
    if len(candidates) != 1:
        raise RuntimeError('test binary is missing or ambiguous')
    return {'name': candidates[0].name, **hash_file(candidates[0])}


def witness_outcome(exit_code, runtime):
    if exit_code == 0 and runtime.count('PL_CLASSIC_WITNESS_JOIN_SUCCEEDED') == 1:
        return 'classic_join_returned_before_process_exit'
    if re.search(r"thread 'live_compatibility_required'(?: \(\d+\))? has overflowed its stack", runtime):
        return 'live_test_stack_overflow'
    return 'failed_or_inconclusive'


def execute(subject, output, case, pins):
    validate_paths(subject, output)
    check_space([subject, output.parent])
    output.mkdir(mode=0o700)
    receipt = {'source_commit': COMMIT, 'case': case, 'diagnostic_only': True,
               'original_caller_and_live_test_source_unchanged': True,
               'helper_terminates_process_after_classic_join': True,
               'full_compatibility_qualified': False, 'performance_measurement': False,
               'outcome': 'not_started'}
    receipt_path = output / 'receipt.json'
    receipt_path.write_text(json.dumps(receipt, indent=2) + '\n')
    token = secrets.token_hex(16)
    try:
        check_space([subject, output])
        validate_checkout(subject)
        if pins['commit'] != COMMIT:
            raise RuntimeError('unexpected manifest commit')
        receipt['manifest'] = hash_file(THIS / 'subject-pins.json')
        receipt['patches'] = {name: pins['patches'][name] for name in
                              ['harness-classic-only.patch', 'future-state-diagnostic.patch',
                               'classic-helper-stop.patch', *CASE_PATCHES[case]]}
        receipt['original_helper_commit'] = prepare(subject, output, case, pins)
        owned_command = load_owner(subject)
        env = child_environment(case, output, token)
        receipt['rustc'] = run(['rustc', '+stable', '-Vv'], cwd=subject, env=env).stdout.strip()
        receipt['cargo'] = run(['cargo', '+stable', '-V'], cwd=subject, env=env).stdout.strip()
        state_command = ['cargo', 'test', '--locked', '--test', 'broker_compatibility', '--',
                         '--ignored', '--exact', 'report_classic_witness_future_states', '--nocapture']
        first = command(subject, output, 'future-original', state_command, env, owned_command)
        if first['exit_code'] != 0:
            raise RuntimeError('original helper state diagnostic failed')
        original_states = parse_future_states((output / 'future-original.log').read_text(errors='replace'))
        receipt['original_state_binary_after_exit'] = test_binary(Path(env['CARGO_TARGET_DIR']))
        verify_files(subject, pins, pins['known_postimages'][case + '_original_helper'])
        verify_caller(subject, pins)
        if git(subject, 'status', '--porcelain=v1', '--untracked-files=normal'):
            raise RuntimeError('source changed during original state diagnostic')
        receipt['guarded_helper_commit'] = guard(subject, output, case, pins)
        second = command(subject, output, 'future-guarded', state_command, env, owned_command)
        if second['exit_code'] != 0:
            raise RuntimeError('guarded helper state diagnostic failed')
        guarded_states = parse_future_states((output / 'future-guarded.log').read_text(errors='replace'))
        receipt['guarded_state_binary_after_exit'] = test_binary(Path(env['CARGO_TARGET_DIR']))
        receipt['future_state_original'] = original_states
        receipt['future_state_guarded'] = guarded_states
        receipt['future_state_size_and_alignment_match'] = original_states == guarded_states
        receipt['state_size_is_not_field_layout_or_poll_stack_proof'] = True
        if original_states != guarded_states:
            receipt['outcome'] = 'future_states_differ_live_witness_not_run'
            return receipt
        verify_files(subject, pins, pins['known_postimages'][case + '_guarded_helper'])
        verify_caller(subject, pins)
        if git(subject, 'status', '--porcelain=v1', '--untracked-files=normal'):
            raise RuntimeError('source changed during guarded state diagnostic')
        if list_owned(token):
            raise RuntimeError('diagnostic container token already in use')
        try:
            live = command(subject, output, 'harness',
                           ['bash', 'scripts/ci-broker-compatibility.sh', '4.1.2'], env, owned_command)
        finally:
            try:
                receipt['containers_removed_by_supervisor'] = cleanup_owned(token)
                receipt['container_cleanup'] = 'confirmed'
            except Exception as exc:
                receipt['container_cleanup'] = 'unconfirmed'
                receipt['container_cleanup_error'] = type(exc).__name__ + ': ' + str(exc)
                raise
        runtime_path = output / 'report/runtime.log'
        runtime = runtime_path.read_text(errors='replace') if runtime_path.is_file() else ''
        receipt['live_exit_code'] = live['exit_code']
        receipt['live_binary_after_exit'] = test_binary(Path(env['CARGO_TARGET_DIR']))
        if receipt['live_binary_after_exit'] != receipt['guarded_state_binary_after_exit']:
            raise RuntimeError('live binary differs from guarded state diagnostic binary')
        receipt['outcome'] = witness_outcome(live['exit_code'], runtime)
        verify_files(subject, pins, pins['known_postimages'][case + '_guarded_helper'])
        verify_caller(subject, pins)
        if git(subject, 'status', '--porcelain=v1', '--untracked-files=normal'):
            raise RuntimeError('source changed during live witness')
    except subprocess.TimeoutExpired as exc:
        receipt['outcome'] = 'collector_error'
        receipt['process_ownership'] = 'unverified_after_probe_timeout'
        receipt['error'] = type(exc).__name__ + ': ' + str(exc)
    except Exception as exc:
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
    return 0 if receipt['outcome'] == 'classic_join_returned_before_process_exit' else 1


if __name__ == '__main__':
    sys.exit(main())

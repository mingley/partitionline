#!/usr/bin/env python3
"""Record clean-tree broker CI commands, resource samples, and bounded execution."""
import argparse
import datetime
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import signal
import subprocess
import sys
import time

ROOT = Path('/workspace/partitionline')
TARGET = '/workspace/work/qualification-20261027/broker-check-target'
ROOTS = ['src', 'tests', 'examples', 'benches', '.cargo', 'scripts', 'fuzz',
         'partitionline-broker/src', 'partitionline-broker/tests',
         'partitionline-broker/examples', 'partitionline-broker/benches',
         'partitionline-broker/.cargo', 'partitionline-broker/scripts']
FILES = ['Cargo.toml', 'Cargo.lock', 'rust-toolchain.toml', 'clippy.toml', 'deny.toml',
         'README.md', 'CHANGELOG.md', 'LICENSE-APACHE', 'LICENSE-MIT', 'NOTICE', 'SECURITY.md',
         'partitionline-broker/README.md', 'partitionline-broker/Cargo.toml', 'partitionline-broker/Cargo.lock',
         'partitionline-broker/build.rs', 'build.rs', 'docs/latency-baseline.json']
BROKER = 'partitionline-broker/Cargo.toml'
CASES = [
    ('format', ['fmt', '--manifest-path', BROKER, '--', '--check']),
    ('default', ['test', '--locked', '--manifest-path', BROKER, '--all-targets', '--no-fail-fast']),
    ('all-features', ['test', '--locked', '--manifest-path', BROKER, '--all-targets', '--all-features', '--no-fail-fast']),
    ('clippy', ['clippy', '--locked', '--manifest-path', BROKER, '--all-targets', '--all-features', '--', '-D', 'warnings']),
    ('doc-build', ['doc', '--locked', '--manifest-path', BROKER, '--all-features', '--no-deps']),
    ('doc-default', ['test', '--locked', '--manifest-path', BROKER, '--doc']),
    ('doc-all-features', ['test', '--locked', '--manifest-path', BROKER, '--doc', '--all-features']),
]

def sha(path):
    with path.open('rb') as f:
        return hashlib.file_digest(f, 'sha256').hexdigest()

def git(*args):
    return subprocess.check_output(['git', *args], cwd=ROOT, text=True).rstrip('\n')

def input_hashes():
    paths = set()
    for name in ROOTS:
        root = ROOT / name
        if root.is_dir():
            paths.update(p for p in root.rglob('*') if p.is_file()
                         and '__pycache__' not in p.parts and p.suffix != '.pyc')
    paths.update(ROOT / name for name in FILES if (ROOT / name).is_file())
    return {str(p.relative_to(ROOT)): sha(p) for p in sorted(paths)}

def ignored_inputs():
    values = git('ls-files', '--others', '--ignored', '--exclude-standard', '--', *ROOTS)
    return [s for s in values.splitlines() if '__pycache__' not in Path(s).parts and not s.endswith('.pyc')]

def read_cgroup(name):
    p = Path('/sys/fs/cgroup') / name
    return p.read_text().strip() if p.exists() else None

def memory_counter(contents, key):
    for line in (contents or '').splitlines():
        parts = line.split()
        if len(parts) == 2 and parts[0] == key:
            return int(parts[1])
    return None

def snapshot(base, process=None):
    item = {'utc': datetime.datetime.now(datetime.timezone.utc).isoformat(),
            'free_bytes': shutil.disk_usage(base).free,
            'memory_current': read_cgroup('memory.current'), 'memory_events': read_cgroup('memory.events'),
            'cpu_stat': read_cgroup('cpu.stat')}
    if process is not None:
        item['process_exit'] = process.poll()
    return item

def group_processes(pgid):
    result = subprocess.run(['ps', '-eo', 'pid,ppid,pgid,stat,etime,comm'], capture_output=True, text=True)
    lines = []
    for line in result.stdout.splitlines()[1:]:
        fields = line.split(None, 5)
        if len(fields) == 6 and fields[2] == str(pgid):
            lines.append(line.strip())
    return lines

def stop_group(proc):
    try:
        os.killpg(proc.pid, signal.SIGTERM)
    except ProcessLookupError:
        return
    try:
        proc.wait(timeout=5)
    except subprocess.TimeoutExpired:
        try:
            os.killpg(proc.pid, signal.SIGKILL)
        except ProcessLookupError:
            pass
        proc.wait()

def archive_executables(log_text, out, label, stop_bytes):
    found = []
    paths = dict.fromkeys(re.findall(r'Running [^\n]*?\((/[^)]+)\)', log_text))
    for index, name in enumerate(paths):
        source = Path(name)
        item = {'source_path': name, 'retained': False}
        if not source.is_file():
            item['note'] = 'binary missing at post-run capture'
            found.append(item)
            continue
        item.update(size=source.stat().st_size, sha256=sha(source))
        if shutil.disk_usage(out).free < stop_bytes:
            item['note'] = 'hash only: disk floor reached'
        else:
            copy = out / f'{label}-{index}.elf'
            if copy.exists():
                raise RuntimeError(f'refusing to overwrite {copy}')
            attempt = subprocess.run(['cp', '--reflink=always', '--preserve=mode', '--', str(source), str(copy)],
                                     capture_output=True, text=True)
            if attempt.returncode == 0:
                if sha(copy) != item['sha256']:
                    raise RuntimeError(f'reflink copy hash mismatch {copy}')
                item.update(retained=True, path=str(copy), method='reflink')
            else:
                # Retain an unsuccessful partial copy, if any, rather than deleting it silently.
                item['note'] = 'hash only: reflink unavailable or failed'
                item['reflink_error'] = attempt.stderr.strip()
                if copy.exists():
                    item['partial_copy'] = str(copy)
        found.append(item)
    return found

def run_case(label, args, output, env, head, baseline, timeout, stop_bytes):
    log_path = output / f'{label}.log'
    receipt_path = output / f'{label}.json'
    if log_path.exists() or receipt_path.exists():
        raise RuntimeError(f'output exists for {label}')
    before_head, before_status, before_ignored = git('rev-parse', 'HEAD'), git('status', '--porcelain=v1', '--untracked-files=all'), ignored_inputs()
    before = input_hashes()
    if before_head != head or before_status or before_ignored or before != baseline:
        raise RuntimeError(f'clean-source preflight failed before {label}')
    command = ['cargo', '+stable', *args]
    rec = {'command': command, 'base_source_sha': head, 'source_sha': head, 'status_before': before_status,
           'input_scope': {'directories': ROOTS, 'files': FILES, 'limitations': 'Inputs are hashed before and after each case; reverted transient changes between captures are not detected. External tools, credentials, host kernel, and all environment variables are not exhaustively hashed.'},
           'input_files': before, 'ignored_inputs_before': before_ignored,
           'started_utc': datetime.datetime.now(datetime.timezone.utc).isoformat(),
           'timeout_seconds': timeout, 'stop_free_bytes': stop_bytes,
           'rustc_version': subprocess.check_output(['rustc', '-Vv'], env=env, text=True),
           'cargo_version': subprocess.check_output(['cargo', '-Vv'], env=env, text=True),
           'cargo_target_dir': env['CARGO_TARGET_DIR'], 'cargo_build_jobs': env['CARGO_BUILD_JOBS'],
           'rust_test_threads': env.get('RUST_TEST_THREADS'), 'memory_limit': read_cgroup('memory.max'),
           'cpu_max': read_cgroup('cpu.max'), 'samples': [snapshot(output)]}
    start = time.monotonic()
    stop_reason = None
    print('START', label, flush=True)
    with log_path.open('x') as log:
        proc = subprocess.Popen(command, cwd=ROOT, env=env, stdout=log, stderr=subprocess.STDOUT,
                                start_new_session=True)
        while proc.poll() is None:
            elapsed = time.monotonic() - start
            time.sleep(min(1, max(0.1, timeout - elapsed)))
            sample = snapshot(output, proc)
            sample['elapsed_seconds'] = time.monotonic() - start
            rec['samples'].append(sample)
            if proc.poll() is not None:
                break
            if sample['free_bytes'] < stop_bytes:
                stop_reason = 'disk_free_below_floor'
            elif sample['elapsed_seconds'] >= timeout:
                stop_reason = 'whole_command_timeout'
            if stop_reason:
                rec['processes_before_termination'] = group_processes(proc.pid)
                stop_group(proc)
                break
        rec['exit'] = proc.wait()
    rec['elapsed_seconds'] = time.monotonic() - start
    rec['stop_reason'] = stop_reason
    log_text = log_path.read_text(errors='replace')
    rec['log_sha256'] = sha(log_path)
    rec['results'] = re.findall(r'^test result:.*$', log_text, re.M)
    rec['failures'] = re.findall(r'^test (.*?) \.\.\. FAILED$', log_text, re.M)
    rec['executables'] = archive_executables(log_text, output, label, stop_bytes)
    after = input_hashes()
    rec['input_changes_during_run'] = sorted(k for k in before.keys() | after.keys() if before.get(k) != after.get(k))
    rec['ending_head'] = git('rev-parse', 'HEAD')
    rec['status_after'] = git('status', '--porcelain=v1', '--untracked-files=all')
    rec['ignored_inputs_after'] = ignored_inputs()
    rec['inputs_stable'] = not rec['input_changes_during_run'] and rec['ending_head'] == head and not rec['status_after'] and not rec['ignored_inputs_after']
    if not rec['inputs_stable']:
        rec['source_sha'] = None
    rec['resource_after'] = snapshot(output)
    initial_oom = memory_counter(rec['samples'][0]['memory_events'], 'oom_kill')
    final_oom = memory_counter(rec['resource_after']['memory_events'], 'oom_kill')
    rec['oom_kills_increased'] = initial_oom is not None and final_oom is not None and final_oom > initial_oom
    rec['finished_utc'] = datetime.datetime.now(datetime.timezone.utc).isoformat()
    receipt_path.write_text(json.dumps(rec, indent=2) + '\n')
    print('END', label, json.dumps({k: rec[k] for k in ('exit', 'stop_reason', 'inputs_stable', 'elapsed_seconds')}), flush=True)
    return rec

def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output', type=Path, required=True, help='New output directory outside the repository')
    parser.add_argument('--target-dir', default=TARGET)
    parser.add_argument('--start-free-gib', type=float, default=3.0)
    parser.add_argument('--stop-free-gib', type=float, default=2.0)
    parser.add_argument('--timeout-seconds', type=int, default=2400)
    args = parser.parse_args()
    if args.timeout_seconds < 1 or args.stop_free_gib <= 0 or args.start_free_gib < args.stop_free_gib:
        parser.error('invalid timeout or disk floors')
    output = args.output.resolve()
    if output.is_relative_to(ROOT):
        parser.error('output must be outside the repository')
    if output.exists():
        parser.error('output directory already exists')
    if not output.parent.is_dir():
        parser.error('output parent directory does not exist')
    if shutil.disk_usage(output.parent).free < int(args.start_free_gib * 1024**3):
        parser.error('insufficient free disk for start floor')
    output.mkdir(parents=True)
    env = dict(os.environ)
    env.update(RUSTUP_HOME='/workspace/work/rustup', CARGO_HOME='/workspace/work/cargo',
               PATH='/workspace/work/cargo/bin:' + env['PATH'], CARGO_TARGET_DIR=args.target_dir,
               CARGO_BUILD_JOBS='2', CARGO_INCREMENTAL='0', CARGO_PROFILE_DEV_DEBUG='0', CARGO_PROFILE_TEST_DEBUG='0')
    # Match Cargo/CI default test threading rather than inheriting a caller override.
    for key in ('RUST_TEST_THREADS', 'RUST_MIN_STACK', 'RUSTFLAGS', 'RUSTDOCFLAGS',
                'CARGO_ENCODED_RUSTFLAGS', 'RUSTC_WRAPPER', 'RUSTC_WORKSPACE_WRAPPER'):
        env.pop(key, None)
    head = git('rev-parse', 'HEAD')
    status = git('status', '--porcelain=v1', '--untracked-files=all')
    ignored = ignored_inputs()
    if status or ignored:
        raise SystemExit('refusing dirty/untracked/ignored build input tree')
    baseline = input_hashes()
    sequence = []
    for label, command in CASES:
        if shutil.disk_usage(output).free < int(args.start_free_gib * 1024**3):
            print('STOP before', label, 'start disk floor', flush=True)
            break
        result = run_case(label, command, output, env, head, baseline, args.timeout_seconds,
                          int(args.stop_free_gib * 1024**3))
        sequence.append({key: result[key] for key in ('command', 'exit', 'stop_reason', 'inputs_stable', 'oom_kills_increased')})
        if result['stop_reason'] or not result['inputs_stable'] or result['oom_kills_increased']:
            break
    summary = {'head': head, 'planned_cases': [x[0] for x in CASES], 'completed_cases': sequence,
               'complete': len(sequence) == len(CASES),
               'all_commands_succeeded': len(sequence) == len(CASES) and all(x['exit'] == 0 and not x['stop_reason'] and x['inputs_stable'] and not x['oom_kills_increased'] for x in sequence),
               'note': 'Success does not imply ignored tests or external interoperability profiles were exercised.'}
    (output / 'summary.json').write_text(json.dumps(summary, indent=2) + '\n')
    return 0 if summary['all_commands_succeeded'] else 1

if __name__ == '__main__':
    sys.exit(main())

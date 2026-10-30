#!/usr/bin/env python3
"""Build, run and audit source-bound broker fuzz campaigns on stable Rust."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import platform
import re
import shutil
import signal
import subprocess
import sys
import time

ROOT = Path(__file__).resolve().parents[1]
TARGETS = ('parse_header', 'decode_kraft', 'controller_respond', 'recover_journal', 'recover_epoch')
TRIPLE = 'x86_64-unknown-linux-gnu'
COVERAGE = '-Cpasses=sancov-module -Cllvm-args=-sanitizer-coverage-level=4 -Cllvm-args=-sanitizer-coverage-inline-8bit-counters -Cllvm-args=-sanitizer-coverage-pc-table'
PARENT = ROOT / 'benchmarks/runtime/tools/parent-bound-exec.py'
DAY = 86400


def sha(path):
    with Path(path).open('rb') as stream:
        return hashlib.file_digest(stream, 'sha256').hexdigest()


def read(path):
    return json.loads(Path(path).read_text())


def write(path, data):
    path = Path(path)
    temporary = path.with_suffix(path.suffix + '.tmp')
    temporary.write_text(json.dumps(data, indent=2, sort_keys=True) + '\n')
    temporary.replace(path)


def utc():
    return time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime())


def cpu_quota_metadata():
    path = Path('/sys/fs/cgroup/cpu.max')
    try:
        value = path.read_text(encoding='ascii').strip()
    except FileNotFoundError:
        # Some Linux runners expose cgroup v1 or no visible CPU controller.
        # This is observational metadata, not an enforced worker CPU limit.
        return {'cpu_quota': None, 'cpu_quota_source': None,
                'cpu_quota_status': 'unavailable'}
    return {'cpu_quota': value, 'cpu_quota_source': str(path),
            'cpu_quota_status': 'available'}


def pin_sources():
    names = subprocess.check_output(['git', 'ls-files', '-z', 'partitionline-broker/src',
                                    'partitionline-broker/Cargo.toml', 'fuzz/broker'], cwd=ROOT).decode().split('\0')
    names += ['scripts/broker-fuzz-campaign.py', str(PARENT.relative_to(ROOT))]
    return {name: sha(ROOT / name) for name in sorted(set(names)) if name}


def build(output):
    if platform.machine() != 'x86_64' or sys.platform != 'linux':
        raise ValueError('This instrumented build profile is Linux x86_64 only')
    output.mkdir(parents=True, exist_ok=False)
    before = pin_sources()
    cargo = shutil.which('cargo')
    if cargo is None:
        raise ValueError('cargo is required in PATH')
    compiler = subprocess.check_output(['rustc', '+stable', '-vV'], text=True)
    env = dict(os.environ)
    for key in ('RUSTFLAGS', 'CARGO_ENCODED_RUSTFLAGS', 'CARGO_BUILD_TARGET'):
        env.pop(key, None)
    env.update(CARGO_TARGET_DIR=str(output / 'target'), CARGO_INCREMENTAL='0', CARGO_BUILD_JOBS='4',
               CARGO_TARGET_X86_64_UNKNOWN_LINUX_GNU_RUSTFLAGS=COVERAGE)
    command = [cargo, '+stable', 'build', '--release', '--locked', '--target', TRIPLE]
    receipt = {'schema_version': 1, 'state': 'building', 'source_commit': subprocess.check_output(
        ['git', 'rev-parse', 'HEAD'], cwd=ROOT, text=True).strip(), 'sources': before,
        'compiler': compiler, 'command': command, 'cwd': str(ROOT / 'fuzz/broker'),
        'coverage_flags': COVERAGE, 'started_utc': utc(), 'features': 'broker default features only'}
    write(output / 'build.json', receipt)
    with (output / 'build.log').open('xb') as log:
        result = subprocess.run(command, cwd=ROOT / 'fuzz/broker', env=env, stdout=log, stderr=subprocess.STDOUT)
    receipt.update(exit_code=result.returncode, finished_utc=utc(), build_log_sha256=sha(output / 'build.log'))
    if result.returncode or pin_sources() != before:
        receipt['state'] = 'failed'
        write(output / 'build.json', receipt)
        raise ValueError('Build failed or source changed during compilation; retain build.log')
    binaries = {}
    for name in TARGETS:
        source = output / 'target' / TRIPLE / 'release' / name
        destination = output / 'bin' / name
        destination.parent.mkdir(exist_ok=True)
        shutil.copyfile(source, destination)
        destination.chmod(0o700)
        binaries[name] = {'path': str(destination), 'sha256': sha(destination)}
    receipt.update(state='built', binaries=binaries)
    write(output / 'build.json', receipt)
    return receipt


def checked_build(directory):
    receipt = read(directory / 'build.json')
    if receipt.get('state') != 'built' or receipt.get('exit_code') != 0 or receipt['sources'] != pin_sources():
        raise ValueError('Successful exact-source build receipt required')
    if receipt['coverage_flags'] != COVERAGE or receipt['features'] != 'broker default features only':
        raise ValueError('Unsupported build profile')
    for name in TARGETS:
        if sha(receipt['binaries'][name]['path']) != receipt['binaries'][name]['sha256']:
            raise ValueError('Built executable changed')
    return receipt


def stats(log):
    with log.open('rb') as stream:
        stream.seek(max(0, log.stat().st_size - 262144))
        text = stream.read().decode(errors='replace')
    coverage = re.findall(r'\bcov:\s*(\d+)', text)
    progress = re.findall(r'^#(\d+)\s', text, re.M)
    units = re.findall(r'stat::number_of_executed_units:\s*(\d+)', text)
    elapsed = re.findall(r'Done \d+ runs in (\d+) second', text)
    return {'coverage': int(coverage[-1]) if coverage else 0,
            'executions': int(units[-1]) if units else int(progress[-1]) if progress else 0,
            'engine_elapsed_seconds': int(elapsed[-1]) if elapsed else None,
            'final_stats_present': bool(units and elapsed)}


def disk_usage(directory):
    total = count = 0
    for base, _, names in os.walk(directory):
        for name in names:
            try:
                total += (Path(base) / name).stat().st_size
                count += 1
            except FileNotFoundError:
                pass
    return total, count


def sample(pid):
    fields = {}
    try:
        for line in Path(f'/proc/{pid}/status').read_text().splitlines():
            if line.startswith(('VmRSS:', 'VmHWM:', 'Threads:')):
                key, value = line.split(':', 1)
                fields[key] = int(value.split()[0]) * (1024 if key.startswith('Vm') else 1)
    except FileNotFoundError:
        pass
    return fields


def terminate(children):
    for child in children:
        if child.poll() is None:
            os.killpg(child.pid, signal.SIGTERM)
    deadline = time.monotonic() + 5
    for child in children:
        try:
            child.wait(timeout=max(.01, deadline - time.monotonic()))
        except subprocess.TimeoutExpired:
            os.killpg(child.pid, signal.SIGKILL)
            child.wait(timeout=5)


def run(build_dir, output, kind, seconds, resume_from=None):
    if kind not in ('smoke', 'campaign') or type(seconds) is not int or not 1 <= seconds <= 7 * DAY:
        raise ValueError('Invalid kind or duration')
    if kind == 'campaign' and seconds < DAY:
        raise ValueError('Campaign requires at least 86400 seconds per target')
    if kind == 'smoke' and seconds > 300:
        raise ValueError('Smoke must be at most 300 seconds')
    receipt = checked_build(build_dir)
    previous = None
    if resume_from:
        old = read(resume_from / 'manifest.json')
        if not (resume_from / 'result.json').exists() or old['build_sha256'] != sha(build_dir / 'build.json'):
            raise ValueError('Resume requires a stopped attempt with the same frozen build')
        previous = {'path': str(resume_from), 'manifest_sha256': sha(resume_from / 'manifest.json'),
                    'result_sha256': sha(resume_from / 'result.json')}
    output.mkdir(parents=True, exist_ok=False)
    children, logs, jobs = [], [], []
    manifest = {'schema_version': 1, 'kind': kind, 'seconds_per_target': seconds, 'started_utc': utc(),
                'supervisor_pid': os.getpid(), 'build_directory': str(build_dir), 'build_sha256': sha(build_dir / 'build.json'),
                'build': receipt, 'resume_from': previous, 'targets': {},
                'host': {'machine': platform.machine(), 'platform': platform.platform(),
                         **cpu_quota_metadata()},
                'limits': {'rss_mib_per_target': 512, 'input_bytes': 65536, 'timeout_seconds_per_input': 2,
                           'disk_bytes_per_target': 512 * 1024 * 1024, 'files_per_target': 100000,
                           'log_bytes_per_target': 64 * 1024 * 1024},
                'scope': 'Default-feature local parser/recovery fuzzing; no broker production or speed claim'}
    for index, name in enumerate(TARGETS):
        target = output / name
        for directory in ('corpus', 'artifacts', 'tmp'):
            (target / directory).mkdir(parents=True)
        seeds = resume_from / name / 'corpus' if resume_from else ROOT / 'fuzz/broker/seeds' / name
        seed_hashes = {}
        for source in sorted(seeds.iterdir()):
            if not source.is_file() or source.is_symlink():
                raise ValueError('Corpus must contain only ordinary files')
            seed_hashes[source.name] = sha(source)
            shutil.copyfile(source, target / 'corpus' / source.name)
        if not seed_hashes:
            raise ValueError('Missing seed corpus')
        binary = receipt['binaries'][name]['path']
        command = [binary, f'-seed={4269539329 + index}', f'-max_total_time={seconds}', '-timeout=2',
                   '-rss_limit_mb=512', '-malloc_limit_mb=128', '-max_len=65536', '-print_final_stats=1',
                   '-use_value_profile=1', f'-artifact_prefix={target / "artifacts"}/', str(target / 'corpus')]
        manifest['targets'][name] = {'command': command, 'seed_sha256': seed_hashes, 'binary_sha256': receipt['binaries'][name]['sha256']}
    write(output / 'manifest.json', manifest)
    write(output / 'frozen.json', {'manifest_sha256': sha(output / 'manifest.json')})
    stopping = []
    def stop(signum, _frame):
        stopping.append(f'signal {signum}')
    old_handlers = {sig: signal.signal(sig, stop) for sig in (signal.SIGINT, signal.SIGTERM)}
    start = time.monotonic()
    reason = None
    try:
        for name in TARGETS:
            target = output / name
            log = (target / 'fuzzer.log').open('xb')
            logs.append(log)
            env = dict(os.environ, TMPDIR=str(target / 'tmp'))
            child = subprocess.Popen([sys.executable, str(PARENT), str(os.getpid()), *manifest['targets'][name]['command']],
                                     cwd=target, env=env, stdout=log, stderr=subprocess.STDOUT, start_new_session=True)
            children.append(child)
            jobs.append({'target': name, 'pid': child.pid, 'started_utc': utc(), 'started_monotonic': time.monotonic()})
        write(output / 'jobs.json', {'supervisor_pid': os.getpid(), 'jobs': jobs})
        with (output / 'telemetry.jsonl').open('x') as telemetry:
            while True:
                rows = []
                for job, child in zip(jobs, children):
                    target = output / job['target']
                    size, count = disk_usage(target)
                    rows.append({'target': job['target'], 'pid': child.pid, 'returncode': child.poll(),
                                 'elapsed_seconds': time.monotonic() - job['started_monotonic'],
                                 'disk_bytes': size, 'files': count, **sample(child.pid), **stats(target / 'fuzzer.log')})
                    if size > manifest['limits']['disk_bytes_per_target'] or count > 100000 or (target / 'fuzzer.log').stat().st_size > 64 * 1024 * 1024:
                        reason = 'artifact resource budget exceeded'
                    if child.returncode is not None and child.returncode != 0:
                        reason = f'{job["target"]} failed with {child.returncode}'
                telemetry.write(json.dumps({'utc': utc(), 'elapsed_seconds': time.monotonic() - start, 'targets': rows}) + '\n')
                telemetry.flush()
                write(output / 'status.json', {'state': 'running', 'utc': utc(), 'supervisor_pid': os.getpid(), 'targets': rows})
                if reason or stopping or (output / 'STOP').exists() or all(child.poll() is not None for child in children):
                    break
                if time.monotonic() - start > seconds + 30:
                    reason = 'supervisor duration deadline exceeded'
                    break
                time.sleep(.2 if kind == 'smoke' else 5)
    except Exception as error:
        reason = f'supervisor error: {type(error).__name__}: {error}'
    finally:
        terminate(children)
        for log in logs:
            log.close()
        for sig, handler in old_handlers.items():
            signal.signal(sig, handler)
        for job, child in zip(jobs, children):
            job.update(returncode=child.returncode, parent_waited=True,
                       elapsed_seconds=time.monotonic() - job['started_monotonic'])
    interrupted = bool(stopping or (output / 'STOP').exists())
    result = {'schema_version': 1, 'finished_utc': utc(), 'elapsed_seconds': time.monotonic() - start,
              'state': 'interrupted' if interrupted else 'failed' if reason else 'completed',
              'reason': reason or (stopping[0] if stopping else 'STOP file' if interrupted else None),
              'jobs': jobs, 'artifacts': {str(p.relative_to(output)): sha(p) for p in output.rglob('*') if p.is_file()
              and p.name not in ('status.json', 'result.json')}, 'production_qualified': False}
    write(output / 'result.json', result)
    verdict = audit(output)
    write(output / 'status.json', verdict)
    return verdict


def audit(output):
    manifest = read(output / 'manifest.json')
    if sha(output / 'manifest.json') != read(output / 'frozen.json')['manifest_sha256']:
        raise ValueError('Frozen manifest changed')
    if not (output / 'result.json').exists():
        return {'state': 'incomplete', 'campaign_qualified': False, 'production_qualified': False}
    result = read(output / 'result.json')
    required = {'manifest.json', 'frozen.json', 'jobs.json', 'telemetry.jsonl'}
    required.update(f'{name}/fuzzer.log' for name in TARGETS)
    if not required <= result['artifacts'].keys():
        raise ValueError('Required artifacts missing from receipt')
    for name, digest in result['artifacts'].items():
        path = output / name
        if not path.resolve().is_relative_to(output.resolve()) or sha(path) != digest:
            raise ValueError('Retained artifact changed')
    jobs = result['jobs']
    if {j['target'] for j in jobs} != set(TARGETS) or len(jobs) != len(TARGETS) or not all(j['parent_waited'] for j in jobs):
        raise ValueError('Missing target or unjoined child')
    summaries = {}
    complete = result['state'] == 'completed'
    for job in jobs:
        observed = stats(output / job['target'] / 'fuzzer.log')
        summaries[job['target']] = observed
        complete = complete and job['returncode'] == 0 and observed['final_stats_present'] and observed['coverage'] > 0 and observed['executions'] > 0
        complete = complete and observed['engine_elapsed_seconds'] is not None and observed['engine_elapsed_seconds'] >= manifest['seconds_per_target']
        complete = complete and job['elapsed_seconds'] >= manifest['seconds_per_target']
    qualified = complete and manifest['kind'] == 'campaign' and manifest['seconds_per_target'] >= DAY
    if qualified:
        previous = -1
        with (output / 'telemetry.jsonl').open() as stream:
            for line in stream:
                row = json.loads(line)
                elapsed = row['elapsed_seconds']
                if elapsed < previous or elapsed - previous > 60 or {r['target'] for r in row['targets']} != set(TARGETS):
                    raise ValueError('Missing or discontinuous campaign telemetry')
                previous = elapsed
        if previous < manifest['seconds_per_target']:
            raise ValueError('Telemetry does not cover the required interval')
    return {'state': 'completed' if complete else result['state'] if result['state'] != 'completed' else 'failed',
            'kind': manifest['kind'], 'campaign_qualified': qualified, 'production_qualified': False,
            'targets': summaries, 'reason': result['reason'], 'elapsed_seconds': result['elapsed_seconds']}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('action', choices=('build', 'run', 'audit'))
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--build', type=Path)
    parser.add_argument('--kind', choices=('smoke', 'campaign'), default='campaign')
    parser.add_argument('--seconds', type=int, default=DAY)
    parser.add_argument('--resume-from', type=Path)
    args = parser.parse_args()
    output = args.output.resolve()
    if args.action == 'build':
        result = build(output)
        result = {key: result[key] for key in ('state', 'source_commit', 'compiler', 'binaries')}
    elif args.action == 'audit':
        result = audit(output)
    else:
        if args.build is None:
            parser.error('run requires --build')
        result = run(args.build.resolve(), output, args.kind, args.seconds,
                     args.resume_from.resolve() if args.resume_from else None)
    print(json.dumps(result, sort_keys=True))
    if args.action != 'build' and result['state'] not in ('completed', 'running', 'incomplete', 'interrupted'):
        raise SystemExit(1)


if __name__ == '__main__':
    main()

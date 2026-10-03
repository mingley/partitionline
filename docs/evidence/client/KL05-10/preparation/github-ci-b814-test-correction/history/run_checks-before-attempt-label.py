"""Bounded offline Python/format proof; never invokes Cargo, Java, or a broker."""
import hashlib
import json
import os
from pathlib import Path
import signal
import stat
import subprocess
import sys
import time

ROOT = Path(__file__).resolve().parent
CANDIDATE = ROOT / 'candidate'
FLOOR = 350 * 1024 * 1024

def inventory():
    rows = []
    for path in sorted(CANDIDATE.rglob('*')):
        if path.is_symlink():
            raise RuntimeError(f'Unexpected candidate symlink: {path}')
        if path.is_file():
            raw = path.read_bytes()
            rows.append({'path': str(path.relative_to(CANDIDATE)), 'bytes': len(raw),
                         'sha256': hashlib.sha256(raw).hexdigest(),
                         'mode_07777': oct(stat.S_IMODE(path.stat().st_mode))})
    return rows

def save(path, value):
    assert not path.exists(), str(path)
    path.write_text(json.dumps(value, indent=2, sort_keys=True) + '\n')
    path.chmod(0o600)

phase = sys.argv[1]
if phase == 'failed-first':
    cmd = ['taskset', '-c', '2,4', sys.executable, '-m', 'unittest', '-v',
           'test_verifiable_scenario.VerifiableScenario.test_single_case_cannot_pass_full_primary_registry']
    cwd = CANDIDATE / 'tests/conformance'
elif phase == 'corrected-cohort':
    cmd = ['taskset', '-c', '2,4', sys.executable, '-m', 'unittest', 'discover', '-v',
           '-s', 'tests/conformance', '-p', 'test_*.py']
    cwd = CANDIDATE
elif phase in ('stable-format', 'msrv-format'):
    toolchain = 'stable' if phase == 'stable-format' else '1.85.0'
    cmd = ['taskset', '-c', '2,4',
           f'/workspace/work/rustup/toolchains/{toolchain}-x86_64-unknown-linux-gnu/bin/rustfmt',
           '--edition', '2021', '--check', str(CANDIDATE / 'tests/sticky_partitioner.rs')]
    cwd = CANDIDATE
else:
    raise ValueError(phase)

out = ROOT / 'checks' / phase
out.mkdir(parents=True, exist_ok=False)
temporary = ROOT / 'temporary'
temporary.mkdir(exist_ok=True)
env = os.environ.copy()
env.update(PYTHONDONTWRITEBYTECODE='1', TMPDIR=str(temporary),
           CARGO_HOME='/workspace/work/cargo', RUSTUP_HOME='/workspace/work/rustup')
before = inventory()
save(out / 'source-before.json', before)
save(out / 'command.json', {'argv': cmd, 'cwd': str(cwd), 'deadline_seconds': 120,
                          'cpu_set': '2,4', 'disk_floor_bytes': FLOOR,
                          'env_overrides': {key: env[key] for key in ['PYTHONDONTWRITEBYTECODE', 'TMPDIR', 'CARGO_HOME', 'RUSTUP_HOME']},
                          'runtime_scope': 'Offline Python unit fixtures and syntax formatting only; no Cargo/JVM/SDK/broker.'})
minimum_free = 2**63 - 1
start = time.monotonic()
wall_start = time.time_ns()
termination = None
with (out / 'stdout.log').open('wb') as stdout, (out / 'stderr.log').open('wb') as stderr:
    proc = subprocess.Popen(cmd, cwd=cwd, env=env, stdout=stdout, stderr=stderr, start_new_session=True)
    while proc.poll() is None:
        free = os.statvfs(ROOT).f_bavail * os.statvfs(ROOT).f_frsize
        minimum_free = min(minimum_free, free)
        if free < FLOOR:
            termination = 'disk-floor'
        elif time.monotonic() - start > 120:
            termination = 'absolute-deadline'
        elif stdout.tell() + stderr.tell() > 2 * 1024 * 1024:
            termination = 'output-bound'
        if termination:
            os.killpg(proc.pid, signal.SIGTERM)
            try:
                proc.wait(timeout=3)
            except subprocess.TimeoutExpired:
                os.killpg(proc.pid, signal.SIGKILL)
            break
        time.sleep(0.2)
    exit_code = proc.wait()
after = inventory()
save(out / 'source-after.json', after)
assert before == after, 'Candidate paths, bytes, or full modes changed during command.'
for path in (out / 'stdout.log', out / 'stderr.log'):
    path.chmod(0o600)
save(out / 'receipt.json', {'phase': phase, 'exit_code': exit_code, 'termination': termination,
                         'wall_started_ns': wall_start, 'wall_finished_ns': time.time_ns(),
                         'elapsed_seconds': time.monotonic() - start,
                         'minimum_observed_free_bytes': minimum_free,
                         'source_files': len(before), 'source_unchanged': True,
                         'compiled_rust': False, 'real_sdk_or_broker': False})
print(json.dumps({'phase': phase, 'exit_code': exit_code, 'elapsed': time.monotonic() - start,
                  'minimum_free_bytes': minimum_free, 'source_unchanged': True}))
sys.exit(0 if phase == 'failed-first' and exit_code == 1 else exit_code)

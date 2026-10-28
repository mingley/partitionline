import datetime
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import subprocess
import sys
import time

STUDY = Path("/workspace/work/goal-qualification-20261028/preauth-budget")
ENV = dict(os.environ)
ENV.update(RUSTUP_HOME='/workspace/work/rustup', CARGO_HOME='/workspace/work/cargo',
    PATH='/workspace/work/cargo/bin:' + ENV['PATH'],
    CARGO_TARGET_DIR='/workspace/work/preauth-budget-target',
    CARGO_BUILD_JOBS=os.environ.get('QUALIFICATION_CARGO_JOBS', '2'),
    CARGO_INCREMENTAL='0', CARGO_PROFILE_DEV_DEBUG='0', CARGO_PROFILE_TEST_DEBUG='0')
ROOTS = ['src', 'tests', 'examples', 'benches', '.cargo', 'scripts', 'fuzz',
         'partitionline-broker/src', 'partitionline-broker/tests']
FILES = ['Cargo.toml', 'Cargo.lock', 'rust-toolchain.toml', 'clippy.toml', 'deny.toml',
         'README.md', 'CHANGELOG.md', 'LICENSE-APACHE', 'LICENSE-MIT', 'NOTICE', 'SECURITY.md',
         'partitionline-broker/Cargo.toml', 'partitionline-broker/Cargo.lock',
         'partitionline-broker/build.rs', 'build.rs', 'docs/latency-baseline.json']

def sha256(path):
    digest = hashlib.sha256()
    with path.open('rb') as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b''):
            digest.update(chunk)
    return digest.hexdigest()

def inputs():
    paths = set()
    for name in ROOTS:
        root = Path(name)
        if root.is_dir():
            paths.update(path for path in root.rglob('*') if path.is_file()
                         and '__pycache__' not in path.parts and path.suffix != '.pyc')
    for name in FILES:
        if Path(name).is_file():
            paths.add(Path(name))
    return {str(path): sha256(path) for path in sorted(paths)}

def git(*args):
    return subprocess.check_output(['git', *args], text=True).strip()

def ignored_build_inputs():
    output = git('ls-files', '--others', '--ignored', '--exclude-standard', '--', *ROOTS)
    return '\n'.join(name for name in output.splitlines()
                     if '__pycache__' not in Path(name).parts and not name.endswith('.pyc'))

def run(name, command):
    log = STUDY / (name + '.log')
    assert not log.exists(), log
    base = git('rev-parse', 'HEAD')
    status_before = git('status', '--porcelain=v1', '--untracked-files=all')
    ignored_inputs = ignored_build_inputs()
    before = inputs()
    receipt = {'command': command, 'base_source_sha': base,
        'source_sha': base if not status_before and not ignored_inputs else None,
        'input_scope': {'directories': ROOTS, 'files': FILES,
                        'limitations': 'External tools and environment are not exhaustively hashed; ignored and untracked input status is recorded.'},
        'input_files': before, 'git_status_before': status_before,
        'ignored_input_files_before': ignored_inputs,
        'rustc_version': subprocess.check_output(['rustc', '-Vv'], env=ENV, text=True),
        'cargo_version': subprocess.check_output(['cargo', '-Vv'], env=ENV, text=True),
        'started_utc': datetime.datetime.now(datetime.timezone.utc).isoformat(),
        'cargo_build_jobs': ENV['CARGO_BUILD_JOBS'],
        'codec_matrix_output': ENV.get('PL_CODEC_MATRIX_OUTPUT'),
        'rust_min_stack': ENV.get('RUST_MIN_STACK'),
        'memory_events_before': Path('/sys/fs/cgroup/memory.events').read_text()}
    print('Running ' + name, flush=True)
    start = time.monotonic()
    with log.open('x') as stream:
        result = subprocess.run(command, env=ENV, stdout=stream, stderr=subprocess.STDOUT)
    log_text = log.read_text()
    after = inputs()
    end_head = git('rev-parse', 'HEAD')
    status_after = git('status', '--porcelain=v1', '--untracked-files=all')
    ignored_after = ignored_build_inputs()
    changes = sorted(path for path in before.keys() | after.keys() if before.get(path) != after.get(path))
    stable = not changes and base == end_head and ignored_inputs == ignored_after
    if not stable or status_before != status_after:
        receipt['source_sha'] = None
    receipt.update(exit=result.returncode, elapsed_seconds=time.monotonic() - start,
        results=re.findall(r'^test result:.*$', log_text, re.M),
        failures=re.findall(r'^test (.*?) \.\.\. FAILED$', log_text, re.M),
        log_sha256=sha256(log), input_changes_during_run=changes,
        inputs_stable=stable, ending_head=end_head, git_status_after=status_after,
        ignored_input_files_after=ignored_after)
    executables = []
    for index, match in enumerate(re.finditer(r'Running .*?\((/[^)]+)\)', log_text)):
        binary = Path(match[1])
        if binary.is_file():
            artifact = {'source_path': str(binary), 'sha256': sha256(binary), 'retained': False}
            retain = binary.name.startswith('api_versions_allocation-') or (name == 'preauth-unit-01' and binary.name.startswith('partitionline-'))
            if retain and shutil.disk_usage(STUDY).free > binary.stat().st_size + 2 * 1024**3:
                retained = STUDY / (name + f'-{index}.elf')
                shutil.copyfile(binary, retained)
                assert sha256(retained) == artifact['sha256'], retained
                artifact.update(path=str(retained), retained=True)
            elif retain:
                artifact['retention_note'] = 'not copied: reserved 2 GiB free space'
            else:
                artifact['retention_note'] = 'hash only: outside selected critical test targets'
            executables.append(artifact)
    receipt['executables'] = executables
    receipt['memory_events_after'] = Path('/sys/fs/cgroup/memory.events').read_text()
    receipt['disk_free_bytes_after'] = shutil.disk_usage(STUDY).free
    (STUDY / (name + '.json')).write_text(json.dumps(receipt, indent=2) + '\n')
    print(json.dumps({k: receipt[k] for k in ['exit', 'elapsed_seconds', 'results', 'failures', 'inputs_stable']}), flush=True)
    return 0 if result.returncode == 0 and stable else 1

if __name__ == '__main__':
    name = sys.argv[1]
    command = ['cargo', '+stable'] + sys.argv[2:]
    seconds = os.environ.get('QUALIFICATION_PROCESS_TIMEOUT_SECS')
    if seconds:
        assert seconds.isdigit() and int(seconds) > 0
        command = ['timeout', '--signal=TERM', '--kill-after=2s', seconds + 's'] + command
    sys.exit(run(name, command))

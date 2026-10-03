#!/usr/bin/env python3
"""Run both integration and capture-enabled library diagnostics with immutable-source/cache/disk guards."""
import argparse
import gzip
import hashlib
import json
import os
from pathlib import Path
import re
import signal
import subprocess
import time

P = argparse.ArgumentParser()
P.add_argument('--source', required=True)
P.add_argument('--tree', required=True)
P.add_argument('--origin', required=True)
P.add_argument('--scratch', required=True)
A = P.parse_args()
def interrupted(signum, _frame):
    raise KeyboardInterrupt('focused runner signal ' + str(signum))
signal.signal(signal.SIGTERM, interrupted)
REPO = Path('/workspace/partitionline')
TREE = Path(A.tree).resolve()
OUT = Path(A.scratch).resolve()
OUT.mkdir(parents=True, exist_ok=False)
DISK_FLOOR_BYTES = 350 * 1024 * 1024
DISK_POLL_SECONDS = 0.5
ENV = dict(os.environ, CARGO_HOME='/workspace/work/cargo',
           RUSTUP_HOME='/workspace/work/rustup',
           PATH='/workspace/work/cargo/bin:' + os.environ['PATH'],
           CARGO_INCREMENTAL='0', CARGO_BUILD_JOBS='1',
           CARGO_PROFILE_DEV_DEBUG='0', CARGO_PROFILE_TEST_DEBUG='0',
           CARGO_TARGET_DIR='/workspace/work/target-broker-segments',
           CARGO_NET_OFFLINE='true')
os.sched_setaffinity(0, {2, 4})
assert os.sched_getaffinity(0) == {2, 4}
for protected in (REPO, TREE, OUT):
    cache = Path(ENV['CARGO_TARGET_DIR']).resolve()
    assert not protected.is_relative_to(cache) and not cache.is_relative_to(protected)
driver = Path(__file__).resolve()
driver_sha256 = hashlib.sha256(driver.read_bytes()).hexdigest()
helpers = driver.with_name('guard-functions.py')
helper_provenance = json.loads(driver.with_name('guard-provenance.json').read_text())
assert hashlib.sha256(helpers.read_bytes()).hexdigest() == helper_provenance['extracted_sha256']
elf_objects = {}
elf_snapshots = {}
receipt = {'schema_version': 1, 'task': 'KL11-76',
           'scope': 'first immutable focused diagnostic; not final qualification',
           'source_commit': A.source, 'source_tree': str(TREE),
           'driver_sha256': driver_sha256, 'guard_provenance': helper_provenance,
           'commands': [], 'retained_elf_objects': [],
           'affinity': [2, 4],
           'environment': {key: ENV[key] for key in ('CARGO_HOME', 'RUSTUP_HOME',
               'CARGO_TARGET_DIR', 'CARGO_INCREMENTAL', 'CARGO_BUILD_JOBS',
               'CARGO_PROFILE_DEV_DEBUG', 'CARGO_PROFILE_TEST_DEBUG', 'CARGO_NET_OFFLINE')}}
exec(compile(helpers.read_text(), str(helpers), 'exec'), globals())
origin_path = Path(A.origin).resolve()
origin = json.loads(origin_path.read_text())
assert origin['source_commit'] == A.source and Path(origin['source_directory']).resolve() == TREE
manifest_path = Path(origin['source_manifest']['path'])
assert sha256_file(manifest_path) == origin['source_manifest']['compressed_sha256']
raw_manifest = gzip.decompress(manifest_path.read_bytes())
assert hashlib.sha256(raw_manifest).hexdigest() == origin['source_manifest']['uncompressed_sha256']
expected = json.loads(raw_manifest)
for pin in expected.values():
    pin['filesystem_permission_mode'] = pin['full_permission_mode']
git_rows = subprocess.check_output(['git', 'ls-tree', '-r', '-z', A.source], cwd=REPO)
actual_git = {}
for row in git_rows.split(b'\0'):
    if row:
        metadata, name = row.split(b'\t', 1)
        mode, kind, blob = metadata.decode().split()
        assert kind == 'blob'
        actual_git[name.decode()] = {'mode': mode, 'blob': blob}
assert set(actual_git) == set(expected)
for name, pin in expected.items():
    assert actual_git[name] == {'mode': pin['mode'], 'blob': pin['git_blob_sha1']}
receipt['source_origin'] = {'path': str(origin_path), 'sha256': sha256_file(origin_path),
                            'manifest': origin['source_manifest'], 'complete_git_path_set_verified': True}
receipt['toolchain'] = subprocess.check_output(['rustc', '+stable', '-Vv'], env=ENV, text=True)
capture_root = OUT / 'captures'
capture_root.mkdir()

def save():
    receipt['retained_elf_objects'] = list(elf_objects.values())
    (OUT / 'validation.json').write_text(json.dumps(receipt, indent=2) + '\n')

schedule = [
    ('package-clean', ['cargo', '+stable', 'clean', '--offline', '--locked',
                       '--manifest-path', 'partitionline-broker/Cargo.toml',
                       '--target-dir', ENV['CARGO_TARGET_DIR'], '--package', 'partitionline-broker']),
    ('stable-default-runtime-tests', ['cargo', '+stable', 'test', '--offline', '--locked',
                       '--manifest-path', 'partitionline-broker/Cargo.toml', '--test', 'raft_runtime',
                       '--', '--test-threads=1', '--nocapture']),
    ('stable-default-runtime-lib-tests', ['cargo', '+stable', 'test', '--offline', '--locked',
                       '--manifest-path', 'partitionline-broker/Cargo.toml', '--lib', 'raft::runtime::',
                       '--', '--test-threads=1', '--nocapture'])]
try:
    for name, argv in schedule:
        assert sha256_file(driver) == driver_sha256
        before = verify()
        owners = cache_owners()
        assert not owners, ('active cache reference before command', owners)
        forecast = retention_forecast()
        receipt.setdefault('pre_command_guards', []).append({'command': name, 'owners': owners, 'forecast': forecast})
        save()
        assert forecast['sufficient'], ('retention reserve refuses command; originals untouched', forecast)
        records = retain_cache(name, 'all current cache ELF preserved before clean/build/overwrite', force_verify=True)
        owners_after = cache_owners()
        assert not owners_after, ('active cache reference after retention', owners_after)
        lane = OUT / name
        lane.mkdir()
        capture = capture_root / name
        capture.mkdir()
        ENV.update(PL_PEER_RUNTIME_CAPTURE_DIR=str(capture), PL_PEER_RUNTIME_SOURCE_SHA=A.source)
        started = time.time()
        code = 75
        failure = None
        monitor = None
        try:
            with (lane / 'command.log').open('w') as log:
                code, monitor = monitored_command(['taskset', '-c', '2,4', *argv], TREE, ENV,
                                                  log, lane / 'disk-monitor.jsonl')
        except BaseException as error:
            failure = repr(error)
        output = (lane / 'command.log').read_text()
        item = {'name': name, 'argv': ['taskset', '-c', '2,4', *argv],
                'environment_additions': {'PL_PEER_RUNTIME_CAPTURE_DIR': str(capture),
                                           'PL_PEER_RUNTIME_SOURCE_SHA': A.source},
                'source_before': before, 'source_after': verify(),
                'pre_command_cache_elfs': records, 'owners_before': owners,
                'owners_after_retention': owners_after, 'exit_code': code,
                'elapsed_seconds': round(time.time() - started, 3),
                'log_sha256': sha256_file(lane / 'command.log'), 'disk_monitor': monitor}
        receipt['commands'].append(item)
        if failure:
            item['command_interruption'] = failure
        # Preserve success and failure artifacts before any next clean or build.
        item['executed_elfs'] = executed_elfs(output, name)
        item['post_command_cache_elfs'] = retain_cache(name, 'all post-command success/failed ELF artifacts')
        item['source_after_retention'] = verify()
        (lane / 'command.exit').write_text(str(code) + '\n')
        receipt.setdefault('capture_files', {})[name] = {str(p.relative_to(capture)):
                                    {'sha256': sha256_file(p), 'bytes': p.stat().st_size,
                                     'full_permission_mode': p.stat().st_mode & 0o7777}
                                    for p in sorted(capture.rglob('*')) if p.is_file()}
        save()
        print(name, code, flush=True)
        if code:
            raise SystemExit(code)
except BaseException as error:
    receipt['runner_outcome'] = repr(error)
    save()
    raise
receipt['final_source'] = verify()
receipt['runner_outcome'] = 'focused diagnostic commands passed'
save()

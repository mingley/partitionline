#!/usr/bin/env python3
"""One declared regression on exact selective ea9 inputs; never full qualification."""
import gzip
import hashlib
import io
import json
import os
from pathlib import Path
import re
import signal
import subprocess
import tarfile
import time

BASE = Path('/workspace/work/broker-merged-source-ea9ff293')
REPO = Path('/workspace/partitionline')
PLAN = Path('/workspace/work/raft-runtime-76/coverage-proposal-01/failing-first-selective-plan-02.json')
OUT = Path('/workspace/work/raft-runtime-76/development/incoming-lifetime-ea9ff293-02')
TREE = OUT / 'source'
SOURCE = 'ea9ff29393526b57e1203612bb12c9c99deccc61'
DISK_FLOOR_BYTES = 350 * 1024 * 1024
DISK_POLL_SECONDS = 0.2
ENV = dict(os.environ, CARGO_HOME='/workspace/work/cargo', RUSTUP_HOME='/workspace/work/rustup',
           PATH='/workspace/work/cargo/bin:' + os.environ['PATH'], CARGO_INCREMENTAL='0',
           CARGO_BUILD_JOBS='1', CARGO_PROFILE_DEV_DEBUG='0', CARGO_PROFILE_TEST_DEBUG='0',
           CARGO_TARGET_DIR='/workspace/work/target-broker-segments', CARGO_NET_OFFLINE='true')
CACHE = Path(ENV['CARGO_TARGET_DIR'])
os.sched_setaffinity(0, {2, 4})
assert os.sched_getaffinity(0) == {2, 4}
OUT.mkdir(parents=True, exist_ok=False)
elf_objects = {}
elf_snapshots = {}
receipt = {'schema_version': 1, 'task': 'KL11-76', 'source_commit': SOURCE,
           'scope': 'selective66-input experiment with one declared test overlay; not complete-source qualification',
           'commands': [], 'affinity': [2, 4], 'source_tree': str(TREE),
           'driver_sha256': hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
           'environment': {key: ENV[key] for key in ('CARGO_HOME', 'RUSTUP_HOME', 'CARGO_TARGET_DIR',
               'CARGO_INCREMENTAL', 'CARGO_BUILD_JOBS', 'CARGO_PROFILE_DEV_DEBUG', 'CARGO_PROFILE_TEST_DEBUG',
               'CARGO_NET_OFFLINE')}}
HELPER = Path('/workspace/work/raft-runtime-76/diagnostic-runner-02/guard-functions.py')
PROVENANCE = Path('/workspace/work/raft-runtime-76/diagnostic-runner-02/guard-provenance.json')
provenance = json.loads(PROVENANCE.read_text())
assert hashlib.sha256(HELPER.read_bytes()).hexdigest() == provenance['extracted_sha256']
exec(compile(HELPER.read_text(), str(HELPER), 'exec'), globals())
receipt['guard_provenance'] = provenance
plan = json.loads(PLAN.read_text())
assert plan['basis_sha'] == SOURCE and plan['selected_files'] == 66
origin_path = Path(plan['basis_origin'])
assert sha256_file(origin_path) == plan['basis_origin_sha256']
origin = json.loads(origin_path.read_text())
assert origin['source_commit'] == SOURCE and Path(origin['source_directory']) == BASE
manifest = Path(origin['source_manifest']['path'])
assert sha256_file(manifest) == origin['source_manifest']['compressed_sha256']
raw = gzip.decompress(manifest.read_bytes())
assert hashlib.sha256(raw).hexdigest() == origin['source_manifest']['uncompressed_sha256']
baseline = json.loads(raw)
assert len(baseline) == 73171
del raw
git = {}
for row in subprocess.check_output(['git', 'ls-tree', '-r', '-z', SOURCE], cwd=REPO).split(b'\0'):
    if row:
        metadata, name = row.split(b'\t', 1)
        mode, kind, blob = metadata.decode().split()
        assert kind == 'blob'
        git[name.decode()] = {'mode': mode, 'blob': blob}
assert set(git) == set(baseline)
for name, pin in baseline.items():
    assert git[name] == {'mode': pin['mode'], 'blob': pin['git_blob_sha1']}
receipt['original_complete_source'] = {'origin': str(origin_path), 'origin_sha256': sha256_file(origin_path),
    'manifest': origin['source_manifest'], 'files': len(baseline), 'complete_git_path_set_verified': True}
receipt['selected_plan'] = {'path': str(PLAN), 'sha256': sha256_file(PLAN)}

def save():
    receipt['retained_elf_objects'] = list(elf_objects.values())
    (OUT / 'validation.json').write_text(json.dumps(receipt, indent=2) + '\n')

def free_bytes():
    stat = os.statvfs(OUT)
    return stat.f_bavail * stat.f_frsize

def guard_write():
    assert free_bytes() >= DISK_FLOOR_BYTES, ('350MiB write guard', free_bytes())

def work_inodes():
    names = subprocess.check_output(['git', 'ls-files', '--cached', '--others', '--exclude-standard', '-z'], cwd=REPO)
    result = set()
    for name in names.split(b'\0'):
        if name:
            path = REPO / os.fsdecode(name)
            if path.exists() or path.is_symlink():
                stat = path.lstat()
                result.add((stat.st_dev, stat.st_ino))
    return result

def verify_rows(tree, pins, disjoint=True):
    actual_paths = {str(p.relative_to(tree)) for p in tree.rglob('*') if p.is_file() or p.is_symlink()}
    assert actual_paths == set(pins), {'missing': sorted(set(pins) - actual_paths), 'extra': sorted(actual_paths - set(pins))}
    work = work_inodes() if disjoint else set()
    digest = hashlib.sha256()
    total = 0
    for name, pin in sorted(pins.items()):
        path = tree / name
        before = path.lstat()
        data = os.readlink(path).encode() if pin['mode'] == '120000' else path.read_bytes()
        blob = hashlib.sha1(b'blob ' + str(len(data)).encode() + b'\0' + data).hexdigest()
        mode = '120000' if path.is_symlink() else ('100755' if before.st_mode & 0o111 else '100644')
        fullmode = before.st_mode & 0o7777
        assert blob == pin['git_blob_sha1'] and hashlib.sha256(data).hexdigest() == pin['sha256'], name
        assert mode == pin['mode'] and fullmode == pin['full_permission_mode'] and len(data) == pin['bytes'], name
        after = path.lstat()
        assert elf_identity(before) == elf_identity(after), ('source changed during read', name)
        assert (before.st_dev, before.st_ino) not in work, ('source shares repository WORK inode', name)
        digest.update(json.dumps([name, blob, pin['sha256'], mode, fullmode, len(data)], separators=(',', ':')).encode())
        digest.update(b'\n')
        total += len(data)
    return {'files': len(pins), 'bytes': total, 'exact_path_blob_sha256_and_full07777_match': True,
            'repository_tracked_untracked_inodes_disjoint': disjoint, 'audit_sha256': digest.hexdigest()}

def guards():
    original = verify_rows(BASE, baseline)
    selected = verify_rows(TREE, selected_pins)
    return {'original_full73171': original, 'selective66': selected,
            'selected_scope': '65 exact baseline rows plus one declared overlay; not a complete Git tree'}

def cache_map():
    rows = {}
    for path in sorted(CACHE.rglob('*')):
        stat = path.lstat()
        name = str(path.relative_to(CACHE))
        assert not path.is_symlink(), ('special cache path refused before cleanup', path)
        if path.is_dir():
            rows[name] = {'type': 'directory', 'full_mode': stat.st_mode & 0o7777}
        else:
            assert path.is_file(), ('special cache path refused', path)
            rows[name] = {'type': 'file', 'bytes': stat.st_size, 'sha256': sha256_file(path),
                          'full_mode': stat.st_mode & 0o7777}
            assert elf_identity(stat) == elf_identity(path.stat()), ('cache changed during map', path)
    return rows

def combined_forecast():
    files = []; directories = []
    retained = reference_cache_map
    for path in sorted(CACHE.rglob('*')):
        assert not path.is_symlink()
        if path.is_file():
            name = str(path.relative_to(CACHE)); stat = path.stat()
            pin = {'type': 'file', 'bytes': stat.st_size, 'sha256': sha256_file(path), 'full_mode': stat.st_mode & 0o7777}
            if retained.get(name) != pin: files.append(stat.st_size)
        elif path.is_dir():
            name = str(path.relative_to(CACHE))
            if retained.get(name) != {'type':'directory','full_mode':path.stat().st_mode & 0o7777}: directories.append(path)
        else: raise AssertionError(('special cache path', path))
    tar_upper = sum(files) + sum(512 + (-size % 512) + 1024 for size in files) + 2048 * len(directories) + 10240
    archive_upper = tar_upper + ((tar_upper + 16382) // 16383) * 5 + 64
    elf = retention_forecast()
    reserves = {'complete_cache_archive': archive_upper, 'all_new_elf_objects': elf['new_gzip_upper_bound_bytes'],
                'additional_generated_growth': 64 * 1024 * 1024, 'raw_capture_and_facts': plan['raw_capture_bytes_upper_bound'],
                'source_and_audit_metadata': 12 * 1024 * 1024, 'free_floor': DISK_FLOOR_BYTES}
    required = sum(reserves.values())
    return {'sampled_free_bytes': free_bytes(), 'new_or_changed_cache_bytes_to_preserve': sum(files), 'new_or_changed_cache_files_to_preserve': len(files),
            'cache_directories': len(directories), 'reserves': reserves, 'required_free_bytes': required,
            'sufficient': free_bytes() >= required}

def preserve_whole_cache(label):
    assert not cache_owners(), ('active cache references', cache_owners())
    forecast = combined_forecast()
    receipt.setdefault('whole_cache_forecasts', []).append({'label': label, **forecast})
    save()
    assert forecast['sufficient'], ('complete cache archive reserve refusal; originals retained', forecast)
    current = cache_map()
    if label == 'pre-clean':
        assert current == reference_cache_map, 'prior archive does not exactly cover present cache'
        validate_reference_archive()
        assert cache_map() == current, 'cache changed during prior full archive verification'
        record = {'reference': reference_cache_receipt, 'reference_validation': prior_validation_pin, 'present_cache_paths':len(current), 'source_before_after_and_archive_pathset_sha256_bytes_full07777_verified':True, 'scope':'verified reused complete-cache archive, no omitted current generated outputs'}
        receipt.setdefault('whole_cache_retention',[]).append(record)
        save()
        return record
    before = {name:pin for name,pin in current.items() if reference_cache_map.get(name) != pin}
    target = OUT / (label + '-all-cache.tar.gz')
    guard_write()
    with target.open('wb') as raw_target, gzip.GzipFile(fileobj=raw_target, mode='wb', mtime=0, compresslevel=1) as zipped:
        with tarfile.open(fileobj=zipped, mode='w|', format=tarfile.PAX_FORMAT) as archive:
            for name, pin in before.items():
                guard_write()
                path = CACHE / name
                info = tarfile.TarInfo(name)
                info.mode = pin['full_mode']
                info.mtime = 0
                if pin['type'] == 'directory':
                    info.type = tarfile.DIRTYPE
                    archive.addfile(info)
                else:
                    info.size = pin['bytes']
                    with path.open('rb') as source:
                        class GuardedReader:
                            def read(self, count=-1):
                                guard_write()
                                return source.read(count)
                        archive.addfile(info, GuardedReader())
    decoded = {}
    with tarfile.open(target, 'r|gz') as archive:
        for member in archive:
            name = member.name.rstrip('/')
            assert name not in decoded
            if member.isdir(): decoded[name] = {'type': 'directory', 'full_mode': member.mode & 0o7777}
            else:
                assert member.isfile()
                state = hashlib.sha256(); size = 0
                stream = archive.extractfile(member)
                while chunk := stream.read(1024 * 1024): state.update(chunk); size += len(chunk)
                decoded[name] = {'type': 'file', 'bytes': size, 'sha256': state.hexdigest(), 'full_mode': member.mode & 0o7777}
    assert decoded == before, 'complete cache archive decode/pathset/bytes/fullmodes differs'
    assert cache_map() == current, 'original cache changed during delta retention'
    map_path = OUT / (label + '-cache-map.json')
    guard_write()
    map_path.write_text(json.dumps(current, indent=2) + '\n')
    record = {'archive': str(target), 'archive_sha256': sha256_file(target), 'archive_bytes': target.stat().st_size,
              'map': str(map_path), 'map_sha256': sha256_file(map_path), 'paths': len(before),
              'regular_files': sum(pin['type'] == 'file' for pin in before.values()),
              'source_before_after_and_archive_pathset_sha256_bytes_full07777_verified': True,
              'scope': 'complete current map reconstructible from verified prior complete archive plus all new/changed outputs in this delta archive', 'inherited_archive': reference_cache_receipt, 'delta_rows': before, 'deleted_prior_paths': sorted(set(reference_cache_map)-set(current))}
    receipt.setdefault('whole_cache_retention', []).append(record)
    save()
    return record

prior_validation_pin = plan['prior_actual_experiment']
prior_validation_path = Path(prior_validation_pin['validation'])
assert sha256_file(prior_validation_path) == prior_validation_pin['sha256']
prior = json.loads(prior_validation_path.read_text())
assert len(prior['pre_clean_elfs']) == 116
reference_cache_receipt = prior['whole_cache_retention'][-1]
reference_cache_map_path = Path(reference_cache_receipt['map'])
assert sha256_file(reference_cache_map_path) == reference_cache_receipt['map_sha256']
reference_cache_map = json.loads(reference_cache_map_path.read_text())
for obj in prior['retained_elf_objects']:
    obj = dict(obj)
    path = prior_validation_path.parent / obj['path']
    assert sha256_file(path) == obj['sha256']
    obj['path'] = str(path)
    elf_objects[obj['uncompressed_sha256']] = obj
for row in prior['commands'][-1]['post_command_elfs']:
    path = Path(row['original_path'])
    assert sha256_file(path) == row['sha256'] and path.stat().st_mode & 0o7777 == row['original_mode']
    elf_snapshots[str(path)] = {'identity':elf_identity(path.stat()),'sha256':row['sha256']}
receipt['inherited_preservation'] = prior_validation_pin

def validate_reference_archive():
    archive = Path(reference_cache_receipt['archive'])
    assert sha256_file(archive) == reference_cache_receipt['archive_sha256']
    decoded = {}
    with tarfile.open(archive, 'r|gz') as source:
        for member in source:
            name=member.name.rstrip('/')
            assert name not in decoded
            if member.isdir(): decoded[name]={'type':'directory','full_mode':member.mode & 0o7777}
            else:
                assert member.isfile()
                state=hashlib.sha256();count=0
                stream=source.extractfile(member)
                while chunk:=stream.read(1024*1024):state.update(chunk);count+=len(chunk)
                decoded[name]={'type':'file','bytes':count,'sha256':state.hexdigest(),'full_mode':member.mode & 0o7777}
    assert decoded == reference_cache_map, 'inherited complete cache archive does not reproduce map'

try:
    forecast = combined_forecast()
    receipt['initial_forecast'] = forecast
    save()
    assert forecast['sufficient'], ('serial reserve forecast refusal; no clean/build', forecast)
    receipt['original_before_materialization'] = verify_rows(BASE, baseline)
    assert not cache_owners(), ('active generated-cache owner', cache_owners())
    selected_pins = {name: dict(baseline[name]) for name in plan['selected_paths']}
    overlay = plan['overlay'][0]
    overlay_data = Path(overlay['source']).read_bytes()
    assert hashlib.sha256(overlay_data).hexdigest() == overlay['sha256'] and len(overlay_data) == overlay['bytes']
    assert Path(overlay['source']).stat().st_mode & 0o7777 == overlay['fullmode']
    pin = selected_pins[overlay['path']]
    pin.update(bytes=len(overlay_data), sha256=overlay['sha256'], full_permission_mode=overlay['fullmode'],
               git_blob_sha1=hashlib.sha1(b'blob ' + str(len(overlay_data)).encode() + b'\0' + overlay_data).hexdigest())
    TREE.mkdir()
    for name, pin in selected_pins.items():
        guard_write()
        path = TREE / name
        path.parent.mkdir(parents=True, exist_ok=True)
        if name == overlay['path']:
            path.write_bytes(overlay_data)
            path.chmod(pin['full_permission_mode'])
            assert path.stat().st_ino != (BASE / name).stat().st_ino
        else: os.link(BASE / name, path)
    (OUT / 'selected-source-manifest.json').write_text(json.dumps(selected_pins, indent=2) + '\n')
    receipt['selected_overlay'] = overlay
    receipt['selected_source_manifest_sha256'] = sha256_file(OUT / 'selected-source-manifest.json')
    receipt['materialized_guards'] = guards()
    receipt['toolchain'] = subprocess.check_output(['rustc', '+stable', '-Vv'], env=ENV, text=True)
    preserve_whole_cache('pre-clean')
    receipt['pre_clean_elfs'] = retain_cache('pre-clean', 'all existing ELF bytes and full modes before package clean', force_verify=True)
    receipt['original116_elfs_preserved_at_prior_proof'] = True
    assert len(receipt['pre_clean_elfs']) == len(prior['commands'][-1]['post_command_elfs'])
    save()
    capture = OUT / 'captures'
    capture.mkdir()
    ENV['PL_IMAGE_REGRESSION_CAPTURE_DIR'] = str(capture)
    for index, argv in enumerate(plan['commands']):
        name = 'baseline-one-regression'
        before = guards()
        owners = cache_owners()
        assert not owners
        forecast = retention_forecast()
        reserve = forecast['required_free_bytes'] + 64 * 1024 * 1024 + plan['raw_capture_bytes_upper_bound'] + 8 * 1024 * 1024
        assert free_bytes() >= reserve, ('pre-command future artifact reserve', free_bytes(), reserve)
        lane = OUT / name
        lane.mkdir()
        start = time.time()
        with (lane / 'command.log').open('w') as log:
            code, monitor = monitored_command(argv, TREE, ENV, log, lane / 'disk-monitor.jsonl')
        output = (lane / 'command.log').read_text()
        item = {'name': name, 'argv': argv, 'exit_code': code, 'elapsed_seconds': time.time() - start,
                'source_before': before, 'source_after': guards(), 'cache_owners_before': owners,
                'future_artifact_required_free_bytes': reserve, 'disk_monitor': monitor,
                'log_sha256': sha256_file(lane / 'command.log'),
                'environment_additions': {'PL_IMAGE_REGRESSION_CAPTURE_DIR': str(capture)}}
        receipt['commands'].append(item)
        (lane / 'command.exit').write_text(str(code) + '\n')
        item['executed_elfs'] = executed_elfs(output, name)
        item['post_command_elfs'] = retain_cache(name, 'all executed/failed ELF artifacts', force_verify=True)
        item['source_after_retention'] = guards()
        save()
        print(name, code, flush=True)
        receipt['captured_files'] = {str(path.relative_to(capture)): {'sha256': sha256_file(path),
            'bytes': path.stat().st_size, 'full_mode': path.stat().st_mode & 0o7777}
            for path in sorted(capture.rglob('*')) if path.is_file()}
        receipt['semantic_expected_failure'] = bool(code == 101 and 'running 1 test' in output
            and 'InvalidPeer' in output and 'test result: FAILED. 0 passed; 1 failed; 0 ignored;' in output
            and not monitor['triggered'])
        receipt['baseline_behavior'] = 'actual one regression fails InvalidPeer' if receipt['semantic_expected_failure'] else 'unexpected outcome; no semantic reproduction claimed'
        save()
        preserve_whole_cache('post-regression')
        assert receipt['semantic_expected_failure'], receipt['baseline_behavior']
    receipt['final_source_guards'] = guards()
    receipt['final_cache_owners'] = cache_owners()
    assert not receipt['final_cache_owners']
    receipt['runner_outcome'] = 'desired actual baseline behavioral failure reproduced; no candidate or qualification'
    save()
except BaseException as error:
    receipt['runner_outcome'] = repr(error)
    save()
    raise

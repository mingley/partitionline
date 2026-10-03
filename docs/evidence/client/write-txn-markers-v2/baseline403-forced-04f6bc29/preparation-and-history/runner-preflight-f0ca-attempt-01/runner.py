#!/usr/bin/env python3
"""Owned, monitored first stable capability qualification; no broad matrix."""
from pathlib import Path
import gzip
import hashlib
import json
import os
import resource
import signal
import stat
import subprocess
import sys
import time

BASE = Path('/workspace/work/client-capability-qa-preparation-e90efb49')
SOURCE = Path('/workspace/work/client-capabilities-source-f0ca05da')
TARGET = Path('/workspace/work/client-share-target')
RUN = BASE / 'stable-focused-f0ca05da-attempt-01'
FLOOR = 350 * 1024 * 1024
ORIGIN_PATH = Path('/workspace/work/integration/client-capabilities-source-f0ca05da/receipt.json')
ORIGIN_BYTES = ORIGIN_PATH.read_bytes()
assert hashlib.sha256(ORIGIN_BYTES).hexdigest() == 'f18fc74f39bb561a4c0ed0f667d0bee98fe329866139fcabd957c87121433857'
ORIGIN = json.loads(ORIGIN_BYTES)
assert ORIGIN['source_commit'] == 'f0ca05da9c04942b211e2f56ddc615b9860cdea2'
assert ORIGIN['source_directory'] == str(SOURCE)
SOURCE_MAP = ORIGIN['source_manifest']
COMPRESSED_MANIFEST = Path(SOURCE_MAP['path']).read_bytes()
assert hashlib.sha256(COMPRESSED_MANIFEST).hexdigest() == SOURCE_MAP['compressed_sha256']
RAW_MANIFEST = gzip.decompress(COMPRESSED_MANIFEST)
assert hashlib.sha256(RAW_MANIFEST).hexdigest() == SOURCE_MAP['uncompressed_sha256']
assert len(RAW_MANIFEST) == SOURCE_MAP['uncompressed_bytes']
MANIFEST = json.loads(RAW_MANIFEST)
assert len(MANIFEST) == ORIGIN['verified_files'] == 73779
BASE_PLAN_BYTES = (BASE/'qa-plan.json').read_bytes()
assert hashlib.sha256(BASE_PLAN_BYTES).hexdigest() == 'ed67566b33de3d5b3aeda12314ab08498bb2afb6ad9e42a6607a78800cbc1548'
PLAN = json.loads(BASE_PLAN_BYTES)
PLAN['source_sha'] = ORIGIN['source_commit']
PLAN['execution_sequence'][0]['argv'] = [arg.replace('/workspace/work/client-capabilities-source-e90efb49','/workspace/work/client-capabilities-source-f0ca05da') for arg in PLAN['execution_sequence'][0]['argv']]
INTERRUPTED = None
ACTIVE = None

def interrupted(signum, frame):
    global INTERRUPTED
    INTERRUPTED = signum

signal.signal(signal.SIGTERM, interrupted)
signal.signal(signal.SIGINT, interrupted)
RUN.mkdir(mode=0o700)
(RUN / 'retained-elfs').mkdir(mode=0o700)
ROWS = []
ALL_ELFS = []
RETAINED = {}

def digest(data):
    return hashlib.sha256(data).hexdigest()

def free_bytes():
    st = os.statvfs('/workspace')
    return st.f_bavail * st.f_frsize

def files(root):
    found = {}
    for directory, dirs, names in os.walk(root, followlinks=False):
        for name in list(dirs):
            p = Path(directory) / name
            if p.is_symlink():
                names.append(name)
                dirs.remove(name)
        for name in names:
            p = Path(directory) / name
            found[p.relative_to(root).as_posix()] = p.lstat()
    return found

def source_guard(root, manifest):
    actual = files(root)
    assert actual.keys() == manifest.keys(), ('source path set', root)
    hashed = hashlib.sha256()
    total = 0
    for name in sorted(manifest):
        expected = manifest[name]
        p = root / name
        st = actual[name]
        mode = stat.S_IMODE(st.st_mode)
        assert mode == expected['full_permission_mode'], (root, name, 'mode')
        data = os.fsencode(os.readlink(p)) if stat.S_ISLNK(st.st_mode) else p.read_bytes()
        assert len(data) == expected['bytes'], (root, name, 'length')
        assert digest(data) == expected['sha256'], (root, name, 'SHA-256')
        blob = hashlib.sha1(b'blob ' + str(len(data)).encode() + b'\0' + data).hexdigest()
        assert blob == expected['git_blob_sha1'], (root, name, 'Git blob')
        hashed.update(name.encode() + b'\0' + blob.encode() + b'\0' + str(mode).encode() + b'\0')
        total += len(data)
    return {'files': len(actual), 'bytes': total, 'whole_declared_path_set_match': True,
            'all_declared_blobs_sha256_lengths_and_full_modes_match': True,
            'set_blob_fullmode_sha256': hashed.hexdigest()}

def cache_allocated():
    seen = set()
    result = 0
    for st in files(TARGET).values():
        inode = (st.st_dev, st.st_ino)
        if inode not in seen:
            result += st.st_blocks * 512
            seen.add(inode)
    return result

def forecast():
    allocated = cache_allocated()
    needed = 784209242 + 32*1024*1024
    current = free_bytes()
    row = {'sampled_free_bytes':current,'current_generated_cache_allocated_bytes':allocated,
           'focused_incremental_forecast_bytes':784209242,'additional_C_compiler_allowance_bytes':32*1024*1024,
           'required_free_bytes':needed,'minimum_remaining_free_bytes':FLOOR,
           'headroom_bytes':current-needed,'fits':current>=needed,
           'reference_forecast_sha256':'597d923ca7ae70e45be89b13171d95a1dcd6459f16e2674534716fa7281677b0',
           'scope':'initial candidate two-target no-run only; no baseline/test/strict/MSRV/JVM/fullQA'}
    assert row['fits'],('focused launch forecast no longer fits',row)
    return row

def group_members(group):
    members = []
    for path in Path('/proc').iterdir():
        if not path.name.isdigit():continue
        try:
            value=(path/'stat').read_text(); fields=value[value.rfind(')')+2:].split()
            if int(fields[2])==group:
                members.append({'pid':int(path.name),'state':fields[0]})
        except (FileNotFoundError,ProcessLookupError):pass
    return members

def close_group(child, terminate):
    before = group_members(child.pid)
    signals = []
    if terminate or any(row['state']!='Z' for row in before):
        for requested,seconds in [(signal.SIGTERM,1.0),(signal.SIGKILL,1.0)]:
            try:os.killpg(child.pid,requested);signals.append(int(requested))
            except ProcessLookupError:pass
            deadline=time.monotonic()+seconds
            while time.monotonic()<deadline:
                live=[row for row in group_members(child.pid) if row['state']!='Z']
                if not live:break
                time.sleep(0.05)
            if not live:break
    child.wait(timeout=2)
    after=group_members(child.pid)
    return {'owned_process_group':child.pid,'before':before,'after':after,'signals_sent':signals,
            'no_live_group_members':not any(row['state']!='Z' for row in after),
            'zombie_exclusion':'A /proc stat Z process has no executing context or open files; retained separately, no cleanup of shared caches inferred.'}

def retain_cache():
    captured = []
    for name, st in sorted(files(TARGET).items()):
        p = TARGET / name
        if not stat.S_ISREG(st.st_mode):
            continue
        with p.open('rb') as f:
            if f.read(4) != b'\x7fELF':
                continue
        data = p.read_bytes()
        sha = digest(data)
        original_mode = stat.S_IMODE(st.st_mode)
        if sha not in RETAINED:
            # compressBound-shaped reserve: refuse before writing beyond the floor.
            needed = len(data) + (len(data)//16383 + 1)*5 + 64 + 1024*1024
            assert free_bytes() >= FLOOR + needed, ('retention forecast', name, needed)
            output = RUN / 'retained-elfs' / (sha + '.elf.gz')
            compressed = gzip.compress(data, compresslevel=1, mtime=0)
            output.write_bytes(compressed)
            output.chmod(0o600)
            assert gzip.decompress(output.read_bytes()) == data
            RETAINED[sha] = {'path': str(output), 'sha256': digest(compressed),
                             'compressed_bytes': len(compressed), 'uncompressed_sha256': sha,
                             'uncompressed_bytes': len(data), 'decompression_verified': True}
        info = {'cache_relative_path': name, 'original_full_mode': original_mode,
                'elf_magic_verified': True, **RETAINED[sha]}
        captured.append(info)
    ALL_ELFS.extend(captured)
    return captured

def persist():
    row = {'schema_version': 1, 'source_sha': PLAN['source_sha'], 'plan_sha256': digest((BASE/'qa-plan.json').read_bytes()),
           'runner_sha256': digest(Path(__file__).read_bytes()), 'passed': False,
           'commands': ROWS, 'retained_cache_elfs': ALL_ELFS,
           'scope': 'initial corrected f0ca stable two-target no-run only; no baseline, behavioral test, strictlint, MSRV, JVM, full matrix or actual broker claim',
           'source_origin_receipt_sha256':digest(ORIGIN_BYTES),'source_manifest_compressed_sha256':SOURCE_MAP['compressed_sha256'],
           'source_manifest_uncompressed_sha256':SOURCE_MAP['uncompressed_sha256'],'source_hidden_overlays':False}
    (RUN/'validation.json').write_text(json.dumps(row, indent=2)+'\n')
    (RUN/'validation.json').chmod(0o600)

def env():
    tool = '/workspace/work/rustup/toolchains/stable-x86_64-unknown-linux-gnu/bin'
    result = os.environ.copy()
    result.update({'CARGO_HOME': '/workspace/work/cargo', 'RUSTUP_HOME': '/workspace/work/rustup',
                   'CARGO_TARGET_DIR': str(TARGET), 'CARGO_BUILD_JOBS': '1', 'CARGO_INCREMENTAL': '0',
                   'CARGO_PROFILE_DEV_DEBUG': '0', 'CARGO_PROFILE_TEST_DEBUG': '0', 'CARGO_NET_OFFLINE': 'true',
                   'RUSTC': tool+'/rustc', 'RUSTDOC': tool+'/rustdoc', 'RUSTFMT': tool+'/rustfmt',
                   'PATH': tool+':/workspace/work/cargo/bin:'+os.environ['PATH']})
    for key in ['PARTITIONLINE_CAPABILITY_PEER_DIR','PARTITIONLINE_CAPABILITY_PEER_MODE','PARTITIONLINE_CAPABILITY_PEER_VERSION']:
        result.pop(key,None)
    return result

def run(name, argv, root, manifest, expected_exit=0, timeout=600):
    global ACTIVE
    if INTERRUPTED is not None:raise InterruptedError('controlled before-launch interruption')
    before = source_guard(SOURCE, MANIFEST)
    local_before = before if root == SOURCE else source_guard(root, manifest)
    cache_before = retain_cache()
    predicted = forecast()
    stdout = RUN/(name+'.stdout.log')
    stderr = RUN/(name+'.stderr.log')
    monitor = RUN/(name+'.disk-monitor.jsonl')
    started = time.monotonic()
    observations = []
    process = None
    trigger = None
    pre_launch_free=free_bytes()
    with stdout.open('wb') as out, stderr.open('wb') as err, monitor.open('w') as observations_file:
        def limits():
            resource.setrlimit(resource.RLIMIT_CORE, (0,0))
        process = subprocess.Popen(argv, cwd=root, env=env(), stdout=out, stderr=err,
                                   start_new_session=True, preexec_fn=limits)
        ACTIVE=process
        print('started '+name+' pid='+str(process.pid), flush=True)
        initial={'elapsed_seconds':0,'free_bytes':pre_launch_free,'process_group':process.pid,'pre_launch':True,'process_completed':False,'exit_code':None}
        observations.append(initial);observations_file.write(json.dumps(initial)+'\n');observations_file.flush()
        while True:
            code = process.poll()
            observation = {'elapsed_seconds': time.monotonic()-started, 'free_bytes': free_bytes(),
                           'process_group': process.pid, 'process_completed': code is not None,
                           'exit_code': code}
            observations.append(observation)
            observations_file.write(json.dumps(observation)+'\n')
            observations_file.flush()
            if INTERRUPTED is not None:
                trigger='controlled_signal_interruption'
            elif observation['free_bytes'] < FLOOR:
                trigger = 'sampled_disk_reserve'
            elif observation['elapsed_seconds'] > timeout:
                trigger = 'bounded_command_timeout'
            elif stdout.stat().st_size+stderr.stat().st_size > 16*1024*1024:
                trigger = 'bounded_command_output'
            if trigger or code is not None:
                cleanup=close_group(process,trigger is not None)
                ACTIVE=None
                break
            time.sleep(0.2)
    after = source_guard(SOURCE, MANIFEST)
    local_after = after if root == SOURCE else source_guard(root, manifest)
    cache_after = retain_cache()
    actual_code = process.returncode
    row = {'name': name, 'argv': argv, 'cwd': str(root), 'exit_code': actual_code,
           'expected_exit_code': expected_exit, 'trigger': trigger,
           'cpu_affinity': '0,1', 'explicit_forced_environment': {k:env()[k] for k in PLAN['environment']},
           'source_before': before, 'source_after': after,
           'declared_operation_source_before': local_before, 'declared_operation_source_after': local_after,
           'forecast_before': predicted, 'elapsed_seconds': time.monotonic()-started,
           'stdout_path': str(stdout), 'stdout_sha256': digest(stdout.read_bytes()),
           'stderr_path': str(stderr), 'stderr_sha256': digest(stderr.read_bytes()),
           'disk_monitor_path': str(monitor), 'disk_monitor_sha256': digest(monitor.read_bytes()),
           'disk_samples': len(observations), 'minimum_sampled_free_bytes': min(x['free_bytes'] for x in observations),
           'cache_elfs_before': cache_before, 'cache_elfs_after': cache_after,'owned_group_cleanup':cleanup,
           'passed_expected_process_outcome': actual_code == expected_exit and trigger is None and cleanup['no_live_group_members']}
    ROWS.append(row)
    persist()
    print('closed '+name+' exit='+str(actual_code)+' minfree='+str(row['minimum_sampled_free_bytes']), flush=True)
    assert row['passed_expected_process_outcome'], ('candidate command failed or regression outcome differs', name, actual_code, trigger)
    return row

try:
    initial_forecast=forecast()
    (RUN/'launch-contract.json').write_text(json.dumps({'schema_version':1,'source_sha':PLAN['source_sha'],'origin_receipt_sha256':digest(ORIGIN_BYTES),'source_manifest_compressed_sha256':SOURCE_MAP['compressed_sha256'],'root_approved_forecast':initial_forecast,'argv':PLAN['execution_sequence'][0]['argv'],'product_source_identities':{name:MANIFEST[name] for name in ['src/admin.rs','src/protocol/admin.rs','src/protocol/txn.rs','src/lib.rs','src/producer.rs','src/partitioner.rs','src/protocol/produce.rs']},'root_authorized_scope':'candidate2target no-run only'},indent=2)+'\n')
    # The parent first GO currently authorizes exactly this no-run command.
    # Further phases are launched separately after this receipt is inspected.
    run('stable-candidate-no-run', PLAN['execution_sequence'][0]['argv'], SOURCE, MANIFEST)
except BaseException as failure:
    if ACTIVE is not None:
        emergency=close_group(ACTIVE,True)
        (RUN/'emergency-owned-group-cleanup.json').write_text(json.dumps(emergency,indent=2)+'\n')
    persist()
    (RUN/'failure.json').write_text(json.dumps({'type': type(failure).__name__, 'detail': str(failure),
        'source_sha': PLAN['source_sha'], 'passed': False, 'no_following_command_launched': True}, indent=2)+'\n')
    raise
print('candidate f0ca no-run closedPASS; baseline/tests/strict remain held for updatedforecastROOTGO',flush=True)

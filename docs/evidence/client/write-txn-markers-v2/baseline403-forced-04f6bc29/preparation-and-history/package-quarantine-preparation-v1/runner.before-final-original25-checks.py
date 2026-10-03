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
import argparse
import re

BASE = Path('/workspace/work/client-capability-qa-preparation-e90efb49')
ARG_PARSER=argparse.ArgumentParser()
ARG_PARSER.add_argument('--source',required=True)
ARG_PARSER.add_argument('--source-sha',required=True)
ARG_PARSER.add_argument('--origin-receipt',required=True)
ARG_PARSER.add_argument('--origin-receipt-sha256',required=True)
ARG_PARSER.add_argument('--run-label',required=True)
ARGS=ARG_PARSER.parse_args()
assert re.fullmatch('[0-9a-f]{40}',ARGS.source_sha)
assert re.fullmatch('[0-9a-f]{64}',ARGS.origin_receipt_sha256)
assert re.fullmatch('[A-Za-z0-9][A-Za-z0-9_-]{0,80}',ARGS.run_label)
SOURCE=Path(ARGS.source).resolve()
TARGET=Path('/workspace/work/client-share-target')
RUN=BASE/('baseline403-forced-'+ARGS.run_label)
BASELINE=BASE/('baseline403-forced-source-'+ARGS.run_label)
QUARANTINE=BASE/('baseline403-quarantine-'+ARGS.run_label)
PACKAGE_MAP_PATH=BASE/'package-invalidation-proposal-after-baseline-setup/proposal.json'
PACKAGE_MAP_BYTES=PACKAGE_MAP_PATH.read_bytes()
assert hashlib.sha256(PACKAGE_MAP_BYTES).hexdigest()=='791e813056e1e8aafb0f2a8af17b217d86864369b06192780c7176ab9adac528'
PACKAGE_MAP=json.loads(PACKAGE_MAP_BYTES)
INITIAL_PACKAGE_IDENTITIES={row['target_relative_path']:row for row in PACKAGE_MAP['package_output_restore_map']}
INVALIDATION_DONE=False
LAST_FORECAST_KIND='compile'
BASELINE_LIBRARY_PROOF=None
PLATFORM_ALLOW_PATH=BASE/'platform-daemon-exception-preparation/exact-platform-daemons.json'
PLATFORM_ALLOW_BYTES=PLATFORM_ALLOW_PATH.read_bytes()
assert hashlib.sha256(PLATFORM_ALLOW_BYTES).hexdigest()=='5b225336055754c792fecb6f2ce24364a6330fb93f728011ebb78ab53bd51ebd'
PLATFORM_ALLOW=json.loads(PLATFORM_ALLOW_BYTES)
ROOT_PUBLICATION_RESERVE=32*1024*1024
FULL_CACHE_REFERENCE_PATH=BASE/'platform-daemon-exception-preparation-v2/current-complete-cache-hash-fullmode-map.json'
FULL_CACHE_REFERENCE_BYTES=FULL_CACHE_REFERENCE_PATH.read_bytes()
assert hashlib.sha256(FULL_CACHE_REFERENCE_BYTES).hexdigest()=='396be0836255e97c50becf37172d2a3402f25a868f0c27aaa8e86811062e286b'
FULL_CACHE_REFERENCE=json.loads(FULL_CACHE_REFERENCE_BYTES)
assert len(FULL_CACHE_REFERENCE)==608 and set(FULL_CACHE_REFERENCE)==set(PACKAGE_MAP['complete_cache_file_inventory'])
CACHE_TAG_SOURCE=BASE/'cache-tag-protection-preparation/standard-CACHEDIR.TAG.proposed'
CACHE_TAG_BYTES=CACHE_TAG_SOURCE.read_bytes()
assert hashlib.sha256(CACHE_TAG_BYTES).hexdigest()=='6d9d1d216e0f83abc5e5662ca62c92b4f23009466b54fa27321a69acdb778bb2'
assert len(CACHE_TAG_BYTES)==177 and CACHE_TAG_BYTES.startswith(b'Signature: 8a477f597d28d172789f06886806bc55\n')
MARKER_RECEIPT_PATH=BASE/'baseline403-forced-04f6bc29-marker-v5-attempt-01/standard-cache-marker-repair.json'
MARKER_RECEIPT_BYTES=MARKER_RECEIPT_PATH.read_bytes()
assert hashlib.sha256(MARKER_RECEIPT_BYTES).hexdigest()=='caeb86a5f72cec3de50e68d39b59218c678c5280c2775e0697837ad880987514'
EXISTING_MARKER_IDENTITY=json.loads(MARKER_RECEIPT_BYTES)['new609_complete_cache_identity_map']['CACHEDIR.TAG']
BASELINE_INPUT_PATH = BASE/'baseline-compiler-inputs.json'
BASELINE_INPUT_BYTES = BASELINE_INPUT_PATH.read_bytes()
assert hashlib.sha256(BASELINE_INPUT_BYTES).hexdigest() == '9ab0fc64bdbfb64e10d06b3fbe657f8726788c4e9df2604f0394b937ec592592'
BASELINE_INPUTS = json.loads(BASELINE_INPUT_BYTES)
assert BASELINE_INPUTS['source_sha'] == '403be1e3db073df86921d6fb21189f695c4f1eaf'
assert len(BASELINE_INPUTS['files']) == 66
OVERLAY_BYTES = (BASE/'baseline-peer-correction-after-first-controls/baseline-overlay.proposed.json').read_bytes()
assert hashlib.sha256(OVERLAY_BYTES).hexdigest() == '997749b60d857ed3bc0ea15a1844b17c6e8ca097f892510eb38f61e5ac5c136e'
OVERLAY = json.loads(OVERLAY_BYTES)
# No native allowance or RPC overlap is assumed. ROOT must wait for RPC close,
# materialize the reviewed new source, then approve a fresh measured launch.
LIBRARY_ALLOWANCE=83002787
TARGET_ALLOWANCE=17333508
FORECAST_METADATA=16*1024*1024
SOURCE_METADATA=4*1024*1024
BASELINE_MANIFEST = None
WORKFLOW_PASSED = False
FLOOR = 350 * 1024 * 1024
ORIGIN_PATH = Path(ARGS.origin_receipt)
ORIGIN_BYTES = ORIGIN_PATH.read_bytes()
assert hashlib.sha256(ORIGIN_BYTES).hexdigest() == ARGS.origin_receipt_sha256
ORIGIN = json.loads(ORIGIN_BYTES)
assert ORIGIN['source_commit'] == ARGS.source_sha
assert ORIGIN['source_directory'] == str(SOURCE)
SOURCE_MAP = ORIGIN['source_manifest']
COMPRESSED_MANIFEST = Path(SOURCE_MAP['path']).read_bytes()
assert hashlib.sha256(COMPRESSED_MANIFEST).hexdigest() == SOURCE_MAP['compressed_sha256']
RAW_MANIFEST = gzip.decompress(COMPRESSED_MANIFEST)
assert hashlib.sha256(RAW_MANIFEST).hexdigest() == SOURCE_MAP['uncompressed_sha256']
assert len(RAW_MANIFEST) == SOURCE_MAP['uncompressed_bytes']
MANIFEST = json.loads(RAW_MANIFEST)
assert len(MANIFEST) == ORIGIN['verified_files']
assert MANIFEST['tests/fixtures/write-txn-markers-v2/socket_peer.rs']['sha256']=='104e6490511fe8acc5a75d96b3ca08aa810986dbdaf66b3c15567259d382d5ad'
OLD_ORIGIN_PATH=Path('/workspace/work/integration/broker-merged-source-403be1e3/receipt.json')
OLD_ORIGIN_BYTES=OLD_ORIGIN_PATH.read_bytes()
assert hashlib.sha256(OLD_ORIGIN_BYTES).hexdigest()=='d6e9a3578b31fe76ba8e764303a83cc615437744498e4874b37ef0c8af2d2fff'
OLD_ORIGIN=json.loads(OLD_ORIGIN_BYTES)
OLD_SOURCE=Path(OLD_ORIGIN['source_directory'])
OLD_MAP=OLD_ORIGIN['source_manifest']
OLD_COMPRESSED=Path(OLD_MAP['path']).read_bytes()
assert hashlib.sha256(OLD_COMPRESSED).hexdigest()==OLD_MAP['compressed_sha256']
OLD_RAW=gzip.decompress(OLD_COMPRESSED)
assert hashlib.sha256(OLD_RAW).hexdigest()==OLD_MAP['uncompressed_sha256'] and len(OLD_RAW)==OLD_MAP['uncompressed_bytes']
OLD_MANIFEST=json.loads(OLD_RAW)
assert len(OLD_MANIFEST)==71885
BASE_PLAN_BYTES = (BASE/'qa-plan.json').read_bytes()
assert hashlib.sha256(BASE_PLAN_BYTES).hexdigest() == 'ed67566b33de3d5b3aeda12314ab08498bb2afb6ad9e42a6607a78800cbc1548'
PLAN = json.loads(BASE_PLAN_BYTES)
PLAN['source_sha'] = ORIGIN['source_commit']
PLAN['execution_sequence'][0]['argv'] = [arg.replace('/workspace/work/client-capabilities-source-e90efb49',str(SOURCE)) for arg in PLAN['execution_sequence'][0]['argv']]
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
    index=sum(row['name'].startswith('stable-baseline-') for row in ROWS)
    remaining=max(0,3-index)
    library=LIBRARY_ALLOWANCE if index==0 else 0
    captures=library+remaining*TARGET_ALLOWANCE+8*1024*1024
    needed=library+remaining*TARGET_ALLOWANCE+LIBRARY_ALLOWANCE+captures+FORECAST_METADATA+SOURCE_METADATA+FLOOR
    current=free_bytes()
    if LAST_FORECAST_KIND=='quarantine':
        # Moving raw originals retains every allocated byte. Only a fresh
        # post-quarantine sample admits compilation; projected reclaim is zero.
        required=FLOOR+8*1024*1024
    else:required=needed+ROOT_PUBLICATION_RESERVE
    row={'sampled_free_bytes':current,'current_cache_allocated_bytes':cache_allocated(),
         'kind':LAST_FORECAST_KIND,'named_baseline_targets_remaining':remaining,
         'required_free_bytes':required,'postquarantine_compile_required_free_bytes':needed+ROOT_PUBLICATION_RESERVE,
         'postquarantine_compile_base_without_publication_reserve_bytes':needed,
         'native_allowance_bytes':0,'RPC_overlap_assumed':False,'RPC_closed_gate_required':True,
         'reference_refined_forecast_sha256':'726a66f74dc147314f5461c8243b74a8560181c3280baa5ea8b9feb36f29a4bf',
         'expected_quarantine_reclaim_bytes':0,
         'already_retained_package_objects_not_added_again_bytes':27156162,
         'root_publication_reserve_bytes':ROOT_PUBLICATION_RESERVE,
         'shared_floor_bytes':FLOOR,'fits':current>=required,'headroom_bytes':current-required}
    assert row['fits'],('forced baseline forecast no longer fits',row)
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
           'runner_sha256': digest(Path(__file__).read_bytes()), 'passed': WORKFLOW_PASSED,
           'commands': ROWS, 'retained_cache_elfs': ALL_ELFS,
           'scope': 'selective403 fail-first baseline only, not old full-tree qualification or candidate behavior/strict/MSRV/JVM/full/broker qualification',
           'baseline_source_sha':BASELINE_INPUTS['source_sha'],'baseline_input_manifest_sha256':digest(BASELINE_INPUT_BYTES),
           'five_overlay_contract_sha256':digest(OVERLAY_BYTES),'baseline_manifest':BASELINE_MANIFEST,
           'original403_origin_sha256':digest(OLD_ORIGIN_BYTES),'original403_manifest_compressed_sha256':OLD_MAP['compressed_sha256'],
           'own_package_restore_map_sha256':digest(PACKAGE_MAP_BYTES),'full608_frozen_cache_reference_sha256':digest(FULL_CACHE_REFERENCE_BYTES),'baseline_library_proof':BASELINE_LIBRARY_PROOF,
           'RPC_overlap_assumed':False,'native_allowance_bytes':0,
           'remaining_phases_held':True,
           'root_approved_platform_exception_sha256':digest(PLATFORM_ALLOW_BYTES),
           'platform_daemon_uninspected_fields':['cwd','exe','environ','maps','fd'],
           'universal_all_pid_readability_or_no_owner_claim':False,
           'actual_zero_running_Docker_before_after_quarantine_required':True,
           'already_present_standard_cache_marker_verified':True,
           'actual_exact25_atomicrename_quarantine_required':True,
           'actual_Cargo_clean_or_dryrun_commands':0,
           'expected_actual_command_count':6,
           'quarantine_directory':str(QUARANTINE),
           'standard_cache_marker_sha256':digest(CACHE_TAG_BYTES),
           'original_cache_inventory608_plus_previous_177B_marker609_disclosed':True,
           'previous_marker_repair_receipt_sha256':digest(MARKER_RECEIPT_BYTES),
           'root_publication_reserve_bytes':ROOT_PUBLICATION_RESERVE,
           'source_origin_receipt_sha256':digest(ORIGIN_BYTES),'source_manifest_compressed_sha256':SOURCE_MAP['compressed_sha256'],
           'source_manifest_uncompressed_sha256':SOURCE_MAP['uncompressed_sha256'],'candidate_hidden_overlays':False,'baseline_explicit_overlay_count':5}
    (RUN/'validation.json').write_text(json.dumps(row, indent=2)+'\n')
    (RUN/'validation.json').chmod(0o600)

def env():
    tool = '/workspace/work/rustup/toolchains/stable-x86_64-unknown-linux-gnu/bin'
    result = os.environ.copy()
    result.update({'CARGO_HOME': '/workspace/work/cargo', 'RUSTUP_HOME': '/workspace/work/rustup',
                   'CARGO_TARGET_DIR': str(TARGET), 'CARGO_BUILD_JOBS': '1', 'CARGO_INCREMENTAL': '0',
                   'CARGO_PROFILE_DEV_DEBUG': '0', 'CARGO_PROFILE_TEST_DEBUG': '0', 'CARGO_NET_OFFLINE': 'true', 'CARGO_TERM_COLOR':'never',
                   'RUSTC': tool+'/rustc', 'RUSTDOC': tool+'/rustdoc', 'RUSTFMT': tool+'/rustfmt',
                   'PATH': tool+':/workspace/work/cargo/bin:'+os.environ['PATH']})
    for key in ['DOCKER_CONTEXT','DOCKER_HOST','DOCKER_TLS_VERIFY','DOCKER_CERT_PATH','PARTITIONLINE_CAPABILITY_PEER_DIR','PARTITIONLINE_CAPABILITY_PEER_MODE','PARTITIONLINE_CAPABILITY_PEER_VERSION']:
        result.pop(key,None)
    return result

def run(name, argv, root, manifest, expected_exit=0, timeout=600):
    global ACTIVE
    if INTERRUPTED is not None:raise InterruptedError('controlled before-launch interruption')
    before = source_guard(SOURCE, MANIFEST)
    original403_before=source_guard(OLD_SOURCE,OLD_MANIFEST)
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
    original403_after=source_guard(OLD_SOURCE,OLD_MANIFEST)
    assert original403_before==original403_after
    local_after = after if root == SOURCE else source_guard(root, manifest)
    cache_after = retain_cache()
    actual_code = process.returncode
    row = {'name': name, 'argv': argv, 'cwd': str(root), 'exit_code': actual_code,
           'expected_exit_code': expected_exit, 'trigger': trigger,
           'cpu_affinity': '0,1', 'explicit_forced_environment': {k:env()[k] for k in PLAN['environment']},
           'source_before': before, 'source_after': after,
           'full_original403_before':original403_before,'full_original403_after':original403_after,
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
    assert row['passed_expected_process_outcome'], ('baseline command failed or expected regression outcome differs', name, actual_code, trigger)
    return row

def selected_origin_guard():
    total=0
    for row in BASELINE_INPUTS['files']:
        p=Path(row['path']); st=p.lstat()
        assert stat.S_ISREG(st.st_mode), ('selected origin not regular',p)
        data=p.read_bytes()
        assert len(data)==row['bytes'] and digest(data)==row['sha256']
        assert stat.S_IMODE(st.st_mode)==row['full_mode']
        assert hashlib.sha1(b'blob '+str(len(data)).encode()+b'\0'+data).hexdigest()==row['git_blob_sha1']
        total+=len(data)
    return {'scope':'exact66 selected403 compiler inputs only','files':66,'bytes':total,
            'all_selected_blobs_sha256_lengths_fullmodes_match':True}

def materialize_baseline():
    global BASELINE_MANIFEST
    original_before=selected_origin_guard()
    before=source_guard(SOURCE,MANIFEST)
    assert not BASELINE.exists(), 'never overwrite a prior baseline attempt/source'
    BASELINE.mkdir(mode=0o700)
    manifest={}
    for row in BASELINE_INPUTS['files']:
        source=Path(row['path']); dest=BASELINE/row['relative_path']
        dest.parent.mkdir(parents=True,exist_ok=True)
        os.link(source,dest)
        assert source.stat().st_ino==dest.stat().st_ino and source.stat().st_dev==dest.stat().st_dev
        manifest[row['relative_path']]={'bytes':row['bytes'],'sha256':row['sha256'],
            'full_permission_mode':row['full_mode'],'git_blob_sha1':row['git_blob_sha1'],
            'provenance':'exact403 selected input immutable hardlink'}
    assert len(OVERLAY['overlays'])==5
    for row in OVERLAY['overlays']:
        source=(SOURCE/'tests/fixtures/write-txn-markers-v2/socket_peer.rs') if row['destination']=='tests/fixtures/write-txn-markers-v2/socket_peer.rs' else Path(row['path']); st=source.lstat(); data=source.read_bytes()
        assert len(data)==row['bytes'] and digest(data)==row['sha256'] and stat.S_IMODE(st.st_mode)==row['full_mode']
        dest=BASELINE/row['destination']; assert not dest.exists()
        dest.parent.mkdir(parents=True,exist_ok=True);dest.write_bytes(data);dest.chmod(row['full_mode'])
        manifest[row['destination']]={'bytes':len(data),'sha256':digest(data),
            'full_permission_mode':row['full_mode'],'git_blob_sha1':hashlib.sha1(b'blob '+str(len(data)).encode()+b'\0'+data).hexdigest(),
            'provenance':'exact separately declared test overlay'}
    assert len(manifest)==71
    BASELINE_MANIFEST=manifest
    local=source_guard(BASELINE,manifest)
    original_after=selected_origin_guard();after=source_guard(SOURCE,MANIFEST)
    assert original_before==original_after and before==after
    receipt={'schema_version':1,'candidate_source_sha':PLAN['source_sha'],'candidate_before':before,'candidate_after':after,
        'baseline_input_manifest_sha256':digest(BASELINE_INPUT_BYTES),'overlay_contract_sha256':digest(OVERLAY_BYTES),
        'selected_origin_before':original_before,'selected_origin_after':original_after,
        'materialized_source_path':str(BASELINE),'materialized_manifest':manifest,'materialized_guard':local,
        'candidate_scope_full_immutable':True,'baseline_scope_selected_inputs_plus_declared_overlay':True,
        'no_mutation_of_hardlinked_inputs':True}
    output=RUN/'baseline-materialization.json';output.write_text(json.dumps(receipt,indent=2)+'\n');output.chmod(0o600)
    return receipt

def test_result(row, passed, failed, expected_names):
    stdout=Path(row['stdout_path']).read_text();stderr=Path(row['stderr_path']).read_text()
    import re
    summaries=re.findall(r'^test result: (ok|FAILED)\. (\d+) passed; (\d+) failed;',stdout,re.M)
    assert len(summaries)==1, ('one actual test result required, compile/setup failures are not behavioral proof', row['name'])
    status, actual_passed, actual_failed=summaries[0]
    assert (int(actual_passed),int(actual_failed))==(passed,failed), ('actual test counts',row['name'],summaries)
    assert status==('FAILED' if failed else 'ok')
    actual_tests=re.findall(r'^test ([A-Za-z_][A-Za-z_0-9]*) \.\.\. (ok|FAILED)$',stdout,re.M)
    assert {name for name,outcome in actual_tests}==set(expected_names), ('unexpected test names',actual_tests)
    assert all(outcome==('FAILED' if failed else 'ok') for name,outcome in actual_tests)
    assert 'could not compile' not in stderr and 'error[E' not in stderr
    result={'actual_test_summary':{'passed':passed,'failed':failed},'actual_test_names':actual_tests,
            'compiler_or_setup_failure_counted_as_behavior':False,'required_actual_behavior_observed':True}
    row.update(result);persist();return result

def seed_retention():
    # Existing lossless payloads are verified/referenced, never counted as future
    # writes or recopied simply because a new receipt is being prepared.
    for row in PACKAGE_MAP['package_output_restore_map']:
        compressed=Path(row['gzip_path']).read_bytes()
        assert digest(compressed)==row['gzip_sha256']
        data=gzip.decompress(compressed)
        assert digest(data)==row['sha256'] and len(data)==row['bytes']
        RETAINED[row['sha256']]={'path':row['gzip_path'],'sha256':row['gzip_sha256'],
            'compressed_bytes':row['gzip_bytes'],'uncompressed_sha256':row['sha256'],
            'uncompressed_bytes':row['bytes'],'decompression_verified':True,
            'origin_restore_map_sha256':digest(PACKAGE_MAP_BYTES)}
    prior=BASE/'stable-baseline403-after-7942-attempt-01/validation.json'
    raw=prior.read_bytes()
    assert digest(raw)=='d185913b2a1b89f4d8025d2ebd24625863332c8f9304f9092f5f02228776761e'
    for row in json.loads(raw)['commands'][0]['cache_elfs_after']:
        compressed=Path(row['path']).read_bytes()
        assert digest(compressed)==row['sha256']
        data=gzip.decompress(compressed)
        assert digest(data)==row['uncompressed_sha256'] and len(data)==row['uncompressed_bytes']
        RETAINED.setdefault(row['uncompressed_sha256'],{key:row[key] for key in ['path','sha256','compressed_bytes','uncompressed_sha256','uncompressed_bytes','decompression_verified']})

def verify_platform_daemons():
    verified=[]
    for expected in PLATFORM_ALLOW['platform_daemons']:
        proc=Path('/proc')/str(expected['pid'])
        status=(proc/'status').read_text()
        fields={line.split(':',1)[0]:line.split(':',1)[1].strip() for line in status.splitlines() if ':' in line}
        raw=(proc/'stat').read_text();stat_fields=raw[raw.rfind(')')+2:].split()
        comm=raw[raw.find('(')+1:raw.rfind(')')]
        cmd=(proc/'cmdline').read_bytes()
        actual={'pid':expected['pid'],'comm':comm,'parent_pid':int(fields['PPid']),
                'uid_all_four':[int(x) for x in fields['Uid'].split()],
                'starttime_ticks':int(stat_fields[19]),'cmdline_sha256':digest(cmd),'cmdline_bytes':len(cmd)}
        assert fields['Name']==expected['comm'] and actual['comm']==expected['comm']
        assert actual['uid_all_four']==[0,0,0,0]
        assert all(actual[key]==expected[key] for key in actual), 'platform daemon identity changed/unverifiable'
        actual['explicitly_uninspected_fields']=['cwd','exe','environ','maps','fd']
        verified.append(actual)
    return verified

def verify_docker_cli():
    expected=PLATFORM_ALLOW['docker_cli'];p=Path(expected['path']);st=p.lstat()
    assert stat.S_ISREG(st.st_mode), 'pinned Docker CLI must remain a regular file, not a symlink'
    data=p.read_bytes()
    assert digest(data)==expected['sha256'] and len(data)==expected['bytes'] and stat.S_IMODE(st.st_mode)==expected['full_mode']
    return expected

def docker_zero_running_workloads(name):
    verified_before=verify_platform_daemons();verify_docker_cli()
    # Never infer zero workload from process names or inaccessible daemon fields.
    row=run(name,['taskset','-c','0,1',*PLATFORM_ALLOW['docker_ps_argv']],SOURCE,MANIFEST,timeout=10)
    assert Path(row['stdout_path']).read_bytes().strip()==b'', 'Docker API reports running workload(s)'
    verified_after=verify_platform_daemons();assert verified_before==verified_after
    row.update({'actual_docker_api_zero_running_workloads':True,'actual_platform_daemons_before':verified_before,
                'actual_platform_daemons_after':verified_after,'daemon_fields_uninspected':True,
                'scope':'relevant Docker workloads/readable processes, not universal all-PID field readability'})
    persist();return row

def process_reference_guard():
    # Only safe public target-match booleans/errors are recorded. Never dump
    # commandlines, unrelated env values, credentials or complete maps.
    refs=[];faults=[];checked=0;zombies=0;closed_descriptors=0;vanished_or_nonexecuting_after_scan=0
    def disappeared_or_nonexecuting(proc):
        # ENOENT on a field alone does not establish process exit. A live
        # process with an unknown field must fail closed; genuine exit/Z is
        # excluded only after a fresh stat observation.
        try:
            value=(proc/'stat').read_text();parts=value[value.rfind(')')+2:].split()
        except (FileNotFoundError,ProcessLookupError):return True
        except OSError as error:
            faults.append({'pid':int(proc.name),'field':'stat-recheck','errno':error.errno});return False
        return parts[0]=='Z'
    platform_before=verify_platform_daemons()
    exempt={x['pid'] for x in platform_before}
    exempt_seen=[]
    target=os.fsencode(str(TARGET))
    for proc in sorted(Path('/proc').iterdir(),key=lambda p:p.name):
        if not proc.name.isdigit():continue
        pid=int(proc.name)
        try:
            raw=(proc/'stat').read_text(); fields=raw[raw.rfind(')')+2:].split()
        except (FileNotFoundError,ProcessLookupError):continue
        except OSError as error:faults.append({'pid':pid,'field':'stat','errno':error.errno});continue
        if fields[0]=='Z':zombies+=1;continue
        if pid==os.getpid():continue
        if pid in exempt:
            exempt_seen.append(pid)
            continue
        checked+=1
        identity_before=(raw[raw.find('(')+1:raw.rfind(')')],int(fields[19]))
        for field in ['cwd','exe']:
            try:value=os.fsencode(os.readlink(proc/field))
            except (FileNotFoundError,ProcessLookupError) as error:
                if not disappeared_or_nonexecuting(proc):faults.append({'pid':pid,'field':field,'errno':error.errno,'same_process_not_observed_exited':True})
                continue
            except OSError as error:faults.append({'pid':pid,'field':field,'errno':error.errno});continue
            if target in value:refs.append({'pid':pid,'field':field,'target_match':True})
        for field in ['environ','maps']:
            try:value=(proc/field).read_bytes()
            except (FileNotFoundError,ProcessLookupError) as error:
                if not disappeared_or_nonexecuting(proc):faults.append({'pid':pid,'field':field,'errno':error.errno,'same_process_not_observed_exited':True})
                continue
            except OSError as error:faults.append({'pid':pid,'field':field,'errno':error.errno});continue
            if field=='environ':value=b'\0'.join(x for x in value.split(b'\0') if x.startswith(b'CARGO_TARGET_DIR='))
            if target in value:refs.append({'pid':pid,'field':field,'target_match':True})
        try:descriptors=list((proc/'fd').iterdir())
        except (FileNotFoundError,ProcessLookupError) as error:
            if not disappeared_or_nonexecuting(proc):faults.append({'pid':pid,'field':'fd','errno':error.errno,'same_process_not_observed_exited':True})
            continue
        except OSError as error:faults.append({'pid':pid,'field':'fd','errno':error.errno});continue
        assert len(descriptors)<=16384, 'bounded descriptor inventory'
        for descriptor in descriptors:
            try:value=os.fsencode(os.readlink(descriptor))
            except (FileNotFoundError,ProcessLookupError):
                closed_descriptors+=1;continue
            except OSError as error:faults.append({'pid':pid,'field':'fd-link','errno':error.errno});continue
            if target in value:refs.append({'pid':pid,'field':'fd','target_match':True})
        try:
            value=(proc/'stat').read_text();parts=value[value.rfind(')')+2:].split()
        except (FileNotFoundError,ProcessLookupError):vanished_or_nonexecuting_after_scan+=1
        except OSError as error:faults.append({'pid':pid,'field':'stat-after','errno':error.errno})
        else:
            if parts[0]=='Z':vanished_or_nonexecuting_after_scan+=1
            elif (value[value.find('(')+1:value.rfind(')')],int(parts[19]))!=identity_before:
                faults.append({'pid':pid,'field':'identity-after','identity_changed_during_inspection':True})
    platform_after=verify_platform_daemons()
    assert platform_before==platform_after and set(exempt_seen)==exempt
    row={'checked_live_other_processes':checked,'nonexecuting_zombies_excluded':zombies,
         'supervising_runner_pid_excluded':os.getpid(),'target_references':refs,
         'vanished_fd_links_observed_closed':closed_descriptors,
         'vanished_or_Z_processes_at_final_recheck':vanished_or_nonexecuting_after_scan,
         'ENOENT_on_nonexempt_live_process_field_rejected':True,
         'nonexempt_live_PID_starttime_comm_rechecked':True,'unexplained_live_field_failures':faults,
         'all_relevant_nonexempt_live_fields_readable':not faults,
         'no_target_references_from_inspectable_processes':not refs,
         'root_approved_exact_platform_daemons':platform_after,
         'exception_allowlist_sha256':digest(PLATFORM_ALLOW_BYTES),
         'universal_all_pid_readability_or_no_owner_claim':False,
         'scope':'relevant workloads and readable processes; explicitly uninspected exact pinned Docker daemons'}
    output=RUN/'process-reference-guard.json';output.write_text(json.dumps(row,indent=2)+'\n')
    assert not refs and not faults, 'cache process owner/field unknown: no removal permitted'
    return row

def guard_package_outputs_and_processes():
    current=files(TARGET)
    assert set(current)==set(FULL_CACHE_REFERENCE)|{'CACHEDIR.TAG'} and len(current)==609, 'cache pathset changed from reviewed609'
    identities={}
    for name,st in current.items():
        p=TARGET/name;assert stat.S_ISREG(st.st_mode),'cache unexpected nonregular path'
        data=p.read_bytes();mode=stat.S_IMODE(st.st_mode)
        original=PACKAGE_MAP['complete_cache_file_inventory'].get(name,EXISTING_MARKER_IDENTITY)
        assert len(data)==original['bytes'] and mode==original['full_mode']
        frozen=FULL_CACHE_REFERENCE.get(name,EXISTING_MARKER_IDENTITY)
        assert digest(data)==frozen['sha256'] and len(data)==frozen['bytes'] and mode==frozen['full_mode'] and st.st_mtime_ns==frozen['mtime_ns'], ('frozen complete608 cache identity changed',name)
        if name in INITIAL_PACKAGE_IDENTITIES:
            expected=INITIAL_PACKAGE_IDENTITIES[name]
            assert digest(data)==expected['sha256'] and st.st_mtime_ns==expected['mtime_ns']
            restored=gzip.decompress(Path(expected['gzip_path']).read_bytes());assert restored==data
        identities[name]={'sha256':digest(data),'bytes':len(data),'full_mode':mode,'mtime_ns':st.st_mtime_ns}
    return {'complete_cache_identity_map':identities,'package_output_count':len(INITIAL_PACKAGE_IDENTITIES),
            'all_package_output_gzip_decompression_verified':True,'process_guard':process_reference_guard(),
            'original403_fullguard':source_guard(OLD_SOURCE,OLD_MANIFEST),'candidate_fullguard':source_guard(SOURCE,MANIFEST)}

def verify_complete_cache_map(expected):
    current=files(TARGET)
    assert current.keys()==expected.keys(), 'cache pathset changed during owned marker/dry-run gate'
    for name,row in expected.items():
        p=TARGET/name;st=current[name]
        assert stat.S_ISREG(st.st_mode), ('cache unexpectedly nonregular',name)
        data=p.read_bytes()
        assert digest(data)==row['sha256'] and len(data)==row['bytes'] and stat.S_IMODE(st.st_mode)==row['full_mode'], ('cache identity changed',name)
        if 'mtime_ns' in row:assert st.st_mtime_ns==row['mtime_ns'], ('cache mtime changed',name)
    return {'files':len(current),'all_bytes_sha256_fullmodes_and_recorded_mtimes_match':True}

def observe_quarantine_identity(path,expected):
    # Diagnostic inspection remains bounded by each reviewed artifact's length.
    if not os.path.lexists(path):return {'present':False}
    try:
        st=path.lstat()
        result={'present':True,'regular':stat.S_ISREG(st.st_mode),'bytes':st.st_size,
                'full_mode':stat.S_IMODE(st.st_mode),'mtime_ns':st.st_mtime_ns,
                'inode':[st.st_dev,st.st_ino]}
        if result['regular'] and st.st_size==expected['bytes']:
            result['sha256']=digest(path.read_bytes())
        result['identity_matches_expected']=all(result.get(k)==expected[k] for k in ['bytes','full_mode','mtime_ns','inode','sha256']) and result['regular']
        return result
    except BaseException as fault:
        return {'present':True,'inspection_error':type(fault).__name__,'identity_matches_expected':False}

def require_quarantine_identity(path,expected):
    result=observe_quarantine_identity(path,expected)
    assert result.get('identity_matches_expected'), ('reviewed package identity changed',str(path),result)
    return result

def quarantine_location_snapshot(moved):
    rows=[]
    for name,expected in sorted(INITIAL_PACKAGE_IDENTITIES.items()):
        source=observe_quarantine_identity(TARGET/name,expected)
        retained=observe_quarantine_identity(QUARANTINE/name,expected)
        exactly_once=source.get('present',False)!=retained.get('present',False)
        intact=exactly_once and (source if source.get('present') else retained).get('identity_matches_expected',False)
        rows.append({'cache_relative_path':name,'source_path':str(TARGET/name),
                     'quarantine_path':str(QUARANTINE/name),'source':source,'quarantine':retained,
                     'exactly_once_and_intact':intact,'completed_in_ledger':name in moved})
    return {'observed_locations':rows,'all25_present_exactly_once_and_intact':len(rows)==25 and all(r['exactly_once_and_intact'] for r in rows),
            'completed_rename_count':len(moved),'automatic_rollback_or_retry':False}

def quarantine_log_event(event):
    # Fresh exclusive ledger; each planned and completed rename is flushed.
    path=RUN/'quarantine-moves.jsonl'
    with path.open('a') as output:
        output.write(json.dumps(event,sort_keys=True)+'\n');output.flush();os.fsync(output.fileno())

def quarantine_own_package_outputs(checked):
    expected=checked['complete_cache_identity_map']
    assert len(expected)==609 and set(expected)==set(FULL_CACHE_REFERENCE)|{'CACHEDIR.TAG'}
    verify_complete_cache_map(expected)
    targetst=TARGET.lstat()
    assert stat.S_ISDIR(targetst.st_mode) and targetst.st_uid==os.getuid() and stat.S_IMODE(targetst.st_mode)==0o700
    assert (targetst.st_dev,targetst.st_ino)==(27,524404), 'owned target identity changed'
    assert len(INITIAL_PACKAGE_IDENTITIES)==25
    for name,row in sorted(INITIAL_PACKAGE_IDENTITIES.items()):
        assert not Path(name).is_absolute() and '..' not in Path(name).parts and len(Path(name).parts)<=4
        assert name in expected and all(row[k]==expected[name][k] for k in ['sha256','bytes','full_mode','mtime_ns'])
        require_quarantine_identity(TARGET/name,row)
        compressed=Path(row['gzip_path']).read_bytes()
        assert digest(compressed)==row['gzip_sha256'] and gzip.decompress(compressed)==(TARGET/name).read_bytes()
    assert not os.path.lexists(QUARANTINE), 'never overwrite a prior quarantine directory'
    parent=QUARANTINE.parent.lstat()
    assert stat.S_ISDIR(parent.st_mode) and parent.st_uid==os.getuid() and parent.st_dev==targetst.st_dev
    # mkdir without exist_ok is exclusive. Trusted0700 destination plus the
    # exclusive lease prevents cooperating writers; no universal race-free claim.
    QUARANTINE.mkdir(mode=0o700)
    qst=QUARANTINE.lstat()
    assert stat.S_ISDIR(qst.st_mode) and stat.S_IMODE(qst.st_mode)==0o700 and qst.st_uid==os.getuid() and qst.st_dev==targetst.st_dev
    ledger=RUN/'quarantine-moves.jsonl'
    with ledger.open('x') as output:output.flush();os.fsync(output.fileno())
    ledger.chmod(0o600)
    moved=[]
    try:
        parents={str(Path(name).parent) for name in INITIAL_PACKAGE_IDENTITIES}
        assert len(parents)<=25
        for relative in sorted(parents):
            path=QUARANTINE
            for component in Path(relative).parts:
                path=path/component
                if not os.path.lexists(path):path.mkdir(mode=0o700)
                st=path.lstat()
                assert stat.S_ISDIR(st.st_mode) and st.st_uid==os.getuid() and stat.S_IMODE(st.st_mode)==0o700 and st.st_dev==targetst.st_dev
        for name,row in sorted(INITIAL_PACKAGE_IDENTITIES.items()):
            if INTERRUPTED is not None:raise InterruptedError('controlled before-rename interruption')
            assert set(files(TARGET))==set(expected)-set(moved), 'cache pathset changed during quarantine'
            source=TARGET/name;dest=QUARANTINE/name
            require_quarantine_identity(source,row)
            path=QUARANTINE
            for component in Path(name).parts[:-1]:
                path=path/component;st=path.lstat()
                assert stat.S_ISDIR(st.st_mode) and stat.S_IMODE(st.st_mode)==0o700 and st.st_uid==os.getuid() and st.st_dev==targetst.st_dev
            assert not os.path.lexists(dest), ('existing quarantine destination',str(dest))
            quarantine_log_event({'operation':'rename_planned','cache_relative_path':name,
                'source_path':str(source),'quarantine_path':str(dest),
                'expected_identity':{k:row[k] for k in ['sha256','bytes','full_mode','mtime_ns','inode']}})
            os.rename(source,dest)
            assert not os.path.lexists(source), ('source path remained after rename',str(source))
            actual=require_quarantine_identity(dest,row)
            moved.append(name)
            quarantine_log_event({'operation':'rename_completed','cache_relative_path':name,'actual_identity':actual})
        after=files(TARGET)
        assert set(after)==set(expected)-set(INITIAL_PACKAGE_IDENTITIES) and len(after)==584
        survivors={name:row for name,row in expected.items() if name not in INITIAL_PACKAGE_IDENTITIES}
        verify_complete_cache_map(survivors)
        assert set(files(QUARANTINE))==set(INITIAL_PACKAGE_IDENTITIES), 'quarantine regular pathset changed'
        locations=quarantine_location_snapshot(moved)
        assert locations['all25_present_exactly_once_and_intact']
        result={'schema_version':1,'kind':'ROOT-authorized-reversible-exact25-owned-package-quarantine',
            'source_sha':PLAN['source_sha'],'quarantine_directory':str(QUARANTINE),
            'quarantine_directory_identity':{'inode':[qst.st_dev,qst.st_ino],'uid':qst.st_uid,'full_mode':stat.S_IMODE(qst.st_mode)},
            'original_cache_files':609,'retained_survivor_files':584,'quarantined_original_files':25,
            'all584_survivor_sha_bytes_fullmodes_mtimes_unchanged':True,
            'all25_original_sha_bytes_fullmodes_mtimes_inodes_preserved':True,
            'all25_existing_gzip_decompression_verified':True,'retained_locations':locations,
            'actual_reclaimed_file_bytes':0,'raw_original_bytes_deleted_copied_or_reencoded':False,
            'standard_CACHEDIR_TAG_left_unchanged':True,'actual_Cargo_clean_or_dryrun_commands':0,
            'rename_ledger_sha256':digest(ledger.read_bytes()),'automatic_rollback_or_retry':False,
            'universal_race_free_ownership_or_power_loss_durability_claim':False}
        output=RUN/'package-quarantine-receipt.json';output.write_text(json.dumps(result,indent=2)+'\n');output.chmod(0o600)
        return result
    except BaseException as fault:
        # Preserve every surviving source/destination and the actual planned vs
        # completed ledger. No delete, chmod of originals, rollback or retry.
        result={'schema_version':1,'kind':'actual-partial-quarantine-failure','error_type':type(fault).__name__,
            'quarantine_directory':str(QUARANTINE),'locations':quarantine_location_snapshot(moved),
            'rename_ledger_sha256':digest(ledger.read_bytes()),'automatic_rollback_or_retry':False,
            'following_compiler_or_test_launch_permitted':False}
        output=RUN/'package-quarantine-failure.json';output.write_text(json.dumps(result,indent=2)+'\n');output.chmod(0o600)
        raise

def prove_forced_old_library(row):
    assert INVALIDATION_DONE
    log=Path(row['stderr_path']).read_text()
    assert any('Running ' in line and '--crate-name partitionline ' in line and '--crate-type lib' in line for line in log.splitlines()), 'actual rustc library invocation absent'
    artifacts=[]
    for suffix in ['rlib','rmeta']:
        found=list((TARGET/'debug/deps').glob('libpartitionline-*.'+suffix));assert len(found)==1
        p=found[0];data=p.read_bytes();sha=digest(data)
        previous={x['sha256'] for x in PACKAGE_MAP['package_output_restore_map'] if x['kind']==suffix}
        assert sha not in previous, 'old package bytes unexpectedly identical to cached new package'
        artifacts.append({'path':str(p),'bytes':len(data),'sha256':sha,'full_mode':stat.S_IMODE(p.stat().st_mode)})
        compressed=gzip.compress(data,compresslevel=1,mtime=0)
        needed=len(data)+(len(data)//16383+1)*5+64+1024*1024
        assert free_bytes()>=FLOOR+needed
        output=RUN/'retained-elfs'/(sha+'.'+suffix+'.gz');output.write_bytes(compressed);output.chmod(0o600)
        assert gzip.decompress(output.read_bytes())==data
        artifacts[-1].update({'gzip_path':str(output),'gzip_sha256':digest(compressed),'gzip_bytes':len(compressed),'decompression_verified':True})
    # The verbose command/cwd and independently differing bytes are the direct
    # forced-compilation proof. Future genuine SDK/JVM work stays separately held.
    return {'actual_library_rustc_invocation_retained':True,'compile_cwd':str(BASELINE),
        'all_checked_old_input_and_five_overlay_blobs_bound':True,'rebuilt_library_artifacts':artifacts,
        'new_package_artifact_reuse':False,'scope':'actualold selectivebaseline only, nofulloldtree/candidate qualification'}

try:
    LAST_FORECAST_KIND='quarantine';initial_forecast=forecast()
    assert initial_forecast['sampled_free_bytes']>=initial_forecast['postquarantine_compile_required_free_bytes'], 'pre-quarantine actual free insufficient; raw files reclaim zero'
    (RUN/'launch-contract.json').write_text(json.dumps({'schema_version':1,'candidate_source_sha':PLAN['source_sha'],
        'origin_receipt_sha256':digest(ORIGIN_BYTES),'forecast':initial_forecast,
        'platform_exception_sha256':digest(PLATFORM_ALLOW_BYTES),'root_external_publication_reserve_bytes':ROOT_PUBLICATION_RESERVE,
        'baseline_source_sha':BASELINE_INPUTS['source_sha'],'baseline_selected_inputs':66,'explicit_overlay_count':5,
        'expected_actual_behavior':'positive2 passes then exact3+1 old behavior failures; never compile/setup failures',
        'root_authorized_scope':'exact25 reversible quarantine then forced selective403 baseline once ROOT GO; candidate phases held',
        'expected_actual_subprocesses':6,'actual_Cargo_clean_or_dryrun_commands':0,'quarantine_reclaimed_bytes':0},indent=2)+'\n')
    seed_retention()
    docker_zero_running_workloads('platform-docker-before-quarantine')
    checked_prequarantine=guard_package_outputs_and_processes()
    (RUN/'prequarantine609-guard.json').write_text(json.dumps(checked_prequarantine,indent=2)+'\n')
    quarantine_own_package_outputs(checked_prequarantine)
    docker_zero_running_workloads('platform-docker-after-quarantine')
    postquarantine_process_guard=process_reference_guard()
    (RUN/'postquarantine-relevant-process-guard.json').write_text(json.dumps(postquarantine_process_guard,indent=2)+'\n')
    survivor_map={name:row for name,row in checked_prequarantine['complete_cache_identity_map'].items() if name not in INITIAL_PACKAGE_IDENTITIES}
    verify_complete_cache_map(survivor_map)
    locations=quarantine_location_snapshot(list(INITIAL_PACKAGE_IDENTITIES))
    assert locations['all25_present_exactly_once_and_intact'], 'quarantine changed before old compiler'
    assert set(files(QUARANTINE))==set(INITIAL_PACKAGE_IDENTITIES)
    (RUN/'postquarantine-before-compiler-guard.json').write_text(json.dumps({'survivor584_guard':verify_complete_cache_map(survivor_map),'quarantine25_locations':locations},indent=2)+'\n')
    INVALIDATION_DONE=True;LAST_FORECAST_KIND='compile';forecast()
    materialize_baseline()
    commands=[
      ('stable-baseline-positive-controls','baseline_positive_controls',0,2,0,
       ['old_supported_v1_abort_positive_control','old_supported_v0_share_positive_control']),
      ('stable-baseline-api27-red','fail_first_write_txn_markers_v2',101,0,3,
       ['missing_result_must_not_mean_success','broker_and_replica_unavailability_must_remap_and_retry','version2_only_public_abort_must_negotiate_the_default_marker']),
      ('stable-baseline-api90-red','fail_first_share_offsets_v1',101,0,1,
       ['existing_public_operation_must_consume_v1_lag'])]
    for name,target,expected_exit,passed,failed,names in commands:
        argv=['taskset','-c','0,1','/workspace/work/rustup/toolchains/stable-x86_64-unknown-linux-gnu/bin/cargo',
              'test','--verbose','--test',target,'--offline','--locked','--manifest-path',str(BASELINE/'Cargo.toml'),'--','--test-threads=1']
        old_before=selected_origin_guard()
        row=run(name,argv,BASELINE,BASELINE_MANIFEST,expected_exit)
        old_after=selected_origin_guard();assert old_before==old_after
        row['selected_origin_before']=old_before;row['selected_origin_after']=old_after
        test_result(row,passed,failed,names)
        if target=='baseline_positive_controls':
            BASELINE_LIBRARY_PROOF=prove_forced_old_library(row)
            nmrow=run('old-library-symbol-probe',['taskset','-c','0,1','/usr/bin/nm','-C','--defined-only',BASELINE_LIBRARY_PROOF['rebuilt_library_artifacts'][0]['path']],BASELINE,BASELINE_MANIFEST)
            symbols=Path(nmrow['stdout_path']).read_text()
            assert 'describe_share_group_offsets_with_lag_timeout' not in symbols, 'new candidate symbol survived forced old compile'
            BASELINE_LIBRARY_PROOF['old_library_new_lag_symbol_absent']=True
            BASELINE_LIBRARY_PROOF['actual_nm_command_index']=len(ROWS)-1
            persist()
    WORKFLOW_PASSED=True;persist()
except BaseException as failure:
    if ACTIVE is not None:
        emergency=close_group(ACTIVE,True)
        (RUN/'emergency-owned-group-cleanup.json').write_text(json.dumps(emergency,indent=2)+'\n')
    persist()
    (RUN/'failure.json').write_text(json.dumps({'type': type(failure).__name__, 'detail': str(failure),
        'source_sha': PLAN['source_sha'], 'passed': False, 'no_following_command_launched': True}, indent=2)+'\n')
    raise
print('baseline403 exactpositive2/red3+1 closed; candidate tests/strict/MSRV/JVM/full remain held',flush=True)

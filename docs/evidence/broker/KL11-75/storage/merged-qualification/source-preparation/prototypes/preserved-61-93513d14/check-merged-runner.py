#!/usr/bin/env python3
"""Tiny controls only: no Cargo, source extraction, or real-cache access."""
import ast
import gzip
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import signal
import sys
import subprocess
import time

BASE = Path(__file__).resolve().parent
DRIVER = BASE / 'run-merged-final.py'
SOURCE = DRIVER.read_bytes()
TREE_AST = ast.parse(SOURCE)
SUFFIX = hashlib.sha256(SOURCE).hexdigest()[:8]
OUT = BASE / ('merged-helper-check-' + SUFFIX)
OUT.mkdir(exist_ok=False)
CACHE = OUT / 'toy-cache'
CACHE.mkdir()
TREE = OUT / 'toy-source'
TREE.mkdir()
ENV = {'CARGO_TARGET_DIR': str(CACHE)}
receipt = {'mock_helper_scope': True}
elf_objects = {}
elf_snapshots = {}
space = globals()
for node in TREE_AST.body:
    if isinstance(node, ast.FunctionDef) and node.name in ('cache_owners', 'sha256_file', 'elf_identity', 'retain_elf', 'retain_cache', 'executed_elfs', 'verify_external_tools', 'verify', 'write_json_audit', 'stop_command_group', 'monitored_command', 'retention_forecast', 'parse_cpus'):
        exec(compile(ast.Module(body=[node], type_ignores=[]), str(DRIVER), 'exec'), space)
for node in TREE_AST.body:
    if isinstance(node, ast.Assign) and any(isinstance(t, ast.Name) and t.id in ('PROFILES','TOOLCHAINS','DISK_FLOOR_BYTES','DISK_POLL_SECONDS','AUDIT_COMPRESSION_THRESHOLD') for t in node.targets):
        exec(compile(ast.Module(body=[node], type_ignores=[]), str(DRIVER), 'exec'), space)
checks = []

# This actually exits unsuccessfully, then the same path is overwritten. The
# failed bytes must survive independently of the next command's new bytes.
harness = CACHE / 'failed-harness'
shutil.copyfile('/usr/bin/false', harness)
harness.chmod(0o751)
result = subprocess.run([str(harness)], check=False)
assert result.returncode == 1
failed_hash = sha256_file(harness)
reported = executed_elfs('Running tests/failure.rs (' + str(harness) + ')\n', 'failed-test')
assert len(reported) == 1 and reported[0]['sha256'] == failed_hash
assert reported[0]['original_mode'] == 0o751
retain_cache('failed-test', 'immediate post-failure snapshot')
checks.append({'name': 'actual_failed_ELF_retained_before_overwrite', 'exit_code': result.returncode, 'sha256': failed_hash, 'original_mode': 0o751})

shutil.copyfile('/usr/bin/true', harness)
harness.chmod(0o640)
new_hash = sha256_file(harness)
assert new_hash != failed_hash
current = retain_cache('next-build', 'new bytes at same cache path')
assert current[0]['sha256'] == new_hash and current[0]['original_mode'] == 0o640
assert len(elf_objects) == 2
for item in (reported[0], current[0]):
    restored = OUT / ('restored-' + item['sha256'])
    with gzip.open(OUT / item['retained_object'], 'rb') as source, restored.open('wb') as target:
        shutil.copyfileobj(source, target)
    restored.chmod(item['original_mode'])
    assert sha256_file(restored) == item['sha256']
    assert restored.stat().st_mode & 0o7777 == item['original_mode']
    restored.unlink()
checks.append({'name': 'same_path_new_bytes_preserves_both_generations_and_modes', 'objects': len(elf_objects), 'restored_bytes_and_full_permission_modes_match': True})

# Before cleaning, even cached identities must re-hash source and decompress
# the saved object. Deliberate object corruption must stop that guard.
verified = retain_cache('before-clean', 'verify bytes before cleanup', force_verify=True)
assert all(item['pre_clean_bytes_and_gzip_verified'] for item in verified)
object_path = OUT / verified[0]['retained_object']
object_original = object_path.read_bytes()
object_path.write_bytes(b'corrupted-control')
try:
    retain_cache('bad-object-clean', 'controlled corruption', force_verify=True)
    raise AssertionError('corrupted retained object accepted')
except (gzip.BadGzipFile, EOFError, AssertionError) as error:
    assert str(error) != 'corrupted retained object accepted'
    checks.append({'name': 'corrupted_retained_object_blocks_clean', 'rejection': type(error).__name__})
finally:
    object_path.write_bytes(object_original)
retain_cache('restored-object-clean', 'restored control', force_verify=True)

# A running ELF inside the target is a live cache owner even without Cargo.
sleeper = CACHE / 'sleeper'
shutil.copyfile('/usr/bin/sleep', sleeper)
sleeper.chmod(0o755)
process = subprocess.Popen([str(sleeper), '5'])
try:
    found = []
    for _ in range(20):
        found = [item for item in cache_owners() if item['pid'] == process.pid]
        if found:
            break
        time.sleep(0.01)
    assert found, 'running cache ELF owner missed'
    checks.append({'name': 'active_cache_owner_detected_before_clean', 'owner': found[0], 'clean_would_be_blocked': True})
finally:
    process.terminate()
    process.wait()
assert not cache_owners()
checks.append({'name': 'released_cache_has_no_owners', 'owners': []})

# A separately bound external test tool must stay at the resolved path with
# identical bytes and full permission mode. This mock does not invoke OpenSSL.
external = CACHE / 'openssl'
shutil.copyfile('/usr/bin/true', external)
external.chmod(0o751)
retained_tool = retain_elf(external, 'mock external test tool', 'prerequisite')
receipt['external_test_tools'] = [{**retained_tool, 'resolved_path': str(external)}]
ENV['PATH'] = str(CACHE)
assert len(verify_external_tools()) == 1
external.chmod(0o755)
try:
    verify_external_tools()
    raise RuntimeError('changed external tool mode accepted')
except AssertionError:
    pass
external.chmod(0o751)
checks.append({'name': 'external_tool_exact_path_bytes_and_full_mode_bound', 'controlled_changed_mode_rejected': True})

# Full source set/blob checks are independent of whether a tree is reused.
source_file = TREE / 'source.txt'
source_data = b'bounded immutable source mock\n'
source_file.write_bytes(source_data)
source_file.chmod(0o644)
expected = {'source.txt': {'mode': '100644', 'filesystem_permission_mode': 0o644, 'git_blob_sha1': hashlib.sha1(b'blob ' + str(len(source_data)).encode() + b'\0' + source_data).hexdigest()}}
assert verify()['all_git_blobs_match']
source_file.write_bytes(b'changed')
try:
    verify()
    raise RuntimeError('mutated source accepted')
except AssertionError:
    pass
source_file.write_bytes(source_data)
extra = TREE / 'unexpected.txt'
extra.write_text('extra')
try:
    verify()
    raise RuntimeError('extra source file accepted')
except AssertionError:
    pass
extra.unlink()
assert verify()['all_git_blobs_match']
source_file.chmod(0o600)
try:
    verify()
    raise RuntimeError('changed full source permissions accepted')
except AssertionError:
    pass
source_file.chmod(0o644)
assert verify()['all_baseline_permission_modes_match']
checks.append({'name': 'reused_source_set_blob_and_full_permission_guard', 'controlled_mutation_extra_file_and_permission_change_rejected': True})

# Exercise large-audit compression at a tiny injected threshold. Production
# threshold remains the literal four MiB, independently checked here.
assert AUDIT_COMPRESSION_THRESHOLD == 4 * 1024 * 1024
original_threshold = AUDIT_COMPRESSION_THRESHOLD
AUDIT_COMPRESSION_THRESHOLD = 128
audit_path = OUT / 'source-integrity-control.json'
audit = write_json_audit(audit_path, {'synthetic_rows': ['public audit control'] * 40})
assert audit['compression'] == 'gzip' and not audit_path.exists()
restored = OUT / 'restored-audit.json'
with gzip.open(OUT / audit['path'], 'rb') as source, restored.open('wb') as target:
    shutil.copyfileobj(source, target)
restored.chmod(audit['original_mode'])
assert sha256_file(restored) == audit['uncompressed_sha256']
assert restored.stat().st_mode & 0o7777 == audit['original_mode']
assert audit['restore_path'] == 'source-integrity-control.json'
restored.unlink()
AUDIT_COMPRESSION_THRESHOLD = original_threshold
checks.append({'name': 'audit_gzip_restores_raw_hash_bytes_and_full_mode', 'audit': audit, 'production_threshold_bytes': AUDIT_COMPRESSION_THRESHOLD})

# Probes are injected; no real disk is filled. A healthy command completes.
high = DISK_FLOOR_BYTES + 1024 * 1024
low = DISK_FLOOR_BYTES - 1
with (OUT/'monitor-success.log').open('w') as log:
    code, monitor = monitored_command(['/usr/bin/true'], OUT, dict(os.environ), log, OUT/'monitor-success.jsonl', probe=lambda: high)
assert code == 0 and not monitor['triggered']
checks.append({'name': 'healthy_live_monitor_command_completes', 'exit_code': code, 'monitor': monitor, 'probe_scope': 'synthetic free-space observations'})

# At the synthetic threshold crossing, stop both parent and child in the new
# session, while this test and unrelated workers remain alive.
pid_path = OUT/'monitor-child.pid'
script = 'import pathlib,subprocess,time; p=subprocess.Popen(["/usr/bin/sleep","20"]); pathlib.Path('+repr(str(pid_path))+').write_text(str(p.pid)); time.sleep(20)'
values = iter([high, high, low])
with (OUT/'monitor-stop.log').open('w') as log:
    code, monitor = monitored_command([sys.executable,'-c',script], OUT, dict(os.environ), log, OUT/'monitor-stop.jsonl', probe=lambda: next(values, low))
assert code != 0 and monitor['triggered'] and monitor['minimum_free_bytes'] == low
assert pid_path.exists()
child_pid = int(pid_path.read_text())
status = Path('/proc')/str(child_pid)/'status'
if status.exists():
    assert '\nState:\tZ' in status.read_text(), 'isolated command child still running'
assert not cache_owners()
checks.append({'name': 'live_floor_stops_only_owned_parent_child_group', 'exit_code': code, 'monitor': monitor, 'child_no_longer_running': True, 'probe_scope': 'synthetic threshold crossing; real child processes'})

# Even a successful fast command must be refused when the final sample falls
# below the reserve, preventing a later command from being launched.
values = iter([high, high, low])
with (OUT/'monitor-final-low.log').open('w') as log:
    code, monitor = monitored_command(['/usr/bin/true'], OUT, dict(os.environ), log, OUT/'monitor-final-low.jsonl', probe=lambda: next(values, low))
assert code == 75 and monitor['triggered'] and monitor['actual_process_exit_code'] == 0
checks.append({'name': 'final_low_sample_rejects_zero_exit_before_next_command', 'exit_code': code, 'monitor': monitor, 'probe_scope': 'synthetic free-space observations'})

# A failing observation must also stop the isolated command, rather than leave
# a compiler running after the monitor raises.
values = iter([high, high])
def bad_probe():
    try: return next(values)
    except StopIteration: raise RuntimeError('controlled observation failure')
with (OUT/'monitor-error.log').open('w') as log:
    try:
        monitored_command(['/usr/bin/sleep','20'], OUT, dict(os.environ), log, OUT/'monitor-error.jsonl', probe=bad_probe)
        raise AssertionError('failed observation did not propagate')
    except RuntimeError as error:
        assert str(error) == 'controlled observation failure'
checks.append({'name': 'monitor_error_stops_owned_process_before_propagating', 'controlled_error_rejected': True})


# Forecast refusal performs no output write and leaves source cache bytes
# intact. No compression ratio assumption is used for unknown ELF contents.
forecast_elf = CACHE/'forecast-unknown'
shutil.copyfile('/usr/bin/true', forecast_elf)
old_objects = len(elf_objects)
forecast = retention_forecast(probe=lambda: 1 << 40)
assert forecast['sufficient'] and forecast['new_gzip_upper_bound_bytes'] > 0
refused = retention_forecast(probe=lambda: forecast['required_free_bytes'] - 1)
assert not refused['sufficient']
assert len(elf_objects) == old_objects and forecast_elf.exists()
checks.append({'name': 'compressed_retention_forecast_refuses_before_writes', 'required_free_bytes': forecast['required_free_bytes'], 'unretained_elfs': len(forecast['unretained_elfs']), 'no_cleanup_or_gzip_written': True})

# A process outside the cache still owns it when its environment names that
# exact target. This deliberately does not launch Cargo.
owner_env = dict(os.environ, CARGO_TARGET_DIR=str(CACHE))
process = subprocess.Popen(['/usr/bin/sleep','5'],env=owner_env)
try:
    found=[]
    for _ in range(20):
        found=[row for row in cache_owners() if row['pid']==process.pid]
        if found:break
        time.sleep(0.01)
    assert found and any(value.startswith('CARGO_TARGET_DIR=') for value in found[0]['cache_references'])
finally:
    process.terminate();process.wait()
checks.append({'name': 'noncompiler_environment_cache_owner_guard', 'owner':found[0], 'released':True})

# An mmap survives closing its source descriptor. The maps guard must detect
# that use even when executable/cwd/environment never name the cache.
map_marker=OUT/'mapped-ready'
map_code='import mmap,pathlib,time; f=open('+repr(str(harness))+',"rb"); data=mmap.mmap(f.fileno(),0,access=mmap.ACCESS_READ); f.close(); pathlib.Path('+repr(str(map_marker))+').write_text("ready"); time.sleep(5)'
map_env=dict(os.environ);map_env.pop('CARGO_TARGET_DIR',None)
process=subprocess.Popen([sys.executable,'-c',map_code],cwd=OUT,env=map_env)
try:
    for _ in range(50):
        if map_marker.exists():break
        time.sleep(0.01)
    assert map_marker.exists()
    found=[row for row in cache_owners() if row['pid']==process.pid]
    assert found and any(value.startswith('mapped:') for value in found[0]['cache_references'])
finally:
    process.terminate();process.wait()
checks.append({'name': 'mapped_cache_owner_after_closed_descriptor_guard', 'owner':found[0], 'released':True})
assert not cache_owners()

assert parse_cpus('2,4')==(2,4) and parse_cpus('0-2,4')==(0,1,2,4)
for invalid in ('3','0-4','2,2','x','2,'):
    try:
        parse_cpus(invalid)
        raise RuntimeError('invalidCPUaccepted')
    except AssertionError:pass
checks.append({'name':'explicit_cpu_affinity_binds_canonical_ids_and_rejects_reserved_cpu3','default_cpu_ids':[2,4],'invalid_controls':5})

# Use only AST construction of command declarations; never invoke Cargo.
commands_ast = []
inside = False
for node in TREE_AST.body:
    if isinstance(node, ast.Assign) and any(isinstance(t, ast.Name) and t.id == 'commands' for t in node.targets):
        inside = True
    if inside:
        if isinstance(node, ast.For) and isinstance(node.target, ast.Tuple) and [item.id for item in node.target.elts] == ['ledger','name','argv','extra']:
            break
        commands_ast.append(node)
space['ENV'] = {'CARGO_TARGET_DIR': '/fake-target'}
space['CPU_ARGUMENT'] = '2,4'
receipt['qualification_matrix'] = {'expected_command_count':51}
exec(compile(ast.Module(body=commands_ast, type_ignores=[]), str(DRIVER), 'exec'), space)
assert len(commands) == 51
cells = [name for name, _, extra in commands if extra.get('capture')]
assert cells == [tc+'-'+profile+'-all-targets' for tc in TOOLCHAINS for profile,_ in PROFILES]
assert len(cells)==12
assert len(schedule)==61 and len(maintenance_specs)==10
assert [row['name'] for row in maintenance_specs]==[tc+'-maintenance-after-'+profile for tc in TOOLCHAINS for profile,_ in PROFILES if profile!='all-features']
for row in maintenance_specs:
    assert '--package' not in row['argv'] and row['operation']=='full-generated-cache-clean'
    assert '--offline' in row['argv'] and '--locked' in row['argv']
    assert row['argv'][:3]==['taskset','-c','2,4']
checks.append({'name':'ten_separate_between_profile_full_cache_cleans','qualification_commands':51,'maintenance_commands':10,'actual_commands':61,'operations':maintenance_specs})
for name,argv,extra in commands:
    if argv[2] != 'fmt': assert '--offline' in argv and '--locked' in argv
    if extra.get('capture'):
        profile=name.removeprefix(argv[1][1:]+'-').removesuffix('-all-targets')
        flags=dict(PROFILES)[profile]
        for flag in flags: assert flag in argv
checks.append({'name': 'fifty_one_exact_command_six_profile_contract', 'commands': len(commands), 'capture_cells': cells})

result = {'schema': 1, 'driver_sha256': hashlib.sha256(SOURCE).hexdigest(), 'scope': 'tiny stdlib/real ELF helper controls only; no Cargo, actual source extraction or real target access', 'checks': checks, 'elf_objects': list(elf_objects.values()), 'source_unchanged': DRIVER.read_bytes() == SOURCE}
assert result['source_unchanged']
(BASE / ('merged-helper-check-' + SUFFIX + '.json')).write_text(json.dumps(result, indent=2) + '\n')
print(json.dumps({'checks_passed': len(checks), 'driver_sha256': result['driver_sha256'], 'command_count': len(commands), 'actual_cache_or_Cargo_access': False}))

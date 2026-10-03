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
import subprocess
import time

BASE = Path(__file__).resolve().parent
DRIVER = BASE / 'run-final.py'
SOURCE = DRIVER.read_bytes()
TREE_AST = ast.parse(SOURCE)
SUFFIX = hashlib.sha256(SOURCE).hexdigest()[:8]
OUT = BASE / ('retention-helper-check-' + SUFFIX)
OUT.mkdir(exist_ok=False)
CACHE = OUT / 'toy-cache'
CACHE.mkdir()
TREE = OUT / 'toy-source'
TREE.mkdir()
ENV = {'CARGO_TARGET_DIR': str(CACHE)}
receipt = {}
elf_objects = {}
elf_snapshots = {}
space = globals()
for node in TREE_AST.body:
    if isinstance(node, ast.FunctionDef) and node.name in ('cache_owners', 'sha256_file', 'elf_identity', 'retain_elf', 'retain_cache', 'executed_elfs', 'verify_external_tools', 'verify'):
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
expected = {'source.txt': {'mode': '100644', 'git_blob_sha1': hashlib.sha1(b'blob ' + str(len(source_data)).encode() + b'\0' + source_data).hexdigest()}}
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
checks.append({'name': 'reused_source_set_and_blob_guard', 'controlled_mutation_and_extra_file_rejected': True})

# Use only AST construction of command declarations; never invoke Cargo.
commands_ast = []
inside = False
for node in TREE_AST.body:
    if isinstance(node, ast.Assign) and any(isinstance(t, ast.Name) and t.id == 'commands' for t in node.targets):
        inside = True
    if inside:
        if isinstance(node, ast.For) and isinstance(node.target, ast.Tuple):
            break
        commands_ast.append(node)
space['ENV'] = {'CARGO_TARGET_DIR': '/fake-target'}
exec(compile(ast.Module(body=commands_ast, type_ignores=[]), str(DRIVER), 'exec'), space)
assert len(commands) == 19
cells = [name for name, _, extra in commands if extra.get('capture')]
assert cells == ['stable-default-all-targets', 'stable-all-features-all-targets', '1.85.0-default-all-targets', '1.85.0-all-features-all-targets']
checks.append({'name': 'nineteen_exact_command_contract', 'commands': len(commands), 'capture_cells': cells})

result = {'schema': 1, 'driver_sha256': hashlib.sha256(SOURCE).hexdigest(), 'scope': 'tiny stdlib/real ELF helper controls only; no Cargo, actual source extraction or real target access', 'checks': checks, 'elf_objects': list(elf_objects.values()), 'source_unchanged': DRIVER.read_bytes() == SOURCE}
assert result['source_unchanged']
(BASE / ('retention-helper-check-' + SUFFIX + '.json')).write_text(json.dumps(result, indent=2) + '\n')
print(json.dumps({'checks_passed': len(checks), 'driver_sha256': result['driver_sha256'], 'command_count': len(commands), 'actual_cache_or_Cargo_access': False}))

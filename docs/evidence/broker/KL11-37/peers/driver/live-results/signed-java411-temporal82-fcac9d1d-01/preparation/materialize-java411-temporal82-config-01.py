from pathlib import Path
import hashlib, importlib.util, json, os, shutil, sys
sys.dont_write_bytecode = True
B = Path('/workspace/work/broker-oidc')
SOURCE = Path('/workspace/work/broker-merged-source-fcac9d1d')
OUT = B / 'live-configs-fcac9d1d-java411-temporal82-01'
RECIPE = B / 'future-nbf-recipe-review-01/signed-stable-java-4-1-2-temporal-query-review-01.recipe.json'
ORIGINAL = B / 'live-configs-fcac9d1d-predeclared-01/signed-stable-java-4-1-2.json'

def sha(p):
    return hashlib.sha256(p.read_bytes()).hexdigest()

assert not OUT.exists()
assert sha(RECIPE) == '0dfaba71c3beea9fba1b8b57359a518b5ba4d8904d23126fadcd77ca51b3fc87'
recipe = json.loads(RECIPE.read_text())
assert len(recipe['steps']) == 82
original_bytes = ORIGINAL.read_bytes()
cfg = json.loads(original_bytes)
assert cfg['source_sha'] == 'fcac9d1d783b63890b3316601de72a4075973e10'
assert cfg['profile'] == 'metadata' and [p['id'] for p in cfg['peers']] == ['java-4-1-2']
cfg['steps'] = recipe['steps']
for name in ['driver', 'outer', 'issuer']:
    assert sha(SOURCE / cfg[name + '_relative_path']) == cfg[name + '_sha256']
runtime_count = 0
for peer in cfg['peers']:
    p = Path(peer['build_receipt'])
    assert sha(p) == peer['build_receipt_sha256']
    r = json.loads(p.read_text())
    assert r['source_sha'] == cfg['source_sha'] and r['exit_code'] == 0
    for row in peer['runtime_inputs']:
        p = Path(row['path'])
        assert sha(p) == row['sha256'] and p.stat().st_size == row['bytes']
        assert oct(p.stat().st_mode & 0o7777) == row['full_mode']
        runtime_count += 1
    for row in peer['source_inputs']:
        assert sha(SOURCE / row['path']) == row['sha256']
for key in ['normal_example_build', 'immutable_origin']:
    assert sha(Path(cfg[key + '_receipt'])) == cfg[key + '_receipt_sha256']
normal = json.loads(Path(cfg['normal_example_build_receipt']).read_text())
p = Path(normal['executable'])
assert sha(p) == normal['executable_sha256'] and p.stat().st_mode & 0o7777 == 0o700
assert normal['actual_Cargo_artifact']['target']['kind'] == ['example']
assert normal['actual_Cargo_artifact']['profile']['test'] is False
spec = importlib.util.spec_from_file_location('frozen_temporal82_source_guard', SOURCE / cfg['driver_relative_path'])
module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(module)
OUT.mkdir(mode=0o700)
guard = module.Run(cfg, OUT, B / 'temporal82-private-not-created')
before = guard.source_guard()
assert len(before) == 73196
name = 'signed-stable-java-4-1-2-temporal82-revised-01'
path = OUT / (name + '.json')
path.write_text(json.dumps(cfg, separators=(',', ':')) + '\n')
os.chmod(path, 0o600)
assert path.stat().st_size <= 65536
after = guard.source_guard()
assert before == after and ORIGINAL.read_bytes() == original_bytes
output = B / 'live-runs-fcac9d1d-java411-temporal82-01' / name
private = B / 'private-live-fcac9d1d-java411-temporal82-01' / name
assert not output.exists() and not private.exists()
temporal = [r for r in cfg['steps'] if r.get('authority_witness', {}).get('policy') in ['expired', 'future_nbf']]
assert len(temporal) == 2
assert all(r['operation'] == 'send' and r['expected_pass'] is False and r['authority_witness']['expected_rejection_layer'] == 'broker-authentication' for r in temporal)
missing = [r for r in cfg['steps'] if r.get('authority_witness', {}).get('policy') == 'missing_subject']
assert len(missing) == 1 and missing[0]['operation'] == 'start-peer' and missing[0]['expected_ready'] is False
row = {
    'name': name, 'source_sha': cfg['source_sha'], 'profile': 'metadata',
    'config': str(path), 'config_sha256': sha(path), 'config_bytes': path.stat().st_size,
    'config_mode': oct(path.stat().st_mode & 0o7777), 'steps': 82,
    'recipe': str(RECIPE), 'recipe_sha256': sha(RECIPE),
    'original_config': str(ORIGINAL), 'original_config_sha256': hashlib.sha256(original_bytes).hexdigest(),
    'original_config_unchanged': True, 'original32_recipes_not_replaced': True,
    'runtime_rehashes': runtime_count, 'normal_binding': cfg['normal_example_build_receipt'],
    'normal_binding_sha256': cfg['normal_example_build_receipt_sha256'],
    'source_guard': {'files': len(before), 'before_equals_after': True, 'full07777_and_exact_pathset': True},
    'fresh_output_private_paths': True,
    'outer_command': ['taskset', '-c', '0,1', sys.executable, str(SOURCE / cfg['outer_relative_path']), '--config', str(path), '--output', str(output), '--private-scratch', str(private)],
    'temporal_negative_witnesses': temporal, 'missing_subject_local_constructor_unchanged': True,
    'execution': 'WORK-only configuration preparation; NO live/Cargo. Root runtime GO required after Avro exclusive CPU0,1 lease and fresh disk/source/artifact forecast.',
    'root_review_basis': '5fc22 recipe approved for config preparation; focused12 expired actual broker-authentication PASS remains a separate cohort.',
    'free_bytes': shutil.disk_usage('/workspace').free,
}
p = OUT / 'manifest.json'
p.write_text(json.dumps(row, indent=2) + '\n')
os.chmod(p, 0o600)
print(json.dumps({'manifest': str(p), 'sha256': sha(p), 'config_sha256': sha(path), 'config_bytes': path.stat().st_size, 'steps': 82, 'runtime_rehashes': runtime_count, 'source_files': len(before)}))

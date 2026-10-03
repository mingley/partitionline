from pathlib import Path
import gzip, hashlib, json, os, shutil

B = Path('/workspace/work/broker-oidc')
LIVE = B / 'live-runs-fcac9d1d-expired-focused-01/expired-stable-java-4-1-2-focused-01'
SOURCE = Path('/workspace/work/broker-merged-source-fcac9d1d')
CONFIG = B / 'live-configs-fcac9d1d-expired-focused-01/expired-stable-java-4-1-2-focused-01.json'
PREFIX = Path('docs/evidence/broker/KL11-37/peers/driver/live-results/expired-java411-focused12-fcac9d1d-01')
DEST = B / 'focused-expired12-public-stage-01'

def sha(p):
    return hashlib.sha256(p.read_bytes()).hexdigest()

def write(p, obj):
    assert not p.exists()
    p.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    p.write_text(json.dumps(obj, indent=2) + '\n')
    os.chmod(p, 0o600)

assert not DEST.exists()
DEST.mkdir(mode=0o700)
cfg = json.loads(CONFIG.read_text())
assert sha(CONFIG) == '2ca85767c92c4097be8c08c315cd91f856abf803c8ad0c51662bbf9627365d4a'
v = json.loads((LIVE / 'validation.json').read_text())
outer = json.loads((LIVE / 'outer-ownership.json').read_text())
modes = json.loads((LIVE / 'private-mode-inventory.json').read_text())
events = json.loads((LIVE / 'events.json').read_text())
assert v['passed'] and len(v['steps']) == 12 and all(r['passed'] for r in v['steps'])
assert v['complete_source_unchanged'] and not v['phase_dispatch_denial_proved']
assert outer['passed'] and outer['driver_exit_code'] == 0 and not outer['remaining']
assert not outer['forced_cleanup'] and not outer['unverified_ownership'] and not outer['timed_out']
assert all(r['joined'] and not r.get('forced_close', False) for r in v['cleanup'])
assert modes['passed'] and modes['path_count'] == 23
assert all(r['driver_sequence'] == i for i, r in enumerate(events))
queries = {r['phase']: r for r in events if r.get('event') == 'query'}
assert queries['expired_baseline']['passed'] and queries['expired_recovered']['passed']
assert not queries['expired']['passed']
assert queries['expired']['error_types'] == ['org.apache.kafka.common.errors.SaslAuthenticationException']
witness = [r for r in events if r.get('event') == 'negative-causal-witness']
assert len(witness) == 1 and witness[0]['condition'] == 'signed-policy'
assert witness[0]['observed_rejection_layer'] == 'broker-authentication'
ready = [r for r in events if r.get('event') == 'ready' and r.get('actor') == 'java-4-1-2']
assert len(ready) == 3 and all(r['callback']['token_success'] == 1 for r in ready)
statuses = [r for r in events if r.get('event') == 'process-cleanup-status']
assert len(statuses) == 4
assert all(r['exit_code'] == 0 and not r['forced_close'] and not r['stdout_receipt_rejected'] and not r['stderr_retained'] for r in statuses)

runtime = []
for peer in cfg['peers']:
    p = Path(peer['build_receipt'])
    assert sha(p) == peer['build_receipt_sha256']
    receipt = json.loads(p.read_text())
    assert receipt['source_sha'] == cfg['source_sha'] and receipt['exit_code'] == 0
    for row in peer['runtime_inputs']:
        p = Path(row['path'])
        assert sha(p) == row['sha256'] and p.stat().st_size == row['bytes']
        assert oct(p.stat().st_mode & 0o7777) == row['full_mode']
        runtime.append(row)
    for row in peer['source_inputs']:
        assert sha(SOURCE / row['path']) == row['sha256']
for name in ['immutable_origin', 'normal_example_build']:
    p = Path(cfg[name + '_receipt'])
    assert sha(p) == cfg[name + '_receipt_sha256']
normal = json.loads(Path(cfg['normal_example_build_receipt']).read_text())
p = Path(normal['executable'])
assert sha(p) == normal['executable_sha256'] and p.stat().st_mode & 0o7777 == 0o700
assert normal['actual_Cargo_artifact']['target']['kind'] == ['example']
assert normal['actual_Cargo_artifact']['profile']['test'] is False
for name in ['driver', 'outer', 'issuer']:
    assert sha(SOURCE / cfg[name + '_relative_path']) == cfg[name + '_sha256']
old_manifest = B / 'live-configs-fcac9d1d-predeclared-01/manifest.json'
assert sha(old_manifest) == '0639fc4f66058a657d14f45db113347e451efc62a60f0e1d47953d75d8e45515'
originals = json.loads(old_manifest.read_text())['cohorts']
assert len(originals) == 32
for row in originals:
    assert sha(Path(row['config'])) == row['config_sha256']

restore = json.loads((LIVE / 'complete-source-identity.restore.json').read_text())
compressed = LIVE / restore['compressed_path']
assert sha(compressed) == restore['compressed_sha256']
raw = gzip.decompress(compressed.read_bytes())
assert len(raw) == restore['original_bytes']
assert hashlib.sha256(raw).hexdigest() == restore['original_sha256']
source = json.loads(raw)
assert source['identical_before_after'] and len(source['files']) == 73196
assert source['source_sha'] == cfg['source_sha']
issued = json.loads((LIVE / 'issued-public-metadata.json').read_text())
tokens = [r for r in issued if r['policy'] == 'expired']
assert len(tokens) == 1
authority = json.loads((LIVE / 'issuer-events.json').read_text())
acquisitions = [r for r in authority if r['endpoint'] == '/issuer/token' and r['status'] == 200]
assert len(acquisitions) == 3
expired = tokens[0]
acquisition_epoch = acquisitions[1]['epoch']
assert acquisition_epoch - expired['expires_epoch'] == 60

rows = []
def copy(p, relative):
    assert p.is_file() and not p.is_symlink()
    d = DEST / relative
    d.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    assert not d.exists()
    s = p.stat()
    h = sha(p)
    shutil.copy2(p, d)
    assert sha(d) == h and d.stat().st_mode & 0o7777 == s.st_mode & 0o7777
    rows.append({'path': str(PREFIX / relative), 'prepared_path': str(d), 'sha256': h, 'bytes': s.st_size, 'full_mode': oct(s.st_mode & 0o7777), 'original_path': str(p)})

for p in sorted(LIVE.iterdir()):
    copy(p, Path('raw') / p.name)
for p in [CONFIG, CONFIG.parent / 'manifest.json', B / 'focused-expired12-launch-preflight.json', B / 'focused-expired12-outer.log', B / 'materialize-focused-expired-config-01.py', B / 'audit-private-modes.py', Path(__file__), B / 'signed-expired-recipe-revision-01/expired-stable-java-4-1-2-focused-01.recipe.json']:
    copy(p, Path('preparation') / p.name)
summary = {
    'source_sha': cfg['source_sha'], 'passed': True, 'actual_steps': 12,
    'elapsed_seconds': outer['elapsed_seconds'], 'broker_verdict': 'actual Java metadata query rejected with SaslAuthenticationException; public Admin and Consumer failed-authentication counters each 1',
    'provider_verdict': 'all three fresh Java peers READY/token_success=1; local constructor accepts the expired token, separately from broker rejection',
    'positive_baseline_and_recovery': True, 'negative_rejection_layer': 'broker-authentication',
    'expired_authority_metadata': expired, 'expired_acquisition_public_event': acquisitions[1],
    'expired_seconds_before_actual_acquisition': acquisition_epoch - expired['expires_epoch'],
    'authority_link_limit': 'Token hashes are public metadata only. Endpoint events omit token hashes; the unique expired row is linked by fixed issuer policy, sequential fresh-peer issuance boundaries, and one-second timestamp granularity. No private token was read or hashed for this evidence.',
    'source_whole_files_before_after': 73196, 'runtime_inputs_rehashed_after': len(runtime),
    'runtime_bindings_match_original': True, 'original_32_config_hashes_unchanged': True,
    'processes_joined_unforced': len(statuses), 'logical_broker_issuer_joined': True,
    'outer_confirmed_identities': outer['confirmed_identity_count'], 'outer_remaining': [],
    'private_paths_only_audit': {'passed': True, 'path_count': modes['path_count'], 'sha256': sha(LIVE / 'private-mode-inventory.json')},
    'complete_source_gzip_roundtrip': {'passed': True, 'raw_bytes': len(raw), 'raw_sha256': restore['original_sha256']},
    'raw_source_mode_limit': 'Raw JSON was serialized in memory and compressed directly. Restore mode600 is designated policy; source row full07777 is measured.',
    'phase_dispatch_denial_proved': False,
    'scope': 'Focused actual HTTPS acquisition + metadata6 socket expiry denial/recovery on stable broker and official Java4.1.2. No other signed-policy/SDK/whole-Kafka/reauth claim.',
    'original_failed_signed_cohort': 'Original 32 recipes/configs and 3PASS1FAIL observations remain unchanged; the original constructor-expectation failure issued no expired query. This additive cohort closes that specific broker-verdict gap.',
    'free_bytes_after': shutil.disk_usage('/workspace').free,
}
p = DEST / 'VALIDATION.json'
write(p, summary)
rows.append({'path': str(PREFIX / p.name), 'prepared_path': str(p), 'sha256': sha(p), 'bytes': p.stat().st_size, 'full_mode': '0o600', 'original_path': 'additive public validation'})
p = DEST / 'SHA256SUMS'
p.write_text(''.join(r['sha256'] + '  ' + str(Path(r['path']).relative_to(PREFIX)) + '\n' for r in rows))
os.chmod(p, 0o600)
rows.append({'path': str(PREFIX / p.name), 'prepared_path': str(p), 'sha256': sha(p), 'bytes': p.stat().st_size, 'full_mode': '0o600', 'original_path': 'publication checksum manifest'})
packet = B / 'focused-expired12-stage-packet-01.json'
write(packet, {'source_sha': cfg['source_sha'], 'scope': 'Public text/gzip proof preparation only; no repository mutation, live execution or private payload', 'files': rows, 'count': len(rows), 'bytes': sum(r['bytes'] for r in rows), 'destination_prefix': str(PREFIX)})
print(json.dumps({'packet': str(packet), 'sha256': sha(packet), 'count': len(rows), 'bytes': sum(r['bytes'] for r in rows), 'validation_sha256': sha(DEST / 'VALIDATION.json')}))

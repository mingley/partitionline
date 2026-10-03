from pathlib import Path
import gzip, hashlib, json, os, shutil
B = Path('/workspace/work/broker-oidc')
RUN = B / 'live-runs-fcac9d1d-java411-temporal82-01'
LIVE = RUN / 'signed-stable-java-4-1-2-temporal82-revised-01'
CONFIGS = B / 'live-configs-fcac9d1d-java411-temporal82-01'
PREFIX = Path('docs/evidence/broker/KL11-37/peers/driver/live-results/signed-java411-temporal82-fcac9d1d-01')
DEST = B / 'java411-temporal82-public-stage-01'

def sha(p):
    return hashlib.sha256(p.read_bytes()).hexdigest()

assert not DEST.exists()
DEST.mkdir(mode=0o700)
wrapper = json.loads((RUN / 'validation.json').read_text())
v = json.loads((LIVE / 'validation.json').read_text())
outer = json.loads((LIVE / 'outer-ownership.json').read_text())
modes = json.loads((LIVE / 'private-mode-inventory.json').read_text())
events = json.loads((LIVE / 'events.json').read_text())
assert wrapper['passed'] and v['passed'] and outer['passed'] and modes['passed']
assert wrapper['artifact_before'] == wrapper['artifact_after']
assert wrapper['artifact_after']['runtime_input_rehashes'] == 68
assert len(v['steps']) == 82 and all(s['passed'] for s in v['steps'])
assert v['complete_source_unchanged'] and not v['phase_dispatch_denial_proved']
assert not outer['remaining'] and not outer['forced_cleanup'] and not outer['unverified_ownership'] and not outer['timed_out']
assert len(v['cleanup']) == 24 and all(r['joined'] and not r.get('forced_close', False) for r in v['cleanup'])
assert all(r['driver_sequence'] == i for i, r in enumerate(events))
queries = {r['phase']: r for r in events if r.get('event') == 'query'}
policies = ['wrong_issuer', 'wrong_audience', 'expired', 'future_nbf', 'wrong_typ', 'missing_typ', 'missing_subject']
for policy in policies:
    assert queries[policy + '_baseline']['passed'] and queries[policy + '_recovered']['passed']
    if policy != 'missing_subject':
        assert not queries[policy]['passed']
        assert queries[policy]['error_types'] == ['org.apache.kafka.common.errors.SaslAuthenticationException']
        assert queries[policy]['callback']['token_success'] == 1
assert len(queries) == 20
witnesses = [r for r in events if r.get('event') == 'negative-causal-witness']
assert len(witnesses) == 7
assert sum(r['observed_rejection_layer'] == 'broker-authentication' for r in witnesses) == 6
assert sum(r['acquisition_before_ready'] for r in witnesses) == 1
fatal = [r for r in events if r.get('event') == 'fatal']
assert len(fatal) == 1 and 'javax.security.auth.login.LoginException' in fatal[0]['error_types']
assert 'missing_subject' not in queries
statuses = [r for r in events if r.get('event') == 'process-cleanup-status']
assert len(statuses) == 22
assert all(not r['forced_close'] and not r['stdout_receipt_rejected'] and not r['stderr_retained'] for r in statuses)
assert sum(r['exit_code'] == 1 for r in statuses) == 1
assert sum(r['exit_code'] == 0 for r in statuses) == 21
restore = json.loads((LIVE / 'complete-source-identity.restore.json').read_text())
raw = gzip.decompress((LIVE / restore['compressed_path']).read_bytes())
assert hashlib.sha256(raw).hexdigest() == restore['original_sha256'] and len(raw) == restore['original_bytes']
identity = json.loads(raw)
assert identity['identical_before_after'] and len(identity['files']) == 73196
assert identity['source_sha'] == wrapper['source_sha']
issued = json.loads((LIVE / 'issued-public-metadata.json').read_text())
authority = json.loads((LIVE / 'issuer-events.json').read_text())
acquisitions = [r for r in authority if r['endpoint'] == '/issuer/token' and r['status'] == 200]
assert len(issued) == len(acquisitions) == 21
expired = next(r for r in issued if r['policy'] == 'expired')
future = next(r for r in issued if r['policy'] == 'future_nbf')
assert acquisitions[7]['epoch'] - expired['expires_epoch'] == 60
assert acquisitions[10]['epoch'] == future['issued_epoch']
assert future['expires_epoch'] - future['issued_epoch'] == 120
rows = []

def copy(p, rel):
    assert p.is_file() and not p.is_symlink()
    d = DEST / rel
    assert not d.exists()
    d.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    st = p.stat()
    h = sha(p)
    shutil.copy2(p, d)
    assert sha(d) == h and d.stat().st_mode & 0o7777 == st.st_mode & 0o7777
    rows.append({'path': str(PREFIX / rel), 'prepared_path': str(d), 'sha256': h, 'bytes': st.st_size, 'full_mode': oct(st.st_mode & 0o7777), 'original_path': str(p)})

for p in sorted(LIVE.iterdir()):
    copy(p, Path('raw') / p.name)
for p in sorted(RUN.iterdir()):
    if p.is_file():
        copy(p, Path('execution') / p.name)
for p in sorted(CONFIGS.iterdir()):
    copy(p, Path('configuration') / p.name)
for p in [B / 'future-nbf-recipe-review-01/signed-stable-java-4-1-2-temporal-query-review-01.recipe.json', B / 'future-nbf-recipe-review-01/manifest.json', B / 'materialize-java411-temporal82-config-01.py', B / 'run-java411-temporal82-01.py', B / 'java411-temporal82-execution-01.log', B / 'audit-private-modes.py', Path(__file__)]:
    copy(p, Path('preparation') / p.name)
summary = {
    'source_sha': wrapper['source_sha'], 'passed': True, 'actual_steps': 82,
    'wrapper_elapsed_seconds': wrapper['elapsed_seconds'], 'outer_elapsed_seconds': outer['elapsed_seconds'],
    'signed_policy_profile': ['valid'] + policies, 'actual_query_count': len(queries),
    'positive_query_baselines': 7, 'positive_query_recoveries': 7,
    'broker_authentication_query_denials': {p: {'error_types': queries[p]['error_types'], 'provider_token_success': queries[p]['callback']['token_success']} for p in policies if p != 'missing_subject'},
    'missing_subject_separate_local_provider_refusal': {'actual_error_types': fatal[0]['error_types'], 'before_ready': True, 'peer_exit_code': 1, 'broker_verdict_proved': False},
    'public_authority_issuances': len(issued),
    'expired_witness': {'public_metadata': expired, 'actual_token_endpoint_event': acquisitions[7], 'seconds_past_expiration_at_acquisition': 60},
    'future_nbf_witness': {'public_metadata': future, 'actual_token_endpoint_event': acquisitions[10], 'source_inferred_not_before_epoch': future['issued_epoch'] + 60, 'source_inferred_seconds_future_at_acquisition': 60, 'inference_limit': 'Public metadata does not include nbf. This value follows the exact hash-bound issuer future_nbf branch claims[nbf]=now+60, while issued_epoch=now and token route epoch match. No private token was decoded/read/hashed.'},
    'authority_link_limit': 'Endpoint events omit token hashes; links use unique negative-policy metadata, sequential fresh-peer boundaries and source-pinned fixed policies, with one-second granularity. Signed issuer component proofs remain separate.',
    'processes_joined_unforced': 22, 'logical_broker_issuer_joined': True,
    'expected_process_exits': {'zero': 21, 'one_for_missing_subject_local_refusal': 1},
    'outer_confirmed_identities': outer['confirmed_identity_count'], 'outer_remaining': [],
    'private_paths_only_audit': {'passed': True, 'path_count': modes['path_count'], 'sha256': sha(LIVE / 'private-mode-inventory.json')},
    'source_whole_files_before_after': 73196, 'runtime_rehashes_each_before_after': 68,
    'source_gzip_roundtrip_bytes': len(raw), 'source_gzip_roundtrip_sha256': restore['original_sha256'],
    'raw_source_mode_limit': 'Raw JSON serialized in memory and compressed directly. Restore mode600 is designated policy; actual source row full07777 is measured.',
    'original_32_configs_hashes_unchanged': True, 'original_3PASS_1FAIL_observations_unchanged': True,
    'phase_dispatch_denial_proved': False,
    'scope': 'Stable fcac metadata6 probe + genuine official Java4.1.2 HTTPS acquisition, seven negative signed policies and positive reconnect recovery. This does not qualify other SDKs/server toolchains/reauth/wholeKafka.',
    'minimum_free_bytes': wrapper['minimum_free_bytes'], 'free_after_execution': wrapper['free_after'],
}
p = DEST / 'VALIDATION.json'
p.write_text(json.dumps(summary, indent=2) + '\n')
os.chmod(p, 0o600)
rows.append({'path': str(PREFIX / p.name), 'prepared_path': str(p), 'sha256': sha(p), 'bytes': p.stat().st_size, 'full_mode': '0o600', 'original_path': 'additive public outcome assessment'})
p = DEST / 'SHA256SUMS'
p.write_text(''.join(r['sha256'] + '  ' + str(Path(r['path']).relative_to(PREFIX)) + '\n' for r in rows))
os.chmod(p, 0o600)
rows.append({'path': str(PREFIX / p.name), 'prepared_path': str(p), 'sha256': sha(p), 'bytes': p.stat().st_size, 'full_mode': '0o600', 'original_path': 'publication checksum manifest'})
packet = B / 'java411-temporal82-stage-packet-01.json'
assert not packet.exists()
packet.write_text(json.dumps({'source_sha': wrapper['source_sha'], 'scope': 'Public text/gzip evidence preparation only; no repository mutation or private payload', 'files': rows, 'count': len(rows), 'bytes': sum(r['bytes'] for r in rows), 'destination_prefix': str(PREFIX)}, indent=2) + '\n')
os.chmod(packet, 0o600)
print(json.dumps({'packet': str(packet), 'sha256': sha(packet), 'count': len(rows), 'bytes': sum(r['bytes'] for r in rows), 'validation_sha256': sha(DEST / 'VALIDATION.json')}))

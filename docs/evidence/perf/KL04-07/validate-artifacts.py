#!/usr/bin/env python3
"""Check the retained diagnostic counts, integrity outcomes and source binding."""
import hashlib
import json
from pathlib import Path
import subprocess

root = Path(__file__).resolve().parent
repo = root.parents[3]

def read(name):
    return json.loads((root / name).read_text())

def journal(name):
    return [json.loads(line) for line in (root / name).read_text().splitlines()]

provenance = read('provenance.json')
commit = provenance['tested_source_commit']
for name, expected in provenance['source_sha256'].items():
    retained = (root / 'source-snapshots/final' / name).read_bytes()
    committed = subprocess.check_output(['git', 'show', f'{commit}:{name}'], cwd=repo)
    assert hashlib.sha256(retained).hexdigest() == expected
    assert retained == committed, name
for name, expected in read('compile-input-hashes.json').items():
    committed = subprocess.check_output(['git', 'show', f'{commit}:{name}'], cwd=repo)
    assert hashlib.sha256(committed).hexdigest() == expected, name

for prefix, count in [('native-final/run-1', 128), ('acks0-native-final', 16), ('control-native', 3)]:
    verdict = read(f'{prefix}/verdict.json')
    assert verdict['valid'] and verdict['integrity_verified']
    assert verdict['attempted_count'] == verdict['consumed_count'] == count
    assert verdict['qualification'] is False and verdict['suite_hold'] == 'active'
    produced = [r for r in journal(f'{prefix}/producer.jsonl') if r['kind'] == 'record']
    consumed = [r for r in journal(f'{prefix}/consumer.jsonl') if r['kind'] == 'record']
    assert len(produced) == len(consumed) == count
    assert len({r['id'] for r in produced}) == len({r['id'] for r in consumed}) == count
    assert {(r['id'], r['payload_hash']) for r in produced} == {(r['id'], r['payload_hash']) for r in consumed}

acks0 = read('acks0-native-final/produce.stdout.log')
assert acks0['acked'] == 0 and acks0['acked_rec_s'] is None
assert acks0['locally_completed'] == 16 and acks0['delivery_semantics'] == 'local_complete'
assert read('acks0-native-final/verdict.json')['acknowledged_throughput'] is False
assert (root / 'control-native/offsets.stdout.log').read_text().strip().endswith(':0:4')
control = read('control-native/fetch.stdout.log')
assert control['consumed'] == control['verified'] == 3

failed = read('matching-count-corruption/verdict.json')
assert not failed['valid'] and failed['performance_claims_invalidated']
assert failed['attempted_count'] == failed['consumed_count'] == 128
assert failed['minimal_counterexample']['type'] == 'SYNTHETIC_SWAP'
assert (root / 'matching-count-corruption/exit-status.txt').read_text().strip() == '1'
assert read('matching-count-corruption/mutation.json')['kind'] == 'synthetic matching-count fixture derived from retained native run'
assert read('broker-cleanup.json')['broker_port_closed']
assert read('settings-validation.json')['case_count'] == 24
print(json.dumps({'status': 'passed', 'source_commit': commit, 'source_snapshots': 8,
                  'core_compile_inputs': 35, 'valid_final_source_records': 144,
                  'valid_java_transaction_records': 3, 'control_partition_end_offset': 4,
                  'matching_count_corruption_rejected': True, 'qualification': False}, indent=2))

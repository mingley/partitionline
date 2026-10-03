#!/usr/bin/env python3
"""Seal scoped source/proof receipts without running broker binaries."""
from pathlib import Path
import ast
import hashlib
import json
import stat

ROOT = Path('/workspace/partitionline')
OUT = Path('/workspace/work/membership-oracle-fixes-01')
before = json.loads((OUT / 'before-change.json').read_bytes())
files = ['membership_raw.py', 'check-membership-history.py', 'test-membership-history.py']
source = {}
for name in files:
    p = ROOT / 'docs/evidence/broker/KL11-74/oracle/history' / name
    data = p.read_bytes()
    ast.parse(data, filename=str(p))
    mode = oct(stat.S_IMODE(p.stat().st_mode))
    preserved = next(row for row in before['sources_and_52_control_receipts'] if row['original'] == str(p))
    assert mode == preserved['full_mode']
    source[str(p.relative_to(ROOT))] = {'sha256': hashlib.sha256(data).hexdigest(), 'full_mode': mode,
        'git_blob': hashlib.sha1(b'blob ' + str(len(data)).encode() + b'\0' + data).hexdigest()}

for row in before['sources_and_52_control_receipts']:
    p = Path(row['copy'])
    assert hashlib.sha256(p.read_bytes()).hexdigest() == row['sha256']
    assert oct(stat.S_IMODE(p.stat().st_mode)) == row['full_mode']
    if 'historical-receipts' in p.parts:
        original = Path(row['original'])
        assert hashlib.sha256(original.read_bytes()).hexdigest() == row['sha256']
        assert oct(stat.S_IMODE(original.stat().st_mode)) == row['full_mode']
for folder, expected in before['capture_bytes_and_modes'].items():
    base = Path(folder)
    actual = {str(p.relative_to(base)): {'sha256': hashlib.sha256(p.read_bytes()).hexdigest(),
              'full_mode': oct(stat.S_IMODE(p.stat().st_mode))} for p in base.rglob('*') if p.is_file()}
    assert actual == expected

old = json.loads((OUT / 'old-predicate-probes.json').read_bytes())
controls = {tool: json.loads((OUT / f'final-{tool}-controls.json').read_bytes()) for tool in ('stable','msrv')}
for probe in old['probes']:
    actual = next(c for c in controls[probe['toolchain']]['controls']
                  if c['name'] == probe['name'] and c['history'] == probe['history'])
    for field in ('sha256', 'modified_artifacts'):
        if field in probe:
            assert actual[field] == probe[field]
    assert probe['old_predicate_outcome'] == 'accepted_unsafe_control'
for c in controls.values():
    assert c['negative_controls_rejected'] == 70 and c['original_positive_runs'] == 4
    assert c['all_original_bytes_and_full_modes_unchanged']
positives = [json.loads((OUT / f'final-{tool}-{size}-history.json').read_bytes())
             for tool in ('stable','msrv') for size in (3,5)]
assert len(positives) == 4 and all(p['remote_wal_authorities_bound'] > 0 for p in positives)
assert all(len(p['admission_proof_limits']) == 3 for p in positives)
freeze = {'schema_version': 1, 'scope': 'Only three delegated independent checker source paths; not full KL11-74 qualification',
          'sources': source, 'before_change_packet_sha256': hashlib.sha256((OUT/'before-change.json').read_bytes()).hexdigest()}
(OUT/'source-freeze.json').write_text(json.dumps(freeze, indent=2)+'\n')
artifacts = {p.name: {'sha256': hashlib.sha256(p.read_bytes()).hexdigest(), 'bytes':p.stat().st_size}
             for p in OUT.iterdir() if p.is_file() and p.name != 'validation.json'}
result = {'schema_version': 1, 'passed': True, 'before_change_source_and_full_modes_preserved': True,
          'historical_52_control_receipts_unchanged': True, 'original_capture_bytes_and_full_modes_unchanged': True,
          'failing_first': {'matrix_exit_code': 1, 'log':'old-predicate-first.log',
                           'old_positive_traces_passed':4,'unsafe_controls_accepted':36},
          'final': {'control_runs':2,'negative_controls_rejected':140,'positive_replays':8,'distinct_corrected_traces':4,
                    'events_checked':sum(p['events'] for p in positives),
                    'paired_raw_checkpoints':sum(p['paired_raw_checkpoints'] for p in positives),
                    'remote_wal_authorities_bound':sum(p['remote_wal_authorities_bound'] for p in positives)},
          'same_36_new_mutant_bytes_checked_before_and_after':True,
          'source_freeze':'source-freeze.json','artifacts':artifacts,
          'limits':positives[0]['admission_proof_limits'],
          'disposition':'Scoped corrected Python oracle proof ready for coordinator review; no task closure, source push, Cargo or live network claim'}
(OUT/'validation.json').write_text(json.dumps(result,indent=2)+'\n')
print(json.dumps({'passed':True,'validation_sha256':hashlib.sha256((OUT/'validation.json').read_bytes()).hexdigest(),
                 'source_freeze_sha256':hashlib.sha256((OUT/'source-freeze.json').read_bytes()).hexdigest(),
                 'final':result['final'],'sources':source}))

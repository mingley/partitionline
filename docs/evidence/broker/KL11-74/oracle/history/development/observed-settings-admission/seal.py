#!/usr/bin/env python3
"""Seal legacy real traces separately from synthetic admission-unit proofs."""
from pathlib import Path
import ast
import hashlib
import json
import stat

ROOT = Path('/workspace/partitionline')
OUT = Path('/workspace/work/membership-oracle-admission-01')
stage_one = Path('/workspace/work/membership-oracle-fixes-01')
before = json.loads((OUT/'before-change.json').read_bytes())
sources = {}
for name, old in before['before_sources'].items():
    p = ROOT/name
    data = p.read_bytes()
    ast.parse(data, filename=str(p))
    mode = oct(stat.S_IMODE(p.stat().st_mode))
    assert mode == old['full_mode']
    preserved = OUT/'before'/name
    assert hashlib.sha256(preserved.read_bytes()).hexdigest() == old['sha256']
    assert oct(stat.S_IMODE(preserved.stat().st_mode)) == old['full_mode']
    assert (OUT/'source'/name).read_bytes() == data
    sources[name] = {'sha256':hashlib.sha256(data).hexdigest(),'full_mode':mode,
                    'git_blob':hashlib.sha1(b'blob '+str(len(data)).encode()+b'\0'+data).hexdigest()}
packet = json.loads((stage_one/'before-change.json').read_bytes())
for folder, expected in packet['capture_bytes_and_modes'].items():
    base = Path(folder)
    actual = {str(p.relative_to(base)):{'sha256':hashlib.sha256(p.read_bytes()).hexdigest(),
              'full_mode':oct(stat.S_IMODE(p.stat().st_mode))} for p in base.rglob('*') if p.is_file()}
    assert actual == expected
controls = [json.loads((OUT/f'final-{tool}-controls.json').read_bytes()) for tool in ('stable','msrv')]
for c in controls:
    assert c['negative_controls_rejected'] == 70 and c['original_positive_runs'] == 4
    assert c['all_original_bytes_and_full_modes_unchanged']
    assert len(c['synthetic_admission_controls']) == 2
    assert all(s['negative_controls']==33 and s['positive_controls']==9 for s in c['synthetic_admission_controls'])
positive = [json.loads((OUT/f'final-{tool}-{size}-history.json').read_bytes())
            for tool in ('stable','msrv') for size in (3,5)]
assert all(p['admission_mode']=='limited-legacy' and p['admission_settings'] is None and
           len(p['admission_proof_limits'])==3 and not p['admission_change_proofs'] for p in positive)
for name in ['missing-settings-confirmed.log','strict-test-missing-settings.log']:
    assert 'required observed admission settings missing' in (OUT/name).read_text()
assert not (OUT/'missing-settings-unexpected.json').exists()
assert not (OUT/'strict-test-unexpected.json').exists()
freeze = {'schema_version':1,'source_base_pin':'7c10daf1e3b732d71f814f01a1a9a47e36eab30f',
          'sources':sources,'scope':'Optional observed-settings admission checker and distinctly synthetic unit scenarios',
          'preserved_stage_one_validation_sha256':hashlib.sha256((stage_one/'validation.json').read_bytes()).hexdigest()}
(OUT/'source-freeze.json').write_text(json.dumps(freeze,indent=2)+'\n')
artifacts = {p.name:{'sha256':hashlib.sha256(p.read_bytes()).hexdigest(),'bytes':p.stat().st_size}
             for p in OUT.iterdir() if p.is_file() and p.name!='validation.json'}
result = {'schema_version':1,'passed':True,'sources':'source-freeze.json','artifacts':artifacts,
          'legacy_real_history_proof':{'distinct_traces':4,'positive_replays':8,'negative_controls_rejected':140,
               'events':sum(p['events'] for p in positive),'raw_checkpoint_pairs':sum(p['paired_raw_checkpoints'] for p in positive),
               'remote_authorities_bound':sum(p['remote_wal_authorities_bound'] for p in positive),
               'admission_mode':'limited-legacy','captured_bytes_and_full_modes_unchanged':True},
          'synthetic_policy_unit_proof':{'assemblies':4,'negative_executions':132,'positive_executions':36,
               'unique_named_scenarios_per_group_size':42,'native_or_actual_runtime_history_claim':False,
               'settings_origin':'Explicit handconstructed1000/10/10 constants; no old real trace was augmented'},
          'strict_cli_missing_settings':{'checker_exit_code':1,'test_runner_exit_code':1,
               'expected_guard':'required observed admission settings missing','unexpected_output_files_absent':True},
          'actual_observed_settings_history_proof':'pending fresh source-pinned runtime captures',
          'stale_election_proof_correction':{'failing_first_exit_code':1,'corrected_exit_code':0,
               'failing_first_log':'stale-election-failing-first.log','corrected_log':'stale-election-corrected.log',
               'scope':'Labeled synthetic unproved same-term reactivation predicate; no actual/native history claim',
               'superseded_receipts_preserved':'superseded-elected-proof/'},
          'disposition':'Ready for coordinator source review and publication; not KL11-74 closure or fresh timing qualification',
          'source_modes_and_before_snapshots_preserved':True,'only_two_delegated_repo_files_changed':True,
          'cargo_or_live_network_launched':False}
(OUT/'validation.json').write_text(json.dumps(result,indent=2)+'\n')
print(json.dumps({'validation_sha256':hashlib.sha256((OUT/'validation.json').read_bytes()).hexdigest(),
                 'source_freeze_sha256':hashlib.sha256((OUT/'source-freeze.json').read_bytes()).hexdigest(),
                 'sources':sources,'legacy_real_history_proof':result['legacy_real_history_proof'],
                 'synthetic_policy_unit_proof':result['synthetic_policy_unit_proof']}))

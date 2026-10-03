#!/usr/bin/env python3
"""Root's independent replay/control job; no broker, SDK or Cargo children."""
import copy
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import stat
import sys

sys.dont_write_bytecode = True
BASE = Path(__file__).parent
VERIFIER = BASE / 'source/verify-live-v3.py'
EXPECTED = '38a51db2009b0387b7453928fed2e9c0881290db74cbc010dde44a71edf328cd'
assert hashlib.sha256(VERIFIER.read_bytes()).hexdigest() == EXPECTED
spec = importlib.util.spec_from_file_location('independent_v3', VERIFIER)
v = importlib.util.module_from_spec(spec)
spec.loader.exec_module(v)
LANE = Path('/workspace/work/compaction-public-403be1e3/mixed-f0ca05da/live/stable-default/attempt-1/validation.json')
assert hashlib.sha256(LANE.read_bytes()).hexdigest() == 'a09dedb95cfe06d5ed17c3f5a3a26e40186ae2cf42229eea1e41f6d27d55ee69'

def identity(p):
    return {'sha256': hashlib.sha256(p.read_bytes()).hexdigest(), 'bytes': p.stat().st_size,
            'mode': stat.S_IMODE(p.stat().st_mode)}

def guard():
    return {str(p): identity(p) for p in sorted(LANE.parent.rglob('*')) if p.is_file()}

before = guard()
checker_before = {str(p): identity(p) for p in (VERIFIER, Path(__file__))}
lane = v.check_lane(LANE, '403be1e3db073df86921d6fb21189f695c4f1eaf',
                    'f0ca05da9c04942b211e2f56ddc615b9860cdea2')
raw = v.read(LANE)
first = {}
for row in raw['results']:
    if row['operation'] == 'read' and row['producer'] == 'j412' and row['stage'] == 'initial':
        first[row['reader']] = v.read(Path(row['report']))
controls = []

def mutate(name, reader, change):
    peer = copy.deepcopy(first[reader])
    change(peer)
    try:
        v.check_read(peer, reader, 'j412', 'initial')
    except (ValueError, KeyError, TypeError) as error:
        controls.append({'name': name, 'rejected': True, 'reason': str(error)})
    else:
        raise AssertionError('Negative accepted: ' + name)

def record(peer):
    return next(e for e in peer['history'] if e['label'] == 'public-consumer-record')

mutate('rust-missing-digest', 'rust', lambda p: record(p).pop('record_sha256'))
mutate('rust-wrong-digest', 'rust', lambda p: record(p).update(record_sha256='0' * 64))
mutate('rust-boolean-digest', 'rust', lambda p: record(p).update(record_sha256=True))
mutate('rust-null-digest', 'rust', lambda p: record(p).update(record_sha256=None))
mutate('rust-truncated-digest', 'rust', lambda p: record(p).update(record_sha256='a' * 63))
mutate('rust-extra-field', 'rust', lambda p: record(p).update(unreviewed=0))
mutate('rust-boolean-offset', 'rust', lambda p: record(p)['record'].update(offset=False))
mutate('rust-changed-header', 'rust', lambda p: record(p)['record']['headers'][0].update(value_hex='62'))
mutate('rust-frontier-before-records', 'rust', lambda p: p['history'].insert(0, p['history'].pop(9)))
for reader in ('j412', 'j421', 'j431', 'native'):
    mutate(reader + '-unreviewed-digest', reader, lambda p: record(p).update(record_sha256='0' * 64))
mutate('native-missing-eof', 'native', lambda p: p['history'].remove(next(e for e in p['history'] if e['label'] == 'public-consumer-eof')))
mutate('native-wrong-eof', 'native', lambda p: next(e for e in p['history'] if e['label'] == 'public-consumer-eof').update(eof_offset=100))
mutate('native-boolean-count', 'native', lambda p: p.update(records=True))

assert guard() == before
assert {str(p): identity(p) for p in (VERIFIER, Path(__file__))} == checker_before
out = BASE / 'controls-and-actual-replay.json'
assert not out.exists()
out.write_text(json.dumps({'schema_version': 1, 'passed': True, 'scope': 'Independent one-cohort actual replay and synthetic malformed-history controls; remaining three cohorts unrun',
    'actual_new_SDK_or_broker_or_Cargo_executions': 0, 'actual_affinity': sorted(os.sched_getaffinity(0)),
    'verifier_sha256': EXPECTED, 'checker_inputs': checker_before, 'actual_lane': lane,
    'positive_actual_read_jobs': 150, 'negative_controls': controls,
    'actual_cohort_files_verified_before_after': len(before), 'all_cohort_file_bytes_and_full_modes_unchanged': True}, indent=2) + '\n')
out.chmod(0o600)
print(json.dumps({'passed': True, 'actual_jobs': 160, 'actual_read_jobs': 150,
                  'negative_controls': len(controls), 'receipt_sha256': identity(out)['sha256']}))

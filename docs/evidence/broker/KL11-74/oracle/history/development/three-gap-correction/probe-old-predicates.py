#!/usr/bin/env python3
"""Execute the new real-byte controls against preserved old checker sources."""
from pathlib import Path
import hashlib
import importlib.util
import json
import stat
import sys
import tempfile

ROOT = Path('/workspace/partitionline')
OUT = Path('/workspace/work/membership-oracle-fixes-01')
OLD = OUT / 'before/docs/evidence/broker/KL11-74/oracle/history'
sys.path.insert(0, str(OLD))
import membership_raw as raw

def load(name, path):
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module

old = load('old_history', OLD / 'check-membership-history.py')
tests = load('new_controls', ROOT / 'docs/evidence/broker/KL11-74/oracle/history/test-membership-history.py')
tests.history = old
probes, positives = [], []
for tool in ('stable', 'msrv'):
    captures = ROOT / f'docs/evidence/broker/KL11-74/runtime/development/corrected-complete-{tool}-tests/captures'
    for source in sorted(captures.glob('history-*/trace.json')):
        positives.append({'toolchain': tool, 'history': source.parent.name, 'result': old.verify(source)})
        trace = json.loads(source.read_bytes())
        with tempfile.TemporaryDirectory(prefix='old-predicates-', dir=OUT) as folder:
            for control in tests.supplementary_cases(source.parent, trace, Path(folder)):
                call = control.pop('call')
                try:
                    call()
                    control['old_predicate_outcome'] = 'accepted_unsafe_control'
                except raw.Rejected as error:
                    control['old_predicate_outcome'] = 'rejected'
                    control['old_guard'] = str(error)
                probes.append({'toolchain': tool, 'history': source.parent.name, **control})

packet = json.loads((OUT / 'before-change.json').read_bytes())
unchanged = True
for folder, expected in packet['capture_bytes_and_modes'].items():
    base = Path(folder)
    actual = {str(p.relative_to(base)): {'sha256': hashlib.sha256(p.read_bytes()).hexdigest(),
              'full_mode': oct(stat.S_IMODE(p.stat().st_mode))} for p in base.rglob('*') if p.is_file()}
    unchanged &= actual == expected
result = {'old_positive_traces_passed': len(positives), 'new_control_attempts': len(probes),
          'unsafe_controls_accepted': sum(p['old_predicate_outcome'] == 'accepted_unsafe_control' for p in probes),
          'all_original_capture_bytes_and_modes_unchanged': unchanged, 'positives': positives, 'probes': probes,
          'new_control_source_sha256': hashlib.sha256((ROOT / 'docs/evidence/broker/KL11-74/oracle/history/test-membership-history.py').read_bytes()).hexdigest()}
(OUT / 'old-predicate-probes.json').write_text(json.dumps(result, indent=2) + '\n')
print(json.dumps({k: result[k] for k in ['old_positive_traces_passed','new_control_attempts','unsafe_controls_accepted','all_original_capture_bytes_and_modes_unchanged']}))
assert len(positives) == 4 and len(probes) == 36 and result['unsafe_controls_accepted'] == 36 and unchanged

#!/usr/bin/env python3
"""Exercise independent checks; synthetic controls never count as live jobs."""
import copy
import hashlib
import importlib.util
import json
import sys
from pathlib import Path

sys.dont_write_bytecode = True
root = Path(__file__).parent
spec = importlib.util.spec_from_file_location('independent', root / 'verify-live.py')
v = importlib.util.module_from_spec(spec)
spec.loader.exec_module(v)

def reject(name, callback, rows):
    try:
        callback()
    except (ValueError, KeyError, TypeError, OSError) as error:
        rows.append({'name': name, 'rejected': True, 'reason': str(error)})
    else:
        raise AssertionError('Control accepted: ' + name)

java = []
base = Path('/workspace/work/compaction-public-403be1e3/live/stable-default/attempt-1')
for release in ('j412', 'j421', 'j431'):
    path = base / ('initial-read-' + release + '-j412.json')
    count = v.check_read(v.read(path), release, 'j412', 'initial')
    java.append({'path': str(path), 'sha256': v.sha(path), 'actual_records': count})

positives = []
controls = []
sample = None
for stage in v.STAGES:
    records, positions = v.expected_read('j412', stage, 'native')
    history = []
    for position in positions:
        for seek, record in records:
            if seek == position['seek'] and record['topic'] == position['topic']:
                history.append({'label': 'public-consumer-record', 'seek': seek, 'stage': stage, 'record': record})
        history.extend(v.expected_native_eofs([position]))
        history.append(position)
    receipt = {'records': len(records), 'history': history}
    v.check_read(receipt, 'native', 'j412', stage)
    positives.append(stage)
    if stage == 'initial':
        sample = receipt

def mutate(name, change):
    receipt = copy.deepcopy(sample)
    change(receipt)
    reject(name, lambda: v.check_read(receipt, 'native', 'j412', 'initial'), controls)

def event(receipt, label):
    return next(row for row in receipt['history'] if row['label'] == label)

mutate('missing-eof', lambda r: r['history'].remove(event(r, 'public-consumer-eof')))
mutate('fabricated-end-position', lambda r: event(r, 'public-consumer-position').update(position=900))
mutate('bool-integer-eof', lambda r: event(r, 'public-consumer-eof').update(partition=False))
mutate('wrong-eof-highwatermark', lambda r: event(r, 'public-consumer-eof').update(eof_offset=100))
mutate('false-zero-eof-records', lambda r: event(r, 'public-consumer-eof').update(records_since_seek=0))
mutate('unknown-event', lambda r: r['history'].append({'label': 'unreviewed'}))
mutate('extra-control-field', lambda r: event(r, 'public-consumer-eof').update(unreviewed=True))
mutate('frontier-before-delivery', lambda r: r['history'].insert(0, r['history'].pop(r['history'].index(event(r, 'public-consumer-eof')))))
mutate('changed-header', lambda r: event(r, 'public-consumer-record')['record']['headers'][0].update(value_hex='62'))
mutate('bool-record-count', lambda r: r.update(records=True))

native_path = Path('/workspace/work/compaction-public-403be1e3/native-fcac9d1d/bound-native-build-v2.json')
native = v.read(native_path)
server_sha = '403be1e3db073df86921d6fb21189f695c4f1eaf'
peer_sha = 'fcac9d1d783b63890b3316601de72a4075973e10'
v.check_native_binding(native, server_sha, peer_sha)
binding_controls = []
for name, change in [
    ('wrong-helper-pin', lambda n: n.update(peer_source_sha='0' * 40)),
    ('wrong-server-pin', lambda n: n.update(runtime_source_sha='0' * 40)),
    ('compiler-failed', lambda n: n.update(exit_code=1)),
    ('core-source-changed', lambda n: n['core_source_after'].update(files=1)),
    ('helper-source-changed', lambda n: n['helper_source_after'].update(complete_files=1)),
    ('altered-raw-command', lambda n: n.update(command=['fabricated'])),
    ('wrong-own-git-blob', lambda n: n['native_source'].update(git_blob_sha1='0' * 40)),
    ('wrong-private-elf', lambda n: n['compiler_ELF_private'].update(sha256='0' * 64)),
    ('wrong-native-mode', lambda n: n['native_source'].update(full_mode='0o777')),
    ('changed-sdk', lambda n: n['SDK_dependencies_after'][0].update(bytes=1)),
]:
    candidate = copy.deepcopy(native)
    change(candidate)
    reject(name, lambda: v.check_native_binding(candidate, server_sha, peer_sha), binding_controls)

result = {
    'scope': 'Independent verifier preparation: preserved actual Java reads and actual closed native compile binding; synthetic controls are not runtime qualification',
    'verifier_sha256': v.sha(root / 'verify-live.py'),
    'control_driver_sha256': v.sha(Path(__file__)),
    'preserved_actual_java_reads': java,
    'synthetic_native_stages_passed': positives,
    'synthetic_read_negative_controls': controls,
    'actual_closed_native_compile_binding': {'path': str(native_path), 'sha256': v.sha(native_path), 'passed': True},
    'synthetic_native_binding_negative_controls': binding_controls,
    'new_live_native_or_server_jobs': 0,
    'passed': True,
}
path = root / 'control-result-v2.json'
path.write_text(json.dumps(result, indent=2) + '\n')
path.chmod(0o600)
print(json.dumps({'passed': True, 'actual_java_reads': 3, 'synthetic_stages': 6,
                  'read_controls': len(controls), 'binding_controls': len(binding_controls),
                  'actual_closed_native_binding': True, 'new_live_jobs': 0,
                  'result_sha256': v.sha(path)}))

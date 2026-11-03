#!/usr/bin/env python3
"""Recompute the report from the saved connector responses, without network access."""
import gzip
import hashlib
import json
import pathlib
import re
import zipfile

HERE = pathlib.Path(__file__).resolve().parent
HEAD = '45cbff24de76490594970fc962cd04e7be9fafb1'
sha256 = lambda data: hashlib.sha256(data).hexdigest()
read_json = lambda p: json.loads((HERE / p).read_text())

def log(job):
    return gzip.decompress((HERE / 'logs' / (str(job) + '.log.gz')).read_bytes()).decode('utf-8')

def strip_timestamps(s):
    return re.sub(r'(?m)^\d{4}-\d\d-\d\dT\S+Z ', '', s.replace('\r\n', '\n'))

def failed_library(job):
    s = strip_timestamps(log(job))
    result = re.findall(r'test result: FAILED\. (\d+) passed; (\d+) failed; (\d+) ignored;', s)
    assert len(result) == 1, (job, result)
    passed, failed, ignored = map(int, result[0])
    section = s.rsplit('\nfailures:\n', 1)[1].split('\ntest result:', 1)[0]
    names = re.findall(r'^\s+([\w:]+)\s*$', section, re.M)
    assert len(names) == len(set(names)) == failed, (job, len(names), failed)
    return {'job_id': job, 'passed': passed, 'failed': failed, 'ignored': ignored, 'failure_identities': names}

runs = {}
for run_id in (41604650418, 41604650426, 41604650337, 41604650398):
    run = read_json(f'run-{run_id}.json')
    assert run['head_sha'] == HEAD and run['run_attempt'] == 1 and run['status'] == 'completed'
    jobs = read_json(f'jobs-{run_id}.json')['jobs']
    runs[str(run_id)] = {'conclusion': run['conclusion'], 'jobs': {j['name']: {'id': j['id'], 'conclusion': j['conclusion']} for j in jobs}}
assert runs['41604650418']['conclusion'] == 'failure'
assert all(runs[str(i)]['conclusion'] == 'success' for i in (41604650426, 41604650337, 41604650398))
client_default = failed_library(125811643487)
client_all = failed_library(125811643576)
assert (client_default['passed'], client_default['failed']) == (2177, 19)
assert (client_all['passed'], client_all['failed']) == (2177, 19)
platforms = {}
for name, job, expected in [('windows', 125811643362, (2161, 35)), ('macos_arm64', 125811643267, (2179, 17))]:
    platforms[name] = failed_library(job)
    assert (platforms[name]['passed'], platforms[name]['failed']) == expected
live = {}
for version, job in [('4.1.2',125811643708),('4.2.1',125811643587),('4.3.1',125811643614)]:
    s = log(job)
    assert 'compatibility group phase: classic join' in s and 'stack overflow' in s
    live[version] = {'job_id': job, 'classic_join_marker': True, 'stack_overflow': True}
broker = log(125811361276)
start = broker.index('##[group]Run cargo test --locked --manifest-path partitionline-broker/Cargo.toml --all-targets\n')
all_start = broker.index('##[group]Run cargo test --locked --manifest-path partitionline-broker/Cargo.toml --all-targets --all-features\n')
end = broker.index('##[group]Run cargo clippy --locked --manifest-path partitionline-broker/Cargo.toml --all-targets --all-features -- -D warnings')
profiles = {}
for name, section, expected in [('default',broker[start:all_start],734),('all-features',broker[all_start:end],882)]:
    rows = re.findall(r'test result: ok\. (\d+) passed; (\d+) failed; (\d+) ignored;', section)
    result = [sum(int(row[i]) for row in rows) for i in range(3)]
    assert result == [expected, 0, 0] and len(rows) == 36, (name, result, len(rows))
    profiles[name] = {'result_sections': len(rows), 'passed': result[0], 'failed': result[1], 'ignored': result[2]}
capabilities = {}
for name, digest in [('default','1f704d1a947e9e1dbf257a9c9409f7d9074c39ef7804b844aaef8435b4c4c8d6'),('all-features','75c7abb45ad1c20892de0fe62a8b44ce3efe2a61f3e1bd0f731f6243452159f5')]:
    path = HERE / ('capability-' + name + '.zip')
    assert sha256(path.read_bytes()) == digest
    with zipfile.ZipFile(path) as z:
        validation_raw = z.read('evidence/validation.json')
        manifest = json.loads(z.read('artifact-manifest.json'))
        v = json.loads(validation_raw)
        assert sha256(validation_raw) == manifest['inner_validation_sha256']
        assert v['source_sha'] == HEAD and v['passed'] is True
        assert [v[k] for k in ('actual_executed_tests','actual_passed_tests','actual_unique_verified_test_cases')] == [93,93,93]
        assert v['SDK_public_runtime_qualification'] is False and v['java_fixture_reverse_qualified'] is True
        capabilities[name] = {'zip_sha256': digest, 'validation_sha256': sha256(validation_raw), 'executed': 93, 'passed': 93, 'unique': 93, 'actual_command_count': v['actual_command_count'], 'sdk_public_runtime_qualification': False, 'java_fixture_reverse_qualified': True}
summary = {'head_sha':HEAD, 'run_attempt':1, 'runs':runs, 'client_default':client_default, 'client_all_features':client_all, 'platform_libraries':platforms, 'live_kafka':live, 'broker_tests':profiles, 'capability_compact_reports':capabilities}
print(json.dumps(summary, indent=2, sort_keys=True))

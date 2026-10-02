#!/usr/bin/env python3
"""Bind the workflow's complete local shell coverage and hosted result to source."""
import hashlib
import json
from pathlib import Path
import shlex
import subprocess

import yaml

root = Path(__file__).resolve().parent
repo = root.parents[3]
source_sha = '39bd19a2f824a2c25b847eecfd3b3484aa5ca338'
workflow_bytes = subprocess.check_output(['git', 'show', source_sha + ':.github/workflows/broker.yml'], cwd=repo)
assert (root / 'broker.yml').read_bytes() == workflow_bytes
workflow = yaml.load(workflow_bytes, Loader=yaml.BaseLoader)
assert set(workflow['on']) == {'push', 'pull_request', 'workflow_dispatch'}
assert workflow['on']['push']['branches'] == ['main']
assert workflow['permissions'] == {'contents': 'read'}

source_hashes = json.loads((root / 'source-hashes.json').read_text())
for name, expected in source_hashes.items():
    committed = subprocess.check_output(['git', 'show', source_sha + ':' + name], cwd=repo)
    assert hashlib.sha256(committed).hexdigest() == expected, name

preliminary = json.loads((root / 'preliminary/commands.json').read_text())
coverage = []
for tc in workflow['jobs']['broker']['strategy']['matrix']['rust']:
    result = json.loads((root / 'rust-matrix' / (tc + '-results.json')).read_text())
    assert result['source_sha'] == source_sha and result['passed']
    assert result['snapshot_unchanged_during_checks'] and result['preparation']['exit_code'] == 0
    for name, expected in result['source_sha256'].items():
        assert source_hashes.get(name) == expected, name
    for step in workflow['jobs']['broker']['steps']:
        if 'run' not in step:
            continue
        expected = shlex.split(step['run'])
        if expected[0] == 'cargo':
            candidates = [row for row in result['checks'] if row['command'] == ['cargo', '+' + tc, *expected[1:]]]
            assert len(candidates) == 1, (tc, step['name'])
            row = candidates[0]
            assert row['exit_code'] == 0 and (root / 'rust-matrix' / row['log']).is_file()
            if step.get('env', {}).get('RUSTDOCFLAGS'):
                assert result['environment']['RUSTDOCFLAGS'] == step['env']['RUSTDOCFLAGS']
            attribution = '/root/zstd_decision; immutable-source KL11-35 logs'
        else:
            candidates = [row for row in preliminary if row['lane'] == tc and row['command'] == step['run']]
            assert len(candidates) == 1 and candidates[0]['exit_code'] == 0
            attribution = '/root/open_loop; exact-source local shell command'
        coverage.append({'lane': tc, 'step': step['name'], 'command': step['run'], 'exit_code': 0, 'executed_by': attribution})

for step in workflow['jobs']['api-inventory']['steps']:
    if 'run' not in step:
        continue
    candidates = [row for row in preliminary if row['lane'] == 'api-inventory' and row['command'] == step['run']]
    assert len(candidates) == 1 and candidates[0]['exit_code'] == 0
    coverage.append({'lane': 'api-inventory', 'step': step['name'], 'command': step['run'], 'exit_code': 0, 'executed_by': '/root/open_loop; exact-source local shell command'})
assert len(coverage) == 18
inventory = json.loads((root / 'api-matrix-verification.json').read_text())
assert inventory['verdict'] == 'passed' and inventory['implementation_claim'] is False
assert [row['api_keys'] for row in inventory['releases']] == [93, 93, 93]
negative_log = (root / 'api-inventory/reject-corrupted-pins-inventories-and-unsupported-claims.stderr.log').read_text()
assert 'Ran 24 tests' in negative_log and negative_log.strip().endswith('OK')

hosted = json.loads((root / 'hosted-run.json').read_text())
assert hosted['run']['head_sha'] == source_sha and hosted['run']['path'] == '.github/workflows/broker.yml'
assert hosted['run']['status'] == 'completed' and hosted['run']['conclusion'] == 'success'
assert len(hosted['jobs']) == 3
assert all(job['status'] == 'completed' and job['conclusion'] == 'success' for job in hosted['jobs'])
assert all(step['conclusion'] == 'success' for job in hosted['jobs'] for step in job['steps'])
assert hosted['production_qualification'] is False
(root / 'command-coverage.json').write_text(json.dumps(coverage, indent=2) + '\n')
print(json.dumps({'source_sha': source_sha, 'verdict': 'passed', 'local_workflow_shell_executions': len(coverage),
                  'local_inventory_negative_tests': 24, 'hosted_successful_jobs': 3,
                  'source_file_hashes_verified': len(source_hashes), 'production_qualification': False}, indent=2))

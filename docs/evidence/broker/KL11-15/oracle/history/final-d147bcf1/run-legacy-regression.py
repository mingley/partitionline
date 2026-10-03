#!/usr/bin/env python3
"""Independently check all inherited fixed-voter histories from the snapshot replay."""
import hashlib
import json
import os
from pathlib import Path
import subprocess

repo = Path('/workspace/partitionline')
source = Path('/workspace/work/retention-final-d147bcf1/source')
source_sha = 'd147bcf1c0164778bdbad625842363f3721bc10e'
raw_root = repo / 'docs/evidence/broker/KL11-15/runtime/legacy-replay-d147bcf1-fixed'
out = repo / 'docs/evidence/broker/KL11-15/oracle/history/final-d147bcf1/legacy-regression'
out.mkdir(parents=True, exist_ok=False)


def digest(data):
    return hashlib.sha256(data).hexdigest()


def files(root):
    return {str(p.relative_to(root)): {'sha256': digest(p.read_bytes()), 'mode': p.stat().st_mode & 0o777}
            for p in sorted(root.rglob('*')) if p.is_file() and '__pycache__' not in p.parts}


script_root = source / 'docs/evidence/broker/KL11-73/oracle/history'
pins = {}
for name in ('history_oracle.py', 'wal_oracle.py', 'election_oracle.py'):
    path = script_root / name
    relative = str(path.relative_to(source))
    blob = subprocess.check_output(['git', 'show', source_sha + ':' + relative], cwd=repo)
    mode = subprocess.check_output(['git', 'ls-tree', source_sha, '--', relative], cwd=repo).decode().split()[0]
    assert mode in ('100644', '100755')
    pins[relative] = {'sha256': digest(blob), 'mode': 0o755 if mode == '100755' else 0o644}


def verify():
    for name, expected in pins.items():
        path = source / name
        assert digest(path.read_bytes()) == expected['sha256'] and path.stat().st_mode & 0o777 == expected['mode']


sealer_bytes = (raw_root / 'validation.json').read_bytes()
sealer = json.loads(sealer_bytes)
assert sealer['source_sha'] == source_sha and sealer['results']['original_runtime_captures_unchanged']
commands = []
counts = {'histories': 0, 'events': 0, 'journal_pairs': 0}
for tc in ('stable', '1.85.0'):
    for feature in ('default', 'all-features'):
        for voters in (3, 5):
            label = tc + '-' + feature + '-' + str(voters)
            raw = raw_root / (tc + '-' + feature) / ('history-' + str(voters))
            trace = json.loads((raw / 'trace.json').read_bytes())
            assert trace['source_sha'] == source_sha
            before = files(raw)
            (out / (label + '-inputs-before.json')).write_text(json.dumps(before, indent=2) + '\n')
            argv = ['taskset', '-c', '0-2,4', 'python3', str(script_root / 'history_oracle.py'),
                    str(raw / 'trace.json'), '--output', str(out / (label + '.json'))]
            log = out / (label + '.log')
            verify()
            with log.open('wb') as stream:
                result = subprocess.run(argv, stdout=stream, stderr=subprocess.STDOUT,
                                        env={**os.environ, 'PYTHONDONTWRITEBYTECODE': '1'})
            after = files(raw)
            (out / (label + '-inputs-after.json')).write_text(json.dumps(after, indent=2) + '\n')
            commands.append({'argv': argv, 'exit_code': result.returncode, 'log_sha256': digest(log.read_bytes()),
                             'raw_inputs_unchanged': before == after, 'raw_input_files': len(before)})
            (out / 'commands.json').write_text(json.dumps(commands, indent=2) + '\n')
            verify()
            assert result.returncode == 0 and before == after, 'authentic history check failed: ' + label
            report = json.loads((out / (label + '.json')).read_bytes())
            assert report['source_sha_reported'] == source_sha
            counts['histories'] += 1
            counts['events'] += report['events']
            counts['journal_pairs'] += len(report['raw_journal_checkpoints'])
assert counts == {'histories': 8, 'events': 776, 'journal_pairs': 104}, counts
assert (raw_root / 'validation.json').read_bytes() == sealer_bytes
(out / 'validation.json').write_text(json.dumps({'schema_version': 1, 'passed': True, 'source_sha': source_sha,
    'source_files': pins, 'counts': counts, 'commands': commands, 'sealer_receipt_sha256': digest(sealer_bytes),
    'artifacts': files(out), 'runner_sha256': digest(Path(__file__).read_bytes()),
    'scope': 'Fresh inherited fixed-voter causal safety regression; sealed copies preserve every original captured field',
    'limitations': ['Finite captured histories; no dynamic voting or autonomous network qualification.',
                    'Independent validator controls from the earlier accepted identical validator remain separately source-pinned.']}, indent=2) + '\n')
print(json.dumps(counts))

"""Prepare an isolated, exact-Git dependency closure for two authorized CI fixes."""
import hashlib
import json
import os
from pathlib import Path
import stat
import subprocess

REPO = Path('/workspace/partitionline')
OUT = Path(__file__).resolve().parent
PIN = 'a85b241765981a21cf2b3161ae6eac155200a441'
CI_PIN = 'b8148087f08b205d7d19fd0be10994ebcd1fcc3a'
TARGETS = ['tests/sticky_partitioner.rs', 'tests/conformance/test_verifiable_scenario.py']

def git(*args):
    return subprocess.check_output(['git', *args], cwd=REPO)

def blob(pin, path):
    return git('show', f'{pin}:{path}')

def info(path):
    raw = path.read_bytes()
    return {'bytes': len(raw), 'sha256': hashlib.sha256(raw).hexdigest(),
            'mode_07777': oct(stat.S_IMODE(path.stat().st_mode))}

def save(path, data, mode=0o600):
    path.parent.mkdir(parents=True, exist_ok=True)
    assert not path.exists(), str(path)
    path.write_bytes(data)
    path.chmod(mode)

def json_save(path, value):
    save(path, (json.dumps(value, indent=2, sort_keys=True) + '\n').encode())

tree = {}
for row in git('ls-tree', '-r', '-z', PIN).split(b'\0'):
    if row:
        meta, path = row.split(b'\t', 1)
        mode, kind, oid = meta.decode().split()
        if kind == 'blob':
            tree[path.decode()] = (mode, oid)

closure = {'Cargo.toml', 'src/protocol/api_keys.rs', 'docs/plan/tasks.json',
           'tests/fixtures/protocol_oracles/matrix.json', *TARGETS}
closure.update(path for path in tree if path.startswith('scripts/') and path.endswith('.py'))
closure.update(path for path in tree if path.startswith('tests/conformance/') and
               (path.count('/') == 2 and path.endswith(('.py', '.json')) or
                path in {'tests/conformance/broker/api-matrix.json',
                         'tests/conformance/broker/features.json',
                         'tests/conformance/broker/implemented-api-versions.json'} or
                path.startswith('tests/conformance/broker/upstream/') and path.endswith('.tar.gz')))
registry = json.loads(blob(PIN, 'tests/conformance/cases.json'))
closure.update(ref for case in registry['cases'] for ref in case.get('artifacts', []))
closure.update(registry['coverage_contract'].get('upstream_applicability_artifacts', []))
closure.add('docs/evidence/client/KL05-14/three-sdk/manifest.json')
qual = 'docs/evidence/client/KL05-15/whole-compat-12f43986'
closure.add(qual + '/qualification.json')
qualification = json.loads(blob(PIN, qual + '/qualification.json'))
closure.update(qual + '/' + cell['report'] for cell in qualification['cells'].values())

rows = []
for path in sorted(closure):
    mode, oid = tree[path]
    target = OUT / 'candidate' / path
    save(target, blob(PIN, path), 0o700 if mode == '100755' else 0o600)
    rows.append({'path': path, 'git_mode': mode, 'git_blob_oid': oid, **info(target)})
json_save(OUT / 'dependency-source-before.json', {'source_sha': PIN,
          'scope': 'Exact Git subset for offline Python unit tests, not a complete immutable source archive.',
          'files': rows, 'logical_bytes': sum(row['bytes'] for row in rows)})

originals = []
for path in TARGETS:
    current = blob(PIN, path)
    old = blob(CI_PIN, path)
    assert current == old, path
    assert (REPO / path).read_bytes() == current, path
    target = OUT / 'original-b814' / path
    save(target, old, stat.S_IMODE((REPO / path).stat().st_mode))
    originals.append({'path': path, 'ci_source_sha': CI_PIN, 'current_source_sha': PIN,
                      'current_equals_ci': True, **info(target)})
json_save(OUT / 'original-two-sources.json', {'files': originals})

guards = []
for path in [*TARGETS, 'src/producer.rs', 'src/partitioner.rs',
             'src/protocol/records.rs', 'src/protocol/buf.rs']:
    target = REPO / path
    if target.exists():
        guards.append({'path': path, **info(target)})
json_save(OUT / 'repository-before.json', {'observed_head': git('rev-parse', 'HEAD').decode().strip(),
                                        'scope': 'Named test and production/held file bytes plus full file modes.',
                                        'files': guards})

# Correct only the authorized post-close introspection test.
rust = OUT / 'candidate/tests/sticky_partitioner.rs'
text = rust.read_text()
old = '    producer.close().await.unwrap();\n    let state = producer.__test_sticky_state().unwrap();\n    assert_eq!((state.0, state.1, state.2), (0, 0, 0));'
new = old.replace('producer.close()', 'producer.clone().close()')
assert text.count(old) == 1
rust.write_text(text.replace(old, new))
rust.chmod(0o600)

# Preserve the uncorrected Python source in the isolated closure until the
# failed-first unit test has actually executed. apply_python.py performs the edit.
json_save(OUT / 'prepared-source-scope.json', {'source_sha': PIN, 'ci_source_sha': CI_PIN,
          'rust_change': 'Clone the public shared producer handle before consuming close; preserve post-close state assertions.',
          'rust_compilation': 'Not executed; no Cargo lease.',
          'python_change_pending': True, 'candidate_files': TARGETS,
          'dependency_files': len(rows), 'dependency_bytes': sum(row['bytes'] for row in rows)})
print(json.dumps({'source_sha': PIN, 'files': len(rows), 'bytes': sum(row['bytes'] for row in rows),
                  'candidate_root': str(OUT / 'candidate')}))

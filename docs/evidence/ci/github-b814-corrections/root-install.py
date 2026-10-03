import hashlib
import json
import os
from pathlib import Path
import shutil
import stat
import subprocess

ROOT = Path('/workspace/partitionline')
WORK = Path('/workspace/work/integration/ci-b814-fixes-01')
CLIENT = Path('/workspace/work/client-ci-b814-correction-01/stage-handoff.json')
CI = Path('/workspace/work/github-ci-b814-review-01')
PREFIX = 'docs/evidence/ci/github-b814-corrections'

def identity(path):
    p = Path(path)
    s = p.stat()
    assert p.is_file() and not p.is_symlink()
    return {'sha256': hashlib.sha256(p.read_bytes()).hexdigest(), 'bytes': s.st_size,
            'full_mode': stat.S_IMODE(s.st_mode)}

def exact(path, row):
    actual = identity(path)
    mode = row.get('full_mode', row.get('mode_07777'))
    if isinstance(mode, str):
        mode = int(mode, 8)
    assert actual == {'sha256': row['sha256'], 'bytes': row['bytes'], 'full_mode': mode}, (path, actual, row)

assert subprocess.check_output(['git', 'rev-parse', 'HEAD'], cwd=ROOT, text=True).strip() == 'a85b241765981a21cf2b3161ae6eac155200a441'
assert not subprocess.check_output(['git', 'diff', '--name-only'], cwd=ROOT)
assert not subprocess.check_output(['git', 'diff', '--cached', '--name-only'], cwd=ROOT)
assert hashlib.sha256(CLIENT.read_bytes()).hexdigest() == '0ca35ab7f484e75732cc16cc7b9900a81291f97d3c7afff310abe3d9032fd21b'
ci_inventory = json.loads((CI / 'publication-inventory.json').read_text())
assert hashlib.sha256((CI / 'publication-inventory.json').read_bytes()).hexdigest() == '530d4ea55d4ff3873cc35dcd8fca8b0df48f46391ff8352761306c49886da5ec'
for job in ci_inventory['jobs']:
    exact(job['structured_tool_result']['path'], job['structured_tool_result'])
    exact(job['decoded_UTF8_log']['path'], job['decoded_UTF8_log'])
for key in ['preserved_original_retrieval_receipt', 'exact_Git_source_provenance_receipt', 'Admin_source_only_proposal']:
    exact(ci_inventory[key]['path'], ci_inventory[key])
provenance = json.loads(Path(ci_inventory['exact_Git_source_provenance_receipt']['path']).read_text())
exact(provenance['complete_tree_index']['path'], provenance['complete_tree_index'])
for row in provenance['selected_exact_git_blob_copies']:
    exact(row['path'], row)
client = json.loads(CLIENT.read_text())
for row in client['files']:
    exact(row['source'], row)

held = {name: identity(ROOT / name) for name in ['src/protocol/records.rs', 'src/protocol/buf.rs']}
before = {name: identity(ROOT / name) for name in ['src/admin.rs', *client['write_paths']]}
assert before['src/admin.rs']['sha256'] == 'e78e63d5fac51b74a5ce9784d671d1aa4fb23ead6dc3e27cf6f7039dcc274ab8'
assert before['tests/sticky_partitioner.rs']['sha256'] == '959361daf4d29ba1615fa33ea97f0a06e7992044ea7d5fb10ffe57b2f3b9808f'
assert before['tests/conformance/test_verifiable_scenario.py']['sha256'] == '1092df0d9174b8b06712c6e34b1cc5c1ae323c76015407a2bfe30293af23c982'

rows = []
def install(source, name, replace=False):
    source = Path(source)
    dest = ROOT / name
    original = identity(source)
    assert not dest.exists() or replace, name
    dest.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    shutil.copy2(source, dest)
    assert identity(source) == original
    assert identity(dest) == original
    rows.append({'path': name, 'source': str(source), **original})

for source in sorted(CI.rglob('*')):
    if source.is_file():
        install(source, PREFIX + '/' + str(source.relative_to(CI)))
for row in client['files']:
    install(row['source'], row['path'], replace=row['path'] in client['write_paths'])
install(CLIENT, client['proof_prefix'] + '/stage-handoff.json')
formatted = WORK / 'formatted/src/admin.rs'
assert identity(formatted)['sha256'] == '10bc276939b42486dd40bb889bff802f33237086219b59410bac2cdc514da668'
install(formatted, 'src/admin.rs', replace=True)
assert {name: identity(ROOT / name) for name in held} == held
receipt = {
    'source_parent': 'a85b241765981a21cf2b3161ae6eac155200a441',
    'source_before': before, 'source_after': {name: identity(ROOT/name) for name in before},
    'held_before_after_identical': held,
    'actual_commands': [
        {'argv': ['rustup', 'run', 'stable', 'rustfmt', '--edition', '2021', str(formatted)], 'exit_code': 0},
        {'argv': ['rustup', 'run', '1.85.0', 'rustfmt', '--edition', '2021', '--check', str(formatted)], 'exit_code': 0}
    ],
    'scope': 'Three CI source corrections. Actual failed CI logs, source and original candidates retained. Client packet contains actual113 Python methods and format checks. Root formatting only; no new Rust compilation, Clippy, SDK or behavioral qualification claimed.',
    'installed': rows
}
out = WORK / 'installation.json'
out.write_text(json.dumps(receipt, indent=2) + '\n'); out.chmod(0o600)
install(out, PREFIX + '/root-installation.json')
install(Path(__file__), PREFIX + '/root-install.py')
(WORK / 'stage-paths.json').write_text(json.dumps([row['path'] for row in rows], indent=2) + '\n')
print(json.dumps({'files': len(rows), 'bytes': sum(row['bytes'] for row in rows), 'receipt': str(out)}))

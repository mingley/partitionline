import hashlib
import json
from pathlib import Path
import shutil
import stat
import subprocess

R=Path('/workspace/partitionline')
W=Path(__file__).parent
P='docs/evidence/conformance/fetch-recovery-ci-e8fc-01'
Z=Path('/workspace/work/client-ci-lint-corrections-3dbc/revision-01')
ZP='docs/evidence/ci/github-e8fc-test-lint-corrections'

def info(path):
    p=Path(path);s=p.stat();assert p.is_file() and not p.is_symlink()
    return dict(sha256=hashlib.sha256(p.read_bytes()).hexdigest(),bytes=s.st_size,full_mode=stat.S_IMODE(s.st_mode))

assert subprocess.check_output(['git','rev-parse','HEAD'],cwd=R,text=True).strip()=='3dbc216bf90fdd2527428335293b8c84a26e5e6e'
assert not subprocess.check_output(['git','diff','--name-only'],cwd=R)
assert not subprocess.check_output(['git','diff','--cached','--name-only'],cwd=R)
held={n:info(R/n) for n in ['src/protocol/records.rs','src/protocol/buf.rs']}
compiler={n:info(R/n) for n in ['src/admin.rs','src/lib.rs','src/producer.rs','src/partitioner.rs']}
assert info(Z/'freeze.json')['sha256']=='05fbf4561f1ff6d69fc337286fd7262e385e3b89fa62320bc39fa3274f32cb3c'
freeze=json.loads((Z/'freeze.json').read_text())
for row in freeze['files']:
    assert info(row['path'])==dict(sha256=row['sha256'],bytes=row['bytes'],full_mode=row['full07777'])
manifest=json.loads((Z/'install-manifest.json').read_text())
assert info(Z/'install-manifest.json')['sha256']=='1d11537d1caffa82e29d0748130acff5136edaf010df1982f62cf08137b0f2ec'
for row in manifest['files']:
    assert info(R/row['destination'])['sha256']==row['expected_destination_sha256']
for n in ['scripts/report-fetch-session-recovery.py','tests/conformance/test_fetch_session_recovery.py']:
    assert info(W/'original'/n)==info(R/n)

rows=[]
before={row['destination']:info(R/row['destination']) for row in manifest['files']}
before.update({n:info(R/n) for n in ['scripts/report-fetch-session-recovery.py','tests/conformance/test_fetch_session_recovery.py']})
def copy(source,target,replace=False):
    source=Path(source);original=info(source);dest=R/target
    assert not dest.exists() or replace,target
    dest.parent.mkdir(parents=True,exist_ok=True,mode=0o700)
    shutil.copy2(source,dest)
    assert info(source)==original and info(dest)==original
    rows.append(dict(source=str(source),path=target,**original))

for source in sorted((W/'stage').rglob('*')):
    if source.is_file():
        n=str(source.relative_to(W/'stage'));copy(source,n,replace=n in before)
for source in sorted((W/'original').rglob('*')):
    if source.is_file() and '__pycache__' not in source.parts:
        copy(source,P+'/original-source/'+str(source.relative_to(W/'original')))
for row in freeze['files']:
    source=Path(row['path']);copy(source,ZP+'/'+str(source.relative_to(Z)))
copy(Z/'freeze.json',ZP+'/freeze.json')
for row in manifest['files']:copy(row['source'],row['destination'],replace=True)
assert {n:info(R/n) for n in held}==held
assert {n:info(R/n) for n in compiler}==compiler
out=W/'installation.json'
receipt=dict(source_parent='3dbc216bf90fdd2527428335293b8c84a26e5e6e',six_sources_before=before,six_sources_after={n:info(R/n) for n in before},held_and_client_compiler_unchanged={**held,**compiler},installed=rows,
    observed_validation='Actual21 Python unit methods pass, including complete real e8fc Fetch18 runtime replay; original failed wrapper remains rejected. Four Rust test-source fixes have standalone source formatting only. No new Rust compilation, strict Clippy, SDK or complete passing CI claim.',
    known_peer_semantic_change='Test-fixture short critical sections use parking_lot instead of std poisoning mutex; all27 callers adapted and async filesystem operations awaited outside guards. All raw wire history fields and requests retained.',
    first_failures_retained=True,full_production_or_performance_claim=False)
out.write_text(json.dumps(receipt,indent=2)+'\n');out.chmod(0o600)
copy(out,P+'/root-installation.json');copy(Path(__file__),P+'/root-install.py')
(W/'stage-paths.json').write_text(json.dumps([row['path'] for row in rows],indent=2)+'\n')
print(json.dumps(dict(files=len(rows),bytes=sum(row['bytes'] for row in rows),sources=6)))

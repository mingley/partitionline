import hashlib
import json
from pathlib import Path
import shutil
import stat
import subprocess

R = Path('/workspace/partitionline')
W = Path('/workspace/work/integration/broker-sticky-source-checkpoint-01')
P = 'docs/evidence/broker/raft-runtime-tcp/preparation/'

def info(p):
    p = Path(p); s = p.stat()
    assert p.is_file() and not p.is_symlink()
    return dict(sha256=hashlib.sha256(p.read_bytes()).hexdigest(), bytes=s.st_size, full_mode=stat.S_IMODE(s.st_mode))

def check(p, row):
    assert info(p) == dict(sha256=row['sha256'], bytes=row['bytes'], full_mode=row.get('full_mode', row.get('full07777'))), str(p)

assert subprocess.check_output(['git', 'rev-parse', 'HEAD'], cwd=R, text=True).strip() == 'e8fc0b72724ebfcef99d4742c7870f95a7a9a47f'
dirty=set(subprocess.check_output(['git', 'diff', '--name-only'], cwd=R, text=True).splitlines())
assert dirty <= {'partitionline-broker/src/raft/runtime.rs', 'partitionline-broker/tests/common/raft_runtime.rs'}
assert not subprocess.check_output(['git', 'diff', '--cached', '--name-only'], cwd=R)
held = {n: info(R/n) for n in ['src/protocol/records.rs', 'src/protocol/buf.rs']}
client = {n: info(R/n) for n in ['src/admin.rs', 'src/lib.rs', 'src/producer.rs', 'src/partitioner.rs', 'tests/sticky_partitioner.rs', 'tests/streams_protocol.rs']}
before = {}
for n in ['partitionline-broker/src/raft/runtime.rs', 'partitionline-broker/tests/common/raft_runtime.rs']:
    original=subprocess.check_output(['git', 'show', 'HEAD:'+n], cwd=R)
    before[n]=dict(sha256=hashlib.sha256(original).hexdigest(), bytes=len(original), full_mode=info(R/n)['full_mode'])
rows = []
def copy(source, target, replace=False):
    s = Path(source); original = info(s); dest=R/target
    assert not dest.exists() or replace or info(dest)==original, target
    dest.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    if not dest.exists() or info(dest)!=original:
        shutil.copy2(s, dest)
    assert info(dest) == original and info(s) == original
    rows.append(dict(source=str(s), path=target, **original))

pins = {'02':'e4250889d4cd2e68818a21b19e6a68a8caeb7b516ad54643a24b34880a54591b',
        '03':'c745ffa88433439b7e3d785be8c64a0d81ec936e413c029f94160f34a98507cf',
        '04':'01dada13e68830986afb4f468782668ae6b2816a766045a9d0f70d0b2bb06a41'}
for rev, sha in pins.items():
    h = Path('/workspace/work/raft-runtime-76/tcp-qualification-'+rev+'/handoff.json')
    assert info(h)['sha256'] == sha
    d = json.loads(h.read_text())
    for row in d['rows']:
        check(row['source'], row)
    for row in d['rows']:
        copy(row['source'], P+'revision-'+rev+'/'+row['path'])
    copy(h, P+'revision-'+rev+'/handoff.json')
for rev, sha in [('02','f6b400873'),('03','83ff9100b650463f2465d8974bd4dd8f3ef6520b6f9c0bd36236732e5472ce87'),('04','654c38b5a2db5135126166b19d6da04339311aa00acc095ae7debc6ded221b47')]:
    d = Path('/workspace/work/integration/raft-runtime-broader-review-'+rev)
    assert info(d/'validation.json')['sha256'].startswith(sha)
    for p in sorted(d.iterdir()):
        if p.is_file(): copy(p, P+'independent-review-'+rev+'/'+p.name)
for n in before:
    copy('/workspace/work/raft-runtime-76/tcp-qualification-04/candidate/'+n, n, replace=True)

h = Path('/workspace/work/client-sticky-performance-installation-02/ready-stage.json')
assert info(h)['sha256'] == '0e37ab2539d8793aef5b54d67618fc4126e8e04a8067929b4d12a1f3c6c6f6f7'
d = json.loads(h.read_text())
assert len(d['base_payloads']) == 114
for row in d['base_payloads'] + d['additional_metadata_and_peer_proof']:
    check(row['source'], row)
for row in d['base_payloads'] + d['additional_metadata_and_peer_proof']:
    copy(row['source'], row['target'])
copy(h, 'docs/evidence/client/KL05-10/performance/preparation/practical-review-02/installation-ready/ready-stage.json')

book = R/'docs/plan/tasks.json'; raw=book.read_text(); old=json.loads(raw)
(W/'taskbook-before.json').write_text(raw); (W/'taskbook-before.json').chmod(0o600)
changes = {
    'KL11-76': {'evidence_add': P+'revision-04/handoff.json'},
    'KL05-10': {'evidence_add': 'docs/evidence/client/KL05-10/performance/preparation/practical-review-02/installation-ready/ready-stage.json'},
    'KL11-71': {'status':'in_progress', 'owner':'codex-open-loop-20261003'}
}
for key, change in changes.items():
    pos=raw.index('"id": "'+key+'"'); brace=raw.rfind('{',0,pos); start=raw.rfind('\n',0,brace)+1; chunk=raw[start:]; lead=len(chunk)-len(chunk.lstrip()); task,n=json.JSONDecoder().raw_decode(chunk.lstrip()); end=start+lead+n
    assert task['id']==key
    if 'evidence_add' in change: task['evidence'].append(change['evidence_add'])
    for k,v in change.items():
        if k!='evidence_add': task[k]=v
    replacement='\n'.join('    '+line for line in json.dumps(task,indent=2).splitlines())
    raw=raw[:start]+replacement+raw[end:]
new=json.loads(raw)
assert {a['id'] for a,b in zip(old['tasks'],new['tasks']) if a!=b} == set(changes)
book.write_text(raw); rows.append(dict(path='docs/plan/tasks.json', **info(book)))
assert {n:info(R/n) for n in held} == held
assert {n:info(R/n) for n in client} == client
receipt = dict(source_parent='e8fc0b72724ebfcef99d4742c7870f95a7a9a47f', broker_before=before, broker_after={n:info(R/n) for n in before}, client_and_held_unchanged={**client, **held}, installed=rows,
    source_review='Broker revisions03/04 independent source/model checks passed. Sticky benchmark corrected02 independent44 replay+24 controls passed. Source-only checkpoint; no new Cargo, compiler, TCP runtime, JVM, SDK, benchmark, throughput/ranking or qualification claims. Tasks76/10 remain in_progress; server71 implementation assigned.',
    actual_source_files=['partitionline-broker/src/raft/runtime.rs', 'partitionline-broker/tests/common/raft_runtime.rs'], performance_claims=False)
out=W/'installation.json';out.write_text(json.dumps(receipt,indent=2)+'\n');out.chmod(0o600)
copy(out,P+'root-source-checkpoint-01/installation.json')
copy(Path(__file__),P+'root-source-checkpoint-01/install.py')
(W/'stage-paths.json').write_text(json.dumps([row['path'] for row in rows],indent=2)+'\n')
print(json.dumps(dict(files=len(rows),logical_bytes=sum(row['bytes'] for row in rows),held_and_client_unchanged=True)))

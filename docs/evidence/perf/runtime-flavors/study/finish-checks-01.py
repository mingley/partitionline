import collections
import difflib
import hashlib
import json
import os
from pathlib import Path
import subprocess

s = Path(__file__).parent
repo = Path('/workspace/partitionline')
source = Path('/workspace/work/open-cards-20261006/runtime-flavors-source-17a634')
formatter = Path('/workspace/work/open-cards-20261006/runtime-flavors-formatter-63a89f')
sha = lambda p: hashlib.file_digest(Path(p).open('rb'), 'sha256').hexdigest()
def save(name, data):
    with (s/name).open('x') as f:
        json.dump(data, f, indent=2); f.write('\n')

pins=json.loads((s/'source-pins-04.json').read_text())
assert all(sha(source/name)==digest for name,digest in pins.items())
assert not subprocess.check_output(['git','status','--porcelain'],cwd=source)
derivations=[]
for original,derived in [('bench_produce.rs','native-produce-runtime.rs'),('bench_latency.rs','native-latency-runtime.rs')]:
    a=source/'examples'/original; b=source/'benchmarks/runtime/src/bin'/derived
    diff=''.join(difflib.unified_diff(a.read_text().splitlines(True),b.read_text().splitlines(True),fromfile=str(a),tofile=str(b)))
    path=s/(derived+'.derivation.diff')
    with path.open('x') as f:f.write(diff)
    derivations.append(dict(original=str(a),original_sha256=sha(a),derived=str(b),derived_sha256=sha(b),diff=str(path),diff_sha256=sha(path)))
save('native-driver-derivation-02.json',dict(source_commit='17a6344b019e65ad209800e1f93481f7bf50ad36',derivations=derivations,
    additions='Explicit runtime wrapper, process allocation/CPU/RSS observations and bounded post-close cancellation barrier. Bulk sparse admission-to-final-flush bounds and latency warmup sidecars are diagnostic instrumentation. Original examples and core source remain unchanged.'))
processes=[];rows=[]
for name in ('nb-01','native-01'):
    root=s/name
    completion=json.loads((root/'completion.json').read_text())
    assert completion['source_guards_passed'] and completion['owned_process_groups_empty']
    for row in json.loads((root/'rows.json').read_text()):
        assert sha(row['artifact'])==row['sha256']
        assert row['runtime']['observed_alive_tasks']==0
        rows.append(row)
    for path in root.rglob('*.process.json'):
        x=json.loads(path.read_text())
        assert x['parent_waited']
        assert x['exit_code'] in (0,1)
        if x['exit_code']==1: assert path.name=='client.process.json'
        processes.append(dict(path=str(path),exit_code=x['exit_code'],sha256=sha(path)))
native=[r for r in rows if r['cell'].startswith('lb-')]
failed=[r for r in native if r['disposition']=='failed']
assert len(rows)==180 and len(failed)==32
manifest=json.loads((s/'canonical-native-01/manifest.json').read_text())
assert not manifest['partial'] and len(manifest['results'])==120
for row in manifest['results']:
    assert row['schema_validation'] and row['actual_cli_exit_code']==0 and row['controls']==4
    assert sha(row['original'])==row['original_sha256'] and sha(row['view'])==row['view_sha256']
closure=json.loads((s/'native-01/closure.json').read_text())
assert closure['parent_waited'] and closure['supervisor_joined'] and closure['group_empty'] and not closure['adopted_children']
assert all(p['reusable'] for p in closure['ports'])
for i,path in enumerate(sorted((s/'producer-close-observation-01').rglob('*.result.json'))):
    receipt=s/f'failed-observer-validator-{i}.process.json'
    argv=['python3',str(formatter/'benchmarks/runtime/tools/parent-bound-exec.py'),str(os.getpid()),'python3',str(source/'benchmarks/runtime/tools/measure-process.py'),str(receipt),'python3',str(formatter/'scripts/benchmark-report.py'),str(path)]
    with (s/f'failed-observer-validator-{i}.log').open('x') as f:
        subprocess.run(argv,stdout=f,stderr=subprocess.STDOUT,check=True,timeout=30)
    assert json.loads(receipt.read_text())['exit_code']==0
save('final-verification-01.json',dict(source_files_verified=len(pins),rows_sha256_verified=len(rows),
    native_failed_results=len(failed),failed_load_counts=dict(collections.Counter(str(r['load']) for r in failed)),
    process_receipts=processes,nonzero_processes=sum(p['exit_code']!=0 for p in processes),
    canonical_schema_and_cli_verified=120,canonical_mutation_controls=480,failed_observer_cli_verified=2,
    native_closure=closure,scope='Local unsigned benchmark qualification; capacity failures retained.'))
print('verified',len(rows),'rows;',len(failed),'native capacity failures;',len(processes),'waited process receipts')

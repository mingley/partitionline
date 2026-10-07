import collections
import copy
import hashlib
import json
from pathlib import Path
import subprocess
import jsonschema

s=Path(__file__).parent
source=Path('/workspace/work/open-cards-20261006/build-profiles-source-999133')
driver=Path('/workspace/work/open-cards-20261006/build-profiles-driver-7667b6')
def sha(path):
    with Path(path).open('rb') as f:return hashlib.file_digest(f,'sha256').hexdigest()
def read(path):return json.loads(Path(path).read_text())
def save(name,value):
    with (s/name).open('x') as f:json.dump(value,f,indent=2);f.write('\n')

pins=read(s/'source-pins-01.json')
assert all(sha(source/name)==digest for name,digest in pins.items())
assert not subprocess.check_output(['git','status','--porcelain'],cwd=source)
witness=read(s/'driver-source-witness-02.json')
assert all(sha(name)==digest for name,digest in witness['driver_files'].items())
assert not subprocess.check_output(['git','status','--porcelain'],cwd=driver)
assert subprocess.check_output(['git','rev-parse','HEAD'],cwd=driver,text=True).strip()==witness['driver_source_commit']
for manifest in (s/'build-initial-01/build-manifest.json',s/'build-pgo-use-01/build-manifest.json'):
    for config in read(manifest)['configs']:
        for binary in config['binaries'].values():
            assert sha(binary['path'])==binary['sha256']
            assert Path(binary['path']).stat().st_size==binary['bytes']
merge=read(s/'merge-profiles-01/merge-manifest.json')
assert sha(merge['merged']['path'])==merge['merged']['sha256']
assert all(sha(row['path'])==row['sha256'] for row in merge['raws'])
schema=read(source/'benchmarks/result-schema.json')
validator=jsonschema.Draft202012Validator(schema)
rows=[];schemas=[];closures=[];processes=[];controls=[]
for name,count,repetitions in [('train-nb-01',2,1),('train-native-01',1,1),('compare-nb-01',70,5),('compare-native-01',35,5)]:
    root=s/name
    complete=read(root/'completion.json')
    assert complete['source_guards_passed'] and complete['owned_process_groups_empty']
    assert read(root/'matrix-plan.json')['repetitions']==repetitions
    actual=read(root/'rows.json');assert len(actual)==count
    for row in actual:
        assert sha(row['artifact'])==row['sha256']
        assert row['runtime']['observed_alive_tasks']==0
        assert row.get('disposition','executed')=='executed'
        assert row['outcomes']['rejected']==0
        doc=read(row['artifact']);config=row['config']
        binding=read(Path(row['artifact']).parent/'benchmark-build.json')
        assert binding==dict(name=config['name'],build=config['build'],binaries=config['binaries'],instrumented_training=config['instrumented_training'])
        binary=config['binaries']['runtime' if name.endswith('nb-01') else 'native-produce-runtime']
        assert doc['provenance']['binary']['sha256']==binary['sha256']
        assert doc['provenance']['binary']['path']==binary['path']
        assert doc['provenance']['source']['git_commit']=='999133586acfd88604222dc92c01c9eadd2fc6a5'
        receipt=read(Path(row['artifact']).parent/'validator.process.json')
        assert receipt['parent_waited'] and receipt['exit_code']==0
        errors=[e.message for e in validator.iter_errors(doc)]
        schemas.append(dict(artifact=row['artifact'],sha256=row['sha256'],valid=not errors,errors=errors))
        if name.endswith('native-01'):
            assert not errors,errors
            assert doc['execution']['total_repetitions']==repetitions
            assert row['outcomes']['acknowledged']==8_000_000
            job=Path(row['artifact']).parent
            closure=read(job/'topic-closure.json');assert all(closure[k] for k in ('readback_completed','deletion_after_readback','owned_topic_files_absent'))
            semantic=read(job/'changed-result-controls.json');assert len(semantic)==4 and all(c['rejected'] for c in semantic)
            controls.extend(semantic)
            changed=copy.deepcopy(doc);changed['provenance']['broker']['mode']='unqualified-mode'
            errors=list(validator.iter_errors(changed));assert errors
            controls.append(dict(control='schema-broker-mode',artifact=row['artifact'],rejected=True,errors=[e.message for e in errors]))
        rows.append(row)
    if name.endswith('native-01'):
        closure=read(root/'closure.json')
        assert closure['parent_waited'] and closure['supervisor_joined'] and closure['group_empty'] and not closure['adopted_children']
        assert all(p['reusable'] for p in closure['ports'])
        closures.append(dict(directory=str(root),closure=closure))
for path in s.rglob('*.process.json'):
    if path.parent.name.startswith('integration'):continue
    receipt=read(path)
    assert receipt['parent_waited'] and receipt['exit_code']==0, (str(path),receipt)
    processes.append(dict(path=str(path),sha256=sha(path),exit_code=receipt['exit_code']))
assert len(rows)==108
assert len(controls)==180
save('schema-verification-01.json',dict(schema=str(source/'benchmarks/result-schema.json'),schema_sha256=sha(source/'benchmarks/result-schema.json'),results=schemas))
save('changed-schema-controls-01.json',controls)
save('final-verification-01.json',dict(source_files_verified=len(pins),driver_source=witness,retained_result_count=len(rows),
    comparison_results=105,instrumented_training_results=3,positive_report_cli_invocations=108,
    full_schema_native_results=36,full_schema_all_results=sum(x['valid'] for x in schemas),actual_changed_result_controls=len(controls),
    semantic_controls=144,schema_controls=36,waited_process_receipts=processes,owned_broker_closures=closures,
    native_independently_verified_records=36*8_010_000,comparison_native_verified_records=35*8_010_000,
    profile_merge=merge,scope='Local exploratory source-bound compiler-profile measurements; training excluded from comparisons.'))
print('verified',len(rows),'actual results;',len(processes),'waited process receipts;',sum(x['valid'] for x in schemas),'full-schema passes')

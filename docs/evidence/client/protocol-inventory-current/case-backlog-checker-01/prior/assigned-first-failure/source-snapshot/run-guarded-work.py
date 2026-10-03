#!/usr/bin/env python3
import hashlib,json,os,re,stat,subprocess,time
from pathlib import Path
P=Path('/workspace/work/consumer-protocol-checker-implementation-02');S=P/'source'
OUT=P/'guarded-final-work';OUT.mkdir(mode=0o700,exist_ok=True)
M=json.loads((P/'immutable-inputs.json').read_text());F=json.loads((P/'candidate-source-freeze-work.json').read_text());expected={r['path']:r for r in M['rows']};expected.update({r['path']:r for r in F['rows']})
def sha(b):return hashlib.sha256(b).hexdigest()
def save(p,j):p.write_text(json.dumps(j,indent=2,sort_keys=True)+'\n')
def guard():
 actual={str(p.relative_to(S)) for p in S.rglob('*') if p.is_file() or p.is_symlink()}
 assert actual==set(expected),(actual-set(expected),set(expected)-actual)
 for path,row in expected.items():
  p=S/path;assert not p.is_symlink();assert sha(p.read_bytes())==row['sha256'];assert oct(stat.S_IMODE(p.stat().st_mode))==row['full_mode']
 return {'verified_inputs':len(actual),'byte_full_mode_pathset_unchanged':True,'scope':'minimal static checker input copy, not a full Git archive'}
commands=[('unit-tests',['python3','tests/conformance/test_check_protocol_coverage.py','-v'],0),('self-test',['python3','scripts/check-protocol-coverage.py','--self-test'],0)]
commands += [(m,['python3','scripts/check-protocol-coverage.py','--mode',m,'--json'],0 if m in ('classification','backlog') else 1) for m in ('classification','backlog','core','full')]
results=[];env=dict(os.environ,PYTHONDONTWRITEBYTECODE='1')
for ordinal,(name,argv,code) in enumerate(commands,1):
 before=guard();start=time.time();result=subprocess.run(['taskset','-c','2,4',*argv],cwd=S,env=env,capture_output=True,timeout=120)
 (OUT/(name+'.stdout')).write_bytes(result.stdout);(OUT/(name+'.stderr')).write_bytes(result.stderr);after=guard()
 row={'ordinal':ordinal,'name':name,'argv':['taskset','-c','2,4',*argv],'exit_code':result.returncode,'expected_exit_code':code,'passed':result.returncode==code,'before':before,'after':after,'seconds':time.time()-start,'stdout_sha256':sha(result.stdout),'stderr_sha256':sha(result.stderr)}
 if name=='unit-tests':
  row['tests']=int(re.search(rb'Ran (\d+) tests',result.stderr)[1]);assert row['tests']==53
 if name in ('classification','backlog','core','full'):
  j=json.loads(result.stdout);assert j['summary']['exit_code']==result.returncode
  assert j['authoritative_inventory'];assert not j['core_protocol_complete'] and not j['full_current_protocol_complete']
  row.update(required_cases=j['conformance_backlog']['required_cases'],independent_cases=j['conformance_backlog']['independent_cases'],unqualified_cases=j['conformance_backlog']['unqualified_cases'],issues=len(j['conformance_backlog']['issues']))
 results.append(row);save(OUT/'validation.json',{'passed':all(r['passed'] for r in results),'source_input_pin':M['base_source_sha'],'WORK_only_actual_assigned_taskbook':True,'candidate_four_path_freeze_sha256':sha((P/'candidate-source-freeze-work.json').read_bytes()),'results':results,'no_sdk_cargo_jvm_product_runtime':True})
 assert row['passed'],row
print(json.dumps({'passed':True,'commands':len(results),'tests':53,'all83_inputs_before_after_each_command':True}))

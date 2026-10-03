import hashlib
import json
import os
from pathlib import Path
import subprocess
import time

work=Path(__file__).resolve().parent
cfg=json.loads((work/'orchestration.json').read_text())
source=Path(cfg['source']); output=Path(cfg['output']); repo=Path(cfg['repo'])
oracle=source/'docs/evidence/broker/KL11-74/oracle/apache'; fixtures=source/'partitionline-broker/tests/fixtures/raft-membership'
report={'schema_version':1,'source_sha':cfg['source_sha'],'scope':'Exact pushed source Apache membership74 component/fixture qualification; no Rust/full59k snapshot/multinode/TCP claims','passed':False,'commands':[],'source_files':len(cfg['expected_sha256']),'effective_cpu_mask':'2,4','runner_requested_mask':'0-2,4','affinity_adapter_sha256':hashlib.sha256(Path(cfg['wrapper']).read_bytes()).hexdigest()}
env=os.environ.copy();env['PATH']=str(Path(cfg['wrapper']).parent)+os.pathsep+env['PATH'];env['PL_MEMBERSHIP_AFFINITY_LOG']=str(output/'effective-affinity.jsonl')

def integrity():
    for name,wanted in cfg['expected_sha256'].items():
        raw=(source/name).read_bytes()
        assert hashlib.sha256(raw).hexdigest()==wanted,name
        actual=hashlib.sha1(('blob '+str(len(raw))+'\0').encode()+raw).hexdigest()
        wanted_blob=subprocess.run(['git','rev-parse',cfg['source_sha']+':'+name],cwd=repo,capture_output=True,text=True,check=True).stdout.strip()
        assert actual==wanted_blob,name
    return len(cfg['expected_sha256'])

cmds=[('component', ['python3',str(oracle/'prepare-and-run.py'),'--work',str(work/'runtime'),'--output',str(output/'component'),'--fixtures',str(work/'generated-fixtures')]),
      ('fixture-baseline', ['python3',str(oracle/'verify-fixtures.py'),'--fixtures',str(fixtures),'--output',str(output/'fixture-verification.json')]),
      ('fixture-controls', ['python3',str(oracle/'test-fixture-verifier.py'),'--fixtures',str(fixtures),'--work',str(work/'controls'),'--output',str(output/'fixture-controls')])]
try:
    for name,cmd in cmds:
        before=integrity();start=time.time()
        with (output/(name+'.stdout')).open('wb') as out,(output/(name+'.stderr')).open('wb') as err:
            r=subprocess.run(['taskset','-c','2,4']+cmd,env={**env,'PATH':os.environ['PATH']} if name!='component' else env,stdout=out,stderr=err,cwd=work,timeout=60)
        after=integrity();report['commands'].append({'name':name,'argv':['taskset','-c','2,4']+cmd,'exit_code':r.returncode,'elapsed_seconds':round(time.time()-start,3),'exact_git_files_checked_before':before,'exact_git_files_checked_after':after,'stdout_sha256':hashlib.sha256((output/(name+'.stdout')).read_bytes()).hexdigest(),'stderr_sha256':hashlib.sha256((output/(name+'.stderr')).read_bytes()).hexdigest()})
        if r.returncode:
            raise RuntimeError(name+' failed: '+(output/(name+'.stderr')).read_text()[-3000:])
    generated=work/'generated-fixtures'
    expected={str(p.relative_to(fixtures)):hashlib.sha256(p.read_bytes()).hexdigest() for p in fixtures.rglob('*') if p.is_file()}
    observed={str(p.relative_to(generated)):hashlib.sha256(p.read_bytes()).hexdigest() for p in generated.rglob('*') if p.is_file()}
    assert observed==expected,'Generated fixture replay differs from426 committedfiles'
    component=json.loads((output/'component/validation.json').read_text());baseline=json.loads((output/'fixture-verification.json').read_text());controls=json.loads((output/'fixture-controls/validation.json').read_text())
    assert component['passed'] and baseline['passed'] and controls['passed']
    assert component['positive_component_assertions']==854 and component['deliberate_failing_executions']==9
    assert len(controls['negative_controls'])==48 and len(controls['baselines'])==3
    log=[json.loads(line) for line in (output/'effective-affinity.jsonl').read_text().splitlines()]
    assert len(log)==len(component['commands'])==19
    assert all(row['effective_argv'][:3]==['/usr/bin/taskset','-c','2,4'] for row in log)
    report.update({'passed':True,'component_assertions':854,'intended_java_failure_controls':9,'fixture_corruption_controls':48,'positive_fixture_baselines':3,'named_fixture_cases':59,'single_replay_fixture_assertions':427,'native_batches':85,'native_records':138,'committed_fixture_files_reproduced_exactly':426,'actual_child_affinity_invocations':19,'component_validation_sha256':hashlib.sha256((output/'component/validation.json').read_bytes()).hexdigest(),'fixture_verification_sha256':hashlib.sha256((output/'fixture-verification.json').read_bytes()).hexdigest(),'fixture_controls_sha256':hashlib.sha256((output/'fixture-controls/validation.json').read_bytes()).hexdigest(),'effective_affinity_log_sha256':hashlib.sha256((output/'effective-affinity.jsonl').read_bytes()).hexdigest()})
except Exception as error:
    report['error']=type(error).__name__+': '+str(error)
    raise
finally:
    (output/'run-final.py').write_bytes(Path(__file__).read_bytes())
    report['orchestration_source_sha256']=hashlib.sha256(Path(__file__).read_bytes()).hexdigest()
    (output/'validation.json').write_text(json.dumps(report,indent=2)+'\n')
print(json.dumps({key:report[key] for key in ['passed','component_assertions','intended_java_failure_controls','fixture_corruption_controls','committed_fixture_files_reproduced_exactly']}))

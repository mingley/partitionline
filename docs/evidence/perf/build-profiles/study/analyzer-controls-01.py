import ctypes
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import shutil
import signal
import subprocess
import time

s=Path(__file__).parent
source=Path('/workspace/work/open-cards-20261006/build-profiles-source-999133')
analysis=Path('/workspace/work/open-cards-20261006/build-profiles-analysis-8d32f5')
analyzer=analysis/'benchmarks/runtime/analyze-build-profiles.py'
root=s/'analyzer-controls-01';root.mkdir(exist_ok=False)
spec=importlib.util.spec_from_file_location('profile_analysis_owner',source/'scripts/run-benchmark-matrix.py')
owner=importlib.util.module_from_spec(spec);spec.loader.exec_module(owner)
if ctypes.CDLL(None,use_errno=True).prctl(36,1,0,0,0):raise OSError('subreaper unavailable')
def interrupt(signum,frame):raise InterruptedError('analysis owner interrupted')
for signum in (signal.SIGTERM,signal.SIGINT):signal.signal(signum,interrupt)
def sha(path):
 with Path(path).open('rb') as f:return hashlib.file_digest(f,'sha256').hexdigest()
inputs={**json.loads((s/'analysis-source-pins-01.json').read_text())['files']}
inputs.update({str(source/n):digest for n,digest in json.loads((s/'source-pins-01.json').read_text()).items()})
for family in ('nb','native'):
 for name in ('rows.json','completion.json','matrix-plan.json','host-before.json','host-after.json'):
  p=s/('compare-'+family+'-01')/name;inputs[str(p)]=sha(p)
def guard():
 assert all(sha(p)==digest for p,digest in inputs.items())
 assert not subprocess.check_output(['git','status','--porcelain'],cwd=source)
 assert not subprocess.check_output(['git','status','--porcelain'],cwd=analysis)
 assert subprocess.check_output(['git','rev-parse','HEAD'],cwd=analysis,text=True).strip()=='8d32f523dd98611e8f503dcc5667553ab8107327'
def run(nb,native,output,directory,label,expected):
 guard()
 argv=['python3','-B',str(source/'benchmarks/runtime/tools/parent-bound-exec.py'),str(os.getpid()),'python3','-B',str(analyzer),'--nb',str(nb),'--native',str(native),'--output',str(output)]
 try:owner.execute(argv,owner.base_env(),directory,label,30)
 except ValueError:
  if expected==0:raise
 finally:guard()
 receipt=json.loads((directory/(label+'.process.json')).read_text())
 assert receipt['parent_waited'] and receipt['exit_code']==expected and not receipt.get('failure'),receipt
 return receipt
normal_nb=s/'compare-nb-01';normal_native=s/'compare-native-01'
run(normal_nb,normal_native,s/'analysis-02.json',root,'positive',0)
results=[]
for control in ('changed_artifact_hash','missing_repetition','instrumented_comparison','changed_build_binding'):
 directory=root/control;directory.mkdir();nb=directory/'nb';native=directory/'native'
 for original,copy in ((normal_nb,nb),(normal_native,native)):
  copy.mkdir()
  for name in ('rows.json','completion.json','matrix-plan.json','host-before.json','host-after.json'):shutil.copy2(original/name,copy/name)
 rows=json.loads((nb/'rows.json').read_text())
 if control=='changed_artifact_hash':rows[0]['sha256']='0'*64
 elif control=='missing_repetition':rows.pop()
 elif control=='instrumented_comparison':
  for cohort in (nb,native):
   plan=json.loads((cohort/'matrix-plan.json').read_text());plan['configs'][-1]['instrumented_training']=True;(cohort/'matrix-plan.json').write_text(json.dumps(plan))
 else:
  artifact=Path(rows[0]['artifact']);copy=directory/'actual-artifact-copy';copy.mkdir()
  shutil.copy2(artifact,copy/artifact.name)
  binding=json.loads((artifact.parent/'benchmark-build.json').read_text());binding['build']['lto']='changed-control'
  (copy/'benchmark-build.json').write_text(json.dumps(binding));rows[0]['artifact']=str(copy/artifact.name)
 (nb/'rows.json').write_text(json.dumps(rows))
 receipt=run(nb,native,directory/'must-not-exist.json',directory,'reject',1)
 assert not (directory/'must-not-exist.json').exists()
 results.append(dict(control=control,rejected=True,receipt=receipt,stderr_sha256=sha(directory/'reject.stderr')))
with (root/'summary.json').open('x') as f:json.dump(dict(source_commit='8d32f523dd98611e8f503dcc5667553ab8107327',inputs_sha256=inputs,positive_qualified=True,controls=results,scope='Actual measured-matrix mutations; no timed workload repeated.'),f,indent=2);f.write('\n')
print('Owned positive analysis and four rejected actual-matrix controls passed')

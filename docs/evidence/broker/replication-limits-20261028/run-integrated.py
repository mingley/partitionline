#!/usr/bin/env python3
import hashlib,json,os,shutil,signal,subprocess,time
from pathlib import Path
root=Path('/workspace/partitionline'); study=Path('/workspace/work/goal-qualification-20261028'); here=Path('/workspace/work/replication-limits-candidate'); out=here/'integrated-validation-01'
if out.exists():raise SystemExit('output already exists')
out.mkdir()
def digest(p):
 h=hashlib.sha256()
 with p.open('rb') as f:
  for chunk in iter(lambda:f.read(1024*1024),b''):h.update(chunk)
 return h.hexdigest()
expected={'partitionline-broker/src/journal.rs':'2ff177e85969470e35ccdb22b8453a3350ee9f2e1931961a57ce2513496fd5bf','partitionline-broker/src/replication.rs':'a98dd6ba343491d66ef10004cb3184e5920f776c235abf8a72619e43d89cc0c0'}
if any(digest(root/path)!=sha for path,sha in expected.items()):raise SystemExit('candidate source hash mismatch')
base=subprocess.check_output(['git','rev-parse','HEAD'],cwd=root,text=True).strip(); status=subprocess.check_output(['git','status','--porcelain=v1','--untracked-files=all'],cwd=root,text=True)
for path in expected:(root/path).touch()
env=os.environ.copy();env['QUALIFICATION_PROCESS_TIMEOUT_SECS']='300'
cases=[('broker-replication-integrated-unit-01',['test','--locked','--manifest-path','partitionline-broker/Cargo.toml','--lib','--message-format=json-render-diagnostics','replication::tests','--','--test-threads=1']),('broker-replication-integrated-all-features-01',['test','--locked','--manifest-path','partitionline-broker/Cargo.toml','--all-features','--lib','--message-format=json-render-diagnostics','replication::tests','--','--test-threads=1']),('broker-replication-integrated-clippy-01',['clippy','--locked','--manifest-path','partitionline-broker/Cargo.toml','--all-features','--lib','--','-D','warnings'])]
summary={'head':base,'status_before':status,'expected_sources':expected,'runner_sha256':digest(Path(__file__)),'cases':[],'limitations':'dirty source snapshot; selected repo inputs only, shared target, external tools and environment not exhaustively pinned'}
(out/'summary.json').write_text(json.dumps(summary,indent=2)+'\n')
for name,args in cases:
 if shutil.disk_usage(root).free < 2*1024**3:summary['stop']='disk floor before case';break
 command=['python3',str(study/'run-v2.py'),name]+args;processlog=out/(name+'-driver.log');started=time.monotonic()
 with processlog.open('wb') as log:
  child=subprocess.Popen(command,cwd=root,env=env,stdout=log,stderr=subprocess.STDOUT,start_new_session=True)
  reason=None
  while child.poll() is None:
   if shutil.disk_usage(root).free < 2*1024**3:reason='disk floor';break
   if time.monotonic()-started > 330:reason='driver timeout';break
   time.sleep(1)
  if reason:
   os.killpg(child.pid,signal.SIGTERM)
   try:child.wait(timeout=10)
   except subprocess.TimeoutExpired:os.killpg(child.pid,signal.SIGKILL);child.wait()
 receipt_path=study/(name+'.json'); log_path=study/(name+'.log'); result={'name':name,'driver_exit':child.returncode,'stop_reason':reason,'driver_log_sha256':digest(processlog)}
 if receipt_path.exists():
  receipt=json.loads(receipt_path.read_text());result.update(exit=receipt.get('exit'),inputs_stable=receipt.get('inputs_stable'),results=receipt.get('results'),receipt_sha256=digest(receipt_path),log_sha256=digest(log_path))
  if args[0]=='test' and receipt.get('exit')==0:
   artifacts=[]
   for line in log_path.open(errors='replace'):
    if line.startswith('{'):
     try:obj=json.loads(line)
     except ValueError:continue
     if obj.get('reason')=='compiler-artifact' and obj.get('profile',{}).get('test') and obj.get('executable') and obj.get('target',{}).get('name')=='partitionline_broker':artifacts.append(obj)
   if len(artifacts)!=1 or artifacts[0].get('fresh') is not False:result['integrity_error']='did not report exactly one freshly built test executable'
   elif not any('test result: ok.' in s and ' 0 passed;' not in s for s in receipt.get('results',[])):result['integrity_error']='no passing test result'
   else:
    src=Path(artifacts[0]['executable']); sha=digest(src); dst=out/(name+'.elf')
    if shutil.disk_usage(root).free < src.stat().st_size+2*1024**3:result['integrity_error']='cannot retain binary above disk floor'
    else:
     shutil.copy2(src,dst)
     if digest(dst)!=sha:result['integrity_error']='copied binary hash mismatch'
     else:result['executable']={'path':str(dst),'sha256':sha,'fresh':False}
 summary['cases'].append(result);(out/'summary.json').write_text(json.dumps(summary,indent=2)+'\n')
 print(name,json.dumps(result),flush=True)
 if reason or result.get('integrity_error') or result.get('exit')!=0 or not result.get('inputs_stable'):break
summary['status_after']=subprocess.check_output(['git','status','--porcelain=v1','--untracked-files=all'],cwd=root,text=True);summary['head_after']=subprocess.check_output(['git','rev-parse','HEAD'],cwd=root,text=True).strip();summary['source_hashes_after']={path:digest(root/path) for path in expected};(out/'summary.json').write_text(json.dumps(summary,indent=2)+'\n')

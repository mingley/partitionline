#!/usr/bin/env python3
"""Execute ROOT-authorized remaining frozen cohorts sequentially. No builds/config edits."""
from pathlib import Path
import hashlib,json,os,shutil,signal,subprocess,sys,time
BASE=Path('/workspace/work/broker-oidc');SOURCE_SHA='fcac9d1d783b63890b3316601de72a4075973e10';FLOOR=350*1024*1024;RESERVE=48*1024*1024
MANIFEST=BASE/'live-configs-fcac9d1d-predeclared-01/manifest.json';BATCH=BASE/'live-runs-fcac9d1d-batch-01';BATCH.mkdir(mode=0o700,exist_ok=False)
RUNS=BASE/'live-runs-fcac9d1d-predeclared-01';SOURCE=Path('/workspace/work/broker-merged-source-fcac9d1d');results=[];interrupted=False
os.umask(0o077);os.sched_setaffinity(0,{0,1})
def sha(p):
 h=hashlib.sha256()
 with p.open('rb') as f:
  while b:=f.read(1048576):h.update(b)
 return h.hexdigest()
def require(v,reason):
 if not v:raise RuntimeError(reason)
def write(p,v):p.write_text(json.dumps(v,indent=2)+'\n');os.chmod(p,0o600)
def on_signal(signum,frame):
 global interrupted
 interrupted=True
signal.signal(signal.SIGTERM,on_signal);signal.signal(signal.SIGINT,on_signal)
manifest=json.loads(MANIFEST.read_text());require(sha(MANIFEST)=='0639fc4f66058a657d14f45db113347e451efc62a60f0e1d47953d75d8e45515' and manifest['source_sha']==SOURCE_SHA,'frozen manifest')
first=json.loads((RUNS/'ordinary-stable/validation.json').read_text());require(first['passed'],'first approved cohort already passed')
cohorts=[r for r in manifest['cohorts'] if r['name']!='ordinary-stable'];require(len(cohorts)==31,'exact remaining31')
producer_sha=sha(Path(__file__))
def save(failure=None):
 write(BATCH/'validation.json',{'source_sha':SOURCE_SHA,'producer_sha256':producer_sha,'frozen_manifest_sha256':sha(MANIFEST),'planned_remaining':31,'completed':len(results),'commands':results,'stopped_reason':failure,'scope':'sequential actual frozen livecohorts; no source/config/build mutation; SDK local/transport/native-unproved errors remain distinct from server protocol observations','passed':len(results)==31 and all(r['passed'] for r in results),'firstordinary_separate':str(RUNS/'ordinary-stable/validation.json'),'source_checks':'Each frozen driver performs complete73196 Gitblob/full07777/exactpathset guards before/after; validations and sourceaudit gzip retained per cohort. External artifact preflight/postflight bind bytes/fullmodes.'})
def artifact_check(row):
 require(sha(MANIFEST)=='0639fc4f66058a657d14f45db113347e451efc62a60f0e1d47953d75d8e45515','manifest unchanged')
 p=Path(row['config']);require(sha(p)==row['config_sha256'],'exact frozen config');v=json.loads(p.read_text());require(v['source_sha']==SOURCE_SHA,'actual immutable source pin')
 require(all(not os.environ.get(k) for k in ['LD_PRELOAD','LD_LIBRARY_PATH','JAVA_TOOL_OPTIONS','_JAVA_OPTIONS','JDK_JAVA_OPTIONS']),'loader/JVM overrides empty')
 count=0
 for kind in ['driver','outer','issuer']:require(sha(SOURCE/v[kind+'_relative_path'])==v[kind+'_sha256'],'actual frozen authority source')
 for peer in v['peers']:
  receipt=Path(peer['build_receipt']);require(sha(receipt)==peer['build_receipt_sha256'],'actual peer receipt bytes');bound=json.loads(receipt.read_text());require(bound['source_sha']==SOURCE_SHA and bound['exit_code']==0,'samepin actual peer build')
  for f in peer['runtime_inputs']:
   p=Path(f['path']);require(sha(p)==f['sha256'] and p.stat().st_size==f['bytes'] and oct(p.stat().st_mode&0o7777)==f['full_mode'],'runtime bytes/fullmode');count+=1
  for f in peer['source_inputs']:require(sha(SOURCE/f['path'])==f['sha256'],'actual peer inputsource')
 p=Path(v['normal_example_build_receipt']);require(sha(p)==v['normal_example_build_receipt_sha256'],'actual normal receipt');b=json.loads(p.read_text());p=Path(b['executable']);require(b['source_sha']==SOURCE_SHA and b['exit_code']==0 and b['actual_Cargo_artifact']['target']['kind']==['example'] and not b['actual_Cargo_artifact']['profile']['test'] and sha(p)==b['executable_sha256'] and p.stat().st_mode&0o7777==b['executable_full_mode']==0o700,'actual normal700 samepin non-testartifact')
 return {'passed':True,'runtime_rehashes':count,'config_sha256':row['config_sha256'],'normal_sha256':b['executable_sha256']}
save()
for ordinal,row in enumerate(cohorts,1):
 if interrupted:save('batch interruption before next cohort');break
 name=row['name'];output=RUNS/name;private=BASE/'private-live-fcac9d1d'/name
 require(not output.exists() and not private.exists(),'fresh cohort paths preserve all originals')
 before=artifact_check(row);free=shutil.disk_usage('/workspace').free;require(free-RESERVE>=FLOOR,'immediate48MiBforecast respects350MiBfloor')
 started=time.monotonic();samples=[];abort_reason=None;last_progress=started
 print(json.dumps({'event':'cohort-start','ordinal':ordinal,'total':31,'name':name,'steps':row['steps'],'free_bytes':free}),flush=True)
 log=BATCH/(name+'.outer.log')
 with log.open('wb') as handle:
  child=subprocess.Popen(row['outer_command'],stdout=handle,stderr=subprocess.STDOUT,start_new_session=True,umask=0o077)
  while child.poll() is None:
   now=time.monotonic();current_free=shutil.disk_usage('/workspace').free;samples.append({'elapsed_seconds':now-started,'free_bytes':current_free})
   if not abort_reason and (interrupted or current_free<FLOOR or now-started>310):
    abort_reason='batch-interrupted' if interrupted else 'disk-floor' if current_free<FLOOR else 'outer-observed-overrun'
    child.send_signal(signal.SIGTERM)  # Frozen outer's controlled handler owns all descendant group cleanup.
   if now-last_progress>=30:
    print(json.dumps({'event':'cohort-pending','ordinal':ordinal,'name':name,'elapsed_seconds':round(now-started,2),'free_bytes':current_free,'completed_prior':len(results)}),flush=True);last_progress=now
   time.sleep(.5)
  exit_code=child.wait()
 validation=json.loads((output/'validation.json').read_text()) if (output/'validation.json').exists() else None
 outer=json.loads((output/'outer-ownership.json').read_text()) if (output/'outer-ownership.json').exists() else None
 after=artifact_check(row)
 joined=bool(outer and not outer['remaining'] and validation and all(r['joined'] for r in validation['cleanup']))
 audit_log=BATCH/(name+'.mode-audit.log');audit_exit=None
 if joined:
  with audit_log.open('wb') as handle:
   audit_exit=subprocess.run([sys.executable,str(BASE/'audit-private-modes.py'),'--private-scratch',str(private),'--output',str(output/'private-mode-inventory.json')],stdout=handle,stderr=subprocess.STDOUT,timeout=20).returncode
 mode=json.loads((output/'private-mode-inventory.json').read_text()) if (output/'private-mode-inventory.json').exists() else None
 public_bytes=sum(p.stat().st_size for p in output.rglob('*') if p.is_file()) if output.exists() else 0
 passed=bool(exit_code==0 and not abort_reason and validation and validation['passed'] and validation['complete_source_unchanged'] and outer and outer['passed'] and joined and audit_exit==0 and mode and mode['passed'] and public_bytes<=16*1024*1024)
 result={'ordinal':ordinal,'name':name,'source_sha':SOURCE_SHA,'command':row['outer_command'],'steps_declared':row['steps'],'steps_observed':len(validation['steps']) if validation else 0,'exit_code':exit_code,'elapsed_seconds':time.monotonic()-started,'minimum_free_bytes':min((x['free_bytes'] for x in samples),default=free),'free_after':shutil.disk_usage('/workspace').free,'public_output_bytes':public_bytes,'artifact_before':before,'artifact_after':after,'complete_source_unchanged':bool(validation and validation['complete_source_unchanged']),'outer_passed':bool(outer and outer['passed']),'all_cleanup_joined':joined,'private_paths_only_audit_passed':bool(mode and mode['passed']),'private_path_count':mode['path_count'] if mode else None,'failure_stage':validation['failure_stage'] if validation else 'missing-validator','failed_step':validation['failed_step'] if validation else None,'abort_reason':abort_reason,'log_sha256':sha(log),'validation_path':str(output/'validation.json'),'validation_sha256':sha(output/'validation.json') if (output/'validation.json').exists() else None,'outer_sha256':sha(output/'outer-ownership.json') if (output/'outer-ownership.json').exists() else None,'mode_audit_sha256':sha(output/'private-mode-inventory.json') if (output/'private-mode-inventory.json').exists() else None,'disk_samples':samples,'passed':passed}
 results.append(result);save(None if passed else 'first actual failure; later cohorts held')
 print(json.dumps({'event':'cohort-closed','ordinal':ordinal,'name':name,'passed':passed,'steps':result['steps_observed'],'exit_code':exit_code,'failure_stage':result['failure_stage'],'failed_step':result['failed_step'],'elapsed_seconds':round(result['elapsed_seconds'],2),'free_bytes':result['free_after']}),flush=True)
 if not passed:break
require(sha(Path(__file__))==producer_sha,'actual batch producer unchanged');save(None if len(results)==31 and all(r['passed'] for r in results) else 'first failure/interruption; remaining held')
raise SystemExit(0 if len(results)==31 and all(r['passed'] for r in results) else 1)

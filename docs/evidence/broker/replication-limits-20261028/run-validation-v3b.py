#!/usr/bin/env python3
import hashlib,json,os,resource,shutil,signal,subprocess,time
from pathlib import Path

root=Path('/workspace/work/replication-limits-validation')
out=Path('/workspace/work/replication-limits-candidate/validation-v3b')
if out.exists(): raise SystemExit('output already exists')
out.mkdir()
items=['partitionline-broker','tests/conformance/broker','Cargo.toml','Cargo.lock','clippy.toml','rust-toolchain.toml','.cargo','src','LICENSE-APACHE','LICENSE-MIT','NOTICE','examples']
def digest(p):
 h=hashlib.sha256()
 with p.open('rb') as stream:
  for chunk in iter(lambda: stream.read(1024*1024),b''):h.update(chunk)
 return h.hexdigest()
def snapshot():
 result={}
 for item in items:
  p=root/item
  if p.is_dir():
   for f in sorted(p.rglob('*')):
    if f.is_file() and 'target' not in f.relative_to(p).parts:result[f.relative_to(root).as_posix()]=digest(f)
  else:result[item]=digest(p)
 return result
def resource_data():
 return {'disk_free':shutil.disk_usage(root).free,'memory_events':Path('/sys/fs/cgroup/memory.events').read_text()}

env=os.environ.copy()
env.update(RUSTUP_HOME='/workspace/work/rustup', CARGO_HOME='/workspace/work/cargo', PATH='/workspace/work/cargo/bin:'+env.get('PATH',''), CARGO_TARGET_DIR='/workspace/work/qualification-20261027/broker-check-target', CARGO_BUILD_JOBS='2', CARGO_INCREMENTAL='0', CARGO_PROFILE_DEV_DEBUG='0', CARGO_PROFILE_TEST_DEBUG='0')
for key in ('RUSTFLAGS','RUSTC_WRAPPER','RUSTC_WORKSPACE_WRAPPER','RUST_TEST_THREADS','RUST_MIN_STACK'):env.pop(key,None)
source=snapshot()
expected={'partitionline-broker/src/replication.rs':'a98dd6ba343491d66ef10004cb3184e5920f776c235abf8a72619e43d89cc0c0','partitionline-broker/src/journal.rs':'2ff177e85969470e35ccdb22b8453a3350ee9f2e1931961a57ce2513496fd5bf'}
if any(source.get(p)!=h for p,h in expected.items()):raise SystemExit('candidate hashes differ')
receipt={'source_snapshot':json.loads((root/'source-snapshot.json').read_text())['source_head'], 'before':source,'candidate_sha256':expected, 'runner_sha256':digest(Path(__file__)),'scope':items,'limits':'input capture excludes docs, ignored files, external dependencies, operating system and toolchain binaries; shared Cargo target', 'environment':{k:env[k] for k in ('RUSTUP_HOME','CARGO_HOME','CARGO_TARGET_DIR','CARGO_BUILD_JOBS','CARGO_INCREMENTAL','CARGO_PROFILE_DEV_DEBUG','CARGO_PROFILE_TEST_DEBUG')}, 'cases':[]}
receipt['rustc']=subprocess.check_output(['rustc','+stable','-vV'],env=env,text=True)
(out/'receipt.json').write_text(json.dumps(receipt,indent=2,sort_keys=True)+'\n')
cases=[('default',['cargo','+stable','test','--locked','--manifest-path','partitionline-broker/Cargo.toml','--lib','--message-format=json-render-diagnostics','replication::tests','--','--test-threads=1']),('all-features',['cargo','+stable','test','--locked','--manifest-path','partitionline-broker/Cargo.toml','--all-features','--lib','--message-format=json-render-diagnostics','replication::tests','--','--test-threads=1']),('clippy',['cargo','+stable','clippy','--locked','--manifest-path','partitionline-broker/Cargo.toml','--all-features','--lib','--','-D','warnings'])]
for name,cmd in cases:
 if shutil.disk_usage(root).free<2*1024**3:raise SystemExit('disk below 2 GiB floor')
 started=time.time(); before=resource_data(); log=out/(name+'.log')
 with log.open('wb') as f:
  child=subprocess.Popen(cmd,cwd=root,env=env,stdout=f,stderr=subprocess.STDOUT,start_new_session=True)
  reason=None
  while child.poll() is None:
   if time.time()-started>300:reason='timeout';break
   if shutil.disk_usage(root).free<2*1024**3:reason='disk_floor';break
   time.sleep(1)
  if reason:
   os.killpg(child.pid,signal.SIGTERM)
   try:child.wait(timeout=10)
   except subprocess.TimeoutExpired:os.killpg(child.pid,signal.SIGKILL);child.wait()
 exit_code=child.returncode; artifacts=[]; running_lines=[]
 with log.open(errors='replace') as f:
  for line in f:
   if line.startswith('{'):
    try:obj=json.loads(line)
    except ValueError:continue
    if obj.get('reason')=='compiler-artifact' and obj.get('profile',{}).get('test') and obj.get('executable'):
     path=Path(obj['executable']); artifacts.append({'path':str(path),'sha256':digest(path),'fresh':obj.get('fresh'),'target':obj.get('target',{}).get('name')})
   elif line.startswith('running ') or line.startswith('test result:'):running_lines.append(line.strip())
  
 test_artifacts=[a for a in artifacts if a['target']=='partitionline_broker']
 integrity_error=None
 if name!='clippy' and exit_code==0:
  if len(test_artifacts)!=1 or test_artifacts[0]['fresh'] is not False:
   integrity_error='Cargo did not report exactly one freshly built library test executable'
  elif not any(line.startswith('running ') and not line.startswith('running 0 tests') for line in running_lines):
   integrity_error='test filter selected no cases'
  else:
   artifact=test_artifacts[0]; dest=out/(name+'-lib.elf')
   if shutil.disk_usage(root).free - Path(artifact['path']).stat().st_size < 2*1024**3:
    artifact['retained']=None;artifact['retention_reason']='disk floor'
   else:
    shutil.copy2(artifact['path'],dest)
    if digest(dest)!=artifact['sha256']:integrity_error='saved executable hash mismatch'
    else:artifact['retained']=str(dest)
 after=snapshot(); stable=after==source
 case={'name':name,'command':cmd,'exit':exit_code,'stop_reason':reason,'elapsed_seconds':time.time()-started,'resources_before':before,'resources_after':resource_data(),'log_sha256':digest(log),'artifacts':artifacts,'test_results':running_lines,'inputs_stable':stable,'integrity_error':integrity_error}
 receipt['cases'].append(case);(out/'receipt.json').write_text(json.dumps(receipt,indent=2,sort_keys=True)+'\n')
 print(name,'exit',exit_code,'reason',reason,'stable',stable,'results',running_lines,flush=True)
 if not stable or reason or integrity_error or exit_code != 0:break
receipt['after']=snapshot();receipt['inputs_stable']=receipt['before']==receipt['after'];(out/'receipt.json').write_text(json.dumps(receipt,indent=2,sort_keys=True)+'\n')

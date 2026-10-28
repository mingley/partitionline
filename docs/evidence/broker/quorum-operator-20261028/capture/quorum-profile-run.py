import datetime, hashlib, json, os, re, shutil, subprocess, sys, time
from pathlib import Path
root=Path('/workspace/work/partitionline-quorum')
out=Path('/workspace/work/quorum-profile-runs'); out.mkdir(exist_ok=True)
name=sys.argv[1]; command=['cargo','+stable',*sys.argv[2:]]
log=out/(name+'.log'); receipt=out/(name+'.json')
if log.exists() or receipt.exists(): raise SystemExit('refusing to overwrite')
env=dict(os.environ, RUSTUP_HOME='/workspace/work/rustup', CARGO_HOME='/workspace/work/cargo', PATH='/workspace/work/cargo/bin:'+os.environ['PATH'],CARGO_TARGET_DIR='/workspace/work/qualification-20261027/broker-check-target',CARGO_BUILD_JOBS='2',CARGO_INCREMENTAL='0',CARGO_PROFILE_DEV_DEBUG='0',CARGO_PROFILE_TEST_DEBUG='0')
def inputs():
 files=[p for p in (root/'partitionline-broker').rglob('*') if p.is_file()]
 files += [p for prefix in ('src','examples','tests/conformance/broker') for p in (root/prefix).rglob('*') if p.is_file()]
 files += [root/n for n in ('Cargo.toml','Cargo.lock','rust-toolchain.toml') if (root/n).is_file()]
 return {str(p.relative_to(root)):hashlib.file_digest(p.open('rb'),'sha256').hexdigest() for p in sorted(files)}
before=inputs(); head=subprocess.check_output(['git','rev-parse','HEAD'],cwd=root,text=True).strip(); start=time.monotonic()
r={'command':command,'cwd':str(root),'base_commit':head,'exact_commit':None,'input_sha256':before,'started_utc':datetime.datetime.now(datetime.timezone.utc).isoformat(),'toolchain':subprocess.check_output(['rustc','+stable','-vV'],env=env,text=True),'build_jobs':2,'memory_events_before':Path('/sys/fs/cgroup/memory.events').read_text()}
print('Running',name,flush=True)
with log.open('x') as f: result=subprocess.run(command,cwd=root,env=env,stdout=f,stderr=subprocess.STDOUT)
after=inputs(); text=log.read_text();r.update(exit=result.returncode,elapsed_seconds=time.monotonic()-start,source_stable=before==after,changed_inputs=sorted(k for k in before.keys()|after.keys() if before.get(k)!=after.get(k)),log_sha256=hashlib.sha256(log.read_bytes()).hexdigest(),results=re.findall(r'^test result:.*$',text,re.M),failures=re.findall(r'^test (.*?) \.\.\. FAILED$',text,re.M),memory_events_after=Path('/sys/fs/cgroup/memory.events').read_text())
r['executables']=[]
for match in re.finditer(r'Running .*?\((/[^)]+)\)',text):
 p=Path(match[1]);
 if p.is_file():
  digest=hashlib.file_digest(p.open('rb'),'sha256').hexdigest(); target=out/(name+'-'+str(len(r['executables']))+'.elf'); shutil.copyfile(p,target); assert hashlib.file_digest(target.open('rb'),'sha256').hexdigest()==digest
  r['executables'].append({'source':str(p),'retained':str(target),'sha256':digest})
receipt.write_text(json.dumps(r,indent=2)+'\n'); print(json.dumps({k:r[k] for k in ['exit','elapsed_seconds','source_stable','changed_inputs','results','failures']}),flush=True)

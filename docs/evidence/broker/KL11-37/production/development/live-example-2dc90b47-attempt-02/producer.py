import gzip,hashlib,json,os,shutil,signal,subprocess,time
from pathlib import Path
REPO=Path('/workspace/partitionline');TREE=Path('/workspace/work/broker-oidc/live-example-base-2dc90b47');SHA='2dc90b471b384e88741a3d8db97963530a754d97';OUT=REPO/'docs/evidence/broker/KL11-37/production/development/live-example-2dc90b47-attempt-02';TARGET=Path('/workspace/work/target-broker-segments');OUT.mkdir(parents=True,exist_ok=False)
name='partitionline-broker/examples/oidc_live_probe.rs'; overlay=TREE/name;assert overlay.is_file();overlay.parent.mkdir(parents=True,exist_ok=True);shutil.copyfile(REPO/name,overlay);overlay.chmod(0o644);overlay_sha=hashlib.sha256(overlay.read_bytes()).hexdigest()
os.sched_setaffinity(0,{2,4})
env=dict(os.environ,CARGO_HOME='/workspace/work/cargo',RUSTUP_HOME='/workspace/work/rustup',PATH='/workspace/work/cargo/bin:'+os.environ['PATH'],CARGO_INCREMENTAL='0',CARGO_BUILD_JOBS='1',CARGO_PROFILE_DEV_DEBUG='0',CARGO_PROFILE_TEST_DEBUG='0',CARGO_TARGET_DIR=str(TARGET))
rows=[]
for p in sorted(TREE.rglob('*')):
 if p.is_file():
  assert not p.is_symlink();data=p.read_bytes();rows.append({'path':str(p.relative_to(TREE)),'sha256':hashlib.sha256(data).hexdigest(),'mode':p.stat().st_mode&0o7777,'bytes':len(data)})
raw=(json.dumps({'source_sha':SHA,'scope':'complete Git tree plus only new example overlay','files':rows},indent=2)+'\n').encode();raw_path=Path('/workspace/work/broker-oidc/live-example-source-audit-2dc90b47-attempt-02.json');raw_path.write_bytes(raw);raw_path.chmod(0o600);compressed=gzip.compress(raw,mtime=0);(OUT/'source-before.json.gz').write_bytes(compressed);assert gzip.decompress(compressed)==raw
receipt={'source_sha':SHA,'qualification':'development complete2dc source plus one example overlay; check/lint only, no behavior/live/full51 qualification','example_sha256':overlay_sha,'source_files':len(rows),'source_materialization_receipt':str(Path('/workspace/work/integration/live-example-source-2dc90b47/receipt.json')),'source_materialization_sha256':hashlib.sha256(Path('/workspace/work/integration/live-example-source-2dc90b47/receipt.json').read_bytes()).hexdigest(),'audit_restore':{'raw_path':str(raw_path),'raw_sha256':hashlib.sha256(raw).hexdigest(),'raw_bytes':len(raw),'raw_mode':'0o600','stored_path':'source-before.json.gz','gzip_sha256':hashlib.sha256(compressed).hexdigest(),'roundtrip':True},'checks':[],'commands':[],'elfs':[],'toolchains':{},'environment':{k:env[k]for k in ['CARGO_HOME','RUSTUP_HOME','CARGO_TARGET_DIR','CARGO_INCREMENTAL','CARGO_BUILD_JOBS','CARGO_PROFILE_DEV_DEBUG','CARGO_PROFILE_TEST_DEBUG']}}
objects={};snapshots={};object_dir=Path('/workspace/work/broker-oidc/live-example-ELF-retention-2dc90b47-attempt-02');object_dir.mkdir(exist_ok=False)
def save():
 (OUT/'validation.json').write_text(json.dumps(receipt,indent=2)+'\n')
def verify(label,phase):
 actual={str(p.relative_to(TREE))for p in TREE.rglob('*')if p.is_file()};assert actual=={r['path']for r in rows};h=hashlib.sha256()
 for row in rows:
  p=TREE/row['path'];digest=hashlib.sha256(p.read_bytes()).hexdigest();mode=p.stat().st_mode&0o7777;assert digest==row['sha256']and mode==row['mode'],row['path'];h.update((row['path']+'\0'+digest+'\0'+str(mode)+'\n').encode())
 receipt['checks'].append({'command':label,'phase':phase,'files':len(rows),'aggregate_sha256':h.hexdigest(),'bytes_and_full_modes_match':True});save()
def retain(label):
 records=[]
 if TARGET.exists():
  for p in sorted(TARGET.rglob('*')):
   if not p.is_file()or p.is_symlink():continue
   with p.open('rb')as s:
    if s.read(4)!=b'\x7fELF':continue
   st=p.stat();ident=(st.st_dev,st.st_ino,st.st_size,st.st_mtime_ns,st.st_ctime_ns,st.st_mode)
   if snapshots.get(str(p),{}).get('identity')==ident:digest=snapshots[str(p)]['sha256']
   else:
    data=p.read_bytes();assert ident==(lambda t:(t.st_dev,t.st_ino,t.st_size,t.st_mtime_ns,t.st_ctime_ns,t.st_mode))(p.stat());digest=hashlib.sha256(data).hexdigest();dest=object_dir/(digest+'.gz')
    if not dest.exists():dest.write_bytes(gzip.compress(data,mtime=0))
    assert gzip.decompress(dest.read_bytes())==data;objects[digest]={'retained_path':str(dest),'sha256':digest,'bytes':len(data),'gzip_sha256':hashlib.sha256(dest.read_bytes()).hexdigest(),'roundtrip':True};snapshots[str(p)]={'identity':ident,'sha256':digest}
   records.append({'path':str(p),'sha256':digest,'mode':st.st_mode&0o7777,'bytes':st.st_size})
 receipt['elfs'].append({'after_command':label,'all_current_ELF_files':records});receipt['retained_objects']=list(objects.values());save()
# Reuse only own inactive retained target from the prior failed candidate.
commands=[]
for tc in ['stable','1.85.0']:
 receipt['toolchains'][tc]=subprocess.check_output(['rustc','+'+tc,'-vV'],env=env,text=True)
 for profile,flags in [('default',[]),('all-features',['--all-features'])]:
  common=['--manifest-path','partitionline-broker/Cargo.toml','--offline','--locked','--example','oidc_live_probe']+flags
  commands.extend([(tc+'-'+profile+'-check',['cargo','+'+tc,'check']+common),(tc+'-'+profile+'-clippy',['cargo','+'+tc,'clippy']+common+['--','-Dwarnings'])])
 commands.append((tc+'-fmt',['cargo','+'+tc,'fmt','--manifest-path','partitionline-broker/Cargo.toml','--','--check']))
for label,argv in commands:
 free=shutil.disk_usage('/workspace').free
 if free<350*1024*1024:receipt['pre_command_hold']={'command':label,'free':free};save();raise SystemExit('disk guard before command')
 verify(label,'before');print('START',label,flush=True);log=OUT/(label+'.log');minimum=free;started=time.monotonic();samples=[];interrupted=False
 with log.open('wb')as stream:
  child=subprocess.Popen(['taskset','-c','2,4']+argv,cwd=TREE,env=env,stdout=stream,stderr=subprocess.STDOUT,start_new_session=True)
  while child.poll()is None:
   free=shutil.disk_usage('/workspace').free;minimum=min(minimum,free);samples.append({'elapsed_s':round(time.monotonic()-started,3),'free':free})
   (OUT/(label+'-disk.json')).write_text(json.dumps({'minimum_free':minimum,'sampling_s':0.5,'samples':samples,'interrupted':interrupted})+'\n')
   if free<350*1024*1024:
    interrupted=True;os.killpg(child.pid,signal.SIGTERM)
    try:child.wait(timeout=5)
    except subprocess.TimeoutExpired:os.killpg(child.pid,signal.SIGKILL);child.wait()
    break
   time.sleep(0.5)
 (OUT/(label+'-disk.json')).write_text(json.dumps({'minimum_free':minimum,'sampling_s':0.5,'samples':samples,'interrupted':interrupted})+'\n')
 receipt['commands'].append({'name':label,'argv':['taskset','-c','2,4']+argv,'exit_code':child.returncode,'log':log.name,'log_sha256':hashlib.sha256(log.read_bytes()).hexdigest(),'duration_s':round(time.monotonic()-started,3),'minimum_sampled_free':minimum,'disk_interrupted':interrupted});retain(label);verify(label,'after');print('END',label,child.returncode,flush=True)
 if child.returncode or interrupted:raise SystemExit(1)
receipt['all_pass']=True;receipt['command_count']=len(commands);receipt['behavior_cases']=0;receipt['live_qualification']=False;receipt['example_after_sha256']=hashlib.sha256(overlay.read_bytes()).hexdigest();assert receipt['example_after_sha256']==overlay_sha;save();print('PASS10 check/lint/format only',flush=True)

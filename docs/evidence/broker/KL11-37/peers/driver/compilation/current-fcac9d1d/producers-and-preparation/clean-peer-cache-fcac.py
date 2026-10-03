#!/usr/bin/env python3
"""Remove only explicitly delegated inactive generated cache after exact retention."""
import gzip,hashlib,importlib.util,json,os,pathlib,shutil,subprocess,sys,time
sys.dont_write_bytecode=True
P=pathlib.Path;TARGET=P('/workspace/work/broker-oidc/live-peers-target');SOURCE=P('/workspace/work/broker-merged-source-fcac9d1d');BUILD=P('/workspace/work/broker-oidc/peer-builds-fcac9d1d-attempt-03');OUT=BUILD/'cache-maintenance-01';FLOOR=350*1024*1024
OUT.mkdir(mode=0o700,exist_ok=False)

def require(x,label):
 if not x:raise RuntimeError(label)

def sha(p):
 h=hashlib.sha256()
 with P(p).open('rb') as f:
  while data:=f.read(1048576):h.update(data)
 return h.hexdigest()

def write(p,v):p.write_text(json.dumps(v,indent=2)+'\n')

def owners():
 found=[];prefix=str(TARGET)
 for e in P('/proc').iterdir():
  if not e.name.isdigit() or int(e.name)==os.getpid():continue
  categories=set()
  for kind in ['cwd','exe']:
   try:value=os.readlink(e/kind).removesuffix(' (deleted)');categories.update([kind] if value==prefix or value.startswith(prefix+'/') else [])
   except OSError:pass
  try:
   if prefix.encode() in (e/'cmdline').read_bytes():categories.add('argv target reference')
  except OSError:pass
  try:
   if b'CARGO_TARGET_DIR='+prefix.encode() in (e/'environ').read_bytes().split(b'\0'):categories.add('CARGO_TARGET_DIR')
  except OSError:pass
  try:
   if prefix+'/' in (e/'maps').read_text():categories.add('maps')
  except OSError:pass
  try:
   for fd in (e/'fd').iterdir():
    try:value=os.readlink(fd).removesuffix(' (deleted)');categories.update(['fd'] if value==prefix or value.startswith(prefix+'/') else [])
    except OSError:pass
  except OSError:pass
  if categories:found.append({'pid':int(e.name),'categories':sorted(categories)})
 require(not found,'no active cache references');return found

config_manifest=P('/workspace/work/broker-oidc/live-configs-fcac9d1d-predeclared-01/manifest.json');configs=json.loads(config_manifest.read_text())
def config_rehash():
 count=0
 for r in configs['cohorts']:
  p=P(r['config']);require(sha(p)==r['config_sha256'],'frozen actual config bytes');x=json.loads(p.read_text())
  for peer in x['peers']:
   require(not P(peer['build_receipt']).is_relative_to(TARGET),'receipt outside removed cache');require(sha(peer['build_receipt'])==peer['build_receipt_sha256'],'peer receipt bytes')
   for f in peer['runtime_inputs']:
    require(not P(f['path']).is_relative_to(TARGET),'runtime input outside removed cache');require(sha(f['path'])==f['sha256'] and P(f['path']).stat().st_size==f['bytes'] and oct(P(f['path']).stat().st_mode&0o7777)==f['full_mode'],'actual runtime input bytes/fullmode');count+=1
  normal=P(x['normal_example_build_receipt']);require(sha(normal)==x['normal_example_build_receipt_sha256'],'normal receipt bytes');n=json.loads(normal.read_text());require(not P(n['executable']).is_relative_to(TARGET) and sha(n['executable'])==n['executable_sha256'] and P(n['executable']).stat().st_mode&0o7777==n['executable_full_mode'],'normal executable bytes/fullmode outside removed cache')
 for tool in ['stable','1.85.0']:require((BUILD/'bin'/tool/'rust-peer').stat().st_mode&0o7777==0o700,'copied Rust executable0700')
 return {'configs':32,'runtime_rehashes':count,'outside_removed_tree':True,'rust_copies0700':True}

spec=importlib.util.spec_from_file_location('immutable_guard',SOURCE/'docs/evidence/broker/KL11-37/peers/driver/run-live.py');module=importlib.util.module_from_spec(spec);spec.loader.exec_module(module)
guard=module.Run({'source_sha':configs['source_sha'],'source_root':str(SOURCE),'git_repository':'/workspace/partitionline'},OUT,OUT.parent/'unused-private-maintenance')
before=guard.source_guard();first_owners=owners();before_disk=shutil.disk_usage('/workspace').free
require(TARGET.resolve()==TARGET and TARGET.is_dir() and not TARGET.is_symlink(),'exact authorized generated directory')
existing={r['original_sha256']:r for r in json.loads((BUILD/'validation.json').read_text())['retained_elf_objects']}
files=[];whole=[]
for p in sorted(TARGET.rglob('*')):
 if p.is_symlink():whole.append({'path':str(p),'symlink':os.readlink(p),'full_mode':oct(p.lstat().st_mode&0o7777)});continue
 if not p.is_file():continue
 with p.open('rb') as f:magic=f.read(8)
 row={'path':str(p),'bytes':p.stat().st_size,'full_mode':oct(p.stat().st_mode&0o7777),'sha256':sha(p)};whole.append(row)
 if magic.startswith(b'\x7fELF') or magic.startswith(b'!<arch>\n') or p.suffix in ['.a','.rlib','.rmeta','.o']:
  row['kind']='ELF' if magic.startswith(b'\x7fELF') else 'AR' if magic.startswith(b'!<arch>\n') else p.suffix
  files.append(row)
unknown={r['sha256']:r for r in files if r['sha256'] not in existing}
forecast=sum(r['bytes']+r['bytes']//1000+1024 for r in unknown.values())+1048576
require(before_disk>=FLOOR+forecast,'worst-case retention forecast keeps350MiB floor')
objects=OUT/'objects';objects.mkdir(mode=0o700);min_disk=before_disk
for digest,row in unknown.items():
 require(shutil.disk_usage('/workspace').free>=FLOOR,'disk floor during retention')
 dst=objects/(digest+'.gz')
 with P(row['path']).open('rb') as source,dst.open('wb') as target:
  with gzip.GzipFile(fileobj=target,mode='wb',mtime=0) as encoded:shutil.copyfileobj(source,encoded,1048576)
 existing[digest]={'path':str(dst),'gzip_sha256':sha(dst),'original_sha256':digest,'bytes':row['bytes'],'roundtrip_verified':True}
 min_disk=min(min_disk,shutil.disk_usage('/workspace').free)
# Verify every unique preserved object and each actual byte/fullmode before removal.
for digest in {r['sha256'] for r in files}:
 obj=existing[digest];require(sha(obj['path'])==obj['gzip_sha256'],'preserved encoded bytes');h=hashlib.sha256();total=0
 with gzip.open(obj['path'],'rb') as f:
  while chunk:=f.read(1048576):h.update(chunk);total+=len(chunk)
 require(h.hexdigest()==digest and total==obj['bytes'],'lossless compiled output restore bytes')
for row in files:
 p=P(row['path']);require(sha(p)==row['sha256'] and oct(p.stat().st_mode&0o7777)==row['full_mode'],'compiled output unchanged');row['object']=existing[row['sha256']]
write(OUT/'compiled-output-restore-map.json',{'files':files,'all_unique_objects_roundtrip_verified':True});write(OUT/'whole-cache-inventory.json',{'files':whole})
rehash_before=config_rehash();final_owners=owners();argv=['/usr/bin/rm','-rf','--',str(TARGET)];began=time.monotonic();r=subprocess.run(argv,capture_output=True,timeout=60)
(OUT/'cleanup.stdout').write_bytes(r.stdout);(OUT/'cleanup.stderr').write_bytes(r.stderr)
require(r.returncode==0 and not TARGET.exists(),'only own cache removed');rehash_after=config_rehash();after=guard.source_guard();require(before==after,'complete immutable source unchanged during maintenance')
receipt={'source_sha':configs['source_sha'],'passed':True,'authorization':'root explicit inactiveowned cache-only maintenance','command':argv,'exit_code':r.returncode,'elapsed_seconds':time.monotonic()-began,'compiled_output_paths':len(files),'unique_preserved_outputs':len({r['sha256'] for r in files}),'kind_counts':{k:sum(r['kind']==k for r in files) for k in sorted({r['kind'] for r in files})},'generated_cache_file_count':len(whole),'generated_cache_bytes':sum(r.get('bytes',0) for r in whole),'owners_before':first_owners,'owners_immediately_before_remove':final_owners,'before_free_bytes':before_disk,'worst_case_retention_forecast_bytes':forecast,'minimum_free_during_retention':min_disk,'after_free_bytes':shutil.disk_usage('/workspace').free,'live_config_rehash_before':rehash_before,'live_config_rehash_after':rehash_after,'source_guards':{'files':len(before),'before_equals_after':before==after,'scope':'complete Git blobs/full07777/exact actual regular-symlink pathset'},'restore_map_sha256':sha(OUT/'compiled-output-restore-map.json'),'whole_cache_inventory_sha256':sha(OUT/'whole-cache-inventory.json'),'cleanup_stdout_sha256':sha(OUT/'cleanup.stdout'),'cleanup_stderr_sha256':sha(OUT/'cleanup.stderr'),'private_proof_sources_and_external_executables':'untouched'}
write(OUT/'validation.json',receipt);print(json.dumps({'path':str(OUT/'validation.json'),'sha256':sha(OUT/'validation.json'),'outputs':len(files),'after_free':receipt['after_free_bytes']}))

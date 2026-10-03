from pathlib import Path
import hashlib,importlib.util,json,os,shutil,sys
sys.dont_write_bytecode=True
B=Path('/workspace/work/broker-oidc');OUT=B/'live-configs-fcac9d1d-expired-focused-01';OUT.mkdir(mode=0o700,exist_ok=False);SOURCE=Path('/workspace/work/broker-merged-source-fcac9d1d')
def sha(p):return hashlib.sha256(p.read_bytes()).hexdigest()
recipe=B/'signed-expired-recipe-revision-01/expired-stable-java-4-1-2-focused-01.recipe.json';assert sha(recipe)=='a46bed886f1b32d50db540ffc1ccb88e142fda1d734f683500efdc53b2550c14';r=json.loads(recipe.read_text());assert len(r['steps'])==12
original=B/'live-configs-fcac9d1d-predeclared-01/signed-stable-java-4-1-2.json';original_bytes=original.read_bytes();cfg=json.loads(original_bytes);assert cfg['source_sha']=='fcac9d1d783b63890b3316601de72a4075973e10' and cfg['profile']=='metadata' and [p['id'] for p in cfg['peers']]==['java-4-1-2'];cfg['steps']=r['steps']
for kind in ['driver','outer','issuer']:assert sha(SOURCE/cfg[kind+'_relative_path'])==cfg[kind+'_sha256']
count=0
for peer in cfg['peers']:
 p=Path(peer['build_receipt']);assert sha(p)==peer['build_receipt_sha256'];v=json.loads(p.read_text());assert v['source_sha']==cfg['source_sha'] and v['exit_code']==0
 for f in peer['runtime_inputs']:
  p=Path(f['path']);assert sha(p)==f['sha256'] and p.stat().st_size==f['bytes'] and oct(p.stat().st_mode&0o7777)==f['full_mode'];count+=1
 for f in peer['source_inputs']:assert sha(SOURCE/f['path'])==f['sha256']
p=Path(cfg['normal_example_build_receipt']);assert sha(p)==cfg['normal_example_build_receipt_sha256'];n=json.loads(p.read_text());p=Path(n['executable']);assert n['source_sha']==cfg['source_sha'] and sha(p)==n['executable_sha256'] and p.stat().st_mode&0o7777==n['executable_full_mode']==0o700 and not n['actual_Cargo_artifact']['profile']['test']
spec=importlib.util.spec_from_file_location('frozen_focused_source_guard',SOURCE/cfg['driver_relative_path']);module=importlib.util.module_from_spec(spec);spec.loader.exec_module(module);guard=module.Run(cfg,OUT,B/'focused-private-not-created');before=guard.source_guard();assert len(before)==73196
path=OUT/(r['name']+'.json');path.write_text(json.dumps(cfg,separators=(',',':'))+'\n');os.chmod(path,0o600);assert path.stat().st_size<=65536
after=guard.source_guard();assert before==after and original.read_bytes()==original_bytes
output=B/'live-runs-fcac9d1d-expired-focused-01'/r['name'];private=B/'private-live-fcac9d1d-expired-focused-01'/r['name'];assert not output.exists() and not private.exists()
row={'name':r['name'],'config':str(path),'config_sha256':sha(path),'config_bytes':path.stat().st_size,'config_mode':oct(path.stat().st_mode&0o7777),'steps':12,'profile':'metadata','source_sha':cfg['source_sha'],'recipe':str(recipe),'recipe_sha256':sha(recipe),'normal_binding':cfg['normal_example_build_receipt'],'normal_binding_sha256':cfg['normal_example_build_receipt_sha256'],'runtime_rehashes':count,'source_guard':{'files':len(before),'before_equals_after':True,'full07777_and_exact_pathset':True},'original32config_recipe_untouched':True,'outer_command':['taskset','-c','0,1',sys.executable,str(SOURCE/cfg['outer_relative_path']),'--config',str(path),'--output',str(output),'--private-scratch',str(private)],'required_negative_witness':r['steps'][6]['authority_witness'],'scope':'Focusedrevision; providerREADY is separatefrom demanded broker-authmetadatafailure with actualHTTPSsignedexpiredtoken condition and positivebaseline/recovery. No newlive execution.','execution':'helduntilrootGO afterAvrolease','free_bytes':shutil.disk_usage('/workspace').free}
p=OUT/'manifest.json';p.write_text(json.dumps(row,indent=2)+'\n');os.chmod(p,0o600);print(json.dumps({'manifest':str(p),'sha256':sha(p),'config_sha256':sha(path),'config_bytes':path.stat().st_size,'source_files':len(before),'runtime_rehashes':count}))

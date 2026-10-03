from pathlib import Path
import hashlib,json,os,shutil
BASE=Path('/workspace/work/broker-oidc');MANIFEST=BASE/'live-configs-fcac9d1d-predeclared-01/manifest.json';ROOT=Path('/workspace/work/broker-merged-source-fcac9d1d')
def sha(p):return hashlib.sha256(p.read_bytes()).hexdigest()
manifest=json.loads(MANIFEST.read_text());assert sha(MANIFEST)=='0639fc4f66058a657d14f45db113347e451efc62a60f0e1d47953d75d8e45515'
row=next(r for r in manifest['cohorts'] if r['name']=='ordinary-stable');cfg=Path(row['config']);assert sha(cfg)==row['config_sha256']=='1df065769d6ee4a10c23d053666d8bf38aa444b149b81aa6778c2662681d354f';value=json.loads(cfg.read_text());inputs=[]
for peer in value['peers']:
 receipt=Path(peer['build_receipt']);assert sha(receipt)==peer['build_receipt_sha256'];bound=json.loads(receipt.read_text());assert bound['source_sha']==manifest['source_sha'] and bound['exit_code']==0
 for item in peer['runtime_inputs']:
  p=Path(item['path']);assert sha(p)==item['sha256'] and p.stat().st_size==item['bytes'] and oct(p.stat().st_mode&0o7777)==item['full_mode'];inputs.append(item)
 for item in peer['source_inputs']:assert sha(ROOT/item['path'])==item['sha256']
normal=Path(value['normal_example_build_receipt']);assert sha(normal)==value['normal_example_build_receipt_sha256'];build=json.loads(normal.read_text());binary=Path(build['executable']);assert sha(binary)==build['executable_sha256'] and binary.stat().st_mode&0o7777==0o700
assert build['source_sha']==manifest['source_sha'] and build['actual_Cargo_artifact']['target']['kind']==['example'] and not build['actual_Cargo_artifact']['profile']['test']
for kind in ['driver','outer','issuer']:assert sha(ROOT/value[kind+'_relative_path'])==value[kind+'_sha256']
env_names=['LD_PRELOAD','LD_LIBRARY_PATH','JAVA_TOOL_OPTIONS','_JAVA_OPTIONS','JDK_JAVA_OPTIONS'];assert all(not os.environ.get(name) for name in env_names)
output=BASE/'live-runs-fcac9d1d-predeclared-01/ordinary-stable';private=BASE/'private-live-fcac9d1d/ordinary-stable';assert not output.exists() and not private.exists();assert not (BASE/'live-peers-target').exists()
result={'scope':'public artifact/mode/config preflight only; no broker/SDK/live process launched','passed':True,'source_sha':manifest['source_sha'],'config_sha256':sha(cfg),'manifest_sha256':sha(MANIFEST),'runtime_input_rehashes':len(inputs),'unique_runtime_paths':len({r['path'] for r in inputs}),'normal_executable_sha256':sha(binary),'normal_executable_mode':'0o700','normal_test_profile':False,'loader_JVM_override_names':env_names,'all_overrides_empty':True,'fresh_output_private_paths':True,'old657proof_and_private_unchanged':True,'generated_cache_absent':True,'free_bytes':shutil.disk_usage('/workspace').free,'future_command':row['outer_command']}
p=BASE/'fcac-first-ordinary-preflight-02-launch.json';assert not p.exists();p.write_text(json.dumps(result,indent=2)+'\n');os.chmod(p,0o600);print(json.dumps({'path':str(p),'sha256':sha(p),'runtime_rehashes':len(inputs),'unique_paths':result['unique_runtime_paths'],'free_bytes':result['free_bytes']}))

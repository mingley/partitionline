#!/usr/bin/env python3
"""Bind predeclared recipes to completed immutable compiler inputs. No launch."""
import hashlib,importlib.util,json,os,pathlib,sys
sys.dont_write_bytecode=True
P=pathlib.Path
SOURCE=P('/workspace/work/broker-merged-source-fcac9d1d')
SHA='fcac9d1d783b63890b3316601de72a4075973e10'
PREP=P('/workspace/work/broker-oidc/live-config-preparation-f500')
BIND=P('/workspace/work/broker-oidc/peer-builds-fcac9d1d-attempt-03/build-bindings-01')
OUTPUT=P('/workspace/work/broker-oidc/live-configs-fcac9d1d-predeclared-01')
OUTPUT.mkdir(mode=0o700,exist_ok=False)

def sha(p):
 h=hashlib.sha256()
 with P(p).open('rb') as f:
  while b:=f.read(1048576):h.update(b)
 return h.hexdigest()

def write(p,v,compact=False):
 p.write_text(json.dumps(v,separators=(',',':') if compact else None,indent=None if compact else 2)+'\n');os.chmod(p,0o600)

manifest=json.loads((PREP/'manifest.json').read_text());assert manifest['run_count']==32
peers=json.loads((BIND/'peers.json').read_text());proof=json.loads((BIND/'validation.json').read_text());assert proof['passed']
origin=P('/workspace/work/integration/broker-merged-source-fcac9d1d/normalized-origin.json')
assert sha(origin)=='dbc16f9c8a4dfe09f5c169e3f1a0b6dee8233fc41dc31015e52e3a57026e96d2'
base={'source_sha':SHA,'git_repository':'/workspace/partitionline','source_root':str(SOURCE),
 'immutable_origin_receipt':str(origin),'immutable_origin_receipt_sha256':sha(origin),
 'driver_relative_path':'docs/evidence/broker/KL11-37/peers/driver/run-live.py',
 'driver_sha256':'7097d0001acde3563ebd70cb56324cbca308a7268e1132aaa06a2cc1d81c85f6',
 'outer_relative_path':'docs/evidence/broker/KL11-37/peers/driver/own-live.py',
 'outer_sha256':'1e4898de042eef1900070d626df8129260547a7c81c2aea04b65b73254acd4dc',
 'issuer_relative_path':'docs/evidence/broker/KL11-37/oracle/https/live-issuer/source/live-issuer.py',
 'issuer_sha256':'3a1447f2c59cd9f49cc88ca9eb99f332c52285ebc7185df03e2ff4c6fd492d08'}
for kind in ['driver','outer','issuer']:assert sha(SOURCE/base[kind+'_relative_path'])==base[kind+'_sha256']
spec=importlib.util.spec_from_file_location('immutable_driver_for_configuration',SOURCE/base['driver_relative_path']);module=importlib.util.module_from_spec(spec);spec.loader.exec_module(module)
guard=module.Run(base,OUTPUT,OUTPUT.parent/'private-not-created-657');before=guard.source_guard()
rows=[]
for row in manifest['recipes']:
 recipe=P(row['path']);assert sha(recipe)==row['sha256']
 item=json.loads(recipe.read_text());assert len(item['steps'])==row['steps']<=96
 tool=item['server_toolchain']
 normal=P('/workspace/work/normal-oidc-example-fcac9d1d/build-bindings')/(tool+'.json')
 normal_value=json.loads(normal.read_text());assert normal_value['source_sha']==SHA and normal_value['exit_code']==0 and normal_value['artifact_kind']=='normal-example-executable'
 assert sha(normal_value['executable'])==normal_value['executable_sha256']
 used=sorted({step['peer'] for step in item['steps'] if 'peer' in step})
 definitions=[]
 for id in used:
  peer=peers[tool+'-rust-public' if id=='rust-public' else id]
  bound=json.loads(P(peer['build_receipt']).read_text())
  assert sha(peer['build_receipt'])==peer['build_receipt_sha256'] and bound['source_sha']==SHA and bound['exit_code']==0 and bound['runtime_inputs']==peer['runtime_inputs']
  for x in peer['runtime_inputs']:assert sha(x['path'])==x['sha256']
  for x in peer['source_inputs']:assert sha(SOURCE/x['path'])==x['sha256']
  definitions.append(peer)
 config={**base,'normal_example_build_receipt':str(normal),'normal_example_build_receipt_sha256':sha(normal),'profile':item['profile'],'peers':definitions,'steps':item['steps']}
 path=OUTPUT/(item['name']+'.json');write(path,config,compact=True);assert path.stat().st_size<=65536
 rows.append({'name':item['name'],'config':str(path),'config_sha256':sha(path),'config_bytes':path.stat().st_size,'server_toolchain':tool,'profile':item['profile'],'steps':len(item['steps']),'peer_ids':used,'recipe_path':str(recipe),'recipe_sha256':sha(recipe),'outer_command':['taskset','-c','0,1',sys.executable,str(SOURCE/base['outer_relative_path']),'--config',str(path),'--output',str(OUTPUT.parent/'live-runs-fcac9d1d-predeclared-01'/item['name']),'--private-scratch',str(OUTPUT.parent/'private-live-fcac9d1d'/item['name'])]})
after=guard.source_guard();assert before==after
write(OUTPUT/'manifest.json',{'source_sha':SHA,'status':'actual immutable compiler inputs bound and statically checked; ROOT live GO still required; no live invocation','config_count':len(rows),'recipe_manifest':{'path':str(PREP/'manifest.json'),'sha256':sha(PREP/'manifest.json')},'peer_build_validation':{'path':str(BIND/'validation.json'),'sha256':sha(BIND/'validation.json')},'normal_build_validation':{'path':'/workspace/work/normal-oidc-example-fcac9d1d/validation.json','sha256':sha('/workspace/work/normal-oidc-example-fcac9d1d/validation.json')},'source_guard':{'files':len(before),'before_equals_after':before==after,'scope':'complete actual regular/symlink pathset, all Git blobs, full07777'},'run_count':len(rows),'maximum_steps':max(r['steps'] for r in rows),'maximum_config_bytes':max(r['config_bytes'] for r in rows),'execution':'sequential cohorts; actual outer inclusive300s deadline and driver240s, no concurrent broker/SDK groups across cohorts','private_mode_requirement':'actual issuer/config/state directories0700, files0600 under driver umask077; validate path/mode only at future runtime; private bytes never included in evidence','denial_limits':'typed client/local/transport/native-unproved errors remain separate from server protocol responses; no operation-synchronized no-dispatch claim','cohorts':rows})
print(json.dumps({'path':str(OUTPUT/'manifest.json'),'sha256':sha(OUTPUT/'manifest.json'),'count':len(rows),'max_config_bytes':max(r['config_bytes'] for r in rows),'source_files':len(before)}))

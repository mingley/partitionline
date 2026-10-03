#!/usr/bin/env python3
"""Derive bindings from completed actual compiler receipts; never run peers."""
import hashlib,json,os,pathlib,re,stat,subprocess
P=pathlib.Path
SOURCE=P('/workspace/work/broker-merged-source-657d38ea')
SHA='657d38ea298a8f84a6a9bb8465dcba30cf2bd731'
BUILD=P('/workspace/work/broker-oidc/peer-builds-657d38ea-attempt-02')
OUT=BUILD/'build-bindings-02'
OUT.mkdir(mode=0o700,exist_ok=False)

def checksum(p):
 h=hashlib.sha256()
 with P(p).open('rb') as f:
  while b:=f.read(1048576):h.update(b)
 return h.hexdigest()

def identity(p,role):
 p=P(p).resolve(strict=True);s=p.stat()
 assert stat.S_ISREG(s.st_mode)
 return {'path':str(p),'sha256':checksum(p),'bytes':s.st_size,'full_mode':oct(s.st_mode&0o7777),'role':role}

def source_inputs(paths):
 return [{'path':p,'sha256':checksum(SOURCE/p)} for p in paths]

def write(p,v):
 p.write_text(json.dumps(v,indent=2)+'\n');os.chmod(p,0o600)

def libs(p):
 result=subprocess.run(['ldd',str(p)],capture_output=True,text=True,check=True,timeout=10,env=dict(os.environ,LD_LIBRARY_PATH='/usr/lib/jvm/java-21-openjdk-amd64/lib:/usr/lib/jvm/java-21-openjdk-amd64/lib/server'))
 assert 'not found' not in result.stdout
 paths=[]
 for line in result.stdout.splitlines():
  found=re.search(r'(?:=>\s+)?(/\S+)\s+\(',line)
  if found:paths.append(P(found.group(1)).resolve(strict=True))
 return paths

def dedup(rows):
 return [r for _,r in sorted({r['path']:r for r in rows}.items())]

validation=json.loads((BUILD/'validation.json').read_text());assert validation['passed'] and len(validation['commands'])==9
commands={r['name']:r for r in validation['commands']}
proof={'path':str(BUILD/'validation.json'),'sha256':checksum(BUILD/'validation.json')}
origin=P('/workspace/work/integration/broker-merged-source-657d38ea/normalized-origin.json')
assert checksum(origin)=='d3251d75e402c845699e7e7ad871ec0391e0961076842348e996f50f98d9e7a1'
base={'source_sha':SHA,'exit_code':0,'scope':'actual peer compiler binding; no live invocation','immutable_origin':identity(origin,'normalized complete immutable origin'),'full_build_validation':proof,'source_unchanged':all(r[k]['identical_to_before'] for r in commands.values() for k in ['source_before','source_after'])}
peers={}
for tool in ['stable','1.85.0']:
 binary=BUILD/'bin'/tool/'rust-peer';row=commands[tool+'-rust-build']
 artifacts=[]
 for line in P(row['log_path']).read_text().splitlines():
  try:x=json.loads(line)
  except ValueError:continue
  if x.get('reason')=='compiler-artifact' and x.get('target',{}).get('name')=='partitionline-oidc-live-peer' and x.get('executable'):artifacts.append(x)
 assert len(artifacts)==1 and artifacts[0]['profile']['test'] is False and artifacts[0]['target']['kind']==['bin']
 inputs=dedup([identity(binary,'normal Rust peer executable')]+[identity(p,'ELF linked runtime dependency') for p in libs(binary)])
 source=source_inputs(['docs/evidence/broker/KL11-37/peers/rust/src/main.rs','docs/evidence/broker/KL11-37/peers/rust/Cargo.toml','docs/evidence/broker/KL11-37/peers/rust/Cargo.lock','Cargo.toml','Cargo.lock','clippy.toml'])
 receipt={**base,'command':row['command'],'command_log':{'path':row['log_path'],'sha256':row['log_sha256']},'strict_command':commands[tool+'-rust-strict'],'artifact_kind':'normal-peer-executable','cargo_artifact':artifacts[0],'runtime_inputs':inputs,'source_inputs':source}
 path=OUT/(tool+'-rust-public.json');write(path,receipt)
 peers[tool+'-rust-public']={'id':'rust-public','kind':'rust','runtime_inputs':inputs,'source_inputs':source,'command':[str(binary),'{config}'],'build_receipt':str(path),'build_receipt_sha256':checksum(path)}

binary=BUILD/'native/oidc-native-peer';row=commands['native-strict-link']
inputs=dedup([identity(binary,'native callback peer executable')]+[identity(p,'ELF linked runtime dependency') for p in libs(binary)])
source=source_inputs(['docs/evidence/broker/KL11-37/peers/native/oidc-native-peer.c','docs/evidence/broker/KL11-37/peers/native/pins.json'])
external=[identity(p,'authentic librdkafka2.15 compilation input') for p in ['/workspace/work/c-peer/source/src/cJSON.c','/workspace/work/c-peer/source/src/cJSON.h','/workspace/work/c-peer/source/src/rdkafka.h']]
assert next(r['sha256'] for r in inputs if r['path'].endswith('librdkafka.so.1'))=='8083662863399e55acd8ac411ec7b81b6da707974111591182f1105db967adbc'
receipt={**base,'command':row['command'],'command_log':{'path':row['log_path'],'sha256':row['log_sha256']},'sdk_object_command':commands['native-cjson-object'],'sdk_header_classification':'-isystem for unchanged authentic upstream header; own-source Wall/Wextra/Wpedantic/Werror unchanged','artifact_kind':'normal-peer-executable','runtime_inputs':inputs,'source_inputs':source,'external_compile_inputs':external,'compiler':identity('/usr/bin/gcc','actual native compiler')}
path=OUT/'native-2-15.json';write(path,receipt)
peers['native-2-15']={'id':'native-2-15','kind':'native','runtime_inputs':inputs,'source_inputs':source,'command':[str(binary),'{bootstrap}','{ca_pem}','{token_url}','{client_secret_file}'],'build_receipt':str(path),'build_receipt_sha256':checksum(path)}

java=P('/usr/bin/java').resolve(strict=True);jdk=java.parent.parent
jvm_paths=set([java,jdk/'lib/modules',jdk/'conf/security/java.security']+[p.resolve() for p in (jdk/'lib').rglob('*.so')])
for p in list(jvm_paths):
 if p.suffix=='.so' or p==java:jvm_paths.update(libs(p))
jvm_inputs=[identity(p,'actual Java21 runtime input') for p in sorted(jvm_paths)]
pins=json.loads((SOURCE/'docs/evidence/broker/KL11-37/peers/java/pins.json').read_text())
for release in pins['releases']:
 version=release['release'];classes=BUILD/'java'/version/'classes';jars=release['runtime_jars']
 for j in jars:assert checksum(j['path'])==j['sha256']
 class_files=sorted(classes.rglob('*.class'));assert class_files
 inputs=dedup(jvm_inputs+[identity(j['path'],'pinned official Apache OAuth runtime JAR') for j in jars]+[identity(p,'actual strict-compiled peer class') for p in class_files])
 source=source_inputs(['docs/evidence/broker/KL11-37/peers/java/OAuthMetadataPeer.java','docs/evidence/broker/KL11-37/peers/java/pins.json'])
 row=commands['java-'+version+'-strict']
 receipt={**base,'command':row['command'],'command_log':{'path':row['log_path'],'sha256':row['log_sha256']},'artifact_kind':'strict-compiled-Java-peer','runtime_inputs':inputs,'source_inputs':source,'official_release':version,'jvm_version':subprocess.run([str(java),'-version'],capture_output=True,text=True,check=True,timeout=10).stderr.splitlines(),'class_count':len(class_files)}
 path=OUT/('java-'+version.replace('.','-')+'.json');write(path,receipt)
 identity_id='java-'+version.replace('.','-')
 peers[identity_id]={'id':identity_id,'kind':'java','release':version,'runtime_inputs':inputs,'source_inputs':source,'command':[str(java),'-Xmx128m','-Dorg.slf4j.simpleLogger.defaultLogLevel=off','-cp',str(classes)+':'+':'.join(j['path'] for j in jars),'OAuthMetadataPeer','{config}'],'build_receipt':str(path),'build_receipt_sha256':checksum(path)}
write(OUT/'peers.json',peers)
write(OUT/'validation.json',{'source_sha':SHA,'passed':True,'peer_binding_count':len(peers),'original_build_validation':proof,'bindings':[identity(OUT/f,'derived actual build binding') for f in sorted(p.name for p in OUT.glob('*.json')) if f!='validation.json'],'no_live':True,'runtime_dependency_limit':'direct ELF resolution including transitive ldd output and Java lib/modules/native .so inputs; dynamic system policy outside these hash bindings is not an immutable OS claim'})
print(json.dumps({'path':str(OUT/'validation.json'),'sha256':checksum(OUT/'validation.json'),'runtime_counts':{k:len(v['runtime_inputs']) for k,v in peers.items()},'peer_binding_count':len(peers)}))

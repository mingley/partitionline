import argparse,ctypes,hashlib,importlib.util,json,os,socket,subprocess,time
from pathlib import Path
p=argparse.ArgumentParser()
for key in ['source','binary','binding','sdks','malformed','output']:p.add_argument('--'+key,type=Path,required=True)
a=p.parse_args();a.output.mkdir(exist_ok=False);end=time.monotonic()+600
spec=importlib.util.spec_from_file_location('owner',a.source/'scripts/run-benchmark-matrix.py');o=importlib.util.module_from_spec(spec);spec.loader.exec_module(o);assert ctypes.CDLL(None).prctl(36,1,0,0,0)==0
binding=json.loads(a.binding.read_text());sdks=json.loads(a.sdks.read_text());assert len(sdks)==3
pins={str(a.source/n):d for n,d in binding['sources'].items()}|{str(a.binary):binding['binary_sha256']}
for sdk in sdks:
 pins.update(sdk['classes']);pins[str(Path('/workspace/work/open-cards-20261006/init-v6-peers')/('kafka-clients-'+sdk['tag']+'.jar'))]=sdk['jar_sha256']
slf=Path('/workspace/work/open-cards-20261006/java-benchmark/retained-build/slf4j-api-1.7.36.jar');pins[str(slf)]='d3ef575e3e4979678dc01bf1dcce51021493b4d11fb7f1be8ad982877c16a1c0'
for f in [a.binding,a.sdks,Path(__file__).resolve()]:pins[str(f)]=hashlib.sha256(f.read_bytes()).hexdigest()
for f in a.malformed.rglob('*.bin'):pins[str(f)]=hashlib.sha256(f.read_bytes()).hexdigest()
(a.output/'input-bindings.json').write_text(json.dumps(dict(source_commit=binding['source_commit'],inputs=pins),indent=2)+'\n')
def guard():
 if time.monotonic()>=end:raise TimeoutError('600 second overall controller budget')
 for name,d in pins.items():
  with Path(name).open('rb') as f:assert hashlib.file_digest(f,'sha256').hexdigest()==d,name
 assert not subprocess.check_output(['git','status','--porcelain'],cwd=a.source)
helper=['python3','-B',str(a.source/'benchmarks/runtime/tools/parent-bound-exec.py'),str(os.getpid())];env=o.base_env()
def execute(argv,label,directory,extra=None):
 guard();o.execute(helper+argv,env|(extra or {}),directory,label,min(8,end-time.monotonic()));guard()
results=[]
for sdk in sdks:
 tag=sdk['tag'];execute([str(a.binary),'actual_sdk_update_features_malformed_bodies','--ignored','--exact','--nocapture'],tag+'-Rust-malformed',a.output,{'UPDATE_FEATURES_MALFORMED':str(a.malformed/(tag+'-malformed'))})
 for version in [1,2]:
  for scenario in ['success','top-error','retry','deadline','names']:
   root=a.output/(tag+'-v'+str(version)+'-'+scenario);root.mkdir();peer=root/'peer';ports=[];child=None;receipt={}
   try:
    guard()
    with (root/'server.log').open('x') as log:
     command=helper+sdk['java']+['server',str(peer),str(version),'success' if scenario=='names' else scenario];child=subprocess.Popen(command,env=env,stdout=log,stderr=subprocess.STDOUT,start_new_session=True);receipt=dict(command=command,pid=child.pid,parent_waited=False)
     ready=min(end,time.monotonic()+10)
     while not (peer/'ready.json').exists():
      if child.poll() is not None or time.monotonic()>ready:raise RuntimeError('SDK TCP peer startup failed')
      time.sleep(.02)
     ports=json.loads((peer/'ready.json').read_text())['ports'];address='127.0.0.1:'+str(ports[0])
     java=['client-names',address,str(root/'java.json')] if scenario=='names' else ['client',address,scenario,str(root/'java.json')]
     execute(sdk['java']+java,'Java-public',root)
     lane='public_update_features_name_validation_history' if scenario=='names' else 'public_update_features_controller_and_deadline_history'
     execute([str(a.binary),lane,'--ignored','--exact','--nocapture'],'Rust-public',root,{'UPDATE_FEATURES_BROKER':address,'UPDATE_FEATURES_SCENARIO':scenario,'UPDATE_FEATURES_OUTPUT':str(root/'rust.json')})
     java=json.loads((root/'java.json').read_text());rust=json.loads((root/'rust.json').read_text())
     if scenario=='names':assert java==rust and len(rust)==7,(java,rust)
     else:
      assert java['error_code']==rust['error_code'] and java['recovery_code']==rust['recovery_code'],(java,rust)
      if scenario=='deadline':assert java['error_code']==7 and rust['recovery_code']==0 and 200000<=rust['elapsed_us']<350000,(java,rust)
     receipt['actual_public_outcomes_match']=True
   finally:
    if child is not None:
     if child.poll() is None and peer.exists():(peer/'stop').touch()
     try:child.wait(timeout=6)
     except subprocess.TimeoutExpired:receipt['forced_shutdown']=True
     receipt['adopted_children']=o.stop_group(child);receipt.update(parent_waited=True,exit_code=child.returncode,group_empty=not o.group_members(child.pid),ports=[])
     for value in ports:
      with socket.socket() as stream:stream.setsockopt(socket.SOL_SOCKET,socket.SO_REUSEADDR,1);stream.bind(('127.0.0.1',value))
      receipt['ports'].append(dict(port=value,reusable=True))
     (root/'server.process.json').write_text(json.dumps(receipt,indent=2)+'\n')
    guard()
   assert child is not None and child.returncode==0 and not receipt.get('forced_shutdown') and receipt['group_empty'] and not receipt['adopted_children'],receipt
   assert json.loads((peer/'closed.json').read_text())==dict(handlers_joined=True,acceptors_joined=True,sockets_closed=True)
   events=json.loads((peer/'events.json').read_text());assert not any('peer_failure' in row for row in events)
   if scenario in ['retry','deadline']:
    for caller in ['update-features-java','update-features-rust']:
     updates=[row for row in events if row.get('caller')==caller and row.get('api')==57]
     assert [row['node'] for row in updates]==([0,1,1] if scenario=='deadline' else [0,1]),updates
     assert [row['response_code'] for row in updates]==([41,0,0] if scenario=='deadline' else [41,0]),updates
     if scenario=='deadline':assert 0<updates[1]['timeout_ms']<updates[0]['timeout_ms']<=250,updates
   results.append(dict(SDK=tag,version=version,scenario=scenario,java=java,rust=rust,owned_peer_closed=True))
   print(tag,version,scenario,'passed',flush=True)
guard();(a.output/'qualification.json').write_text(json.dumps(dict(source_commit=binding['source_commit'],binary_sha256=binding['binary_sha256'],peer_cases=len(results),malformed_rejections=30,ports_rebound=60,results=results),indent=2)+'\n')
print('All 30 finite SDK TCP cases and 30 malformed bodies passed; 30 peers joined, 60 ports rebound',flush=True)

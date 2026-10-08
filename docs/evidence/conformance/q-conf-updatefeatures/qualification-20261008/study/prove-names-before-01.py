import ctypes,hashlib,importlib.util,json,os,signal,socket,subprocess,time
from pathlib import Path
s=Path(__file__).parent;src=Path('/workspace/work/open-cards-20261006/update-features-retry-source-0cf6c1');out=s/'prove-names-before-01';out.mkdir(exist_ok=False)
spec=importlib.util.spec_from_file_location('owner',src/'scripts/run-benchmark-matrix.py');o=importlib.util.module_from_spec(spec);spec.loader.exec_module(o);assert ctypes.CDLL(None).prctl(36,1,0,0,0)==0
sdk=json.loads((s/'compile-preparation-06/sdks.json').read_text())[0];binding=json.loads((s/'compile-preparation-05/binary-binding.json').read_text());binary=s/'compile-preparation-05/before-fix.elf';assert hashlib.sha256(binary.read_bytes()).hexdigest()==binding['binary_sha256']
pins={str(src/n):d for n,d in binding['sources'].items()}|sdk['classes']|{str(binary):binding['binary_sha256']}
jar=Path('/workspace/work/open-cards-20261006/init-v6-peers')/('kafka-clients-'+sdk['tag']+'.jar');pins[str(jar)]=sdk['jar_sha256']
slf=Path('/workspace/work/open-cards-20261006/java-benchmark/retained-build/slf4j-api-1.7.36.jar');pins[str(slf)]='d3ef575e3e4979678dc01bf1dcce51021493b4d11fb7f1be8ad982877c16a1c0'
peer_source=Path('/workspace/work/open-cards-20261006/update-features-retry-driver-35cbe4/tests/conformance/java/ConformanceUpdateFeaturesPeer.java');pins[str(peer_source)]=hashlib.sha256(peer_source.read_bytes()).hexdigest()
pins[str(Path(__file__).resolve())]=hashlib.sha256(Path(__file__).read_bytes()).hexdigest()
(out/'input-bindings.json').write_text(json.dumps(pins,indent=2)+'\n')
def guard():
 for n,d in pins.items():
  with Path(n).open('rb') as f:assert hashlib.file_digest(f,'sha256').hexdigest()==d,n
helper=['python3','-B',str(src/'benchmarks/runtime/tools/parent-bound-exec.py'),str(os.getpid())];env=o.base_env();child=None;peer=out/'peer';receipt={};ports=[]
try:
 guard()
 with (out/'server.log').open('x') as log:
  command=helper+sdk['java']+['server',str(peer),'2','success'];child=subprocess.Popen(command,env=env,stdout=log,stderr=subprocess.STDOUT,start_new_session=True);receipt=dict(command=command,pid=child.pid,parent_waited=False)
  deadline=time.monotonic()+10
  while not (peer/'ready.json').exists():
   if child.poll() is not None or time.monotonic()>deadline:raise RuntimeError('SDK peer startup failed')
   time.sleep(.02)
  ports=json.loads((peer/'ready.json').read_text())['ports'];address='127.0.0.1:'+str(ports[0])
  o.execute(helper+sdk['java']+['client-names',address,str(out/'java.json')],env,out,'Java-deadline',8);guard()
  rust_env=env|{'UPDATE_FEATURES_BROKER':address,'UPDATE_FEATURES_OUTPUT':str(out/'rust.json')}
  try:o.execute(helper+[str(binary),'public_update_features_name_validation_history','--ignored','--exact','--nocapture'],rust_env,out,'Rust-deadline',8)
  except ValueError:pass
  guard();actual=json.loads((out/'Rust-deadline.process.json').read_text());assert actual['parent_waited'] and not actual.get('failure') and actual['exit_code']==0,actual
  rust=json.loads((out/'rust.json').read_text());java=json.loads((out/'java.json').read_text());assert len(java)==len(rust)==7 and java!=rust,(java,rust);assert sum(x!=y for x,y in zip(java,rust))==4,(java,rust)
  (out/'actual-failure.json').write_text(json.dumps(dict(source_commit=binding['source_commit'],SDK=sdk['tag'],java=java,rust=rust,qualification='Actual before-fix public argument mismatch for NUL, unit separator, NBSP and en quad.',binary_sha256=binding['binary_sha256']),indent=2)+'\n')
finally:
 if child is not None:
  if child.poll() is None and peer.exists():(peer/'stop').touch()
  try:child.wait(timeout=6)
  except subprocess.TimeoutExpired:receipt['forced_shutdown']=True
  receipt['adopted_children']=o.stop_group(child);receipt.update(parent_waited=True,exit_code=child.returncode,group_empty=not o.group_members(child.pid))
  receipt['ports']=[]
  for value in ports:
   with socket.socket() as stream:stream.setsockopt(socket.SOL_SOCKET,socket.SO_REUSEADDR,1);stream.bind(('127.0.0.1',value))
   receipt['ports'].append(dict(port=value,reusable=True))
  (out/'server.process.json').write_text(json.dumps(receipt,indent=2)+'\n')
 if child is not None:assert child.returncode==0 and not receipt.get('forced_shutdown') and receipt['group_empty'],receipt
 guard()
print('Actual public name mismatches retained; owned SDK peer and ports closed')

import hashlib,json,pathlib,signal,subprocess,time
P=pathlib.Path('/workspace/work/goal-qualification-20261028/integrated-peer-20261028/native');ROOT=pathlib.Path('/workspace/partitionline');E=P.parent/'binary';LIB=pathlib.Path('/workspace/work/goal-qualification-20261028/narrow-profile/c-peer/lib/librdkafka.so.1'); BIN=E/'operator.elf'; receipt=json.loads((E/'receipt.json').read_text());D=P/'attempt-01';D.mkdir();DATA=D/'data';DATA.mkdir()
def sha(p):return hashlib.sha256(p.read_bytes()).hexdigest()
def guard():
 bad=[n for n,h in receipt['source_files'].items() if sha(ROOT/n)!=h]
 if bad:raise ValueError('source drift: '+str(bad))
 if sha(BIN)!=receipt['binaries']['operator.elf'] or sha(LIB)!='23698e1d7ec1f3f496133b946474f997ed690fa57222d3785c3a176ed57563ca':raise ValueError('binary drift')
state={'status':'running','base_commit':receipt['base_commit'],'exact_commit':receipt['exact_commit'],'source_receipt_sha256':sha(E/'receipt.json'),'binary_sha256':sha(BIN),'library_sha256':sha(LIB),'peer_sha256':sha(P/'ordinary-native'),'peer_source_sha256':sha(P/'ordinary-peer.c'),'header_sha256':sha(P/'headers/rdkafka.h'),'start_ns':time.time_ns(),'steps':[]}
def save():(D/'result.json').write_text(json.dumps(state,indent=2)+'\n')
def start(label):
 args=[str(BIN),'serve','--data',str(DATA),'--listen','127.0.0.1:19398','--advertise-host','127.0.0.1','--cluster','rf1-native-integrated','--broker-id','0'];o=(D/(label+'.stdout')).open('wb');e=(D/(label+'.stderr')).open('wb');p=subprocess.Popen(args,stdout=o,stderr=e);s={'phase':label,'command':args,'pid':p.pid,'started_ns':time.time_ns()};state['steps'].append(s);save()
 for _ in range(400):
  if p.poll() is not None:raise RuntimeError('server exit '+str(p.returncode))
  if (D/(label+'.stdout')).read_text().startswith('ready '):return p,o,e,s
  time.sleep(.05)
 p.kill();p.wait();raise RuntimeError('readiness timeout')
def stop(h):
 p,o,e,s=h
 if p.poll() is None:p.send_signal(signal.SIGTERM)
 try:code=p.wait(timeout=15)
 except subprocess.TimeoutExpired:p.kill();code=p.wait();s['forced_kill']=True
 o.close();e.close();s.update(exit=code,ended_ns=time.time_ns());save()
 if code or 'stopped accepted=' not in (D/(s['phase']+'.stdout')).read_text():raise RuntimeError('shutdown failure')
def peer(phase):
 args=[str(P/'ordinary-native'),'127.0.0.1:19398',phase];s={'phase':'native-'+phase,'command':args,'started_ns':time.time_ns()};state['steps'].append(s);save()
 with (D/(s['phase']+'.stdout')).open('wb') as o,(D/(s['phase']+'.stderr')).open('wb') as e:
  try:code=subprocess.run(args,stdout=o,stderr=e,timeout=50).returncode
  except subprocess.TimeoutExpired:code=124
 s.update(exit=code,ended_ns=time.time_ns());save()
 if code:raise RuntimeError(s['phase']+' exit '+str(code))
 if '"status":"pass"' not in (D/(s['phase']+'.stdout')).read_text():raise RuntimeError('missing pass receipt')
h=None
try:
 guard();h=start('server-seed');peer('append');peer('fetch');stop(h);h=None;guard();h=start('server-restart');peer('restart');stop(h);h=None;guard();state.update(status='pass',scope='actual librdkafka ordinary 2 partitions 24 receipts; acks=1 idempotence=false; manual fetch and ListOffsets before and after clean restart')
except Exception as e:state.update(status='failed',failure=str(e))
finally:
 if h:
  try:stop(h)
  except Exception as e:state['cleanup_failure']=str(e)
 state['end_ns']=time.time_ns();save()
print(D,state['status'],state.get('failure',''))

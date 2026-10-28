import hashlib,json,os,pathlib,signal,socket,subprocess,time
P=pathlib.Path('/workspace/work/goal-qualification-20261028/integrated-peer-20261028/java');ROOT=pathlib.Path('/workspace/partitionline');E=pathlib.Path('/workspace/work/goal-qualification-20261028/integrated-peer-20261028/binary')
BIN=E/'operator.elf';RECEIPT=E/'receipt.json'; JARS=pathlib.Path('/workspace/work/security-qualification-20261028/java-peer/jars'); PORT=19397; TOPIC='rf1-java-integrated-20261028'
D=P/'attempt-01';D.mkdir(exist_ok=False);DATA=D/'data';DATA.mkdir();receipt=json.loads(RECEIPT.read_text())
def sha(p):return hashlib.sha256(p.read_bytes()).hexdigest()
def guard():
 changed=[n for n,h in receipt['source_files'].items() if sha(ROOT/n)!=h]
 if changed:raise RuntimeError('source differs from build receipt: '+repr(changed))
 if sha(BIN)!=receipt['binaries']['operator.elf']:raise RuntimeError('binary differs from build receipt')
 if sha(JARS/'kafka-clients-4.1.2.jar')!='33b4d9f24ba793ce0ed06607aa92b61d764015d8a0ef72d2558dbb81def4b3ed':raise RuntimeError('Kafka jar differs')
 if sha(JARS/'slf4j-api-1.7.36.jar')!='d3ef575e3e4979678dc01bf1dcce51021493b4d11fb7f1be8ad982877c16a1c0':raise RuntimeError('SLF4J jar differs')
def inventory():return {str(p.relative_to(DATA)):sha(p) for p in sorted(DATA.rglob('*')) if p.is_file()}
state={'status':'running','source_receipt_sha256':sha(RECEIPT),'base_commit':receipt['base_commit'],'exact_commit':receipt['exact_commit'],'binary_sha256':sha(BIN),'java_source_sha256':sha(P/'Rf1Peer.java'),'java_class_sha256':sha(P/'classes/Rf1Peer.class'),'kafka_client_sha256':sha(JARS/'kafka-clients-4.1.2.jar'),'kafka_source':'apache/kafka@sha256:5cc2a2fd93fa2687b44015eee04fb2c3edd9e526bd64bf8bec5ff1e268772e0e, extracted previously as documented by security-qualification-20261028/java-peer/receipt.json','topic':TOPIC,'addr':f'127.0.0.1:{PORT}','start_ns':time.time_ns(),'steps':[]}
def save():(D/'result.json').write_text(json.dumps(state,indent=2)+'\n')
def start(label):
 args=[str(BIN),'serve','--data',str(DATA),'--listen',state['addr'],'--advertise-host','127.0.0.1','--cluster','rf1-java-integrated','--broker-id','0'];so=(D/(label+'.stdout')).open('wb');se=(D/(label+'.stderr')).open('wb');p=subprocess.Popen(args,stdout=so,stderr=se);step={'phase':label,'command':args,'pid':p.pid,'started_ns':time.time_ns()};state['steps'].append(step);save()
 deadline=time.monotonic()+20
 while time.monotonic()<deadline:
  if p.poll() is not None:raise RuntimeError(f'{label} exited early: {p.returncode}')
  if (D/(label+'.stdout')).read_text().startswith('ready '):return (p,so,se,step)
  time.sleep(.05)
 raise RuntimeError(label+' readiness timeout')
def stop(holder):
 p,so,se,step=holder
 if p.poll() is None:p.send_signal(signal.SIGTERM)
 try:code=p.wait(timeout=15)
 except subprocess.TimeoutExpired:p.kill();code=p.wait();step['forced_kill']=True
 so.close();se.close();step.update(exit=code,ended_ns=time.time_ns());save()
 if code:raise RuntimeError(step['phase']+' exit '+str(code))
 if 'stopped accepted=' not in (D/(step['phase']+'.stdout')).read_text():raise RuntimeError('missing graceful shutdown')
def peer(phase):
 cp=str(P/'classes')+':'+str(JARS/'kafka-clients-4.1.2.jar')+':'+str(JARS/'slf4j-api-1.7.36.jar');args=['java','-Xmx128m','-cp',cp,'Rf1Peer',phase,state['addr'],TOPIC];s={'phase':'java-'+phase,'command':args,'started_ns':time.time_ns()};state['steps'].append(s);save()
 with (D/(s['phase']+'.stdout')).open('wb') as o,(D/(s['phase']+'.stderr')).open('wb') as e:
  try:code=subprocess.run(args,stdout=o,stderr=e,timeout=75).returncode
  except subprocess.TimeoutExpired:code=124
 s.update(exit=code,ended_ns=time.time_ns());save()
 if code:raise RuntimeError(s['phase']+' exit '+str(code))
 if 'PHASE_COMPLETE\t'+phase not in (D/(s['phase']+'.stdout')).read_text():raise RuntimeError('missing phase completion')
holder=None
try:
 guard();holder=start('server-seed');peer('seed');stop(holder);holder=None;state['journal_after_seed']=inventory();save()
 guard();holder=start('server-restart');peer('recover');stop(holder);holder=None;state['journal_after_restart']=inventory();guard();state.update(status='pass',scope='single plaintext RF1, two partitions, acks=1 and idempotence=false; six seed records and eight total after clean restart, exact Java manual-consumer payload and headers')
except Exception as e:state.update(status='failed',failure=str(e))
finally:
 if holder is not None:
  try:stop(holder)
  except Exception as e:state['cleanup_failure']=str(e)
 state['end_ns']=time.time_ns();save()
print(D,state['status'],state.get('failure',''))

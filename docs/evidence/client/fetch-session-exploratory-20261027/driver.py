import hashlib, importlib.util, json, os, pathlib, platform, re, shutil, subprocess, time, uuid
ROOT=pathlib.Path('/workspace/partitionline')
BASE=pathlib.Path('/workspace/work/goal-qualification-20261028')
BINARY=pathlib.Path('/workspace/work/qualification-20261027/broker-check-target/debug/deps/consumer_fetch_semantics-40d8232eced90b73')
REF='apache/kafka:4.1.2@sha256:5cc2a2fd93fa2687b44015eee04fb2c3edd9e526bd64bf8bec5ff1e268772e0e'
SOURCE='e7f5761a9c6fe13e08efb0759f7924709287165b'
IDENT=uuid.uuid4().hex
NAME='pl-fetch-recovery-'+IDENT
TOPIC='plfetch-recovery-'+IDENT
PORT='19292'; PROXY='19293'
OUT=BASE/('live-fetch-baseline-'+IDENT); OUT.mkdir()
ENV=os.environ.copy()
for k in ['DOCKER_HOST','DOCKER_CONTEXT','DOCKER_TLS','DOCKER_TLS_VERIFY','DOCKER_CERT_PATH']:ENV.pop(k,None)
DOCKER=['docker','--host=unix:///var/run/docker.sock']
record={'reference':REF,'container':NAME,'topic':TOPIC,'proxy':'127.0.0.1:'+PROXY,'backend':'127.0.0.1:'+PORT,'status':'running', 'build_provenance':'Prebuilt test executable produced during baseline cargo build, which failed in a different test target. Exact mapping from Cargo executable to the recorded source receipt is not cryptographically bound; this run is exploratory, not a candidate qualification.', 'baseline_source_receipt':'client-all-features.json', 'baseline_source_receipt_sha256':hashlib.sha256((BASE/'client-all-features.json').read_bytes()).hexdigest(),'source_label':SOURCE, 'binary':str(BINARY),'binary_sha256':hashlib.sha256(BINARY.read_bytes()).hexdigest(),'platform':platform.platform(),'machine':platform.machine(),'start_ns':time.time_ns(),'commands':[]}

def save(): (OUT/'identity.json').write_text(json.dumps(record,indent=2)+'\n')
def run(name,args,timeout=30,env=ENV,required=True):
    cmd={'name':name,'args':args,'start_ns':time.time_ns()};record['commands'].append(cmd);save()
    with (OUT/(name+'.stdout')).open('wb') as o,(OUT/(name+'.stderr')).open('wb') as e:
        try: p=subprocess.run(args,stdout=o,stderr=e,timeout=timeout,env=env);code=p.returncode
        except subprocess.TimeoutExpired: code=124
    cmd.update(exit=code,end_ns=time.time_ns());save()
    if required and code: raise RuntimeError(f'{name}: exit {code}')
    return code

owned=False
try:
  run('binary-list',[str(BINARY),'--list'])
  if 'live_fetch_session_recovery_required: test' not in (OUT/'binary-list.stdout').read_text():raise RuntimeError('test absent from binary')
  run('create',DOCKER+['create','--name',NAME,'--cpus=1','--memory=1g','--pids-limit=256','-p','127.0.0.1:'+PORT+':9092','-e','KAFKA_NODE_ID=1','-e','KAFKA_PROCESS_ROLES=broker,controller','-e','KAFKA_LISTENERS=PLAINTEXT://:9092,INTERNAL://:9094,CONTROLLER://:9093','-e','KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://127.0.0.1:'+PROXY+',INTERNAL://localhost:9094','-e','KAFKA_LISTENER_SECURITY_PROTOCOL_MAP=PLAINTEXT:PLAINTEXT,INTERNAL:PLAINTEXT,CONTROLLER:PLAINTEXT','-e','KAFKA_INTER_BROKER_LISTENER_NAME=INTERNAL','-e','KAFKA_CONTROLLER_LISTENER_NAMES=CONTROLLER','-e','KAFKA_CONTROLLER_QUORUM_VOTERS=1@localhost:9093','-e','KAFKA_AUTO_CREATE_TOPICS_ENABLE=false','-e','KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=1','-e','KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR=1','-e','KAFKA_TRANSACTION_STATE_LOG_MIN_ISR=1','-e','KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS=0','-e','KAFKA_HEAP_OPTS=-Xmx256m -Xms256m',REF]);owned=True
  run('start',DOCKER+['start',NAME]);run('inspect',DOCKER+['inspect',NAME]);run('image-inspect',DOCKER+['image','inspect',REF]);run('port',DOCKER+['port',NAME,'9092/tcp']);run('java-version',DOCKER+['exec',NAME,'/opt/kafka/bin/kafka-topics.sh','--version'])
  ready=False
  for i in range(1,16):
    if run('readiness-'+str(i),DOCKER+['exec',NAME,'/opt/kafka/bin/kafka-broker-api-versions.sh','--bootstrap-server','localhost:9094'],15,required=False)==0:ready=True;break
  if not ready:raise RuntimeError('broker not ready')
  run('create-topic',DOCKER+['exec',NAME,'/opt/kafka/bin/kafka-topics.sh','--bootstrap-server','localhost:9094','--create','--topic',TOPIC,'--partitions','32','--replication-factor','1'],40)
  env=dict(ENV,REQUIRE_BROKER='1',PL_FETCH_TOPIC=TOPIC,PL_FETCH_SOURCE_SHA=SOURCE,PL_FETCH_PROXY='127.0.0.1:'+PROXY,PL_FETCH_BACKEND='127.0.0.1:'+PORT)
  run('runtime',[str(BINARY),'--ignored','--exact','live_fetch_session_recovery_required','--nocapture'],90,env)
  run('java-records',DOCKER+['exec',NAME,'/opt/kafka/bin/kafka-console-consumer.sh','--bootstrap-server','localhost:9094','--topic',TOPIC,'--from-beginning','--max-messages','65','--timeout-ms','20000','--property','print.partition=true','--property','print.offset=true','--property','print.key=true'],40)
  spec=importlib.util.spec_from_file_location('validator',ROOT/'scripts/report-fetch-session-recovery.py'); validator=importlib.util.module_from_spec(spec);spec.loader.exec_module(validator)
  hist=validator.history((OUT/'runtime.stdout').read_text(),SOURCE,TOPIC)
  java=[]
  for line in (OUT/'java-records.stdout').read_text().splitlines():
    m=re.fullmatch(r'Partition:(\d+)\tOffset:(\d+)\t([^\t]+)\t([^\t]+)',line)
    if not m:raise ValueError('unparsed java row: '+line)
    java.append((int(m[1]),int(m[2]),m[3],m[4]))
  if len(java)!=65 or set(java)!=set(hist['acks']):raise ValueError('independent Java history mismatch')
  record.update(status='observed pass, exploratory baseline only',wire_cases=len(hist['wire']),java_records=len(java),scope='32 partitions; exact producer acknowledgments, consumer deliveries, positions, observed fetch session lifecycle and independent Apache Java readback; binary not source-bound')
  (OUT/'validated-history.json').write_text(json.dumps(hist,indent=2)+'\n')
except Exception as exc:
  record.update(status='failed',failure=str(exc))
finally:
  if owned:
    run('broker-logs',DOCKER+['logs',NAME],30,required=False)
    run('cleanup',DOCKER+['rm','-f',NAME],30,required=False)
  record['end_ns']=time.time_ns();save()
print(OUT,record['status'],record.get('failure',''))

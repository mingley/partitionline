import json,os,pathlib,subprocess
m=json.load(open('/workspace/work/bench-history/native/metadata.json'))
out=pathlib.Path('/workspace/partitionline/docs/evidence/perf/KL04-07')
kafka=pathlib.Path(m['kafka_home']); binary=pathlib.Path('/workspace/work/target-openloop/debug/examples')
env=os.environ|{'KAFKA_BOOTSTRAP':m['bootstrap'],'COUNT':'16','WARMUP_SECS':'0','MEASURE_SECS':'0','LINGER_MS':'0','PAYLOAD_BYTES':'100','SEED':'1592590337','CONNECTIONS':'1','MAX_IN_FLIGHT':'1','VERIFY':'0','KAFKA_HEAP_OPTS':'-Xmx256m'}
def run(args, folder, name, custom=None, timeout=60):
 with (folder/(name+'.stdout.log')).open('w') as stdout,(folder/(name+'.stderr.log')).open('w') as stderr:
  p=subprocess.run(['taskset','-c','0-2,4',*map(str,args)],env=env|(custom or {}),stdout=stdout,stderr=stderr,timeout=timeout)
 (folder/(name+'.exit-status.txt')).write_text(str(p.returncode)+'\n')
 if p.returncode: raise RuntimeError(f'{name} failed: {p.returncode}; raw logs retained')
def topic(name, folder):
 run([kafka/'bin/kafka-topics.sh','--bootstrap-server',m['bootstrap'],'--create','--topic',name,'--partitions','1','--replication-factor','1'],folder,'create-topic')
acks=out/'acks0-native-final'; acks.mkdir()
acks_topic=m['topic']+'-acks0-final'; topic(acks_topic,acks)
run([binary/'bench_produce'],acks,'produce',{'KAFKA_TOPIC':acks_topic,'ACKS':'0','RECORD_HISTORY':str(acks/'producer.jsonl')})
run([kafka/'bin/kafka-get-offsets.sh','--bootstrap-server',m['bootstrap'],'--topic',acks_topic,'--time','-1'],acks,'offsets')
run([binary/'bench_fetch'],acks,'fetch',{'KAFKA_TOPIC':acks_topic,'RECORD_HISTORY':str(acks/'consumer.jsonl')})
row=json.loads((acks/'produce.stdout.log').read_text())
assert row['acked']==0 and row['acked_rec_s'] is None and row['locally_completed']==16
run(['python3','scripts/bench-record-history.py','--producer',acks/'producer.jsonl','--consumer',acks/'consumer.jsonl','--history',acks/'history.json','--output',acks/'verdict.json'],acks,'checker')
assert json.load(open(acks/'verdict.json'))['acknowledged_throughput'] is False
print(json.dumps({'acks0_received':16,'acks0_acknowledged':0,'locally_completed':16,'acknowledged_throughput':False}))

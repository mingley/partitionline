import json,os,pathlib,subprocess
m=json.load(open('/workspace/work/bench-history/native/metadata.json'))
out=pathlib.Path('/workspace/partitionline/docs/evidence/perf/KL04-07'); ctrl=out/'control-native'
kafka=pathlib.Path(m['kafka_home']); classes=pathlib.Path('/workspace/work/bench-history/classes'); cp=str(kafka/'libs/*')
env=os.environ|{'KAFKA_BOOTSTRAP':m['bootstrap'],'KAFKA_TOPIC':m['topic']+'-control','COUNT':'3','VERIFY':'0','PAYLOAD_BYTES':'100','SEED':'1592590337','ISOLATION':'read_committed','RECORD_HISTORY':str(ctrl/'consumer.jsonl'),'KAFKA_HEAP_OPTS':'-Xmx256m'}
def run(args,name,timeout=60):
 with (ctrl/(name+'.stdout.log')).open('x') as stdout,(ctrl/(name+'.stderr.log')).open('x') as stderr:
  p=subprocess.run(['taskset','-c','0-2,4',*map(str,args)],env=env,stdout=stdout,stderr=stderr,timeout=timeout)
 (ctrl/(name+'.exit-status.txt')).write_text(str(p.returncode)+'\n')
 if p.returncode: raise RuntimeError(f'{name} failed ({p.returncode}); previous artifacts retained')
run(['java','-Xmx128m','-m','jdk.compiler/com.sun.tools.javac.Main','-cp',kafka/'libs/kafka-clients-3.9.1.jar','-d',classes,out/'ControlRecords.java'],'compile-java-explicit')
run(['java','-Xmx128m','-cp',str(classes)+':'+cp,'ControlRecords',m['bootstrap'],env['KAFKA_TOPIC'],ctrl/'producer.jsonl'],'produce-java')
run([kafka/'bin/kafka-get-offsets.sh','--bootstrap-server',m['bootstrap'],'--topic',env['KAFKA_TOPIC'],'--time','-1'],'offsets')
run(['/workspace/work/target-openloop/debug/examples/bench_fetch'],'fetch')
run(['python3','scripts/bench-record-history.py','--producer',ctrl/'producer.jsonl','--consumer',ctrl/'consumer.jsonl','--history',ctrl/'history.json','--output',ctrl/'verdict.json'],'checker')
assert (ctrl/'offsets.stdout.log').read_text().strip().endswith(':4')
assert json.loads((ctrl/'fetch.stdout.log').read_text())['consumed']==3
print(json.dumps({'transactional_application_records':3,'transactional_log_end_offset':4,'control_offsets_counted_as_application_records':False,'compiler_fallback':'jdk.compiler module; original missing javac attempt retained'}))

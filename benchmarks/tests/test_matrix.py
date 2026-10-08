"""Real process orchestration tests; synthetic result fixtures are never Kafka evidence."""
import copy
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import signal
import subprocess
import sys
import tempfile
import time
import unittest

ROOT=Path(__file__).resolve().parents[2]
SCRIPT=ROOT/'scripts/run-benchmark-matrix.py'
spec=importlib.util.spec_from_file_location('report_fixture',ROOT/'benchmarks/tests/test_report.py');fixture=importlib.util.module_from_spec(spec);spec.loader.exec_module(fixture)
spec=importlib.util.spec_from_file_location('matrix',SCRIPT);matrix=importlib.util.module_from_spec(spec);spec.loader.exec_module(matrix)
sha=lambda p:hashlib.sha256(p.read_bytes()).hexdigest()

FAKE='''import json,os,sys,time,subprocess,hashlib
from pathlib import Path
mode=sys.argv[1];cfg=json.load(open(sys.argv[2]));cfg['topic']=os.environ.get('KAFKA_TOPIC','unset');cfg['bootstrap']=os.environ.get('KAFKA_BOOTSTRAP','unset')
if mode=='identity':print('owned-test-cluster');raise SystemExit(0)
if mode in ['create','delete']:
 with open(sys.argv[3],'a') as f:f.write(mode+' '+sys.argv[4]+'\\n')
 raise SystemExit(0)
if mode=='config':
 if sys.argv[-1]=='mismatch':cfg['linger_ms']+=1
 print(json.dumps(cfg));raise SystemExit(0)
if mode in ['hang','interrupt']:
 child=subprocess.Popen([sys.executable,'-c','import time;time.sleep(60)'])
 Path(sys.argv[3]+'.grandchild').write_text(str(child.pid))
 if mode=='interrupt':os.kill(os.getppid(),2)
 time.sleep(60)
if mode=='crash':raise SystemExit(7)
if mode=='orphan':
 child=subprocess.Popen([sys.executable,'-c','import time;time.sleep(60)']);Path(sys.argv[3]+'.grandchild').write_text(str(child.pid));raise SystemExit(0)
r=json.load(open(sys.argv[4]));r['provenance']['config']['effective_settings']=cfg
r['execution'].update(repetition_index=int(os.environ['REPETITION_INDEX']),total_repetitions=int(os.environ['TOTAL_REPETITIONS']),pairing_order=os.environ['PAIRING_ORDER'])
r['scenario']['scenario_id']=os.environ['SCENARIO_ID'];r['scenario']['tier']='exploratory'
r['provenance']['broker']['cluster_id']='owned-test-cluster'
r['provenance']['source']['note']='Synthetic orchestration unit fixture; no Kafka or measured delivery claim'
raw=Path(sys.argv[3]+'.raw');raw.write_text('Explicit synthetic fixture')
r['provenance']['artifacts']=[dict(path=str(raw),type='raw_delivery',sha256=hashlib.sha256(raw.read_bytes()).hexdigest(),size_bytes=raw.stat().st_size)]
if mode=='bad':r['integrity']['verified']=False
Path(sys.argv[3]).write_text(json.dumps(r))
'''

class MatrixProcessTests(unittest.TestCase):
 def setUp(self):
  self.tmp=tempfile.TemporaryDirectory();self.base=Path(self.tmp.name);self.peer=self.base/'fake.py';self.peer.write_text(FAKE)
  cfg=dict(acks=-1,idempotence=True,isolation_level='read_uncommitted',security_protocol='PLAINTEXT',compression='none',linger_ms=5,batch_size_bytes=1000000,batch_num_messages=32768,max_in_flight=5,queue_max_messages=1000000,queue_max_kbytes=32768,delivery_timeout_ms=30000,flush_timeout_ms=35000,run_timeout_ms=120000,consume_timeout_ms=30000,count=8000000,warmup=100000,payload_bytes=100,partitions=6,record_seed=7,payload_mode='seeded',key_mode='id',partitioner='explicit_round_robin',connections_per_broker=1,socket_nagle_disable=True)
  self.cfg=self.base/'config.json';self.cfg.write_text(json.dumps(cfg));self.data=self.base/'fixture.json';r=fixture.build_valid_fixture();r['outcomes']['consumed']=r['outcomes']['acknowledged'];r['execution']['warmup_records']=100000;r['execution']['warmup_completed']=True;self.data.write_text(json.dumps(r))
  self.log=self.base/'topics.log';self.output=self.base/'output';sem=copy.deepcopy(r['scenario']['equal_semantics'])
  self.manifest=dict(schema_version=1,seed=7,repetitions=2,timeout_seconds=5,broker=dict(bootstrap='127.0.0.1:19000',image='synthetic-test-only',version='test',cluster_id='owned-test-cluster'),cells=[dict(id='synthetic',env=dict(WARMUP='100000',COUNT='8000000',RECORD_SEED='7'),equal_semantics=sem)],peers=[],provision=dict(identity=[sys.executable,str(self.peer),'identity',str(self.cfg)],identity_stdout='owned-test-cluster',create=[sys.executable,str(self.peer),'create',str(self.cfg),str(self.log),'{topic}'],delete=[sys.executable,str(self.peer),'delete',str(self.cfg),str(self.log),'{topic}']))
  for arm in ['a','b']:
   self.manifest['peers'].append(dict(id=arm,emit_config=[sys.executable,str(self.peer),'config',str(self.cfg),'ok'],command=[sys.executable,str(self.peer),'ok',str(self.cfg),'{result}',str(self.data)],inputs=[dict(path=str(p),sha256=sha(p)) for p in [self.peer,self.cfg,self.data]]))
  self.path=self.base/'manifest.json';self.save()
 def tearDown(self):self.tmp.cleanup()
 def save(self):self.path.write_text(json.dumps(self.manifest))
 def run_cli(self,action='run',approve=True,retry=False,env=None):
  cmd=[sys.executable,str(SCRIPT),action,'--manifest',str(self.path),'--output',str(self.output)]
  if approve:cmd.append('--approve-provision')
  if retry:cmd.append('--retry-failed')
  return subprocess.run(cmd,stdout=subprocess.PIPE,stderr=subprocess.PIPE,text=True,timeout=25,env=env)
 def test_review_plan_has_no_process_or_topic_operations(self):
  r=self.run_cli('plan',False);self.assertEqual(r.returncode,0,r.stderr);self.assertFalse(self.log.exists());self.assertFalse((self.output/'events.jsonl').exists())
  r=self.run_cli('resume',False);self.assertEqual(r.returncode,2);self.assertFalse(self.log.exists())
 def test_randomized_order_repeats_warmup_and_cleanup(self):
  r=self.run_cli();self.assertEqual(r.returncode,0,r.stderr+r.stdout);summary=json.loads((self.output/'summary.json').read_text());self.assertEqual(summary['executed'],4)
  plan=json.loads((self.output/'plan.json').read_text());self.assertEqual(len(plan['rows']),4);self.assertEqual({x['order'] for x in plan['rows']},{'athenb','bthena'})
  topics=self.log.read_text().splitlines();self.assertEqual(len(set(topics)),4)
  r=self.run_cli('cleanup');self.assertEqual(r.returncode,0,r.stderr);self.assertEqual(len(self.log.read_text().splitlines()),8)
  r=self.run_cli('cleanup');self.assertEqual(r.returncode,0,r.stderr);self.assertEqual(len(self.log.read_text().splitlines()),8)
  r=self.run_cli('cleanup');self.assertEqual(r.returncode,0,r.stderr);self.assertEqual(len(self.log.read_text().splitlines()),8)
 def test_client_ceiling_target_cannot_use_a_kafka_manifest(self):
  self.manifest['cells'][0].update(id='nb-produce-bulk',target_kind='null-broker',result_kind='client-ceiling');self.save()
  r=self.run_cli();self.assertEqual(r.returncode,2,r.stderr);self.assertIn('target kind differs',r.stderr);self.assertFalse(self.log.exists());self.assertFalse(self.output.exists())
 def test_ceiling_result_kind_cannot_use_a_kafka_manifest(self):
  self.manifest['cells'][0]['result_kind']='client-ceiling';self.save();r=self.run_cli()
  self.assertEqual(r.returncode,2);self.assertIn('separate manifests',r.stderr);self.assertFalse(self.log.exists())
 def test_ceiling_cell_name_cannot_be_registered_as_kafka_throughput(self):
  self.manifest['cells'][0]['id']='ceiling-exp-null-fetch';self.save();r=self.run_cli()
  self.assertEqual(r.returncode,2);self.assertIn('null-broker cells',r.stderr);self.assertFalse(self.log.exists())
 def test_unqualified_ceiling_adapter_is_refused_before_processes(self):
  self.manifest['broker']['kind']='null-broker';self.manifest['cells'][0].update(id='nb-produce-bulk',target_kind='null-broker',result_kind='client-ceiling');self.save();r=self.run_cli()
  self.assertEqual(r.returncode,2);self.assertIn('not qualified',r.stderr);self.assertFalse(self.log.exists());self.assertFalse(self.output.exists())
 def test_franz_go_missing_settings_are_named_without_guessing_defaults(self):
  cfg=json.loads(self.cfg.read_text());del cfg['queue_max_messages']
  with self.assertRaisesRegex(ValueError,'queue_max_messages'):matrix.settings(cfg)
 def test_ceiling_shaped_output_is_retained_and_refused_in_kafka_cell(self):
  r=json.loads(self.data.read_text());r['client_ceiling']={'classification':'client-ceiling'};self.data.write_text(json.dumps(r))
  for peer in self.manifest['peers']:peer['inputs'][2]['sha256']=sha(self.data)
  self.manifest['repetitions']=1;self.save();r=self.run_cli();self.assertEqual(r.returncode,1,r.stderr)
  outcomes=[e for e in matrix.events(self.output) if e['state']=='failed']
  self.assertEqual(len(outcomes),2);self.assertTrue(all('cannot enter' in e['reason'] for e in outcomes))
  self.assertEqual(len(list((self.output/'attempts').glob('*/result.json'))),2)
 def test_franz_go_crash_keeps_the_failed_process_receipt(self):
  self.manifest['peers'][1]['id']='franz-go';self.save();self.test_unsupported_and_crash_are_retained()
 def test_franz_go_timeout_still_reaps_the_adapter_children(self):
  self.manifest['peers'][1]['id']='franz-go';self.save();self.test_timeout_kills_and_reaps_grandchild()
 def test_franz_go_resume_keeps_original_attempts(self):
  self.manifest['peers'][1]['id']='franz-go';self.save();self.test_interrupted_attempt_resumes_in_new_directory()
 def test_actual_config_mismatch_prevents_second_arm_provisioning(self):
  self.manifest['repetitions']=1;self.manifest['peers'][1]['emit_config'][-1]='mismatch';self.save();r=self.run_cli();self.assertEqual(r.returncode,1,r.stderr)
  summary=json.loads((self.output/'summary.json').read_text());self.assertEqual((summary['executed'],summary['failed']),(1,1));self.assertEqual(len(self.log.read_text().splitlines()),1)
 def test_security_disagreement_prevents_provisioning(self):
  self.manifest['cells'][0]['equal_semantics']['security']['protocol']='SSL';self.save();r=self.run_cli();self.assertEqual(r.returncode,1);self.assertFalse(self.log.exists())
 def test_unsupported_and_crash_are_retained(self):
  self.manifest['repetitions']=1;self.manifest['cells'][0]['unsupported']={'a':'Explicit unsupported test cell'};self.manifest['peers'][1]['command'][2]='crash';self.save();r=self.run_cli();self.assertEqual(r.returncode,1)
  s=json.loads((self.output/'summary.json').read_text());self.assertEqual((s['failed'],s['unsupported']),(1,1));events=matrix.events(self.output);self.assertTrue(any(e.get('reason')=='Explicit unsupported test cell' for e in events))
  receipts=list((self.output/'attempts').glob('*/peer.process.json'));self.assertEqual(json.loads(receipts[0].read_text())['exit_code'],7)
 def test_timeout_kills_and_reaps_grandchild(self):
  self.manifest['repetitions']=1;self.manifest['timeout_seconds']=.5
  for peer in self.manifest['peers']:peer['command'][2]='hang'
  self.save();r=self.run_cli();self.assertEqual(r.returncode,1,r.stderr)
  for path in (self.output/'attempts').glob('*/result.json.grandchild'):
   self.assertFalse(Path('/proc/'+path.read_text()).exists())
  for path in (self.output/'attempts').glob('*/peer.process.json'):
   receipt=json.loads(path.read_text());self.assertTrue(receipt['parent_waited']);self.assertTrue(receipt['adopted_children'])
 def test_interrupted_attempt_resumes_in_new_directory(self):
  # Behavior changes through a marker file, while all source bytes remain pinned.
  self.peer.write_text(FAKE.replace("if mode in ['hang','interrupt']:","if mode=='interrupt' and Path(sys.argv[2]+'.continue').exists():mode='ok'\nif mode in ['hang','interrupt']:"))
  self.manifest['repetitions']=1
  for peer in self.manifest['peers']:
   peer['command'][2]='interrupt';peer['inputs'][0]['sha256']=sha(self.peer)
  self.save();r=self.run_cli();self.assertEqual(r.returncode,130,r.stderr)
  before={str(p):sha(p) for p in (self.output/'attempts').rglob('*') if p.is_file()};Path(str(self.cfg)+'.continue').touch()
  r=self.run_cli('resume');self.assertEqual(r.returncode,0,r.stderr+r.stdout)
  for p,h in before.items():self.assertEqual(sha(Path(p)),h)
  s=json.loads((self.output/'summary.json').read_text());self.assertEqual(s['executed'],2);self.assertEqual(s['retained_interrupted_attempts'],1)
  r=self.run_cli('cleanup');self.assertEqual(r.returncode,0,r.stderr)
 def test_orphan_after_success_is_rejected_and_joined(self):
  self.manifest['repetitions']=1
  for peer in self.manifest['peers']:peer['command'][2]='orphan'
  self.save();r=self.run_cli();self.assertEqual(r.returncode,1,r.stderr)
  for path in (self.output/'attempts').glob('*/result.json.grandchild'):self.assertFalse(Path('/proc/'+path.read_text()).exists())
 def test_resume_rejects_changed_peer_input_and_completed_artifact(self):
  self.assertEqual(self.run_cli().returncode,0);self.cfg.write_text(self.cfg.read_text()+' ');self.assertEqual(self.run_cli('resume').returncode,2)
 def test_completed_raw_artifact_tamper_prevents_resume(self):
  self.assertEqual(self.run_cli().returncode,0)
  artifact=next((self.output/'attempts').glob('*/result.json.raw'));artifact.write_text('changed')
  r=self.run_cli('resume');self.assertEqual(r.returncode,2);self.assertIn('artifact changed',r.stderr)
 def test_plan_tamper_prevents_cleanup(self):
  self.assertEqual(self.run_cli().returncode,0)
  plan=self.output/'plan.json';plan.write_text(plan.read_text()+' ')
  r=self.run_cli('cleanup');self.assertEqual(r.returncode,2);self.assertEqual(len(self.log.read_text().splitlines()),4)
 def test_history_tamper_prevents_cleanup(self):
  self.assertEqual(self.run_cli().returncode,0)
  history=self.output/'events.jsonl';rows=history.read_text().splitlines();row=json.loads(rows[0]);row['event']['topic']='unowned-topic';rows[0]=json.dumps(row);history.write_text('\n'.join(rows)+'\n')
  r=self.run_cli('cleanup');self.assertEqual(r.returncode,2);self.assertIn('history changed',r.stderr);self.assertEqual(len(self.log.read_text().splitlines()),4)
 def test_concurrent_resume_does_not_spawn(self):
  self.assertEqual(self.run_cli('plan',False).returncode,0)
  import fcntl
  with (self.output/'.lock').open('a') as file:
   fcntl.flock(file,fcntl.LOCK_EX|fcntl.LOCK_NB);r=self.run_cli('resume');self.assertEqual(r.returncode,2);self.assertFalse(self.log.exists())
 def test_retained_integrity_failure_is_not_erased(self):
  self.manifest['repetitions']=1
  for peer in self.manifest['peers']:peer['command'][2]='bad'
  self.save();r=self.run_cli();self.assertEqual(r.returncode,1)
  r=self.run_cli('resume');self.assertEqual(r.returncode,1);s=json.loads((self.output/'summary.json').read_text());self.assertEqual(s['retained_failed_attempts'],2)
 def test_secret_environment_and_duplicate_manifest_keys_rejected(self):
  self.manifest['cells'][0]['env']['SASL_PASSWORD']='must-not-run';self.save();r=self.run_cli();self.assertEqual(r.returncode,2);self.assertFalse(self.output.exists());self.assertNotIn('must-not-run',r.stderr)
  self.path.write_text('{"schema_version":1,"schema_version":1}');r=self.run_cli();self.assertEqual(r.returncode,2);self.assertIn('duplicate JSON',r.stderr)

if __name__=='__main__':unittest.main()

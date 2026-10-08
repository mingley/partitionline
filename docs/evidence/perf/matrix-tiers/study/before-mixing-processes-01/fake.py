import json,os,sys,time,subprocess,hashlib
from pathlib import Path
mode=sys.argv[1];cfg=json.load(open(sys.argv[2]));cfg['topic']=os.environ.get('KAFKA_TOPIC','unset');cfg['bootstrap']=os.environ.get('KAFKA_BOOTSTRAP','unset')
if mode=='identity':print('owned-test-cluster');raise SystemExit(0)
if mode in ['create','delete']:
 with open(sys.argv[3],'a') as f:f.write(mode+' '+sys.argv[4]+'\n')
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

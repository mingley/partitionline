#!/usr/bin/env python3
"""Three actual tiny child-process diagnostics controls, never an OAuth peer."""
import hashlib,importlib.util,json,pathlib,sys,time
sys.dont_write_bytecode=True
P=pathlib.Path;source=P('/workspace/partitionline/docs/evidence/broker/KL11-37/peers/driver/run-live.py');out=P('/workspace/work/broker-oidc/safe-status-controls-01');out.mkdir(mode=0o700,exist_ok=False)
spec=importlib.util.spec_from_file_location('candidate_status',source);module=importlib.util.module_from_spec(spec);spec.loader.exec_module(module)
rows=[]
controls=[('pre-ready-exit',"import sys;sys.stderr.write('synthetic-startup-error\\n');sys.exit(7)",7,False,0,False,b'synthetic-startup-error\n'),('successful-ready',"print('{\"event\":\"ready\"}')",0,True,1,False,b''),('rejected-stdout',"import sys;print('synthetic-non-json');sys.exit(9)",9,False,0,True,b'')]
for name,code,exitcode,ready,events,rejected,stderr in controls:
 captured=[];process=module.OwnedProcess(name,[sys.executable,'-c',code],lambda actor,event:captured.append({'actor':actor,**event}),time.monotonic()+5,lambda p:None)
 observed_ready=False
 try:process.await_event('ready',seconds=2);observed_ready=True
 except BaseException:pass
 process.abort();status=process.cleanup_status()
 assert observed_ready==ready and status['exit_code']==exitcode and status['stdout_events']==events and status['stdout_receipt_rejected']==rejected and status['stderr_bytes']==len(stderr) and status['stderr_sha256']==hashlib.sha256(stderr).hexdigest() and not status['stderr_retained'] and not status['forced_close'] and not process.group_exists() and not any(t.is_alive() for t in process.readers)
 assert ('synthetic-startup-error' not in json.dumps(status)) and ('synthetic-non-json' not in json.dumps(status))
 rows.append({'name':name,'expected_readiness':ready,'observed_readiness':observed_ready,'safe_status':status,'no_group_survivor':True,'reader_threads_joined':True,'passed':True})
value={'source_sha256':hashlib.sha256(source.read_bytes()).hexdigest(),'scope':'infrastructure childprocess status serialization only; zero broker/SDK/issuer/OAuth cohorts','passed':True,'controls':rows,'mandatory_readiness_not_weakened':True,'raw_synthetic_stderr_or_rejected_stdout_bodies_retained':False}
p=out/'validation.json';p.write_text(json.dumps(value,indent=2)+'\n');print(json.dumps({'path':str(p),'sha256':hashlib.sha256(p.read_bytes()).hexdigest(),'controls':len(rows),'passed':True}))

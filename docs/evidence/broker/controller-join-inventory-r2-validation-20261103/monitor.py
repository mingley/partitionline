#!/usr/bin/env python3
import hashlib,json,os,signal,subprocess,sys,time,shutil
from pathlib import Path
root=Path('/workspace/work/broker-join-inventory-investigation-20261103')
label=sys.argv[1];mode=sys.argv[2]
assert mode in {'inventory','controls'}
log=root/(label+'.log');assert not log.exists()
start=time.monotonic();minimum=shutil.disk_usage(root).free;stop=None
with log.open('xb') as f:
 p=subprocess.Popen([sys.executable,'-B',str(root/'run-guarded.py'),mode],stdout=f,stderr=subprocess.STDOUT,start_new_session=True)
 while p.poll() is None:
  free=shutil.disk_usage(root).free;minimum=min(minimum,free)
  if free<320*1024**2:stop='disk floor';break
  if time.monotonic()-start>900:stop='timeout';break
  time.sleep(.2)
 if stop:
  os.killpg(p.pid,signal.SIGTERM)
  try:p.wait(timeout=5)
  except subprocess.TimeoutExpired:os.killpg(p.pid,signal.SIGKILL);p.wait()
 code=p.wait()
record={'label':label,'mode':mode,'exit':code,'stop_reason':stop,'seconds':round(time.monotonic()-start,3),'minimum_free_bytes':minimum,'log_sha256':hashlib.sha256(log.read_bytes()).hexdigest()}
(root/(label+'.json')).write_text(json.dumps(record,indent=2)+'\n')
print(json.dumps(record))

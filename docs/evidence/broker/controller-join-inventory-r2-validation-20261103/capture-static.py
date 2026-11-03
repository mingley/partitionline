#!/usr/bin/env python3
import hashlib,json,subprocess,sys,time
from pathlib import Path
w=Path('/workspace/work/broker-join-inventory-investigation-20261103')
name=sys.argv[1]
assert name in ('static','loader')
script=w/('run-'+name+'-guarded.py');log=w/('r2-'+name+'-final.log')
assert not log.exists()
start=time.monotonic()
with log.open('xb') as f:
 p=subprocess.run([sys.executable,'-B',str(script)],stdout=f,stderr=subprocess.STDOUT,timeout=120)
r={'mode':name,'exit':p.returncode,'seconds':round(time.monotonic()-start,3),'log_sha256':hashlib.sha256(log.read_bytes()).hexdigest()}
(w/('r2-'+name+'-final.json')).write_text(json.dumps(r,indent=2)+'\n')
print(json.dumps(r))

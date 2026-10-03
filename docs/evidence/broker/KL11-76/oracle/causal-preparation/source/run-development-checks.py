#!/usr/bin/env python3
"""Execute only Python preparation checks against immutable retained captures."""
import datetime
import hashlib
import json
from pathlib import Path
import os
import stat
import subprocess
import sys

ROOT=Path(__file__).resolve().parent
OUT=ROOT/'sealed-development-ea9ff293-01'
assert not OUT.exists();OUT.mkdir()
SOURCES=[ROOT/n for n in ('peer_bytes.py','check-peer-bytes.py','decoder-controls.json','causal_runtime.py',
                         'check-causal-controls.py','check-runtime-capture.py','run-development-checks.py')]
def guard():
    return {str(p):{'sha256':hashlib.sha256(p.read_bytes()).hexdigest(),'bytes':p.stat().st_size,
                   'mode':stat.S_IMODE(p.stat().st_mode)} for p in SOURCES}
base=guard();source='ea9ff29393526b57e1203612bb12c9c99deccc61'
captures='/workspace/work/raft-runtime-76/development/diagnostic-ea9ff293-01/captures/stable-default-runtime-lib-tests'
oracle='/workspace/partitionline/docs/evidence/broker/KL11-74/oracle/history'
commands=[['taskset','-c','2,4','env','PYTHONDONTWRITEBYTECODE=1','python3',str(ROOT/'check-causal-controls.py'),
           '--captures',captures,'--source-sha',source,'--membership-oracle',oracle,'--out-dir',str(OUT/'controls')],
          ['taskset','-c','2,4','env','PYTHONDONTWRITEBYTECODE=1','python3',str(ROOT/'check-runtime-capture.py'),
           '--producer-proof','/workspace/work/raft-runtime-76/development/diagnostic-ea9ff293-01/validation.json',
           '--producer-command','stable-default-runtime-lib-tests','--captures',captures,'--source-sha',source,
           '--membership-oracle',oracle,'--out',str(OUT/'causal-replay.json')]]
receipt={'schema_version':1,'source_sha':source,'scope':'WORK source preparation and actual retained diagnostic replay; not full76 qualification',
         'actual_new_broker_or_Cargo_executions':0,'commands':[],'sources_before':base,'python':sys.version}
for i,argv in enumerate(commands):
    assert guard()==base
    log=OUT/f'command-{i+1}.log';start=datetime.datetime.now(datetime.timezone.utc).isoformat()
    with log.open('wb') as stream:p=subprocess.run(argv,stdout=stream,stderr=subprocess.STDOUT,check=False)
    row={'argv':argv,'exit_code':p.returncode,'started_utc':start,
         'ended_utc':datetime.datetime.now(datetime.timezone.utc).isoformat(),'log':str(log),
         'log_sha256':hashlib.sha256(log.read_bytes()).hexdigest(),'log_bytes':log.stat().st_size,
         'sources_before':base,'sources_after':guard(),'actual_affinity':'taskset2,4'}
    receipt['commands'].append(row)
    (OUT/'validation.partial.json').write_text(json.dumps(receipt,indent=2,sort_keys=True)+'\n')
    assert guard()==base and p.returncode==0,(i,p.returncode,log)
receipt.update(passed=True,sources_after=guard(),all_sources_bytes_and_full_modes_unchanged=True)
(OUT/'validation.json').write_text(json.dumps(receipt,indent=2,sort_keys=True)+'\n')
print(json.dumps({'passed':True,'out':str(OUT),'commands':len(commands),'new_broker_or_Cargo_executions':0}))

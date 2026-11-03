#!/usr/bin/env python3
"""Capture read-only source blobs from the hosted job's exact trigger."""
import hashlib,json,os,subprocess
from pathlib import Path
repo=Path('/workspace/partitionline');out=Path('/workspace/work/broker-join-r2-job-125798406078-evidence')
commit='7e91e2bee412857c728ecd5c7a368fdefacf7af7'
env={k:v for k,v in os.environ.items() if not k.startswith('GIT_')};env['LC_ALL']='C'
def sha(b):return hashlib.sha256(b).hexdigest()
paths={
'workflow.yml':'.github/workflows/candidates.yml',
'spec.json':'docs/evidence/broker/candidate-20261030/controller-join-v2-r2/spec.json',
'qualify.py':'docs/evidence/broker/candidate-20261030/controller-join-v2-r2/qualify.py',
'test_static.py':'docs/evidence/broker/candidate-20261030/controller-join-v2-r2/test_static.py',
'join-registry.patch':'docs/evidence/broker/candidate-20261030/controller-join-v2-r2/join-registry.patch',
'legacy-driver.py':'scripts/ci-qualify-broker-candidate.py',
'ownership-helper.py':'scripts/candidate-command-ownership.py',
'v17-base-inputs.json':'docs/evidence/client/candidate-20261030/v17/base-inputs.json'}
record={}
for label,path in paths.items():
 data=subprocess.run(['git','show',commit+':'+path],cwd=repo,env=env,check=True,capture_output=True).stdout
 (out/'source'/label).write_bytes(data)
 record[label]={'git_path':path,'bytes':len(data),'sha256':sha(data)}
spec=json.loads((out/'source/spec.json').read_text())
v17=json.loads((out/'source/v17-base-inputs.json').read_text())
assert record['spec.json']['sha256']=='ab738fcaa40fc9f565c105513d512553105bfde778a1769bfcaf6c239a80eb0e'
assert record['qualify.py']['sha256']=='5921944995f2da7d40772562ea43e3c853703eba6e9c317f0e3c9611124ad50f'
assert record['join-registry.patch']['sha256']==spec['join_registry_patch']['sha256']
assert record['legacy-driver.py']['sha256']==spec['legacy_driver']['sha256']
assert record['ownership-helper.py']['sha256']==spec['ownership_helper']['sha256']
pin=v17[spec['join_registry_path']]
assert pin==spec['join_registry_base_source']['sha256'] and pin!=spec['join_registry_fixed_source']['sha256']
(out/'source.json').write_text(json.dumps({'commit':commit,'files':record,'v17_registry_pin':pin,
 'candidate_fixed_registry_sha256':spec['join_registry_fixed_source']['sha256'],
 'limits':'Captured commit blobs are not a per-command hosted report; r2 patch is applied only in isolated candidate.'},indent=2)+'\n')
print('Trigger source and v17 registry dependency captured.')

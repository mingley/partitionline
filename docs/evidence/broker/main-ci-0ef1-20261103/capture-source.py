#!/usr/bin/env python3
"""Capture source hashes from the exact hosted workflow trigger."""
import hashlib,json,os,subprocess
from pathlib import Path
repo=Path('/workspace/partitionline');out=Path('/workspace/work/main-broker-ci-41603566088-evidence')
commit='0ef1cf8581605cedff1117c99bcbbccc6a9cb68e'
env={k:v for k,v in os.environ.items() if not k.startswith('GIT_')};env['LC_ALL']='C'
paths=['.github/workflows/broker.yml','partitionline-broker/Cargo.lock',
'partitionline-broker/src/raft/protocol.rs','tests/conformance/broker/implemented-api-versions.json',
'scripts/check-broker-api-matrix.py','tests/ci/test_broker_api_matrix.py',
'scripts/broker-fuzz-campaign.py','tests/ci/test_broker_fuzz_campaign.py']
records={}
for path in paths:
 data=subprocess.run(['git','show',commit+':'+path],cwd=repo,env=env,check=True,capture_output=True).stdout
 oid=subprocess.run(['git','rev-parse',commit+':'+path],cwd=repo,env=env,check=True,capture_output=True,text=True).stdout.strip()
 records[path]={'bytes':len(data),'sha256':hashlib.sha256(data).hexdigest(),'git_blob_oid':oid}
 if path=='.github/workflows/broker.yml':(out/'broker-workflow.yml').write_bytes(data)
assert records['partitionline-broker/src/raft/protocol.rs']['sha256']=='86c8971a500208bb93ff015d9d3dd056efd8a34d7204b7de1f0d27c59a4499c3'
assert records['tests/conformance/broker/implemented-api-versions.json']['sha256']=='e127e3daeca095363f66ed45d7a2ffa1adbef17ff584d3f23dcfde63fb576e0a'
(out/'source-pins.json').write_text(json.dumps({'commit':commit,'files':records},indent=2)+'\n')
print('Trigger source pins recorded.')

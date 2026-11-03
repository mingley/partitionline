#!/usr/bin/env python3
"""Capture immutable checkout files and summarize connector-observed results."""
import hashlib
import json
import os
from pathlib import Path
import subprocess

work=Path('/workspace/work/broker-join-125788111325-evidence')
checkout=Path('/workspace/partitionline')
commit='664ba30c5991416e026e82add3e2445dab935b1a'
env={k:v for k,v in os.environ.items() if not k.startswith('GIT_')}
env['LC_ALL']='C'
def sha(data): return hashlib.sha256(data).hexdigest()
def get(name):
 return subprocess.run(['git','show',f'{commit}:{name}'],cwd=checkout,env=env,check=True,capture_output=True).stdout
files={
 'qualify.py':('docs/evidence/broker/candidate-20261030/controller-join-v2-r1/qualify.py','32cf64d1a3d718714ee23d6d1e501aa0d20240fc8817d37c160897f4e69deef2'),
 'spec.json':('docs/evidence/broker/candidate-20261030/controller-join-v2-r1/spec.json','232f4cae75d7ba6efe0f69058ac787df089bdbc44b4daee605ef4bd5a9f6df4b'),
 'legacy-driver.py':('scripts/ci-qualify-broker-candidate.py','c1361fa8cabc00f1702bf2e25c8716cfe247b29e5824aeec90c13cb928f8f7c2'),
 'ownership-helper.py':('scripts/candidate-command-ownership.py','34ddbf04a4bbead974fad4a451d30c7a56818248bd5b58c88c87bdfc41896da9'),
 'test_static.py':('docs/evidence/broker/candidate-20261030/controller-join-v2-r1/test_static.py','487ac493339cd3b4d35c6d0081d8a9873df5792bb5282b6bc20edbb2cf1a27fc'),
}
source=work/'source';source.mkdir(exist_ok=False)
for label,(name,digest) in files.items():
 data=get(name);assert sha(data)==digest
 (source/label).write_bytes(data)
log=(work/'connector-job.log').read_bytes()
assert sha(log)=='426caf9d42af9662dfd1acf3d3223cf8116516e3bc9663424c8604d77320952d'
assert all(x in log for x in [b'Ran 11 tests in', b'"gate": "broker-inventory"',b'"gate": "broker-inventory-negative-controls"',b'"passed": false, "error": null'])
metadata={
 'repository':'mingley/partitionline','trigger_sha':commit,'run_id':41594361732,'job_id':125788111325,
 'compact_reports_artifact':{'id':13213351269,'name':'candidate-reports-broker-controller-join-v2-r1-trigger-664ba30c5991416e026e82add3e2445dab935b1a-checkout-664ba30c5991416e026e82add3e2445dab935b1a-41594361732','reported_size_bytes':442148,'github_reported_sha256':'01e8ac6eb2c3b1f60c1536aabb825ba4bc1e1a3fcde9ba58515be9e2ff3e3879','bytes_downloaded':False},
 'full_artifact':{'id':13213520655,'reported_size_bytes':148541821,'github_reported_sha256':'0f05074014f3f5273f46f1ffc180c00fdda0b79962d5b749c3d3abce8ee058fa','bytes_downloaded':False},
 'console':{'connector_decoded_utf8_reencoding_sha256':sha(log),'static_controls_reported':'Ran 11; OK',
            'final_passed':False,'final_error':None,'reported_gate_errors':['broker-inventory','broker-inventory-negative-controls'],
            'gate_error_message':'command failed','inventory_stderr_present':False},
 'source_context':{'base_commit':'6fa8cdb3588d884580d01331e44df657c02f7670','base_source_sha256':'d2799c22004030742b3922fd5b30947f66939fc27879617b6617bcd8ee85e00f','fixed_capacity_source_sha256':'38c50bcd93e5dea91d10264c984fef1a5bfe9e4e33686a5ec64c03ae7945b66d','fixed_join_source_sha256':'86c8971a500208bb93ff015d9d3dd056efd8a34d7204b7de1f0d27c59a4499c3',
                   'files':{label:{'git_path':path,'sha256':digest} for label,(path,digest) in files.items()}},
 'evidence_limits':['Connector log is decoded text re-encoded as UTF-8, not original compressed GitHub log bytes.','The report artifact bytes and GitHub-reported SHA are unverified locally.','Per-command counts, exit statuses, logs and exact inventory failure cause are not shown in the console.', 'The File Service host is absent from the current enforced network allowlist; the only workspace download tool is documented for Sediment-backed IDs, not this File Service reference. No restricted host bypass or unsupported download was attempted.']
}
(work/'analysis.json').write_text(json.dumps(metadata,indent=2)+'\n')
print('Source blobs match the commit and hosted loader/spec pins.')

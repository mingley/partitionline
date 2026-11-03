#!/usr/bin/env python3
"""Reproduce a v17 helper guard trust-boundary gap without Cargo or network."""
from pathlib import Path
import hashlib
import json
import shutil
import subprocess
import sys
import tempfile

HERE=Path(__file__).resolve().parent
SOURCE=HERE/'source'
manifest_name='docs/evidence/client/candidate-20261030/v17/import-manifest.json'
pins=json.loads((HERE/'source-pins.json').read_text())
for name, info in pins['files'].items():
    data=(SOURCE/name).read_bytes()
    assert len(data)==info['bytes'] and hashlib.sha256(data).hexdigest()==info['sha256'], name
workflow=(SOURCE/'.github/workflows/candidates.yml').read_text()
guard_name='scripts/ci-verify-client-candidate-helpers.py'
guard_pin=pins['files'][guard_name]
assert workflow.count(guard_pin['sha256']) == 2
assert workflow.count(manifest_name) == 2
assert pins['files'][manifest_name]['sha256'] not in workflow
with tempfile.TemporaryDirectory(prefix='v17-pins-review-') as tmp:
    fixture=Path(tmp)
    doc=json.loads((SOURCE/manifest_name).read_text())
    for name in doc['required_existing_root_files']:
        target=fixture/name
        target.parent.mkdir(parents=True,exist_ok=True)
        shutil.copyfile(SOURCE/name,target)
    manifest=fixture/manifest_name
    manifest.parent.mkdir(parents=True,exist_ok=True)
    shutil.copyfile(SOURCE/manifest_name,manifest)
    def verify():
        result=subprocess.run([sys.executable,'-B',str(fixture/guard_name),
            '--checkout',str(fixture),'--manifest',manifest_name],
            capture_output=True,text=True)
        return {'exit':result.returncode,'stdout':result.stdout.strip(),'stderr':result.stderr.strip()}
    exact=verify()
    changed='scripts/ci-qualify-client-candidate.py'
    path=fixture/changed
    original=path.read_bytes()
    assert original.count(b'Qualify the pinned client candidate') == 1
    mutated=original.replace(b'Qualify the pinned client candidate',b'qualify the pinned client candidate',1)
    assert len(mutated)==len(original) and mutated!=original
    path.write_bytes(mutated)
    helper_only=verify()
    doc['required_existing_root_files'][changed]={'sha256':hashlib.sha256(mutated).hexdigest(),'bytes':len(mutated)}
    manifest.write_text(json.dumps(doc,indent=2)+'\n')
    paired=verify()
    assert exact['exit']==0 and helper_only['exit']!=0 and paired['exit']==0
    print(json.dumps({
       'source_commit':pins['commit'],
       'scope':'real v17 helper guard in a private local fixture; not a complete GitHub workflow run',
       'guard_sha256':guard_pin['sha256'],
       'manifest_sha256':pins['files'][manifest_name]['sha256'],
       'mutated_helper':changed,
       'mutation':'same-size inert docstring byte change; modified helper was never executed',
       'original':exact,'helper_only':helper_only,'helper_and_manifest':paired,
       'boundary':'a separately trusted immutable checkout commit can bind both files; this guard alone cannot distinguish the paired edit',
       'no_cargo_network_or_rust_execution':True},indent=2))

#!/usr/bin/env python3
"""Apply only the hash-bound registry repair to the verified scratch candidate."""
import hashlib,importlib.util,json,os,subprocess
from pathlib import Path
root=Path('/workspace/work/broker-join-inventory-investigation-20261103')
candidate=Path('/workspace/work/broker-join-inventory-r2-20261103')
source=root/'source';repo=Path('/workspace/partitionline')
spec=json.loads((candidate/'spec.json').read_text())
driver=repo/spec['legacy_driver']['path']
s=importlib.util.spec_from_file_location('pinned_broker_driver',driver);m=importlib.util.module_from_spec(s);s.loader.exec_module(m)
before=m.load(candidate/spec['base_files']['path'])
before[spec['runtime_path']]=spec['fixed_runtime']
before[spec['join_source_path']]=spec['join_fixed_source']
assert before[spec['join_registry_path']]==spec['join_registry_base_source']
m.verify_source(source,before)
patch=candidate/spec['join_registry_patch']['path']
assert hashlib.sha256(patch.read_bytes()).hexdigest()==spec['join_registry_patch']['sha256']
env={k:v for k,v in os.environ.items() if not k.startswith('GIT_')};env['LC_ALL']='C'
for args in (['--check'],[]):
 subprocess.run(['git','apply',*args,str(patch)],cwd=source,env=env,capture_output=True,check=True)
after=dict(before);after[spec['join_registry_path']]=spec['join_registry_fixed_source']
m.restore_patched_source_modes(source,after,{spec['join_registry_path']},root/'registry-fixed-modes.json')
m.verify_source(source,after)
reg=json.loads((source/spec['join_registry_path']).read_text())
assert reg['controller_source_sha256']==spec['join_fixed_source']['sha256']
print('Applied and source inventory/modes verified:',spec['join_registry_fixed_source']['sha256'])

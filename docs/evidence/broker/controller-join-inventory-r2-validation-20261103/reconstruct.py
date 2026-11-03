#!/usr/bin/env python3
"""Reconstruct the hosted broker candidate without Cargo or native tests."""
import hashlib, importlib.util, json, os, shutil, subprocess
from pathlib import Path
capsule=Path('/workspace/work/broker-join-qualification-import-20261103')
out=Path('/workspace/work/broker-join-inventory-investigation-20261103')
repo=Path('/workspace/partitionline')
base=Path('/workspace/work').resolve()
assert shutil.disk_usage(base).free > 400*1024**2
assert base.is_dir() and not base.is_symlink()
for p in (base,*base.parents): assert not (p/'.git').exists() and not (p/'.git').is_symlink()
env={k:v for k,v in os.environ.items() if not k.startswith('GIT_')};env['LC_ALL']='C'
probe=subprocess.run(['git','-C',str(base),'rev-parse','--is-inside-work-tree'],env=env,capture_output=True,text=True)
assert probe.returncode==128 and probe.stderr.startswith('fatal: not a git repository')
def sha(p): return hashlib.sha256(p.read_bytes()).hexdigest()
spec=json.loads((capsule/'spec.json').read_text())
assert sha(capsule/'spec.json')=='232f4cae75d7ba6efe0f69058ac787df089bdbc44b4daee605ef4bd5a9f6df4b'
for key in ('base_source','base_files','fixed_patch','join_fixed_patch'):
 item=spec[key]; path=capsule/item['path']
 assert path.stat().st_size==item['bytes'] and sha(path)==item['sha256']
driver=repo/spec['legacy_driver']['path'];helper=repo/spec['ownership_helper']['path']
assert sha(driver)==spec['legacy_driver']['sha256'] and sha(helper)==spec['ownership_helper']['sha256']
s=importlib.util.spec_from_file_location('pinned_broker_driver',driver);m=importlib.util.module_from_spec(s);s.loader.exec_module(m)
files=m.load(capsule/spec['base_files']['path'])
assert len(files)==5982
out.mkdir();source=out/'source';source.mkdir();(out/'tmp').mkdir()
m.extract_source(capsule/spec['base_source']['path'],source,files)
m.verify_source(source,files)
fixed=dict(files)
for key,path_key,entry_key in [('fixed_patch','runtime_path','fixed_runtime'),('join_fixed_patch','join_source_path','join_fixed_source')]:
 patch=capsule/spec[key]['path'];path=spec[path_key];fixed[path]=spec[entry_key]
 for args in (['--check'],[]):
  subprocess.run(['git','apply',*args,str(patch)],cwd=source,check=True,env=env,capture_output=True)
 m.restore_patched_source_modes(source,fixed,{path},out/(key+'-modes.json'))
m.verify_source(source,fixed)
registry=source/'tests/conformance/broker/implemented-api-versions.json'
r=json.loads(registry.read_text())
assert r['controller_source']=='partitionline-broker/src/raft/protocol.rs'
print(json.dumps({'source':str(source),'base_archive_sha256':spec['base_source']['sha256'],
 'candidate_spec_sha256':sha(capsule/'spec.json'),'source_files':len(fixed),
 'capacity_fixed_sha256':fixed[spec['runtime_path']]['sha256'],
 'join_fixed_sha256':fixed[spec['join_source_path']]['sha256'],
 'registry_sha256':sha(registry),'registry_controller_pin':r['controller_source_sha256'],
 'remaining_bytes':shutil.disk_usage(base).free},indent=2))

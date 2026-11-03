#!/usr/bin/env python3
"""Reproduce the v3 outer-unittest failure at manifest version 0.2.0 offline."""
import hashlib
import importlib.util
import json
from pathlib import Path
import shutil
import subprocess

BASE=Path('/workspace/work/release-rehearsal-candidate-20261103/v3')
PATCH=Path('/workspace/work/release-rehearsal-candidate-20261103/v3.patch')
OUT=Path('/workspace/work/release-rehearsal-v3-cross-version-review-20261103')
ROOT=OUT/'fixture'

def digest(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()

assert digest(PATCH)=='3a5472275f305d714f5146196f34f6dcbf91b029b4442e23a950790ac883f28f'
spec=importlib.util.spec_from_file_location('frozen_rehearsal',BASE/'scripts/report-release-rehearsal.py')
module=importlib.util.module_from_spec(spec)
spec.loader.exec_module(module)
ROOT.mkdir(exist_ok=False)
names=sorted(set(module.SOURCE_FILES)|{'tests/release/test_rehearsal.py'})
for name in names:
    destination=ROOT/name
    destination.parent.mkdir(parents=True,exist_ok=True)
    shutil.copyfile(BASE/name,destination)
manifest=ROOT/'Cargo.toml'
text=manifest.read_text(encoding='utf-8')
assert text.count('version = "0.1.0"') == 1
manifest.write_text(text.replace('version = "0.1.0"','version = "0.2.0"',1),encoding='utf-8')
before={name:digest(ROOT/name) for name in names}
command=['python3','-B','tests/release/test_rehearsal.py',
         'ReleaseRehearsal.test_incomplete_or_invented_report_cannot_pass']
result=subprocess.run(command,cwd=ROOT,env={'PATH':'/usr/bin:/bin','LANG':'C.UTF-8',
    'PYTHONDONTWRITEBYTECODE':'1'},capture_output=True,text=True,timeout=45)
after={name:digest(ROOT/name) for name in names}
(OUT/'stdout.log').write_text(result.stdout,encoding='utf-8')
(OUT/'stderr.log').write_text(result.stderr,encoding='utf-8')
receipt={'scope':'Offline local fixture and shimmed services; no Cargo, credentials, publication or network.',
         'candidate_patch_sha256':digest(PATCH),'fixture_version':'0.2.0',
         'fixture_cargo_toml_sha256':before['Cargo.toml'],
         'source_files_before':before,'source_files_after':after,'source_files_stable':before==after,
         'cwd':str(ROOT),'command':command,'exit_code':result.returncode,
         'stdout_sha256':digest(OUT/'stdout.log'),'stderr_sha256':digest(OUT/'stderr.log'),
         'expected_failure':'ValueError not raised',
         'failure_observed':result.returncode==1 and 'ValueError not raised' in result.stderr}
(OUT/'receipt.json').write_text(json.dumps(receipt,indent=2)+'\n',encoding='utf-8')
print(json.dumps({'exit_code':receipt['exit_code'],'failure_observed':receipt['failure_observed'],
                  'source_files_stable':receipt['source_files_stable'],
                  'manifest_sha256':receipt['fixture_cargo_toml_sha256']}))
assert receipt['failure_observed'] and receipt['source_files_stable']

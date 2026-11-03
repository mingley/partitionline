#!/usr/bin/env python3
"""Assemble the completed local inventory validation without hosted claims."""
import hashlib,json,re,shutil
from pathlib import Path
w=Path('/workspace/work/broker-join-inventory-investigation-20261103')
r=Path('/workspace/work/broker-join-inventory-r2-20261103')
out=Path('/workspace/work/broker-join-inventory-r2-validation-20261103')
def sha(p):return hashlib.sha256(p.read_bytes()).hexdigest()
assert sha(r/'manifest.json')=='4af9522fce97817864f1a5f4ae17bb7d553d3e2396b4575d71d9dbeee34578cc'
assert sha(r/'SHA256SUMS')=='bb90d2f37e2e10c8ed71c3ace7ca4fbf6b8219fd77d83f1d8a6e127a8358185f'
for name,expected in [('before-inventory',1),('after-inventory',0),('after-controls',0)]:
 result=json.loads((w/(name+'.json')).read_text())
 assert result['exit']==expected and result['stop_reason'] is None
 assert result['log_sha256']==sha(w/(name+'.log'))
 assert (w/(name+'.log')).read_text().startswith('socket guard active\n')
pre=json.loads((w/'before-controls.json').read_text())
assert pre['stop_reason']=='timeout' and pre['exit']==-15 and pre['log_sha256']==sha(w/'before-controls.log')
log=(w/'after-controls.log').read_text()
assert re.search(r'^Ran 55 tests in [0-9.]+s$',log,re.M) and log.rstrip().endswith('OK')
assert len(re.findall(r'^test_.* \.\.\. ok$',log,re.M))==55
assert not re.search(r'^test_.* \.\.\. (FAIL|ERROR|skipped)',log,re.M)
assert 'controller_source: profile source checksum mismatch' in (w/'before-inventory.log').read_text()
assert json.loads((w/'inventory-report.json').read_text())['verdict']=='passed'
assert json.loads((w/'after-controls-source-verified.json').read_text())['all_source_hashes_lengths_and_modes_verified']
assert not (w/'forbidden.log').exists()
assert "time.monotonic()-start>900" in (w/'monitor.py').read_text()
for name in ('r2-static-final','r2-loader-final'):
 result=json.loads((w/(name+'.json')).read_text())
 assert result['exit']==0 and result['log_sha256']==sha(w/(name+'.log'))
 text=(w/(name+'.log')).read_text()
 assert text.startswith('socket guard active\n') and 'Ran 12 tests in' in text and re.search(r'^OK$',text,re.M)
out.mkdir()
files=['before-inventory.log','before-inventory.json','after-inventory.log','after-inventory.json','inventory-report.json',
       'before-controls.log','before-controls.json','after-controls.log','after-controls.json','after-controls-source-verified.json',
       'r2-static.log','r2-loader.log','r2-static-final.log','r2-static-final.json','r2-loader-final.log','r2-loader-final.json',
       'apply-registry-repair.log','registry-fixed-modes.json','fixed_patch-modes.json','join_fixed_patch-modes.json',
       'r2-pins.json','audit-r2.log','run-guarded.py','run-static-guarded.py','run-loader-guarded.py','monitor.py',
       'replay-workflow-loader.py','apply-registry-repair.py','verify-fixed-source.py','after-controls-source-verified.log','freeze-validation.py','capture-static.py','prepare-r2.py','registry-preimage.json','registry-postimage.json']
for name in files:shutil.copyfile(w/name,out/name)
shutil.copyfile('/workspace/work/broker-join-reconstruct-20261103.py',out/'reconstruct.py')
shutil.copyfile('/workspace/work/broker-join-reconstruct-20261103.json',out/'reconstruction.json')
shutil.copyfile(r/'manifest.json',out/'candidate-manifest.json')
receipt={'status':'local Python-only inventory and unchanged 55 controls passed; hosted qualification not run',
 'candidate_manifest_sha256':sha(r/'manifest.json'),'candidate_checksums_sha256':sha(r/'SHA256SUMS'),
 'before_inventory':json.loads((w/'before-inventory.json').read_text()),
 'before_controls':pre,
 'after_inventory':json.loads((w/'after-inventory.json').read_text()),
 'after_controls':json.loads((w/'after-controls.json').read_text()),
 'static_12':json.loads((w/'r2-static-final.json').read_text()),
 'workflow_loader_12':json.loads((w/'r2-loader-final.json').read_text()),
 'after_source_verification':json.loads((w/'after-controls-source-verified.json').read_text()),
 'limits':['No Rust compilation, Cargo, native tests, live broker, hosted r2 run, or File Service artifact access.',
           'The hosted inventory stderr was inaccessible. Local source-bound reproduction is not direct proof of the hosted failure text.',
           'The pre-repair Python controls were interrupted at 240 seconds and must not be reported as a complete 55-control result.',
           'The inventory checks source hashes and retained fixtures; its report explicitly does not claim compiled-handler execution or production readiness.'],
 'local_replay_limits':{'monitor_disk_floor_bytes':320*1024**2,'suite_timeout_seconds':900,'per_file_size_limit_bytes':16*1024**2,'hosted_qualification_disk_floor_bytes':1536*1024**2,'hosted_floor_unchanged':True}}
(out/'validation.json').write_text(json.dumps(receipt,indent=2)+'\n')
(out/'README.md').write_text('''# Local broker inventory validation\n\nThe pinned r1 fixed source reproduces the controller-source checksum mismatch in `before-inventory.log`; the one-field registry repair produces the passing `after-inventory.log` and `inventory-report.json`. The pre-repair control run was interrupted at 240 seconds and is incomplete. The unchanged 55 Python controls passed after repair; their full output and the 900-second monitored result are retained. Source hashes, sizes and modes were rechecked after the run.\n\nThe candidate source/workflow is bound by `candidate-manifest.json`. The 12 static controls and verified-byte workflow-loader replay also passed; these stop before qualification. This package is local source-only evidence. The hosted report artifact was unavailable, and no r2 Cargo/native test or hosted qualification ran. The scripts record the paths and commands used in this workspace.\n\nThe local Python-only monitor used a 320 MiB disk floor and observed at least 410,361,856 free bytes during the repaired controls. The hosted qualification retains its separate 1,536 MiB disk floor; no local Rust qualification was attempted below that floor.\n''')
files=sorted(p for p in out.iterdir() if p.is_file() and p.name!='SHA256SUMS')
(out/'SHA256SUMS').write_text(''.join(f'{sha(p)}  {p.name}\n' for p in files))
print('validation',sha(out/'validation.json'))
print('SHA256SUMS',sha(out/'SHA256SUMS'))

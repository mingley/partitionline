import hashlib
import json
import os
from pathlib import Path
import shutil
import struct
import tarfile

script=Path('/workspace/work/broker-oidc/run-jwks-media-focus.py').read_text(); context={}; exec(compile(script.split("manifest = ['--manifest-path'")[0], '<existing-focus-helpers>', 'exec'), context)
out=context['out']; snapshot=context['snapshot']; base=context['base']; repo=context['repo']; context['results'].extend(json.loads((out/'commands.json').read_text())['commands']); context['checks'].extend(json.loads((out/'source-per-command.json').read_text())['checks']); run=context['run']; subprocess=context['subprocess']; target=Path('/workspace/work/broker-oidc/target'); retention=Path('/workspace/work/broker-oidc/jwks-media-retention'); retention.mkdir(exist_ok=True); (out/'bin').mkdir(exist_ok=True)
rows=[]
for path in sorted(target.rglob('*')):
    if not path.is_file() or path.is_symlink(): continue
    with path.open('rb') as stream: header=stream.read(20)
    if header[:4] != b'\x7fELF' or len(header)<18: continue
    order='<' if header[5]==1 else '>'
    if struct.unpack(order+'H',header[16:18])[0] not in (2,3): continue
    rows.append(dict(path=str(path.relative_to(target)),sha256=hashlib.sha256(path.read_bytes()).hexdigest(),mode=path.stat().st_mode&0o777,bytes=path.stat().st_size))
archive=retention/'candidate-test-executables.tar.gz'
with tarfile.open(archive,'w:gz') as tar:
    for row in rows: tar.add(target/row['path'],arcname=row['path'],recursive=False)
with tarfile.open(archive,'r:gz') as tar:
    for row in rows:
        member=tar.getmember(row['path']); data=tar.extractfile(member).read(); assert hashlib.sha256(data).hexdigest()==row['sha256'] and member.mode==row['mode']
original_limitation='Original first baseline executable was overwritten at its same target path by the subsequent candidate build before its binary hash/bytes were retained. Its complete raw log and source/mode checks remain. The repeated baseline below is a separately labeled reproduction; no byte identity with the unretained original executable is claimed.'
(out/'bin/manifest.json').write_text(json.dumps(dict(archive_local_path=str(archive),archive_sha256=hashlib.sha256(archive.read_bytes()).hexdigest(),all_bytes_and_modes_roundtrip_verified=True,ELF_paths=len(rows),files=rows,original_baseline_retention_limitation=original_limitation),indent=2)+'\n')
manifest=['--manifest-path',str(snapshot/'partitionline-broker/Cargo.toml')]; common=manifest+['--locked','--offline','--no-default-features','--features','oidc']; regression=['--test','oidc_http','registered_jwks_media_is_accepted_only_for_signing_keys','--','--exact']; restored={}; overrides={}
try:
    for name in ['partitionline-broker/src/security/oidc/http.rs','partitionline-broker/src/security/oidc/cache.rs']:
        path=snapshot/name; restored[name]=path.read_bytes(); old=subprocess.check_output(['git','show',base+':'+name],cwd=repo);path.write_bytes(old);overrides[name]=hashlib.sha256(old).hexdigest()
    run('baseline-media-regression-reproduced-for-retention',['taskset','-c','2,4','cargo','+stable','test']+common+regression,101,overrides)
    log=(out/'baseline-media-regression-reproduced-for-retention.log').read_text(); import re
    candidates=re.findall(r'Running tests/oidc_http.rs \(([^\)]+)\)',log); assert len(candidates)==1
    source=Path(candidates[0]); saved=retention/'reproduced-baseline-oidc-http'; shutil.copy2(source,saved); assert source.read_bytes()==saved.read_bytes() and source.stat().st_mode&0o777==saved.stat().st_mode&0o777
    receipt=dict(executable_local_path=str(saved),sha256=hashlib.sha256(saved.read_bytes()).hexdigest(),mode=saved.stat().st_mode&0o777,scope='separate exact-source baseline reproduction, not original first execution',original_baseline_retention_limitation=original_limitation,base_source_sha=base,old_http_cache_source_sha=base,otherwise_same_four_path_fixture_overlay=True,exit_code=101)
    (out/'bin/reproduced-baseline.json').write_text(json.dumps(receipt,indent=2)+'\n')
finally:
    for name,data in restored.items(): (snapshot/name).write_bytes(data)
run('restored-media-regression-after-retention',['taskset','-c','2,4','cargo','+stable','test']+common+regression)
print(json.dumps(dict(candidate_ELF_paths_preserved=len(rows),candidate_archive_sha256=hashlib.sha256(archive.read_bytes()).hexdigest(),original_baseline_missing_binary_explicit=True,reproduced_baseline_preserved=True,restored=True)),flush=True)

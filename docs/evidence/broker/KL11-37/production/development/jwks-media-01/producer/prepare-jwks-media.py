import hashlib, json, os, subprocess, gzip
from pathlib import Path
repo=Path('/workspace/partitionline'); base='0b9797ef166a5d067869be1fe988ee21051c86ad'; old=Path('/workspace/work/broker-oidc/http-final-'+base[:8]); dest=Path('/workspace/work/broker-oidc/jwks-media-overlay-01'); scratch=Path('/workspace/work/broker-oidc/jwks-media-audit-retention'); out=repo/'docs/evidence/broker/KL11-37/production/development/jwks-media-01'; out.mkdir(exist_ok=True); scratch.mkdir(exist_ok=True); dest.mkdir(exist_ok=True)
changes=['partitionline-broker/src/security/oidc/http.rs','partitionline-broker/src/security/oidc/cache.rs','partitionline-broker/tests/oidc_support/mod.rs','partitionline-broker/tests/oidc_http.rs']; socket=json.loads((repo/'docs/evidence/broker/KL11-37/production/socket-checkpoint-source20/manifest.json').read_text())['source']; assert all(hashlib.sha256((repo/r['path']).read_bytes()).hexdigest()==r['sha256'] for r in socket)
tree=subprocess.check_output(['git','ls-tree','-r','-z',base],cwd=repo); files=[]; links=0
for entry in tree.split(b'\0'):
 if not entry:continue
 fields,name=entry.split(b'\t',1); mode,kind,oid=fields.decode().split();name=name.decode();assert kind=='blob'and mode in('100644','100755');p=dest/name;p.parent.mkdir(parents=True,exist_ok=True); src=old/name;data=src.read_bytes();perm=0o755 if mode=='100755'else 0o644
 assert src.stat().st_mode&0o777==perm and hashlib.sha1(b'blob '+str(len(data)).encode()+b'\0'+data).hexdigest()==oid,name
 if name in changes:
  data=(repo/name).read_bytes()
  if p.exists():p.unlink()
  p.write_bytes(data);p.chmod(perm)
 else:
  if not p.exists():os.link(src,p)
  links+=1
 assert p.read_bytes()==data and p.stat().st_mode&0o777==perm,name
 files.append(dict(path=name,base_git_oid=oid,base_git_mode=mode,mode=perm,sha256=hashlib.sha256(data).hexdigest(),overlay=name in changes))
manifest=dict(base_source_sha=base,scope='complete pushed Git base plus exactly four authorized JWKS media followup paths; excludes six draft OAuth socket paths',mutable_work_links=False,files=files);raw=(json.dumps(manifest,indent=2)+'\n').encode();rawpath=scratch/'source-before.json';rawpath.write_bytes(raw);rawpath.chmod(0o600);compressed=gzip.compress(raw,mtime=0);(out/'source-before.json.gz').write_bytes(compressed);assert gzip.decompress(compressed)==raw
rows=[]
for name in changes:
 p=out/'source'/name;p.parent.mkdir(parents=True,exist_ok=True);p.write_bytes((repo/name).read_bytes());p.chmod(0o644);rows.append(dict(path=name,sha256=hashlib.sha256(p.read_bytes()).hexdigest(),working_mode=oct((repo/name).stat().st_mode&0o777),snapshot_mode='0o644'))
prep=dict(base_source_sha=base,source_scope=manifest['scope'],snapshot=str(dest),files=len(files),immutable_hardlinks=links,all_linked_git_bytes_and_full_modes_verified=True,changed_source=rows,six_socket_files_unchanged=True,audit=dict(stored_path='source-before.json.gz',original_scratch=str(rawpath),original_sha256=hashlib.sha256(raw).hexdigest(),original_mode='0o600',original_bytes=len(raw),gzip_sha256=hashlib.sha256(compressed).hexdigest(),lossless_roundtrip=True),planned_commands=['stable baseline regression with old http/cache and new test/helper, expected failure','stable rustc/fmt/OIDC library+HTTP+validation tests/all-target strict','MSRV rustc/fmt/OIDC library+HTTP+validation tests/all-target strict','stable restored focused regression, expected pass'],qualification='development overlay only; not a pushed candidate or socket/live peer qualification')
(out/'preparation.json').write_text(json.dumps(prep,indent=2)+'\n');print(json.dumps({k:v for k,v in prep.items()if k not in('changed_source','planned_commands')},indent=2))
# Earlier formatting command failed before Rust could start; its explanatory record is reconstructed from the prior tool transcript, not a retained raw log.
p=out/'format-first-attempt.json';record=json.loads(p.read_text());record['record_scope']='failure description reconstructed from prior tool response; no original raw output was captured';record.pop('output',None);p.write_text(json.dumps(record,indent=2)+'\n')

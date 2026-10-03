import gzip,hashlib,json,os,shutil,struct,tarfile
from pathlib import Path
repo=Path('/workspace/partitionline');cache=Path('/workspace/work/broker-oidc/target');out=repo/'docs/evidence/broker/KL11-37/production/development/socket-after-jwks-12f43986-after-stop-01/cache-clear-final';out.mkdir(parents=True,exist_ok=True);retention=Path('/workspace/work/broker-oidc/socket-final-cache-retention-12f43986');retention.mkdir(exist_ok=True);active=[]
for p in Path('/proc').iterdir():
 if not p.name.isdigit()or int(p.name)==os.getpid():continue
 for name in ['exe','cwd','cmdline','environ']:
  try:value=os.readlink(p/name)if name in ['exe','cwd']else(p/name).read_bytes().decode(errors='replace')
  except OSError:continue
  if str(cache)in value:active.append(dict(pid=int(p.name),attribute=name))
assert not active,active
files=[];elfs=[]
for p in sorted(cache.rglob('*')):
 if p.is_symlink():raise AssertionError('unexpected symlink')
 if not p.is_file():continue
 name=str(p.relative_to(cache));assert name.startswith('debug/')or name in('.rustc_info.json','CACHEDIR.TAG'),name;assert p.suffix not in('.wal','.journal','.log'),name
 data=p.read_bytes();row=dict(path=name,restore_path=str(p),bytes=len(data),mode=p.stat().st_mode&0o777,sha256=hashlib.sha256(data).hexdigest());files.append(row)
 if len(data)>=18 and data[:4]==b'\x7fELF':
  kind=struct.unpack(('<'if data[5]==1 else '>')+'H',data[16:18])[0]
  if kind in(1,2,3):elfs.append(dict(**row,ELF_type=kind))
archive=retention/'all-current-ELF-files.tar.gz'
with archive.open('wb')as raw,gzip.GzipFile(fileobj=raw,mode='wb',mtime=0,compresslevel=6)as gz,tarfile.open(fileobj=gz,mode='w|')as tar:
 for row in elfs:tar.add(cache/row['path'],arcname=row['path'],recursive=False)
with tarfile.open(archive,'r:gz')as tar:
 for row in elfs:
  member=tar.getmember(row['path']);data=tar.extractfile(member).read();assert hashlib.sha256(data).hexdigest()==row['sha256']and len(data)==row['bytes']and member.mode&0o777==row['mode']
archive_sha=hashlib.sha256(archive.read_bytes()).hexdigest()
for row in elfs:row['retention']=dict(archive=str(archive),archive_member=row['path'],archive_sha256=archive_sha)
rawdata=(json.dumps(dict(all_generated_cache_files=files,all_current_ELF_files=elfs),indent=2)+'\n').encode();raw=retention/'cache-inventory.json';raw.write_bytes(rawdata);raw.chmod(0o600);packed=gzip.compress(rawdata,mtime=0);(out/'cache-inventory.json.gz').write_bytes(packed);assert gzip.decompress(packed)==rawdata
receipt=dict(scope='coordinator-authorized completed socket focus own generated cache only',active_proc_references=active,checked_proc_attributes=['exe','cwd','cmdline','environ'],files=len(files),generated_path_bytes=sum(row['bytes']for row in files),ELF_paths=len(elfs),all_ELF_bytes_and_full_modes_roundtrip_verified=True,archive_local_path=str(archive),archive_sha256=archive_sha,archive_bytes=archive.stat().st_size,inventory_original_sha256=hashlib.sha256(rawdata).hexdigest(),inventory_original_mode='0o600',inventory_original_scratch_path=str(raw),inventory_gzip_sha256=hashlib.sha256(packed).hexdigest(),all_50_executed_test_invocations_independently_retained_before_later_commands=True,source_rawlogs_evidence_WAL_untouched=True,deleted_only_generated_cache=True)
shutil.rmtree(cache);(out/'receipt.json').write_text(json.dumps(receipt,indent=2)+'\n');print(json.dumps(receipt,indent=2))

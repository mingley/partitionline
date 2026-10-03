import gzip,hashlib,json,os,shutil,struct,tarfile
from pathlib import Path
repo=Path('/workspace/partitionline');cache=Path('/workspace/work/broker-oidc/target');out=repo/'docs/evidence/broker/KL11-37/production/development/socket-after-jwks-12f43986/cache-clear-interruption-01';out.mkdir(parents=True,exist_ok=True);retention=Path('/workspace/work/broker-oidc/socket-cache-clear-retention-12f43986');retention.mkdir(exist_ok=True)
active=[]
for p in Path('/proc').iterdir():
 if not p.name.isdigit()or int(p.name)==os.getpid():continue
 for name in ['exe','cwd','cmdline','environ']:
  try:text=os.readlink(p/name)if name in ['exe','cwd']else(p/name).read_bytes().decode(errors='replace')
  except OSError:continue
  if str(cache)in text:active.append(dict(pid=int(p.name),attribute=name))
assert not active,active
sources=[];coverage={}
for name in ['foundation-final-96211da1/bin/manifest.json','http-final-0b9797ef/bin/manifest.json','development/jwks-media-01/bin/manifest.json']:
 p=repo/'docs/evidence/broker/KL11-37/production'/name;m=json.loads(p.read_text());archive=Path(m.get('archive_path',m.get('archive_local_path')));assert hashlib.sha256(archive.read_bytes()).hexdigest()==m['archive_sha256'];sources.append(dict(manifest=str(p.relative_to(repo)),manifest_sha256=hashlib.sha256(p.read_bytes()).hexdigest(),archive=str(archive),archive_sha256=m['archive_sha256']))
 with tarfile.open(archive,'r:gz')as tar:
  for row in m['files']:
   member=tar.getmember(row['path']);data=tar.extractfile(member).read();assert hashlib.sha256(data).hexdigest()==row['sha256']and len(data)==row['bytes']and member.mode&0o777==row['mode'];coverage[(row['sha256'],row['bytes'],row['mode'])]=dict(archive=str(archive),archive_member=row['path'],archive_sha256=m['archive_sha256'])
files=[];elfs=[];uncovered=[]
for p in sorted(cache.rglob('*')):
 if p.is_symlink():raise AssertionError('unexpected cache symlink')
 if not p.is_file():continue
 data=p.read_bytes();row=dict(path=str(p.relative_to(cache)),restore_path=str(p),bytes=len(data),mode=p.stat().st_mode&0o777,sha256=hashlib.sha256(data).hexdigest());files.append(row)
 if len(data)>=18 and data[:4]==b'\x7fELF' and struct.unpack(('<'if data[5]==1 else '>')+'H',data[16:18])[0]in(1,2,3):
  key=(row['sha256'],row['bytes'],row['mode']);elf=dict(**row,retention=coverage.get(key));elfs.append(elf)
  if elf['retention']is None:uncovered.append(elf)
if uncovered:
 archive=retention/'additional-executables.tar.gz'
 with archive.open('wb')as raw,gzip.GzipFile(fileobj=raw,mode='wb',mtime=0)as gz,tarfile.open(fileobj=gz,mode='w|')as tar:
  for row in uncovered:tar.add(cache/row['path'],arcname=row['path'],recursive=False)
 with tarfile.open(archive,'r:gz')as tar:
  for row in uncovered:
   member=tar.getmember(row['path']);data=tar.extractfile(member).read();assert hashlib.sha256(data).hexdigest()==row['sha256']and len(data)==row['bytes']and member.mode&0o777==row['mode'];row['retention']=dict(archive=str(archive),archive_member=row['path'],archive_sha256=hashlib.sha256(archive.read_bytes()).hexdigest())
assert all(row['retention']for row in elfs)
inventory=json.dumps(dict(all_generated_cache_files=files,all_current_ELF_files_including_relocatable_objects=elfs),indent=2).encode()+b'\n';raw=retention/'cache-inventory.json';raw.write_bytes(inventory);raw.chmod(0o600)
packed=out/'cache-inventory.json.gz'
with packed.open('wb')as stream,gzip.GzipFile(fileobj=stream,mode='wb',mtime=0)as gz:gz.write(inventory)
assert gzip.decompress(packed.read_bytes())==inventory
receipt=dict(scope='coordinator-authorized interrupted and safely stopped own generated cache only; all current ELF file types1/2/3 retained before deletion',cache=str(cache),proc_reference_checks=['exe','cwd','cmdline','environ'],active_references=[],files=len(files),generated_bytes=sum(row['bytes']for row in files),ELF_paths=len(elfs),new_uncovered_ELFs_preserved=len(uncovered),all_required_ELFs_byte_and_mode_roundtrip=True,retention_sources=sources,inventory_gzip_sha256=hashlib.sha256(packed.read_bytes()).hexdigest(),inventory_original_sha256=hashlib.sha256(inventory).hexdigest(),inventory_original_full_mode='0o600',inventory_original_scratch_path=str(raw),source_rawlogs_evidence_WAL_unchanged=True,deleted_only_generated_cache=True)
shutil.rmtree(cache);(out/'receipt.json').write_text(json.dumps(receipt,indent=2)+'\n');print(json.dumps({key:receipt[key]for key in ['files','generated_bytes','ELF_paths','new_uncovered_ELFs_preserved','all_required_ELFs_byte_and_mode_roundtrip']}))

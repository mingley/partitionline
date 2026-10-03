from pathlib import Path
import hashlib,json,stat,sys,tarfile
cache=Path(sys.argv[1]);output=Path(sys.argv[2]);source_sha,profile=sys.argv[3:];output.mkdir(parents=True,exist_ok=True)
archive=output/'cache-elfs.tar.gz';assert not archive.exists()
files={};unique={}
for path in sorted(cache.rglob('*')):
    if not path.is_file() or path.is_symlink():continue
    with path.open('rb') as stream:
        if stream.read(4)!=b'\x7fELF':continue
    digest=hashlib.sha256(path.read_bytes()).hexdigest();mode=stat.S_IMODE(path.stat().st_mode);key=(digest,mode)
    member=f'elf/{digest}-{mode:o}'
    files[str(path.relative_to(cache))]={'sha256':digest,'bytes':path.stat().st_size,'mode':mode,'archive_member':member}
    unique.setdefault(key,(path,member))
with tarfile.open(archive,'w:gz',compresslevel=3) as tar:
    for key,(path,member) in unique.items():tar.add(path,arcname=member,recursive=False)
verified={}
with tarfile.open(archive,'r:gz') as tar:
    for row in tar:
        stream=tar.extractfile(row);assert stream is not None
        data=stream.read();digest=hashlib.sha256(data).hexdigest();assert row.name==f'elf/{digest}-{row.mode:o}',row.name
        verified[row.name]={'sha256':digest,'bytes':len(data),'mode':row.mode}
assert len(verified)==len(unique)
for path,row in files.items():assert {name:row[name] for name in ['sha256','bytes','mode']}==verified[row['archive_member']],path
receipt={'schema_version':1,'source_sha':source_sha,'profile':profile,'cache':str(cache),'archive':str(archive),'archive_sha256':hashlib.sha256(archive.read_bytes()).hexdigest(),'archive_bytes':archive.stat().st_size,'elf_paths_preserved':len(files),'unique_elf_members':len(unique),'preserved_logical_bytes':sum(row['bytes'] for row in files.values()),'unique_elf_bytes':sum(row['bytes'] for row in verified.values()),'verified_roundtrip_bytes_and_modes':True,'files':files,'cleanup_authorization':'Root authorized sequential same-cache lanes after accepted CLI/test ELF preservation; cache generated files only, source/rawlogs/fixture/proof files retained.'}
(output/'cache-elf-preservation.json').write_text(json.dumps(receipt,indent=2)+'\n')
print(f'PASS: {len(files)} ELFpaths/{len(unique)} uniqueELFs {receipt["unique_elf_bytes"]}bytes preserved→{archive.stat().st_size} archivebytes; all decodedbytes/modes verified')

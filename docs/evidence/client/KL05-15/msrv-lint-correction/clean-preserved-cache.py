from pathlib import Path
import hashlib,json,os,shutil,sys
cache=Path('/workspace/work/client-share-target');lane=Path(sys.argv[1]);receipt=json.loads((lane/'retained-cache/cache-elf-preservation.json').read_text())
assert receipt['verified_roundtrip_bytes_and_modes']
assert hashlib.sha256(Path(receipt['archive']).read_bytes()).hexdigest()==receipt['archive_sha256']
exclude={os.getpid(),os.getppid()};active=[]
for proc in Path('/proc').iterdir():
    if not proc.name.isdigit() or int(proc.name) in exclude:continue
    try:cmd=(proc/'cmdline').read_bytes().replace(b'\0',b' ').decode(errors='replace')
    except (OSError,PermissionError):continue
    if str(cache) in cmd and any(name in cmd for name in ['cargo ','rustc ','broker_compatibility-','share_semantics-','/debug/examples/']):active.append({'pid':int(proc.name),'cmd':cmd})
assert not active,active
paths=[p for p in cache.rglob('*') if p.is_file() and not p.is_symlink()];before=sum(p.stat().st_size for p in paths);before_files=len(paths)
shutil.rmtree(cache);cache.mkdir()
free=shutil.disk_usage(cache).free
(lane/'cache-cleanup.json').write_text(json.dumps({'schema_version':1,'source_sha':receipt['source_sha'],'cache_path':str(cache),'removed_generated_file_bytes':before,'removed_generated_files':before_files,'retained_elf_archive_sha256':receipt['archive_sha256'],'retained_elf_archive_verified':True,'active_references':active,'source_rawlogs_fixture_and_retained_binaries_untouched':True,'free_bytes_after':free},indent=2)+'\n')
print('PASS preserved',receipt['elf_paths_preserved'],'ELFpaths and modes; removed only inactivegeneratedcache',before,'bytes; free',free)

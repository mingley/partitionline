import hashlib,json,sys,zipfile
from pathlib import Path,PurePosixPath
root=Path(__file__).parent
name,source,artifact_id,size,expected=sys.argv[1:]
payload=Path(source).read_bytes()
got=hashlib.sha256(payload).hexdigest()
if got!=expected or len(payload)!=int(size): raise SystemExit('GitHub artifact digest or size mismatch')
dest=root/name
dest.mkdir(exist_ok=False)
archive=dest/'artifact.zip'
archive.write_bytes(payload)
entries=[]
with zipfile.ZipFile(archive) as z:
    if sum(x.file_size for x in z.infolist()) > 20*1024*1024: raise SystemExit('unexpected ZIP expansion')
    for info in z.infolist():
        relative=PurePosixPath(info.filename)
        if relative.is_absolute() or '..' in relative.parts or not relative.parts: raise SystemExit('unsafe ZIP path')
        if info.is_dir(): continue
        data=z.read(info)
        target=dest/'files'/Path(*relative.parts)
        target.parent.mkdir(parents=True,exist_ok=True)
        target.write_bytes(data)
        entries.append({'path':str(relative),'size':len(data),'sha256':hashlib.sha256(data).hexdigest()})
manifest={'case':name,'artifact_id':int(artifact_id),'github_sha256':expected,'computed_sha256':got,'size_bytes':len(payload),'members':entries}
(dest/'manifest.json').write_text(json.dumps(manifest,indent=2,sort_keys=True)+'\n')
print(f'{name}: verified artifact {artifact_id}; {len(entries)} files')

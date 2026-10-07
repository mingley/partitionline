import gzip
import hashlib
import json
from pathlib import Path
import shutil

s=Path(__file__).parent
repo=Path('/workspace/partitionline')
source=Path('/workspace/work/open-cards-20261006/assign-dirs-source-14f90f')
q=repo/'docs/evidence/conformance/q-conf-assignreplicastodirsraw/qualification'
q.mkdir(exist_ok=False)
files=[];excluded=[]
def sha(path):
    with path.open('rb') as stream:return hashlib.file_digest(stream,'sha256').hexdigest()
roots=[('study',s),('execution-source',source)]
source_pins=json.loads((s/'execution-source-pins-01.json').read_text())['files_sha256']
for prefix,root in roots:
    paths=sorted(root/name for name in source_pins) if prefix=='execution-source' else sorted(p for p in root.rglob('*') if p.is_file())
    for path in paths:
        relative=path.relative_to(root)
        if '__pycache__' in relative.parts or path.name.endswith(('.index','.paths')):
            excluded.append(dict(original_path=str(path),reason='Generated cache or Git publication scratch index'))
            continue
        with path.open('rb') as stream:elf=stream.read(4)==b'\x7fELF'
        encoding='gzip' if path.stat().st_size>262144 or elf else 'identity'
        target=q/prefix/relative
        if encoding=='gzip':target=target.with_name(target.name+'.gz')
        target.parent.mkdir(parents=True,exist_ok=True)
        digest=sha(path);size=path.stat().st_size
        with path.open('rb') as src,target.open('xb') as out:
            if encoding=='gzip':
                with gzip.GzipFile(filename='',mode='wb',compresslevel=6,fileobj=out,mtime=0) as dest:shutil.copyfileobj(src,dest)
            else:shutil.copyfileobj(src,out)
        assert sha(path)==digest
        files.append(dict(original_path=str(path),original_sha256=digest,original_bytes=size,stored_path=str(target.relative_to(q)),stored_sha256=sha(target),stored_bytes=target.stat().st_size,encoding=encoding))
with (q/'archive-inventory.json').open('x') as f:json.dump(dict(files=files,excluded=excluded,scope='Finite current API73 raw-extension qualification',upstream_binary_dependencies='Kafka distribution and SDK jar archives are external checksum-pinned inputs, not copied into this archive.'),f,indent=2);f.write('\n')
with (q/'summary.json').open('xb') as f:f.write((s/'qualification-summary-01.json').read_bytes())
print(len(files),'files archived;',sum(p['stored_bytes'] for p in files),'stored bytes',flush=True)

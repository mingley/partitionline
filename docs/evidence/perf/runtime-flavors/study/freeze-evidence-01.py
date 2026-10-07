import gzip
import hashlib
import json
from pathlib import Path
import shutil

s=Path(__file__).parent
repo=Path('/workspace/partitionline')
q=repo/'docs/evidence/perf/runtime-flavors'
q.mkdir(exist_ok=False)
rows=[];excluded=[]
roots=[('study',s),('source-17a634',Path('/workspace/work/open-cards-20261006/runtime-flavors-source-17a634')),
       ('formatter-63a89f',Path('/workspace/work/open-cards-20261006/runtime-flavors-formatter-63a89f'))]
def sha(path):
    with path.open('rb') as f:return hashlib.file_digest(f,'sha256').hexdigest()
for prefix,root in roots:
    if prefix.startswith('source'):
        files=[root/name for name in json.loads((s/'source-pins-04.json').read_text())]
    elif prefix.startswith('formatter'):
        files=[p for p in root.rglob('*') if p.is_file() and '.git' not in p.parts and '__pycache__' not in p.parts]
    else:
        files=[p for p in root.rglob('*') if p.is_file()]
    for path in sorted(files):
        relative=path.relative_to(root)
        if '__pycache__' in relative.parts or path.name.endswith(('.index','.paths')) or path.name=='.local-baseline.lock':
            excluded.append(dict(original_path=str(path),reason='Python cache or Git publication scratch file; not measured evidence'))
            continue
        encoding='gzip' if path.stat().st_size>262144 or path.read_bytes()[:4]==b'\x7fELF' else 'identity'
        destination=q/prefix/relative
        if encoding=='gzip': destination=destination.with_name(destination.name+'.gz')
        destination.parent.mkdir(parents=True,exist_ok=True)
        digest=sha(path);size=path.stat().st_size
        if encoding=='gzip':
            with path.open('rb') as src,destination.open('xb') as out:
                with gzip.GzipFile(filename='',mode='wb',compresslevel=6,fileobj=out,mtime=0) as dest:shutil.copyfileobj(src,dest)
        else:
            with path.open('rb') as src,destination.open('xb') as dest:shutil.copyfileobj(src,dest)
        assert sha(path)==digest and path.stat().st_size==size
        rows.append(dict(original_path=str(path),original_sha256=digest,original_bytes=size,stored_path=str(destination.relative_to(q)),
                         stored_sha256=sha(destination),stored_bytes=destination.stat().st_size,encoding=encoding))
inventory=dict(scope='local/unsigned',files=rows,excluded=excluded,original_bytes=sum(r['original_bytes'] for r in rows),
               stored_bytes=sum(r['stored_bytes'] for r in rows),upstream_dependencies='Kafka distribution and SDK archives are external checksum-pinned inputs in study/native-01/native-inputs.json; not copied into this archive.')
with (q/'archive-inventory.json').open('x') as f:json.dump(inventory,f,indent=2);f.write('\n')
print(len(rows),'archived files;',inventory['stored_bytes'],'stored bytes;',inventory['original_bytes'],'original bytes',flush=True)

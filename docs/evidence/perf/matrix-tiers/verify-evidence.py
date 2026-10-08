import gzip,hashlib,json
from pathlib import Path
q=Path(__file__).resolve().parent;rows=json.loads((q/'archive-manifest.json').read_text())['files']
for x in rows:
 p=q/x['stored_path'];assert p.stat().st_size==x['stored_bytes']
 with p.open('rb') as f:assert hashlib.file_digest(f,'sha256').hexdigest()==x['stored_sha256'],p
 with (gzip.open(p,'rb') if x['gzip'] else p.open('rb')) as f:assert hashlib.file_digest(f,'sha256').hexdigest()==x['sha256'],p
print(json.dumps(dict(mapped_files=len(rows),stored_bytes=sum(x['stored_bytes'] for x in rows),original_bytes=sum(x['bytes'] for x in rows),stored_and_decoded_bytes_verified=True)))

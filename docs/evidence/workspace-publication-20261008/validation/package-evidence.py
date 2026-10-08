import gzip
import hashlib
import io
import json
from pathlib import Path
import tarfile

repo = Path('/workspace/partitionline')
study = Path('/workspace/work/main-publication-20261008')
root = repo / 'docs/evidence/workspace-publication-20261008'
root.mkdir()
(root / 'chunks').mkdir()
paths = [p for p in json.loads((study / 'files.json').read_text()) if p.startswith('docs/evidence/')]
# This bytecode is a retained negative control, rather than a generated cache.
paths.append('docs/evidence/perf/baseline-profile/initial-20261007/capture/bytecode-from-control-01.pyc')
paths = sorted(set(paths))


class Chunks(io.RawIOBase):
    def __init__(self):
        super().__init__()
        self.stream = None
        self.rows = []
        self.limit = 32 * 1024 * 1024
        self.size = 0
        self.hasher = None

    def finish(self):
        if self.stream is not None:
            self.stream.close()
            self.rows.append({'path': self.name, 'bytes': self.size, 'sha256': self.hasher.hexdigest()})
            self.stream = None

    def writable(self):
        return True

    def write(self, data):
        length = len(data)
        data = memoryview(data)
        while data:
            if self.stream is None:
                self.name = f'chunks/part-{len(self.rows)+1:04d}.gz'
                self.stream = (root / self.name).open('xb')
                self.hasher = hashlib.sha256()
                self.size = 0
            count = min(len(data), self.limit-self.size)
            self.stream.write(data[:count])
            self.hasher.update(data[:count])
            self.size += count
            data = data[count:]
            if self.size == self.limit:
                self.finish()
        return length


chunks = Chunks()
files = []
objects = set()
with gzip.GzipFile(fileobj=chunks, mode='wb', compresslevel=6, mtime=0, filename='') as compressed:
    with tarfile.open(fileobj=compressed, mode='w|', format=tarfile.USTAR_FORMAT) as archive:
        for index, name in enumerate(paths, 1):
            path = repo / name
            stat = path.stat()
            with path.open('rb') as source:
                sha = hashlib.file_digest(source, 'sha256').hexdigest()
                files.append({'path':name,'sha256':sha,'bytes':stat.st_size,'mode':stat.st_mode & 0o777})
                if sha not in objects:
                    objects.add(sha)
                    source.seek(0)
                    member = tarfile.TarInfo(sha)
                    member.size = stat.st_size
                    member.mode = 0o600
                    member.mtime = 0
                    archive.addfile(member, source)
            if index % 5000 == 0:
                print(json.dumps({'files':index,'objects':len(objects),'stored_bytes':sum(r['bytes'] for r in chunks.rows)+chunks.size}), flush=True)
chunks.finish()
manifest={'schema_version':1,'files':files,'chunks':chunks.rows}
with (root/'manifest.json.gz').open('xb') as target:
    with gzip.GzipFile(fileobj=target, mode='wb', compresslevel=9, mtime=0, filename='') as compressed:
        compressed.write(json.dumps(manifest,separators=(',',':')).encode())
with (root/'manifest.json.gz').open('rb') as stream:
    manifest_sha=hashlib.file_digest(stream,'sha256').hexdigest()
summary={'schema_version':1,'scope':'Preservation of existing workspace evidence; no new qualification or performance claim',
         'source_review_commit':'a18bd45dcddcbee7969af9e1c140ee3996d6ffc4','files':len(files),'objects':len(objects),
         'original_bytes':sum(r['bytes'] for r in files),'stored_chunk_bytes':sum(r['bytes'] for r in chunks.rows),
         'manifest_sha256':manifest_sha,'chunk_limit_bytes':chunks.limit}
(root/'summary.json').write_text(json.dumps(summary,indent=2)+'\n')
(root/'SHA256SUMS').write_text(manifest_sha+'  manifest.json.gz\n'+''.join(r['sha256']+'  '+r['path']+'\n' for r in chunks.rows))
(study/'archived-paths.json').write_text(json.dumps(paths,indent=2)+'\n')
print(json.dumps(summary), flush=True)

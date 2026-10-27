import gzip
import hashlib
import json
from pathlib import Path
import shutil

study = Path('/workspace/work/qualification-20261027')
repo = Path('/workspace/partitionline')
root = repo / 'docs/evidence/broker/fuzz/20261027-launch'
root.mkdir(parents=True)
rows = []


def save(source, target):
    compressed = source.stat().st_size > 65536 and source.suffix != '.gz'
    stored = target + ('.gz' if compressed else '')
    destination = root / stored
    destination.parent.mkdir(parents=True, exist_ok=True)
    with source.open('rb') as stream:
        original = hashlib.file_digest(stream, 'sha256').hexdigest()
    if compressed:
        with destination.open('xb') as out, gzip.GzipFile(fileobj=out, mode='wb', filename='', mtime=0) as output, source.open('rb') as inp:
            shutil.copyfileobj(inp, output)
    else:
        shutil.copyfile(source, destination)
    with destination.open('rb') as stream:
        digest = hashlib.file_digest(stream, 'sha256').hexdigest()
    rows.append({'original_path': str(source), 'original_sha256': original,
                 'original_bytes': source.stat().st_size, 'stored_path': stored,
                 'stored_sha256': digest, 'stored_bytes': destination.stat().st_size,
                 'encoding': 'gzip' if compressed else 'identity'})


for source in sorted(study.iterdir()):
    if source.is_file() and source.name not in ('campaign-supervisor.log', 'freeze-launch.py'):
        save(source, 'capture/' + source.name)
for name in ('fuzz-build-01', 'fuzz-build-02', 'fuzz-build-03'):
    directory = study / name
    for source in sorted(directory.rglob('*')):
        relative = source.relative_to(directory)
        if source.is_file() and relative.parts[0] != 'target':
            save(source, 'capture/' + name + '/' + str(relative))
for name in ('runner-tests', 'launch-snapshot-01'):
    for source in sorted((study / name).rglob('*')):
        if source.is_file():
            save(source, 'capture/' + name + '/' + str(source.relative_to(study / name)))
save(repo / '.github/workflows/broker.yml', 'source/broker-workflow.yml')
save(Path(__file__), 'source/freeze-launch.py')
(root / 'archive-inventory.json').write_text(json.dumps({'schema_version': 1, 'files': rows}, indent=2) + '\n')
(root / 'SHA256SUMS').write_text(''.join(row['stored_sha256'] + '  ' + row['stored_path'] + '\n' for row in rows))
summary = {'mapped_files': len(rows), 'stored_bytes': sum(row['stored_bytes'] for row in rows),
           'original_bytes': sum(row['original_bytes'] for row in rows),
           'scope': 'Builds, real short supervisor checks, their deliberate negative copies, and a prefix snapshot of the ongoing campaign; no completed 24-hour evidence',
           'cache_exclusions': 'Generated Cargo target caches; live changing campaign corpus and files are retained only in the separate live directory'}
(root / 'retention.json').write_text(json.dumps(summary, indent=2) + '\n')
print(json.dumps(summary))

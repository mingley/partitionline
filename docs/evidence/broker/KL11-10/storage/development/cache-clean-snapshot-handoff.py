#!/usr/bin/env python3
import gzip
import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess
import time

repo = Path('/workspace/partitionline')
roots = [Path('/workspace/work/target-snapshot-runtime'),
         Path('/workspace/work/target-snapshot-runtime-msrv')]
manifest_path = repo / 'docs/evidence/broker/KL11-15/runtime/cache-handoff-binary-retention.json'
manifest = json.loads(manifest_path.read_text())

def refers(value):
    return any(value == str(root) or value.startswith(str(root) + '/') for root in roots)

refs = []
for proc in Path('/proc').iterdir():
    if not proc.name.isdigit() or int(proc.name) == os.getpid():
        continue
    try:
        args = proc.joinpath('cmdline').read_bytes().split(b'\0')
        values = [arg.decode(errors='replace').partition('=')[-1] for arg in args]
        values += [line.decode(errors='replace').partition('=')[-1]
                   for line in proc.joinpath('environ').read_bytes().split(b'\0')]
        for link in [proc / 'cwd', proc / 'exe', *list(proc.joinpath('fd').iterdir())]:
            try:
                values.append(os.readlink(link))
            except OSError:
                pass
        if any(refers(value) for value in values):
            refs.append({'pid': int(proc.name), 'args': [arg.decode(errors='replace') for arg in args if arg]})
    except OSError:
        pass
assert not refs, refs

retained = []
for row in manifest['files']:
    path = Path(row['compressed'])
    encoded = path.read_bytes()
    assert hashlib.sha256(encoded).hexdigest() == row['compressed_sha256'], path
    raw = gzip.decompress(encoded)
    assert len(raw) == row['bytes'] and hashlib.sha256(raw).hexdigest() == row['sha256'], path
    retained.append({'path': str(path), 'compressed_sha256': row['compressed_sha256'],
                     'sha256': row['sha256'], 'bytes': len(raw), 'mode': row['mode']})

receipt = {'schema': 1, 'authorization': 'root explicitly released these two generated caches; RPC preserved86ELFs',
           'cleanup_script_sha256': hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
           'time_utc': time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime()),
           'process_reference_check': refs, 'retention_manifest': str(manifest_path.relative_to(repo)),
           'retention_manifest_sha256': hashlib.sha256(manifest_path.read_bytes()).hexdigest(),
           'retained_gzip_and_decompressed_hashes_verified': retained, 'removed': []}
output = repo / 'docs/evidence/broker/KL11-10/storage/development/cache-clean-snapshot-handoff.json'
output.parent.mkdir(parents=True, exist_ok=True)
for root in roots:
    assert root.is_dir() and not root.is_symlink(), root
    size = int(subprocess.check_output(['du', '-sb', str(root)], text=True).split()[0])
    shutil.rmtree(root)
    receipt['removed'].append({'path': str(root), 'apparent_bytes': size, 'absent_after': not root.exists()})
    output.write_text(json.dumps(receipt, indent=2) + '\n')
receipt['free_bytes_after'] = shutil.disk_usage('/workspace').free
output.write_text(json.dumps(receipt, indent=2) + '\n')
print(json.dumps({'receipt': str(output), 'preserved_elf_count': len(retained),
                  'removed': receipt['removed'], 'free_bytes_after': receipt['free_bytes_after']}))

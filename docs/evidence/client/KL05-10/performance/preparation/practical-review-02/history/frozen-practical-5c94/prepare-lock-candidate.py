"""Read-only lock/cache inspection; no Cargo resolver/compiler is run."""
import hashlib
import json
from pathlib import Path
import tomllib

ROOT = Path('/workspace/partitionline')
OUT = Path(__file__).parent
root_path = ROOT / 'Cargo.lock'
peer_path = ROOT / 'benchmarks/peers/rust/Cargo.lock'
root = tomllib.loads(root_path.read_text())
peer = tomllib.loads(peer_path.read_text())
def key(package):
    return package['name'], package['version'], package.get('source', '')
packages = {key(package): package for package in root['package']}
peer_by_name = {}
for package in peer['package']:
    peer_by_name.setdefault(package['name'], []).append(package)
added = {}
def visit(package):
    identity = key(package)
    if identity in packages:  # Retain root's already accepted transitive bindings.
        return
    packages[identity] = package
    added[identity] = package
    for reference in package.get('dependencies', []):
        fields = reference.split()
        choices = peer_by_name[fields[0]]
        if len(fields) > 1:
            choices = [candidate for candidate in choices if candidate['version'] == fields[1]]
        assert len(choices) == 1, reference
        visit(choices[0])
visit(next(package for package in peer['package']
           if package['name'] == 'serde_json' and package['version'] == '1.0.149'))
packages[('partitionline-sticky-benchmark', '0.1.0', '')] = {
    'name': 'partitionline-sticky-benchmark', 'version': '0.1.0',
    'dependencies': ['bytes', 'partitionline', 'serde_json', 'sha2', 'tokio']}
text = '# WORK-only graph candidate; Cargo resolution has not been executed.\nversion = 4\n'
for package in sorted(packages.values(), key=key):
    assert set(package).issubset({'name','version','source','checksum','dependencies'})
    text += '\n[[package]]\n'
    for field in ('name', 'version', 'source', 'checksum'):
        if field in package:
            text += field + ' = ' + json.dumps(package[field]) + '\n'
    if package.get('dependencies'):
        text += 'dependencies = [\n' + ''.join(' ' + json.dumps(ref) + ',\n' for ref in package['dependencies']) + ']\n'
path = OUT / 'benchmarks/sticky-partitioner/Cargo.lock'
path.write_text(text)
assert tomllib.loads(path.read_text())['package'] == sorted(packages.values(), key=key)
cache = Path('/workspace/work/cargo/registry/cache')
artifacts = []
for package in sorted(packages.values(), key=key):
    if not package.get('source'):
        continue
    choices = list(cache.glob('*/' + package['name'] + '-' + package['version'] + '.crate'))
    assert len(choices) <= 1
    if choices:
        file = choices[0]
        sha = hashlib.sha256(file.read_bytes()).hexdigest()
        assert sha == package['checksum'], str(file)
        artifacts.append({'name':package['name'], 'version':package['version'],
                          'archive':str(file), 'sha256':sha, 'bytes':file.stat().st_size,
                          'Cargo_lock_checksum_match':True})
    else:
        artifacts.append({'name':package['name'], 'version':package['version'],
                          'cached_archive':False})
receipt = {'classification':'WORK-only manual lock graph candidate; Cargo resolver unexecuted',
           'root_lock_sha256':hashlib.sha256(root_path.read_bytes()).hexdigest(),
           'peer_lock_sha256':hashlib.sha256(peer_path.read_bytes()).hexdigest(),
           'candidate_lock_sha256':hashlib.sha256(path.read_bytes()).hexdigest(),
           'packages':len(packages),'root_bindings_retained':len(root['package']),
           'added_peer_packages':[[name,version] for name,version,_ in added],
           'registry_archive_checksum_checks':artifacts,
           'missing_archive_count':sum(entry.get('cached_archive') is False for entry in artifacts),
           'sum_checked_cached_archive_bytes':sum(entry.get('bytes',0) for entry in artifacts),
           'offline_lock_validation_pending':True,
           'Cargo_commands_executed':False,
           'future_step':'Root-authorized cargo generate-lockfile --offline in shallow WORK adopter bound to actual immutable client. Compare real resolved lock; freeze/push any correction before final --offline --locked compile.'}
(OUT / 'offline-lock-candidate.json').write_text(json.dumps(receipt,indent=2)+'\n')
print(json.dumps({k:v for k,v in receipt.items() if k!='registry_archive_checksum_checks'},indent=2))

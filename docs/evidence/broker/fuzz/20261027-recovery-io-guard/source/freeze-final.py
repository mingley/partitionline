import gzip
import hashlib
import io
import json
from pathlib import Path
import shutil
import tarfile

repo = Path('/workspace/partitionline')
study = Path('/workspace/work/qualification-20261027')
checks = Path('/workspace/work/qualification-checks-20261027')
oracle = repo / 'docs/evidence/broker/faults/20261027-generation-oracle'
fuzz = repo / 'docs/evidence/broker/fuzz/20261027-recovery-io-guard'


def bundle(directory, destination):
    entries = []
    with destination.open('xb') as out, gzip.GzipFile(fileobj=out, mode='wb', mtime=0, filename='') as z, tarfile.open(fileobj=z, mode='w|') as archive:
        for path in sorted(directory.rglob('*')):
            if path.is_symlink():
                raise ValueError('Symlink in retained campaign')
            if not path.is_file():
                continue
            name = str(path.relative_to(directory))
            with path.open('rb') as stream:
                digest = hashlib.file_digest(stream, 'sha256').hexdigest()
                stream.seek(0)
                info = tarfile.TarInfo(name)
                info.size = path.stat().st_size
                info.mode = path.stat().st_mode & 0o777
                archive.addfile(info, stream)
            entries.append({'path': name, 'sha256': digest, 'bytes': info.size})
    expected = {row['path']: row for row in entries}
    with tarfile.open(destination, 'r:gz') as archive:
        seen = set()
        for member in archive:
            assert member.isfile() and member.name in expected and member.name not in seen
            seen.add(member.name)
            stream = archive.extractfile(member)
            assert hashlib.file_digest(stream, 'sha256').hexdigest() == expected[member.name]['sha256']
            assert member.size == expected[member.name]['bytes']
        assert seen == set(expected)
    index = destination.with_suffix('.index.json')
    index.write_text(json.dumps({'files': entries, 'verified_entries': len(entries)}, indent=2) + '\n')
    return index


stopped = checks / 'stopped-fuzz-day-01.tar.gz'
stopped_index = bundle(study / 'fuzz-day-01', stopped)
controls = checks / 'all-real-supervisor-controls.tar.gz'
controls_index = bundle(study / 'runner-tests', controls)

live = study / 'fuzz-day-02'
snapshot = checks / 'live-day-02-snapshot'
snapshot.mkdir()
provenance = []
for name in ('manifest.json', 'frozen.json', 'jobs.json', 'status.json', 'telemetry.jsonl',
             *[f'{target}/fuzzer.log' for target in ('parse_header', 'decode_kraft', 'controller_respond', 'recover_journal', 'recover_epoch')]):
    source = live / name
    dest = snapshot / name
    dest.parent.mkdir(parents=True, exist_ok=True)
    length = source.stat().st_size
    with source.open('rb') as inp, dest.open('xb') as out:
        while length:
            chunk = inp.read(min(length, 1024 * 1024))
            assert chunk
            out.write(chunk)
            length -= len(chunk)
    provenance.append({'live_path': str(source), 'snapshot_path': name, 'bytes': dest.stat().st_size,
                       'scope': 'Prefix of an ongoing job; no completion evidence'})
(snapshot / 'provenance.json').write_text(json.dumps(provenance, indent=2) + '\n')


class Archive:
    def __init__(self, root):
        self.root = root
        self.root.mkdir(parents=True)
        self.rows = []

    def add(self, source, name):
        compressed = source.stat().st_size > 65536 and source.suffix != '.gz'
        target = name + ('.gz' if compressed else '')
        path = self.root / target
        path.parent.mkdir(parents=True, exist_ok=True)
        with source.open('rb') as inp:
            original = hashlib.file_digest(inp, 'sha256').hexdigest()
        if compressed:
            with source.open('rb') as inp, path.open('xb') as out, gzip.GzipFile(fileobj=out, mode='wb', mtime=0, filename='') as z:
                shutil.copyfileobj(inp, z)
        else:
            shutil.copyfile(source, path)
        with path.open('rb') as inp:
            stored = hashlib.file_digest(inp, 'sha256').hexdigest()
        self.rows.append({'original_path': str(source), 'original_sha256': original,
                          'original_bytes': source.stat().st_size, 'stored_path': target,
                          'stored_sha256': stored, 'stored_bytes': path.stat().st_size,
                          'encoding': 'gzip' if compressed else 'identity'})

    def finish(self):
        (self.root / 'archive-inventory.json').write_text(json.dumps({'schema_version': 1, 'files': self.rows}, indent=2) + '\n')
        (self.root / 'SHA256SUMS').write_text(''.join(row['stored_sha256'] + '  ' + row['stored_path'] + '\n' for row in self.rows))
        return {'files': len(self.rows), 'stored_bytes': sum(row['stored_bytes'] for row in self.rows)}


a = Archive(oracle)
for path in sorted(checks.iterdir()):
    if path.is_file() and (path.name.startswith(('broker-', 'fault-', 'oracle-', 'consumer-', 'share-', 'raft-', 'write-prefetch', 'isolated-'))):
        a.add(path, 'capture/' + path.name)
for path in (repo / 'partitionline-broker/tests/fault_campaign.rs', repo / 'partitionline-broker/src/transactions/offsets.rs', study / 'toolchain-pins.json'):
    a.add(path, 'source/' + path.name)
oracle_summary = a.finish()

b = Archive(fuzz)
for path in sorted(checks.iterdir()):
    if path.is_file() and path.name.startswith(('invalid-tmp', 'controlled-', 'runner-io', 'fuzz-io')):
        b.add(path, 'capture/' + path.name)
for directory in sorted(checks.glob('invalid-tmp-after-*')):
    if directory.is_dir():
        for path in sorted(directory.rglob('*')):
            if path.is_file():
                b.add(path, 'capture/' + str(path.relative_to(checks)))
for path in (stopped, stopped_index, controls, controls_index):
    b.add(path, 'capture/' + path.name)
for path in sorted((study / 'fuzz-build-04').rglob('*')):
    relative = path.relative_to(study / 'fuzz-build-04')
    if path.is_file() and relative.parts[0] != 'target':
        b.add(path, 'build/' + str(relative))
for path in sorted(snapshot.rglob('*')):
    if path.is_file():
        b.add(path, 'live-snapshot/' + str(path.relative_to(snapshot)))
for path in (repo / 'tests/ci/test_broker_fuzz_campaign.py', repo / 'fuzz/broker/fuzz_targets/recovery_fixture.rs', Path(__file__)):
    b.add(path, 'source/' + path.name)
fuzz_summary = b.finish()
print(json.dumps({'oracle': oracle_summary, 'fuzz': fuzz_summary,
                  'stopped_attempt_files': len(json.loads(stopped_index.read_text())['files']),
                  'control_files': len(json.loads(controls_index.read_text())['files'])}))

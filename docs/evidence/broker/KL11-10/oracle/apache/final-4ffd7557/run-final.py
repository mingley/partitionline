"""Bind the Apache retention component runner to an exact pushed scoped source."""
import argparse
import hashlib
import json
from pathlib import Path
import shutil
import subprocess
import tarfile
import time

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument('--source-sha', required=True)
args = parser.parse_args()
root = Path('/workspace/partitionline')
pin = args.source_sha
work = Path('/workspace/work/retention-apache/final-' + pin[:8])
source = work / 'source'
output = root / 'docs/evidence/broker/KL11-10/oracle/apache' / ('final-' + pin[:8])
oracle_path = 'docs/evidence/broker/KL11-10/oracle/apache'
seeds_path = 'partitionline-broker/tests/fixtures/fetch'
prefixes = [oracle_path] + [seeds_path + '/' + version + '/' + name
                          for version in ['4.1.2', '4.2.1', '4.3.1']
                          for name in ['log-batch-0.bin', 'log-batch-3.bin']]
work.mkdir(parents=True)
source.mkdir()
output.mkdir()
archive_path = work / 'claimed-source.tar'
with archive_path.open('wb') as archive:
    subprocess.run(['git', 'archive', '--format=tar', pin, '--'] + prefixes,
                   cwd=root, stdout=archive, check=True)


def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


with tarfile.open(archive_path) as archive:
    files = {member.name: hashlib.sha256(archive.extractfile(member).read()).hexdigest()
             for member in archive if member.isfile()}
with tarfile.open(archive_path) as archive:
    archive.extractall(source, filter='data')
entries = subprocess.check_output(['git', 'ls-tree', '-rz', pin, '--'] + prefixes,
                                  cwd=root).split(b'\0')
git_blobs = {}
for entry in entries:
    if entry:
        meta, name = entry.split(b'\t', 1)
        mode, kind, blob = meta.decode().split()
        assert mode == '100644' and kind == 'blob', (name, meta)
        git_blobs[name.decode()] = blob
assert set(git_blobs) == set(files)


def identity():
    for name, wanted in files.items():
        data = (source / name).read_bytes()
        assert hashlib.sha256(data).hexdigest() == wanted, name
        blob = hashlib.sha1(b'blob ' + str(len(data)).encode() + b'\0' + data).hexdigest()
        assert blob == git_blobs[name], name
    return {'passed': True, 'source_files_checked': len(files), 'git_blob_identities_checked': len(git_blobs)}


oracle = source / oracle_path
frozen = json.loads((oracle / 'source-freeze.json').read_text())
assert all(files[name] == digest for name, digest in frozen['files_sha256'].items())
review = json.loads((oracle / 'independent-storage-review.json').read_text())
storage_binding = {}
for name, wanted in review['reviewed_files_sha256'].items():
    data = subprocess.check_output(['git', 'show', pin + ':' + name], cwd=root)
    assert hashlib.sha256(data).hexdigest() == wanted, name
    storage_binding[name] = {'sha256': wanted, 'git_blob': subprocess.check_output(
        ['git', 'rev-parse', pin + ':' + name], cwd=root, text=True).strip()}
review_receipt = {
    'schema_version': 1, 'source_sha': pin, 'passed': True,
    'scope': 'Exact committed four storage source/test hashes bind the existing independent static review; no runtime or fault execution claim.',
    'candidate_review_sha256': sha(oracle / 'independent-storage-review.json'),
    'candidate_review': oracle_path + '/independent-storage-review.json',
    'committed_reviewed_files': storage_binding,
    'review_status': 'historical candidate review with subsequent scalar getter concern retained',
    'pending_followup': 'Open_loop subsequently identified that a poisoned Log.log_start_offset scalar can expose an unpublished target before first manifest durability, then reopen at the old floor. Store wire reads already fence poisoned handles. A separate committed_floor scalar correction is pending; this receipt binds source identity and does not qualify that getter behavior.',
}
(output / 'storage-review-source-binding.json').write_text(json.dumps(review_receipt, indent=2) + '\n')
report = {
    'schema_version': 1, 'source_sha': pin, 'passed': False,
    'source_archive_sha256': sha(archive_path), 'source_files_sha256': files, 'source_git_blobs': git_blobs,
    'frozen_executable_reference_files_checked': len(frozen['files_sha256']),
    'scope': 'Exact scoped pushed Apache retention oracle plus six committed input batches; official component execution and static storage source binding, not partitionline runtime qualification.',
    'commands': [],
}


def save():
    (output / 'validation.json').write_text(json.dumps(report, indent=2) + '\n')


before = identity()
start = time.time()
command = ['taskset', '-c', '0-2,4', 'python3', str(oracle / 'prepare-and-run.py'),
           '--work', str(work / 'components'), '--output', str(output / 'components'),
           '--seed-root', str(source / seeds_path)]
log = output / 'component-runner.log'
with log.open('wb') as stream:
    result = subprocess.run(command, cwd=source, stdout=stream, stderr=subprocess.STDOUT, timeout=300)
report['commands'].append({'argv': command, 'cwd': str(source), 'exit_code': result.returncode,
                           'elapsed_seconds': round(time.time() - start, 3),
                           'source_before': before, 'source_after': identity(),
                           'log': log.name, 'log_sha256': sha(log)})
save()
assert result.returncode == 0, log
component = json.loads((output / 'components/validation.json').read_text())
assert component['passed'] is True and component['positive_checks'] == 402
assert component['deliberate_failures_detected'] == 12 and len(component['commands']) == 22
assert component['assertions_identical_across_replays_and_releases'] is True
report['counts'] = {'releases': 3, 'independent_replays': 6, 'actual_component_assertions': 402,
                    'named_controlled_failures': 12, 'component_commands': 22,
                    'source_only_references_per_release': 12, 'committed_storage_files_bound': 4}
report['limitations'] = component['known_policy_differences'] + [
    'Scope includes only the exclusive Apache oracle subtree and six seed batches; unrelated evolving WORK runtime files are excluded.',
    'The real single-thread scheduler delays physical deletion; no immediate unlink or power-loss guarantee is inferred.',
    'DeleteRecords facade offsets and duplicate-map handling are source-only references, not executed Kafka broker behavior.',
    'The storage review is static; actual local storage fault/restart, wire/runtime and independent raw-state proofs remain separate.',
    'A subsequent poisoned log-start scalar getter concern is retained in storage-review-source-binding.json and remains pending a separate storage correction; component results are unaffected.',
]
report['passed'] = True
shutil.copyfile(Path(__file__), output / 'run-final.py')
report['artifacts_sha256'] = {str(path.relative_to(output)): sha(path)
                            for path in sorted(output.rglob('*'))
                            if path.is_file() and path != output / 'validation.json'}
save()
(output / 'SHA256SUMS').write_text(''.join(sha(path) + '  ' + str(path.relative_to(output)) + '\n'
                                          for path in sorted(output.rglob('*'))
                                          if path.is_file() and path.name != 'SHA256SUMS'))
print(json.dumps({'passed': True, 'source_sha': pin, 'counts': report['counts'],
                  'source_files_checked': len(files)}))

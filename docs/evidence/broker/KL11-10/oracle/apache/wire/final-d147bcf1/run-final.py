"""Bind additive DeleteRecords wire SDK and corruption proofs to pushed source."""
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
work = Path('/workspace/work/retention-apache/wire-final-' + pin[:8])
source = work / 'source'
output = root / 'docs/evidence/broker/KL11-10/oracle/apache/wire' / ('final-' + pin[:8])
oracle_path = 'docs/evidence/broker/KL11-10/oracle/apache/wire'
followup_path = 'docs/evidence/broker/KL11-10/oracle/apache/independent-storage-getter-followup.json'
prefixes = [oracle_path, followup_path]
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
for name, expected in {**frozen['source_files_sha256'], **frozen['corpus_files_sha256']}.items():
    assert sha(oracle / name) == expected, name
review = json.loads((source / followup_path).read_text())
binding = {}
for name, wanted in review['candidate_files_sha256'].items():
    data = subprocess.check_output(['git', 'show', pin + ':' + name], cwd=root)
    assert hashlib.sha256(data).hexdigest() == wanted, name
    binding[name] = {'sha256': wanted, 'git_blob': subprocess.check_output(
        ['git', 'rev-parse', pin + ':' + name], cwd=root, text=True).strip()}
(output / 'corrective-storage-review-binding.json').write_text(json.dumps({
    'schema_version': 1, 'source_sha': pin, 'passed': True,
    'scope': 'Exact committed storage hashes bind the narrow static committed-floor getter correction review; no independently executed local runtime or fault claim.',
    'followup_review_sha256': sha(source / followup_path), 'foundation_review_sha256': review['foundation_review_sha256'],
    'reviewed_files': binding, 'static_blockers': [],
    'separate_execution_scope': review['validation_separation'],
}, indent=2) + '\n')
report = {
    'schema_version': 1, 'source_sha': pin, 'passed': False, 'source_archive_sha256': sha(archive_path),
    'scope': 'Exact scoped additive API21 wire SDK/corruption oracle source; independently declared local normal policy, no Apache broker or partitionline runtime execution.',
    'source_files_sha256': files, 'source_git_blobs': git_blobs,
    'frozen_source_files_checked': len(frozen['source_files_sha256']),
    'frozen_corpus_files_checked': len(frozen['corpus_files_sha256']), 'commands': [],
}


def save():
    (output / 'validation.json').write_text(json.dumps(report, indent=2) + '\n')


def run(argv, label):
    before = identity()
    start = time.time()
    command = ['taskset', '-c', '0-2,4'] + argv
    log = output / (label + '.log')
    with log.open('wb') as stream:
        result = subprocess.run(command, cwd=source, stdout=stream, stderr=subprocess.STDOUT, timeout=300)
    report['commands'].append({'argv': command, 'cwd': str(source), 'exit_code': result.returncode,
                               'elapsed_seconds': round(time.time() - start, 3),
                               'source_before': before, 'source_after': identity(),
                               'log': log.name, 'log_sha256': sha(log)})
    save()
    assert result.returncode == 0, log


run(['python3', str(oracle / 'prepare-and-run.py'), '--work', str(work / 'sdk'),
     '--output', str(output / 'sdk')], 'official-sdk-wire-matrix')
run(['python3', str(oracle / 'check-counterexamples.py'), '--corpus', str(oracle / 'corpus/4.3.1'),
     '--work', str(work / 'counterexamples'), '--report', str(output / 'corpus-corruption-controls.json')],
    'corpus-corruption-controls')
sdk = json.loads((output / 'sdk/validation.json').read_text())
controls = json.loads((output / 'corpus-corruption-controls.json').read_text())
assert sdk['passed'] and sdk['declared_cases'] == 363 and sdk['response_goldens'] == 345
assert sdk['local_structural_rejections'] == 18 and sdk['actual_serializer_parser_checks'] == 1488
assert sdk['actual_global_error_helper_executions'] == 72 and len(sdk['commands']) == 10
assert controls['passed'] and controls['positive_baseline_passed'] and controls['counterexamples_rejected'] == 9
fresh_hashes = {}
for version in ['4.1.2', '4.2.1', '4.3.1']:
    for path in sorted((output / 'sdk' / version).iterdir()):
        if path.is_file():
            name = 'corpus/' + version + '/' + path.name
            assert sha(path) == frozen['corpus_files_sha256'][name], name
            fresh_hashes[name] = sha(path)
assert set(fresh_hashes) == set(frozen['corpus_files_sha256'])
report['counts'] = {'releases': 3, 'independent_sdk_replays': 6, 'declared_wire_cases': 363,
                    'response_goldens': 345, 'local_structural_rejections': 18,
                    'actual_serializer_parser_checks': 1488, 'actual_global_error_helper_executions': 72,
                    'sdk_commands': 10, 'corruption_controls_rejected': 9,
                    'regenerated_corpus_files_match_committed': len(fresh_hashes)}
report['limitations'] = frozen['limitations'] + [
    'Public getErrorResponse helpers preserve duplicate/empty shapes; normal mapped responses are declared local policy.',
    'Apache accepts trailing byte with one-byte remainder; local rejection is an explicit stricter structural policy.',
    'The separate committed-floor getter review is static and does not replace owner fault/restart execution or the independent storage raw-state checker.',
    'The frozen parent UnifiedLog proof of 402 assertions remains independently bound to 4ffd7557; this supplement does not relabel or rerun it.',
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

#!/usr/bin/env python3
"""Reproduce the Apache compaction corpus from one exact pushed scoped source."""
import argparse
import hashlib
import json
from pathlib import Path
import subprocess
import tarfile
import time

HERE = Path(__file__).resolve().parent
ROOT = HERE.parents[5]
PREFIXES = ['docs/evidence/broker/KL11-75/oracle/apache',
            'partitionline-broker/tests/fixtures/records-compacted']


def digest(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def require(condition, reason):
    if not condition:
        raise ValueError(reason)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--source-sha', required=True)
    parser.add_argument('--work', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    args.work.mkdir(parents=True); args.output.mkdir(parents=True)
    archive = args.work / 'claimed-source.tar'
    with archive.open('wb') as stream:
        subprocess.run(['git', 'archive', '--format=tar', args.source_sha, '--'] + PREFIXES,
                       cwd=ROOT, stdout=stream, check=True)
    source = args.work / 'source'; source.mkdir()
    blobs = {}
    for entry in subprocess.check_output(['git', 'ls-tree', '-rz', args.source_sha, '--'] + PREFIXES, cwd=ROOT).split(b'\0'):
        if entry:
            meta, name = entry.split(b'\t', 1); mode, kind, blob = meta.decode().split()
            require(kind == 'blob' and mode in ['100644', '100755'], 'regular scoped source')
            blobs[name.decode()] = {'mode': mode, 'git_blob_sha1': blob}
    with tarfile.open(archive) as bundle:
        bundle.extractall(source, filter='data')
    require(1 <= len(blobs) <= 8192, 'scoped archive file envelope')
    for name, expected in blobs.items():
        path = source / name
        require(path.is_file() and not path.is_symlink(), 'regular archived source')
        raw = path.read_bytes(); require(len(raw) <= 4 * 1024 * 1024, 'scoped source file ceiling')
        expected.update(sha256=hashlib.sha256(raw).hexdigest(), bytes=len(raw))
    def identity():
        require({str(p.relative_to(source)) for p in source.rglob('*') if p.is_file()} == set(blobs), 'exact scoped source inventory')
        for name, expected in blobs.items():
            path = source / name; raw = path.read_bytes()
            require(path.is_file() and not path.is_symlink() and len(raw) == expected['bytes'] and
                    hashlib.sha256(raw).hexdigest() == expected['sha256'] and
                    hashlib.sha1(b'blob ' + str(len(raw)).encode() + b'\0' + raw).hexdigest() == expected['git_blob_sha1'] and
                    ('100755' if path.stat().st_mode & 0o111 else '100644') == expected['mode'], 'exact source bytes/modes/Git object')
        return {'passed': True, 'source_files_checked': len(blobs)}
    oracle = source / PREFIXES[0]
    frozen = json.loads((oracle / 'source-freeze.json').read_text())
    for name, expected in frozen['inputs'].items():
        require(blobs[name]['sha256'] == expected['sha256'] and blobs[name]['mode'] == expected['mode'], 'frozen oracle/fixture input')
    report = {'schema_version': 1, 'passed': False, 'source_sha': args.source_sha,
              'scope': 'Exact scoped committed Apache oracle and independent compacted-record fixtures; actual official component reruns, no partitionline product/runtime qualification.',
              'source_archive_sha256': digest(archive), 'source_git_blobs': blobs,
              'source_freeze_sha256': digest(oracle / 'source-freeze.json'), 'commands': []}
    def save():
        (args.output / 'validation.json').write_text(json.dumps(report, indent=2) + '\n')
    def execute(command, label):
        before = identity(); started = time.monotonic()
        log = args.output / (label + '.log')
        with log.open('wb') as stream:
            result = subprocess.run(['taskset', '-c', '0-2,4'] + command, cwd=source, stdout=stream,
                                    stderr=subprocess.STDOUT, timeout=180, check=False)
        report['commands'].append({'command': ['taskset', '-c', '0-2,4'] + command, 'cwd': str(source),
                                   'exit_code': result.returncode, 'elapsed_seconds': round(time.monotonic() - started, 3),
                                   'source_before': before, 'source_after': identity(), 'log': log.name, 'log_sha256': digest(log)})
        save(); require(result.returncode == 0, 'unexpected final runner result')
    fixtures = args.work / 'regenerated-fixtures'
    execute(['python3', str(oracle / 'prepare-and-run.py'), '--work', str(args.work / 'components'),
             '--output', str(args.output / 'components'), '--fixture-root', str(fixtures)], 'component-replay')
    execute(['python3', str(oracle / 'derive-tsv.py'), '--fixture-root', str(fixtures)], 'derive-tsv')
    expected_directory = source / PREFIXES[1]
    original = {str(p.relative_to(expected_directory)): digest(p) for p in expected_directory.rglob('*') if p.is_file()}
    regenerated = {str(p.relative_to(fixtures)): digest(p) for p in fixtures.rglob('*') if p.is_file()}
    require(len(original) == 114 and regenerated == original, 'all committed component frames/JSON/TSV reproduced exactly')
    component = json.loads((args.output / 'components/validation.json').read_text())
    require(component['passed'] and component['counts']['actual_component_assertions'] == 684 and
            component['counts']['deliberate_assertion_failures_detected'] == 9, 'actual component/negative denominators')
    report.update(passed=True, counts=dict(component['counts'], regenerated_fixture_files=114),
                  fixture_files_sha256=original, final_source=identity(),
                  retained_libraries='Exact official distribution jar hashes verified each replay; jars remain outside Git.',
                  limitations=component['limits'])
    report['artifacts_sha256'] = {str(p.relative_to(args.output)): digest(p)
                                 for p in sorted(args.output.rglob('*')) if p.is_file() and p != args.output / 'validation.json'}
    save()
    sums = args.output / 'SHA256SUMS'
    sums.write_text(''.join(digest(p) + '  ' + str(p.relative_to(ROOT)) + '\n'
                           for p in sorted(args.output.rglob('*')) if p.is_file() and p != sums))
    print(json.dumps({'passed': True, 'source_sha': args.source_sha, 'counts': report['counts'],
                      'source_files_checked': len(blobs), 'receipt_sha256': digest(args.output / 'validation.json')}))


if __name__ == '__main__':
    main()

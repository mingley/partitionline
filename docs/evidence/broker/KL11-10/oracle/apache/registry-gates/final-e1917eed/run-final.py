#!/usr/bin/env python3
"""Check the pushed retention gate against separately pinned compiled receipts."""
import argparse
import gzip
import hashlib
import json
import os
from pathlib import Path, PurePosixPath
import shutil
import subprocess
import tarfile
import time

REPO = Path('/workspace/partitionline')
RUNTIME_SHA = 'd147bcf1c0164778bdbad625842363f3721bc10e'
RUNTIME = Path('/workspace/work/retention-final-d147bcf1')


def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def save(path, value):
    path.write_text(json.dumps(value, indent=2) + '\n')


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--source-sha', required=True)
    args = parser.parse_args()
    pin = subprocess.check_output(['git', 'rev-parse', args.source_sha + '^{commit}'], cwd=REPO, text=True).strip()
    work = Path('/workspace/work/retention-registry-final-' + pin[:8])
    work.mkdir()
    source = work / 'source'
    source.mkdir()
    output = REPO / 'docs/evidence/broker/KL11-10/oracle/apache/registry-gates' / ('final-' + pin[:8])
    output.mkdir()
    shutil.copyfile(Path(__file__), output / 'run-final.py')
    archive_path = work / 'source.tar.gz'
    archive_process = subprocess.Popen(['git', 'archive', '--format=tar', pin], cwd=REPO, stdout=subprocess.PIPE)
    with archive_path.open('wb') as raw:
        with gzip.GzipFile(filename='', mode='wb', fileobj=raw, mtime=0) as compressed:
            shutil.copyfileobj(archive_process.stdout, compressed)
    if archive_process.wait() != 0:
        raise RuntimeError('git archive failed')
    git_files = {}
    for entry in subprocess.check_output(['git', 'ls-tree', '-rz', pin], cwd=REPO).split(b'\0'):
        if not entry:
            continue
        attributes, raw_name = entry.split(b'\t', 1)
        mode, kind, blob = attributes.decode().split(' ')
        if mode not in ('100644', '100755') or kind != 'blob':
            raise ValueError('Unsupported committed source kind')
        git_files[raw_name.decode()] = {'git_blob': blob, 'mode': mode}
    linked = 0
    seen = set()
    # Reuse bytes only after checking them against the new commit's Git blob.
    # Commands never write source files. Every file is checked again before and
    # after every command, including files reused from the prior immutable tree.
    reuse = RUNTIME / 'source'
    with tarfile.open(archive_path, 'r:gz') as archive:
        for member in archive:
            if member.isdir():
                continue
            name = member.name
            if (name not in git_files or name in seen or not member.isfile()
                    or PurePosixPath(name).is_absolute() or '..' in PurePosixPath(name).parts):
                raise ValueError('Unexpected archive source member')
            seen.add(name)
            data = archive.extractfile(member).read()
            blob = hashlib.sha1(b'blob ' + str(len(data)).encode() + b'\0' + data).hexdigest()
            if blob != git_files[name]['git_blob']:
                raise ValueError('Committed archive/Git blob mismatch: ' + name)
            git_files[name]['sha256'] = hashlib.sha256(data).hexdigest()
            target = source / name
            target.parent.mkdir(parents=True, exist_ok=True)
            old = reuse / name
            if old.is_file() and not old.is_symlink() and sha(old) == git_files[name]['sha256']:
                os.link(old, target)
                linked += 1
            else:
                target.write_bytes(data)
                target.chmod(0o755 if git_files[name]['mode'] == '100755' else 0o644)
    if seen != git_files.keys():
        raise ValueError('Incomplete whole-source archive')
    save(output / 'source-files.json', {'source_sha': pin, 'archive_sha256': sha(archive_path), 'files': git_files})
    freeze = json.loads((source / 'docs/evidence/broker/KL11-10/oracle/apache/registry-gates/source-freeze.json').read_text())
    if any(git_files[name]['sha256'] != checksum for name, checksum in freeze['files_sha256'].items()):
        raise ValueError('Frozen gate source changed')
    runtime_receipt = RUNTIME / 'validation.json'
    runtime_integrity = RUNTIME / 'source-integrity.json'
    if (sha(runtime_receipt) != '94e6f2b35d73ba1f70775cadc6de41635e9ee8741d30b3a9e019f0d380bd0bfa'
            or sha(runtime_integrity) != '17b1a87c5ee3230f1cffd5422c5eacd082ac554de19b1795e879d0e983b1e00e'):
        raise ValueError('Frozen runtime receipt/integrity changed')
    runtime_manifest = json.loads(runtime_receipt.read_text())
    if (runtime_manifest['source_commit'] != RUNTIME_SHA or len(runtime_manifest['commands']) != 19
            or any(command['exit_code'] != 0 or not command['source_before']['all_git_blobs_match']
                   or not command['source_after']['all_git_blobs_match'] for command in runtime_manifest['commands'])):
        raise ValueError('Incomplete exact-source runtime matrix')
    shutil.copyfile(runtime_receipt, output / 'runtime-validation.json')
    shutil.copyfile(runtime_integrity, output / 'runtime-source-integrity.json')

    def identity():
        names = {str(path.relative_to(source)) for path in source.rglob('*') if path.is_file() or path.is_symlink()}
        if names != git_files.keys():
            raise ValueError('Missing/extra immutable source files')
        for name, reference in git_files.items():
            path = source / name
            if path.is_symlink():
                raise ValueError('Immutable source replaced by symlink')
            data = path.read_bytes()
            if hashlib.sha256(data).hexdigest() != reference['sha256'] or hashlib.sha1(
                    b'blob ' + str(len(data)).encode() + b'\0' + data).hexdigest() != reference['git_blob']:
                raise ValueError('Immutable source changed: ' + name)
        return {'source_files_checked': len(git_files), 'sha256_and_git_blob_identity': True}

    commands = []

    def run(argv, label):
        before = identity()
        started = time.time()
        log = output / (label + '.log')
        with log.open('wb') as stream:
            result = subprocess.run(['taskset', '-c', '0-2,4'] + argv, cwd=source,
                                    stdout=stream, stderr=subprocess.STDOUT, timeout=300)
        command = {'argv': ['taskset', '-c', '0-2,4'] + argv, 'cwd': str(source),
                   'exit_code': result.returncode, 'elapsed_seconds': round(time.time() - started, 3),
                   'log': log.name, 'log_sha256': sha(log), 'source_before': before, 'source_after': identity()}
        commands.append(command)
        save(output / 'commands.json', commands)
        if result.returncode:
            raise RuntimeError('Command failed: ' + label)
        return log

    log = run(['python', '-B', '-m', 'unittest', 'discover', '-s', 'tests/ci', '-p', 'test_broker_api_matrix.py'], 'ci-55')
    if 'Ran 55 tests' not in log.read_text() or not log.read_text().rstrip().endswith('OK'):
        raise ValueError('Incomplete CI guard execution')
    cells = []
    for tool in ('stable', '1.85.0'):
        for feature in ('default', 'all-features'):
            cell = tool + '-' + feature
            retained = output / 'inputs' / cell
            retained.mkdir(parents=True)
            prefix = RUNTIME / cell
            cargo_command = next(command for command in runtime_manifest['commands']
                                 if command['name'] == cell + '-all-targets')
            cargo_log = RUNTIME / cargo_command['name'] / 'command.log'
            if sha(cargo_log) != cargo_command['log_sha256']:
                raise ValueError('Fresh runtime command log changed')
            shutil.copyfile(cargo_log, retained / 'cargo-all-targets.log')
            reports = {}
            flags = {'wire': '--handler-report', 'metadata': '--metadata-handler-report',
                     'produce': '--produce-handler-report', 'fetch': '--read-write-handler-report',
                     'retention': '--retention-handler-report', 'controller': '--controller-handler-report'}
            argv = ['python', '-B', 'scripts/check-broker-api-matrix.py']
            for name, flag in flags.items():
                path = retained / (name + '-report.json')
                shutil.copyfile(prefix / path.name, path)
                reports[name] = {'original_path': str(prefix / path.name), 'retained_path': str(path.relative_to(output)), 'sha256': sha(path)}
                argv.extend([flag, str(path)])
            captures = retained / 'controller-tcp'
            shutil.copytree(prefix / 'controller-responses/transport', captures)
            capture_hashes = {str(p.relative_to(captures)): sha(p) for p in sorted(captures.rglob('*')) if p.is_file()}
            if len(capture_hashes) != 36:
                raise ValueError('Incomplete fresh controller TCP capture set')
            result_path = output / (cell + '-report.json')
            argv.extend(['--controller-tcp-responses', str(captures), '--data-api-versions-report',
                         str(source / 'docs/evidence/broker/KL11-68/live-produce-api-versions/validation.json'),
                         '--read-write-api-versions-report', str(source / 'docs/evidence/broker/KL11-68/live-read/api-versions.json'),
                         '--report', str(result_path)])
            run(argv, cell)
            result = json.loads(result_path.read_text())
            local = result['local_implementation']
            if result['verdict'] != 'passed' or not local['compiled_retention_report_checked'] or local['compiled_retention_api_versions_layouts_checked'] != 15:
                raise ValueError('Incomplete compiled retention gate')
            cells.append({'cell': cell, 'runtime_source_sha': RUNTIME_SHA, 'report': result_path.name,
                          'report_sha256': sha(result_path), 'compiled_inputs': reports,
                          'fresh_controller_tcp_capture_sha256': capture_hashes})
    receipt = {'schema_version': 1, 'passed': True, 'gate_source_sha': pin, 'runtime_source_sha': RUNTIME_SHA,
               'source_archive_sha256': sha(archive_path), 'source_files_checked': len(git_files),
               'source_files_hardlinked_after_exact_byte_verification': linked,
               'source_freeze_sha256': sha(source / 'docs/evidence/broker/KL11-10/oracle/apache/registry-gates/source-freeze.json'),
               'ci_tests': 55, 'commands': len(commands), 'compiled_cells': cells,
               'per_cell_cases': {'wire': 99, 'metadata': 555, 'produce': 708, 'fetch_list_offsets': 366,
                                  'controller': 201, 'retention': 363, 'retention_responses': 345,
                                  'retention_rejections': 18, 'retention_api_versions_layouts': 15,
                                  'read_write_api_versions_layouts': 15, 'fresh_controller_tcp_pairs': 18},
               'historical_live_api_versions': {'produce_exchanges': 30,
                   'produce_source_sha': 'c34bdfd3fbba65492ea49b3804dd0ae5311364b0', 'read_write_exchanges': 60,
                   'read_write_source_sha': '58810f2ed90ac9643d59a60098a29f5a2f87a8d0'},
               'runtime_validation_sha256': sha(output / 'runtime-validation.json'),
               'runtime_source_integrity_sha256': sha(output / 'runtime-source-integrity.json'),
               'qualification': 'not_run',
               'limitations': ['All fresh compiled reports and controller TCP captures retain exact d147 runtime source; no report is relabeled as the newer gate source.',
                   'Historical five/seven live client profiles retain their original source pins. Eight-profile actual live client validation is separate.',
                   'Python synthetic report mutations verify rejection; immutable compiled command receipts establish Rust execution provenance.',
                   'The optional eight-profile does not change the default/metadata4/Produce5/read-write7/controller4 advertisements or establish production qualification.']}
    receipt['artifacts_sha256'] = {str(p.relative_to(output)): sha(p) for p in sorted(output.rglob('*')) if p.is_file()}
    save(output / 'validation.json', receipt)
    (output / 'SHA256SUMS').write_text(''.join(sha(p) + '  ' + str(p.relative_to(output)) + '\n'
        for p in sorted(output.rglob('*')) if p.is_file() and p.name != 'SHA256SUMS'))
    print(json.dumps({'passed': True, 'gate_source_sha': pin, 'runtime_source_sha': RUNTIME_SHA,
                      'ci_tests': 55, 'compiled_cells': len(cells), 'validation_sha256': sha(output / 'validation.json')}))


if __name__ == '__main__':
    main()

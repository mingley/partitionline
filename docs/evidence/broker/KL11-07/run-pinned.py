#!/usr/bin/env python3
"""Archive an exact source and retain real stable/MSRV broker QA/captures."""
import argparse
import hashlib
import io
import json
import os
from pathlib import Path
import re
import subprocess
import tarfile
import time


def digest(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def files(root):
    return {str(p.relative_to(root)): digest(p) for p in sorted(root.rglob('*')) if p.is_file()}


def verify_tree(root, expected):
    found = {}
    for path in sorted(root.rglob('*')):
        if path.is_file():
            name = str(path.relative_to(root))
            data = path.read_bytes()
            blob = hashlib.sha1(b'blob ' + str(len(data)).encode() + b'\0' + data).hexdigest()
            if name not in expected or expected[name] != blob:
                raise ValueError('Git blob identity mismatch: ' + name)
            found[name] = hashlib.sha256(data).hexdigest()
    if set(found) != set(expected):
        raise ValueError('full archived file set differs from the pinned Git tree')
    receipt = hashlib.sha256(json.dumps(found, sort_keys=True).encode()).hexdigest()
    return found, receipt


def write(path, value):
    path.write_text(json.dumps(value, indent=2) + '\n')


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--repo', type=Path, required=True)
    parser.add_argument('--source-pin', required=True)
    parser.add_argument('--source', type=Path, required=True)
    parser.add_argument('--target', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    if not re.fullmatch('[0-9a-f]{40}', args.source_pin):
        raise ValueError('exact 40-character source SHA required')
    if args.source.exists():
        raise ValueError('new archive directory required; preserve old attempts')
    args.output.mkdir(parents=True, exist_ok=False)
    tree = subprocess.check_output(['git', 'ls-tree', '-r', '-z', args.source_pin], cwd=args.repo)
    expected = {}
    for entry in tree.split(b'\0'):
        if not entry:
            continue
        metadata, name = entry.split(b'\t', 1)
        mode, kind, identity = metadata.split()
        if kind != b'blob' or mode not in [b'100644', b'100755']:
            raise ValueError('unexpected archived tree object')
        expected[name.decode()] = identity.decode()
    archive = subprocess.check_output(['git', 'archive', args.source_pin], cwd=args.repo)
    args.source.mkdir(parents=True)
    with tarfile.open(fileobj=io.BytesIO(archive), mode='r:') as tar:
        tar.extractall(args.source, filter='data')
    before, manifest_identity = verify_tree(args.source, expected)
    write(args.output / 'source-integrity.json', {
        'source_pin': args.source_pin, 'archive_sha256': hashlib.sha256(archive).hexdigest(),
        'archive_scope': 'complete committed Git tree', 'file_count': len(before),
        'source_sha256': before, 'git_blob_identities': expected,
        'source_manifest_sha256': manifest_identity})
    env = os.environ.copy()
    env.update(CARGO_HOME='/workspace/work/cargo', RUSTUP_HOME='/workspace/work/rustup',
               CARGO_TARGET_DIR=str(args.target), CARGO_INCREMENTAL='0', CARGO_BUILD_JOBS='1',
               CARGO_PROFILE_DEV_DEBUG='0', CARGO_PROFILE_TEST_DEBUG='0',
               RUSTDOCFLAGS='-D warnings')
    env['PATH'] = '/workspace/work/cargo/bin:' + env['PATH']
    manifest = str(args.source / 'partitionline-broker/Cargo.toml')
    overall = []
    for toolchain in ['stable', '1.85.0']:
        lane = args.output / toolchain
        lane.mkdir()
        tool = subprocess.check_output(['rustc', '+' + toolchain, '-Vv'], env=env, text=True)
        cargo = subprocess.check_output(['cargo', '+' + toolchain, '-V'], env=env, text=True)
        (lane / 'rustc.txt').write_text(tool)
        (lane / 'cargo.txt').write_text(cargo)
        commands = [
            ('clean', ['clean', '--manifest-path', manifest, '-p', 'partitionline-broker']),
            ('fmt', ['fmt', '--manifest-path', manifest, '--check']),
        ]
        for feature, flags in [('default', []), ('all-features', ['--all-features'])]:
            commands.extend([
                (feature + '-all-targets', ['test', '--locked', '--manifest-path', manifest, '--all-targets', *flags]),
                (feature + '-clippy', ['clippy', '--locked', '--manifest-path', manifest, '--all-targets', *flags, '--', '-D', 'warnings']),
                (feature + '-rustdoc', ['doc', '--locked', '--manifest-path', manifest, '--no-deps', *flags]),
                (feature + '-doctests', ['test', '--locked', '--manifest-path', manifest, '--doc', *flags]),
            ])
        results = []
        for name, arguments in commands:
            _, before_identity = verify_tree(args.source, expected)
            command = ['taskset', '-c', '0-2,4', 'cargo', '+' + toolchain, *arguments]
            local_env = env.copy()
            if name.endswith('-all-targets'):
                capture = lane / name
                capture.mkdir()
                local_env['PARTITIONLINE_FETCH_REPORT'] = str(capture / 'compiled-report.json')
                local_env['PARTITIONLINE_FETCH_RESPONSE_DIR'] = str(capture / 'responses')
                local_env['PARTITIONLINE_WIRE_REPORT'] = str(capture / 'wire-report.json')
                local_env['PARTITIONLINE_METADATA_REPORT'] = str(capture / 'metadata-report.json')
                local_env['PARTITIONLINE_PRODUCE_REPORT'] = str(capture / 'produce-report.json')
            start = time.monotonic()
            with (lane / (name + '.log')).open('wb') as log:
                result = subprocess.run(command, cwd=args.source, env=local_env, stdout=log, stderr=subprocess.STDOUT)
            _, after_identity = verify_tree(args.source, expected)
            if before_identity != after_identity:
                raise ValueError('source changed during command')
            text = (lane / (name + '.log')).read_text()
            summaries = re.findall(r'test result: ok\. (\d+) passed; (\d+) failed; (\d+) ignored', text)
            item = {'name': name, 'command': command, 'exit_code': result.returncode,
                    'elapsed_seconds': round(time.monotonic() - start, 3),
                    'log_sha256': digest(lane / (name + '.log')),
                    'full_git_blob_identities_match': True, 'source_file_count': len(expected),
                    'before_source_manifest_sha256': before_identity,
                    'after_source_manifest_sha256': after_identity,
                    'test_summaries': [{'passed': int(p), 'failed': int(f), 'ignored': int(i)} for p,f,i in summaries]}
            if name.endswith('-all-targets') and result.returncode == 0:
                report = json.loads((capture / 'compiled-report.json').read_text())
                if len(report['case_results']) != 366 or len(report['api_versions_cases']) != 15:
                    raise ValueError('incomplete authentic read coverage')
                item['case_results'] = len(report['case_results'])
                item['api_versions_cases'] = len(report['api_versions_cases'])
                item['compiled_report_sha256'] = digest(capture / 'compiled-report.json')
                item['response_sha256'] = files(capture / 'responses')
            results.append(item)
            write(lane / 'results.json', {'source_pin': args.source_pin, 'toolchain': toolchain,
                  'commands': results, 'affinity': '0-2,4', 'env': {k: env[k] for k in [
                  'CARGO_INCREMENTAL', 'CARGO_BUILD_JOBS', 'CARGO_PROFILE_DEV_DEBUG', 'CARGO_PROFILE_TEST_DEBUG', 'RUSTDOCFLAGS']}})
            print(toolchain, name, result.returncode, flush=True)
            if result.returncode:
                raise SystemExit(result.returncode)
        after, _ = verify_tree(args.source, expected)
        if before != after:
            raise ValueError('source archive changed during execution')
        overall.append({'toolchain': toolchain, 'commands': results, 'source_unchanged': True})
        write(args.output / 'matrix-results.json', {'source_pin': args.source_pin, 'matrix': overall})


if __name__ == '__main__':
    main()

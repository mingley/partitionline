#!/usr/bin/env python3
"""Check offset UUID schemas and public callers against pinned Apache clients."""
import argparse
import csv
import ctypes
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import shutil
import subprocess
import time

REPO = Path(__file__).resolve().parents[2]
SOURCES = [REPO / 'tests/fixtures/offset-topic-ids-v10' / name for name in
           ['OffsetTopicIdsOracle.java', 'OffsetTopicIdsPublic.java']]
PINS = {'4.1.2': 'afe861c05067f4018a3148d73c1ed1e5fc90808757c15b043527d7e535a5d431',
        '4.2.1': '6a281026416938a53c105f2d91d2807fdc83d5658452abf0ad1b6d8ab8a553c8',
        '4.3.1': '52501b7b47510c66f898871adaf6d2968ab7246561d44ced43643a8a587f0b36'}
SLF4J = 'd3ef575e3e4979678dc01bf1dcce51021493b4d11fb7f1be8ad982877c16a1c0'
PROFILES = ['v8', 'v9', 'v10', 'mixed']
DRIVERS = ['java-admin', 'java-consumer', 'java-batch', 'rust-admin', 'rust-group', 'rust-typed', 'rust-batch']
spec = importlib.util.spec_from_file_location('process_owner', REPO / 'scripts/run-benchmark-matrix.py')
owner = importlib.util.module_from_spec(spec)
spec.loader.exec_module(owner)


def sha(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def save(path, data):
    with path.open('x') as file:
        json.dump(data, file, indent=2)
        file.write('\n')


def tree(path):
    return {str(p.relative_to(path)): sha(p) for p in sorted(path.rglob('*')) if p.is_file()}


def run_case(args, release, profile, driver, fixtures, classes, cp):
    directory = args.output / (release + '-' + profile + '-' + driver)
    directory.mkdir()
    env = owner.base_env()
    env.update(OFFSET_ID_DIRECTORY=str(directory), OFFSET_ID_FIXTURES=str(fixtures),
               OFFSET_ID_PROFILE=profile, OFFSET_ID_DRIVER=driver)
    command = [str(args.binary), 'serve_offset_id_probe', '--ignored', '--exact', '--nocapture']
    receipt = dict(command=command, release=release, profile=profile, driver=driver, parent_waited=False)
    peer = None
    try:
        with (directory / 'peer.log').open('x') as log:
            peer = subprocess.Popen(command, env=env, stdout=log, stderr=subprocess.STDOUT, start_new_session=True)
            receipt['pid'] = peer.pid
            deadline = time.monotonic() + 8
            while not (directory / 'ready').exists():
                if peer.poll() is not None or time.monotonic() > deadline:
                    raise RuntimeError('offset peer startup failed')
                time.sleep(.01)
            if driver.startswith('java'):
                expected = 'unsupported' if profile == 'v10' else 'supported'
                owner.execute(['java', '-Xmx128m', '-cp', str(classes) + os.pathsep + cp,
                               'OffsetTopicIdsPublic', (directory / 'ready').read_text(),
                               driver.removeprefix('java-'), expected, str(directory / 'java-outcome.json')],
                              env, directory, 'java', 10)
            else:
                peer.wait(timeout=8)
    finally:
        (directory / 'stop').touch(exist_ok=True)
        if peer:
            try:
                peer.wait(timeout=8)
            except subprocess.TimeoutExpired:
                receipt['forced_stop'] = True
                owner.stop_group(peer)
            receipt.update(exit_code=peer.returncode, parent_waited=True)
        save(directory / 'process.json', receipt)
    if receipt.get('forced_stop') or receipt.get('exit_code') != 0:
        raise ValueError('offset peer failed owned shutdown')
    closure = (directory / 'closure.txt').read_text()
    if 'runtime_tasks=0' not in closure or 'ports_closed_and_rebound=3' not in closure:
        raise ValueError('missing offset closure receipt')
    outcome = json.loads((directory / ('java-outcome.json' if driver.startswith('java') else 'rust-outcome.json')).read_text())
    parsed = owner.execute(['java', '-Xmx128m', '-cp', str(classes) + os.pathsep + cp,
                            'OffsetTopicIdsOracle', 'parse-runtime', str(directory), driver],
                           env, directory, 'parse-runtime', 8).read_text()
    counts = json.loads(parsed.splitlines()[-1])
    counts.update(release=release, profile=profile, driver=driver, public_outcome=outcome)
    unsupported = driver.startswith('java') and profile == 'v10' or driver == 'rust-typed' and profile in ['v8', 'v9']
    if unsupported:
        if outcome['successful_public_calls'] != 0 or counts['actual_commit_frames'] != 0 or counts['actual_fetch_frames'] != 0:
            raise ValueError('unsupported intent dispatched an offset operation')
        counts['status'] = 'expected_unsupported'
    else:
        batch = driver.endswith('batch')
        if outcome['successful_public_calls'] != (1 if batch else 2) or counts['actual_commit_frames'] != (0 if batch else 1) or counts['actual_fetch_frames'] < 1:
            raise ValueError('missing actual public offset calls')
        counts['status'] = 'pass'
    with (directory / 'frames.tsv').open() as file:
        rows = list(csv.DictReader(file, delimiter='\t'))
    app = [row for row in rows if row['kind'] == 'request' and row['api'] in ['8', '9']]
    if any(row['slot'] == '0' for row in app):
        raise ValueError('offset operation went to bootstrap')
    expected_version = '8' if profile == 'v8' else '9' if profile == 'v9' or profile == 'mixed' and driver.startswith('java') else '10'
    if any(row['version'] != expected_version for row in app):
        raise ValueError('actual negotiated offset version differs')
    if driver.startswith('rust') and any(row['kind'] == 'request' and row['api'] == '10' and row['slot'] != '0' for row in rows):
        raise ValueError('Rust offset discovery did not use bootstrap')
    return counts


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ['binary', 'binding', 'wire-binary', 'wire-binding', 'jars', 'slf4j', 'output']:
        parser.add_argument('--' + name, type=Path, required=True)
    args = parser.parse_args()
    args.binary = args.binary.resolve()
    args.wire_binary = args.wire_binary.resolve()
    args.output = args.output.resolve()
    args.output.mkdir(parents=True, exist_ok=False)
    if ctypes.CDLL(None, use_errno=True).prctl(36, 1, 0, 0, 0) != 0:
        raise OSError('subreaper unavailable')
    inputs = SOURCES + [Path(__file__).resolve(), REPO / 'scripts/run-benchmark-matrix.py', args.slf4j]
    for binary, path, lane in [(args.binary, args.binding, 'serve_offset_id_probe'),
                                (args.wire_binary, args.wire_binding, 'actual_sdk_offset_id_bodies')]:
        binding = json.loads(path.read_text())
        if sha(binary) != binding['binary_sha256']:
            raise ValueError('offset binary binding differs')
        for name, digest in binding['sources'].items():
            if sha(REPO / name) != digest:
                raise ValueError('offset source binding differs: ' + name)
        inputs += [binary, path] + [REPO / name for name in binding['sources']]
        listing = owner.execute([str(binary), '--list'], owner.base_env(), args.output, lane + '-list', 8).read_text()
        if lane + ': test' not in listing:
            raise ValueError('required offset lane missing')
    if sha(args.slf4j) != SLF4J:
        raise ValueError('logging SDK pin differs')
    for release, digest in PINS.items():
        jar = args.jars / ('kafka-clients-' + release + '.jar')
        if sha(jar) != digest:
            raise ValueError('offset SDK pin differs')
        inputs.append(jar)
    guard = {str(path): sha(path) for path in inputs}
    save(args.output / 'source-bindings.json', guard)
    for path in inputs:
        if path.is_relative_to(REPO):
            destination = args.output / 'executed-source' / path.relative_to(REPO)
            destination.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(path, destination)
    results = []
    for release in PINS:
        classes = args.output / (release + '-classes')
        classes.mkdir()
        cp = str(args.jars / ('kafka-clients-' + release + '.jar')) + os.pathsep + str(args.slf4j)
        env = owner.base_env()
        owner.execute(['java', '--add-modules', 'jdk.compiler', 'com.sun.tools.javac.Main', '-source', '21',
                       '-target', '21', '-Xlint:all', '-Werror', '-cp', cp, '-d', str(classes),
                       *[str(source) for source in SOURCES]], env, args.output, release + '-compile', 30)
        java = ['java', '-Xmx128m', '-cp', str(classes) + os.pathsep + cp, 'OffsetTopicIdsOracle']
        fixtures = args.output / (release + '-fixtures-1')
        second = args.output / (release + '-fixtures-2')
        owner.execute(java + [str(fixtures)], env, args.output, release + '-generate-1', 8)
        owner.execute(java + [str(second)], env, args.output, release + '-generate-2', 8)
        if tree(fixtures) != tree(second):
            raise ValueError('offset SDK generations differ')
        reverse = args.output / (release + '-reverse')
        wire_env = env | {'OFFSET_TOPIC_IDS_FIXTURES': str(fixtures), 'OFFSET_TOPIC_IDS_REVERSE': str(reverse)}
        owner.execute([str(args.wire_binary), 'actual_sdk_offset_id_bodies', '--ignored', '--exact', '--nocapture'],
                      wire_env, args.output, release + '-rust-wire', 8)
        parsed = owner.execute(java + ['verify', str(reverse)], env, args.output, release + '-verify-rust', 8).read_text()
        if json.loads(parsed.splitlines()[-1])['independently_parsed_rust_bodies'] != 60:
            raise ValueError('missing independently parsed offset bodies')
        seeds = {}
        for version in [8, 9, 10]:
            seeds[version] = args.output / (release + '-runtime-' + str(version))
            duplicate = args.output / (release + '-runtime-' + str(version) + '-repeat')
            for number, destination in [(1, seeds[version]), (2, duplicate)]:
                owner.execute(java + ['runtime-seed', str(destination), str(version)], env, args.output,
                              release + '-runtime-seed-' + str(version) + '-' + str(number), 8)
            if tree(seeds[version]) != tree(duplicate):
                raise ValueError('runtime offset SDK generations differ')
        for profile in PROFILES:
            for driver in DRIVERS:
                version = 8 if profile == 'v8' else 9 if profile == 'v9' or profile == 'mixed' and driver.startswith('java') else 10
                results.append(run_case(args, release, profile, driver, seeds[version], classes, cp))
        for version in [10, 11, 12, 13]:
            results.append(run_case(args, release, 'metadata-' + str(version), 'rust-admin', seeds[10], classes, cp))
    for name, digest in guard.items():
        if sha(name) != digest:
            raise ValueError('executed offset input changed')
    save(args.output / 'summary.json', dict(status='pass', actual_sdk=True, source_bound=True,
         all_processes_parent_waited=True, process_profiles=len(results), results=results,
         independent_offset_bodies=180, scope='Owned scripted peers, actual public Java Admin/manual Consumer and Rust Admin/group/typed calls. No live broker qualification.'))


if __name__ == '__main__':
    main()

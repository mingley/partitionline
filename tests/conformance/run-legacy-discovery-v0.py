#!/usr/bin/env python3
"""Compare legacy discovery wire bodies and public calls with Apache SDKs."""
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
PINS = {'4.1.2': 'afe861c05067f4018a3148d73c1ed1e5fc90808757c15b043527d7e535a5d431',
        '4.2.1': '6a281026416938a53c105f2d91d2807fdc83d5658452abf0ad1b6d8ab8a553c8',
        '4.3.1': '52501b7b47510c66f898871adaf6d2968ab7246561d44ced43643a8a587f0b36'}
SLF4J = 'd3ef575e3e4979678dc01bf1dcce51021493b4d11fb7f1be8ad982877c16a1c0'
SOURCES = [REPO / 'tests/fixtures/legacy-discovery-v0' / name for name in
           ['LegacyDiscoveryOracle.java', 'LegacyDiscoveryPublic.java']]
PROFILES = ['legacy', 'modern-group0', 'mixed']
FAULTS = ['loading', 'unavailable', 'wrong-coordinator', 'disconnect', 'moved', 'same-node-address', 'terminal', 'deadline']
GROUP_DRIVERS = ['rust-admin', 'java-admin-offset', 'java-consumer-offset']
DRIVERS = ['rust-admin', 'rust-consumer', 'rust-producer', 'java-admin-offset',
           'java-consumer-offset', 'java-consumer-metadata', 'java-producer-metadata']
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


def run_case(args, release, profile, driver, classes, cp):
    directory = args.output / (release + '-' + profile + '-' + driver)
    directory.mkdir()
    env = owner.base_env() | dict(LEGACY_DISCOVERY_DIRECTORY=str(directory),
                                 LEGACY_DISCOVERY_PROFILE=profile, LEGACY_DISCOVERY_DRIVER=driver)
    command = [str(args.binary), 'serve_legacy_discovery_probe', '--ignored', '--exact', '--nocapture']
    receipt = dict(command=command, release=release, profile=profile, driver=driver, parent_waited=False)
    peer = None
    unsupported = profile == 'legacy' and driver.startswith('java')
    try:
        with (directory / 'peer.log').open('x') as log:
            peer = subprocess.Popen(command, env=env, stdout=log, stderr=subprocess.STDOUT, start_new_session=True)
            receipt['pid'] = peer.pid
            deadline = time.monotonic() + 8
            while not (directory / 'ready').exists():
                if peer.poll() is not None or time.monotonic() > deadline:
                    raise RuntimeError('legacy peer startup failed')
                time.sleep(.01)
            if driver.startswith('java'):
                owner.execute(['java', '-Xmx128m', '-cp', str(classes) + os.pathsep + cp,
                               'LegacyDiscoveryPublic', (directory / 'ready').read_text(),
                               driver.removeprefix('java-'), 'legacy-policy' if unsupported else 'not-coordinator' if profile == 'wrong-coordinator' and driver == 'java-admin-offset' else profile if profile in ['terminal', 'deadline'] else 'supported',
                               str(directory / 'java-outcome.json')], env, directory, 'java', 6)
            else:
                peer.wait(timeout=8)
    finally:
        (directory / 'stop').touch(exist_ok=True)
        if peer:
            try:
                peer.wait(timeout=8)
            except subprocess.TimeoutExpired:
                receipt['forced_stop'] = True
                receipt['adopted_children'] = owner.stop_group(peer)
            receipt.update(exit_code=peer.returncode, parent_waited=True)
            if owner.group_members(peer.pid):
                raise RuntimeError('owned peer left processes running')
        save(directory / 'process.json', receipt)
    if receipt.get('forced_stop') or receipt.get('exit_code') != 0:
        raise ValueError('legacy peer failed owned shutdown')
    closure = (directory / 'closure.txt').read_text()
    if 'runtime_tasks=0' not in closure or 'ports_closed_and_rebound=3' not in closure:
        raise ValueError('missing closure receipt')
    outcome = json.loads((directory / ('java-outcome.json' if driver.startswith('java') else 'rust-outcome.json')).read_text())
    parsed = owner.execute(['java', '-Xmx128m', '-cp', str(classes) + os.pathsep + cp,
                            'LegacyDiscoveryOracle', 'parse-runtime', str(directory)],
                           env, directory, 'parse-runtime', 6).read_text()
    counts = json.loads(parsed.splitlines()[-1])
    with (directory / 'frames.tsv').open() as file:
        rows = list(csv.DictReader(file, delimiter='\t'))
    requests = [row for row in rows if row['kind'] == 'request']
    metadata = [row for row in requests if row['api'] == '3']
    find = [row for row in requests if row['api'] == '10']
    offsets = [row for row in requests if row['api'] == '9']
    if any(row['version'] != ('0' if profile == 'legacy' else '13') for row in metadata):
        raise ValueError('actual Metadata version differs')
    if any(row['version'] != ('6' if profile == 'mixed' else '0') for row in find):
        raise ValueError('actual FindCoordinator version differs')
    if unsupported:
        if outcome['successful_public_calls'] != 0 or metadata:
            raise ValueError('SDK legacy Metadata factory refusal dispatched a Metadata frame')
    elif profile in ['terminal', 'deadline'] or profile == 'wrong-coordinator' and driver == 'java-admin-offset':
        if not find or offsets or outcome['successful_public_calls'] != 0:
            raise ValueError('failed discovery dispatched an offset request')
    elif profile in ['moved', 'same-node-address']:
        expected_slots = ['1', '1'] if profile == 'same-node-address' and driver == 'java-admin-offset' else ['1', '2']
        if len(find) < 2 or [row['slot'] for row in offsets] != expected_slots:
            raise ValueError('coordinator address movement was not observed')
    elif driver.endswith('offset') or driver == 'rust-admin':
        if not find or not offsets or any(row['slot'] != '1' for row in offsets):
            raise ValueError('missing public coordinator route')
    elif not metadata:
        raise ValueError('missing public Metadata call')
    if (profile in ['loading', 'unavailable', 'disconnect'] or profile == 'wrong-coordinator' and driver != 'java-admin-offset') and len(find) < 2:
        raise ValueError('coordinator retry was not observed')
    counts.update(release=release, profile=profile, driver=driver, public_outcome=outcome,
                  status='expected_legacy_policy' if unsupported else 'expected_discovery_failure' if profile in ['terminal', 'deadline'] else 'expected_reference_policy' if profile == 'wrong-coordinator' and driver == 'java-admin-offset' else 'pass')
    return counts


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ['binary', 'binding', 'jars', 'slf4j', 'output']:
        parser.add_argument('--' + name, type=Path, required=True)
    args = parser.parse_args()
    args.binary = args.binary.resolve()
    args.output = args.output.resolve()
    args.output.mkdir(parents=True, exist_ok=False)
    if ctypes.CDLL(None, use_errno=True).prctl(36, 1, 0, 0, 0) != 0:
        raise OSError('subreaper unavailable')
    binding = json.loads(args.binding.read_text())
    if sha(args.binary) != binding['binary_sha256']:
        raise ValueError('legacy binary binding differs')
    for name, digest in binding['sources'].items():
        if sha(REPO / name) != digest:
            raise ValueError('legacy source binding differs: ' + name)
    inputs = SOURCES + [Path(__file__).resolve(), REPO / 'scripts/run-benchmark-matrix.py', args.slf4j,
                        args.binary, args.binding] + [REPO / name for name in binding['sources']]
    if sha(args.slf4j) != SLF4J:
        raise ValueError('logging SDK pin differs')
    for release, digest in PINS.items():
        jar = args.jars / ('kafka-clients-' + release + '.jar')
        if sha(jar) != digest:
            raise ValueError('SDK pin differs')
        inputs.append(jar)
    guard = {str(path): sha(path) for path in inputs}
    save(args.output / 'source-bindings.json', guard)
    for path in inputs:
        if path.is_relative_to(REPO):
            destination = args.output / 'executed-source' / path.relative_to(REPO)
            destination.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(path, destination)
    listing = owner.execute([str(args.binary), '--list'], owner.base_env(), args.output, 'required-lanes', 8).read_text()
    for lane in ['serve_legacy_discovery_probe', 'actual_sdk_legacy_discovery_bodies']:
        if lane + ': test' not in listing:
            raise ValueError('required legacy lane missing')
    results = []
    for release in PINS:
        classes = args.output / (release + '-classes')
        classes.mkdir()
        cp = str(args.jars / ('kafka-clients-' + release + '.jar')) + os.pathsep + str(args.slf4j)
        env = owner.base_env()
        owner.execute(['java', '--add-modules', 'jdk.compiler', 'com.sun.tools.javac.Main', '-source', '21',
                       '-target', '21', '-Xlint:all', '-Werror', '-cp', cp, '-d', str(classes),
                       *[str(source) for source in SOURCES]], env, args.output, release + '-compile', 30)
        java = ['java', '-Xmx128m', '-cp', str(classes) + os.pathsep + cp, 'LegacyDiscoveryOracle']
        fixtures = args.output / (release + '-fixtures-1')
        second = args.output / (release + '-fixtures-2')
        owner.execute(java + [str(fixtures)], env, args.output, release + '-generate-1', 8)
        owner.execute(java + [str(second)], env, args.output, release + '-generate-2', 8)
        if tree(fixtures) != tree(second):
            raise ValueError('SDK generations differ')
        reverse = args.output / (release + '-reverse')
        wire_env = env | {'LEGACY_DISCOVERY_FIXTURES': str(fixtures), 'LEGACY_DISCOVERY_REVERSE': str(reverse)}
        owner.execute([str(args.binary), 'actual_sdk_legacy_discovery_bodies', '--ignored', '--exact', '--nocapture'],
                      wire_env, args.output, release + '-rust-wire', 8)
        parsed = owner.execute(java + ['verify', str(reverse), str(fixtures)], env, args.output, release + '-verify-rust', 8).read_text()
        if json.loads(parsed.splitlines()[-1])['independently_parsed_rust_bodies'] != 42:
            raise ValueError('missing independently parsed legacy bodies')
        for profile in PROFILES:
            for driver in DRIVERS:
                results.append(run_case(args, release, profile, driver, classes, cp))
        for profile in FAULTS:
            for driver in GROUP_DRIVERS:
                results.append(run_case(args, release, profile, driver, classes, cp))
    for name, digest in guard.items():
        if sha(name) != digest:
            raise ValueError('executed legacy input changed')
    save(args.output / 'summary.json', dict(status='pass', actual_sdk=True, source_bound=True,
         all_processes_parent_waited=True, process_profiles=len(results), results=results,
         independent_legacy_bodies=126, scope='Owned socket peers; actual public Java and Rust callers. Java Metadata v0 factory policy retained. No live broker or performance qualification.'))


if __name__ == '__main__':
    main()

#!/usr/bin/env python3
"""Run public Rust Streams heartbeat histories using actual Apache builder/error inputs."""
import argparse
import csv
import ctypes
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import shutil

REPO = Path(__file__).resolve().parents[2]
SOURCE = REPO / 'tests/fixtures/streams-heartbeat/StreamsHeartbeatOracle.java'
PINS = {
    '4.1.2': 'afe861c05067f4018a3148d73c1ed1e5fc90808757c15b043527d7e535a5d431',
    '4.2.1': '6a281026416938a53c105f2d91d2807fdc83d5658452abf0ad1b6d8ab8a553c8',
    '4.3.1': '52501b7b47510c66f898871adaf6d2968ab7246561d44ced43643a8a587f0b36',
}
SLF4J = 'd3ef575e3e4979678dc01bf1dcce51021493b4d11fb7f1be8ad982877c16a1c0'
spec = importlib.util.spec_from_file_location('process_owner', REPO / 'scripts/run-benchmark-matrix.py')
owner = importlib.util.module_from_spec(spec)
spec.loader.exec_module(owner)


def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def save(path, value):
    with path.open('x') as file:
        json.dump(value, file, indent=2);file.write('\n')


def tree(path):
    return {str(p.relative_to(path)): sha(p) for p in sorted(path.rglob('*')) if p.is_file()}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ('binary', 'binding', 'jars', 'slf4j', 'output'):
        parser.add_argument('--' + name, type=Path, required=True)
    args = parser.parse_args();args.binary = args.binary.resolve();args.output = args.output.resolve()
    args.output.mkdir(parents=True, exist_ok=False)
    if ctypes.CDLL(None, use_errno=True).prctl(36, 1, 0, 0, 0) != 0:
        raise OSError(ctypes.get_errno(), 'subreaper unavailable')
    binding = json.loads(args.binding.read_text())
    if sha(args.binary) != binding['binary_sha256']:
        raise ValueError('Rust binary differs from source-bound executable')
    for name, digest in binding['sources'].items():
        if sha(REPO / name) != digest:
            raise ValueError('Rust source differs: ' + name)
    inputs = [SOURCE, Path(__file__).resolve(), REPO / 'scripts/run-benchmark-matrix.py', args.binary, args.binding, args.slf4j]
    inputs.extend(REPO / name for name in binding['sources'])
    for release, digest in PINS.items():
        jar = args.jars / ('kafka-clients-' + release + '.jar')
        if sha(jar) != digest:raise ValueError('Apache SDK pin differs')
        inputs.append(jar)
        inputs.extend(p for p in (REPO / 'tests/fixtures/streams' / release).glob('*') if p.is_file())
    if sha(args.slf4j) != SLF4J:raise ValueError('Logging SDK pin differs')
    guard = {str(p): sha(p) for p in inputs};save(args.output / 'source-bindings.json', guard)
    save(args.output / 'rust-binary-binding.json', binding)
    for p in inputs:
        if p.is_relative_to(REPO):
            dst = args.output / 'executed-source' / p.relative_to(REPO);dst.parent.mkdir(parents=True, exist_ok=True);shutil.copy2(p, dst)
    env = owner.base_env()
    listing = owner.execute([str(args.binary), '--list'], env, args.output, 'binary-tests', 10).read_text()
    if 'actual_sdk_public_heartbeat_histories: test' not in listing:
        raise ValueError('Required external heartbeat lane missing')
    results = []
    for release in PINS:
        classes = args.output / (release + '-classes');classes.mkdir()
        cp = str(args.jars / ('kafka-clients-' + release + '.jar')) + os.pathsep + str(args.slf4j)
        owner.execute(['java', '--add-modules', 'jdk.compiler', 'com.sun.tools.javac.Main', '-source', '21', '-target', '21',
                       '-Xlint:all', '-Werror', '-cp', cp, '-d', str(classes), str(SOURCE)], env, args.output, release + '-compile', 30)
        java = ['java', '-Xmx128m', '-cp', str(classes) + os.pathsep + cp, 'StreamsHeartbeatOracle']
        original = REPO / 'tests/fixtures/streams' / release
        first = args.output / (release + '-fixtures-1');second = args.output / (release + '-fixtures-2')
        owner.execute(java + ['generate', str(original), str(first)], env, args.output, release + '-generate-1', 30)
        owner.execute(java + ['generate', str(original), str(second)], env, args.output, release + '-generate-2', 30)
        if tree(first) != tree(second):raise ValueError('Actual SDK generations differ')
        with (first / 'cases.tsv').open() as file:cases = list(csv.DictReader(file, delimiter='\t'))
        proof = args.output / (release + '-public-proof')
        rust_env = dict(env, STREAMS_HEARTBEAT_FIXTURES=str(first), STREAMS_HEARTBEAT_PROOF=str(proof))
        owner.execute([str(args.binary), 'actual_sdk_public_heartbeat_histories', '--ignored', '--exact', '--nocapture'],
                      rust_env, args.output, release + '-public-rust', 30)
        parsed = owner.execute(java + ['parse', str(first), str(proof)], env, args.output, release + '-actual-parse', 30).read_text()
        result = json.loads(parsed.splitlines()[-1]);result['release'] = release
        calls = len(cases) * 3;frames = (len(cases) + sum(14 <= int(c['code']) <= 16 for c in cases)) * 3
        if result['public_Rust_calls'] != calls or result['actual_heartbeat_frames'] != frames or result['actual_GROUP_discovery_frames'] != frames:
            raise ValueError('Missing actual public histories')
        closures = list(proof.glob('*.closure.txt'))
        if len(closures) != calls or any('runtime_tasks=0' not in p.read_text() or 'ports_closed_and_rebound=3' not in p.read_text() for p in closures):
            raise ValueError('Missing explicit owned shutdown proof')
        result['builder_default'] = (first / 'builder-default.txt').read_text().strip();results.append(result)
    for name, digest in guard.items():
        if sha(Path(name)) != digest:raise ValueError('Executed source/input changed')
    save(args.output / 'summary.json', dict(status='pass', results=results, source_bound=True, all_processes_parent_waited=True,
        actual_sdk=True, scope='Public Rust heartbeat against scripted GROUP peers; actual Apache builders/error factories/parsers. No broker or framework execution.'))


if __name__ == '__main__':
    main()

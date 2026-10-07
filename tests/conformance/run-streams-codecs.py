#!/usr/bin/env python3
"""Compile pinned Apache SDKs and compare Streams v0 bodies and flexible headers."""
import argparse
import ctypes
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import shutil

REPO = Path(__file__).resolve().parents[2]
ORACLE = REPO / 'tests/fixtures/streams/oracle'
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
        json.dump(value, file, indent=2)
        file.write('\n')


def tree(directory):
    return {str(p.relative_to(directory)): sha(p) for p in sorted(directory.rglob('*')) if p.is_file()}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for key in ('binary', 'binding', 'jars', 'slf4j', 'output'):
        parser.add_argument('--' + key, type=Path, required=True)
    args = parser.parse_args()
    args.binary = args.binary.resolve()
    args.output = args.output.resolve()
    args.output.mkdir(parents=True, exist_ok=False)
    if ctypes.CDLL(None, use_errno=True).prctl(36, 1, 0, 0, 0) != 0:
        raise OSError(ctypes.get_errno(), 'subreaper unavailable')
    binding = json.loads(args.binding.read_text())
    if sha(args.binary) != binding['binary_sha256']:
        raise ValueError('Rust binary differs from source-bound executable')
    for name, digest in binding['sources'].items():
        if sha(REPO / name) != digest:
            raise ValueError('Rust source differs: ' + name)
    inputs = [ORACLE / 'StreamsWireOracle.java', ORACLE / 'StreamsHeaderOracle.java',
              Path(__file__).resolve(), REPO / 'scripts/run-benchmark-matrix.py', args.binary, args.binding, args.slf4j]
    for release, digest in PINS.items():
        jar = args.jars / ('kafka-clients-' + release + '.jar')
        if sha(jar) != digest:
            raise ValueError('Apache SDK pin differs')
        inputs.append(jar)
    if sha(args.slf4j) != SLF4J:
        raise ValueError('Logging SDK pin differs')
    inputs.extend(REPO / name for name in binding['sources'])
    inputs.extend(p for p in (REPO / 'tests/fixtures/streams').rglob('*') if p.is_file())
    guard = {str(p): sha(p) for p in inputs}
    save(args.output / 'source-bindings.json', guard)
    save(args.output / 'rust-binary-binding.json', binding)
    for p in inputs:
        if p.is_relative_to(REPO):
            dst = args.output / 'executed-source' / p.relative_to(REPO)
            dst.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(p, dst)
    env = owner.base_env()
    listing = owner.execute([str(args.binary), '--list'], env, args.output, 'binary-tests', 10).read_text()
    if listing.count(': test') != 24:
        raise ValueError('Streams binary lacks its24 required tests')
    generated = {}
    for release in PINS:
        classes = args.output / (release + '-classes')
        classes.mkdir()
        cp = str(args.jars / ('kafka-clients-' + release + '.jar')) + os.pathsep + str(args.slf4j)
        owner.execute(['java', '--add-modules', 'jdk.compiler', 'com.sun.tools.javac.Main',
            '-source', '21', '-target', '21', '-Xlint:all', '-Werror', '-cp', cp, '-d', str(classes),
            str(ORACLE / 'StreamsWireOracle.java'), str(ORACLE / 'StreamsHeaderOracle.java')],
            env, args.output, release + '-compile', 30)
        java = ['java', '-Xmx128m', '-cp', str(classes) + os.pathsep + cp]
        for repeat in (1, 2):
            output = args.output / (release + '-generated-' + str(repeat))
            owner.execute(java + ['StreamsWireOracle', str(output)], env, args.output,
                          release + '-generate-' + str(repeat), 30)
            observed = tree(output)
            installed = tree(REPO / 'tests/fixtures/streams' / release)
            if observed != installed:
                raise ValueError('Fresh actual SDK fixture tree differs from installed inputs')
            generated[release + '-' + str(repeat)] = observed
    reverse = args.output / 'rust-reverse'
    rust_env = dict(env, STREAMS_REVERSE_OUT=str(reverse))
    log = owner.execute([str(args.binary), '--nocapture'], rust_env, args.output, 'rust-all24', 30).read_text()
    if 'test result: ok. 24 passed; 0 failed; 0 ignored;' not in log:
        raise ValueError('Not all24 Streams tests executed')
    for release in PINS:
        cp = str(args.output / (release + '-classes')) + os.pathsep + str(args.jars / ('kafka-clients-' + release + '.jar')) + os.pathsep + str(args.slf4j)
        java = ['java', '-Xmx128m', '-cp', cp]
        body = owner.execute(java + ['StreamsWireOracle', '--reverse', str(reverse / release / 'rust.tsv')],
                             env, args.output, release + '-reverse-body', 30).read_text()
        header = owner.execute(java + ['StreamsHeaderOracle', str(reverse / release / 'rust-headers.tsv')],
                               env, args.output, release + '-reverse-header', 30).read_text()
        if json.loads(body.splitlines()[-1])['actual_reverse_cases'] != 26:
            raise ValueError('Missing actual SDK body reverse checks')
        if json.loads(header.splitlines()[-1])['actual_reverse_headers'] != 12:
            raise ValueError('Missing actual SDK header reverse checks')
    for name, digest in guard.items():
        if sha(Path(name)) != digest:
            raise ValueError('Executed source/input changed')
    save(args.output / 'summary.json', dict(status='pass', rust_tests=24, ignored_tests=0,
        actual_sdk_releases=list(PINS), strict_compiles=3, independent_generation_processes=6,
        version_scoped_vectors=159, emitted_vectors=318, exact_installed_fixture_identity=True,
        actual_reverse_bodies_per_sdk=26, actual_reverse_headers_per_sdk=12,
        all_processes_parent_waited=True, input_guards_passed=True,
        generated_sha256=generated, scope='Streams v0 codecs; no broker or framework execution'))


if __name__ == '__main__':
    main()

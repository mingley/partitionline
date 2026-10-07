#!/usr/bin/env python3
"""Generate reproducible API67 fixtures with three pinned Apache SDKs."""
import argparse
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import shutil

REPO = Path(__file__).resolve().parents[3]
PINS = {'4.1.2': 'afe861c05067f4018a3148d73c1ed1e5fc90808757c15b043527d7e535a5d431',
        '4.2.1': '6a281026416938a53c105f2d91d2807fdc83d5658452abf0ad1b6d8ab8a553c8',
        '4.3.1': '52501b7b47510c66f898871adaf6d2968ab7246561d44ced43643a8a587f0b36'}
SLF4J = 'd3ef575e3e4979678dc01bf1dcce51021493b4d11fb7f1be8ad982877c16a1c0'
SOURCE = REPO / 'tests/conformance/java/ConformanceAllocateProducerIdsRaw.java'
spec = importlib.util.spec_from_file_location('process_owner', REPO / 'scripts/run-benchmark-matrix.py')
owner = importlib.util.module_from_spec(spec)
spec.loader.exec_module(owner)


def sha(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def tree(directory):
    return {str(p.relative_to(directory)): sha(p) for p in sorted(directory.rglob('*')) if p.is_file()}


def generate(jars, slf4j, output):
    if sha(slf4j) != SLF4J:
        raise ValueError('logging SDK differs')
    results = []
    for release, digest in PINS.items():
        jar = jars / ('kafka-clients-' + release + '.jar')
        if sha(jar) != digest:
            raise ValueError('SDK pin differs')
        classes = output / (release + '-classes')
        classes.mkdir()
        cp = str(jar) + os.pathsep + str(slf4j)
        env = owner.base_env()
        owner.execute(['java', '--add-modules', 'jdk.compiler', 'com.sun.tools.javac.Main', '-source', '21',
                       '-target', '21', '-Xlint:all', '-Werror', '-cp', cp, '-d', str(classes), str(SOURCE)],
                      env, output, release + '-compile', 30)
        java = ['java', '-Xmx128m', '-cp', str(classes) + os.pathsep + cp, 'ConformanceAllocateProducerIdsRaw']
        directories = [output / (release + '-fixtures-' + str(i)) for i in [1, 2]]
        for index, directory in enumerate(directories, 1):
            result = owner.execute(java + ['generate', str(directory)], env, output,
                                   release + '-generate-' + str(index), 8).read_text()
            if json.loads(result.splitlines()[-1])['generated_bodies'] != 26:
                raise ValueError('missing generated bodies')
        if tree(directories[0]) != tree(directories[1]):
            raise ValueError('SDK generations differ')
        results.append(dict(release=release, jar_sha256=digest, java=java, fixtures=str(directories[0]),
                            generated_bodies=26, independent_generations_match=True))
    return results


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ['jars', 'slf4j', 'output']:
        parser.add_argument('--' + name, type=Path, required=True)
    args = parser.parse_args()
    args.output.mkdir(parents=True, exist_ok=False)
    results = generate(args.jars, args.slf4j, args.output)
    (args.output / 'summary.json').write_text(json.dumps(results, indent=2) + '\n')


if __name__ == '__main__':
    main()

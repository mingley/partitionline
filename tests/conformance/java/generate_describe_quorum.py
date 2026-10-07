#!/usr/bin/env python3
"""Generate DescribeQuorum bodies with three pinned Apache Kafka SDKs."""
import argparse
import importlib.util
import json
import os
from pathlib import Path

REPO = Path(__file__).resolve().parents[3]
spec = importlib.util.spec_from_file_location('pinned_generator', Path(__file__).with_name('generate_allocate_producer_ids_raw.py'))
gen = importlib.util.module_from_spec(spec)
spec.loader.exec_module(gen)
SOURCE = Path(__file__).with_name('ConformanceDescribeQuorum.java')


def generate(jars, slf4j, output):
    if gen.sha(slf4j) != gen.SLF4J:
        raise ValueError('logging SDK differs')
    inputs = [SOURCE, Path(__file__).resolve(), Path(__file__).with_name('generate_allocate_producer_ids_raw.py'), REPO / 'scripts/run-benchmark-matrix.py', slf4j]
    results = []
    for release, digest in gen.PINS.items():
        jar = jars / ('kafka-clients-' + release + '.jar')
        if gen.sha(jar) != digest:
            raise ValueError('SDK pin differs')
        inputs.append(jar)
    guard = {str(path):gen.sha(path) for path in inputs}
    gen.owner.atomic(output / 'source-bindings.json', guard)
    for release,digest in gen.PINS.items():
        classes = output / (release + '-classes')
        classes.mkdir()
        cp = str(jars / ('kafka-clients-' + release + '.jar')) + os.pathsep + str(slf4j)
        gen.owner.execute(['java','--add-modules','jdk.compiler','com.sun.tools.javac.Main','-source','21','-target','21','-Xlint:all','-Werror','-cp',cp,'-d',str(classes),str(SOURCE)],gen.owner.base_env(),output,release + '-compile',30)
        java = ['java','-Xmx128m','-cp',str(classes) + os.pathsep + cp,'ConformanceDescribeQuorum']
        paths = [output / (release + '-fixtures-' + str(index)) for index in [1,2]]
        for index,path in enumerate(paths,1):
            gen.owner.execute(java + ['generate',str(path)],gen.owner.base_env(),output,release + '-generate-' + str(index),8)
        if gen.tree(paths[0]) != gen.tree(paths[1]):
            raise ValueError('SDK generations differ')
        results.append(dict(release=release,jar_sha256=digest,bodies=45,source_sha256=gen.sha(SOURCE),fixtures=str(paths[0]),files=gen.tree(paths[0]),independent_generations_match=True))
    if any(gen.sha(path) != digest for path,digest in guard.items()):
        raise ValueError('generator input changed')
    gen.owner.atomic(output / 'summary.json',dict(actual_sdk=True,results=results))


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ['jars','slf4j','output']:
        parser.add_argument('--' + name,type=Path,required=True)
    args = parser.parse_args()
    args.output.mkdir(parents=True,exist_ok=False)
    generate(args.jars,args.slf4j,args.output)

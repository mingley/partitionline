#!/usr/bin/env python3
"""Build pinned Apache Produce v13 fixtures and preserve every prerequisite status."""
import argparse
import hashlib
import json
from pathlib import Path
import subprocess

PIN='180c9228a9ee3ccce6c1dffefe4808c8d74e3b7b1f9e2639aea9a60adc37f2cb'
SOURCE=Path(__file__).with_name('ProduceV13Fixtures.java')


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--kafka-jar',type=Path,required=True)
    parser.add_argument('--slf4j-jar',type=Path,required=True)
    parser.add_argument('--java-bin',type=Path,required=True)
    parser.add_argument('--output',type=Path,required=True)
    parser.add_argument('--artifacts',type=Path,required=True)
    parser.add_argument('--verify',action='store_true')
    args=parser.parse_args()
    if hashlib.sha256(args.kafka_jar.read_bytes()).hexdigest()!=PIN:raise ValueError('wrong Apache 4.1.0 distribution jar')
    args.artifacts.mkdir(parents=True,exist_ok=False)
    args.output.mkdir(parents=True,exist_ok=True)
    if not args.verify and any(args.output.glob('produce_v13_*')):raise ValueError('refusing to overwrite existing v13 fixtures; use --verify')
    identity={'apache_client_version':'4.1.0','jar_sha256':PIN,
        'slf4j_jar_sha256':hashlib.sha256(args.slf4j_jar.read_bytes()).hexdigest(),
        'generator_sha256':hashlib.sha256(SOURCE.read_bytes()).hexdigest(),'exit_codes':{},'mode':'verify' if args.verify else 'generate'}
    def retain(): (args.artifacts/'identity.json').write_text(json.dumps(identity,indent=2)+'\n')
    def run(phase,command):
        result=subprocess.run(command,capture_output=True,text=True,timeout=30,check=False)
        (args.artifacts/(phase+'.stdout.log')).write_text(result.stdout)
        (args.artifacts/(phase+'.stderr.log')).write_text(result.stderr)
        identity['exit_codes'][phase]=result.returncode;retain()
        if result.returncode:raise ValueError(f'{phase} exited {result.returncode}; full logs in {args.artifacts}')
        return result.stdout
    cp=str(args.kafka_jar.resolve())+':'+str(args.slf4j_jar.resolve())
    run('java-version',[str(args.java_bin/'java'),'-version'])
    run('compile',[str(args.java_bin/'javac'),'--release','21','-cp',cp,'-d',str(args.artifacts),str(SOURCE)])
    command=[str(args.java_bin/'java'),'-cp',str(args.artifacts.resolve())+':'+cp,'ProduceV13Fixtures',str(args.kafka_jar.resolve()),str(args.output)]
    if args.verify:command.append('--verify')
    rows=[json.loads(line) for line in run('execute',command).splitlines()]
    sizes={'empty':(9,6),'paired':(217,108),'reversed':(217,108),'errors':(217,122),'tagged':(234,150),'zero-id':(217,108)}
    if len(rows)!=6 or {r['cell'] for r in rows}!=set(sizes):raise ValueError('missing or duplicated fixture cell')
    for row in rows:
        if row['version']!=13 or (row['request_bytes'],row['response_bytes'])!=sizes[row['cell']]:raise ValueError('fixture row lost fields: '+str(row))
        stem='produce_v13_'+row['cell'].replace('-','_')
        for kind in ['request','response']:
            data=(args.output/(stem+'_'+kind+'.bin')).read_bytes()
            if hashlib.sha256(data).hexdigest()!=row[kind+'_sha256']:raise ValueError('mismatched fixture digest')
    identity['cells']=rows;identity['status']='passed';retain()
    print('Apache 4.1.0 Produce v13: six cells, twelve bodies and every process status passed')


if __name__=='__main__':main()

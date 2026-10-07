#!/usr/bin/env python3
"""Build pinned Apache Produce v13 fixtures and preserve every prerequisite status."""
import argparse
import hashlib
import json
from pathlib import Path
import subprocess

PINS = {
    '4.1.0': '180c9228a9ee3ccce6c1dffefe4808c8d74e3b7b1f9e2639aea9a60adc37f2cb',
    '4.1.2': '33b4d9f24ba793ce0ed06607aa92b61d764015d8a0ef72d2558dbb81def4b3ed',
    '4.2.1': '9eb0bcd658da6623b62c01a551f584d0dbed7222d930ec977e51160f55385159',
    '4.3.1': 'dc3d65e3ac811a446184ea1dca0fe9cf957c2d8984dcb4668d01f4b77fc8f50e',
}
SOURCE_COMMITS = {
    '4.1.0': '13f70256db3c994c590e5d262a7cc50b9e973204',
    '4.1.2': 'c82fd9b934b4c1e6fa799e3f1dcc8f08d997740c',
    '4.2.1': '18d5ecd939c8d510fdd72d0abb1f7099659dcd58',
    '4.3.1': '26b251a451ce941d3d7a55e6487bcb7f16b5ad48',
}
SOURCE=Path(__file__).with_name('ProduceV13Fixtures.java')


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--version',choices=PINS,default='4.1.0')
    parser.add_argument('--kafka-jar',type=Path,required=True)
    parser.add_argument('--slf4j-jar',type=Path,required=True)
    parser.add_argument('--java-bin',type=Path,required=True)
    parser.add_argument('--output',type=Path,required=True)
    parser.add_argument('--artifacts',type=Path,required=True)
    parser.add_argument('--verify',action='store_true')
    args=parser.parse_args()
    pin=PINS[args.version]
    if hashlib.sha256(args.kafka_jar.read_bytes()).hexdigest()!=pin:raise ValueError('wrong pinned Apache distribution jar')
    args.artifacts.mkdir(parents=True,exist_ok=False)
    args.output.mkdir(parents=True,exist_ok=True)
    if not args.verify and any(args.output.glob('produce_v13_*')):raise ValueError('refusing to overwrite existing v13 fixtures; use --verify')
    identity={'apache_client_version':args.version,'source_commit_sha':SOURCE_COMMITS[args.version],'jar_sha256':pin,
        'slf4j_jar_sha256':hashlib.sha256(args.slf4j_jar.read_bytes()).hexdigest(),
        'generator_sha256':hashlib.sha256(SOURCE.read_bytes()).hexdigest(),'exit_codes':{},'commands':{},'mode':'verify' if args.verify else 'generate'}
    def retain(): (args.artifacts/'identity.json').write_text(json.dumps(identity,indent=2)+'\n')
    def run(phase,command):
        result=subprocess.run(command,capture_output=True,text=True,timeout=30,check=False)
        (args.artifacts/(phase+'.stdout.log')).write_text(result.stdout)
        (args.artifacts/(phase+'.stderr.log')).write_text(result.stderr)
        identity['exit_codes'][phase]=result.returncode;identity['commands'][phase]=command;retain()
        if result.returncode:raise ValueError(f'{phase} exited {result.returncode}; full logs in {args.artifacts}')
        return result.stdout
    cp=str(args.kafka_jar.resolve())+':'+str(args.slf4j_jar.resolve())
    run('java-version',[str(args.java_bin/'java'),'-version'])
    # A javac launcher is optional when the installed JDK compiler module exists.
    compile_command=([str(args.java_bin/'javac')] if (args.java_bin/'javac').exists() else
                     [str(args.java_bin/'java'),'--add-modules','jdk.compiler','com.sun.tools.javac.Main'])
    source_text=SOURCE.read_text()
    if args.version=='4.3.1':
        # Apache4.3 moved only these record implementation classes to internal.
        source_text=source_text.replace('org.apache.kafka.common.record.MemoryRecords',
                                        'org.apache.kafka.common.record.internal.MemoryRecords')
        source_text=source_text.replace('org.apache.kafka.common.record.SimpleRecord',
                                        'org.apache.kafka.common.record.internal.SimpleRecord')
    compiled_source=args.artifacts/'ProduceV13Fixtures.java'
    compiled_source.write_text(source_text)
    identity['compiled_source_sha256']=hashlib.sha256(compiled_source.read_bytes()).hexdigest();retain()
    run('compile',compile_command+['--source','21','--target','21','-cp',cp,'-d',str(args.artifacts),str(compiled_source)])
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
    print(f'Apache {args.version} Produce v13: six cells, twelve bodies and every process status passed')


if __name__=='__main__':main()

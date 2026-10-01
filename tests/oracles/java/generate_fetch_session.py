#!/usr/bin/env python3
"""Execute the pinned Apache session handler and retain every frame-size observation."""
import argparse
import hashlib
import json
from pathlib import Path
import subprocess

PIN='dc3d65e3ac811a446184ea1dca0fe9cf957c2d8984dcb4668d01f4b77fc8f50e'
SOURCE=Path(__file__).with_name('FetchSessionOracle.java')


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--kafka-jar',type=Path,required=True)
    parser.add_argument('--slf4j-jar',type=Path,required=True)
    parser.add_argument('--java-bin',type=Path,required=True)
    parser.add_argument('--output',type=Path,required=True)
    args=parser.parse_args()
    assert hashlib.sha256(args.kafka_jar.read_bytes()).hexdigest()==PIN,'wrong Apache 4.3.1 jar'
    args.output.mkdir(parents=True,exist_ok=False)
    classpath=str(args.kafka_jar.resolve())+':'+str(args.slf4j_jar.resolve())
    def run(phase,command):
        result=subprocess.run(command,capture_output=True,text=True,timeout=30,check=False)
        (args.output/(phase+'.stdout.log')).write_text(result.stdout)
        (args.output/(phase+'.stderr.log')).write_text(result.stderr)
        assert result.returncode==0,f'{phase} failed: {result.stderr}'
        return result.stdout
    version=run('java-version',[str(args.java_bin/'java'),'-version'])
    run('compile',[str(args.java_bin/'javac'),'--release','21','-cp',classpath,'-d',str(args.output),str(SOURCE)])
    text=run('execute',[str(args.java_bin/'java'),'-cp',str(args.output.resolve())+':'+classpath,'FetchSessionOracle'])
    rows=[json.loads(line) for line in text.splitlines()]
    assert len(rows)==80
    expected={
        'initial':(0,0,128,0,128),'unchanged':(91,1,0,0,128),
        'changed-offset':(91,2,1,0,128),'removed':(91,3,0,1,127),
        'replaced-id':(91,4,127,127,127),'invalid-epoch-full':(91,0,127,0,127),
        'not-found-initial':(0,0,127,0,127),'connection-error-full':(91,0,127,0,127)}
    sizes={7:(3112,33),8:(3112,33),11:(3626,35),12:(4258,29),13:(4272,29),17:(4268,25)}
    for version in sizes:
        selected=[row for row in rows if row['version']==version]
        recovery={
            'missing-full':(0,0,128,0,128),'throttled-full':(0,0,128,0,128),
            'extra-incremental':(91,0,128,0,128),'topic-id-error':(91,0,128,0,128),
            'terminal-close':(91,-1,128,0,128)}
        if version>=13: recovery['unknown-id']=(91,0,128,0,128)
        cases=expected | recovery
        assert len(selected)==len(cases) and {r['step'] for r in selected}==set(cases)
        for row in selected:
            assert tuple(row[k] for k in ['session_id','epoch','changed','forgotten','cached_partitions'])==cases[row['step']],row
            if row['step']=='initial': assert row['request_bytes']==sizes[version][0],row
            if row['step']=='unchanged': assert row['request_bytes']==sizes[version][1],row
    identity={'apache_client_version':'4.3.1','kafka_jar_sha256':PIN,
        'slf4j_jar_sha256':hashlib.sha256(args.slf4j_jar.read_bytes()).hexdigest(),
        'source_sha256':hashlib.sha256(SOURCE.read_bytes()).hexdigest(),
        'java_version_log':'java-version.stderr.log','rows':rows,'status':'passed',
        'scope':'Executed Apache FetchSessionHandler state transitions and official serialized Fetch request sizes; no throughput claim.'}
    (args.output/'report.json').write_text(json.dumps(identity,indent=2)+'\n')
    print('Apache 4.3.1 session reference: 80 transitions and all six request-size pairs passed')


if __name__=='__main__':main()

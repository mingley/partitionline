#!/usr/bin/env python3
"""Compare public Rust and genuine Apache Admin Streams descriptions on owned peers."""
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

REPO=Path(__file__).resolve().parents[2]
SOURCE=REPO/'tests/fixtures/streams-describe/StreamsDescribeOracle.java'
PINS={'4.1.2':'afe861c05067f4018a3148d73c1ed1e5fc90808757c15b043527d7e535a5d431',
      '4.2.1':'6a281026416938a53c105f2d91d2807fdc83d5658452abf0ad1b6d8ab8a553c8',
      '4.3.1':'52501b7b47510c66f898871adaf6d2968ab7246561d44ced43643a8a587f0b36'}
SLF4J='d3ef575e3e4979678dc01bf1dcce51021493b4d11fb7f1be8ad982877c16a1c0'
MODES=['full','empty','null-empty','error','reroute','disconnect','mixed','downgrade']
spec=importlib.util.spec_from_file_location('process_owner',REPO/'scripts/run-benchmark-matrix.py')
owner=importlib.util.module_from_spec(spec);spec.loader.exec_module(owner)
def sha(path):return hashlib.sha256(Path(path).read_bytes()).hexdigest()
def save(path,data):
    with path.open('x') as file:json.dump(data,file,indent=2);file.write('\n')
def tree(path):return {str(p.relative_to(path)):sha(p) for p in sorted(path.rglob('*')) if p.is_file()}
def run_case(args,release,mode,fixtures,classes,cp,driver):
    directory=args.output/(release+'-'+mode+'-'+driver);directory.mkdir()
    env=owner.base_env();env.update(STREAMS_DESCRIBE_DIRECTORY=str(directory),STREAMS_DESCRIBE_FIXTURES=str(fixtures),STREAMS_DESCRIBE_MODE=mode)
    if driver=='rust':env['STREAMS_DESCRIBE_INVOKE']='1'
    else:env.pop('STREAMS_DESCRIBE_INVOKE',None)
    command=[str(args.binary),'serve_streams_describe_probe','--ignored','--exact','--nocapture']
    receipt=dict(command=command,driver=driver,release=release,mode=mode,parent_waited=False);peer=None
    try:
        with (directory/'peer.log').open('x') as log:
            peer=subprocess.Popen(command,env=env,stdout=log,stderr=subprocess.STDOUT,start_new_session=True)
            receipt['pid']=peer.pid;deadline=time.monotonic()+8
            while not (directory/'ready').exists():
                if peer.poll() is not None or time.monotonic()>deadline:raise RuntimeError('Peer startup failed')
                time.sleep(.01)
            if driver=='java':
                owner.execute(['java','-Xmx128m','-cp',str(classes)+os.pathsep+cp,'StreamsDescribeOracle','live',
                    (directory/'ready').read_text(),str(directory),mode,str(REPO/'tests/fixtures/streams'/release)],env,directory,'java',8)
            else:peer.wait(timeout=8)
    finally:
        (directory/'stop').touch(exist_ok=True)
        if peer:
            try:peer.wait(timeout=8)
            except subprocess.TimeoutExpired:receipt['forced_stop']=True;owner.stop_group(peer)
            receipt.update(exit_code=peer.returncode,parent_waited=True)
        save(directory/'process.json',receipt)
    if receipt.get('forced_stop') or receipt['exit_code']!=0:raise ValueError('Peer failed owned shutdown')
    closure=(directory/'closure.txt').read_text()
    if 'runtime_tasks=0' not in closure or 'ports_closed_and_rebound=3' not in closure:raise ValueError('Missing closure')
    parsed=owner.execute(['java','-Xmx128m','-cp',str(classes)+os.pathsep+cp,'StreamsDescribeOracle','parse',
        str(fixtures),str(directory),mode],env,directory,'parse',8).read_text()
    counts=json.loads(parsed.splitlines()[-1]);counts.update(release=release,mode=mode,driver=driver)
    if counts['actual_describe_frames']<1 or counts['actual_GROUP_lookups']<1:raise ValueError('Missing public application histories')
    rows=list(csv.DictReader((directory/'frames.tsv').open(),delimiter='\t'))
    descriptions=[row for row in rows if row['api']=='89']
    if driver=='rust' and any(row['api']=='10' and row['slot']!='0' for row in rows):raise ValueError('Rust discovery did not use bootstrap')
    if any(row['slot']=='0' or row['version']!='0' for row in descriptions):raise ValueError('Wrong target/version')
    if mode=='mixed' and any(row['slot']=='2' for row in descriptions):raise ValueError('Unsupported coordinator received API89')
    if mode=='downgrade' and len([row for row in descriptions if row['slot']=='1'])!=1:raise ValueError('Downgrade not renegotiated')
    if mode in ('reroute','disconnect') and not {1,2}.issubset({int(row['slot']) for row in descriptions}):raise ValueError('Missing retry authority')
    return directory,counts

def compare(java,rust):
    java_rows={row.split('\t')[0]:row.split('\t')[1:] for row in (java/'java-outcome.tsv').read_text().splitlines()}
    rust_rows=[row.split('\t') for row in (rust/'rust-outcome.tsv').read_text().splitlines()]
    if [row[0] for row in rust_rows]!=['beta','alpha','beta']:raise ValueError('Order/duplicates lost')
    for row in rust_rows:
        expected=java_rows[row[0]]
        if row[1]!=expected[0] or row[1]=='0' and row[2:]!=expected[1:]:raise ValueError('Public outcomes differ')

def main():
    p=argparse.ArgumentParser(description=__doc__)
    for name in ('binary','binding','jars','slf4j','output'):p.add_argument('--'+name,type=Path,required=True)
    args=p.parse_args();args.binary=args.binary.resolve();args.output=args.output.resolve();args.output.mkdir(parents=True,exist_ok=False)
    if ctypes.CDLL(None,use_errno=True).prctl(36,1,0,0,0)!=0:raise OSError('Subreaper unavailable')
    binding=json.loads(args.binding.read_text())
    if sha(args.binary)!=binding['binary_sha256']:raise ValueError('Binary binding differs')
    for name,digest in binding['sources'].items():
        if sha(REPO/name)!=digest:raise ValueError('Source binding differs: '+name)
    inputs=[SOURCE,Path(__file__).resolve(),REPO/'scripts/run-benchmark-matrix.py',args.binary,args.binding,args.slf4j]
    inputs.extend(REPO/name for name in binding['sources'])
    for release,digest in PINS.items():
        jar=args.jars/('kafka-clients-'+release+'.jar')
        if sha(jar)!=digest:raise ValueError('SDK pin differs')
        inputs.append(jar);inputs.extend(p for p in (REPO/'tests/fixtures/streams'/release).glob('*') if p.is_file())
    if sha(args.slf4j)!=SLF4J:raise ValueError('Logging SDK pin differs')
    guard={str(p):sha(p) for p in inputs};save(args.output/'source-bindings.json',guard);save(args.output/'rust-binary-binding.json',binding)
    for path in inputs:
        if path.is_relative_to(REPO):
            dest=args.output/'executed-source'/path.relative_to(REPO);dest.parent.mkdir(parents=True,exist_ok=True);shutil.copy2(path,dest)
    env=owner.base_env();listing=owner.execute([str(args.binary),'--list'],env,args.output,'binary-tests',8).read_text()
    if 'serve_streams_describe_probe: test' not in listing:raise ValueError('Required external lane missing')
    results=[]
    for release in PINS:
        cp=str(args.jars/('kafka-clients-'+release+'.jar'))+os.pathsep+str(args.slf4j);classes=args.output/(release+'-classes');classes.mkdir()
        owner.execute(['java','--add-modules','jdk.compiler','com.sun.tools.javac.Main','-source','21','-target','21','-Xlint:all','-Werror','-cp',cp,'-d',str(classes),str(SOURCE)],env,args.output,release+'-compile',30)
        java=['java','-Xmx128m','-cp',str(classes)+os.pathsep+cp,'StreamsDescribeOracle'];fixtures=args.output/(release+'-fixtures-1');second=args.output/(release+'-fixtures-2')
        for number,out in [(1,fixtures),(2,second)]:owner.execute(java+['generate',str(REPO/'tests/fixtures/streams'/release),str(out)],env,args.output,release+'-generate-'+str(number),8)
        if tree(fixtures)!=tree(second):raise ValueError('SDK generations differ')
        for mode in MODES:
            j,jc=run_case(args,release,mode,fixtures,classes,cp,'java');r,rc=run_case(args,release,mode,fixtures,classes,cp,'rust');compare(j,r);results.extend([jc,rc])
    for name,digest in guard.items():
        if sha(name)!=digest:raise ValueError('Executed input changed')
    save(args.output/'summary.json',dict(status='pass',results=results,actual_sdk=True,source_bound=True,
        all_processes_parent_waited=True,public_Java_calls=24,public_Rust_calls=24,scope='Scripted GROUP peers; genuine SDK public Admin and full wire projection. No live Streams coordinator or execution engine.'))
if __name__=='__main__':main()

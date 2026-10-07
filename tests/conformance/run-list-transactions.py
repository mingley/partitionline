#!/usr/bin/env python3
"""Actual Apache components and public calls against a bounded owned API66 peer."""
import argparse
import csv
import ctypes
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import signal
import shutil
import struct
import subprocess
import time

REPO = Path(__file__).resolve().parents[2]
SOURCE = REPO / 'tests/fixtures/list-transactions-routing/ListTransactionsOracle.java'
PINS = {'4.1.2':'afe861c05067f4018a3148d73c1ed1e5fc90808757c15b043527d7e535a5d431',
        '4.2.1':'6a281026416938a53c105f2d91d2807fdc83d5658452abf0ad1b6d8ab8a553c8',
        '4.3.1':'52501b7b47510c66f898871adaf6d2968ab7246561d44ced43643a8a587f0b36'}
SLF4J = 'd3ef575e3e4979678dc01bf1dcce51021493b4d11fb7f1be8ad982877c16a1c0'
CASES = ['normal','v0','mixed','error-15','error-16','error-29','error-53',
         'loading','disconnect','stall','auth-disconnect','delayed-metadata']
spec=importlib.util.spec_from_file_location('process_owner',REPO/'scripts/run-benchmark-matrix.py')
owner=importlib.util.module_from_spec(spec);spec.loader.exec_module(owner)

def save(path, data):
    with path.open('x') as file: json.dump(data,file,indent=2);file.write('\n')

def sha(path):return hashlib.sha256(Path(path).read_bytes()).hexdigest()

def run_case(args, release, mode, fixtures, classes, cp, driver):
    directory=args.output/(release+'-'+mode+'-'+driver);directory.mkdir()
    env=owner.base_env();env.update(PARTITIONLINE_LIST_TRANSACTIONS_PEER_DIR=str(directory),
        PARTITIONLINE_LIST_TRANSACTIONS_FIXTURES=str(fixtures),PARTITIONLINE_LIST_TRANSACTIONS_CASE=mode,
        PARTITIONLINE_LIST_TRANSACTIONS_PROOF_DIR=str(directory/'wire'))
    if driver=='rust':env['PARTITIONLINE_LIST_TRANSACTIONS_INVOKE']='1'
    else:env.pop('PARTITIONLINE_LIST_TRANSACTIONS_INVOKE',None)
    command=[str(args.binary),'serve_list_transactions_probe','--ignored','--exact','--nocapture']
    receipt=dict(command=command,parent_waited=False,driver=driver,release=release,mode=mode)
    peer=None
    try:
        with (directory/'peer.log').open('x') as log:
            peer=subprocess.Popen(command,env=env,stdout=log,stderr=subprocess.STDOUT,start_new_session=True)
            receipt['pid']=peer.pid;deadline=time.monotonic()+10
            while not (directory/'ready').exists():
                if peer.poll() is not None or time.monotonic()>deadline:raise RuntimeError('peer startup failed')
                time.sleep(.01)
            if driver=='java':
                owner.execute(['java','-Xmx128m','-cp',str(classes)+os.pathsep+cp,
                    'org.apache.kafka.clients.admin.ListTransactionsOracle','live',
                    (directory/'ready').read_text(),str(directory),release],env,directory,'java',12)
            if driver=='rust':peer.wait(timeout=10)
    finally:
        (directory/'stop').touch(exist_ok=True)
        if peer:
            try:peer.wait(timeout=8)
            except subprocess.TimeoutExpired:
                receipt['forced_stop']=True;owner.stop_group(peer)
            receipt.update(exit_code=peer.returncode,parent_waited=True)
        save(directory/'process.json',receipt)
    if receipt.get('forced_stop') or receipt.get('exit_code')!=0:raise ValueError('peer did not close cleanly')
    closure=(directory/'closure.txt').read_text()
    if 'runtime_tasks=0' not in closure or 'ports_closed_and_rebound=3' not in closure:raise ValueError('closure proof missing')
    with (directory/'wire/probe/frames.tsv').open() as file:rows=list(csv.DictReader(file,delimiter='\t'))
    listings=[r for r in rows if r['api_key']=='66']
    if any(r['api_key']=='10' for r in rows):raise ValueError('fabricated coordinator lookup')
    for row in listings:
        frame=(directory/'wire/probe'/row['request_file']).read_bytes()
        header=int(row['request_body_offset']);payload=frame[header:]
        version=int(row['api_version']);expected=(fixtures/f'broker-1-list-v{version}.request.bin').read_bytes()
        client=struct.unpack_from('>h',expected,12)[0];expected_body=expected[15+max(client,0):]
        if payload!=expected_body:raise ValueError('public request differs from actual serializer')
        if int(row['node']) not in (1,2):raise ValueError('wrong target')
        if row['response_file']!='-':
            response=(directory/'wire/probe'/row['response_file']).read_bytes()
            if struct.unpack_from('>i',response,4)[0]!=int(row['correlation_id']) or response[8]!=0:raise ValueError('correlation/header differs')
    if mode not in ['delayed-metadata'] and not {1,2}.issubset({int(r['node']) for r in listings}):raise ValueError('one-broker undercount')
    if mode=='v0' and any(r['api_version']!='0' for r in listings):raise ValueError('v0 fallback differs')
    if mode=='mixed' and any(int(r['api_version'])!=int(r['node'])-1 for r in listings):raise ValueError('per-broker negotiation differs')
    if mode=='auth-disconnect':
        # Each application connection must authenticate; a lost node2 socket
        # requires a fresh SASL exchange, not just a Metadata reconnect.
        if len([r for r in rows if r['api_key']=='36' and r['node']=='2'])<2:raise ValueError('node2 did not reauthenticate')
    return directory,dict(release=release,mode=mode,driver=driver,list_requests=len(listings),closed=True)

def compare(java, rust, mode):
    actual=json.loads((java/'public-outcome.json').read_text());lines=[line.split('\t') for line in (rust/'rust-outcome.tsv').read_text().splitlines()]
    if mode=='delayed-metadata':
        if not any(r[0]=='discovery-error' for r in lines):raise ValueError('discovery budget reset')
        return
    java_rows=sorted((int(b),r['id'],r['pid'],r['state']) for b,v in actual['by_broker'].items() for r in v.get('listings',[]))
    rust_rows=sorted((int(r[1]),r[2],int(r[3]),r[4]) for r in lines if r[0]=='listing')
    if java_rows!=rust_rows:raise ValueError('actual per-broker listings differ')
    failure=mode.startswith('error-') or mode=='stall'
    if failure:
        if not isinstance(actual['all'],dict) or not any(r[0]=='complete-error' for r in lines):raise ValueError('silent partial success')
        if not any(r[0]=='broker-error' and r[1]=='2' for r in lines):raise ValueError('error origin lost')
        if mode.startswith('error-'):
            code=int(mode.split('-')[1])
            if actual['by_broker']['2'].get('error_code')!=code or not any(r[0]=='broker-error' and r[1:3]==['2',f'Some({code})'] for r in lines):raise ValueError('actual broker error differs')
    elif len(actual['all'])!=4 or ['complete-count','4'] not in lines:raise ValueError('complete union differs')
    for row in lines:
        if row[0].endswith('elapsed-ms') and int(row[1])>2400:raise ValueError('original caller deadline exceeded')

def main():
    parser=argparse.ArgumentParser(description=__doc__);parser.add_argument('--output',type=Path,required=True)
    parser.add_argument('--binary',type=Path,required=True);parser.add_argument('--jars',type=Path,required=True)
    parser.add_argument('--slf4j',type=Path,required=True);parser.add_argument('--releases',default=','.join(PINS));parser.add_argument('--cases',default=','.join(CASES))
    args=parser.parse_args();args.output=args.output.resolve();args.binary=args.binary.resolve();args.jars=args.jars.resolve();args.slf4j=args.slf4j.resolve()
    args.output.mkdir(exist_ok=False,parents=True)
    if ctypes.CDLL(None).prctl(36,1,0,0,0)!=0:raise RuntimeError('subreaper unavailable')
    releases=args.releases.split(',');cases=args.cases.split(',')
    if any(r not in PINS for r in releases) or any(c not in CASES for c in cases):raise ValueError('unregistered cell')
    inputs=[SOURCE,Path(__file__),REPO/'scripts/run-benchmark-matrix.py',REPO/'tests/list_transactions_routing.rs',REPO/'tests/fixtures/list-transactions-routing/socket_peer.rs',args.binary,args.slf4j]
    for release in releases:
        jar=args.jars/f'kafka-clients-{release}.jar'
        if sha(jar)!=PINS[release]:raise ValueError('actual Apache SDK pin differs')
        inputs.append(jar)
    if sha(args.slf4j)!=SLF4J:raise ValueError('logging SDK pin differs')
    bindings={str(p):sha(p) for p in inputs};save(args.output/'source-bindings.json',bindings);results=[]
    for path in inputs:
        if path.is_relative_to(REPO):
            retained=args.output/'executed-source'/path.relative_to(REPO);retained.parent.mkdir(parents=True,exist_ok=True);shutil.copyfile(path,retained)
    env=owner.base_env()
    for release in releases:
        classes=args.output/(release+'-classes');classes.mkdir();fixtures=args.output/(release+'-fixtures')
        cp=str(args.jars/f'kafka-clients-{release}.jar')+os.pathsep+str(args.slf4j)
        owner.execute(['java','--add-modules','jdk.compiler','com.sun.tools.javac.Main','-source','21','-target','21','-Xlint:all','-Werror','-cp',cp,'-d',str(classes),str(SOURCE)],env,args.output,release+'-compile',30)
        owner.execute(['java','-Xmx128m','-cp',str(classes)+os.pathsep+cp,'org.apache.kafka.clients.admin.ListTransactionsOracle','generate',str(fixtures),release],env,args.output,release+'-components',20)
        manifest=json.loads((fixtures/'goldens.json').read_text());assert manifest['actual_vectors']==17
        for mode in cases:
            java,j=run_case(args,release,mode,fixtures,classes,cp,'java')
            rust,r=run_case(args,release,mode,fixtures,classes,cp,'rust')
            compare(java,rust,mode);results.extend([j,r]);print(json.dumps(dict(release=release,mode=mode,status='pass')),flush=True)
    for path,h in bindings.items():
        if sha(Path(path))!=h:raise ValueError('executed input changed')
    save(args.output/'summary.json',dict(status='pass',profiles=len(results),results=results,source_bound=True,actual_sdk=True,actual_public_Admin=True,all_processes_waited=True,scope='Client routing/aggregation against scripted multi-broker wire peer; no Kafka transaction state or production qualification'))

if __name__=='__main__':main()

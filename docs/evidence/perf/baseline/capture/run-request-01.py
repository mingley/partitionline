#!/usr/bin/env python3
"""Record five native request-encode repetitions and genuine SDK readbacks."""
import argparse
import importlib.util
import json
import math
import os
from pathlib import Path
import signal
import statistics
import struct
import sys


def crc32c(data):
    crc=0xffffffff
    for b in data:
        crc ^= b
        for _ in range(8): crc=(crc>>1) ^ (0x82f63b78 if crc&1 else 0)
    return crc^0xffffffff


def batch_start(raw):
    pos=0
    def uv():
        nonlocal pos
        result=0
        for shift in range(0,35,7):
            value=raw[pos]; pos+=1; result|=(value&127)<<shift
            if value<128: return result
        raise ValueError('invalid compact length')
    if uv()!=0: raise ValueError('transactional id differs')
    pos+=6
    if uv()!=2: raise ValueError('one topic required')
    length=uv()-1; pos+=length
    if uv()!=2: raise ValueError('one partition required')
    pos+=4
    length=uv()-1
    start=pos
    if struct.unpack_from('>i',raw,start+8)[0]+12!=length or raw[start+16]!=2:
        raise ValueError('record batch length or magic differs')
    return start,length


def main():
    p=argparse.ArgumentParser(description=__doc__)
    for name in ('source','source-pins','output','recorder','parent-exec','binary','verifier-source','java-peer','sdk-jars','slf4j'):
        p.add_argument('--'+name,type=Path,required=True)
    p.add_argument('--commit',required=True)
    a=p.parse_args()
    def interrupt(signum,frame): raise InterruptedError(f'owner signal {signum}')
    for sig in (signal.SIGTERM,signal.SIGINT): signal.signal(sig,interrupt)
    spec=importlib.util.spec_from_file_location('request_baseline_recorder',a.recorder)
    baseline=importlib.util.module_from_spec(spec); spec.loader.exec_module(baseline)
    args=argparse.Namespace(source=a.source,source_pins=a.source_pins,output=a.output,
        commit=a.commit,family='request',repetitions=5,seed=912,cpus='2',cells=None,
        binary=[('baseline-request',a.binary)])
    r=baseline.Recorder(args)
    jars={v:a.sdk_jars/f'kafka-clients-{v}.jar' for v in ('4.1.2','4.2.1','4.3.1')}
    paths=[Path(__file__).resolve(),a.parent_exec.resolve(),a.java_peer.resolve(),a.slf4j.resolve(),
           *jars.values(),*a.verifier_source.rglob('*.rs'),a.verifier_source/'Cargo.toml',a.verifier_source/'Cargo.lock']
    r.input_pins.update({str(p):baseline.sha(p) for p in paths})
    base=r.command
    r.command=lambda command,directory,label,timeout=400,extra_env=None: base(
        [sys.executable,'-B',a.parent_exec.resolve(),str(os.getpid()),*command],directory,label,timeout,extra_env)
    baseline.save(r.output/'request-inputs-before-compile.json',r.input_pins)
    classes={}
    for version,jar in jars.items():
        destination=r.output/('classes-'+version); destination.mkdir()
        cp=str(jar.resolve())+':'+str(a.slf4j.resolve())
        actual_source=a.java_peer.resolve()
        if version=='4.3.1':
            source_dir=destination/'source'; source_dir.mkdir()
            actual_source=source_dir/'RequestBodyPeer.java'
            actual_source.write_text(a.java_peer.read_text().replace(
                'org.apache.kafka.common.record.MemoryRecords',
                'org.apache.kafka.common.record.internal.MemoryRecords'))
            r.input_pins[str(actual_source)]=baseline.sha(actual_source)
        r.command(['java','--add-modules','jdk.compiler','com.sun.tools.javac.Main',
            '-source','21','-target','21','-Xlint:all','-Werror','-cp',cp,'-d',destination,
            actual_source],r.output,'compile-'+version,30)
        classes[version]=str(destination)+':'+cp
        r.input_pins.update({str(p):baseline.sha(p) for p in destination.rglob('*.class')})
    baseline.save(r.output/'request-inputs.json',r.input_pins)
    for rep in range(1,6):
        directory=r.output/f'r{rep:02d}'; directory.mkdir()
        wire=directory/'produce-v9.body'
        print(f'start micro-request repetition {rep}',flush=True)
        raw=r.command([r.binaries['baseline-request'],wire],directory,'native',30)
        lines=[json.loads(s) for s in raw.read_text().splitlines() if s.startswith('{')]
        if len(lines)!=1: raise ValueError('one native completion required')
        data=lines[0]; samples=data['native_samples_ns_per_operation']
        if len(samples)!=30 or any(not math.isfinite(v) or v<=0 for v in samples):
            raise ValueError('30 positive finite native clock samples required')
        if data['warmup_iterations']!=10000 or data['iterations_per_sample']!=10000:
            raise ValueError('measurement population differs')
        peers=[]
        for version,cp in classes.items():
            checked=r.command(['java','-Xmx128m','-cp',cp,'RequestBodyPeer',wire],directory,'sdk-'+version,15)
            peer=json.loads(checked.read_text())
            if peer['status']!='pass' or not peer['full_seeded_records_verified']:
                raise ValueError('SDK record verification incomplete')
            peers.append(dict(version=version,actual=peer))
        row=dict(cell='micro-request',repetition=rep,median_ns_per_operation=statistics.median(samples),
            raw=data,wire_sha256=baseline.sha(wire),sdk_peers=peers)
        baseline.save(directory/'validated.json',row); r.rows.append(row)
        print(f'done micro-request repetition {rep}',flush=True)
    original=(r.output/'r01/produce-v9.body').read_bytes()
    start,length=batch_start(original)
    if struct.unpack_from('>I',original,start+17)[0]!=crc32c(original[start+21:start+length]):
        raise ValueError('independent CRC32C disagrees with actual original batch')
    crc_bad=bytearray(original); crc_bad[start+length-2]^=1
    payload_bad=bytearray(crc_bad)
    struct.pack_into('>I',payload_bad,start+17,crc32c(payload_bad[start+21:start+length]))
    controls=[]
    for name,raw in (('crc-corruption',crc_bad),('payload-corruption-with-valid-crc',payload_bad)):
        file=r.output/(name+'.body'); file.write_bytes(raw)
        assert raw!=original
        for version,cp in classes.items():
            r.guard()
            command=['taskset','-c','2',sys.executable,'-B',str(a.parent_exec.resolve()),str(os.getpid()),
                'java','-Xmx128m','-cp',cp,'RequestBodyPeer',str(file)]
            # Owned execute retains nonzero process receipts and closes the group before raising.
            try:
                r.owned.execute(command,r.env,r.output,name+'-'+version,15)
            except ValueError:
                receipt=json.loads((r.output/(name+'-'+version+'.process.json')).read_text())
                if receipt['exit_code']==0 or receipt.get('timed_out'): raise ValueError('control did not reject normally')
                stderr=(r.output/(name+'-'+version+'.stderr')).read_text()
                expected='complete seeded record differs' if name=='payload-corruption-with-valid-crc' else 'CorruptRecordException'
                if expected not in stderr: raise ValueError('control rejected for an unexpected reason')
                controls.append(dict(control=name,sdk=version,receipt=receipt,rejection=expected))
            else: raise ValueError('corrupt body accepted')
            r.guard()
    baseline.save(r.output/'negative-controls.json',controls)
    r.finish()


if __name__=='__main__': main()

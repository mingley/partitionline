#!/usr/bin/env python3
"""Actual stdlib/file/pipe preparation controls; no broker or SDK execution."""
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import resource
import sys
import tempfile
import time


def require(condition, label):
    if not condition:
        raise RuntimeError(label)


def main():
    sys.dont_write_bytecode = True
    source = Path(__file__).resolve().parent.parent/'run-live.py'
    spec = importlib.util.spec_from_file_location('prepared_driver',source)
    driver = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(driver)
    driver.child_setup()
    results = []
    with tempfile.TemporaryDirectory(prefix='oidc-preparation-control-',dir='/workspace/work') as scratch:
        output = Path(scratch)/'public'
        output.mkdir(mode=0o700)
        run = driver.Run({},output,Path(scratch)/'private')
        baseline = [{'path':'published-synthetic-source','bytes':17,'full_mode':'0o644'}]
        run.source_before = baseline.copy()
        run.steps = [{'ordinal':0,'operation':'public-prior-step','passed':True}]
        run.write_registry()
        run.write_registry()
        require(run.source_before==baseline and len(run.steps)==1,'registry preserves qualification history')
        registry = json.loads((output/'owned-process-groups.json').read_text())
        require(registry['driver']['pid']==os.getpid()
                and registry['driver']['starttime_ticks']==driver.process_identity(os.getpid())['starttime_ticks'],
                'actual PID/starttime registry binding')
        results.append({'name':'real-registry-preserves-history-and-proc-identity','passed':True})
        run.receipt('actual-peer',{'event':'metadata','ordinal':900})
        run.receipt('actual-peer',{'event':'metadata','ordinal':0})
        require(run.records[0]['driver_sequence']==0 and run.records[1]['driver_sequence']==1
                and run.records[0]['ordinal']==900,'separate driver and peer counters')
        rejected = False
        try:
            run.receipt('actual-peer',{'event':'metadata','actor':'forged'})
        except RuntimeError:
            rejected = True
        require(rejected and len(run.records)==2,'reserved identity injection rejected')
        results.append({'name':'counter-and-reserved-identity-injection','passed':True})
        helper = (
            "import json,resource,sys\n"
            "print(json.dumps({'event':'ready','core_limit':resource.getrlimit(resource.RLIMIT_CORE)[0]}),flush=True)\n"
            "assert sys.stdin.readline()=='close\\n'\n"
            "print(json.dumps({'event':'shutdown'}),flush=True)\n"
        )
        process = driver.OwnedProcess('stdio-control',[sys.executable,'-c',helper],
                    run.receipt,time.monotonic()+10,run.register_process)
        ready = process.await_event('ready')
        require(ready['core_limit']==0,'inherited actual core dump limit')
        process.send('close')
        process.await_event('shutdown')
        process.join(seconds=3)
        process.abort()
        require(not process.forced and not process.group_exists()
                and not any(reader.is_alive() for reader in process.readers),'real graceful group and readers joined')
        results.append({'name':'actual-stdio-process-core0-graceful-group-readers','passed':True})
        injected = "print('{\"event\":\"ready\",\"access_token\":\"public-injection-control\"}',flush=True)"
        process = driver.OwnedProcess('injection-control',[sys.executable,'-c',injected],
                    run.receipt,time.monotonic()+10,run.register_process)
        process.process.wait(timeout=3)
        for reader in process.readers:
            reader.join(timeout=3)
        require(process.failure=='rejected stdout receipt'
                and not any(row['actor']=='injection-control' for row in run.records),
                'secret-key stdout injection is rejected without retention')
        process.abort()
        results.append({'name':'actual-secret-key-pipe-injection-rejected','passed':True})
    receipt = {'source_sha256':hashlib.sha256(source.read_bytes()).hexdigest(),
               'scope':'4 actual stdlib/file/proc/pipe controls only; zero HTTPS, broker, KafkaSDK or native/Rust compiler qualification',
               'cases':results,'passed':all(row['passed'] for row in results)}
    path = Path(__file__).with_name('preparation-controls.json')
    path.write_text(json.dumps(receipt,indent=2)+'\n')
    print(json.dumps({'controls':len(results),'passed':receipt['passed'],
                      'receipt_sha256':hashlib.sha256(path.read_bytes()).hexdigest()}))


if __name__=='__main__':
    main()

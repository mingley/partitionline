#!/usr/bin/env python3
"""Capture owned, source-bound lookup repetitions or interleaved A/B pairs."""
import argparse
import importlib.util
import json
import os
from pathlib import Path
import signal
import subprocess
import sys


def module(path,name):
    spec=importlib.util.spec_from_file_location(name,path)
    m=importlib.util.module_from_spec(spec);spec.loader.exec_module(m);return m


def main():
    p=argparse.ArgumentParser(description=__doc__)
    for name in ('source','source-pins','output','recorder','parent-exec','measure','validator','parent'):
        p.add_argument('--'+name,type=Path,required=True)
    for name in ('candidate','candidate-source'):p.add_argument('--'+name,type=Path)
    p.add_argument('--commit',required=True);p.add_argument('--candidate-commit')
    p.add_argument('--repetitions',type=int,default=5)
    a=p.parse_args()
    if not 5<=a.repetitions<=25:p.error('five to25 repetitions required')
    if bool(a.candidate)!=bool(a.candidate_source) or bool(a.candidate)!=bool(a.candidate_commit):p.error('candidate ELF, source and commit required together')
    def interrupt(sig,frame):raise InterruptedError(f'owner signal{sig}')
    for sig in (signal.SIGINT,signal.SIGTERM):signal.signal(sig,interrupt)
    baseline=module(a.recorder,'range_owned_recorder')
    binaries=[('parent',a.parent)]
    if a.candidate:binaries.append(('candidate',a.candidate))
    r=baseline.Recorder(argparse.Namespace(source=a.source,source_pins=a.source_pins,output=a.output,commit=a.commit,
        family='share-ranges',repetitions=a.repetitions,seed=951,cpus='2',binary=binaries))
    extra_pins=[Path(__file__).resolve(),a.parent_exec.resolve(),a.measure.resolve(),a.validator.resolve()]
    if a.candidate_source:
        for relative in r.pins:
            extra_pins.append(a.candidate_source/relative)
    for path in extra_pins:r.input_pins[str(path)]=baseline.sha(path)
    original_guard=r.guard
    def guard():
        original_guard()
        if a.candidate_source:
            if subprocess.check_output(['git','-C',str(a.candidate_source),'rev-parse','HEAD'],text=True).strip()!=a.candidate_commit:
                raise ValueError('candidate commit differs')
            if subprocess.check_output(['git','-C',str(a.candidate_source),'status','--porcelain']):raise ValueError('candidate source dirty')
    r.guard=guard
    baseline.save(r.output/'capture-inputs.json',r.input_pins)
    baseline.save(r.output/'capture-policy.json',dict(iterations=10000,excluded_warmup_iterations=100,
        ordering='Alternating A-B then B-A in successive pairs; both arms repeat the same deterministic fixture',
        primary='ns per offset lookup',acceptance='At least3 percent improvement and paired bootstrap95 percent CI excluding zero; no allocation increase; no guardrail regression above2 percent',
        candidate_commit=a.candidate_commit,primary_cell='micro-share-ranges',scope='Local unsigned production-helper microbenchmark; no full ShareFetch throughput or leadership claim'))
    def execute(argv,directory,label):
        return r.command([sys.executable,'-B',a.parent_exec.resolve(),str(os.getpid()),*argv],directory,label,60)
    try:
        guard()
        for rep in range(a.repetitions):
            order=['parent','candidate'] if a.candidate else ['parent']
            if rep%2:order.reverse()
            for arm in order:
                directory=r.output/(f'r{rep+1:02d}-'+arm);directory.mkdir()
                result=directory/'result.json';resource=directory/'resources.json'
                execute([sys.executable,'-B',a.measure.resolve(),resource,r.binaries[arm],
                    '--iterations','10000','--output',result],directory,'lookup')
                execute([sys.executable,'-B',a.validator.resolve(),result],directory,'validator')
                usage=json.loads(resource.read_text())
                if not usage['parent_waited'] or usage['exit_code']!=0:raise ValueError('measured leaf not reaped successfully')
                row=dict(arm=arm,repetition=rep+1,artifact=str(result),sha256=baseline.sha(result),
                    measurements=json.loads(result.read_text()),whole_lifetime_resources=usage)
                r.rows.append(row)
                print(json.dumps(dict(arm=arm,repetition=rep+1,ns_per_record=row['measurements']['ns_per_record'])),flush=True)
        r.finish()
    except BaseException as error:
        baseline.save(r.output/'failure.json',dict(error=type(error).__name__,message=str(error)));raise


if __name__=='__main__':main()

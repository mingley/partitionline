#!/usr/bin/env python3
"""Capture an owned baseline runtime under local CPU or syscall observation."""
import argparse
import importlib.util
import json
import os
from pathlib import Path
import shlex
import signal
import sys


def main():
    p=argparse.ArgumentParser(description=__doc__)
    for name in ('source','source-pins','output','recorder','parent-exec','runtime','nb-serve','perf','perf-libraries','strace'):
        p.add_argument('--'+name,type=Path,required=True)
    p.add_argument('--commit',required=True);p.add_argument('--cell',required=True)
    p.add_argument('--mode',choices=('cpu','syscalls'),required=True)
    p.add_argument('--repetitions',type=int,default=25)
    a=p.parse_args()
    if not 1<=a.repetitions<=100:p.error('repetitions must be1..100')
    def interrupt(signum,frame):raise InterruptedError(f'owner signal{signum}')
    for sig in (signal.SIGTERM,signal.SIGINT):signal.signal(sig,interrupt)
    spec=importlib.util.spec_from_file_location('profile_owned_recorder',a.recorder)
    baseline=importlib.util.module_from_spec(spec);spec.loader.exec_module(baseline)
    if a.cell not in baseline.NB_CELLS:p.error('unknown baseline runtime cell')
    args=argparse.Namespace(source=a.source,source_pins=a.source_pins,output=a.output,commit=a.commit,
        family='profile',repetitions=a.repetitions,seed=912,cpus='2,4',cells=[a.cell],
        binary=[('runtime',a.runtime),('nb-serve',a.nb_serve)])
    r=baseline.Recorder(args)
    for path in [Path(__file__).resolve(),a.parent_exec.resolve(),a.perf.resolve(),a.strace.resolve(),
                 *[p.resolve() for p in a.perf_libraries.rglob('*') if p.is_file()]]:
        r.input_pins[str(path)]=baseline.sha(path)
    def launcher(path,binary):
        path.write_text('#!/bin/sh\nexec '+shlex.join([sys.executable,'-B',str(a.parent_exec.resolve())])+
                        ' "$PPID" '+shlex.quote(str(binary))+' "$@"\n')
        path.chmod(0o700);r.input_pins[str(path)]=baseline.sha(path);return path
    broker=launcher(r.output/'broker-parent-bound',r.binaries['nb-serve'])
    workload=launcher(r.output/'runtime-parent-bound',r.binaries['runtime'])
    r.env['NB_SERVE']=str(broker)
    base=r.command
    r.command=lambda command,directory,label,timeout=400,extra_env=None:base(
        [sys.executable,'-B',a.parent_exec.resolve(),str(os.getpid()),*command],directory,label,timeout,extra_env)
    baseline.save(r.output/'profile-inputs.json',r.input_pins)
    directory=r.output/'workload';directory.mkdir()
    command=[workload,'--cell',a.cell,'--out',directory,'--repetitions',str(a.repetitions)]
    extra=dict(LD_LIBRARY_PATH=str(a.perf_libraries.resolve()))
    try:
        if a.mode=='cpu':
            profile=r.output/'cpu.data'
            command=[a.perf.resolve(),'record','-e','cpu-clock:u','-F','997','--clockid','realtime',
                     '--call-graph','dwarf,16384','-o',profile,'--',*command]
        else:
            profile=r.output/'syscalls.txt'
            command=[a.strace.resolve(),'-f','-c','-S','calls','-o',profile,*command]
        baseline.save(r.output/'profile-plan.json',dict(cell=a.cell,mode=a.mode,repetitions=a.repetitions,
            source_commit=a.commit,scope='Local diagnostic profile; tool overhead excludes any throughput comparison',
            cpu_event='cpu-clock:u' if a.mode=='cpu' else None,cpu_frequency=997 if a.mode=='cpu' else None,
            timestamp_clock='realtime' if a.mode=='cpu' else None,
            frame_attribution='Must be verified from actual profile; no attribution inferred from absent symbols',
            profile_includes_setup=True,phase_boundaries='Filter CPU samples by each generated actual result timestamp',
            syscall_scope='Whole runtime lifetime including owned broker descendants; syscall totals are not isolated client phase counts',
            broker_parent_death_bound=True,workload_parent_death_bound=True,tool_parent_death_bound=True,
            command=list(map(str,command)),dependency_library_path=extra))
        r.command(command,r.output,'capture',max(400,a.repetitions*15),extra)
        results=sorted(directory.glob('*.result.json'))
        if len(results)!=a.repetitions:raise ValueError('runtime result count differs')
        for index,path in enumerate(results):
            r.command([sys.executable,'-B',a.source/'scripts/benchmark-report.py',path],directory,'validator-'+str(index),15)
            data=json.loads(path.read_text())
            if data['scenario']['cell_disposition']!='executed' or data['provenance']['source']['git_commit']!=a.commit:
                raise ValueError('runtime source/outcome differs')
            r.rows.append(dict(artifact=str(path),sha256=baseline.sha(path),
                phase=data['provenance']['timestamps'],metrics=data['measurements'],execution=data['execution']))
        if a.mode=='cpu':
            r.command([a.perf.resolve(),'report','--stdio','--header','--children','-i',profile],r.output,'report',60,extra)
            r.command([a.perf.resolve(),'script','--ns','-i',profile],r.output,'stacks',60,extra)
        r.finish()
    except BaseException as error:
        baseline.save(r.output/'failure.json',dict(error=type(error).__name__,message=str(error)))
        raise


if __name__=='__main__':main()

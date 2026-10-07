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
    for name in ('source','source-pins','output','recorder','parent-exec','runtime','nb-serve','perf','perf-libraries','strace','tree-supervisor'):
        p.add_argument('--'+name,type=Path,required=True)
    p.add_argument('--commit',required=True);p.add_argument('--cell',required=True)
    p.add_argument('--mode',choices=('cpu','syscalls'),required=True)
    p.add_argument('--repetitions',type=int,default=25)
    p.add_argument('--reuse-capture',type=Path)
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
    for path in [Path(__file__).resolve(),a.parent_exec.resolve(),a.perf.resolve(),a.strace.resolve(),a.tree_supervisor.resolve(),
                 *[p.resolve() for p in a.perf_libraries.rglob('*') if p.is_file()]]:
        r.input_pins[str(path)]=baseline.sha(path)
    def launcher(path,binary,pid_file=None):
        marker='' if pid_file is None else 'printf \'%s\\n\' "$$" > '+shlex.quote(str(pid_file))+'\n'
        path.write_text('#!/bin/sh\n'+marker+'exec '+shlex.join([sys.executable,'-B',str(a.parent_exec.resolve())])+
                        ' "$PPID" '+shlex.quote(str(binary))+' "$@"\n')
        path.chmod(0o700);r.input_pins[str(path)]=baseline.sha(path);return path
    broker=launcher(r.output/'broker-parent-bound',r.binaries['nb-serve'])
    workload=launcher(r.output/'runtime-parent-bound',r.binaries['runtime'],r.output/'runtime.pid')
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
            command=[a.strace.resolve(),'-f','-ttt','-T','-s','0','-e','raw=all','-o',profile,*command]
        baseline.save(r.output/'profile-plan.json',dict(cell=a.cell,mode=a.mode,repetitions=a.repetitions,
            source_commit=a.commit,scope='Local diagnostic profile; tool overhead excludes any throughput comparison',
            cpu_event='cpu-clock:u' if a.mode=='cpu' else None,cpu_frequency=997 if a.mode=='cpu' else None,
            timestamp_clock='realtime' if a.mode=='cpu' else None,
            frame_attribution='Must be verified from actual profile; no attribution inferred from absent symbols',
            profile_includes_setup=True,phase_boundaries='Filter CPU samples by each generated actual result timestamp',
            syscall_scope='Raw trace includes all descendants and setup. Isolate the recorded runtime main PID and actual measured-phase UTC intervals before counting.',
            syscall_argument_policy='Raw hexadecimal arguments only; no decoded strings or packet payloads',
            broker_parent_death_bound=True,workload_parent_death_bound=True,tool_parent_death_bound=True,
            workload_pid_marker=str(r.output/'runtime.pid'),
            command=list(map(str,command)),dependency_library_path=extra))
        if a.reuse_capture is None:
            r.command(command,r.output,'capture',max(400,a.repetitions*15),extra)
        else:
            import shutil
            donor=a.reuse_capture.resolve()
            receipt=json.loads((donor/'capture.process.json').read_text())
            old_plan=json.loads((donor/'plan.json').read_text())
            if not receipt['parent_waited'] or receipt['exit_code']!=0 or receipt.get('failure'):
                raise ValueError('original capture did not close cleanly')
            if old_plan['source_pins']!=r.pins or old_plan['source_commit']!=a.commit:
                raise ValueError('reused source differs')
            for name in ('runtime','nb-serve'):
                if baseline.sha(donor/'bin'/name)!=baseline.sha(r.binaries[name]):raise ValueError('reused ELF differs')
            shutil.copy2(donor/'cpu.data',profile)
            for file in (donor/'workload').iterdir():
                if file.is_file():shutil.copy2(file,directory/file.name)
            baseline.save(r.output/'reused-capture.json',dict(original=str(donor),sha256=baseline.sha(profile),
                original_command_receipt=receipt,fresh_capture=False,
                scope='Replay of cleanly completed capture after failed resolver cleanup; not a new profile repetition'))
        def observe(argv,label,timeout):
            return r.command([sys.executable,'-B',a.tree_supervisor.resolve(),'--receipt',r.output/(label+'-tree.json'),
                '--parent-exec',a.parent_exec.resolve(),'--timeout',str(timeout),'--',*argv],r.output,label,timeout+10,extra)
        results=sorted(directory.glob('*.result.json'))
        if len(results)!=a.repetitions:raise ValueError('runtime result count differs')
        for index,path in enumerate(results):
            r.command([sys.executable,'-B',a.source/'scripts/benchmark-report.py',path],directory,('replay-validator-' if a.reuse_capture is not None else 'validator-')+str(index),15)
            data=json.loads(path.read_text())
            if data['scenario']['cell_disposition']!='executed' or data['provenance']['source']['git_commit']!=a.commit:
                raise ValueError('runtime source/outcome differs')
            r.rows.append(dict(artifact=str(path),sha256=baseline.sha(path),
                phase=data['provenance']['timestamps'],metrics=data['measurements'],execution=data['execution']))
        if a.mode=='cpu':
            observe([a.perf.resolve(),'report','--stdio','--header','--children','-i',profile],'report',60)
            observe([a.perf.resolve(),'script','--ns','--comms','runtime','-i',profile],'stacks',60)
        r.finish()
    except BaseException as error:
        baseline.save(r.output/'failure.json',dict(error=type(error).__name__,message=str(error)))
        raise


if __name__=='__main__':main()

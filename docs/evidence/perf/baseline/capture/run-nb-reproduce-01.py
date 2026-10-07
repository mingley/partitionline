#!/usr/bin/env python3
import argparse
import importlib.util
import os
from pathlib import Path
import shlex
import signal
import sys


def main():
    p = argparse.ArgumentParser()
    for name in ('source','source-pins','output','recorder','parent-exec','runtime','nb-serve'):
        p.add_argument('--'+name,type=Path,required=True)
    p.add_argument('--commit',required=True)
    a=p.parse_args()
    def interrupt(signum,frame): raise InterruptedError(f'owner signal {signum}')
    for sig in (signal.SIGTERM,signal.SIGINT): signal.signal(sig,interrupt)
    spec=importlib.util.spec_from_file_location('nb_baseline_recorder',a.recorder)
    baseline=importlib.util.module_from_spec(spec); spec.loader.exec_module(baseline)
    args=argparse.Namespace(source=a.source,source_pins=a.source_pins,output=a.output,
        commit=a.commit,family='nb',repetitions=5,seed=912,cpus='2,4',cells=['nb-produce-bulk'],
        binary=[('runtime',a.runtime),('nb-serve',a.nb_serve)])
    r=baseline.Recorder(args)
    launcher=r.output/'nb-serve-parent-bound'
    launcher.write_text('#!/bin/sh\nexec '+shlex.join([sys.executable,'-B',str(a.parent_exec.resolve())])+ ' "$PPID" '+shlex.quote(str(r.binaries['nb-serve']))+' "$@"\n')
    launcher.chmod(0o700)
    r.env['NB_SERVE']=str(launcher)
    for path in (Path(__file__).resolve(),a.parent_exec.resolve(),launcher):
        r.input_pins[str(path)]=baseline.sha(path)
    baseline.save(r.output/'reproduce-inputs.json',r.input_pins)
    base=r.command
    r.command=lambda command,directory,label,timeout=400,extra_env=None: base(
        [sys.executable,'-B',a.parent_exec.resolve(),str(os.getpid()),*command],
        directory,label,timeout,extra_env)
    try:
        r.nb()
        r.finish()
    except BaseException as e:
        baseline.save(r.output/'failure.json',dict(error=type(e).__name__,message=str(e)))
        raise


if __name__=='__main__': main()

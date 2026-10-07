#!/usr/bin/env python3
"""Wait for a tool and reap its leftover resolver children."""
import argparse
import ctypes
import json
import os
from pathlib import Path
import signal
import subprocess
import sys
import time


def children():
    owner=os.getpid()
    result=[]
    for stat in Path('/proc').glob('[0-9]*/stat'):
        try:
            fields=stat.read_text().rsplit(')',1)[1].split()
            if int(fields[1])==owner:result.append(int(stat.parent.name))
        except (FileNotFoundError,ProcessLookupError):
            pass
    return result


def main():
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--receipt',type=Path,required=True)
    p.add_argument('--parent-exec',type=Path,required=True)
    p.add_argument('--timeout',type=float,default=120)
    p.add_argument('command',nargs=argparse.REMAINDER)
    a=p.parse_args();command=a.command
    if command and command[0]=='--':command=command[1:]
    if not command or a.receipt.exists() or not 0<a.timeout<=1800:p.error('fresh receipt, command and bounded timeout required')
    library=ctypes.CDLL(None,use_errno=True)
    if library.prctl(36,1,0,0,0):raise OSError(ctypes.get_errno(),'PR_SET_CHILD_SUBREAPER')
    child=None;reaped=[];started=time.monotonic();failure=None;code=None
    def interrupt(signum,frame):raise InterruptedError(f'owner signal{signum}')
    for sig in (signal.SIGINT,signal.SIGTERM):signal.signal(sig,interrupt)
    try:
        child=subprocess.Popen([sys.executable,'-B',str(a.parent_exec.resolve()),str(os.getpid()),*command])
        code=child.wait(timeout=a.timeout)
    except BaseException as error:
        failure=type(error).__name__
    finally:
        # No further interruption may bypass reaping; the outer owner retains
        # its separate hard deadline and process-group cleanup.
        for sig in (signal.SIGINT,signal.SIGTERM):signal.signal(sig,signal.SIG_IGN)
        if child is not None and child.poll() is None:
            child.terminate()
            try:child.wait(timeout=2)
            except subprocess.TimeoutExpired:child.kill();child.wait(timeout=3)
        if child is not None:code=child.returncode
        for sig,grace in ((None,.2),(signal.SIGTERM,.5),(signal.SIGKILL,3)):
            if sig is not None:
                # Kernel child ownership keeps these PIDs from being reused
                # until this single-threaded parent reaps them.
                for pid in children():
                    try:os.kill(pid,sig)
                    except ProcessLookupError:pass
            deadline=time.monotonic()+grace
            while True:
                while True:
                    try:pid,status=os.waitpid(-1,os.WNOHANG)
                    except ChildProcessError:pid=0
                    if not pid:break
                    reaped.append(dict(pid=pid,exit_code=os.waitstatus_to_exitcode(status)))
                if not children():break
                if time.monotonic()>=deadline:break
                time.sleep(.01)
            if not children():break
        empty=not children()
        receipt=dict(command=command,parent_waited=child is not None and child.returncode is not None,
            tool_exit_code=code,adopted_children=reaped,children_empty=empty,failure=failure,
            elapsed_seconds=time.monotonic()-started,supervisor_pid=os.getpid(),
            child_parent_death_bound=True,scope='Owned observer tool tree; leftover resolver children joined before return')
        with a.receipt.open('x') as f:json.dump(receipt,f,indent=2);f.write('\n');f.flush();os.fsync(f.fileno())
        if not empty:raise RuntimeError('owned tool tree remains')
    if failure:raise RuntimeError(failure)
    raise SystemExit(code if code>=0 else 128-code)


if __name__=='__main__':main()

import ctypes
import hashlib
import json
import os
from pathlib import Path
import shutil
import signal
import stat
import subprocess
import sys
import time

ROOT=Path("/workspace/work/streams-codecs/revision-07")
OUTPUT=Path("/workspace/work/streams-codecs/oracle-final-04f6bc29-overlay848-attempt-02")
ARGS=["taskset","-c","0,1","python3",str(ROOT/"run-oracle.py"),"--repo","/workspace/partitionline","--snapshot","/workspace/work/client-capabilities-source-04f6bc29","--jars","/workspace/work/broker-wire/jars","--java","/usr/bin/java","--origin","/workspace/work/integration/client-capabilities-source-04f6bc29/receipt.json","--output",str(OUTPUT),"--oracle","/workspace/work/streams-codecs/revision-06/StreamsWireOracle.java"]
FLOOR=350*1024*1024
LIMIT=96*1024*1024
DEADLINE=720
os.umask(0o077)
# Adopt killed driver descendants so shutdown can join owned children as well.
if ctypes.CDLL(None,use_errno=True).prctl(36,1,0,0,0)!=0:
    raise OSError(ctypes.get_errno(),"PR_SET_CHILD_SUBREAPER failed")
assert not OUTPUT.exists()
for name in ("launch-08.json","supervisor-08.json","driver-08.stdout","driver-08.stderr"):
    assert not (ROOT/name).exists()
started=time.monotonic()
known=set()
def descendants(pid):
    found=set()
    todo=[pid]
    while todo:
        at=todo.pop()
        try:
            children=[int(x) for x in Path(f"/proc/{at}/task/{at}/children").read_text().split()]
        except (FileNotFoundError,ProcessLookupError):
            continue
        for child in children:
            if child not in found:
                found.add(child)
                todo.append(child)
    return found

def usage():
    unique={}
    logical={}
    files=0
    if OUTPUT.exists():
        for p in [OUTPUT,*OUTPUT.rglob("*")]:
            info=p.lstat()
            if stat.S_ISLNK(info.st_mode):
                raise RuntimeError("owned output symlink")
            key=(info.st_dev,info.st_ino)
            unique[key]=info.st_blocks*512
            if stat.S_ISREG(info.st_mode):
                logical[key]=info.st_size
                files+=1
    return sum(unique.values()),sum(logical.values()),files

stdout=(ROOT/"driver-08.stdout").open("xb")
stderr=(ROOT/"driver-08.stderr").open("xb")
env=dict(os.environ,PYTHONDONTWRITEBYTECODE="1")
process=subprocess.Popen(ARGS,stdout=stdout,stderr=stderr,env=env,start_new_session=True)
launch={"driver_pid":process.pid,"driver_pgid":os.getpgid(process.pid),"supervisor_pid":os.getpid(),"supervisor_pgid":os.getpgrp(),"output":str(OUTPUT),"arguments":ARGS,"source_sha":"04f6bc2968c1d721c6815a6389897a62e4ca76f1","runner_sha256":hashlib.sha256((ROOT/"run-oracle.py").read_bytes()).hexdigest(),"sole_standalone_oracle_sha256":"848047c5fb87ca57e3d19dd76c575f3b2edbe86ff3ac1d378daca13f1206110a","poll_seconds":0.2,"whole_run_absolute_deadline_seconds":DEADLINE,"minimum_free_bytes":FLOOR,"output_unique_logical_and_allocated_limit_bytes":LIMIT}
(ROOT/"launch-08.json").write_text(json.dumps(launch,indent=2)+"\n")
print(json.dumps(launch),flush=True)
samples=0
minfree=shutil.disk_usage(ROOT).free
maxphysical=maxlogical=maxfiles=0
failure=None
killed=[]
try:
    while True:
        known.update(descendants(process.pid))
        free=shutil.disk_usage(ROOT).free
        physical,logical,files=usage()
        samples+=1
        minfree=min(minfree,free)
        maxphysical=max(maxphysical,physical)
        maxlogical=max(maxlogical,logical)
        maxfiles=max(maxfiles,files)
        if free<FLOOR:
            raise RuntimeError("whole-run disk350MiB floor crossed")
        if max(physical,logical)>LIMIT or files>2048:
            raise RuntimeError("whole-run96MiB output/files bound crossed")
        if time.monotonic()-started>DEADLINE:
            raise RuntimeError("whole-run720s absolute deadline crossed")
        if any((ROOT/name).stat().st_size>65536 for name in ("driver-08.stdout","driver-08.stderr")):
            raise RuntimeError("outer driver stream64KiB cap crossed")
        if process.poll() is not None:
            break
        time.sleep(0.2)
except BaseException as error:
    failure=f"{type(error).__name__}: {error}"
    # Kill every current owned nested session first. The runner receives SIGINT
    # to preserve its complete failure after-audit and partial command receipts.
    current=descendants(process.pid)
    for child in sorted(current,reverse=True):
        try:
            pgid=os.getpgid(child)
            if pgid==child:
                os.killpg(pgid,signal.SIGKILL)
                killed.append(pgid)
        except ProcessLookupError:
            pass
    try:
        os.killpg(process.pid,signal.SIGINT)
    except ProcessLookupError:
        pass
    try:
        process.wait(timeout=30)
    except subprocess.TimeoutExpired:
        for child in sorted(descendants(process.pid),reverse=True):
            try:
                os.kill(child,signal.SIGKILL)
            except ProcessLookupError:
                pass
        os.killpg(process.pid,signal.SIGKILL)
        process.wait(timeout=5)
finally:
    process.wait(timeout=5)
    stdout.close()
    stderr.close()
    remaining=descendants(os.getpid())
    for child in sorted(remaining,reverse=True):
        try:
            os.kill(child,signal.SIGKILL)
        except ProcessLookupError:
            pass
    reaped=[]
    while True:
        try:
            pid,status=os.waitpid(-1,os.WNOHANG)
        except ChildProcessError:
            break
        if pid==0:
            time.sleep(0.01)
            continue
        reaped.append({"pid":pid,"wait_status":status})
    record={**launch,"driver_exit_status":process.returncode,"driver_joined":True,"remaining_owned_children":sorted(descendants(os.getpid())),"watchdog_failure":failure,"sample_count":samples,"minimum_observed_free_bytes":minfree,"maximum_observed_output_allocated_bytes":maxphysical,"maximum_observed_output_unique_logical_bytes":maxlogical,"maximum_observed_output_files":maxfiles,"duration_seconds":time.monotonic()-started,"killed_nested_pgids":killed,"adopted_children_reaped":reaped,"stdout_sha256":hashlib.sha256((ROOT/"driver-08.stdout").read_bytes()).hexdigest(),"stderr_sha256":hashlib.sha256((ROOT/"driver-08.stderr").read_bytes()).hexdigest()}
    (ROOT/"supervisor-08.json").write_text(json.dumps(record,indent=2)+"\n")
    print(json.dumps(record),flush=True)
sys.exit(process.returncode if process.returncode else (1 if failure else 0))

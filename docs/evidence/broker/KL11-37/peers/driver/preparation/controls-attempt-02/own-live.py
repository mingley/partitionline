#!/usr/bin/env python3
"""Linux outer group owner. Infrastructure controls are not OIDC qualification."""
import argparse
import ctypes
import hashlib
import json
import os
from pathlib import Path
import resource
import signal
import subprocess
import sys
import time

INTERRUPTED = False


def request_stop(_signal,_frame):
    global INTERRUPTED
    INTERRUPTED = True


def require(condition, label):
    if not condition:
        raise RuntimeError(label)


def identity(pid):
    try:
        fields = (Path('/proc')/str(pid)/'stat').read_text().rsplit(')',1)[1].split()
        return {'pid':pid,'ppid':int(fields[1]),'pgid':int(fields[2]),
                'starttime_ticks':int(fields[19]),'state':fields[0]}
    except (OSError,ValueError,IndexError):
        return None


def enable_subreaper():
    require(sys.platform=='linux' and Path('/proc/self/stat').is_file(),'Linux ownership capability')
    libc = ctypes.CDLL(None,use_errno=True)
    libc.prctl.argtypes = [ctypes.c_int,ctypes.c_ulong,ctypes.c_ulong,ctypes.c_ulong,ctypes.c_ulong]
    libc.prctl.restype = ctypes.c_int
    require(libc.prctl(36,1,0,0,0)==0,'Linux subreaper admission')
    enabled = ctypes.c_int()
    pointer = ctypes.cast(ctypes.byref(enabled),ctypes.c_void_p).value
    require(libc.prctl(37,pointer,0,0,0)==0 and enabled.value==1,'Linux subreaper confirmed')


class Owner:
    def __init__(self,registry,budget=300,grace=30,term_grace=3):
        require(0<budget<=300 and 0<grace<=30 and 0<term_grace<=3,'finite owner budgets')
        self.registry,self.budget,self.grace,self.term_grace = Path(registry),budget,grace,term_grace
        self.owner_pid = os.getpid()
        self.known = {}
        self.registered = {}
        self.events = []
        self.registry_hashes = []
        self.unverified = False
        self.forced = False
        self.driver = None
        self.driver_identity = None

    def event(self,name,**fields):
        require(len(self.events)<8192,'finite ownership history')
        self.events.append({'ordinal':len(self.events),'event':name,**fields})

    def scan(self):
        snapshot = {}
        for entry in Path('/proc').iterdir():
            if entry.name.isdigit():
                row = identity(int(entry.name))
                if row:
                    snapshot[row['pid']] = row
        descendants = {self.owner_pid}
        changed = True
        while changed:
            changed = False
            for row in snapshot.values():
                if row['ppid'] in descendants and row['pid'] not in descendants:
                    descendants.add(row['pid'])
                    changed = True
        for pid in descendants-{self.owner_pid}:
            row = snapshot[pid]
            self.known[(pid,row['starttime_ticks'])] = row.copy()
        require(len(self.known)<=4096,'finite confirmed descendant identities')
        # Reap adopted grandchildren only; Popen exclusively reaps its driver.
        for row in snapshot.values():
            if row['ppid']==self.owner_pid and row['pid']!=self.driver.pid and row['state']=='Z':
                try:
                    pid,status = os.waitpid(row['pid'],os.WNOHANG)
                    if pid:
                        self.event('adopted-child-reaped',pid=pid,starttime_ticks=row['starttime_ticks'],status=status)
                except ChildProcessError:
                    pass
        self.read_registry(snapshot,descendants)
        return snapshot,descendants

    def read_registry(self,snapshot,descendants):
        if not self.registry.exists():
            return  # live descendant scan covers the registration race
        try:
            require(self.registry.stat().st_size<=65536 and not self.registry.is_symlink(),
                    'bounded regular owner registry')
            data = self.registry.read_bytes()
            value = json.loads(data)
            driver = value['driver']
            require(driver['pid']==self.driver.pid
                    and driver['starttime_ticks']==self.driver_identity['starttime_ticks']
                    and driver['pgid']==self.driver_identity['pgid'],'registry driver identity')
            rows = value['owned_groups']
            require(len(rows)<=128,'finite registered groups')
            for row in rows:
                require(type(row['pid']) is int and type(row['starttime_ticks']) is int
                        and row['pid']==row['pgid'] and row['pid']>1,'registered new session identity')
                key = (row['pid'],row['starttime_ticks'])
                self.registered[key] = {field:row[field] for field in ('pid','pgid','starttime_ticks')}
                live = snapshot.get(row['pid'])
                if live and live['starttime_ticks']==row['starttime_ticks']:
                    require(live['pid'] in descendants or key in self.known,'registered live ownership confirmed')
                    self.known[key] = live.copy()
                # An already-exited leader is a trusted immutable-driver
                # registry observation. A surviving member still needs a
                # confirmed descendant identity before any signal.
            checksum = hashlib.sha256(data).hexdigest()
            if not self.registry_hashes or checksum!=self.registry_hashes[-1]:
                self.registry_hashes.append(checksum)
                require(len(self.registry_hashes)<=256,'finite registry updates')
        except BaseException:
            self.unverified = True

    def confirmed_live(self,snapshot):
        return [row for row in snapshot.values()
                if (row['pid'],row['starttime_ticks']) in self.known]

    def signal_groups(self,signum,include_driver=False):
        snapshot,_ = self.scan()
        groups = {}
        for row in self.confirmed_live(snapshot):
            if (row['pgid']==self.driver_identity['pgid'] and not include_driver
                    and self.driver.poll() is None):
                continue
            groups.setdefault(row['pgid'],[]).append(row)
        for pgid,members in groups.items():
            if pgid<=1 or pgid==os.getpgrp():
                self.unverified = True
                continue
            # Never signal a group containing an unconfirmed live member.
            all_members = [row for row in snapshot.values() if row['pgid']==pgid]
            if any((row['pid'],row['starttime_ticks']) not in self.known for row in all_members):
                self.unverified = True
                continue
            try:
                os.killpg(pgid,signum)
                self.forced = True
                self.event('owned-group-signalled',pgid=pgid,signal=signum,
                           identities=[{key:row[key] for key in ('pid','starttime_ticks')} for row in members])
            except ProcessLookupError:
                pass

    def wait_cleanup(self,seconds):
        deadline = time.monotonic()+seconds
        while time.monotonic()<deadline:
            self.driver.poll()
            snapshot,_ = self.scan()
            if not self.confirmed_live(snapshot):
                return True
            time.sleep(0.1)
        return False

    def run(self,argv):
        enable_subreaper()
        resource.setrlimit(resource.RLIMIT_CORE,(0,0))
        os.umask(0o077)
        self.driver = subprocess.Popen(argv,stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL,
                                       start_new_session=True,umask=0o077)
        self.driver_identity = identity(self.driver.pid)
        require(self.driver_identity and self.driver_identity['pgid']==self.driver.pid,
                'driver new group identity')
        self.known[(self.driver.pid,self.driver_identity['starttime_ticks'])] = self.driver_identity
        deadline = time.monotonic()+self.budget
        timed_out = False
        try:
            while self.driver.poll() is None:
                self.scan()
                if INTERRUPTED or time.monotonic()>=deadline:
                    timed_out = time.monotonic()>=deadline
                    self.forced = True
                    try:
                        os.kill(self.driver.pid,signal.SIGTERM)
                    except ProcessLookupError:
                        pass
                    self.event('controlled-driver-term',pid=self.driver.pid,outer_interrupted=INTERRUPTED)
                    break
                time.sleep(0.1)
        except BaseException:
            self.forced = True
            self.unverified = True
            self.event('owner-monitor-interrupted')
        if timed_out or INTERRUPTED:
            grace = time.monotonic()+self.grace
            while self.driver.poll() is None and time.monotonic()<grace:
                self.scan()
                time.sleep(0.1)
        # Sweep confirmed separate SDK/broker/descendant sessions before any
        # driver SIGKILL, even if the driver has already exited unexpectedly.
        self.scan()
        self.signal_groups(signal.SIGTERM)
        self.wait_cleanup(self.term_grace)
        self.signal_groups(signal.SIGKILL)
        if self.driver.poll() is None:
            self.signal_groups(signal.SIGKILL,include_driver=True)
        self.wait_cleanup(3)
        try:
            code = self.driver.wait(timeout=3)
        except subprocess.TimeoutExpired:
            code = None
        snapshot,_ = self.scan()
        survivors = self.confirmed_live(snapshot)
        # Any registered PGID with active unconfirmed members cannot be
        # silently labeled gone. It is retained as an ownership failure.
        registered_groups = {row['pgid'] for row in self.registered.values()}
        unconfirmed = [row for row in snapshot.values() if row['pgid'] in registered_groups
                       and (row['pid'],row['starttime_ticks']) not in self.known]
        if unconfirmed:
            self.unverified = True
        if not self.registry_hashes:
            self.unverified = True
        safe = lambda row:{key:row[key] for key in ('pid','ppid','pgid','starttime_ticks','state')}
        return {'scope':'Linux infrastructure group ownership; OIDC results are separate',
                'passed':code==0 and not timed_out and not self.forced and not self.unverified and not survivors,
                'driver_exit_code':code,'timed_out':timed_out,'forced_cleanup':self.forced,
                'outer_interrupted':INTERRUPTED,
                'unverified_ownership':self.unverified,'remaining':list(map(safe,survivors)),
                'unconfirmed_registered_group_members':list(map(safe,unconfirmed)),
                'registry_sha256_history':self.registry_hashes,'events':self.events,
                'confirmed_identity_count':len(self.known),
                'driver_stdout_stderr':'discarded; no arbitrary messages or private token data retained'}


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--config',type=Path,required=True)
    parser.add_argument('--output',type=Path,required=True)
    parser.add_argument('--private-scratch',type=Path,required=True)
    args = parser.parse_args()
    signal.signal(signal.SIGTERM,request_stop)
    signal.signal(signal.SIGINT,request_stop)
    require(args.config.stat().st_size<=65536,'bounded live configuration')
    config = json.loads(args.config.read_text())
    source = Path(config['source_root'])
    driver = source/config['driver_relative_path']
    require(hashlib.sha256(driver.read_bytes()).hexdigest()==config['driver_sha256'],'immutable driver hash')
    require((source/config['outer_relative_path']).resolve()==Path(__file__).resolve()
            and hashlib.sha256(Path(__file__).read_bytes()).hexdigest()==config['outer_sha256'],
            'immutable outer owner hash')
    owner = Owner(args.output/'owned-process-groups.json')
    result = owner.run([sys.executable,str(driver),'--config',str(args.config),
                        '--output',str(args.output),'--private-scratch',str(args.private_scratch)])
    args.output.mkdir(mode=0o700,parents=True,exist_ok=True)
    (args.output/'outer-ownership.json').write_text(json.dumps(result,indent=2)+'\n')
    raise SystemExit(0 if result['passed'] else 1)


if __name__=='__main__':
    main()

#!/usr/bin/env python3
"""Synthetic-only controls: extract runner functions without executing its workflow."""
from pathlib import Path as RealPath
import ast
import copy
import hashlib
import json
import os
import stat
import types

BASE=RealPath('/workspace/work/client-capability-qa-preparation-e90efb49')
PREP=BASE/'platform-daemon-exception-preparation-v2'
ALLOW_PREP=BASE/'platform-daemon-exception-preparation'
RUNNER=BASE/'run-forced-baseline403-platform-exception-v2.py'
RAW=RUNNER.read_bytes()
assert hashlib.sha256(RAW).hexdigest()=='231425e452e5f1045b5eb6a8006a459d6f8ed6f4bd3dca3dc44afc4a6b3ba07c'
TREE=ast.parse(RAW)
FUNCTIONS=['verify_platform_daemons','verify_docker_cli','docker_zero_running_workloads','process_reference_guard','forecast']
MODULE=ast.Module(body=[n for n in TREE.body if isinstance(n,ast.FunctionDef) and n.name in FUNCTIONS],type_ignores=[])
assert len(MODULE.body)==len(FUNCTIONS)
CODE=compile(ast.fix_missing_locations(MODULE),str(RUNNER),'exec')
ACTUAL_ALLOW_BYTES=(ALLOW_PREP/'exact-platform-daemons.json').read_bytes()
assert hashlib.sha256(ACTUAL_ALLOW_BYTES).hexdigest()=='5b225336055754c792fecb6f2ce24364a6330fb93f728011ebb78ab53bd51ebd'
ACTUAL_ALLOW=json.loads(ACTUAL_ALLOW_BYTES)
assert [x['pid'] for x in ACTUAL_ALLOW['platform_daemons']]==[199,251]
assert all(x['uid_all_four']==[0,0,0,0] for x in ACTUAL_ALLOW['platform_daemons'])

class Model:
    def __init__(self):
        self.data={};self.links={};self.faults={};self.writes={};self.stat_modes={};self.accesses=[]
        self.pids=[199,251,700,701,999]
        self.allowed=copy.deepcopy(ACTUAL_ALLOW)
        for expected in self.allowed['platform_daemons']:
            cmd=('synthetic-'+expected['comm']).encode()+b'\0'
            expected['cmdline_sha256']=hashlib.sha256(cmd).hexdigest();expected['cmdline_bytes']=len(cmd)
            self.proc(expected['pid'],expected['comm'],expected['parent_pid'],expected['starttime_ticks'],cmd=cmd,uid=0)
            for field in ['cwd','exe','environ','maps','fd']:
                self.faults['/proc/'+str(expected['pid'])+'/'+field]=PermissionError(13,'synthetic pinned field is intentionally inaccessible')
        self.proc(700,'worker',1,900,cmd=b'synthetic-worker\0',uid=1000)
        self.proc(701,'dead',1,901,state='Z')
        self.proc(999,'supervisor',1,902)
        cli=b'synthetic exact Docker client';path=self.allowed['docker_cli']['path']
        self.data[path]=cli;self.stat_modes[path]=stat.S_IFREG|0o755
        self.allowed['docker_cli'].update(sha256=hashlib.sha256(cli).hexdigest(),bytes=len(cli),full_mode=0o755)
        self.data['/mock.stdout']=b''
        self.run_calls=[];self.persist_calls=0
    def proc(self,pid,comm,ppid,start,state='S',cmd=b'mock\0',uid=1000):
        base='/proc/'+str(pid);fields=[state,str(ppid)]+['0']*18;fields[19]=str(start)
        self.data[base+'/stat']=(str(pid)+' ('+comm+') '+' '.join(fields)+'\n').encode()
        self.data[base+'/status']=('Name:\t'+comm+'\nPPid:\t'+str(ppid)+'\nUid:\t'+('\t'.join([str(uid)]*4))+'\n').encode()
        self.data[base+'/cmdline']=cmd
        self.data[base+'/environ']=b'OTHER=value\0';self.data[base+'/maps']=b'no target mappings\n'
        self.links[base+'/cwd']='/mock-unrelated';self.links[base+'/exe']='/mock-executable';self.links[base+'/fd/0']='/dev/null'
    def node(self,value):return Node(self,str(value))
    def read(self,path):
        self.accesses.append(path)
        if path in self.faults:raise self.faults[path]
        if path not in self.data:raise FileNotFoundError(2,'synthetic nonexistent path')
        return self.data[path]
    def readlink(self,node):
        path=str(node);self.accesses.append(path)
        if path in self.faults:raise self.faults[path]
        if path not in self.links:raise FileNotFoundError(2,'synthetic nonexistent link')
        return self.links[path]
    def run(self,name,argv,root,manifest,timeout):
        self.run_calls.append({'name':name,'argv':argv,'timeout':timeout})
        return {'name':name,'stdout_path':'/mock.stdout'}
    def persist(self):self.persist_calls+=1
    def namespace(self):
        ns={'Path':self.node,'PLATFORM_ALLOW':self.allowed,'PLATFORM_ALLOW_BYTES':ACTUAL_ALLOW_BYTES,
            'digest':lambda b:hashlib.sha256(b).hexdigest(),'os':types.SimpleNamespace(getpid=lambda:999,readlink=self.readlink,fsencode=os.fsencode),
            'stat':stat,'RUN':self.node('/mock-run'),'TARGET':self.node('/mock-target'),'json':json,
            'run':self.run,'SOURCE':self.node('/mock-source'),'MANIFEST':{},'persist':self.persist}
        exec(CODE,ns)
        return ns

class Node:
    def __init__(self,model,path):self.model=model;self.path=path
    def __str__(self):return self.path
    def __truediv__(self,other):return Node(self.model,self.path.rstrip('/')+'/'+str(other))
    @property
    def name(self):return self.path.rsplit('/',1)[-1]
    def read_bytes(self):return self.model.read(self.path)
    def read_text(self):return self.read_bytes().decode()
    def write_text(self,value):self.model.writes[self.path]=value
    def lstat(self):return types.SimpleNamespace(st_mode=self.model.stat_modes[self.path])
    def iterdir(self):
        self.model.accesses.append(self.path)
        if self.path in self.model.faults:raise self.model.faults[self.path]
        if self.path=='/proc':return iter([self/'self',*[self/str(pid) for pid in self.model.pids]])
        if self.path.endswith('/fd'):return iter([self/'0'])
        raise FileNotFoundError(2,'synthetic nonexistent directory')

RESULTS=[]
def control(name,fn):
    fn();RESULTS.append({'name':name,'passed':True})
def rejected(fn):
    try:fn()
    except (AssertionError,OSError,KeyError):return
    raise AssertionError('synthetic hostile control was not rejected')

def positive():
    m=Model();ns=m.namespace();row=ns['process_reference_guard']()
    assert row['checked_live_other_processes']==1 and row['nonexecuting_zombies_excluded']==1
    assert row['all_relevant_nonexempt_live_fields_readable'] and row['no_target_references_from_inspectable_processes']
    assert row['universal_all_pid_readability_or_no_owner_claim'] is False
    assert [x['pid'] for x in row['root_approved_exact_platform_daemons']]==[199,251]
    for pid in [199,251]:
        assert not any('/proc/'+str(pid)+'/'+field in m.accesses for field in ['cwd','exe','environ','maps','fd'])
control('exact-two-pinned-daemons-positive-with-explicit-uninspected-scope',positive)

for key,value in [('comm','different'),('parent_pid',2),('uid_all_four',[1000]*4),('starttime_ticks',537),('cmdline_sha256','0'*64),('cmdline_bytes',999)]:
    def changed(key=key,value=value):
        m=Model();ns=m.namespace();ns['PLATFORM_ALLOW']['platform_daemons'][0][key]=value
        rejected(ns['verify_platform_daemons'])
    control('reject-changed-pinned-'+key,changed)

def changed_status_name():
    m=Model();m.data['/proc/199/status']=m.data['/proc/199/status'].replace(b'dockerd',b'other');rejected(m.namespace()['verify_platform_daemons'])
control('reject-changed-status-Name',changed_status_name)
for field in ['stat','status','cmdline']:
    def unreadable(field=field):
        m=Model();m.faults['/proc/199/'+field]=PermissionError(13,'synthetic');rejected(m.namespace()['verify_platform_daemons'])
    control('reject-unverifiable-pinned-'+field,unreadable)

def changed_after():
    m=Model();ns=m.namespace();verify=ns['verify_platform_daemons'];calls=[0]
    def hook():
        calls[0]+=1
        if calls[0]==2:m.data['/proc/199/cmdline']=b'changed-after-scan\0'
        return verify()
    ns['verify_platform_daemons']=hook;rejected(ns['process_reference_guard'])
control('reject-pinned-identity-change-after-live-process-scan',changed_after)

for field in ['stat','cwd','exe','environ','maps','fd']:
    def unknown_unreadable(field=field):
        m=Model();m.faults['/proc/700/'+field]=PermissionError(13,'unknown process is not exempt');rejected(m.namespace()['process_reference_guard'])
    control('reject-nonexempt-unreadable-'+field,unknown_unreadable)
for field in ['cwd','exe','environ','maps','fd']:
    def reference(field=field):
        m=Model()
        if field in ['cwd','exe']:m.links['/proc/700/'+field]='/mock-target/actual-owner'
        elif field=='environ':m.data['/proc/700/environ']=b'CARGO_TARGET_DIR=/mock-target\0'
        elif field=='maps':m.data['/proc/700/maps']=b'synthetic mapped /mock-target/lib\n'
        else:m.links['/proc/700/fd/0']='/mock-target/current-output'
        rejected(m.namespace()['process_reference_guard'])
    control('reject-inspectable-cache-reference-'+field,reference)

def unknown_name():
    m=Model();m.pids.append(702);m.proc(702,'dockerd',1,999,uid=0);m.faults['/proc/702/maps']=PermissionError(13,'same name does not authorize exception')
    rejected(m.namespace()['process_reference_guard'])
control('reject-new-PID-even-when-comm-and-UID-look-like-daemon',unknown_name)

def zero():
    m=Model();row=m.namespace()['docker_zero_running_workloads']('mock-before-clean')
    assert row['actual_docker_api_zero_running_workloads'] and len(m.run_calls)==1
    assert m.run_calls[0]['argv']==['taskset','-c','0,1',*m.allowed['docker_ps_argv']] and m.run_calls[0]['timeout']==10
control('synthetic-Docker-empty-exit0-positive-exact-argv-and-timeout',zero)
def docker_nonempty():
    m=Model();m.data['/mock.stdout']=b'abc workload Up\n';rejected(lambda:m.namespace()['docker_zero_running_workloads']('mock-before-clean'))
control('reject-Docker-any-running-workload',docker_nonempty)
def docker_failure():
    m=Model();ns=m.namespace()
    def bad_run(*args,**kwargs):raise AssertionError('synthetic Docker API exit1/timeout refused by monitored run')
    ns['run']=bad_run;rejected(lambda:ns['docker_zero_running_workloads']('mock-before-clean'))
control('reject-Docker-query-failure-no-zero-workload-inference',docker_failure)
def docker_changed():
    m=Model();ns=m.namespace();original=m.run
    def hook(*args,**kwargs):
        row=original(*args,**kwargs);m.data['/proc/251/cmdline']=b'changed\0';return row
    ns['run']=hook;rejected(lambda:ns['docker_zero_running_workloads']('mock-before-clean'))
control('reject-Docker-daemon-change-after-API-query',docker_changed)
for field in ['sha256','bytes','full_mode']:
    def cli_change(field=field):
        m=Model();m.allowed['docker_cli'][field]=('0'*64 if field=='sha256' else 1);rejected(m.namespace()['verify_docker_cli'])
    control('reject-Docker-CLI-changed-'+field,cli_change)
def symlink():
    m=Model();m.stat_modes[m.allowed['docker_cli']['path']]=stat.S_IFLNK|0o755;rejected(m.namespace()['verify_docker_cli'])
control('reject-Docker-CLI-symlink-substitution',symlink)

FLOOR=350*1024*1024
for index,required in [(0,782925569),(1,582252979),(2,547585963)]:
    def resource_boundary(index=index,required=required):
        m=Model();ns=m.namespace();sample=[required]
        ns.update(ROWS=[{'name':'stable-baseline-'+str(i)} for i in range(index)],LIBRARY_ALLOWANCE=83002787,TARGET_ALLOWANCE=17333508,
                  FORECAST_METADATA=16*1024*1024,SOURCE_METADATA=4*1024*1024,FLOOR=FLOOR,ROOT_PUBLICATION_RESERVE=32*1024*1024,
                  LAST_FORECAST_KIND='compile',free_bytes=lambda:sample[0],cache_allocated=lambda:227930112)
        row=ns['forecast']();assert row['required_free_bytes']==required and row['headroom_bytes']==0
        assert row['postclean_compile_required_free_bytes']==required
        sample[0]-=1;rejected(ns['forecast'])
    control('compile-forecast-boundary-with32MiB-root-reserve-after-'+str(index)+'-targets',resource_boundary)

for field in ['cwd','exe','environ','maps','fd']:
    def missing_live_field(field=field):
        m=Model();m.faults['/proc/700/'+field]=FileNotFoundError(2,'field absent but exact same process remains live')
        rejected(m.namespace()['process_reference_guard'])
    control('reject-ENOENT-on-still-live-nonexempt-'+field,missing_live_field)
def reused_pid():
    m=Model();ns=m.namespace();original=m.read;count=[0]
    def read_hook(path):
        if path=='/proc/700/stat':
            count[0]+=1
            if count[0]==2:m.proc(700,'new-owner',1,901,cmd=b'new-process',uid=1000)
        return original(path)
    m.read=read_hook;rejected(ns['process_reference_guard'])
control('reject-nonexempt-PID-starttime-comm-change-during-scan',reused_pid)
def closed_fd():
    m=Model();m.faults['/proc/700/fd/0']=FileNotFoundError(2,'descriptor closed after fd listing')
    row=m.namespace()['process_reference_guard']();assert row['vanished_fd_links_observed_closed']==1
    assert row['nonexempt_live_PID_starttime_comm_rechecked'] and row['universal_all_pid_readability_or_no_owner_claim'] is False
control('observed-closed-FD-positive-with-live-identity-recheck',closed_fd)
def genuine_exit():
    m=Model();ns=m.namespace();original=m.read;exited=[False]
    def read_hook(path):
        if path=='/proc/700/maps':exited[0]=True
        if exited[0] and path.startswith('/proc/700/'):raise FileNotFoundError(2,'entire synthetic process exited')
        return original(path)
    original_link=m.readlink
    def link_hook(node):
        if exited[0] and str(node).startswith('/proc/700/'):raise FileNotFoundError(2,'synthetic process exited')
        return original_link(node)
    m.read=read_hook;m.readlink=link_hook;ns['os'].readlink=link_hook
    row=ns['process_reference_guard']();assert not row['unexplained_live_field_failures']
control('genuine-process-exit-positive-only-with-stat-absence',genuine_exit)

assert len(RESULTS)==43,len(RESULTS)
receipt={'schema_version':1,'runner_sha256':hashlib.sha256(RAW).hexdigest(),'control_source_sha256':hashlib.sha256(RealPath(__file__).read_bytes()).hexdigest(),
         'exact_allowlist_sha256':hashlib.sha256(ACTUAL_ALLOW_BYTES).hexdigest(),'passed':True,'synthetic_controls':len(RESULTS),'controls':RESULTS,
         'scope':'mocked extracted functions only; no actual Docker API, process-owner qualification, cleanup, Cargo/JVM/socket/API behavior run',
         'actual_runner_workflow_executions':0,'actual_cache_removals':0,'actual_Cargo_commands':0,'actual_Docker_queries':0}
output=PREP/'helper-controls.json';output.write_text(json.dumps(receipt,indent=2)+'\n');output.chmod(0o600)
print(json.dumps({'passed':True,'controls':len(RESULTS),'receipt_sha256':hashlib.sha256(output.read_bytes()).hexdigest()}))

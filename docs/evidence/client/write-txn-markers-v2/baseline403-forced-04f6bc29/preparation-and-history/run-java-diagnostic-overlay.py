#!/usr/bin/env python3
"""Exact e90 three-SDK Java-only lane; stop first failure and preserve outputs."""
from pathlib import Path
import gzip
import hashlib
import json
import os
import resource
import signal
import stat
import subprocess
import time

BASE = Path('/workspace/work/client-capability-qa-preparation-e90efb49')
SOURCE = Path('/workspace/work/client-capabilities-source-e90efb49')
ROOT = BASE / 'java-diagnostic-overlay-attempt-02'
ROOT.mkdir(mode=0o700)
LIMIT = 16*1024*1024
FLOOR = 350*1024*1024
EXPECTED = json.loads(Path('/workspace/work/integration/client-capabilities-source-e90efb49/complete-source.json').read_bytes())
PLAN = json.loads((BASE/'qa-plan.json').read_bytes())
OVERLAY = BASE / 'java-correction-after-first-compile'
OVERLAY_EXPECTED = {name: (OVERLAY/name).read_bytes() for name in ['CapabilityOracle.java','PublicAdminProbe.java']}
OVERLAY_MODES = {name:stat.S_IMODE((OVERLAY/name).stat().st_mode) for name in OVERLAY_EXPECTED}
JAVA = str(Path('/usr/bin/java').resolve(strict=True))
ROWS = []

def sha(data):
    return hashlib.sha256(data).hexdigest()

def free():
    s=os.statvfs('/workspace');return s.f_bavail*s.f_frsize

def guard():
    actual={}
    for directory,dirs,names in os.walk(SOURCE,followlinks=False):
        for name in list(dirs):
            p=Path(directory)/name
            if p.is_symlink():names.append(name);dirs.remove(name)
        for name in names:
            p=Path(directory)/name;actual[p.relative_to(SOURCE).as_posix()]=p.lstat()
    assert actual.keys()==EXPECTED.keys()
    hashed=hashlib.sha256();size=0
    for name in sorted(actual):
        p=SOURCE/name;s=actual[name];info=EXPECTED[name];mode=stat.S_IMODE(s.st_mode)
        data=os.fsencode(os.readlink(p)) if stat.S_ISLNK(s.st_mode) else p.read_bytes()
        assert mode==info['full_permission_mode'] and len(data)==info['bytes'] and sha(data)==info['sha256'],name
        blob=hashlib.sha1(b'blob '+str(len(data)).encode()+b'\0'+data).hexdigest()
        assert blob==info['git_blob_sha1'],name
        hashed.update(name.encode()+b'\0'+blob.encode()+b'\0'+str(mode).encode()+b'\0');size+=len(data)
    for name,data in OVERLAY_EXPECTED.items():
        assert (OVERLAY/name).read_bytes()==data and stat.S_IMODE((OVERLAY/name).stat().st_mode)==OVERLAY_MODES[name]
    return {'files':len(actual),'bytes':size,'full_path_set_git_blobs_sha256_lengths_full_modes_match':True,'set_blob_fullmode_sha256':hashed.hexdigest()}

def artifact(p):
    data=p.read_bytes();return {'path':str(p),'bytes':len(data),'sha256':sha(data),'full_mode':stat.S_IMODE(p.stat().st_mode)}

def output_manifest(root):
    return {str(p.relative_to(root)):artifact(p) for p in sorted(root.rglob('*')) if p.is_file()}

def volume():
    return sum(p.stat().st_size for p in ROOT.rglob('*') if p.is_file())

def save(passed=False):
    (ROOT/'validation.json').write_text(json.dumps({'schema_version':1,'source_sha':PLAN['source_sha'],'passed':passed,
        'scope':'diagnostic strict Javac only: exact immutable e90 plus explicitly named/hash-bound two-Java WORK overlay; not final main qualification, no handler/generation/runtime claim',
        'named_source_overlays':[artifact(OVERLAY/name) for name in sorted(OVERLAY_EXPECTED)],
        'runner':artifact(Path(__file__)),'commands':ROWS,'java_executable':artifact(Path(JAVA)),
        'output_bound_bytes':LIMIT,'source_worktree_used':False,'no_Cargo_invocation':True},indent=2)+'\n')

def execute(name,argv,timeout=90):
    before=guard()
    # SDK identities are rechecked for every child, even when the next child is compiler identity.
    for jar in PLAN['execution_sequence'][3]['jars']:
        assert artifact(Path(jar['path']))['sha256']==jar['sha256']
    slf4j=PLAN['execution_sequence'][3]['slf4j_runtime']
    assert artifact(Path(slf4j['path']))['sha256']==slf4j['sha256']
    assert free()>=FLOOR+LIMIT and volume()<LIMIT
    out=ROOT/(name+'.stdout.log');err=ROOT/(name+'.stderr.log');log=ROOT/(name+'.monitor.jsonl')
    start=time.monotonic();observations=[];trigger=None
    env=os.environ.copy();env.update(CARGO_HOME='/workspace/work/cargo',RUSTUP_HOME='/workspace/work/rustup',JAVA_TOOL_OPTIONS='')
    with out.open('wb') as stdout,err.open('wb') as stderr,log.open('w') as monitor:
        def limits():resource.setrlimit(resource.RLIMIT_CORE,(0,0))
        child=subprocess.Popen(argv,cwd=SOURCE,env=env,stdout=stdout,stderr=stderr,start_new_session=True,preexec_fn=limits)
        print('started '+name+' pid='+str(child.pid),flush=True)
        while True:
            code=child.poll();row={'elapsed_seconds':time.monotonic()-start,'free_bytes':free(),'output_tree_bytes':volume(),'process_group':child.pid,'exit_code':code}
            observations.append(row);monitor.write(json.dumps(row)+'\n');monitor.flush()
            if row['free_bytes']<FLOOR:trigger='sampled_disk_floor'
            elif row['output_tree_bytes']>LIMIT:trigger='16MiB_output_ceiling'
            elif row['elapsed_seconds']>timeout:trigger='child_timeout'
            if trigger and code is None:
                os.killpg(child.pid,signal.SIGTERM)
                try:child.wait(timeout=2)
                except subprocess.TimeoutExpired:os.killpg(child.pid,signal.SIGKILL);child.wait(timeout=2)
                break
            if code is not None:break
            time.sleep(0.2)
    after=guard();assert before==after
    row={'name':name,'argv':argv,'cwd':str(SOURCE),'exit_code':child.returncode,'trigger':trigger,
         'cpu_affinity':'0,1','max_heap_bytes':128*1024*1024,'timeout_seconds':timeout,'source_before':before,'source_after':after,
         'stdout':artifact(out),'stderr':artifact(err),'monitor':artifact(log),'minimum_sampled_free_bytes':min(o['free_bytes'] for o in observations),
         'maximum_sampled_output_bytes':max(o['output_tree_bytes'] for o in observations),'sample_count':len(observations),
         'classes_present':output_manifest(ROOT/'classes'), 'passed':child.returncode==0 and trigger is None}
    ROWS.append(row);save();print('closed '+name+' exit='+str(child.returncode),flush=True)
    assert row['passed'],('actual Java command failed',name,child.returncode,trigger)

try:
    execute('java-identity',['taskset','-c','0,1',JAVA,'-Xmx128m','-version'],15)
    phase=PLAN['execution_sequence'][3]
    sources=[str(OVERLAY/'CapabilityOracle.java'),str(OVERLAY/'PublicAdminProbe.java')]
    for sdk in phase['jars']:
        release=sdk['release'];classes=ROOT/'classes'/release;classes.mkdir(parents=True)
        dependencies=sdk['path']+':'+phase['slf4j_runtime']['path']
        execute(release+'-strict-compile',['taskset','-c','0,1',JAVA,'-Xmx128m','--add-modules','jdk.compiler','com.sun.tools.javac.Main','-Xlint:all','-Werror','-cp',dependencies,'-d',str(classes),*sources])
    save(True)
except BaseException as e:
    save(False)
    (ROOT/'failure.json').write_text(json.dumps({'passed':False,'failure_type':type(e).__name__,'detail':str(e),'later_commands_not_launched':True,'retained_actual_classes':output_manifest(ROOT/'classes'),'scope':'Java-only actual attempt; no Rust/broker claims'},indent=2)+'\n')
    raise
print('diagnostic3SDKstrictjavac closed; no component/runtime work launched',flush=True)

import hashlib,json,os,pathlib,subprocess,sys,time,signal
ROOT=pathlib.Path('/workspace/partitionline/docs/evidence/broker/KL11-74/runtime/development')
SOURCE=pathlib.Path('/workspace/work/membership-runtime/dev-source')
TARGET=pathlib.Path('/workspace/work/target-broker-segments')
manifest=json.loads((ROOT/'corrected-origin-source.json').read_text())
name=sys.argv[1]; tool=sys.argv[2]; mode=sys.argv[3]
def source_map():
    return {k:hashlib.sha256((SOURCE/k).read_bytes()).hexdigest() for k in manifest['files']}
def used_bytes():
    return sum(p.stat().st_size for p in TARGET.rglob('*') if p.is_file()) if TARGET.exists() else 0
before=source_map()
assert before=={k:v['sha256'] for k,v in manifest['files'].items()}
env=os.environ.copy();env.update(CARGO_HOME='/workspace/work/cargo',RUSTUP_HOME='/workspace/work/rustup',CARGO_TARGET_DIR=str(TARGET),CARGO_INCREMENTAL='0',CARGO_BUILD_JOBS='1',CARGO_PROFILE_DEV_DEBUG='0',CARGO_PROFILE_TEST_DEBUG='0')
env['PL_MEMBERSHIP_CAPTURE_DIR']=str(ROOT/name/'captures')
env['PL_CONTROLLER_RESPONSE_DIR']=str(ROOT/name/'fixed-controller')
env['PL_MEMBERSHIP_SOURCE_SHA']='development-d21-owned-overlay'
env['PATH']='/workspace/work/cargo/bin:'+env['PATH']
argv=['taskset','-c','2,4','cargo','+'+tool,mode,'--locked','--manifest-path',str(SOURCE/'partitionline-broker/Cargo.toml')]
if mode=='test':argv+=['--test','raft_membership','--test','raft_protocol','--','--test-threads=1','--nocapture','--skip','actual_transport_dispatch_advertises_controller_profile_and_joins','--skip','independent_controller_goldens_cross_actual_tcp_transport']
else:argv+=['--lib','--test','raft_membership','--test','raft_replication','--test','raft_election','--test','raft_snapshot','--test','raft_protocol','--','-D','warnings']
start=time.monotonic();minfree=os.statvfs('/workspace').f_bavail*os.statvfs('/workspace').f_frsize;terminated=False
with (ROOT/(name+'.log')).open('wb') as log:
    p=subprocess.Popen(argv,cwd=SOURCE,env=env,stdout=log,stderr=subprocess.STDOUT,start_new_session=True)
    while p.poll() is None:
        stat=os.statvfs('/workspace');free=stat.f_bavail*stat.f_frsize;minfree=min(minfree,free)
        if free<350*1024*1024:
            terminated=True;os.killpg(p.pid,signal.SIGTERM);p.wait();break
        time.sleep(0.25)
    rc=p.wait()
result={'schema_version':1,'command':argv,'source_base_sha':manifest['source_base_sha'],'source_manifest':'corrected-origin-source.json','source_before':before,'source_after':source_map(),'source_unchanged':before==source_map(),'exit_code':rc,'elapsed_seconds':time.monotonic()-start,'environment':{k:env[k] for k in ['CARGO_HOME','RUSTUP_HOME','CARGO_TARGET_DIR','CARGO_INCREMENTAL','CARGO_BUILD_JOBS','CARGO_PROFILE_DEV_DEBUG','CARGO_PROFILE_TEST_DEBUG','PL_MEMBERSHIP_CAPTURE_DIR','PL_MEMBERSHIP_SOURCE_SHA','PL_CONTROLLER_RESPONSE_DIR']},'log':name+'.log','log_sha256':hashlib.sha256((ROOT/(name+'.log')).read_bytes()).hexdigest(),'minimum_free_mib':minfree/1024/1024,'target_bytes_after':used_bytes(),'terminated_for_disk_floor':terminated,'qualification':'Development focused scope only; no final Git source qualification.'}
(ROOT/(name+'.json')).write_text(json.dumps(result,indent=2)+'\n')
print(json.dumps(result,indent=2));sys.exit(rc)

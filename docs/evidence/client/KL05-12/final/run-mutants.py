#!/usr/bin/env python3
"""Compile two behavior regressions and require the intended oracle/socket failures."""
import hashlib,json,os,subprocess,time
from pathlib import Path

BASE=Path('/workspace/work/fetch-v18')
SHA='7bae3e34b5acedb6cac8b8232c9878f83fd7be30'
SOURCE=BASE/'compiled-mutant-source'
OUT=BASE/'compiled-mutants'
SOURCE.mkdir(exist_ok=False);OUT.mkdir(exist_ok=False)
subprocess.run(['tar','-xf',str(BASE/('source-'+SHA+'.tar')),'-C',str(SOURCE)],check=True)
env=os.environ.copy();env.update(CARGO_HOME='/workspace/work/cargo',RUSTUP_HOME='/workspace/work/rustup',CARGO_TARGET_DIR=str(BASE/'target-stable'),CARGO_INCREMENTAL='0',CARGO_BUILD_JOBS='1',CARGO_PROFILE_DEV_DEBUG='0',CARGO_PROFILE_TEST_DEBUG='0')
env['PATH']='/workspace/work/cargo/bin:'+env['PATH']
report={'source_sha':SHA,'status':'running','controls':[]}
def save(): (OUT/'validation.json').write_text(json.dumps(report,indent=2)+'\n')
controls=[('strip-hwm-tag','src/protocol/fetch.rs','let watermark = version >= 18 && high_watermark != REPLICA_HIGH_WATERMARK_NOT_SUPPORTED;','let watermark = version >= 19 && high_watermark != REPLICA_HIGH_WATERMARK_NOT_SUPPORTED;',1,'protocol_oracles','fetch_v18_rust_bodies_match_apache_and_can_be_parsed_independently','assertion `left == right` failed'),('cap-negotiation-at17','src/consumer.rs','pick_version(v.min_version, v.max_version, 4, FETCH_CRATE_MAX_VERSION)','pick_version(v.min_version, v.max_version, 4, 17)',2,'consumer_fetch_semantics','fetch_mixed_version_both_bootstrap_leader_orders','older bootstrap must not cap newer leader 1 below its supported Fetch version')]
save()
for name,file,old,new,count,target,test,marker in controls:
    p=SOURCE/file;original=p.read_bytes();text=original.decode();assert text.count(old)==count
    p.write_text(text.replace(old,new));changed=p.read_bytes()
    (OUT/(name+'.patch')).write_text(subprocess.check_output(['diff','-u',str(BASE/('source-'+SHA)/file),str(p)],text=True) if False else '- '+old+'\n+ '+new+'\n')
    command=['taskset','-c','0-2,4','cargo','+stable','test','--offline','--locked','--all-features','--test',target,test,'--jobs','1','--','--exact','--nocapture']
    start=time.time()
    with (OUT/(name+'.log')).open('w') as log:r=subprocess.run(command,cwd=SOURCE,env=env,stdout=log,stderr=subprocess.STDOUT,timeout=600)
    p.write_bytes(original)
    output=(OUT/(name+'.log')).read_text()
    intended=r.returncode==101 and marker in output and ('test '+test+' ... FAILED') in output and 'could not compile' not in output
    report['controls'].append({'name':name,'command':command,'test':test,'exit_code':r.returncode,'expected_exit_code':101,'intended_behavior_failure':intended,'marker':marker,'changed_file':file,'changed_occurrences':count,'original_sha256':hashlib.sha256(original).hexdigest(),'mutated_sha256':hashlib.sha256(changed).hexdigest(),'source_restored':p.read_bytes()==original,'seconds':round(time.time()-start,3),'log':name+'.log'})
    save();print(name,r.returncode,intended,flush=True)
    if not intended:report['status']='failed';save();raise SystemExit(1)
report['status']='passed';save()

#!/usr/bin/env python3
import hashlib,json,os,subprocess,time
from pathlib import Path
BASE=Path('/workspace/work/fetch-v18');REPO=Path('/workspace/partitionline');SHA='7bae3e34b5acedb6cac8b8232c9878f83fd7be30';SOURCE=BASE/('source-'+SHA);OUT=BASE/'final-supplement';OUT.mkdir(exist_ok=False)
env=os.environ.copy();env.update(CARGO_HOME='/workspace/work/cargo',RUSTUP_HOME='/workspace/work/rustup',CARGO_INCREMENTAL='0',CARGO_BUILD_JOBS='1',CARGO_PROFILE_DEV_DEBUG='0',CARGO_PROFILE_TEST_DEBUG='0');env['PATH']='/workspace/work/cargo/bin:'+env['PATH']
report={'source_sha':SHA,'status':'running','commands':[]}
def run(name,argv,cwd,e):
    start=time.time();command=['taskset','-c','0-2,4']+argv
    with (OUT/(name+'.log')).open('w') as log:r=subprocess.run(command,cwd=cwd,env=e,stdout=log,stderr=subprocess.STDOUT,timeout=600)
    report['commands'].append({'name':name,'command':command,'cwd':str(cwd),'exit_code':r.returncode,'log':name+'.log','seconds':round(time.time()-start,3)})
    (OUT/'validation.json').write_text(json.dumps(report,indent=2)+'\n');print(name,r.returncode,flush=True)
    if r.returncode:raise SystemExit(r.returncode)
for toolchain in ['stable','1.85.0']:
    e=env.copy();e['CARGO_TARGET_DIR']=str(BASE/('target-'+toolchain));run(toolchain+'-strict-default',['cargo','+'+toolchain,'clippy','--offline','--locked','--all-targets','--jobs','1','--','-D','warnings'],SOURCE,e)
run('updated-conformance-coverage',['python3','-B','scripts/check-protocol-coverage.py','--json','--output',str(OUT/'coverage-report.json')],REPO,env)
run('updated-conformance-guards',['python3','-B','-m','unittest','discover','-s','tests/conformance','-p','test_check_protocol_coverage.py'],REPO,env)
for entry in subprocess.check_output(['git','ls-tree','-r','-z',SHA],cwd=REPO).split(b'\0')[:-1]:
    meta,name=entry.split(b'\t',1);b=(SOURCE/name.decode()).read_bytes();assert hashlib.sha1(b'blob '+str(len(b)).encode()+b'\0'+b).hexdigest()==meta.decode().split()[2]
report['status']='passed';report['immutable_source_unchanged']=True;(OUT/'validation.json').write_text(json.dumps(report,indent=2)+'\n')

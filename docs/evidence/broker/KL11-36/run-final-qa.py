#!/usr/bin/env python3
"""Run exact-source stable/MSRV feature and lint matrices, retaining every command."""
import argparse,hashlib,json,os,subprocess,time
from pathlib import Path
p=argparse.ArgumentParser();p.add_argument('--source',type=Path,required=True);p.add_argument('--sha',required=True);p.add_argument('--target',type=Path,required=True);p.add_argument('--output',type=Path,required=True);a=p.parse_args()
a.output.mkdir(parents=True,exist_ok=True)
manifest=a.source/'partitionline-broker/Cargo.toml'
env=os.environ.copy();env.update(CARGO_HOME='/workspace/work/cargo',RUSTUP_HOME='/workspace/work/rustup',PATH='/workspace/work/cargo/bin:'+env['PATH'],CARGO_INCREMENTAL='0',CARGO_BUILD_JOBS='1',CARGO_PROFILE_DEV_DEBUG='0',CARGO_PROFILE_TEST_DEBUG='0',CARGO_TARGET_DIR=str(a.target))
commands=[]
def run(name,argv,extra=None):
    command=['taskset','-c','0,1']+argv;began=time.time();log=a.output/(name+'.log');used=env.copy();used.update(extra or {})
    with log.open('wb') as stream:result=subprocess.run(command,cwd=a.source,env=used,stdout=stream,stderr=subprocess.STDOUT)
    commands.append({'name':name,'argv':command,'cwd':str(a.source),'source_sha':a.sha,'exit_code':result.returncode,'elapsed_seconds':time.time()-began,'log':log.name,'sha256':hashlib.sha256(log.read_bytes()).hexdigest()})
    (a.output/'commands.json').write_text(json.dumps({'source_sha':a.sha,'environment':{k:used[k] for k in ['CARGO_HOME','RUSTUP_HOME','CARGO_INCREMENTAL','CARGO_BUILD_JOBS','CARGO_PROFILE_DEV_DEBUG','CARGO_PROFILE_TEST_DEBUG','CARGO_TARGET_DIR']},'commands':commands},indent=2)+'\n')
    print(name,result.returncode,flush=True)
    if result.returncode:raise SystemExit(result.returncode)
for chain in ['stable','1.85.0']:
    run(chain+'-versions',['cargo','+'+chain,'--version'])
    run(chain+'-rustc',['rustc','+'+chain,'-vV'])
    run(chain+'-format',['cargo','+'+chain,'fmt','--manifest-path',str(manifest),'--','--check'])
    for features,flags in [('default',['--no-default-features']),('sasl',['--no-default-features','--features','sasl']),('all',['--all-features'])]:
        run(chain+'-'+features+'-test',['cargo','+'+chain,'test','--locked','--manifest-path',str(manifest)]+flags)
        run(chain+'-'+features+'-clippy',['cargo','+'+chain,'clippy','--locked','--manifest-path',str(manifest)]+flags+['--all-targets','--','-D','warnings'])
    run(chain+'-all-docs',['cargo','+'+chain,'doc','--locked','--manifest-path',str(manifest),'--all-features','--no-deps'],{'RUSTDOCFLAGS':'-D warnings'})

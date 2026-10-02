import os,sys,json,subprocess,re,time,hashlib
from pathlib import Path
base=Path('/workspace/work/produce-v13')
source=base/'final-source'
toolchain=sys.argv[1]
label='msrv' if toolchain=='1.85.0' else 'stable'
art=base/(sys.argv[2] if len(sys.argv)>2 else 'final-gates')/label;art.mkdir(parents=True,exist_ok=False)
target='/workspace/work/target-produce-v13-msrv' if label=='msrv' else '/workspace/work/target-produce-v13'
env=os.environ.copy();env.update(CARGO_HOME='/workspace/work/cargo',RUSTUP_HOME='/workspace/work/rustup',CARGO_INCREMENTAL='0',CARGO_TARGET_DIR=target,PARTITIONLINE_PRODUCE_V13_JARS='/workspace/work/broker-wire/jars',PARTITIONLINE_PRODUCE_V13_CLASSES=str(base/'final-java'))
env['PATH']='/workspace/work/cargo/bin:'+env['PATH']
if label=='msrv':env.update(CARGO_PROFILE_DEV_DEBUG='0',CARGO_PROFILE_TEST_DEBUG='0')
meta={'source_sha':'7ee1a405fed66a6bc2f600ffd5485eb7fe5a01bb','toolchain':toolchain,'cwd':str(source),'target_dir':target,'cpu_affinity':'0-2,4','commands':[]}
def retain(): (art/'summary.json').write_text(json.dumps(meta,indent=2)+'\n')
def run(name,args,docs=False):
    command=['taskset','-c','0-2,4']+args
    row={'name':name,'command':command,'status':'running','env':{k:env[k] for k in ('CARGO_HOME','RUSTUP_HOME','CARGO_INCREMENTAL','CARGO_TARGET_DIR','PARTITIONLINE_PRODUCE_V13_JARS','PARTITIONLINE_PRODUCE_V13_CLASSES')}}
    if label=='msrv':row['env'].update(CARGO_PROFILE_DEV_DEBUG='0',CARGO_PROFILE_TEST_DEBUG='0')
    local_env=env.copy()
    if docs:local_env['RUSTDOCFLAGS']='-D warnings';row['env']['RUSTDOCFLAGS']='-D warnings'
    meta['commands'].append(row);retain()
    log=art/(name+'.log')
    start=time.monotonic()
    with log.open('w') as out:result=subprocess.run(command,cwd=source,env=local_env,stdout=out,stderr=subprocess.STDOUT,check=False)
    row.update(status='passed' if result.returncode==0 else 'failed',exit_code=result.returncode,elapsed_seconds=time.monotonic()-start,log=str(log.relative_to(base)),log_sha256=hashlib.sha256(log.read_bytes()).hexdigest())
    text=log.read_text();row['suite_counts']=[dict(zip(('passed','failed','ignored','measured','filtered'),map(int,m))) for m in re.findall(r'test result: (?:ok|FAILED)\. (\d+) passed; (\d+) failed; (\d+) ignored; (\d+) measured; (\d+) filtered out',text)]
    if name=='java-live':row['actual_java_decode_processes']=text.count('OK: Produce13 Rust request/response')
    if name=='java-live' and row['actual_java_decode_processes']!=3:
        row.update(status='failed',exit_code=1,guard='exactly three actual Apache decode successes required');result.returncode=1
    retain();print(label,name,row['exit_code'],flush=True)
    if result.returncode:sys.exit(result.returncode)
prefix=['cargo','+'+toolchain]
run('rustc-version',['rustc','+'+toolchain,'--version','--verbose'])
run('fmt',prefix+['fmt','--all','--','--check'])
run('build-examples-default',prefix+['build','--locked','--examples','--jobs','2'])
run('tests-default',prefix+['test','--locked','--all-targets','--jobs','2'])
run('build-examples-all-features',prefix+['build','--locked','--examples','--all-features','--jobs','2'])
run('tests-all-features',prefix+['test','--locked','--all-targets','--all-features','--jobs','2'])
run('java-live',prefix+['test','--locked','--test','protocol_oracles','--all-features','produce_v13_rust_output_decodes','--jobs','2','--','--nocapture'])
run('metrics-lib',prefix+['test','--locked','--lib','metrics::tests','--jobs','2'])
run('credential-default',prefix+['test','--locked','--test','credential_redact','--jobs','2'])
run('credential-tracing',prefix+['test','--locked','--test','credential_redact','--features','tracing','--jobs','2'])
run('clippy',prefix+['clippy','--locked','--all-targets','--all-features','--jobs','2','--','-D','warnings'])
run('docs',prefix+['doc','--locked','--no-deps','--all-features','--jobs','2'],docs=True)
meta['status']='passed';retain()

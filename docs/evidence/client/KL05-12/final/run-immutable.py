#!/usr/bin/env python3
"""Source-pinned full client gates; every process and failed attempt retained."""
import hashlib,json,os,re,subprocess,time
from pathlib import Path

REPO=Path('/workspace/partitionline')
BASE=Path('/workspace/work/fetch-v18')
SHA='7bae3e34b5acedb6cac8b8232c9878f83fd7be30'
SOURCE=BASE/('source-'+SHA)
OUT=BASE/'immutable-attempt1'
SOURCE.mkdir(exist_ok=False);OUT.mkdir(exist_ok=False)
archive=BASE/('source-'+SHA+'.tar')
subprocess.run(['git','archive',SHA,'--output',str(archive)],cwd=REPO,check=True)
subprocess.run(['tar','-xf',str(archive),'-C',str(SOURCE)],check=True)
entries=subprocess.check_output(['git','ls-tree','-r','-z',SHA],cwd=REPO).split(b'\0')[:-1]
expected={}
for entry in entries:
    metadata,name=entry.split(b'\t',1);mode,kind,oid=metadata.decode().split()
    assert mode in ['100644','100755'] and kind=='blob'
    expected[name.decode()]=oid
def integrity():
    actual={str(p.relative_to(SOURCE)) for p in SOURCE.rglob('*') if p.is_file()}
    assert actual==expected.keys(),'source tree changed'
    for name,oid in expected.items():
        data=(SOURCE/name).read_bytes()
        assert hashlib.sha1(b'blob '+str(len(data)).encode()+b'\0'+data).hexdigest()==oid,name
integrity()
summary={'source_sha':SHA,'archive_sha256':hashlib.sha256(archive.read_bytes()).hexdigest(),
         'source_files':len(expected),'commands':[],'status':'running','profile':'client; broker WORK excluded'}
def save(): (OUT/'commands.json').write_text(json.dumps(summary,indent=2)+'\n')
save()
env=os.environ.copy();env.update(CARGO_HOME='/workspace/work/cargo',RUSTUP_HOME='/workspace/work/rustup',
    CARGO_INCREMENTAL='0',CARGO_BUILD_JOBS='1',CARGO_PROFILE_DEV_DEBUG='0',CARGO_PROFILE_TEST_DEBUG='0',
    RUSTDOCFLAGS='-D warnings')
env['PATH']='/workspace/work/cargo/bin:'+env['PATH']
def run(name,command,toolchain,extra=None):
    command=['taskset','-c','0-2,4']+command
    e=env.copy();e['CARGO_TARGET_DIR']=str(BASE/('target-'+toolchain))
    if extra:e.update(extra)
    started=time.time()
    with (OUT/(name+'.log')).open('w') as log:
        result=subprocess.run(command,cwd=SOURCE,env=e,stdout=log,stderr=subprocess.STDOUT,timeout=1800)
    integrity()
    text=(OUT/(name+'.log')).read_text()
    counts=[{'passed':int(a),'failed':int(b),'ignored':int(c)} for a,b,c in re.findall(r'test result: ok\. (\d+) passed; (\d+) failed; (\d+) ignored',text)]
    row={'name':name,'command':command,'toolchain':toolchain,'exit_code':result.returncode,
         'seconds':round(time.time()-started,3),'log':name+'.log','test_counts':counts,'source_unchanged':True}
    summary['commands'].append(row);save();print(name,result.returncode,counts,flush=True)
    if result.returncode:
        summary['status']='failed';save();raise SystemExit(result.returncode)
for toolchain in ['stable','1.85.0']:
    stem=toolchain+'-'
    run(stem+'rustc',['rustc','+'+toolchain,'--version','--verbose'],toolchain)
    run(stem+'fmt',['cargo','+'+toolchain,'fmt','--all','--','--check'],toolchain)
    for features in [[],['--all-features']]:
        lane='all' if features else 'default'
        run(stem+lane+'-examples',['cargo','+'+toolchain,'build','--offline','--locked','--examples','--jobs','1']+features,toolchain)
        run(stem+lane+'-tests',['cargo','+'+toolchain,'test','--offline','--locked','--all-targets','--jobs','1']+features,toolchain)
    run(stem+'fetch18-emitted',['cargo','+'+toolchain,'test','--offline','--locked','--all-features','--test','protocol_oracles','fetch_v18_rust_bodies_match','--jobs','1','--','--nocapture'],toolchain,
        {'PARTITIONLINE_FETCH18_RUST_DIR':str(OUT/(toolchain+'-rust-bodies'))})
    run(stem+'metrics',['cargo','+'+toolchain,'test','--offline','--locked','--lib','metrics::tests','--jobs','1'],toolchain)
    run(stem+'credential-default',['cargo','+'+toolchain,'test','--offline','--locked','--test','credential_redact','--jobs','1'],toolchain)
    run(stem+'credential-tracing',['cargo','+'+toolchain,'test','--offline','--locked','--test','credential_redact','--features','tracing','--jobs','1'],toolchain)
    run(stem+'strict',['cargo','+'+toolchain,'clippy','--offline','--locked','--all-targets','--all-features','--jobs','1','--','-D','warnings'],toolchain)
    run(stem+'docs',['cargo','+'+toolchain,'doc','--offline','--locked','--no-deps','--all-features','--jobs','1'],toolchain)
    run(stem+'doctests',['cargo','+'+toolchain,'test','--offline','--locked','--doc','--all-features','--jobs','1'],toolchain)
summary['status']='passed';save()
print('All immutable client gates passed; external Java and negative mutants run separately.',flush=True)

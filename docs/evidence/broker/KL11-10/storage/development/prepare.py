from pathlib import Path
import hashlib,json,subprocess,os,tarfile
repo=Path('/workspace/partitionline')
out=Path('/workspace/work/retention-storage')
proof=json.loads(Path('/workspace/work/segments-final-45582234/validation.json').read_text())
verified=[]
for lane in ['stable','1.85.0']:
 p=Path('/workspace/work/segments-final-45582234/bin')/lane/'fetch'
 h=hashlib.sha256(p.read_bytes()).hexdigest()
 expected=next(c['sha256'] for c in proof['retained_binaries'] if c['path'].endswith('/'+lane+'/fetch'))
 assert h==expected,(lane,h,expected)
 verified.append({'path':str(p),'sha256':h})
active=subprocess.check_output(['ps','-eo','pid,args'],text=True)
refs=[line for line in active.splitlines() if '/workspace/work/target-broker-segments' in line]
assert not refs,refs
env=os.environ.copy()
env.update(CARGO_HOME='/workspace/work/cargo',RUSTUP_HOME='/workspace/work/rustup',CARGO_TARGET_DIR='/workspace/work/target-broker-segments',CARGO_INCREMENTAL='0',CARGO_BUILD_JOBS='1',CARGO_PROFILE_DEV_DEBUG='0',CARGO_PROFILE_TEST_DEBUG='0')
env['PATH']='/workspace/work/cargo/bin:'+env['PATH']
cmd=['taskset','-c','0-2,4','cargo','+stable','clean','--manifest-path','partitionline-broker/Cargo.toml','--target-dir','/workspace/work/target-broker-segments']
with (out/'cache-clean.log').open('w') as f: code=subprocess.call(cmd,cwd='/workspace/work/segments-final-45582234/source',env=env,stdout=f,stderr=subprocess.STDOUT)
assert code==0
(out/'cache-release.json').write_text(json.dumps({'schema':1,'preserved_binaries':verified,'active_cache_refs':refs,'command':cmd,'exit_code':code,'source_archives_and_raw_logs_preserved':True},indent=2)+'\n')
base='81b490f310bb3dbe4e89e4e140ef658e29fdf9e7'
archive=out/'source-base.tar'
with archive.open('wb') as f: subprocess.run(['git','archive',base],cwd=repo,stdout=f,check=True)
source=out/'source'
source.mkdir()
with tarfile.open(archive) as t: t.extractall(source,filter='data')
files=['partitionline-broker/src/segments.rs','partitionline-broker/src/partition.rs','partitionline-broker/tests/segments.rs','partitionline-broker/tests/partition.rs']
for name in files: (source/name).write_bytes((repo/name).read_bytes())
(out/'development-source.json').write_text(json.dumps({'base_commit':base,'scope':'complete Git archive with four explicitly dirty claimed source/test overlays; not committed-source proof','overlays':[{'path':p,'sha256':hashlib.sha256((source/p).read_bytes()).hexdigest()} for p in files]},indent=2)+'\n')

from pathlib import Path
import os,json,subprocess,hashlib,time
root=Path('/workspace/work/retention-storage')
repo=Path('/workspace/partitionline')
source=root/'source'
files=['partitionline-broker/src/segments.rs','partitionline-broker/src/partition.rs','partitionline-broker/tests/segments.rs','partitionline-broker/tests/partition.rs']
for p in files: (source/p).write_bytes((repo/p).read_bytes())
identity=[{'path':p,'sha256':hashlib.sha256((source/p).read_bytes()).hexdigest()} for p in files]
env=os.environ.copy()
env.update(CARGO_HOME='/workspace/work/cargo',RUSTUP_HOME='/workspace/work/rustup',CARGO_TARGET_DIR='/workspace/work/target-broker-segments',CARGO_INCREMENTAL='0',CARGO_BUILD_JOBS='1',CARGO_PROFILE_DEV_DEBUG='0',CARGO_PROFILE_TEST_DEBUG='0')
env['PATH']='/workspace/work/cargo/bin:'+env['PATH']
name='development-'+str(time.time_ns())
cmd=['taskset','-c','0-2,4','cargo','+stable','test','--offline','--locked','--manifest-path','partitionline-broker/Cargo.toml','--test','segments','--test','partition','--','--test-threads=1','--nocapture']
start=time.monotonic()
with (root/(name+'.log')).open('w') as f: code=subprocess.call(cmd,cwd=source,env=env,stdout=f,stderr=subprocess.STDOUT)
(root/(name+'.json')).write_text(json.dumps({'schema':1,'base_commit':'81b490f310bb3dbe4e89e4e140ef658e29fdf9e7','dirty_four_file_overlay':identity,'command':cmd,'exit_code':code,'elapsed_seconds':time.monotonic()-start,'not_frozen_source_qualification':True},indent=2)+'\n')
print(name,code,flush=True)
raise SystemExit(code)

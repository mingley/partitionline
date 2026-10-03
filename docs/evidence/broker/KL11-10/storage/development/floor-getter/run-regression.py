from pathlib import Path
import os,json,hashlib,subprocess,sys
root=Path('/workspace/work/retention-storage'); repo=Path('/workspace/partitionline'); source=root/'source'
label=sys.argv[1]
out=repo/'docs/evidence/broker/KL11-10/storage/development/floor-getter'/label
out.mkdir(parents=True,exist_ok=False)
files=['partitionline-broker/src/segments.rs','partitionline-broker/src/partition.rs','partitionline-broker/tests/segments.rs','partitionline-broker/tests/partition.rs']
identity=[]
for name in files:
 data=(repo/name).read_bytes(); (source/name).write_bytes(data)
 p=out/'source'/name; p.parent.mkdir(parents=True,exist_ok=True); p.write_bytes(data)
 identity.append({'path':name,'sha256':hashlib.sha256(data).hexdigest()})
env=dict(os.environ,CARGO_HOME='/workspace/work/cargo',RUSTUP_HOME='/workspace/work/rustup',CARGO_TARGET_DIR='/workspace/work/target-broker-segments',CARGO_INCREMENTAL='0',CARGO_BUILD_JOBS='1',CARGO_PROFILE_DEV_DEBUG='0',CARGO_PROFILE_TEST_DEBUG='0')
env['PATH']='/workspace/work/cargo/bin:'+env['PATH']
cmd=['taskset','-c','0-2,4','cargo','+stable','test','--offline','--locked','--manifest-path','partitionline-broker/Cargo.toml','--lib','segments::tests::failed_floor_getter_reports_confirmed_state_and_reopen_never_decreases','--','--exact','--nocapture']
with (out/'command.log').open('w') as f: code=subprocess.call(cmd,cwd=source,env=env,stdout=f,stderr=subprocess.STDOUT)
(out/'validation.json').write_text(json.dumps({'schema':1,'label':label,'base_commit':'81b490f310bb3dbe4e89e4e140ef658e29fdf9e7','dirty_four_file_overlay':identity,'command':cmd,'exit_code':code,'scope':'development getter regression, complete baseline archive plus owned overlays'},indent=2)+'\n')
print(code,out,flush=True)
raise SystemExit(code)

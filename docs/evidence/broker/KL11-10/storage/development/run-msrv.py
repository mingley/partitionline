from pathlib import Path
import os,json,subprocess,hashlib,time
root=Path('/workspace/work/retention-storage')
repo=Path('/workspace/partitionline'); source=root/'source'
files=['partitionline-broker/src/segments.rs','partitionline-broker/src/partition.rs','partitionline-broker/tests/segments.rs','partitionline-broker/tests/partition.rs']
for p in files: (source/p).write_bytes((repo/p).read_bytes())
identity=[{'path':p,'sha256':hashlib.sha256((source/p).read_bytes()).hexdigest()} for p in files]
env=os.environ.copy(); env.update(CARGO_HOME='/workspace/work/cargo',RUSTUP_HOME='/workspace/work/rustup',CARGO_TARGET_DIR='/workspace/work/target-broker-segments',CARGO_INCREMENTAL='0',CARGO_BUILD_JOBS='1',CARGO_PROFILE_DEV_DEBUG='0',CARGO_PROFILE_TEST_DEBUG='0',RUSTDOCFLAGS='-D warnings')
env['PATH']='/workspace/work/cargo/bin:'+env['PATH']
out=root/('storage-msrv-'+str(time.time_ns())); out.mkdir()
env['PARTITIONLINE_RETENTION_FAULT_DIR']=str(out/'fault-states')
commands=[('clean-toolchain',['clean','--manifest-path','partitionline-broker/Cargo.toml','--target-dir','/workspace/work/target-broker-segments']),('integration',['test','--offline','--locked','--manifest-path','partitionline-broker/Cargo.toml','--test','segments','--test','partition','--','--test-threads=1','--nocapture']),('unit-faults',['test','--offline','--locked','--manifest-path','partitionline-broker/Cargo.toml','--lib','segments::tests','--','--test-threads=1','--nocapture']),('fmt',['fmt','--manifest-path','partitionline-broker/Cargo.toml','--check']),('clippy',['clippy','--offline','--locked','--manifest-path','partitionline-broker/Cargo.toml','--all-targets','--','-D','warnings']),('docs',['doc','--offline','--locked','--manifest-path','partitionline-broker/Cargo.toml','--no-deps'])]
results=[]
for name,args in commands:
 cmd=['taskset','-c','0-2,4','cargo','+1.85.0']+args
 with (out/(name+'.log')).open('w') as f: code=subprocess.call(cmd,cwd=source,env=env,stdout=f,stderr=subprocess.STDOUT)
 results.append({'name':name,'command':cmd,'exit_code':code})
 (out/'validation.json').write_text(json.dumps({'schema':1,'base_commit':'81b490f310bb3dbe4e89e4e140ef658e29fdf9e7','dirty_four_file_overlay':identity,'commands':results,'not_frozen_source_qualification':True},indent=2)+'\n')
 print(name,code,str(out),flush=True)
 if code: raise SystemExit(code)

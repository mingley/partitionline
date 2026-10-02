import subprocess,os,json,re,hashlib
from pathlib import Path
root=Path('/workspace/work/listener-profiles');source=root/'source';rows=[]
env=dict(os.environ,CARGO_HOME='/workspace/work/cargo',RUSTUP_HOME='/workspace/work/rustup',PATH='/workspace/work/cargo/bin:'+os.environ['PATH'],CARGO_INCREMENTAL='0',CARGO_BUILD_JOBS='1',CARGO_PROFILE_DEV_DEBUG='0',CARGO_PROFILE_TEST_DEBUG='0',CARGO_TARGET_DIR='/workspace/work/target-partition-store')
for tool in ['stable','1.85.0']:
 for name,tail in [('fmt',['fmt','--manifest-path','partitionline-broker/Cargo.toml','--','--check']),('protocol',['test','--offline','--locked','--manifest-path','partitionline-broker/Cargo.toml','--test','protocol']),('clippy',['clippy','--offline','--locked','--manifest-path','partitionline-broker/Cargo.toml','--lib','--','-D','warnings'])]:
  cmd=['taskset','-c','0-2,4','cargo','+'+tool,*tail];log=root/(tool+'-'+name+'.log')
  with log.open('wb') as f:r=subprocess.run(cmd,cwd=source,env=env,stdout=f,stderr=subprocess.STDOUT)
  rows.append({'toolchain':tool,'name':name,'argv':cmd,'exit_code':r.returncode,'log':log.name,'sha256':hashlib.sha256(log.read_bytes()).hexdigest(),'counts':re.findall(r'test result: ok. (\d+) passed; (\d+) failed; (\d+) ignored',log.read_text())});(root/'commands.json').write_text(json.dumps(rows,indent=2)+'\n');print(tool,name,r.returncode,flush=True)
  if r.returncode:raise SystemExit(r.returncode)

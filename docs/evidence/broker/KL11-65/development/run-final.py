import os,json,subprocess,time,re,hashlib
from pathlib import Path
repo=Path('/workspace/work/partition-store/final-6b4d330')
out=Path('/workspace/work/partition-store/final-results');out.mkdir(exist_ok=False)
env=dict(os.environ,CARGO_HOME='/workspace/work/cargo',RUSTUP_HOME='/workspace/work/rustup',PATH='/workspace/work/cargo/bin:'+os.environ['PATH'],CARGO_TARGET_DIR='/workspace/work/target-partition-store',CARGO_INCREMENTAL='0',CARGO_BUILD_JOBS='1',CARGO_PROFILE_DEV_DEBUG='0',CARGO_PROFILE_TEST_DEBUG='0')
rows=[]
for tool in ['stable','1.85.0']:
 proof=out/('proof-'+tool)
 cmds=[('clean',['cargo','+'+tool,'clean','--manifest-path','partitionline-broker/Cargo.toml','-p','partitionline-broker']),('default-tests',['cargo','+'+tool,'test','--offline','--locked','--manifest-path','partitionline-broker/Cargo.toml','--test','partition']),('all-feature-tests',['cargo','+'+tool,'test','--offline','--locked','--manifest-path','partitionline-broker/Cargo.toml','--test','partition','--all-features']),('fmt',['cargo','+'+tool,'fmt','--manifest-path','partitionline-broker/Cargo.toml','--','--check']),('strict-clippy',['cargo','+'+tool,'clippy','--offline','--locked','--manifest-path','partitionline-broker/Cargo.toml','--all-targets','--all-features','--','-D','warnings']),('strict-rustdoc',['cargo','+'+tool,'doc','--offline','--locked','--manifest-path','partitionline-broker/Cargo.toml','--all-features','--no-deps'])]
 for name,cmd in cmds:
  log=out/(tool+'-'+name+'.log');e=dict(env)
  if name=='default-tests':e['PL_PARTITION_PROOF_DIR']=str(proof)
  if name=='strict-rustdoc':e['RUSTDOCFLAGS']='-D warnings'
  print('RUN',tool,name,flush=True);start=time.monotonic()
  with log.open('w') as f:r=subprocess.run(cmd,cwd=repo,env=e,stdout=f,stderr=subprocess.STDOUT)
  row=dict(toolchain=tool,name=name,command=cmd,exit_code=r.returncode,elapsed_seconds=time.monotonic()-start,log=log.name)
  row['test_totals']=re.findall(r'test result: ok\. (\d+) passed; (\d+) failed; (\d+) ignored',log.read_text());rows.append(row)
  (out/'commands.json').write_text(json.dumps(dict(source_sha='6b4d3306fd517decbc01839ae25835c695de7572',commands=rows),indent=2)+'\n')
  print('RESULT',tool,name,r.returncode,flush=True)
  if r.returncode:raise SystemExit(r.returncode)

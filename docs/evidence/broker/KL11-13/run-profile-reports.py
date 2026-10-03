import hashlib,json,os,pathlib,subprocess,time
source=pathlib.Path('/workspace/work/raft-wire/final-source-04986a07/source')
source_sha='04986a07830a2e10f11946b3bd62c3359f462d90'
output=pathlib.Path('/workspace/work/raft-wire/profile-reports-04986a07'); output.mkdir(exist_ok=False)
integrity=json.loads((source.parent/'source-integrity.json').read_text()); assert integrity['source_sha']==source_sha
receipt={'source_sha':source_sha,'scope':'Focused fresh report emission after successful full gates; no source changes','commands':[]}
def verify():
 for name,digest in integrity['files_sha256'].items(): assert hashlib.sha256((source/name).read_bytes()).hexdigest()==digest,name
for toolchain,label in [('stable','stable'),('1.85.0','msrv')]:
 env=os.environ.copy();env.update(CARGO_HOME='/workspace/work/cargo',RUSTUP_HOME='/workspace/work/rustup',CARGO_INCREMENTAL='0',CARGO_PROFILE_DEV_DEBUG='0',CARGO_PROFILE_TEST_DEBUG='0',CARGO_TARGET_DIR='/workspace/work/target-raft-wire'+('-msrv' if label=='msrv' else ''));env['PATH']='/workspace/work/cargo/bin:'+env['PATH']
 for features in ('default','all-features'):
  cell=output/label/features;cell.mkdir(parents=True)
  for test,name,key in [('protocol','compiled_registry_and_all_apache_goldens','PARTITIONLINE_WIRE_REPORT'),('metadata','independent_apache_metadata_admin_goldens','PARTITIONLINE_METADATA_REPORT'),('produce','independent_apache_produce_goldens','PARTITIONLINE_PRODUCE_REPORT')]:
   verify();local=env.copy();local[key]=str(cell/(test+'-report.json'))
   command=['taskset','-c','0-2,4','cargo','+'+toolchain,'test','--locked','--manifest-path','partitionline-broker/Cargo.toml','--test',test]+(['--all-features'] if features=='all-features' else [])+['--jobs','1',name,'--','--exact','--nocapture']
   log=cell/(test+'.log');started=time.monotonic()
   with log.open('w') as stream:r=subprocess.run(command,cwd=source,env=local,stdout=stream,stderr=subprocess.STDOUT)
   row={'toolchain':toolchain,'features':features,'name':name,'command':command,'env':{k:local[k] for k in ('CARGO_HOME','RUSTUP_HOME','CARGO_INCREMENTAL','CARGO_PROFILE_DEV_DEBUG','CARGO_PROFILE_TEST_DEBUG','CARGO_TARGET_DIR',key)},'exit_code':r.returncode,'elapsed_seconds':time.monotonic()-started,'log':str(log.relative_to(output)),'log_sha256':hashlib.sha256(log.read_bytes()).hexdigest()}
   receipt['commands'].append(row);verify();row['all_tracked_source_files_unchanged']=True;(output/'results.json').write_text(json.dumps(receipt,indent=2)+'\n')
   print(json.dumps({k:row[k] for k in ('toolchain','features','name','exit_code')}),flush=True)
   if r.returncode:raise SystemExit(r.returncode)
receipt['status']='passed';(output/'results.json').write_text(json.dumps(receipt,indent=2)+'\n')

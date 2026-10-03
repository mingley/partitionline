import ast,gzip,hashlib,json,os,re,signal,subprocess,tarfile,time
from pathlib import Path
FAILED=Path('/workspace/work/raft-runtime-76/development/incoming-lifetime-candidate47a9-01')
DRIVER=Path('/workspace/work/raft-runtime-76/coverage-proposal-01/run-selective-candidate-03.py')
REPO=Path('/workspace/partitionline');BASE=Path('/workspace/work/broker-merged-source-ea9ff293');TREE=FAILED/'source'
OUT=FAILED/'retention-supplement-01';OUT.mkdir(exist_ok=False)
ENV=dict(os.environ,CARGO_HOME='/workspace/work/cargo',RUSTUP_HOME='/workspace/work/rustup',PATH='/workspace/work/cargo/bin:'+os.environ['PATH'],CARGO_INCREMENTAL='0',CARGO_BUILD_JOBS='1',CARGO_PROFILE_DEV_DEBUG='0',CARGO_PROFILE_TEST_DEBUG='0',CARGO_TARGET_DIR='/workspace/work/target-broker-segments',CARGO_NET_OFFLINE='true')
CACHE=Path(ENV['CARGO_TARGET_DIR']);DISK_FLOOR_BYTES=350*1024*1024;DISK_POLL_SECONDS=.2
os.sched_setaffinity(0,{2,4})
original=FAILED/'validation.json';original_raw=original.read_bytes();original_sha=hashlib.sha256(original_raw).hexdigest();failed=json.loads(original_raw)
assert all(row['exit_code']==0 for row in failed['commands']) and len(failed['commands'])==2
receipt={'schema_version':1,'task':'KL11-76','scope':'retention-only completion after real4testpass+helperfailure; no Cargo/test rerun or product install','original_failed_driver_receipt':{'path':str(original),'sha256':original_sha,'outcome':failed['runner_outcome']},'commands':[],'operations':[]}
elf_objects={};elf_snapshots={}
helper=Path('/workspace/work/raft-runtime-76/diagnostic-runner-02/guard-functions.py');provenance=json.loads(helper.with_name('guard-provenance.json').read_text());assert hashlib.sha256(helper.read_bytes()).hexdigest()==provenance['extracted_sha256'];exec(compile(helper.read_text(),str(helper),'exec'),globals())
# Definitions only: never import/run the driver top-level Cargo schedule.
tree=ast.parse(DRIVER.read_text());functions=[node for node in tree.body if isinstance(node,(ast.FunctionDef,ast.AsyncFunctionDef))];exec(compile(ast.Module(body=functions,type_ignores=[]),str(DRIVER),'exec'),globals())
plan=json.loads(Path('/workspace/work/raft-runtime-76/coverage-proposal-01/candidate-selective66-review-03-preparation/selective66-candidate-plan.json').read_text())
origin=json.loads(Path(plan['basis_origin']).read_text());manifest=Path(origin['source_manifest']['path']);assert sha256_file(manifest)==origin['source_manifest']['compressed_sha256'];baseline=json.loads(gzip.decompress(manifest.read_bytes()))
selected_pins=json.loads((FAILED/'selected-source-manifest.json').read_text())
prior_validation_pin=plan['prior_actual_experiment'];prior_path=Path(prior_validation_pin['validation']);assert sha256_file(prior_path)==prior_validation_pin['sha256'];prior=json.loads(prior_path.read_text());reference_cache_receipt=prior['whole_cache_retention'][-1];reference_cache_map_path=Path(reference_cache_receipt['map']);assert sha256_file(reference_cache_map_path)==reference_cache_receipt['map_sha256'];reference_cache_map=json.loads(reference_cache_map_path.read_text())
for obj in prior['retained_elf_objects']:
 obj=dict(obj);path=prior_path.parent/obj['path'];assert sha256_file(path)==obj['sha256'];obj['path']=str(path);elf_objects[obj['uncompressed_sha256']]=obj
for row in prior['commands'][-1]['post_command_elfs']:
 path=Path(row['original_path'])
 if path.is_file() and sha256_file(path)==row['sha256'] and path.stat().st_mode&0o7777==row['original_mode']:
  elf_snapshots[str(path)]={'identity':elf_identity(path.stat()),'sha256':row['sha256']}
receipt['before']=guards();receipt['cache_owners_before']=cache_owners();assert not receipt['cache_owners_before'];receipt['forecast']=combined_forecast();assert receipt['forecast']['sufficient'];validate_reference_archive()
receipt['all_current_elfs']=retain_cache('retention-only','preserve every current actual artifact after helper failure beforeanyoverwrite',force_verify=True)
log=FAILED/'candidate-four-regressions/command.log';text=log.read_text();receipt['actual_test_log']={'path':str(log),'sha256':sha256_file(log)}
assert 'running 4 tests' in text and 'test result: ok. 4 passed; 0 failed; 0 ignored;' in text
assert '--crate-name partitionline_broker' in text and '--crate-name raft_membership' in text and str(TREE/'partitionline-broker') in text
receipt['actual_four_tests']={'passed':4,'failed':0,'ignored':0,'filtered':27,'fresh_library_and_test_rustc_logged':True}
# Parse only the Cargo harness execution line, not rustc cfg parentheses.
matches=re.findall(r'^\s*Running `([^`]+)`\s*$',text,re.MULTILINE)
executed=[]
for line in matches:
 executable=line.split(' ',1)[0];path=Path(executable)
 if path.is_file() and path.is_relative_to(CACHE) and path.name.startswith('raft_membership-'):
  executed.append(retain_elf(path,'actual verboseCargo harness execution', 'retention-only',force_verify=True))
assert len(executed)==1;receipt['actual_executed_harness']=executed
receipt['generated_source_depfiles']=[{'path':str(path),'sha256':sha256_file(path),'relative_core_path':'src/raft/replication.rs','resolved_package_path':str(TREE/'partitionline-broker'),'binding':'actualverbose selectedpackage compilerheader plusforcedremovedrlib andexactguardedsource'}for path in CACHE.glob('debug/deps/partitionline_broker-*.d')if 'src/raft/replication.rs' in path.read_text()];assert receipt['generated_source_depfiles']
receipt['after_elf_retention']=guards();preserve_whole_cache('post-candidate-retention-only')
capture=FAILED/'captures';receipt['capture_files']={str(path.relative_to(capture)):{'sha256':sha256_file(path),'bytes':path.stat().st_size,'full07777':path.stat().st_mode&0o7777}for path in sorted(capture.rglob('*'))if path.is_file()}
receipt['final_source_guards']=guards();receipt['cache_owners_after']=cache_owners();assert not receipt['cache_owners_after'];assert original.read_bytes()==original_raw
receipt['original_failed_receipt_unchanged']=True;receipt['runner_outcome']='retention-only proof completed; original helper failure remains frozen; actual4candidate casespassed';receipt['free_bytes_final']=free_bytes();save();print('retention supplement',sha256_file(OUT/'validation.json'),'free',receipt['free_bytes_final'],flush=True)

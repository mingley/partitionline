#!/usr/bin/env python3
"""Read-only cache/source/retention checks after actual strict dry-run setup refusal."""
from pathlib import Path
import ast,gzip,hashlib,json,os,re,stat,time
BASE=Path('/workspace/work/client-capability-qa-preparation-e90efb49')
RUN=BASE/'baseline403-forced-04f6bc29-marker-v5-attempt-01'
TARGET=Path('/workspace/work/client-share-target')
RUNNER=BASE/'run-forced-baseline403-standard-marker-v5.py'
sha=lambda p:hashlib.sha256(p.read_bytes()).hexdigest()
assert sha(RUNNER)=='2c92b36731a1f482e45237972d7d99bdff79fc9ebdb1a52181ad28ce65160e9d'
ns={'Path':Path,'os':os,'stat':stat,'hashlib':hashlib}
tree=ast.parse(RUNNER.read_bytes())
selected=[n for n in tree.body if isinstance(n,ast.FunctionDef) and n.name in {'files','digest','source_guard','group_members'}]
exec(compile(ast.fix_missing_locations(ast.Module(body=selected,type_ignores=[])),'exact-frozen-readonly-functions','exec'),ns)
origins=[('/workspace/work/integration/client-capabilities-source-04f6bc29/receipt.json','5abaab9053b7b81d03a3c6ac161c4188f1f7a6ee909c71ecc69914224ed4ce29'),('/workspace/work/integration/broker-merged-source-403be1e3/receipt.json','d6e9a3578b31fe76ba8e764303a83cc615437744498e4874b37ef0c8af2d2fff')]
sources=[]
for name,expected_sha in origins:
 p=Path(name);assert sha(p)==expected_sha
 origin=json.loads(p.read_bytes());metadata=origin['source_manifest'];compressed=Path(metadata['path']).read_bytes()
 assert hashlib.sha256(compressed).hexdigest()==metadata['compressed_sha256']
 raw=gzip.decompress(compressed);assert hashlib.sha256(raw).hexdigest()==metadata['uncompressed_sha256'] and len(raw)==metadata['uncompressed_bytes']
 guard=ns['source_guard'](Path(origin['source_directory']),json.loads(raw))
 sources.append({'source_sha':origin['source_commit'],**guard})
marker=json.loads((RUN/'standard-cache-marker-repair.json').read_bytes())
expected=marker['new609_complete_cache_identity_map'];original=json.loads((BASE/'platform-daemon-exception-preparation-v2/current-complete-cache-hash-fullmode-map.json').read_bytes())
assert len(expected)==609 and set(expected)==set(original)|{'CACHEDIR.TAG'}
for name,row in original.items():
 for key in ['sha256','bytes','full_mode','mtime_ns']:assert expected[name][key]==row[key]
current=ns['files'](TARGET);assert current.keys()==expected.keys()
allocated=0;seen=set()
for name,row in expected.items():
 p=TARGET/name;st=current[name]
 assert stat.S_ISREG(st.st_mode) and sha(p)==row['sha256'] and st.st_size==row['bytes'] and stat.S_IMODE(st.st_mode)==row['full_mode'] and st.st_mtime_ns==row['mtime_ns'],name
 if (st.st_dev,st.st_ino) not in seen:allocated+=st.st_blocks*512;seen.add((st.st_dev,st.st_ino))
package=json.loads((BASE/'package-invalidation-proposal-after-baseline-setup/proposal.json').read_bytes())
package_rows=package['package_output_restore_map'];assert len(package_rows)==25
for row in package_rows:
 p=TARGET/row['target_relative_path'];data=p.read_bytes();compressed=Path(row['gzip_path']).read_bytes()
 assert hashlib.sha256(data).hexdigest()==row['sha256'] and gzip.decompress(compressed)==data
validation=json.loads((RUN/'validation.json').read_bytes());assert not validation['passed'] and len(validation['commands'])==3 and validation['baseline_library_proof'] is None
expected_names=['platform-docker-before-clean','platform-docker-after-marker-before-clean','stable-own-package-clean-dry-run']
assert [r['name'] for r in validation['commands']]==expected_names
command_summaries=[]
for row in validation['commands']:
 assert row['exit_code']==0 and row['trigger'] is None
 for key in ['stdout','stderr']:
  assert sha(Path(row[key+'_path']))==row[key+'_sha256']
 assert sha(Path(row['disk_monitor_path']))==row['disk_monitor_sha256']
 cleanup=row['owned_group_cleanup'];assert cleanup['no_live_group_members']
 group=cleanup['owned_process_group'];members=ns['group_members'](group);assert not any(m['state']!='Z' for m in members)
 command_summaries.append({'name':row['name'],'exit_code':row['exit_code'],'owned_process_group':group,'no_live_group_members':True,'minimum_sampled_free_bytes':row['minimum_sampled_free_bytes']})
 for collection in ['cache_elfs_before','cache_elfs_after']:
  for elf in row.get(collection,[]):
   obj=Path(elf['path']).read_bytes();assert hashlib.sha256(obj).hexdigest()==elf['sha256'];data=gzip.decompress(obj)
   assert len(data)==elf['uncompressed_bytes'] and hashlib.sha256(data).hexdigest()==elf['uncompressed_sha256']
reported=[];expanded=set()
for key in ['stdout_path','stderr_path']:
 for rawline in Path(validation['commands'][-1][key]).read_text().splitlines():
  line=rawline.strip()
  if str(TARGET) not in line:continue
  match=re.fullmatch(r'(?:Removing\s+)?(`?)('+re.escape(str(TARGET))+r'(?:/[^`\r\n]+)?)(`?)',line)
  assert match is not None and match.group(1)==match.group(3)
  relative=Path(match.group(2)).relative_to(TARGET).as_posix();reported.append(relative)
  if relative in expected:expanded.add(relative)
  else:expanded.update(name for name in expected if name.startswith(relative.rstrip('/')+'/'))
package_names={row['target_relative_path'] for row in package_rows};assert len(expanded)==16 and expanded < package_names
assert not (BASE/'baseline403-forced-source-04f6bc29-marker-v5-attempt-01').exists()
assert not (TARGET/'.partitionline-cache-tag-04f6bc29-marker-v5-attempt-01.tmp').exists()
fd=os.statvfs('/workspace');free=fd.f_bavail*fd.f_frsize
result={'schema_version':1,'scope':'actual177B standard cache marker repair and Cargo dry-run pathset refusal; no compiler/API27/API90 behavior qualification',
 'sampled_at_unix':time.time(),'source_sha':'04f6bc2968c1d721c6815a6389897a62e4ca76f1','runner_sha256':sha(RUNNER),'validation_sha256':sha(RUN/'validation.json'),
 'complete_sources_after_failure':sources,'cache_pathset_count':609,'original608_cache_hashes_bytes_fullmodes_mtimes_unchanged':True,
 'new_standard177B_cache_marker_exact':True,'only_intentional_cache_change':marker['new_marker'],'complete609_after_dryrun_hashes_bytes_fullmodes_mtimes_unchanged':True,
 'all25_own_package_outputs_exact_and_losslessly_retained':True,'cache_allocated_bytes':allocated,
 'actual_Cargo_dryrun_exit_code':0,'actual_reported_paths':reported,'actual_expanded_regular_paths':sorted(expanded),'actual_expanded_regular_path_count':len(expanded),
 'reviewed_package_path_count':25,'prospective_missing_reviewed_package_paths':sorted(package_names-expanded),'prospective_unreviewed_paths':sorted(expanded-package_names),
 'actual_Cargo_summary':Path(validation['commands'][-1]['stderr_path']).read_text().strip(),
 'actual_package_removals':0,'actual_compiler_invocations':0,'actual_test_executions':0,'actual_nm_invocations':0,
 'actual_before_and_after_marker_Docker_API_empty_exit0':True,'actual_after_clean_Docker_API_not_launched':True,
 'all_owned_command_groups_joined_no_live_members':True,'commands':command_summaries,'selected_baseline_source_materialized':False,
 'source_mutations':0,'cache_metadata_mutations':1,'free_bytes':free,'lease_closed_after_readonly_verification':True,
 'next_phase':'held; ROOT review of actual dry-run incomplete report required; no automatic cleanup/retry/relaxation',
 'verifier_sha256':sha(Path(__file__))}
p=RUN/'post-failure-source-cache-guard.json';assert not p.exists();p.write_text(json.dumps(result,indent=2)+'\n');p.chmod(0o600)
print(json.dumps({'guard_sha256':sha(p),'source_count':len(sources),'cache_count':len(current),'free_bytes':free,'actual_compiles':0,'actual_tests':0,'closed_groups':command_summaries}))

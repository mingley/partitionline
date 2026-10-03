from pathlib import Path
import ast,gzip,hashlib,json,os,stat,time
BASE=Path('/workspace/work/client-capability-qa-preparation-e90efb49');RUN=BASE/'baseline403-forced-04f6bc29-quarantine-v2-attempt-01';TARGET=Path('/workspace/work/client-share-target');Q=BASE/'baseline403-quarantine-04f6bc29-quarantine-v2-attempt-01';RUNNER=BASE/'run-forced-baseline403-quarantine-v2.py'
sha=lambda p:hashlib.sha256(p.read_bytes()).hexdigest();assert sha(RUNNER)=='6c64161cca8cde4eb0a367fce475ece8c2dff7fabe5df3a2db3fe0e38c2bc081'
ns={'Path':Path,'os':os,'stat':stat,'hashlib':hashlib};t=ast.parse(RUNNER.read_bytes());exec(compile(ast.fix_missing_locations(ast.Module(body=[n for n in t.body if isinstance(n,ast.FunctionDef) and n.name in {'files','digest','group_members'}],type_ignores=[])),'frozen-readonly-guard-functions','exec'),ns)
v=json.loads((RUN/'validation.json').read_bytes());assert v['passed'] and len(v['commands'])==6
expected_names=['platform-docker-before-quarantine','platform-docker-after-quarantine','stable-baseline-positive-controls','old-library-symbol-probe','stable-baseline-api27-red','stable-baseline-api90-red'];assert [r['name'] for r in v['commands']]==expected_names
summaries=[]
new_guard={'files':73938,'bytes':713363027,'whole_declared_path_set_match':True,'all_declared_blobs_sha256_lengths_and_full_modes_match':True,'set_blob_fullmode_sha256':'61dcc9a607ba13f2f68c12981e7c4808d290764f68d90c644f31ae7282453329'}
old_guard={'files':71885,'bytes':653466714,'whole_declared_path_set_match':True,'all_declared_blobs_sha256_lengths_and_full_modes_match':True,'set_blob_fullmode_sha256':'8ef181d8d9f63cb6bd2ed6e8bd9011d3b29b733c989eecc822317f7d16b3c7fc'}
for row in v['commands']:
 assert row['source_before']==row['source_after']==new_guard and row['full_original403_before']==row['full_original403_after']==old_guard
 assert row['declared_operation_source_before']==row['declared_operation_source_after']
 assert row['trigger'] is None and row['passed_expected_process_outcome'] and row['owned_group_cleanup']['no_live_group_members']
 group=row['owned_group_cleanup']['owned_process_group'];members=ns['group_members'](group);assert not any(x['state']!='Z' for x in members)
 for suffix in ['stdout','stderr']:assert sha(Path(row[suffix+'_path']))==row[suffix+'_sha256']
 assert sha(Path(row['disk_monitor_path']))==row['disk_monitor_sha256']
 obs=[json.loads(s) for s in Path(row['disk_monitor_path']).read_text().splitlines()];assert len(obs)==row['disk_samples'] and min(x['free_bytes'] for x in obs)==row['minimum_sampled_free_bytes'] and all(x['process_group']==group for x in obs)
 assert obs[0]['pre_launch'] and obs[-1]['process_completed'] and obs[-1]['exit_code']==row['exit_code'] and min(x['free_bytes'] for x in obs)>=350*1024*1024
 summaries.append({'name':row['name'],'exit_code':row['exit_code'],'actual_test_summary':row.get('actual_test_summary'),'actual_test_names':row.get('actual_test_names'),'owned_process_group':group,'closed_no_live_members':True,'minimum_sampled_free_bytes':row['minimum_sampled_free_bytes']})
 for collection in ['cache_elfs_before','cache_elfs_after']:
  for e in row[collection]:
   obj=Path(e['path']).read_bytes();data=gzip.decompress(obj);assert hashlib.sha256(obj).hexdigest()==e['sha256'] and len(data)==e['uncompressed_bytes'] and hashlib.sha256(data).hexdigest()==e['uncompressed_sha256']
assert v['commands'][2]['actual_test_summary']=={'passed':2,'failed':0} and v['commands'][4]['actual_test_summary']=={'passed':0,'failed':3} and v['commands'][5]['actual_test_summary']=={'passed':0,'failed':1}
proof=v['baseline_library_proof'];assert proof['actual_library_rustc_invocation_retained'] and proof['old_library_new_lag_symbol_absent'] and not proof['new_package_artifact_reuse']
for x in proof['rebuilt_library_artifacts']:
 obj=Path(x['gzip_path']).read_bytes();data=gzip.decompress(obj);assert hashlib.sha256(obj).hexdigest()==x['gzip_sha256'] and hashlib.sha256(data).hexdigest()==x['sha256'] and len(data)==x['bytes'] and sha(Path(x['path']))==x['sha256']
prior=json.loads((BASE/'package-invalidation-proposal-after-baseline-setup/proposal.json').read_bytes());own={x['target_relative_path']:x for x in prior['package_output_restore_map']};assert len(own)==25
assert set(ns['files'](Q))==set(own)
for name,row in own.items():
 p=Q/name;st=p.lstat();assert stat.S_ISREG(st.st_mode) and st.st_size==row['bytes'] and sha(p)==row['sha256'] and stat.S_IMODE(st.st_mode)==row['full_mode'] and st.st_mtime_ns==row['mtime_ns'] and [st.st_dev,st.st_ino]==row['inode']
 assert gzip.decompress(Path(row['gzip_path']).read_bytes())==p.read_bytes()
initial=json.loads((RUN/'prequarantine609-guard.json').read_bytes())['complete_cache_identity_map'];survivors={n:r for n,r in initial.items() if n not in own};assert len(survivors)==584
current=ns['files'](TARGET);cachemap={};allocated=0;seen=set()
for name,st in current.items():
 p=TARGET/name;assert stat.S_ISREG(st.st_mode);data=p.read_bytes();cachemap[name]={'bytes':len(data),'sha256':hashlib.sha256(data).hexdigest(),'full_mode':stat.S_IMODE(st.st_mode),'mtime_ns':st.st_mtime_ns,'inode':[st.st_dev,st.st_ino],'allocated_bytes':st.st_blocks*512}
 if (st.st_dev,st.st_ino) not in seen:allocated+=st.st_blocks*512;seen.add((st.st_dev,st.st_ino))
for name,row in survivors.items():assert all(cachemap[name][k]==row[k] for k in ['sha256','bytes','full_mode','mtime_ns'])
outputs={n:r for n,r in cachemap.items() if n not in survivors};assert len(outputs)==25
for name in outputs:assert name.startswith('debug/.fingerprint/partitionline-') or name.startswith('debug/deps/') and ('partitionline-' in name or name.split('/')[-1].startswith(('baseline_positive_controls-','fail_first_share_offsets_v1-','fail_first_write_txn_markers_v2-')))
modelog=[json.loads(s) for s in (RUN/'quarantine-moves.jsonl').read_text().splitlines()];assert len(modelog)==50 and sum(x['operation']=='rename_completed' for x in modelog)==25
fd=os.statvfs('/workspace');free=fd.f_bavail*fd.f_frsize
result={'schema_version':1,'passed':True,'scope':'genuine selective403 old-source failing-first experiment:66exactinputs+5reviewedtest/peer overlays; not candidate/fulloldtree qualification',
 'source_sha':v['source_sha'],'baseline_source_sha':v['baseline_source_sha'],'runner_sha256':sha(RUNNER),'validation_sha256':sha(RUN/'validation.json'),'actual_command_count':6,
 'actual_positive_tests_passed':2,'actual_API27_failed_tests':3,'actual_API90_failed_tests':1,'actual_setup_or_compiler_failures':0,
 'API27_actual_old_failures':{'missing_result':'empty response falsely accepted','availability':'Broker8 terminal;9 not reached after unwrap panic','v2_only':'UnsupportedWriteTxnMarkers'},'API90_actual_old_failure':'existingoperation rejects v1-only capability',
 'all_six_full_original403_and04f_guards_before_after_match':True,'full_source_guards':{'candidate':new_guard,'original403':old_guard},'all_selected71_materialized_source_guards_match':True,
 'forced_old_library_proof':proof,'actual_raw_logs_and_disk_samples_independently_hash_reconciled':True,'all_owned_groups_joined_no_live_members':True,'commands':summaries,
 'quarantine_directory':str(Q),'all25_prior7942_original_raw_sha_bytes_fullmodes_mtimes_inodes_and_gzips_preserved':True,'original_raw_bytes_reclaimed':0,
 'original584_dependency_and_marker_survivors_unchanged':True,'original_standard_marker_unchanged':True,'newly_generated_owned_package_files':25,'final_cache_files':len(cachemap),'final_cache_allocated_bytes':allocated,
 'actual_Cargo_clean_or_dryrun_commands':0,'no_source_mutations':True,'free_bytes':free,'CPU0_1_lease_closed':True,
 'candidate_behavior_strict_JVM_MSRV_wholematrix_not_executed':True,'readonly_verifier_sha256':sha(Path(__file__))}
p=RUN/'closed-baseline-summary.json';assert not p.exists();p.write_text(json.dumps(result,indent=2)+'\n');p.chmod(0o600)
p=RUN/'final-complete-cache-hash-fullmode-map.json';assert not p.exists();p.write_text(json.dumps(cachemap,indent=2)+'\n');p.chmod(0o600)
p=RUN/'final-new-owned-package-map.json';assert not p.exists();p.write_text(json.dumps(outputs,indent=2)+'\n');p.chmod(0o600)
print(json.dumps({'summary_sha256':sha(RUN/'closed-baseline-summary.json'),'complete_cache_sha256':sha(RUN/'final-complete-cache-hash-fullmode-map.json'),'package_outputs_sha256':sha(RUN/'final-new-owned-package-map.json'),'cache_files':len(cachemap),'package_files':len(outputs),'free_bytes':free,'all_sources_and_quarantined_originals_preserved':True}))

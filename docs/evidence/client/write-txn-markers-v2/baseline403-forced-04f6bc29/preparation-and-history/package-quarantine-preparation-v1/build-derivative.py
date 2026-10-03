from pathlib import Path
import hashlib,ast,difflib
base=Path('/workspace/work/client-capability-qa-preparation-e90efb49');prep=base/'package-quarantine-preparation-v1'
old=base/'run-forced-baseline403-standard-marker-v5.py';raw=old.read_text();assert hashlib.sha256(old.read_bytes()).hexdigest()=='2c92b36731a1f482e45237972d7d99bdff79fc9ebdb1a52181ad28ce65160e9d'
def change(a,b):
 global raw
 assert raw.count(a)==1,(a[:100],raw.count(a));raw=raw.replace(a,b)
change("CLEAN_DONE=False","INVALIDATION_DONE=False")
change("LAST_FORECAST_KIND='compile'","LAST_FORECAST_KIND='compile'")
change("BASELINE=BASE/('baseline403-forced-source-'+ARGS.run_label)","BASELINE=BASE/('baseline403-forced-source-'+ARGS.run_label)\nQUARANTINE=BASE/('baseline403-quarantine-'+ARGS.run_label)")
change("assert len(CACHE_TAG_BYTES)==177 and CACHE_TAG_BYTES.startswith(b'Signature: 8a477f597d28d172789f06886806bc55\\n')","assert len(CACHE_TAG_BYTES)==177 and CACHE_TAG_BYTES.startswith(b'Signature: 8a477f597d28d172789f06886806bc55\\n')\nMARKER_RECEIPT_PATH=BASE/'baseline403-forced-04f6bc29-marker-v5-attempt-01/standard-cache-marker-repair.json'\nMARKER_RECEIPT_BYTES=MARKER_RECEIPT_PATH.read_bytes()\nassert hashlib.sha256(MARKER_RECEIPT_BYTES).hexdigest()=='caeb86a5f72cec3de50e68d39b59218c678c5280c2775e0697837ad880987514'\nEXISTING_MARKER_IDENTITY=json.loads(MARKER_RECEIPT_BYTES)['new609_complete_cache_identity_map']['CACHEDIR.TAG']")
change("if LAST_FORECAST_KIND=='clean':","if LAST_FORECAST_KIND=='quarantine':")
change("# A clean does not promise reclaimed blocks; only the immediately fresh\n        # post-clean compile forecast admits the compiler launch.","# Moving raw originals retains every allocated byte. Only a fresh\n        # post-quarantine sample admits compilation; projected reclaim is zero.")
change("'postclean_compile_required_free_bytes':needed+ROOT_PUBLICATION_RESERVE,","'postquarantine_compile_required_free_bytes':needed+ROOT_PUBLICATION_RESERVE,")
change("'postclean_compile_base_without_publication_reserve_bytes':needed,","'postquarantine_compile_base_without_publication_reserve_bytes':needed,")
change("'expected_clean_reclaim_bytes_for_projection_only':89194496,","'expected_quarantine_reclaim_bytes':0,")
change("'actual_zero_running_Docker_before_after_cleanup_required':True,","'actual_zero_running_Docker_before_after_quarantine_required':True,")
change("'actual_standard_cache_marker_repair_and_exact25_dryrun_required':True,","'already_present_standard_cache_marker_verified':True,\n           'actual_exact25_atomicrename_quarantine_required':True,\n           'actual_Cargo_clean_or_dryrun_commands':0,\n           'expected_actual_command_count':6,\n           'quarantine_directory':str(QUARANTINE),")
change("'original_cache_inventory608_marker_added609_disclosed':True,","'original_cache_inventory608_plus_previous_177B_marker609_disclosed':True,\n           'previous_marker_repair_receipt_sha256':digest(MARKER_RECEIPT_BYTES),")
change("assert current.keys()==PACKAGE_MAP['complete_cache_file_inventory'].keys(), 'cache pathset changed since retention review'","assert set(current)==set(FULL_CACHE_REFERENCE)|{'CACHEDIR.TAG'} and len(current)==609, 'cache pathset changed from reviewed609'")
change("original=PACKAGE_MAP['complete_cache_file_inventory'][name]\n        assert len(data)==original['bytes'] and mode==original['full_mode']\n        frozen=FULL_CACHE_REFERENCE[name]","original=PACKAGE_MAP['complete_cache_file_inventory'].get(name,EXISTING_MARKER_IDENTITY)\n        assert len(data)==original['bytes'] and mode==original['full_mode']\n        frozen=FULL_CACHE_REFERENCE.get(name,EXISTING_MARKER_IDENTITY)")
# No unused repair/dry-run helper remains in the derivative.
start=raw.index('def repair_standard_owned_cache_marker(');end=raw.index('def prove_forced_old_library(',start)
raw=raw[:start]+(prep/'quarantine-functions.py').read_text()+'\n'+raw[end:]
change('assert CLEAN_DONE','assert INVALIDATION_DONE')
start=raw.index('try:\n    LAST_FORECAST_KIND=');end=raw.index('    materialize_baseline()',start)
new="""try:
    LAST_FORECAST_KIND='quarantine';initial_forecast=forecast()
    assert initial_forecast['sampled_free_bytes']>=initial_forecast['postquarantine_compile_required_free_bytes'], 'pre-quarantine actual free insufficient; raw files reclaim zero'
    (RUN/'launch-contract.json').write_text(json.dumps({'schema_version':1,'candidate_source_sha':PLAN['source_sha'],
        'origin_receipt_sha256':digest(ORIGIN_BYTES),'forecast':initial_forecast,
        'platform_exception_sha256':digest(PLATFORM_ALLOW_BYTES),'root_external_publication_reserve_bytes':ROOT_PUBLICATION_RESERVE,
        'baseline_source_sha':BASELINE_INPUTS['source_sha'],'baseline_selected_inputs':66,'explicit_overlay_count':5,
        'expected_actual_behavior':'positive2 passes then exact3+1 old behavior failures; never compile/setup failures',
        'root_authorized_scope':'exact25 reversible quarantine then forced selective403 baseline once ROOT GO; candidate phases held',
        'expected_actual_subprocesses':6,'actual_Cargo_clean_or_dryrun_commands':0,'quarantine_reclaimed_bytes':0},indent=2)+'\\n')
    seed_retention()
    docker_zero_running_workloads('platform-docker-before-quarantine')
    checked_prequarantine=guard_package_outputs_and_processes()
    (RUN/'prequarantine609-guard.json').write_text(json.dumps(checked_prequarantine,indent=2)+'\\n')
    quarantine_own_package_outputs(checked_prequarantine)
    docker_zero_running_workloads('platform-docker-after-quarantine')
    postquarantine_process_guard=process_reference_guard()
    (RUN/'postquarantine-relevant-process-guard.json').write_text(json.dumps(postquarantine_process_guard,indent=2)+'\\n')
    survivor_map={name:row for name,row in checked_prequarantine['complete_cache_identity_map'].items() if name not in INITIAL_PACKAGE_IDENTITIES}
    verify_complete_cache_map(survivor_map)
    locations=quarantine_location_snapshot(list(INITIAL_PACKAGE_IDENTITIES))
    assert locations['all25_present_exactly_once_and_intact'], 'quarantine changed before old compiler'
    assert set(files(QUARANTINE))==set(INITIAL_PACKAGE_IDENTITIES)
    (RUN/'postquarantine-before-compiler-guard.json').write_text(json.dumps({'survivor584_guard':verify_complete_cache_map(survivor_map),'quarantine25_locations':locations},indent=2)+'\\n')
    INVALIDATION_DONE=True;LAST_FORECAST_KIND='compile';forecast()
"""
raw=raw[:start]+new+raw[end:]
change("'forced-compilation proof.","'forced-compilation proof.") if False else None
assert 'CLEAN_DONE' not in raw and 'postclean' not in raw and 'clean_argv' not in raw
ast.parse(raw)
p=base/'run-forced-baseline403-quarantine-v1.py';assert not p.exists();p.write_text(raw);p.chmod(0o600)
patch=prep/'runner.proposed.patch';patch.write_text(''.join(difflib.unified_diff(old.read_text().splitlines(True),raw.splitlines(True),fromfile=str(old),tofile=str(p))));patch.chmod(0o600)
for p in [prep/'quarantine-functions.py',prep/'build-derivative.py']:p.chmod(0o600)
print(hashlib.sha256(p.read_bytes()).hexdigest() if False else hashlib.sha256((base/'run-forced-baseline403-quarantine-v1.py').read_bytes()).hexdigest())

#!/usr/bin/env python3
"""Read-only source/cache preparation guard; no daemon API, cleanup, or compiler."""
from pathlib import Path
import ast,gzip,hashlib,json,os,stat,time
BASE=Path('/workspace/work/client-capability-qa-preparation-e90efb49');PREP=BASE/'platform-daemon-exception-preparation'
RUNNER=BASE/'run-forced-baseline403-platform-exception.py';raw=RUNNER.read_bytes()
assert hashlib.sha256(raw).hexdigest()=='e0919e0a61cee3d304bff3d822d1bc54c41e412d92f23f7b288951fa3150ba90'
function_names={'digest','files','source_guard','verify_platform_daemons','verify_docker_cli'}
module=ast.Module(body=[n for n in ast.parse(raw).body if isinstance(n,ast.FunctionDef) and n.name in function_names],type_ignores=[])
allowraw=(PREP/'exact-platform-daemons.json').read_bytes();assert hashlib.sha256(allowraw).hexdigest()=='5b225336055754c792fecb6f2ce24364a6330fb93f728011ebb78ab53bd51ebd'
ns={'Path':Path,'hashlib':hashlib,'os':os,'stat':stat,'PLATFORM_ALLOW':json.loads(allowraw)}
exec(compile(ast.fix_missing_locations(module),str(RUNNER),'exec'),ns)
free=lambda:os.statvfs('/workspace').f_bavail*os.statvfs('/workspace').f_frsize
free_before=free()
sources=[]
for pin,originpath,originsha in [('04f6bc2968c1d721c6815a6389897a62e4ca76f1','/workspace/work/integration/client-capabilities-source-04f6bc29/receipt.json','5abaab9053b7b81d03a3c6ac161c4188f1f7a6ee909c71ecc69914224ed4ce29'),('403be1e3db073df86921d6fb21189f695c4f1eaf','/workspace/work/integration/broker-merged-source-403be1e3/receipt.json','d6e9a3578b31fe76ba8e764303a83cc615437744498e4874b37ef0c8af2d2fff')]:
    originraw=Path(originpath).read_bytes();assert ns['digest'](originraw)==originsha
    origin=json.loads(originraw);assert origin['source_commit']==pin
    compressed=Path(origin['source_manifest']['path']).read_bytes();assert ns['digest'](compressed)==origin['source_manifest']['compressed_sha256']
    data=gzip.decompress(compressed);assert ns['digest'](data)==origin['source_manifest']['uncompressed_sha256'];assert len(data)==origin['source_manifest']['uncompressed_bytes']
    manifest=json.loads(data);root=Path(origin['source_directory']);guard=ns['source_guard'](root,manifest)
    sources.append({'source_sha':pin,'origin_receipt':originpath,'origin_receipt_sha256':originsha,'guard':guard})
packagepath=BASE/'package-invalidation-proposal-after-baseline-setup/proposal.json';packageraw=packagepath.read_bytes();assert ns['digest'](packageraw)=='791e813056e1e8aafb0f2a8af17b217d86864369b06192780c7176ab9adac528'
package=json.loads(packageraw);target=Path(package['target_directory']);current=ns['files'](target)
assert current.keys()==package['complete_cache_file_inventory'].keys()
cache={};allocated=0;seen=set()
for name,st in sorted(current.items()):
    assert stat.S_ISREG(st.st_mode)
    old=package['complete_cache_file_inventory'][name];data=(target/name).read_bytes();mode=stat.S_IMODE(st.st_mode)
    assert len(data)==old['bytes'] and mode==old['full_mode'] and st.st_mtime_ns==old['mtime_ns']
    cache[name]={'sha256':ns['digest'](data),'bytes':len(data),'full_mode':mode,'mtime_ns':st.st_mtime_ns,'inode':[st.st_dev,st.st_ino],'allocated_bytes':st.st_blocks*512}
    if (st.st_dev,st.st_ino) not in seen:allocated+=st.st_blocks*512;seen.add((st.st_dev,st.st_ino))
package_allocated=0
for row in package['package_output_restore_map']:
    actual=cache[row['target_relative_path']];assert actual['sha256']==row['sha256'] and actual['bytes']==row['bytes'] and actual['full_mode']==row['full_mode'] and actual['mtime_ns']==row['mtime_ns']
    compressed=Path(row['gzip_path']).read_bytes();assert ns['digest'](compressed)==row['gzip_sha256'];data=gzip.decompress(compressed);assert ns['digest'](data)==row['sha256'] and len(data)==row['bytes']
    package_allocated+=actual['allocated_bytes']
assert len(package['package_output_restore_map'])==25 and package_allocated==89194496
identity=ns['verify_platform_daemons']();cli=ns['verify_docker_cli']()
for name,sha in [('validation.json','300a346df5de9775c517bf04f35948949d948944c2438efc776c7455538b49b0'),('process-reference-guard.json','37821c32c9eeb817f3dc03d97076848d44ecd90f48dbbea3f6ac8ddfe0ccca79'),('failure.json','722db4da6f686d91a50bc7aa40df8dbb7c544ed75a4efa147108ac056e2fd5d5'),('no-cargo-source-cache-after-block.json','65a7fcf7f533e2c0a885bef633cdb7fed872dbeb90f2e26caeb63c9b6fe19af8')]:
    assert ns['digest']((BASE/'baseline403-forced-04f6bc29-attempt-01'/name).read_bytes())==sha
cachemap=PREP/'current-complete-cache-hash-fullmode-map.json';cachemap.write_text(json.dumps(cache,indent=2)+'\n');cachemap.chmod(0o600)
free_after=free();base_required=749371137;publication_reserve=32*1024*1024;required=base_required+publication_reserve;projected=free_after+package_allocated
result={'schema_version':1,'scope':'WORK-only read-only full source/cache/daemon-identity preparation; no Docker API/Cargo/cleanup/API behavioral qualification',
    'sampled_at_unix':time.time(),'runner_sha256':ns['digest'](raw),'verifier_sha256':ns['digest'](Path(__file__).read_bytes()),'complete_sources':sources,
    'all25_package_outputs_exact_bytes_sha256_fullmodes_mtimes_and_lossless_gzip_verified':True,'cache_pathset_count':len(cache),'cache_allocated_bytes':allocated,
    'dependency_frozen_inventory_sizes_fullmodes_mtimes_match':True,'dependency_historical_content_hashes_available':False,
    'current_complete_cache_sha256_map':{'path':str(cachemap),'sha256':ns['digest'](cachemap.read_bytes()),'full_mode':stat.S_IMODE(cachemap.stat().st_mode)},
    'actual_platform_identity_readonly_check':identity,'Docker_CLI_identity_readonly_check':cli,'actual_Docker_API_queries':0,
    'actual_Cargo_commands':0,'actual_cache_removals':0,'prior_blocked_failure_artifact_hashes_unchanged':True,
    'forecast':{'free_bytes_before_readonly_checks':free_before,'free_bytes_after_readonly_checks':free_after,'actual_package_allocated_bytes_before_clean':package_allocated,
       'postclean_compile_base_required_bytes':base_required,'root_publication_allowance_bytes':publication_reserve,'required_postclean_compile_free_bytes':required,
       'projected_postclean_free_bytes':projected,'projected_postclean_fits':projected>=required,'projected_headroom_bytes':projected-required,
       'projected_reclaim_not_actual_until_clean':True,'mandatory_actual_postclean_forecast_before_any_Cargo':True,'shared350MiB_floor_bytes':350*1024*1024,
       'single_link_codegen_allowance_bytes':83002787,'persistent_library_and_three_test_allowance_bytes':135003311,'new_lossless_capture_allowance_bytes':143391919,
       'metadata_allowance_bytes':16*1024*1024,'selected_source_metadata_allowance_bytes':4*1024*1024,'uncertainty':'measured small-target/library references scaled1.5; sampled0.2s guard cannot guarantee instantaneous peak',
       'RPC_overlap_allowed':False,'native_overlap_allowed':False,'other_compiler_SDK_overlap_allowed':False,'existing_package_gzip_objects_not_charged_again_bytes':27156162},
    'cleanup_retry_held_until_explicit_ROOT_GO':True,'passed_readonly_preparation':True}
output=PREP/'readonly-source-cache-forecast.json';output.write_text(json.dumps(result,indent=2)+'\n');output.chmod(0o600)
print(json.dumps({'passed_readonly_preparation':True,'receipt_sha256':ns['digest'](output.read_bytes()),'free_after':free_after,'projected_postclean':projected,'required_postclean':required,'projected_fits':projected>=required,'projected_headroom':projected-required,'actual_Cargo_commands':0,'actual_Docker_queries':0,'actual_removals':0}))

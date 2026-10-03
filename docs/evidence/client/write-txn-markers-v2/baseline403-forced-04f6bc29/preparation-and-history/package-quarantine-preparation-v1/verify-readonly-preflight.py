#!/usr/bin/env python3
"""Read-only quarantine proposal forecast and existing609/package25 checks."""
from pathlib import Path
import ast,gzip,hashlib,json,os,stat,time
BASE=Path('/workspace/work/client-capability-qa-preparation-e90efb49');PREP=BASE/'package-quarantine-preparation-v1'
RUNNER=BASE/'run-forced-baseline403-quarantine-v1.py';TARGET=Path('/workspace/work/client-share-target')
sha=lambda p:hashlib.sha256(p.read_bytes()).hexdigest()
assert sha(RUNNER)=='ff3ab5eeab85c294a979126cce6dcef9e33c31ee092bca30173f2519c179cdb0'
ns={'Path':Path,'os':os,'stat':stat,'hashlib':hashlib};tree=ast.parse(RUNNER.read_bytes())
exec(compile(ast.fix_missing_locations(ast.Module(body=[n for n in tree.body if isinstance(n,ast.FunctionDef) and n.name in {'files','digest'}],type_ignores=[])),'readonly-frozen-functions','exec'),ns)
receipt=BASE/'baseline403-forced-04f6bc29-marker-v5-attempt-01/standard-cache-marker-repair.json'
assert sha(receipt)=='caeb86a5f72cec3de50e68d39b59218c678c5280c2775e0697837ad880987514'
expected=json.loads(receipt.read_bytes())['new609_complete_cache_identity_map'];assert len(expected)==609
actual=ns['files'](TARGET);assert set(actual)==set(expected)
for name,row in expected.items():
 p=TARGET/name;st=actual[name]
 assert stat.S_ISREG(st.st_mode) and sha(p)==row['sha256'] and st.st_size==row['bytes'] and stat.S_IMODE(st.st_mode)==row['full_mode'] and st.st_mtime_ns==row['mtime_ns'],name
package=json.loads((BASE/'package-invalidation-proposal-after-baseline-setup/proposal.json').read_bytes());assert len(package['package_output_restore_map'])==25
for row in package['package_output_restore_map']:
 p=TARGET/row['target_relative_path'];st=p.stat();obj=Path(row['gzip_path']).read_bytes()
 assert [st.st_dev,st.st_ino]==row['inode'] and hashlib.sha256(obj).hexdigest()==row['gzip_sha256'] and gzip.decompress(obj)==p.read_bytes()
label='04f6bc29-quarantine-v1-attempt-01'
for p in [BASE/('baseline403-forced-'+label),BASE/('baseline403-forced-source-'+label),BASE/('baseline403-quarantine-'+label)]:assert not os.path.lexists(p)
proof=BASE/'baseline403-forced-04f6bc29-marker-v5-attempt-01/post-failure-source-cache-guard.json'
assert sha(proof)=='7c50a7b1dc9fcac726031f315291c347c8b7a1eb9561c49eb1408279cba55089'
fd=os.statvfs('/workspace');free=fd.f_bavail*fd.f_frsize
result={'schema_version':1,'status':'WORK-only frozen source/memory controls/read-only cache forecast; actual quarantine/compiler requires newROOTGO',
 'sampled_at_unix':time.time(),'runner_sha256':sha(RUNNER),'source_sha':'04f6bc2968c1d721c6815a6389897a62e4ca76f1',
 'source_full_guards_prior_closed_binding':{'path':str(proof),'sha256':sha(proof),'actual_future_beforeafter_fullguard_required':True},
 'all609_current_sha_bytes_fullmodes_mtimes_match':True,'all25_current_inodes_and_existing_gzip_restore_verified':True,
 'standard177B_marker_current_matches_previous_publicized_receipt':True,'fresh_future_run_source_and_quarantine_paths_absent':True,
 'forecast':{'sampled_free_bytes':free,'raw25_retained_allocated_bytes_not_reclaimed':sum(r['allocated_bytes'] for r in package['package_output_restore_map']),
 'quarantine_reclaimed_file_bytes':0,'required_actual_postquarantine_free_bytes':782925569,'headroom_bytes':free-782925569,'fits':free>=782925569,
 'shared_floor_bytes':350*1024*1024,'root_external_publication_reserve_bytes':32*1024*1024,'library_allowance_bytes':83002787,
 'three_test_allowance_bytes':3*17333508,'one_link_codegen_allowance_bytes':83002787,'new_lossless_capture_allowance_bytes':143391919,
 'metadata_allowance_bytes':16*1024*1024,'selected_source_allowance_bytes':4*1024*1024,'already_retained25_gzip_bytes_not_charged_again':27156162,
 'actual_future_immediately_postquarantine_resample_mandatory':True,'no_RPC_native_or_other_SDK_compiler_overlap_allowed':True,
 'uncertainty':'measured artifacts scaled1.5 and sampled0.2s cutoff; does not guarantee instantaneous peak'},
 'actual_operations':{'target_writes':0,'quarantine_renames':0,'Cargo':0,'DockerAPI':0,'tests':0},'verifier_sha256':sha(Path(__file__))}
p=PREP/'readonly-preflight-forecast.json';assert not p.exists();p.write_text(json.dumps(result,indent=2)+'\n');p.chmod(0o600)
print(json.dumps({'preflight_sha256':sha(p),'free_bytes':free,'headroom_bytes':free-782925569,'cache609_verified':True,'actual_mutations':0}))

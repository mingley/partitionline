#!/usr/bin/env python3
"""Read-only pending marker/dry-run source/tool/cache guards; never launches Cargo."""
from pathlib import Path
import hashlib,json,stat,os,time,ast
BASE=Path('/workspace/work/client-capability-qa-preparation-e90efb49');PREP=BASE/'cache-tag-protection-preparation-v4'
SOURCE=Path('/workspace/work/client-capabilities-source-04f6bc29');TARGET=Path('/workspace/work/client-share-target')
sha=lambda p:hashlib.sha256(p.read_bytes()).hexdigest()
assert sha(BASE/'run-forced-baseline403-standard-marker-v4.py')=='8065020485bec6865e09ffb567130194ccf9bdc6f3121fa5efc032234ea908fe'
original=json.loads((BASE/'platform-daemon-exception-preparation-v2/current-complete-cache-hash-fullmode-map.json').read_bytes())
raw=(BASE/'run-forced-baseline403-standard-marker-v4.py').read_bytes();tree=ast.parse(raw);ns={'Path':Path,'os':os,'stat':stat,'hashlib':hashlib}
module=ast.Module(body=[n for n in tree.body if isinstance(n,ast.FunctionDef) and n.name in {'files','digest'}],type_ignores=[]);exec(compile(ast.fix_missing_locations(module),'readonly-preflight-functions','exec'),ns)
current=ns['files'](TARGET);assert current.keys()==original.keys();cacheallocated=0;seen=set()
for name,expected in original.items():
 p=TARGET/name;st=p.lstat();assert stat.S_ISREG(st.st_mode) and sha(p)==expected['sha256'] and p.stat().st_size==expected['bytes'] and stat.S_IMODE(st.st_mode)==expected['full_mode'] and st.st_mtime_ns==expected['mtime_ns']
 if (st.st_dev,st.st_ino) not in seen:cacheallocated+=st.st_blocks*512;seen.add((st.st_dev,st.st_ino))
package=json.loads((BASE/'package-invalidation-proposal-after-baseline-setup/proposal.json').read_bytes());allocated=sum(current[r['target_relative_path']].st_blocks*512 for r in package['package_output_restore_map']);assert allocated==89194496
inputs=json.loads((BASE/'baseline-compiler-inputs.json').read_bytes())
for row in inputs['files']:
 p=Path(row['path']);assert sha(p)==row['sha256'] and p.stat().st_size==row['bytes'] and stat.S_IMODE(p.stat().st_mode)==row['full_mode']
overlays=json.loads((BASE/'baseline-peer-correction-after-first-controls/baseline-overlay.proposed.json').read_bytes())
for row in overlays['overlays']:
 p=SOURCE/row['destination'] if row['destination']=='tests/fixtures/write-txn-markers-v2/socket_peer.rs' else Path(row['path']);assert sha(p)==row['sha256'] and p.stat().st_size==row['bytes'] and stat.S_IMODE(p.stat().st_mode)==row['full_mode']
executables=[]
for name in ['/workspace/work/rustup/toolchains/stable-x86_64-unknown-linux-gnu/bin/cargo','/workspace/work/rustup/toolchains/stable-x86_64-unknown-linux-gnu/bin/rustc','/usr/bin/nm','/usr/bin/taskset','/usr/local/bin/docker']:
 p=Path(name);st=p.lstat();resolved=p.resolve(strict=True);rs=resolved.lstat();assert stat.S_ISREG(rs.st_mode) and rs.st_mode&0o111
 assert stat.S_ISREG(st.st_mode) or name=='/usr/bin/nm' and stat.S_ISLNK(st.st_mode) and os.readlink(p)=='x86_64-linux-gnu-nm'
 executables.append({'invocation_path':name,'invocation_type':'symlink' if stat.S_ISLNK(st.st_mode) else 'regular','invocation_full_mode':stat.S_IMODE(st.st_mode),'symlink_text':os.readlink(p) if p.is_symlink() else None,'resolved_path':str(resolved),'resolved_sha256':sha(resolved),'resolved_bytes':rs.st_size,'resolved_full_mode':stat.S_IMODE(rs.st_mode),'uid':rs.st_uid})
assert not os.path.lexists(TARGET/'CACHEDIR.TAG')
for label in ['04f6bc29-marker-v4-attempt-01']:
 for p in [BASE/('baseline403-forced-'+label),BASE/('baseline403-forced-source-'+label),TARGET/('.partitionline-cache-tag-'+label+'.tmp')]:assert not os.path.lexists(p)
proof=BASE/'baseline403-forced-04f6bc29-platform-v2-attempt-01/post-failure-source-cache-guard.json';assert sha(proof)=='4f205d43132e8641dc12a52cdcf364ae6f5bcbf3e55db346be09a2ab5abb3f64'
fd=os.statvfs('/workspace');free=fd.f_bavail*fd.f_frsize
result={'schema_version':1,'scope':'readonly standard-marker repair/future-command preflight; no target repair/Docker API/Cargo/test executed','sampled_at_unix':time.time(),'verifier_sha256':sha(Path(__file__)),
 'source_sha':'04f6bc2968c1d721c6815a6389897a62e4ca76f1','full_source_guards_binding':{'receipt':str(proof),'sha256':sha(proof)},
 'target_marker_original_state':{'present':False,'lexists':False},'complete608_cache_hashes_bytes_fullmodes_mtimes_unchanged':True,'cache_allocated_bytes':cacheallocated,
 'own_package25_current_and_retained_exact':True,'future_selected66_old_inputs_checked':True,'future_exact5_overlay_inputs_checked':True,'fresh_run_target_and_temp_paths_absent':True,
 'executables_readonly_identity_preflight':executables,'Cargo_actual_dryrun_paths_not_yet_observed':True,'dryrun_format_support':'strict bare path/Removing/backtick forms or bounded directory expansion only; any unexpected real output stops before clean',
 'forecast':{'sampled_free_bytes':free,'projected_package_reclaim_bytes':allocated,'projected_postclean_free_bytes':free+allocated,'required_postclean_compile_bytes':782925569,'projected_headroom_bytes':free+allocated-782925569,'fits_projection':free+allocated>=782925569,
 'marker_max_new_bytes':177,'marker_disk_metadata_allowance_in_existing16MiB':True,'Root_external_publication_reserved_bytes':32*1024*1024,'shared_floor_bytes':350*1024*1024,'actual_postclean_resample_mandatory':True,'RPC_native_compiler_SDK_overlap_assumed':False},
 'actual_operations':{'target_writes':0,'Cargo':0,'Docker_API':0,'tests':0},'cleanup_and_retry_held_for_ROOT_review':True}
p=PREP/'readonly-preflight-forecast.json';p.write_text(json.dumps(result,indent=2)+'\n');p.chmod(0o600)
print(json.dumps({'preflight_sha256':sha(p),'free_bytes':free,'projected_headroom':free+allocated-782925569,'target_marker_absent':True,'actual_target_writes':0}))

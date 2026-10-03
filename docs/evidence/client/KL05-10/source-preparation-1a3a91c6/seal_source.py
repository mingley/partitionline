import hashlib,json,os,re,shutil,stat,difflib,subprocess
from pathlib import Path
root=Path('/workspace/work/client-sticky-implementation-prep'); candidate=root/'candidate'; repo=Path('/workspace/partitionline')
claim='1a3a91c6facc25e0b250fe06d9aefde9086a184c'
def sha(b): return hashlib.sha256(b).hexdigest()
def info(p):
 b=p.read_bytes(); return {'sha256':sha(b),'bytes':len(b),'mode':oct(stat.S_IMODE(p.stat().st_mode)), 'git_mode':'100755' if p.stat().st_mode&stat.S_IXUSR else '100644'}
baselines={'src/partitioner.rs':'ad9c8977c4f5ef8b54284167020eb35d931e27fd5cc95a23d427c80bb9bd90b5','src/producer.rs':'5ccee6c3691db29d152c7a4bd9f3abfa0a1d804fcd54fbbe0090454b322fc8a9','tests/client_api.rs':'4381fe434363ab13b550dc85414506b705b0af0246b916c2e169f979540c19c8'}
for name,h in baselines.items(): assert info(repo/name)['sha256']==h, name
for name in ['BuiltInPartitioner.java','BuiltInPartitionerTest.java','RecordAccumulator.java','ProducerConfig.java']:
 shutil.copyfile('/workspace/work/client-sticky-assessment/upstream/'+name,candidate/'tests/fixtures/sticky-partitioner'/name)
product_files=sorted(str(p.relative_to(candidate)) for p in candidate.rglob('*') if p.is_file())
owned_hashes={name:info(candidate/name) for name in product_files}
patch=[]
for name in product_files:
 old=(root/'base'/name).read_text() if (root/'base'/name).exists() else ''
 new=(candidate/name).read_text()
 patch.extend(difflib.unified_diff(old.splitlines(keepends=True),new.splitlines(keepends=True),fromfile='a/'+name if old else '/dev/null',tofile='b/'+name))
(root/'source-review.patch').write_text(''.join(patch))
prepared_sockets=re.findall(r'^async fn (\w+)\(', (candidate/'tests/sticky_partitioner.rs').read_text(),re.M)[2:]
new_unit_tests=re.findall(r'^    fn (\w+)\(', (candidate/'src/partitioner.rs').read_text(),re.M)
new_unit_tests=[n for n in new_unit_tests if n not in {'partition','default','fmt','draw','choose','identity','reset','allocate_id','full','maybe_rotate','murmur2_matches_java_utils','abs_matches_java_utils','default_partitioner_keys_match_murmur2','metadata','append'}]
held={name:info(repo/name) for name in ['src/protocol/records.rs','src/protocol/buf.rs','src/net.rs','src/group.rs','src/cluster.rs']}
preparation={'schema':1,'scope':'syntax/format only; no Cargo, rustc, Clippy, tests, JVM, broker or performance command executed','cpu_set':'0,1','toolchain_homes':{'CARGO_HOME':'/workspace/work/cargo','RUSTUP_HOME':'/workspace/work/rustup'},'commands':[]}
for prefix in ['format-final-complete','msrv-final-complete-format-check']:
 preparation['commands'].append({'label':prefix,'command':['taskset','-c','0,1','/workspace/work/rustup/toolchains/'+('1.85.0' if prefix.startswith('msrv') else 'stable')+'-x86_64-unknown-linux-gnu/bin/rustfmt']+(['--check'] if prefix.startswith('msrv') else [])+['--edition','2021','--config','skip_children=true']+[str(candidate/n) for n in ['src/partitioner.rs','src/producer.rs','tests/sticky_partitioner.rs','tests/client_api.rs']],'exit':int((root/'preparation'/f'{prefix}.exit').read_text()),'stdout':info(root/'preparation'/f'{prefix}.stdout'),'stderr':info(root/'preparation'/f'{prefix}.stderr')})
assert all(c['exit']==0 for c in preparation['commands'])
(root/'preparation'/'first-format-tool-output.json').write_text(json.dumps({'kind':'verbatim tool-result excerpt retained from initial attempt; not a redirected process stream','command':'taskset -c 0,1 /workspace/work/rustup/toolchains/stable-x86_64-unknown-linux-gnu/bin/rustfmt --edition 2021 work/client-sticky-implementation-prep/candidate/tests/sticky_partitioner_semantics.rs','exit_code':1,'output':'Error writing files: failed to resolve mod `common`: /workspace/work/client-sticky-implementation-prep/candidate/tests/common.rs does not exist\n','classification':'isolated WORK candidate lacks tests/common; corrected source preparation uses skip_children=true, and the published test path is tests/sticky_partitioner.rs. No compile/runtime failure.'},indent=2)+'\n')
(root/'preparation'/'receipt.json').write_text(json.dumps(preparation,indent=2)+'\n')
freeze={'schema':1,'task':'KL05-10','claimed_main_sha':claim,'status':'first implemented source candidate; uncompiled and unqualified','contract':'Kafka 4.3 uniform adaptive=false state machine with Rust successful-admission conservative packed-byte accounting','write_set_reconciliation':{'old_proposal_sha':'6dc94920ca52b38059199794e455351e19a85f2e42469b3f131e24fb9a988763','actual_pushed_test':'tests/sticky_partitioner.rs','actual_pushed_fixture_directory':'tests/fixtures/sticky-partitioner/','no_unpublished_paths_written':True},'source_files':owned_hashes,'baselines':baselines,'prepared_socket_tests':prepared_sockets,'prepared_policy_tests':new_unit_tests,'held_observed_at_handoff':held,'limits':{'max_topics_ceiling':1024,'max_outstanding_cohorts_ceiling':4096,'retained_topic_text_bytes_ceiling':262144,'pressure_mode':'uniform singleton; no new admission error','opt_in_produce_requests':'one complete bounded cohort per RPC; throughput/memory effects unmeasured','default_routing':'prior default/custom path remains; internal Pending size changed and performance must be measured','reference_execution':'caller prepared; SDK source pinned; no generated Java output fixture','qualified':False,'performance_claims':False}}
(root/'source-freeze.json').write_text(json.dumps(freeze,indent=2)+'\n')
# Authorized sole ownership: publish this reviewable candidate, without commit/push.
for name in product_files:
 target=repo/name; target.parent.mkdir(parents=True,exist_ok=True); shutil.copyfile(candidate/name,target)
 assert info(target)['sha256']==owned_hashes[name]['sha256']
for name,observed in held.items(): assert info(repo/name)==observed,name
print(json.dumps({'source_files':len(product_files),'source_bytes':sum(v['bytes'] for v in owned_hashes.values()),'prepared_socket_tests':len(prepared_sockets),'prepared_policy_tests':len(new_unit_tests),'source_freeze_sha256':sha((root/'source-freeze.json').read_bytes()),'source_review_patch_sha256':sha((root/'source-review.patch').read_bytes()),'source_review_patch_bytes':(root/'source-review.patch').stat().st_size,'compiled_or_runtime_qualified':False},indent=2))

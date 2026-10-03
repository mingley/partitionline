from pathlib import Path
import hashlib,json,os,shutil,subprocess,stat
R=Path('/workspace/partitionline');W=Path('/workspace/work/list-transactions-routing-01');O=Path(__file__).parent
sha=lambda p:hashlib.sha256(p.read_bytes()).hexdigest()
manifest=W/'tests/fixtures/list-transactions-routing/installation-manifest.json'
assert sha(manifest)=='8bc4505a7c444381497c4d613dcfb2565e56c9ad4182cbc9d7fb75fe73c63228'
m=json.loads(manifest.read_bytes())
source=O/'candidate';assert not source.exists();source.mkdir(mode=0o700)
for r in m['installation_map']:
 p=Path(r['path']);assert sha(p)==r['sha256'] and p.stat().st_size==r['bytes'] and stat.S_IMODE(p.stat().st_mode)==r['full_mode']
 q=source/r['coordinator_install_target'];q.parent.mkdir(parents=True,exist_ok=True);shutil.copyfile(p,q);os.chmod(q,r['full_mode'])
for rel in ['src/admin.rs','src/lib.rs','tests/common/mod.rs','tests/full_surface.rs']:
 original=R/rel;backup=O/'before'/rel;backup.parent.mkdir(parents=True,exist_ok=True);shutil.copyfile(original,backup);os.chmod(backup,stat.S_IMODE(original.stat().st_mode))
 q=source/rel;q.parent.mkdir(parents=True,exist_ok=True);shutil.copyfile(original,q);os.chmod(q,stat.S_IMODE(original.stat().st_mode))
assert sha(source/'src/admin.rs')=='278564415d0431d8df21a3bcd5413122eb09625ee27d22cb69d25d8ff33867cc'
assert sha(W/'admin.candidate.rs')=='e78e63d5fac51b74a5ce9784d671d1aa4fb23ead6dc3e27cf6f7039dcc274ab8'
shutil.copyfile(W/'admin.candidate.rs',source/'src/admin.rs')
p=source/'src/lib.rs';s=p.read_text();needle='    AssignReplicasToDirsTopic, ClientQuotaAlteration, ClientQuotaAlterationResult,';assert s.count(needle)==1
p.write_text(s.replace(needle,'    AssignReplicasToDirsTopic, BrokerTransactionListings, ClientQuotaAlteration, ClientQuotaAlterationResult,'))
p=source/'tests/common/mod.rs';s=p.read_text();a=s.index('            LIST_TRANSACTIONS => {');b=s.index('            DESCRIBE_ACLS => {',a)
s=s[:a]+'''            LIST_TRANSACTIONS => {
                let version = header.api_version;
                let (_states, _pids, duration_ms) =
                    decode_list_transactions_request(&mut frame, version).unwrap();
                let mut st = state.lock();
                st.last_list_transactions_version = Some(version);
                st.last_list_transactions_duration = Some(duration_ms);
                // Each broker answers this all-brokers query. Only the current
                // owner has these fixtures; other brokers succeed with no rows.
                let transaction_states: Vec<TransactionListing> = if st.txn_coord_node == node_id {
                    st.last_list_transactions_node = Some(node_id);
                    st.txn_fixtures.values().map(|s| TransactionListing {
                        transactional_id: s.transactional_id.clone(),
                        producer_id: s.producer_id,
                        transaction_state: s.transaction_state.clone(),
                    }).collect()
                } else {
                    Vec::new()
                };
                encode_list_transactions_response(
                    &mut body, version,
                    &ListTransactionsResponse::new(0, Vec::new(), transaction_states),
                ).unwrap();
            }
'''+s[b:]
assert s.count('                let md = if id_based > 0 {')==1
s=s.replace('                let md = if id_based > 0 {','                let mut md = if id_based > 0 {')
needle='''                if header.api_version >= 10 {
                    for topic in &md.topics {''';assert s.count(needle)==1
s=s.replace(needle,'''                // Metadata v1+ distinguishes an empty selection from all topics.
                if header.api_version >= 1 && topics.as_ref().is_some_and(Vec::is_empty) {
                    md.topics.clear();
                }
'''+needle);p.write_text(s)
p=source/'tests/full_surface.rs';s=p.read_text();a=s.index('async fn list_transactions_follows_coordinator() {');b=s.index('\n#[tokio::test]',a);old=s[a:b]
new=old.replace('async fn list_transactions_follows_coordinator() {','async fn list_transactions_queries_all_brokers_without_coordinator_lookup() {')
new=new.replace('"ListTransactions must land on the transaction coordinator, not bootstrap"','"the broker which owns these fixtures must be queried"')
new=new.replace('''        mock.find_coordinator_key_types()
            .contains(&COORDINATOR_TRANSACTION),
        "ListTransactions must FindCoordinator key_type=1"''','''        mock.find_coordinator_key_types().is_empty(),
        "ListTransactions must not look up an empty transactional ID"''')
new=new.replace('"retry on the new coordinator must still return fixture txn ids, not the 16 empty body"','"all-broker discovery must include fixtures after ownership moves"')
new=new.replace('''        mock.list_transactions_not_coordinator(),
        1,
        "stale coordinator must return NOT_COORDINATOR (16) once"''','''        mock.list_transactions_not_coordinator(),
        0,
        "a broker with no matching transactions succeeds with an empty listing"''')
new=new.replace('"ListTransactions must FindCoordinator after NOT_COORDINATOR"','"the new fixture owner must be included in the next all-broker query"')
new=new.replace('''    let timed = admin
        .list_transactions_timeout''','''    assert!(mock.find_coordinator_key_types().is_empty());
    let timed = admin
        .list_transactions_timeout''')
assert new!=old and 'must FindCoordinator' not in new;s=s[:a]+new+s[b:];p.write_text(s)
# Preserve pre-format derivative bytes before parser/format mutations.
pre=O/'pre-format';shutil.copytree(source,pre)
rust=[source/'src/admin.rs',source/'src/lib.rs',source/'tests/common/mod.rs',source/'tests/full_surface.rs']+[source/r['coordinator_install_target'] for r in m['installation_map'] if r['path'].endswith('.rs')]
for tool,flags in [('/workspace/work/rustup/toolchains/stable-x86_64-unknown-linux-gnu/bin/rustfmt',[]),('/workspace/work/rustup/toolchains/1.85.0-x86_64-unknown-linux-gnu/bin/rustfmt',['--check'])]:
 # Standalone lib.rs would recurse into nonexistent production modules in this narrow source tree.
 cmd=['taskset','-c','0,1',tool,'--edition','2021','--config','skip_children=true']+flags+[str(p) for p in rust]
 receipt=subprocess.run(cmd,stdout=subprocess.PIPE,stderr=subprocess.STDOUT)
 name='format-apply' if not flags else 'msrv-format-check';(O/(name+'.log')).write_bytes(receipt.stdout)
 assert receipt.returncode==0,(name,receipt.returncode)
rows=[]
for p in sorted(source.rglob('*')):
 if p.is_file():rows.append(dict(repository_path=str(p.relative_to(source)),source=str(p),bytes=p.stat().st_size,full_mode=stat.S_IMODE(p.stat().st_mode),sha256=sha(p)))
review=dict(scope='Root narrow source integration/pre-format preservation/standalone stable+MSRV formatting only, no Cargo/Rust typing/SDK/listener/runtime qualification.',task='KL05-38',source_manifest_sha256=sha(manifest),candidate_admin_after_format=sha(source/'src/admin.rs'),formatter_invocations=2,actual_cargo_commands=0,actual_sdk_commands=0,held_files_edited=False,changes=['All-broker membership snapshot, per-connection API66 negotiation and complete versus origin-preserving partial results.','One total deadline; finite retries/refreshes; bounded borrowed preflight before allocating decoders.','Metadata v1-3 legacy allow_auto_create=true for an empty topic selection.','Correct shared mock API66 nonowner empty success and Metadata v1+ empty topic selection; preserve historical source copies.','Reexport partial-result type and replace coordinator-only integration expectation.'],files=rows)
(O/'candidate-manifest.json').write_text(json.dumps(review,indent=2)+'\n');print(json.dumps(dict(source_files=len(rows),candidate_admin=review['candidate_admin_after_format'],cargo_commands=0)))

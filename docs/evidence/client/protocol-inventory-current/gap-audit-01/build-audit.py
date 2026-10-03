#!/usr/bin/env python3
"""Static, source-only audit. Does not execute SDKs or mutate the repository."""
import ast
import hashlib
import json
from pathlib import Path
import re
import subprocess
import tarfile

ROOT = Path('/workspace/partitionline')
OUT = Path('/workspace/work/consumer-conformance-gap-audit-01')
PIN = 'd3dfffb2737ec90c14f8c48b8106de92c917df12'
RELEASES = {
    '4.1.2': ('c82fd9b934b4c1e6fa799e3f1dcc8f08d997740c', '22e7e13834390b3a7e38670a1115618ac1e82bf4c5a1bb9cc6dbf6390d0f235a'),
    '4.2.1': ('18d5ecd939c8d510fdd72d0abb1f7099659dcd58', 'faa55f0602830e89dcaaa5923b7d9cd76fd03751ec40767c6b065453b475b955'),
    '4.3.1': ('26b251a451ce941d3d7a55e6487bcb7f16b5ad48', '4b5a65a52cbfdabe217856b6b4eb7cad7067f250524a38592ff27ee315824ea2'),
}
def sha(b):
    return hashlib.sha256(b).hexdigest()
def write(name, value):
    (OUT/name).write_text(json.dumps(value, indent=2, sort_keys=True)+'\n')
git_inputs = {}
def git(path):
    b = subprocess.check_output(['git','show',f'{PIN}:{path}'], cwd=ROOT)
    mode, kind, blob = subprocess.check_output(['git','ls-tree',PIN,'--',path],cwd=ROOT).decode().split('\t')[0].split()
    git_inputs[path] = dict(source_sha=PIN, git_blob=blob, git_mode=mode, bytes=len(b), sha256=sha(b))
    return b
tasks = json.loads(git('docs/plan/tasks.json'))['tasks']
task_map = {t['id']:t for t in tasks}
inventory_path = 'docs/evidence/client/protocol-inventory-current/review-01/corrected-inventory-proposal.json'
inventory = json.loads(git(inventory_path))
assert set(map(int,inventory)) == set(range(93))
source_text = {}
rust_patterns = {
 'src/protocol/api.rs': r'pub const fn is_all_topics|fn build\(|pub fn decode_metadata_response|pub fn encode_metadata_response',
 'src/protocol/group.rs': r'fn offset_commit_flexible|fn offset_fetch_flexible|fn find_coordinator_flexible|fn join_group_flexible|pub struct OffsetTopic|pub struct OffsetFetchTopic|pub fn encode_offset_commit_request|pub fn decode_offset_commit_response|pub fn encode_offset_fetch_groups_request|pub fn decode_offset_fetch_groups_response',
 'src/group.rs': r'offset_commit_version = resp|offset_fetch_version = resp|fn spoken_offset_commit|fn spoken_offset_fetch',
 'src/protocol/idem.rs': r'fn init_producer_id_flexible|pub fn encode_init_producer_id_request|pub fn decode_init_producer_id_response',
 'src/producer.rs': r'let ipid_version = pick|add_partitions_to_txn_version =|ADD_PARTITIONS_TO_TXN, 0, 3',
 'src/protocol/txn.rs': r'LAST_CLIENT_VERSION|add_partitions_to_txn_flexible|pub fn encode_add_partitions_to_txn_request',
 'src/protocol/admin.rs': r'fn list_transactions_flexible|pub fn encode_list_transactions_request|pub fn build\(version: i16, duration_ms|fn add_raft_voter_version|pub struct AddRaftVoterRequest|pub fn encode_add_raft_voter_request|pub fn encode_alter_partition_reassignments_request',
 'src/admin.rs': r'let reassign_version = versions|let list_transactions_version = versions|pub async fn list_transactions_with_duration_timeout|const COORD_KEY:|encode_list_transactions_request\(buf|pick_version\(v.min_version, v.max_version, 2, 9\)|pick_version\(v.min_version, v.max_version, client_min, 9\)|pub async fn add_raft_voter|struct AddRaftVoterOptions|pub async fn assign_replicas_to_dirs|pub async fn allocate_producer_ids',
 'src/protocol/api_keys.rs': r'pub const OFFSET_COMMIT|pub const OFFSET_FETCH|pub const INIT_PRODUCER_ID|pub const ADD_PARTITIONS_TO_TXN|pub const ALTER_PARTITION_REASSIGNMENTS|pub const LIST_TRANSACTIONS|pub const ADD_RAFT_VOTER',
}
def snippets(b, pattern, radius=4, after=11, limit=50):
    lines=b.decode().splitlines(); found=[]
    for i,l in enumerate(lines):
        if re.search(pattern,l):
            a=max(0,i-radius); z=min(len(lines),i+after)
            found.append(dict(match_line=i+1,start_line=a+1,end_line=z,text='\n'.join(lines[a:z])))
    return found[:limit]
rust_evidence={}
for path,pattern in rust_patterns.items():
    b=git(path);source_text[path]=b.decode()
    rust_evidence[path]=dict(identity=git_inputs[path],snippets=snippets(b,pattern))
for path in ['src/consumer.rs','tests/conformance/cases.json','tests/conformance/features.json','tests/conformance/broker/api-matrix.json','scripts/check-protocol-coverage.py','Cargo.toml','src/lib.rs']:
    b=git(path);source_text[path]=b.decode()
declared={}
for node in ast.parse(source_text['scripts/check-protocol-coverage.py']).body:
    if isinstance(node,ast.AnnAssign) and isinstance(node.target,ast.Name) and node.target.id=='CLIENT_SPOKEN_VERSIONS':
        for k,v in zip(node.value.keys,node.value.values):
            if isinstance(v,ast.List): value=ast.literal_eval(v)
            else:
                assert isinstance(v,ast.Call) and isinstance(v.func,ast.Name) and v.func.id=='list'
                call=v.args[0];assert call.func.id=='range'
                value=list(range(*(ast.literal_eval(x) for x in call.args)))
            declared[ast.literal_eval(k)]=value
write('rust-source-evidence.json',rust_evidence)

java_patterns = {
 'clients/src/main/java/org/apache/kafka/common/requests/AddPartitionsToTxnRequest.java': r'LAST_CLIENT_VERSION|EARLIEST_BROKER_VERSION|forClient|forBroker',
 'clients/src/main/java/org/apache/kafka/common/requests/MetadataRequest.java': r'version == 0|version < 1|build\(short version\)|isAllTopics',
 'clients/src/main/java/org/apache/kafka/common/requests/FindCoordinatorRequest.java': r'version == 0|version < 1|build\(short version\)|COORDINATOR_TYPE',
 'clients/src/main/java/org/apache/kafka/common/requests/JoinGroupRequest.java': r'version == 0|version < 1|build\(short version\)|rebalanceTimeoutMs|groupInstanceId',
 'clients/src/main/java/org/apache/kafka/clients/admin/internals/ListTransactionsHandler.java': r'AllBrokersStrategy|setTransactionalIdPattern|buildBatchedRequest',
 'clients/src/main/java/org/apache/kafka/clients/admin/internals/AllBrokersStrategy.java': r'buildRequest|MetadataRequest|brokerId|handleResponse',
 'clients/src/main/java/org/apache/kafka/clients/admin/ListTransactionsResult.java': r'all\(\)|byBrokerId\(\)|allByBrokerId\(\)|addAll\(|completeExceptionally',
 'clients/src/main/java/org/apache/kafka/clients/admin/KafkaAdminClient.java': r'public ListTransactionsResult listTransactions|setAllowReplicationFactorChange|public AddRaftVoterResult addRaftVoter',
 'clients/src/main/java/org/apache/kafka/clients/admin/Admin.java': r'abortTransaction\(|alterPartitionReassignments\(|listTransactions\(|addRaftVoter\(|updateFeatures\(|describeMetadataQuorum\(',
 'clients/src/main/java/org/apache/kafka/clients/admin/ListTransactionsOptions.java': r'filterOnTransactionalIdPattern|filteredTransactionalIdPattern',
 'clients/src/main/java/org/apache/kafka/common/requests/ListTransactionsRequest.java': r'durationFilter\(\)|transactionalIdPattern\(\)|UnsupportedVersionException',
 'clients/src/main/java/org/apache/kafka/clients/admin/AlterPartitionReassignmentsOptions.java': r'allowReplicationFactorChange',
 'clients/src/main/java/org/apache/kafka/common/requests/AlterPartitionReassignmentsRequest.java': r'allowReplicationFactorChange|UnsupportedVersionException',
 'clients/src/main/java/org/apache/kafka/clients/admin/AddRaftVoterOptions.java': r'clusterId|class AddRaftVoterOptions',
 'clients/src/main/java/org/apache/kafka/common/requests/AddRaftVoterRequest.java': r'build\(short version\)',
 'clients/src/main/java/org/apache/kafka/clients/producer/Producer.java': r'initTransactions\(|beginTransaction\(|commitTransaction\(|abortTransaction\(',
 'clients/src/main/java/org/apache/kafka/clients/producer/KafkaProducer.java': r'initializeTransactions\(false\)|TRANSACTION_TWO_PHASE_COMMIT_ENABLE_CONFIG',
 'clients/src/main/java/org/apache/kafka/clients/producer/ProducerConfig.java': r'TRANSACTION_TWO_PHASE_COMMIT_ENABLE|two.phase.commit|2PC',
 'clients/src/main/java/org/apache/kafka/clients/producer/internals/TransactionManager.java': r'initializeTransactions\(boolean|new InitProducerIdRequestData|public synchronized void prepareTransaction|TO_DO|builder.data.keepPreparedTxn',
 'core/src/main/scala/kafka/coordinator/transaction/TransactionCoordinator.scala': r'else if \(keepPreparedTxn\)|enableTwoPCFlag &&|handleInitProducerId',
 'raft/src/main/java/org/apache/kafka/raft/KafkaRaftClient.java': r'data.ackWhenCommitted\(\)|addVoterHandler.handleAddVoterRequest',
 'raft/src/main/java/org/apache/kafka/raft/internals/AddVoterHandler.java': r'ackWhenCommitted|highWatermark.offset\(\) > lastOffset|Wait for the VotersRecord',
 'core/src/main/scala/kafka/server/ReplicaManager.scala': r'directoryEventHandler.handleAssignment',
}
upstream={};schema_proofs={};all_schema_proofs=[]
matrix=json.loads(source_text['tests/conformance/broker/api-matrix.json'])
for release,(commit,archive_hash) in RELEASES.items():
    archive=Path('/workspace/work/broker-api')/(release+'.tar.gz')
    actual=sha(archive.read_bytes());assert actual==archive_hash
    with tarfile.open(archive) as tf:
        index={n.partition('/')[2]:n for n in tf.getnames()}
        rec=dict(commit=commit,archive=str(archive),archive_sha256=actual,sources={})
        for path,pattern in java_patterns.items():
            if path not in index:
                rec['sources'][path]=dict(present=False);continue
            member=tf.getmember(index[path]); b=tf.extractfile(member).read()
            t=b.decode()
            rec['sources'][path]=dict(present=True,member=index[path],tar_mode=oct(member.mode),sha256=sha(b),bytes=len(b),snippets=snippets(b,pattern,after=13,limit=25))
            if path.endswith('/Admin.java'):
                rec['public_Admin_absence_checks']={x:bool(re.search(r'\b'+x+r'\s*\(',t)) for x in ['assignReplicasToDirs','allocateProducerIds']}
            if path.endswith('/Producer.java'):
                rec['public_Producer_prepareTransaction_present']=bool(re.search(r'\bprepareTransaction\s*\(',t))
            if path.endswith('/TransactionManager.java'):
                rec['InitProducerId_flag_setters_present']={x:x in t for x in ['setEnable2Pc(','setKeepPreparedTxn(']}
            if path.endswith('/TransactionCoordinator.scala'):
                assert re.search(r'else if \(keepPreparedTxn\).*?Errors.UNSUPPORTED_VERSION',t,re.S)
                rec['KeepPreparedTxn_source_outcome']='UNSUPPORTED_VERSION; source-only, not newly executed'
        upstream[release]=rec
        for key,row in inventory.items():
            name=row['name'];key=int(key)
            for kind in ['Request','Response']:
                # ApiKeys public name differs from its generated schema family.
                schema_name = 'ListConfigResources' if key == 74 else name
                path=f'clients/src/main/resources/common/message/{schema_name}{kind}.json'
                if path not in index:
                    assert row['versions'][release] is None, (release,key,path)
                    all_schema_proofs.append(dict(release=release,api_key=key,kind=kind,path=path,present=False))
                    continue
                member=tf.getmember(index[path]);b=tf.extractfile(member).read()
                data=json.loads(re.sub(r'(?m)^\s*//[^\n]*(?:\n|$)','',b.decode()))
                valid=data['validVersions'];interval=None if valid=='none' else [int(x) for x in valid.split('-')]
                if interval is not None and len(interval)==1:interval*=2
                assert interval==row['versions'][release],(release,key,interval,row['versions'][release])
                assert data['apiKey']==key
                proof=dict(release=release,api_key=key,kind=kind,path=path,present=True,sha256=sha(b),bytes=len(b),tar_mode=oct(member.mode),valid_versions=valid,flexible_versions=data.get('flexibleVersions'),latest_version_unstable=data.get('latestVersionUnstable',False))
                all_schema_proofs.append(proof)
                if key in [8,9,22,24,45,66,80]:
                    schema_proofs.setdefault(release,{})[name+kind]=dict(identity=proof,fields=data['fields'])
write('upstream-source-evidence.json',upstream)
write('selected-schema-fields.json',schema_proofs)
write('all-93-schema-identities.json',all_schema_proofs)

# Every reference below names an actual card in the frozen taskbook. They are
# ownership/dependency mappings, never a claim that an API's versions pass.
server={
 0:['KL11-06','KL11-19','KL11-23','KL11-75'],1:['KL11-07','KL11-18','KL11-19','KL11-25','KL11-75'],2:['KL11-07','KL11-20','KL11-75'],3:['KL11-05','KL11-17'],
 4:['KL11-17'],5:['KL11-17'],6:['KL11-16','KL11-17'],7:['KL11-16'],
 8:['KL11-29'],9:['KL11-29'],10:['KL11-24','KL11-27','KL11-31','KL11-32'],
 11:['KL11-27','KL11-28'],12:['KL11-28'],13:['KL11-28'],14:['KL11-27'],15:['KL11-29'],16:['KL11-29'],
 17:['KL11-36','KL11-37','KL11-38'],18:['KL11-04','KL11-48'],19:['KL11-05','KL11-59'],20:['KL11-05','KL11-59'],21:['KL11-10'],
 22:['KL11-23','KL11-24'],23:['KL11-20'],24:['KL11-24','KL11-25'],25:['KL11-26'],26:['KL11-24','KL11-25'],27:['KL11-25','KL11-41'],28:['KL11-26'],
 29:['KL11-39'],30:['KL11-39'],31:['KL11-39'],32:['KL11-21'],33:['KL11-21'],34:['KL11-41'],35:['KL11-41'],36:['KL11-36','KL11-37','KL11-38','KL11-71'],37:['KL11-05','KL11-17'],
 38:['KL11-40'],39:['KL11-40'],40:['KL11-40'],41:['KL11-40'],42:['KL11-29'],43:['KL11-17','KL11-20','KL11-41'],44:['KL11-21'],45:['KL11-17','KL11-41'],46:['KL11-17','KL11-41'],47:['KL11-29'],48:['KL11-22'],49:['KL11-22'],50:['KL11-36'],51:['KL11-36'],
 52:['KL11-13','KL11-70','KL11-76'],53:['KL11-13','KL11-70','KL11-76'],54:['KL11-13','KL11-70','KL11-76'],55:['KL11-14','KL11-41','KL11-70','KL11-73'],56:['KL11-17'],57:['KL11-41'],58:['KL11-41'],59:['KL11-15'],60:['KL11-41'],61:['KL11-23','KL11-41'],62:['KL11-16'],63:['KL11-16'],64:['KL11-16','KL11-41'],65:['KL11-24','KL11-41'],66:['KL11-24','KL11-41'],67:['KL11-23'],68:['KL11-31'],69:['KL11-31'],70:['KL11-16'],71:['KL11-43'],72:['KL11-43'],73:['KL11-17','KL11-41'],74:['KL11-43'],75:['KL11-17','KL11-41'],76:['KL11-32'],77:['KL11-32'],78:['KL11-33'],79:['KL11-33'],80:['KL11-74','KL11-70','KL11-41'],81:['KL11-74','KL11-41'],82:['KL11-70','KL11-74'],83:['KL11-34'],84:['KL11-34'],85:['KL11-34'],86:['KL11-34'],87:['KL11-34'],88:['KL11-32'],89:['KL11-32'],90:['KL11-34'],91:['KL11-34'],92:['KL11-34'],
}
client={
 0:['KL05-11','KL05-08','KL03-08','KL03-11','KL03-19'],1:['KL05-12','KL05-06','KL05-07','KL05-09','KL03-02','KL03-03','KL03-22'],2:['KL05-13','KL03-05'],3:['KL01-06','KL05-28'],
 8:['KL03-06','KL03-07','KL03-20'],9:['KL03-06','KL03-20'],10:['KL05-28','KL03-12'],11:['KL03-14','KL03-21'],12:['KL03-21'],13:['KL03-14','KL03-21'],14:['KL03-14','KL03-21'],15:['KL05-28'],16:['KL05-28'],
 17:['KL06-01','KL06-04','KL06-06'],18:['KL05-28'],19:['KL05-28'],20:['KL05-28'],21:['KL05-28'],22:['KL03-09','KL03-10','KL03-20','KL05-28'],23:['KL03-04','KL03-22'],24:['KL03-10','KL03-20'],25:['KL03-20'],26:['KL03-10','KL03-20'],27:['KL05-29'],28:['KL03-20'],
 29:['KL05-28'],30:['KL05-28'],31:['KL05-28'],32:['KL05-28'],33:['KL05-28'],34:['KL05-28'],35:['KL05-22'],36:['KL06-01','KL06-02','KL06-03','KL06-04','KL06-06'],37:['KL05-28'],38:['KL06-06','KL05-28'],39:['KL06-06','KL05-28'],40:['KL06-06','KL05-28'],41:['KL06-06','KL05-28'],42:['KL05-28'],43:['KL05-16','KL05-17'],44:['KL05-28'],45:['KL05-28'],46:['KL05-28'],47:['KL05-28'],48:['KL05-28'],49:['KL05-28'],50:['KL05-28'],51:['KL05-28'],55:['KL05-18','KL05-19'],57:['KL05-28'],60:['KL05-28'],61:['KL05-28'],64:['KL05-28'],65:['KL05-28'],66:['KL05-28'],68:['KL03-12','KL03-15'],69:['KL03-12'],71:['KL05-28'],72:['KL05-28'],74:['KL05-28'],75:['KL05-28'],76:['KL03-13','KL05-15'],77:['KL05-15'],78:['KL05-14','KL05-15','KL03-16'],79:['KL05-14','KL05-15','KL03-16'],80:['KL05-20'],81:['KL05-21'],88:['KL05-31','KL05-32'],89:['KL05-31','KL05-33'],90:['KL05-30'],91:['KL05-15','KL05-28'],92:['KL05-15','KL05-28'],
}
internal={52,53,54,56,58,59,62,63,67,70,73,82,83,84,85,86,87}
notes={
 4:'Removed in all three current pinned schemas; legacy ZooKeeper/controller-to-broker form is not a current public-client gap.',
 5:'Removed in all three current pinned schemas; legacy replica-stop form is not a current public-client gap.',
 6:'Removed in all three current pinned schemas; legacy controller broadcast form is not a current public-client gap.',
 7:'Removed in all three current pinned schemas; legacy ControlledShutdown form is not a current public-client gap.',
 24:'Split by version: client factory supports0–3; broker factory supports4–5 batched Transactions/VerifyOnly. Do not require Producer to send4–5.',
 27:'Dual use: WriteTxnMarkers is internal marker traffic AND public Java Admin.abortTransaction. clusterAction does not justify excluding its public surface.',
 55:'Dual use: consensus diagnostics AND public Java Admin.describeMetadataQuorum; retain public conformance.',
 57:'Dual use: controller-facing feature mutation AND public Java Admin.updateFeatures; retain public conformance.',
 67:'Controller PID-block allocation; no standard Java Admin.allocateProducerIds method. Existing Rust raw Admin helper is a nonstandard extension, not public reference equivalence.',
 73:'Broker directory assignment traffic; no standard Java Admin.assignReplicasToDirs method. Java public alterReplicaLogDirs uses API34, then broker DirectoryEventHandler assignment. Rust raw helper is a nonstandard extension.',
 74:'ApiKeys name ListClientMetricsResources uses generated ListConfigResourcesRequest/Response schema filenames. The alias is retained explicitly; source enumeration uses the actual schema family, not an absent invented filename.',
 3:'Metadata0 remains valid in all3 current upstream schemas but Rust declares a minimum1. Keep the explicit unsupported-minimum negotiation case; do not erase upstream0 or infer independent pass from a version table.',
 10:'FindCoordinator0 remains valid in all3 current upstream schemas but Rust declares a minimum1. Keep the explicit unsupported-minimum negotiation case, including actual0-only coordinator peers.',
 11:'JoinGroup0/1 remain valid in all3 corrected current upstream schemas but Rust declares a minimum2. Keep the explicit unsupported-minimum negotiation cases; this is distinct from latest-version field additions.',
 80:'API80 is AddRaftVoter.4.1.2 supports0 only;4.2/4.3 add1 AckWhenCommitted. Public Java Admin option does not expose false; generated defaulttrue. Membership durability does not follow from local append.',
 88:'Client code/runtime belongs to existing KL05-31/32; server Streams coordinator belongs to existing KL11-32. No KL11-88 card exists at this pin.',
 89:'Client code/Admin belongs to existing KL05-31/33; server Streams coordinator belongs to existing KL11-32. No KL11-89 card exists at this pin.',
}
proposal_by_api={3:['Q-CAP-LegacyDiscovery0'],10:['Q-CAP-LegacyDiscovery0'],11:['Q-CAP-LegacyJoin0_1'],8:['Q-CAP-OffsetsUuid10'],9:['Q-CAP-OffsetsUuid10'],22:['Q-CAP-InitProducerId6','Q-DEC-PreparedTxnLifecycle'],45:['Q-CAP-Reassignment1'],66:['Q-FIX-ListTransactionsRouting','Q-CAP-ListTransactions2'],80:['Q-CAP-AddRaftVoter1']}
rows=[]
for key_s,inv in sorted(inventory.items(),key=lambda x:int(x[0])):
    key=int(key_s)
    classification='removed_current' if key in range(4,8) else 'broker_internal_with_Rust_extension' if key in {67,73} else 'broker_internal' if key in internal else 'public_client_and_internal' if key in {24,27,55,57} else 'public_client'
    existing=list(dict.fromkeys(client.get(key,[])+server[key]))
    assert all(c in task_map for c in existing)
    row=dict(api_key=key,name=inv['name'],upstream_versions=inv['versions'],classification=classification,client_declared_versions=declared.get(key,[]),client_declared_versions_are_static_not_execution=True,client_cards=client.get(key,[]),broker_cards=server[key],existing_card_statuses={c:task_map[c]['status'] for c in existing},qualification_cards=['KL01-03','KL01-14','KL11-48'],new_proposed_cards=proposal_by_api.get(key,[]),notes=notes.get(key,'Existing implementation/dependency mapping only. Broad parent or done task does not qualify every API version, public option, native peer or failure outcome.'),current_three_SDK_full_qualification='not_established_by_this_static_audit')
    row['missing_declared_versions_by_release']={v:[x for x in range(r[0],r[1]+1) if x not in declared.get(key,[])] if r is not None else [] for v,r in inv['versions'].items()}
    if key==24:row['client_applicable_versions']=[0,1,2,3];row['broker_only_versions']=[4,5]
    if key in {3,10,11}:
        row['lower_version_policy']='Unsupported below the declared client minimum, not an excluded API. Existing KL01-14 qualification must retain actual zero-application-frame/typed-refusal cells; no runtime pass inferred here.'
    if key in internal or key in range(4,8):row['missing_declared_versions_are_client_gaps']=False
    rows.append(row)
write('all-93-reconciliation.json',dict(source_sha=PIN,task_count=len(tasks),api_count=len(rows),rows=rows,qualification='STATIC MAPPING ONLY; full conformance remains blocked',separate_existing_broker_log_cards=['KL11-11','KL11-75'],separate_existing_raft_cards=['KL11-14','KL11-73','KL11-74','KL11-76','KL11-70']))
write('input-identities.json',dict(source_sha=PIN,git_inputs=git_inputs,archives={v:dict(commit=c,archive_sha256=h) for v,(c,h) in RELEASES.items()},scope='Source-only. No SDK, JVM, broker, Cargo or other product runtime execution. No repository mutation.'))
write('audit-counts.json',dict(api_rows=len(rows),tasks=len(tasks),request_response_schema_identities=len(all_schema_proofs),named_schema_objects=sum(len(x) for x in schema_proofs.values()),rust_evidence_files=len(rust_evidence),upstream_source_files=sum(sum(bool(x.get('present')) for x in r['sources'].values()) for r in upstream.values()),all_existing_task_references_valid=True,api_key_set=list(range(93)),source_pin=PIN,no_new_runtime_claim=True))
print(json.dumps(dict(rows=len(rows),schema_identities=len(all_schema_proofs),source_files=len(git_inputs),bytes=sum(p.stat().st_size for p in OUT.iterdir() if p.is_file()))))

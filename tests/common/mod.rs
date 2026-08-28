//! Mock Kafka broker for integration tests.
#![expect(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing,
    clippy::cast_possible_truncation,
    clippy::cast_possible_wrap,
    clippy::cast_sign_loss,
    reason = "mock broker is test-only; wire helpers use unwrap on trusted fixtures"
)]
#![expect(
    unreachable_pub,
    unused_results,
    reason = "mod common is private to each integration test binary; mock detaches accept loops"
)]
#![expect(
    clippy::let_underscore_must_use,
    reason = "mock discards frame length prefixes"
)]

use bytes::{BufMut, BytesMut};
use parking_lot::Mutex;
use partitionline::error;
use partitionline::group::assign_range_subscribed;
use partitionline::protocol::acl::{
    decode_create_acls_request, decode_delete_acls_request, decode_describe_acls_request,
    encode_create_acls_response, encode_delete_acls_response, encode_describe_acls_response,
    AclBinding,
};
use partitionline::protocol::admin::{
    decode_allocate_producer_ids_request, decode_alter_client_quotas_request,
    decode_alter_configs_request, decode_alter_partition_reassignments_request,
    decode_alter_replica_log_dirs_request, decode_alter_share_group_offsets_request,
    decode_alter_user_scram_credentials_request, decode_assign_replicas_to_dirs_request,
    decode_consumer_group_describe_request, decode_create_delegation_token_request,
    decode_create_partitions_request, decode_create_topics_request, decode_delete_groups_request,
    decode_delete_records_request, decode_delete_share_group_offsets_request,
    decode_delete_topics_request, decode_describe_client_quotas_request,
    decode_describe_cluster_request, decode_describe_configs_request,
    decode_describe_delegation_token_request, decode_describe_groups_request,
    decode_describe_log_dirs_request, decode_describe_producers_request,
    decode_describe_share_group_offsets_request, decode_describe_topic_partitions_request,
    decode_describe_transactions_request, decode_describe_user_scram_credentials_request,
    decode_expire_delegation_token_request, decode_get_telemetry_subscriptions_request,
    decode_incremental_alter_configs_request, decode_list_config_resources_request,
    decode_list_groups_request, decode_list_partition_reassignments_request,
    decode_list_transactions_request, decode_push_telemetry_request,
    decode_renew_delegation_token_request, decode_share_group_describe_request,
    decode_unregister_broker_request, decode_update_features_request,
    encode_allocate_producer_ids_response, encode_alter_client_quotas_response,
    encode_alter_configs_response, encode_alter_partition_reassignments_response,
    encode_alter_replica_log_dirs_response, encode_alter_share_group_offsets_response,
    encode_alter_user_scram_credentials_response, encode_assign_replicas_to_dirs_response,
    encode_consumer_group_describe_response, encode_create_delegation_token_response,
    encode_create_partitions_response, encode_create_topics_response,
    encode_delete_groups_response, encode_delete_records_response,
    encode_delete_share_group_offsets_response, encode_delete_topics_response,
    encode_describe_client_quotas_response, encode_describe_cluster_response,
    encode_describe_configs_response, encode_describe_delegation_token_response,
    encode_describe_groups_response, encode_describe_log_dirs_response,
    encode_describe_producers_response, encode_describe_share_group_offsets_response,
    encode_describe_topic_partitions_response, encode_describe_transactions_response,
    encode_describe_user_scram_credentials_response, encode_expire_delegation_token_response,
    encode_get_telemetry_subscriptions_response, encode_incremental_alter_configs_response,
    encode_list_config_resources_response, encode_list_groups_response,
    encode_list_partition_reassignments_response, encode_list_transactions_response,
    encode_push_telemetry_response, encode_renew_delegation_token_response,
    encode_share_group_describe_response, encode_unregister_broker_response,
    encode_update_features_response, ActiveProducer, AllocateProducerIdsResponse,
    AlterPartitionReassignmentsResponse, AlterReplicaLogDirsRequest, AlterReplicaLogDirsResponse,
    AlterReplicaLogDirsResponsePartition, AlterReplicaLogDirsResponseTopic,
    AlterUserScramCredentialsResult, AlteredShareGroupOffsets, AssignReplicasToDirsRequest,
    AssignReplicasToDirsResponse, AssignReplicasToDirsResponseDirectory,
    AssignReplicasToDirsResponsePartition, AssignReplicasToDirsResponseTopic,
    ClientQuotaAlterationResult, ClientQuotaEntity, ClientQuotaEntry, ClientQuotaFilterComponent,
    ClientQuotaValue, ClusterDescription, ConfigEntry, CreateDelegationTokenRequest,
    CreateDelegationTokenResponse, DeletableGroupResult, DeletedShareGroupOffsets,
    DescribeClientQuotasResponse, DescribeConfigsResult, DescribeDelegationTokenRequest,
    DescribeDelegationTokenResponse, DescribeLogDirsPartition, DescribeLogDirsRequest,
    DescribeLogDirsResponse, DescribeLogDirsResult, DescribeLogDirsTopic,
    DescribeProducersPartition, DescribeProducersResponse, DescribeProducersTopic,
    DescribeTopicPartitionsResponse, DescribeUserScramCredentialsResponse,
    DescribeUserScramCredentialsResult, DescribedConsumerGroup, DescribedGroup,
    DescribedShareGroup, DescribedShareGroupOffsets, DescribedTopicPartition,
    DescribedTopicPartitions, ExpireDelegationTokenRequest, ExpireDelegationTokenResponse,
    GetTelemetrySubscriptionsResponse, ListConfigResourcesResponse, ListGroupsResponse,
    ListPartitionReassignmentsResponse, ListTransactionsResponse, ListedConfigResource,
    ListedGroup, OngoingPartitionReassignment, OngoingTopicReassignment, PushTelemetryResponse,
    ReassignmentPartitionResult, ReassignmentTopicResult, RenewDelegationTokenRequest,
    RenewDelegationTokenResponse, ScramCredentialInfo, TopicPartitionCursor, TopicResult,
    TransactionListing, TransactionState, UnregisterBrokerResponse, UpdatableFeatureResult,
    UpdateFeaturesResponse, ALTER_CONFIG_DELETE, ALTER_CONFIG_SET, CONFIG_SOURCE_DEFAULT,
    CONFIG_SOURCE_DYNAMIC_TOPIC, RESOURCE_BROKER, RESOURCE_CLIENT_METRICS, RESOURCE_TOPIC,
};
use partitionline::protocol::api::{
    decode_metadata_request, decode_produce_request, encode_api_versions_response,
    encode_metadata_response, encode_produce_response, ApiVersion, ApiVersionsResponse, Broker,
    MetadataResponse, PartitionMetadata, ProducePartitionResponse, TopicMetadata,
};
use partitionline::protocol::api_keys::{
    ADD_OFFSETS_TO_TXN, ADD_PARTITIONS_TO_TXN, ALLOCATE_PRODUCER_IDS, ALTER_CLIENT_QUOTAS,
    ALTER_CONFIGS, ALTER_PARTITION_REASSIGNMENTS, ALTER_REPLICA_LOG_DIRS,
    ALTER_SHARE_GROUP_OFFSETS, ALTER_USER_SCRAM_CREDENTIALS, API_VERSIONS, ASSIGN_REPLICAS_TO_DIRS,
    CONSUMER_GROUP_DESCRIBE, CONSUMER_GROUP_HEARTBEAT, CREATE_ACLS, CREATE_DELEGATION_TOKEN,
    CREATE_PARTITIONS, CREATE_TOPICS, DELETE_ACLS, DELETE_GROUPS, DELETE_RECORDS,
    DELETE_SHARE_GROUP_OFFSETS, DELETE_TOPICS, DESCRIBE_ACLS, DESCRIBE_CLIENT_QUOTAS,
    DESCRIBE_CLUSTER, DESCRIBE_CONFIGS, DESCRIBE_DELEGATION_TOKEN, DESCRIBE_GROUPS,
    DESCRIBE_LOG_DIRS, DESCRIBE_PRODUCERS, DESCRIBE_SHARE_GROUP_OFFSETS, DESCRIBE_TOPIC_PARTITIONS,
    DESCRIBE_TRANSACTIONS, DESCRIBE_USER_SCRAM_CREDENTIALS, END_TXN, EXPIRE_DELEGATION_TOKEN,
    FETCH, FIND_COORDINATOR, GET_TELEMETRY_SUBSCRIPTIONS, HEARTBEAT, INCREMENTAL_ALTER_CONFIGS,
    INIT_PRODUCER_ID, JOIN_GROUP, LEAVE_GROUP, LIST_CONFIG_RESOURCES, LIST_GROUPS, LIST_OFFSETS,
    LIST_PARTITION_REASSIGNMENTS, LIST_TRANSACTIONS, METADATA, OFFSET_COMMIT, OFFSET_DELETE,
    OFFSET_FETCH, OFFSET_FOR_LEADER_EPOCH, PRODUCE, PUSH_TELEMETRY, RENEW_DELEGATION_TOKEN,
    SASL_AUTHENTICATE, SASL_HANDSHAKE, SHARE_ACKNOWLEDGE, SHARE_FETCH, SHARE_GROUP_DESCRIBE,
    SHARE_GROUP_HEARTBEAT, SYNC_GROUP, TXN_OFFSET_COMMIT, UNREGISTER_BROKER, UPDATE_FEATURES,
};
use partitionline::protocol::buf;
use partitionline::protocol::cgheartbeat::{
    decode_consumer_group_heartbeat_request, encode_consumer_group_heartbeat_response,
    ConsumerGroupHeartbeatResponse, TopicPartitions,
};
use partitionline::protocol::epoch::{
    decode_offset_for_leader_epoch_request, encode_offset_for_leader_epoch_response,
};
use partitionline::protocol::fetch::{
    decode_fetch_request, encode_fetch_response, FetchedPartition, FetchedTopic,
};
use partitionline::protocol::group::{
    decode_find_coordinator_request, decode_heartbeat_request, decode_join_group_request,
    decode_leave_group_request, decode_offset_commit_request, decode_offset_delete_request,
    decode_offset_fetch_request, decode_sync_group_request, encode_find_coordinator_response,
    encode_heartbeat_response, encode_join_group_response, encode_leave_group_response,
    encode_offset_commit_response, encode_offset_delete_response, encode_offset_fetch_response,
    encode_sync_group_response, FetchedOffset, FetchedOffsetTopic, JoinMember, OffsetDeleteResult,
    OffsetPartition, OffsetTopic, COORDINATOR_TRANSACTION,
};
use partitionline::protocol::header::{decode_request_header, encode_response_header};
use partitionline::protocol::idem::encode_init_producer_id_response;
use partitionline::protocol::oauth;
use partitionline::protocol::offsets::{
    decode_list_offsets_request, encode_list_offsets_response, ListOffsetsPartition,
    EARLIEST_TIMESTAMP, LATEST_TIMESTAMP,
};
use partitionline::protocol::records::{Record, RecordBatch};
use partitionline::protocol::sasl::{
    decode_sasl_authenticate_request, decode_sasl_handshake_request,
    encode_sasl_authenticate_response, encode_sasl_handshake_response, parse_plain_auth_bytes,
};
use partitionline::protocol::scram;
use partitionline::protocol::share::{
    decode_share_acknowledge_request, decode_share_fetch_request,
    decode_share_group_heartbeat_request, encode_share_acknowledge_response,
    encode_share_fetch_error, encode_share_fetch_response, encode_share_group_heartbeat_response,
    AcknowledgementBatch, AcquiredRange, ShareFetchedPartition, ShareFetchedTopic,
    ShareGroupHeartbeatResponse, ShareTopicPartitions, ACK_ACCEPT, ACK_REJECT,
};
use partitionline::protocol::txn::{
    decode_add_offsets_to_txn_request, decode_add_partitions_to_txn_request,
    decode_end_txn_request, decode_txn_offset_commit_request, encode_add_offsets_to_txn_response,
    encode_add_partitions_to_txn_response, encode_end_txn_response,
    encode_txn_offset_commit_response,
};
use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::sync::{watch, Notify};

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LastPushTelemetry {
    pub client_instance_id: [u8; 16],
    pub subscription_id: i32,
    pub terminating: bool,
    pub compression_type: i8,
    pub metrics: Vec<u8>,
}

#[derive(Clone)]
pub struct Mock {
    pub addr: String,
    state: Arc<Mutex<State>>,
}

#[derive(Clone)]
struct CreatedTopic {
    num_partitions: i32,
    configs: HashMap<String, Option<String>>,
}

#[derive(Clone)]
struct CommittedOffset {
    offset: i64,
    leader_epoch: i32,
    metadata: String,
}

struct State {
    log: HashMap<(String, i32), Vec<Record>>,
    next_offset: HashMap<(String, i32), i64>,
    committed: HashMap<(String, i32), CommittedOffset>,
    member_seq: u32,
    sasl_user: Option<(String, String)>,
    scram_user: Option<(scram::ScramAlg, String, String)>,
    oauth_principal: Option<String>,
    next_pid: i64,
    last_producer_id: Option<i64>,
    expected_seq: HashMap<(i64, String, i32), i32>,
    produce_error: Option<i16>,
    produce_error_left: Option<u32>,
    log_start: HashMap<(String, i32), i64>,
    created_topics: HashMap<String, CreatedTopic>,
    metadata_calls: u32,
    last_metadata_allow_auto: Option<bool>,
    brokers: Vec<Broker>,
    partition_leaders: HashMap<(String, i32), i32>,
    partition_epochs: HashMap<(String, i32), i32>,
    last_epoch_req: Option<(String, i32, i32)>,
    last_epoch_node: Option<i32>,
    epoch_not_leader: u32,
    last_list_offsets: Option<(String, i32, i32)>,
    last_list_offsets_node: Option<i32>,
    list_offsets_not_leader: u32,
    last_delete_records_node: Option<i32>,
    delete_records_not_leader: u32,
    last_describe_producers_node: Option<i32>,
    describe_producers_not_leader: u32,
    controller_node: i32,
    last_create_topics_node: Option<i32>,
    create_topics_not_controller: u32,
    last_delete_topics_node: Option<i32>,
    delete_topics_not_controller: u32,
    last_create_partitions_node: Option<i32>,
    create_partitions_not_controller: u32,
    last_incremental_alter_configs_node: Option<i32>,
    incremental_alter_configs_not_controller: u32,
    last_create_acls_node: Option<i32>,
    create_acls_not_controller: u32,
    last_alter_reassignments_node: Option<i32>,
    alter_reassignments_not_controller: u32,
    last_reassignment: Option<(String, i32, Option<Vec<i32>>)>,
    reassignments: HashMap<(String, i32), Vec<i32>>,
    last_list_reassignments_node: Option<i32>,
    list_reassignments_not_controller: u32,
    last_update_features_node: Option<i32>,
    update_features_not_controller: u32,
    last_feature_update: Option<(String, i16, bool)>,
    features: HashMap<String, i16>,
    last_alter_user_scram_node: Option<i32>,
    alter_user_scram_not_controller: u32,
    last_describe_user_scram_node: Option<i32>,
    describe_user_scram_not_controller: u32,
    last_unregister_broker_node: Option<i32>,
    unregister_broker_not_controller: u32,
    last_unregistered_broker_id: Option<i32>,
    // Fixture broker ids only. Not a live KRaft unregistration.
    unregistered_brokers: HashSet<i32>,
    last_scram_upsert: Option<(String, i8, i32)>,
    last_scram_delete: Option<(String, i8)>,
    scram_users: HashMap<(String, i8), i32>,
    last_describe_client_quotas_node: Option<i32>,
    last_describe_client_quotas: Option<(Vec<ClientQuotaFilterComponent>, bool)>,
    last_alter_client_quotas_node: Option<i32>,
    alter_client_quotas_not_controller: u32,
    last_quota_upsert: Option<(String, Option<String>, String, f64)>,
    last_quota_delete: Option<(String, Option<String>, String)>,
    // Fixture entity/ops only. Not a real cluster quota store.
    quota_fixtures: HashMap<(String, Option<String>, String), f64>,
    last_allocate_producer_ids_node: Option<i32>,
    allocate_producer_ids_not_controller: u32,
    // Fixture broker id/epoch + sequential blocks. Not a real PID allocator.
    last_allocate_producer_ids: Option<(i32, i64, i64, i32)>,
    next_producer_id_block_start: i64,
    last_describe_transactions_node: Option<i32>,
    describe_transactions_not_coordinator: u32,
    last_list_transactions_node: Option<i32>,
    list_transactions_not_coordinator: u32,
    // Fixture transactional ids only. Not a live txn coordinator store.
    txn_fixtures: HashMap<String, TransactionState>,
    last_offset_delete_node: Option<i32>,
    offset_delete_not_coordinator: u32,
    last_consumer_group_describe_node: Option<i32>,
    consumer_group_describe_not_coordinator: u32,
    last_describe_groups_node: Option<i32>,
    describe_groups_not_coordinator: u32,
    last_list_groups_node: Option<i32>,
    last_list_groups: Option<(Vec<String>, Vec<String>)>,
    last_delete_groups_node: Option<i32>,
    delete_groups_not_coordinator: u32,
    last_share_group_describe_node: Option<i32>,
    share_group_describe_not_coordinator: u32,
    last_describe_share_group_offsets_node: Option<i32>,
    describe_share_group_offsets_not_coordinator: u32,
    last_alter_share_group_offsets_node: Option<i32>,
    alter_share_group_offsets_not_coordinator: u32,
    last_delete_share_group_offsets_node: Option<i32>,
    delete_share_group_offsets_not_coordinator: u32,
    last_describe_topic_partitions_node: Option<i32>,
    last_describe_topic_partitions: Option<(Vec<String>, i32, Option<TopicPartitionCursor>)>,
    last_list_config_resources_node: Option<i32>,
    last_list_config_resources: Option<Vec<i8>>,
    last_get_telemetry_subscriptions_node: Option<i32>,
    last_get_telemetry_subscriptions: Option<[u8; 16]>,
    last_push_telemetry_node: Option<i32>,
    last_push_telemetry: Option<LastPushTelemetry>,
    last_assign_replicas_to_dirs_node: Option<i32>,
    assign_replicas_to_dirs_not_controller: u32,
    last_assign_replicas_to_dirs: Option<AssignReplicasToDirsRequest>,
    last_alter_replica_log_dirs_node: Option<i32>,
    last_alter_replica_log_dirs: Option<AlterReplicaLogDirsRequest>,
    last_describe_log_dirs_node: Option<i32>,
    last_describe_log_dirs: Option<DescribeLogDirsRequest>,
    last_create_delegation_token_node: Option<i32>,
    last_create_delegation_token: Option<CreateDelegationTokenRequest>,
    last_renew_delegation_token_node: Option<i32>,
    last_renew_delegation_token: Option<RenewDelegationTokenRequest>,
    last_expire_delegation_token_node: Option<i32>,
    last_expire_delegation_token: Option<ExpireDelegationTokenRequest>,
    last_describe_delegation_token_node: Option<i32>,
    last_describe_delegation_token: Option<DescribeDelegationTokenRequest>,
    accepted_produce: Vec<i32>,
    produce_requests: Vec<i32>,
    accepted_fetch: Vec<i32>,
    groups: HashMap<String, GroupReg>,
    assign_notify: Arc<Notify>,
    last_fetch_isolation: i8,
    last_fetch_rack: String,
    last_fetch_max_bytes: i32,
    last_fetch_partition_max_bytes: i32,
    last_group_instance_id: Option<String>,
    last_group_rack: Option<String>,
    in_txn: bool,
    txn_pending: Vec<(String, i32, i64)>,
    txn_aborted: HashSet<(String, i32, i64)>,
    log_producer: HashMap<(String, i32, i64), i64>,
    last_produce_txn_id: Option<String>,
    acls: Vec<AclBinding>,
    join_group_calls: u32,
    cg_heartbeat_calls: u32,
    sync_group_calls: u32,
    share_heartbeat_calls: u32,
    share_fetch_calls: u32,
    share_ack_calls: u32,
    share_accepted: HashSet<(String, i32, i64)>,
    share_acquired: HashMap<(String, i32, i64), String>,
    share_epochs: HashMap<String, i32>,
    last_share_fetch_epoch: Option<i32>,
    last_share_ack_epoch: Option<i32>,
    last_share_ack_partitions: usize,
    last_share_fetch_node: Option<i32>,
    last_share_ack_node: Option<i32>,
    share_fetch_not_leader: u32,
    offset_commit_calls: u32,
    offset_fetch_calls: u32,
    last_offset_commit_partitions: usize,
    last_offset_fetch_partitions: usize,
    last_offset_commit_node: Option<i32>,
    offset_commit_not_coordinator: u32,
    offset_commit_load_left: u32,
    offset_commit_load_in_progress: u32,
    add_partitions_to_txn_calls: u32,
    last_add_partitions_to_txn: usize,
    txn_offset_commit_calls: u32,
    last_txn_offset_commit_partitions: usize,
    last_txn_offset_epochs: Vec<i32>,
    drop_gen: watch::Sender<u32>,
    refuse_conns: u32,
    accepts: u32,
    coord_node: i32,
    txn_coord_node: i32,
    find_coordinator_key_types: Vec<i8>,
    last_init_producer_id_node: Option<i32>,
    last_init_producer_id_timeout: Option<i32>,
    init_producer_id_nodes: Vec<i32>,
    init_producer_id_not_coordinator: u32,
    stale_txn_finds: u32,
    last_add_partitions_node: Option<i32>,
    last_add_offsets_node: Option<i32>,
    last_end_txn_node: Option<i32>,
    last_txn_offset_commit_node: Option<i32>,
    hb_by_node: HashMap<i32, u32>,
    kip848_groups: HashMap<String, Kip848Reg>,
}

#[derive(Default)]
struct Kip848Reg {
    members: BTreeMap<String, Kip848Member>,
}

struct Kip848Member {
    topics: Vec<String>,
    epoch: i32,
    partitions: Vec<(String, i32)>,
    pending: bool,
}

struct GroupReg {
    members: BTreeMap<String, Vec<u8>>,
    generation: i32,
    joined: HashSet<String>,
    assignments: HashMap<String, Vec<u8>>,
    hb_total: u32,
}

fn new_state(
    sasl_user: Option<(String, String)>,
    scram_user: Option<(scram::ScramAlg, String, String)>,
    oauth_principal: Option<String>,
) -> State {
    let mut created_topics = HashMap::new();
    created_topics.insert(
        "t".into(),
        CreatedTopic {
            num_partitions: 1,
            configs: HashMap::new(),
        },
    );
    State {
        log: HashMap::new(),
        next_offset: HashMap::new(),
        committed: HashMap::new(),
        member_seq: 0,
        sasl_user,
        scram_user,
        oauth_principal,
        next_pid: 1000,
        last_producer_id: None,
        expected_seq: HashMap::new(),
        produce_error: None,
        produce_error_left: None,
        log_start: HashMap::new(),
        created_topics,
        metadata_calls: 0,
        last_metadata_allow_auto: None,
        brokers: Vec::new(),
        partition_leaders: HashMap::new(),
        partition_epochs: HashMap::new(),
        last_epoch_req: None,
        last_epoch_node: None,
        epoch_not_leader: 0,
        last_list_offsets: None,
        last_list_offsets_node: None,
        list_offsets_not_leader: 0,
        last_delete_records_node: None,
        delete_records_not_leader: 0,
        last_describe_producers_node: None,
        describe_producers_not_leader: 0,
        controller_node: 1,
        last_create_topics_node: None,
        create_topics_not_controller: 0,
        last_delete_topics_node: None,
        delete_topics_not_controller: 0,
        last_create_partitions_node: None,
        create_partitions_not_controller: 0,
        last_incremental_alter_configs_node: None,
        incremental_alter_configs_not_controller: 0,
        last_create_acls_node: None,
        create_acls_not_controller: 0,
        last_alter_reassignments_node: None,
        alter_reassignments_not_controller: 0,
        last_reassignment: None,
        reassignments: HashMap::new(),
        last_list_reassignments_node: None,
        list_reassignments_not_controller: 0,
        last_update_features_node: None,
        update_features_not_controller: 0,
        last_feature_update: None,
        features: HashMap::new(),
        last_alter_user_scram_node: None,
        alter_user_scram_not_controller: 0,
        last_describe_user_scram_node: None,
        describe_user_scram_not_controller: 0,
        last_unregister_broker_node: None,
        unregister_broker_not_controller: 0,
        last_unregistered_broker_id: None,
        unregistered_brokers: HashSet::new(),
        last_scram_upsert: None,
        last_scram_delete: None,
        scram_users: HashMap::new(),
        last_describe_client_quotas_node: None,
        last_describe_client_quotas: None,
        last_alter_client_quotas_node: None,
        alter_client_quotas_not_controller: 0,
        last_quota_upsert: None,
        last_quota_delete: None,
        quota_fixtures: HashMap::new(),
        last_allocate_producer_ids_node: None,
        allocate_producer_ids_not_controller: 0,
        last_allocate_producer_ids: None,
        next_producer_id_block_start: 1000,
        last_describe_transactions_node: None,
        describe_transactions_not_coordinator: 0,
        last_list_transactions_node: None,
        list_transactions_not_coordinator: 0,
        txn_fixtures: HashMap::new(),
        last_offset_delete_node: None,
        offset_delete_not_coordinator: 0,
        last_consumer_group_describe_node: None,
        consumer_group_describe_not_coordinator: 0,
        last_describe_groups_node: None,
        describe_groups_not_coordinator: 0,
        last_list_groups_node: None,
        last_list_groups: None,
        last_delete_groups_node: None,
        delete_groups_not_coordinator: 0,
        last_share_group_describe_node: None,
        share_group_describe_not_coordinator: 0,
        last_describe_share_group_offsets_node: None,
        describe_share_group_offsets_not_coordinator: 0,
        last_alter_share_group_offsets_node: None,
        alter_share_group_offsets_not_coordinator: 0,
        last_delete_share_group_offsets_node: None,
        delete_share_group_offsets_not_coordinator: 0,
        last_describe_topic_partitions_node: None,
        last_describe_topic_partitions: None,
        last_list_config_resources_node: None,
        last_list_config_resources: None,
        last_get_telemetry_subscriptions_node: None,
        last_get_telemetry_subscriptions: None,
        last_push_telemetry_node: None,
        last_push_telemetry: None,
        last_assign_replicas_to_dirs_node: None,
        assign_replicas_to_dirs_not_controller: 0,
        last_assign_replicas_to_dirs: None,
        last_alter_replica_log_dirs_node: None,
        last_alter_replica_log_dirs: None,
        last_describe_log_dirs_node: None,
        last_describe_log_dirs: None,
        last_create_delegation_token_node: None,
        last_create_delegation_token: None,
        last_renew_delegation_token_node: None,
        last_renew_delegation_token: None,
        last_expire_delegation_token_node: None,
        last_expire_delegation_token: None,
        last_describe_delegation_token_node: None,
        last_describe_delegation_token: None,
        accepted_produce: Vec::new(),
        produce_requests: Vec::new(),
        accepted_fetch: Vec::new(),
        groups: HashMap::new(),
        assign_notify: Arc::new(Notify::new()),
        last_fetch_isolation: 0,
        last_fetch_rack: String::new(),
        last_fetch_max_bytes: 0,
        last_fetch_partition_max_bytes: 0,
        last_group_instance_id: None,
        last_group_rack: None,
        in_txn: false,
        txn_pending: Vec::new(),
        txn_aborted: HashSet::new(),
        log_producer: HashMap::new(),
        last_produce_txn_id: None,
        acls: Vec::new(),
        join_group_calls: 0,
        cg_heartbeat_calls: 0,
        sync_group_calls: 0,
        share_heartbeat_calls: 0,
        share_fetch_calls: 0,
        share_ack_calls: 0,
        share_accepted: HashSet::new(),
        share_acquired: HashMap::new(),
        share_epochs: HashMap::new(),
        last_share_fetch_epoch: None,
        last_share_ack_epoch: None,
        last_share_ack_partitions: 0,
        last_share_fetch_node: None,
        last_share_ack_node: None,
        share_fetch_not_leader: 0,
        offset_commit_calls: 0,
        offset_fetch_calls: 0,
        last_offset_commit_partitions: 0,
        last_offset_fetch_partitions: 0,
        last_offset_commit_node: None,
        offset_commit_not_coordinator: 0,
        offset_commit_load_left: 0,
        offset_commit_load_in_progress: 0,
        add_partitions_to_txn_calls: 0,
        last_add_partitions_to_txn: 0,
        txn_offset_commit_calls: 0,
        last_txn_offset_commit_partitions: 0,
        last_txn_offset_epochs: Vec::new(),
        drop_gen: watch::channel(0).0,
        refuse_conns: 0,
        accepts: 0,
        coord_node: 1,
        txn_coord_node: 1,
        find_coordinator_key_types: Vec::new(),
        last_init_producer_id_node: None,
        last_init_producer_id_timeout: None,
        init_producer_id_nodes: Vec::new(),
        init_producer_id_not_coordinator: 0,
        stale_txn_finds: 0,
        last_add_partitions_node: None,
        last_add_offsets_node: None,
        last_end_txn_node: None,
        last_txn_offset_commit_node: None,
        hb_by_node: HashMap::new(),
        kip848_groups: HashMap::new(),
    }
}

fn mock_topic_id(name: &str) -> [u8; 16] {
    let mut id = [0u8; 16];
    let bytes = name.as_bytes();
    let n = bytes.len().min(16);
    if let Some(dst) = id.get_mut(..n) {
        if let Some(src) = bytes.get(..n) {
            dst.copy_from_slice(src);
        }
    }
    id
}

fn kip848_topic_partitions(parts: &[(String, i32)]) -> Vec<TopicPartitions> {
    let mut by_topic: Vec<(String, Vec<i32>)> = Vec::new();
    for (topic, part) in parts {
        match by_topic.iter_mut().find(|(t, _)| t == topic) {
            Some((_, ps)) => ps.push(*part),
            None => by_topic.push((topic.clone(), vec![*part])),
        }
    }
    by_topic
        .into_iter()
        .map(|(topic, partitions)| TopicPartitions {
            topic_id: mock_topic_id(&topic),
            partitions,
        })
        .collect()
}

fn kip848_recompute(st: &mut State, group_id: &str) {
    let Some(g) = st.kip848_groups.get(group_id) else {
        return;
    };
    if g.members.is_empty() {
        return;
    }
    let mut topic_names = Vec::new();
    for m in g.members.values() {
        for t in &m.topics {
            if !topic_names.iter().any(|x| x == t) {
                topic_names.push(t.clone());
            }
        }
    }
    if topic_names.is_empty() {
        topic_names.push("t".into());
    }
    let member_subs: Vec<(String, Vec<String>)> = g
        .members
        .iter()
        .map(|(id, m)| {
            let topics = if m.topics.is_empty() {
                topic_names.clone()
            } else {
                m.topics.clone()
            };
            (id.clone(), topics)
        })
        .collect();
    let mut topic_parts = Vec::with_capacity(topic_names.len());
    for topic in &topic_names {
        let npart = st
            .created_topics
            .get(topic)
            .map(|s| s.num_partitions)
            .unwrap_or(1);
        topic_parts.push((topic.clone(), (0..npart).collect()));
    }
    let assigned = assign_range_subscribed(&member_subs, &topic_parts);
    let Some(g) = st.kip848_groups.get_mut(group_id) else {
        return;
    };
    for (id, m) in &mut g.members {
        let new_parts = assigned.get(id).cloned().unwrap_or_default();
        if new_parts != m.partitions {
            m.partitions = new_parts;
            m.epoch = m.epoch.saturating_add(1).max(1);
            m.pending = true;
        }
    }
}

fn metadata_for(st: &State, fallback_host: &str, fallback_port: i32) -> MetadataResponse {
    let brokers = if st.brokers.is_empty() {
        vec![Broker {
            node_id: 1,
            host: fallback_host.to_string(),
            port: fallback_port,
            rack: None,
        }]
    } else {
        st.brokers.clone()
    };
    let replica_nodes: Vec<i32> = brokers.iter().map(|b| b.node_id).collect();
    let default_leader = brokers.first().map(|b| b.node_id).unwrap_or(1);
    let controller_id = if st.controller_node >= 0 {
        st.controller_node
    } else {
        default_leader
    };
    MetadataResponse {
        throttle_time_ms: 0,
        brokers,
        cluster_id: Some("mock".into()),
        controller_id,
        topics: st
            .created_topics
            .iter()
            .map(|(name, spec)| TopicMetadata {
                error_code: 0,
                name: Some(name.clone()),
                topic_id: mock_topic_id(name),
                is_internal: false,
                partitions: (0..spec.num_partitions)
                    .map(|i| {
                        let leader_id = st
                            .partition_leaders
                            .get(&(name.clone(), i))
                            .copied()
                            .unwrap_or(default_leader);
                        PartitionMetadata {
                            error_code: 0,
                            partition_index: i,
                            leader_id,
                            leader_epoch: st
                                .partition_epochs
                                .get(&(name.clone(), i))
                                .copied()
                                .unwrap_or(0),
                            replica_nodes: replica_nodes.clone(),
                            isr_nodes: replica_nodes.clone(),
                            offline_replicas: Vec::new(),
                        }
                    })
                    .collect(),
            })
            .collect(),
    }
}

fn spawn_plain(listener: TcpListener, node_id: i32, state: Arc<Mutex<State>>) {
    tokio::spawn(async move {
        loop {
            let Ok((stream, _)) = listener.accept().await else {
                break;
            };
            if take_refuse(&state) {
                continue;
            }
            note_accept(&state);
            stream.set_nodelay(true).ok();
            let st = state.clone();
            tokio::spawn(handle_conn(stream, node_id, st));
        }
    });
}

fn take_refuse(state: &Mutex<State>) -> bool {
    let mut st = state.lock();
    if st.refuse_conns > 0 {
        st.refuse_conns -= 1;
        true
    } else {
        false
    }
}

fn note_accept(state: &Mutex<State>) {
    let mut st = state.lock();
    st.accepts = st.accepts.saturating_add(1);
}

fn broker_host_port(st: &State, node_id: i32) -> (String, i32) {
    st.brokers
        .iter()
        .find(|b| b.node_id == node_id)
        .map(|b| (b.host.clone(), b.port))
        .unwrap_or_else(|| ("127.0.0.1".into(), 0))
}

impl Mock {
    pub async fn start() -> Self {
        Self::start_with_sasl(None).await
    }

    pub async fn start_with_sasl(creds: Option<(String, String)>) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let port = addr.port() as i32;
        let mut st = new_state(creds, None, None);
        st.brokers = vec![Broker {
            node_id: 1,
            host: "127.0.0.1".into(),
            port,
            rack: None,
        }];
        let state = Arc::new(Mutex::new(st));
        spawn_plain(listener, 1, state.clone());
        Self {
            addr: format!("127.0.0.1:{}", addr.port()),
            state,
        }
    }

    pub async fn start_two_node() -> Self {
        let l1 = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let l2 = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let a1 = l1.local_addr().unwrap();
        let a2 = l2.local_addr().unwrap();
        let mut st = new_state(None, None, None);
        st.brokers = vec![
            Broker {
                node_id: 1,
                host: "127.0.0.1".into(),
                port: a1.port() as i32,
                rack: Some("r1".into()),
            },
            Broker {
                node_id: 2,
                host: "127.0.0.1".into(),
                port: a2.port() as i32,
                rack: Some("r2".into()),
            },
        ];
        st.partition_leaders.insert(("t".into(), 0), 2);
        let state = Arc::new(Mutex::new(st));
        spawn_plain(l1, 1, state.clone());
        spawn_plain(l2, 2, state.clone());
        Self {
            addr: format!("127.0.0.1:{}", a1.port()),
            state,
        }
    }

    pub async fn start_with_scram(creds: (String, String)) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let port = addr.port() as i32;
        let mut st = new_state(
            None,
            Some((scram::ScramAlg::Sha256, creds.0, creds.1)),
            None,
        );
        st.brokers = vec![Broker {
            node_id: 1,
            host: "127.0.0.1".into(),
            port,
            rack: None,
        }];
        let state = Arc::new(Mutex::new(st));
        spawn_plain(listener, 1, state.clone());
        Self {
            addr: format!("127.0.0.1:{}", addr.port()),
            state,
        }
    }

    pub async fn start_with_scram_sha512(creds: (String, String)) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let port = addr.port() as i32;
        let mut st = new_state(
            None,
            Some((scram::ScramAlg::Sha512, creds.0, creds.1)),
            None,
        );
        st.brokers = vec![Broker {
            node_id: 1,
            host: "127.0.0.1".into(),
            port,
            rack: None,
        }];
        let state = Arc::new(Mutex::new(st));
        spawn_plain(listener, 1, state.clone());
        Self {
            addr: format!("127.0.0.1:{}", addr.port()),
            state,
        }
    }

    pub async fn start_with_oauthbearer(principal: String) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let port = addr.port() as i32;
        let mut st = new_state(None, None, Some(principal));
        st.brokers = vec![Broker {
            node_id: 1,
            host: "127.0.0.1".into(),
            port,
            rack: None,
        }];
        let state = Arc::new(Mutex::new(st));
        spawn_plain(listener, 1, state.clone());
        Self {
            addr: format!("127.0.0.1:{}", addr.port()),
            state,
        }
    }

    pub async fn start_tls() -> (Self, partitionline::TlsConfig) {
        partitionline::net::install_crypto_provider();
        let (server, ca_pem) = tls_server_identity();
        let acceptor = tokio_rustls::TlsAcceptor::from(std::sync::Arc::new(server));
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let port = addr.port() as i32;
        let mut st = new_state(None, None, None);
        st.brokers = vec![Broker {
            node_id: 1,
            host: "127.0.0.1".into(),
            port,
            rack: None,
        }];
        let state = Arc::new(Mutex::new(st));
        let st = state.clone();
        tokio::spawn(async move {
            loop {
                let Ok((tcp, _)) = listener.accept().await else {
                    break;
                };
                if take_refuse(&st) {
                    continue;
                }
                note_accept(&st);
                tcp.set_nodelay(true).ok();
                let st = st.clone();
                let acceptor = acceptor.clone();
                tokio::spawn(async move {
                    let Ok(stream) = acceptor.accept(tcp).await else {
                        return;
                    };
                    handle_conn(stream, 1, st).await;
                });
            }
        });
        let tls = partitionline::TlsConfig {
            ca_pem: Some(ca_pem),
            client_cert_pem: None,
            client_key_pem: None,
            server_name: Some("localhost".into()),
        };
        (
            Self {
                addr: format!("127.0.0.1:{}", addr.port()),
                state,
            },
            tls,
        )
    }

    pub fn last_producer_id(&self) -> Option<i64> {
        self.state.lock().last_producer_id
    }

    pub fn set_log_start(&self, topic: &str, partition: i32, offset: i64) {
        self.state
            .lock()
            .log_start
            .insert((topic.to_string(), partition), offset);
    }

    pub fn log_len(&self, topic: &str, partition: i32) -> usize {
        self.state
            .lock()
            .log
            .get(&(topic.to_string(), partition))
            .map(|v| v.len())
            .unwrap_or(0)
    }

    pub fn metadata_calls(&self) -> u32 {
        self.state.lock().metadata_calls
    }

    pub fn last_metadata_allow_auto(&self) -> Option<bool> {
        self.state.lock().last_metadata_allow_auto
    }

    pub fn set_produce_error(&self, code: i16) {
        let mut st = self.state.lock();
        st.produce_error = Some(code);
        st.produce_error_left = None;
    }

    pub fn set_produce_error_times(&self, code: i16, n: u32) {
        let mut st = self.state.lock();
        st.produce_error = Some(code);
        st.produce_error_left = Some(n);
    }

    pub fn produce_nodes(&self) -> Vec<i32> {
        self.state.lock().accepted_produce.clone()
    }

    pub fn produce_request_nodes(&self) -> Vec<i32> {
        self.state.lock().produce_requests.clone()
    }

    pub fn set_partition_leader(&self, topic: &str, partition: i32, node_id: i32) {
        let mut st = self.state.lock();
        st.partition_leaders
            .insert((topic.to_string(), partition), node_id);
        let slot = st
            .partition_epochs
            .entry((topic.to_string(), partition))
            .or_insert(0);
        *slot += 1;
    }

    pub fn fetch_nodes(&self) -> Vec<i32> {
        self.state.lock().accepted_fetch.clone()
    }

    pub fn last_fetch_isolation(&self) -> i8 {
        self.state.lock().last_fetch_isolation
    }

    pub fn last_fetch_rack(&self) -> String {
        self.state.lock().last_fetch_rack.clone()
    }

    pub fn last_fetch_max_bytes(&self) -> i32 {
        self.state.lock().last_fetch_max_bytes
    }

    pub fn last_fetch_partition_max_bytes(&self) -> i32 {
        self.state.lock().last_fetch_partition_max_bytes
    }

    pub fn last_group_instance_id(&self) -> Option<String> {
        self.state.lock().last_group_instance_id.clone()
    }

    pub fn last_group_rack(&self) -> Option<String> {
        self.state.lock().last_group_rack.clone()
    }

    pub fn last_produce_txn_id(&self) -> Option<String> {
        self.state.lock().last_produce_txn_id.clone()
    }

    pub fn bump_leader_epoch(&self, topic: &str, partition: i32) -> i32 {
        let mut st = self.state.lock();
        let slot = st
            .partition_epochs
            .entry((topic.to_string(), partition))
            .or_insert(0);
        *slot += 1;
        *slot
    }

    pub fn last_offset_for_leader_epoch(&self) -> Option<(String, i32, i32)> {
        self.state.lock().last_epoch_req.clone()
    }

    pub fn last_offset_for_leader_epoch_node(&self) -> Option<i32> {
        self.state.lock().last_epoch_node
    }

    pub fn offset_for_leader_epoch_not_leader(&self) -> u32 {
        self.state.lock().epoch_not_leader
    }

    pub fn last_list_offsets(&self) -> Option<(String, i32, i32)> {
        self.state.lock().last_list_offsets.clone()
    }

    pub fn last_list_offsets_node(&self) -> Option<i32> {
        self.state.lock().last_list_offsets_node
    }

    pub fn list_offsets_not_leader(&self) -> u32 {
        self.state.lock().list_offsets_not_leader
    }

    pub fn last_delete_records_node(&self) -> Option<i32> {
        self.state.lock().last_delete_records_node
    }

    pub fn delete_records_not_leader(&self) -> u32 {
        self.state.lock().delete_records_not_leader
    }

    pub fn last_describe_producers_node(&self) -> Option<i32> {
        self.state.lock().last_describe_producers_node
    }

    pub fn describe_producers_not_leader(&self) -> u32 {
        self.state.lock().describe_producers_not_leader
    }

    pub fn set_controller(&self, node_id: i32) {
        self.state.lock().controller_node = node_id;
    }

    pub fn last_create_topics_node(&self) -> Option<i32> {
        self.state.lock().last_create_topics_node
    }

    pub fn create_topics_not_controller(&self) -> u32 {
        self.state.lock().create_topics_not_controller
    }

    pub fn last_delete_topics_node(&self) -> Option<i32> {
        self.state.lock().last_delete_topics_node
    }

    pub fn delete_topics_not_controller(&self) -> u32 {
        self.state.lock().delete_topics_not_controller
    }

    pub fn last_create_partitions_node(&self) -> Option<i32> {
        self.state.lock().last_create_partitions_node
    }

    pub fn create_partitions_not_controller(&self) -> u32 {
        self.state.lock().create_partitions_not_controller
    }

    pub fn last_incremental_alter_configs_node(&self) -> Option<i32> {
        self.state.lock().last_incremental_alter_configs_node
    }

    pub fn incremental_alter_configs_not_controller(&self) -> u32 {
        self.state.lock().incremental_alter_configs_not_controller
    }

    pub fn last_create_acls_node(&self) -> Option<i32> {
        self.state.lock().last_create_acls_node
    }

    pub fn create_acls_not_controller(&self) -> u32 {
        self.state.lock().create_acls_not_controller
    }

    pub fn last_alter_reassignments_node(&self) -> Option<i32> {
        self.state.lock().last_alter_reassignments_node
    }

    pub fn alter_reassignments_not_controller(&self) -> u32 {
        self.state.lock().alter_reassignments_not_controller
    }

    pub fn last_reassignment(&self) -> Option<(String, i32, Option<Vec<i32>>)> {
        self.state.lock().last_reassignment.clone()
    }

    pub fn last_list_reassignments_node(&self) -> Option<i32> {
        self.state.lock().last_list_reassignments_node
    }

    pub fn list_reassignments_not_controller(&self) -> u32 {
        self.state.lock().list_reassignments_not_controller
    }

    pub fn last_update_features_node(&self) -> Option<i32> {
        self.state.lock().last_update_features_node
    }

    pub fn update_features_not_controller(&self) -> u32 {
        self.state.lock().update_features_not_controller
    }

    pub fn last_feature_update(&self) -> Option<(String, i16, bool)> {
        self.state.lock().last_feature_update.clone()
    }

    pub fn feature_level(&self, name: &str) -> Option<i16> {
        self.state.lock().features.get(name).copied()
    }

    pub fn last_alter_user_scram_node(&self) -> Option<i32> {
        self.state.lock().last_alter_user_scram_node
    }

    pub fn alter_user_scram_not_controller(&self) -> u32 {
        self.state.lock().alter_user_scram_not_controller
    }

    pub fn last_describe_user_scram_node(&self) -> Option<i32> {
        self.state.lock().last_describe_user_scram_node
    }

    pub fn describe_user_scram_not_controller(&self) -> u32 {
        self.state.lock().describe_user_scram_not_controller
    }

    pub fn last_unregister_broker_node(&self) -> Option<i32> {
        self.state.lock().last_unregister_broker_node
    }

    pub fn unregister_broker_not_controller(&self) -> u32 {
        self.state.lock().unregister_broker_not_controller
    }

    pub fn last_unregistered_broker_id(&self) -> Option<i32> {
        self.state.lock().last_unregistered_broker_id
    }

    pub fn has_unregistered_broker(&self, broker_id: i32) -> bool {
        self.state.lock().unregistered_brokers.contains(&broker_id)
    }

    /// Fixture user/mechanism/iterations only. Not a credential store.
    pub fn set_scram_fixture(&self, name: &str, mechanism: i8, iterations: i32) {
        let mut st = self.state.lock();
        let _ = st
            .scram_users
            .insert((name.to_string(), mechanism), iterations);
    }

    pub fn last_scram_upsert(&self) -> Option<(String, i8, i32)> {
        self.state.lock().last_scram_upsert.clone()
    }

    pub fn last_scram_delete(&self) -> Option<(String, i8)> {
        self.state.lock().last_scram_delete.clone()
    }

    pub fn has_scram_credential(&self, name: &str, mechanism: i8) -> bool {
        self.state
            .lock()
            .scram_users
            .contains_key(&(name.to_string(), mechanism))
    }

    pub fn last_describe_client_quotas_node(&self) -> Option<i32> {
        self.state.lock().last_describe_client_quotas_node
    }

    pub fn last_describe_client_quotas(&self) -> Option<(Vec<ClientQuotaFilterComponent>, bool)> {
        self.state.lock().last_describe_client_quotas.clone()
    }

    pub fn last_alter_client_quotas_node(&self) -> Option<i32> {
        self.state.lock().last_alter_client_quotas_node
    }

    pub fn alter_client_quotas_not_controller(&self) -> u32 {
        self.state.lock().alter_client_quotas_not_controller
    }

    pub fn last_quota_upsert(&self) -> Option<(String, Option<String>, String, f64)> {
        self.state.lock().last_quota_upsert.clone()
    }

    pub fn last_quota_delete(&self) -> Option<(String, Option<String>, String)> {
        self.state.lock().last_quota_delete.clone()
    }

    pub fn last_allocate_producer_ids_node(&self) -> Option<i32> {
        self.state.lock().last_allocate_producer_ids_node
    }

    pub fn allocate_producer_ids_not_controller(&self) -> u32 {
        self.state.lock().allocate_producer_ids_not_controller
    }

    pub fn last_allocate_producer_ids(&self) -> Option<(i32, i64, i64, i32)> {
        self.state.lock().last_allocate_producer_ids
    }

    pub fn last_describe_transactions_node(&self) -> Option<i32> {
        self.state.lock().last_describe_transactions_node
    }

    pub fn describe_transactions_not_coordinator(&self) -> u32 {
        self.state.lock().describe_transactions_not_coordinator
    }

    pub fn last_list_transactions_node(&self) -> Option<i32> {
        self.state.lock().last_list_transactions_node
    }

    pub fn list_transactions_not_coordinator(&self) -> u32 {
        self.state.lock().list_transactions_not_coordinator
    }

    pub fn set_txn_fixture(&self, state: TransactionState) {
        let mut st = self.state.lock();
        let _ = st
            .txn_fixtures
            .insert(state.transactional_id.clone(), state);
    }

    pub fn has_quota_fixture(&self, entity_type: &str, name: Option<&str>, key: &str) -> bool {
        self.state.lock().quota_fixtures.contains_key(&(
            entity_type.to_string(),
            name.map(str::to_string),
            key.to_string(),
        ))
    }

    pub fn scram_iterations(&self, name: &str, mechanism: i8) -> Option<i32> {
        self.state
            .lock()
            .scram_users
            .get(&(name.to_string(), mechanism))
            .copied()
    }

    pub fn last_offset_delete_node(&self) -> Option<i32> {
        self.state.lock().last_offset_delete_node
    }

    pub fn offset_delete_not_coordinator(&self) -> u32 {
        self.state.lock().offset_delete_not_coordinator
    }

    pub fn last_consumer_group_describe_node(&self) -> Option<i32> {
        self.state.lock().last_consumer_group_describe_node
    }

    pub fn consumer_group_describe_not_coordinator(&self) -> u32 {
        self.state.lock().consumer_group_describe_not_coordinator
    }

    pub fn last_describe_groups_node(&self) -> Option<i32> {
        self.state.lock().last_describe_groups_node
    }

    pub fn describe_groups_not_coordinator(&self) -> u32 {
        self.state.lock().describe_groups_not_coordinator
    }

    pub fn last_list_groups_node(&self) -> Option<i32> {
        self.state.lock().last_list_groups_node
    }

    pub fn last_list_groups(&self) -> Option<(Vec<String>, Vec<String>)> {
        self.state.lock().last_list_groups.clone()
    }

    pub fn last_delete_groups_node(&self) -> Option<i32> {
        self.state.lock().last_delete_groups_node
    }

    pub fn delete_groups_not_coordinator(&self) -> u32 {
        self.state.lock().delete_groups_not_coordinator
    }

    pub fn last_share_group_describe_node(&self) -> Option<i32> {
        self.state.lock().last_share_group_describe_node
    }

    pub fn share_group_describe_not_coordinator(&self) -> u32 {
        self.state.lock().share_group_describe_not_coordinator
    }

    pub fn last_describe_share_group_offsets_node(&self) -> Option<i32> {
        self.state.lock().last_describe_share_group_offsets_node
    }

    pub fn describe_share_group_offsets_not_coordinator(&self) -> u32 {
        self.state
            .lock()
            .describe_share_group_offsets_not_coordinator
    }

    pub fn last_alter_share_group_offsets_node(&self) -> Option<i32> {
        self.state.lock().last_alter_share_group_offsets_node
    }

    pub fn alter_share_group_offsets_not_coordinator(&self) -> u32 {
        self.state.lock().alter_share_group_offsets_not_coordinator
    }

    pub fn last_delete_share_group_offsets_node(&self) -> Option<i32> {
        self.state.lock().last_delete_share_group_offsets_node
    }

    pub fn delete_share_group_offsets_not_coordinator(&self) -> u32 {
        self.state.lock().delete_share_group_offsets_not_coordinator
    }

    pub fn last_describe_topic_partitions_node(&self) -> Option<i32> {
        self.state.lock().last_describe_topic_partitions_node
    }

    pub fn last_describe_topic_partitions(
        &self,
    ) -> Option<(Vec<String>, i32, Option<TopicPartitionCursor>)> {
        self.state.lock().last_describe_topic_partitions.clone()
    }

    pub fn last_list_config_resources_node(&self) -> Option<i32> {
        self.state.lock().last_list_config_resources_node
    }

    pub fn last_list_config_resources(&self) -> Option<Vec<i8>> {
        self.state.lock().last_list_config_resources.clone()
    }

    pub fn last_get_telemetry_subscriptions_node(&self) -> Option<i32> {
        self.state.lock().last_get_telemetry_subscriptions_node
    }

    pub fn last_get_telemetry_subscriptions(&self) -> Option<[u8; 16]> {
        self.state.lock().last_get_telemetry_subscriptions
    }

    pub fn last_push_telemetry_node(&self) -> Option<i32> {
        self.state.lock().last_push_telemetry_node
    }

    pub fn last_push_telemetry(&self) -> Option<LastPushTelemetry> {
        self.state.lock().last_push_telemetry.clone()
    }

    pub fn last_assign_replicas_to_dirs_node(&self) -> Option<i32> {
        self.state.lock().last_assign_replicas_to_dirs_node
    }

    pub fn assign_replicas_to_dirs_not_controller(&self) -> u32 {
        self.state.lock().assign_replicas_to_dirs_not_controller
    }

    pub fn last_assign_replicas_to_dirs(&self) -> Option<AssignReplicasToDirsRequest> {
        self.state.lock().last_assign_replicas_to_dirs.clone()
    }

    pub fn last_alter_replica_log_dirs_node(&self) -> Option<i32> {
        self.state.lock().last_alter_replica_log_dirs_node
    }

    pub fn last_alter_replica_log_dirs(&self) -> Option<AlterReplicaLogDirsRequest> {
        self.state.lock().last_alter_replica_log_dirs.clone()
    }

    pub fn last_describe_log_dirs_node(&self) -> Option<i32> {
        self.state.lock().last_describe_log_dirs_node
    }

    pub fn last_describe_log_dirs(&self) -> Option<DescribeLogDirsRequest> {
        self.state.lock().last_describe_log_dirs.clone()
    }

    pub fn last_create_delegation_token_node(&self) -> Option<i32> {
        self.state.lock().last_create_delegation_token_node
    }

    pub fn last_create_delegation_token(&self) -> Option<CreateDelegationTokenRequest> {
        self.state.lock().last_create_delegation_token.clone()
    }

    pub fn last_renew_delegation_token_node(&self) -> Option<i32> {
        self.state.lock().last_renew_delegation_token_node
    }

    pub fn last_renew_delegation_token(&self) -> Option<RenewDelegationTokenRequest> {
        self.state.lock().last_renew_delegation_token.clone()
    }

    pub fn last_expire_delegation_token_node(&self) -> Option<i32> {
        self.state.lock().last_expire_delegation_token_node
    }

    pub fn last_expire_delegation_token(&self) -> Option<ExpireDelegationTokenRequest> {
        self.state.lock().last_expire_delegation_token.clone()
    }

    pub fn last_describe_delegation_token_node(&self) -> Option<i32> {
        self.state.lock().last_describe_delegation_token_node
    }

    pub fn last_describe_delegation_token(&self) -> Option<DescribeDelegationTokenRequest> {
        self.state.lock().last_describe_delegation_token.clone()
    }

    pub fn join_group_calls(&self) -> u32 {
        self.state.lock().join_group_calls
    }

    pub fn cg_heartbeat_calls(&self) -> u32 {
        self.state.lock().cg_heartbeat_calls
    }

    pub fn sync_group_calls(&self) -> u32 {
        self.state.lock().sync_group_calls
    }

    pub fn share_heartbeat_calls(&self) -> u32 {
        self.state.lock().share_heartbeat_calls
    }

    pub fn share_fetch_calls(&self) -> u32 {
        self.state.lock().share_fetch_calls
    }

    pub fn share_ack_calls(&self) -> u32 {
        self.state.lock().share_ack_calls
    }

    pub fn last_share_fetch_epoch(&self) -> Option<i32> {
        self.state.lock().last_share_fetch_epoch
    }

    pub fn last_share_ack_epoch(&self) -> Option<i32> {
        self.state.lock().last_share_ack_epoch
    }

    pub fn last_share_ack_partitions(&self) -> usize {
        self.state.lock().last_share_ack_partitions
    }

    pub fn last_share_fetch_node(&self) -> Option<i32> {
        self.state.lock().last_share_fetch_node
    }

    pub fn last_share_ack_node(&self) -> Option<i32> {
        self.state.lock().last_share_ack_node
    }

    pub fn share_fetch_not_leader(&self) -> u32 {
        self.state.lock().share_fetch_not_leader
    }

    pub fn offset_commit_calls(&self) -> u32 {
        self.state.lock().offset_commit_calls
    }

    pub fn offset_fetch_calls(&self) -> u32 {
        self.state.lock().offset_fetch_calls
    }

    pub fn last_offset_commit_partitions(&self) -> usize {
        self.state.lock().last_offset_commit_partitions
    }

    pub fn last_offset_commit_node(&self) -> Option<i32> {
        self.state.lock().last_offset_commit_node
    }

    pub fn offset_commit_not_coordinator(&self) -> u32 {
        self.state.lock().offset_commit_not_coordinator
    }

    pub fn offset_commit_load_once(&self) {
        self.state.lock().offset_commit_load_left = 1;
    }

    pub fn offset_commit_load_in_progress(&self) -> u32 {
        self.state.lock().offset_commit_load_in_progress
    }

    pub fn last_offset_fetch_partitions(&self) -> usize {
        self.state.lock().last_offset_fetch_partitions
    }

    pub fn add_partitions_to_txn_calls(&self) -> u32 {
        self.state.lock().add_partitions_to_txn_calls
    }

    pub fn last_add_partitions_to_txn(&self) -> usize {
        self.state.lock().last_add_partitions_to_txn
    }

    pub fn txn_offset_commit_calls(&self) -> u32 {
        self.state.lock().txn_offset_commit_calls
    }

    pub fn last_txn_offset_commit_partitions(&self) -> usize {
        self.state.lock().last_txn_offset_commit_partitions
    }

    pub fn last_txn_offset_epochs(&self) -> Vec<i32> {
        self.state.lock().last_txn_offset_epochs.clone()
    }

    pub fn heartbeat_total(&self, group_id: &str) -> u32 {
        self.state
            .lock()
            .groups
            .get(group_id)
            .map(|g| g.hb_total)
            .unwrap_or(0)
    }

    pub fn drop_connections(&self) {
        let st = self.state.lock();
        let n = *st.drop_gen.borrow();
        let _ = st.drop_gen.send(n.saturating_add(1));
    }

    /// Accept then immediately drop the next `n` TCP connections (no Kafka handshake).
    pub fn refuse_connections(&self, n: u32) {
        self.state.lock().refuse_conns = n;
    }

    pub fn accept_count(&self) -> u32 {
        self.state.lock().accepts
    }

    pub fn move_coordinator(&self) {
        let mut st = self.state.lock();
        if let Some(other) = st
            .brokers
            .iter()
            .map(|b| b.node_id)
            .find(|id| *id != st.coord_node)
        {
            st.coord_node = other;
        }
    }

    pub fn set_txn_coordinator(&self, node_id: i32) {
        self.state.lock().txn_coord_node = node_id;
    }

    pub fn stale_txn_find_once(&self) {
        self.state.lock().stale_txn_finds = 1;
    }

    pub fn move_txn_coordinator(&self) {
        let mut st = self.state.lock();
        if let Some(other) = st
            .brokers
            .iter()
            .map(|b| b.node_id)
            .find(|id| *id != st.txn_coord_node)
        {
            st.txn_coord_node = other;
        }
    }

    pub fn find_coordinator_key_types(&self) -> Vec<i8> {
        self.state.lock().find_coordinator_key_types.clone()
    }

    pub fn last_init_producer_id_node(&self) -> Option<i32> {
        self.state.lock().last_init_producer_id_node
    }

    pub fn last_init_producer_id_timeout(&self) -> Option<i32> {
        self.state.lock().last_init_producer_id_timeout
    }

    pub fn init_producer_id_nodes(&self) -> Vec<i32> {
        self.state.lock().init_producer_id_nodes.clone()
    }

    pub fn init_producer_id_not_coordinator(&self) -> u32 {
        self.state.lock().init_producer_id_not_coordinator
    }

    pub fn last_add_partitions_node(&self) -> Option<i32> {
        self.state.lock().last_add_partitions_node
    }

    pub fn last_add_offsets_node(&self) -> Option<i32> {
        self.state.lock().last_add_offsets_node
    }

    pub fn last_end_txn_node(&self) -> Option<i32> {
        self.state.lock().last_end_txn_node
    }

    pub fn last_txn_offset_commit_node(&self) -> Option<i32> {
        self.state.lock().last_txn_offset_commit_node
    }

    pub fn membership_heartbeats_on(&self, node_id: i32) -> u32 {
        self.state
            .lock()
            .hb_by_node
            .get(&node_id)
            .copied()
            .unwrap_or(0)
    }
}

pub async fn closed_tcp_addr() -> String {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    drop(listener);
    format!("{addr}")
}

pub async fn wait_pred(what: &str, mut pred: impl FnMut() -> bool) {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
    loop {
        if pred() {
            return;
        }
        if tokio::time::Instant::now() >= deadline {
            panic!("{what} not observed in 2s");
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

fn tls_server_identity() -> (rustls::ServerConfig, Vec<u8>) {
    let pair = rcgen::generate_simple_self_signed(["localhost".into(), "127.0.0.1".into()])
        .expect("rcgen");
    let ca_pem = pair.cert.pem().into_bytes();
    let cert_der = rustls::pki_types::CertificateDer::from(pair.cert.der().to_vec());
    let key_der = rustls::pki_types::PrivateKeyDer::Pkcs8(
        rustls::pki_types::PrivatePkcs8KeyDer::from(pair.key_pair.serialize_der()),
    );
    let server = rustls::ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(vec![cert_der], key_der)
        .expect("tls server config");
    (server, ca_pem)
}

async fn read_frame<S: AsyncRead + Unpin>(
    stream: &mut S,
    buf: &mut BytesMut,
) -> std::io::Result<BytesMut> {
    loop {
        if buf.len() >= 4 {
            let size = i32::from_be_bytes(buf[0..4].try_into().unwrap());
            let total = 4 + size as usize;
            if buf.len() >= total {
                let mut frame = buf.split_to(total);
                let _ = frame.split_to(4);
                return Ok(frame);
            }
        }
        let n = stream.read_buf(buf).await?;
        if n == 0 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                "eof",
            ));
        }
    }
}

async fn write_frame<S: AsyncWrite + Unpin>(stream: &mut S, payload: &[u8]) -> std::io::Result<()> {
    let mut out = BytesMut::with_capacity(4 + payload.len());
    out.put_i32(payload.len() as i32);
    out.extend_from_slice(payload);
    stream.write_all(&out).await
}

fn topic_name_for_id(st: &State, id: [u8; 16]) -> String {
    if id == [0u8; 16] {
        return "t".into();
    }
    st.created_topics
        .keys()
        .find(|name| mock_topic_id(name) == id)
        .cloned()
        .unwrap_or_else(|| "t".into())
}

fn apply_share_acks(
    st: &mut State,
    member_id: &str,
    topic: &str,
    partition: i32,
    batches: &[AcknowledgementBatch],
) {
    for b in batches {
        let mut off = b.first_offset;
        while off <= b.last_offset {
            let ty = if b.types.len() == 1 {
                b.types.first().copied().unwrap_or(0)
            } else {
                let i = usize::try_from(off.saturating_sub(b.first_offset)).unwrap_or(usize::MAX);
                match b.types.get(i).copied() {
                    Some(t) => t,
                    None => break,
                }
            };
            let k = (topic.to_string(), partition, off);
            let owned = st
                .share_acquired
                .get(&k)
                .map(|m| m == member_id)
                .unwrap_or(false);
            if owned {
                let _ = st.share_acquired.remove(&k);
                if ty == ACK_ACCEPT || ty == ACK_REJECT {
                    let _ = st.share_accepted.insert(k);
                }
            }
            off = off.saturating_add(1);
        }
    }
}

/// KIP-932 share session epoch. Returns 0 or a broker error.
fn share_partition_leader(st: &State, topic: &str, partition: i32) -> i32 {
    st.partition_leaders
        .get(&(topic.to_string(), partition))
        .copied()
        .or_else(|| st.brokers.first().map(|b| b.node_id))
        .unwrap_or(1)
}

fn share_wrong_leader(st: &State, node_id: i32, tps: &[(String, i32)]) -> bool {
    tps.iter()
        .any(|(topic, p)| share_partition_leader(st, topic, *p) != node_id)
}

fn share_session_step(st: &mut State, member_id: &str, epoch: i32) -> i16 {
    match epoch {
        0 => {
            st.share_acquired.retain(|_, owner| owner != member_id);
            let _ = st.share_epochs.insert(member_id.to_string(), 1);
            0
        }
        -1 => {
            if st.share_epochs.remove(member_id).is_some() {
                st.share_acquired.retain(|_, owner| owner != member_id);
                0
            } else {
                error::SHARE_SESSION_NOT_FOUND
            }
        }
        e if e > 0 => match st.share_epochs.get(member_id).copied() {
            Some(expected) if expected == e => {
                let next = e.saturating_add(1);
                let _ = st.share_epochs.insert(member_id.to_string(), next);
                0
            }
            Some(_) => error::INVALID_SHARE_SESSION_EPOCH,
            None => error::SHARE_SESSION_NOT_FOUND,
        },
        _ => error::INVALID_SHARE_SESSION_EPOCH,
    }
}

fn share_record_batches(taken: Vec<Record>, leader_epoch: i32) -> Vec<RecordBatch> {
    taken
        .into_iter()
        .map(|r| {
            let off = r.offset;
            let mut batch = RecordBatch::from_records(vec![r]);
            batch.base_offset = off;
            batch.partition_leader_epoch = leader_epoch;
            batch
        })
        .collect()
}

fn versions() -> ApiVersionsResponse {
    let keys = [
        (PRODUCE, 3, 9),
        (FETCH, 4, 11),
        (LIST_OFFSETS, 0, 5),
        (METADATA, 1, 12),
        (OFFSET_COMMIT, 2, 7),
        (OFFSET_FETCH, 1, 5),
        (FIND_COORDINATOR, 0, 2),
        (JOIN_GROUP, 0, 5),
        (HEARTBEAT, 0, 3),
        (SYNC_GROUP, 0, 3),
        (LEAVE_GROUP, 0, 2),
        (CONSUMER_GROUP_HEARTBEAT, 0, 0),
        (CONSUMER_GROUP_DESCRIBE, 0, 1),
        (DESCRIBE_GROUPS, 0, 6),
        (LIST_GROUPS, 0, 5),
        (DELETE_GROUPS, 0, 2),
        (SHARE_GROUP_DESCRIBE, 1, 1),
        (DESCRIBE_SHARE_GROUP_OFFSETS, 0, 0),
        (ALTER_SHARE_GROUP_OFFSETS, 0, 0),
        (DELETE_SHARE_GROUP_OFFSETS, 0, 0),
        (DESCRIBE_TOPIC_PARTITIONS, 0, 0),
        (LIST_CONFIG_RESOURCES, 0, 1),
        (GET_TELEMETRY_SUBSCRIPTIONS, 0, 0),
        (PUSH_TELEMETRY, 0, 0),
        (ASSIGN_REPLICAS_TO_DIRS, 0, 0),
        (ALTER_REPLICA_LOG_DIRS, 1, 2),
        (DESCRIBE_LOG_DIRS, 1, 4),
        (CREATE_DELEGATION_TOKEN, 1, 3),
        (RENEW_DELEGATION_TOKEN, 1, 2),
        (EXPIRE_DELEGATION_TOKEN, 1, 2),
        (DESCRIBE_DELEGATION_TOKEN, 1, 3),
        (SHARE_GROUP_HEARTBEAT, 1, 1),
        (SHARE_FETCH, 1, 1),
        (SHARE_ACKNOWLEDGE, 1, 1),
        (SASL_HANDSHAKE, 0, 1),
        (API_VERSIONS, 0, 4),
        (CREATE_TOPICS, 0, 4),
        (DELETE_TOPICS, 0, 3),
        (CREATE_PARTITIONS, 0, 1),
        (DELETE_RECORDS, 0, 1),
        (ALTER_CONFIGS, 0, 1),
        (DESCRIBE_CLUSTER, 0, 0),
        (DESCRIBE_PRODUCERS, 0, 0),
        (DESCRIBE_ACLS, 0, 1),
        (CREATE_ACLS, 0, 1),
        (DELETE_ACLS, 0, 1),
        (INCREMENTAL_ALTER_CONFIGS, 0, 0),
        (ALTER_PARTITION_REASSIGNMENTS, 0, 0),
        (LIST_PARTITION_REASSIGNMENTS, 0, 0),
        (UPDATE_FEATURES, 0, 0),
        (ALTER_USER_SCRAM_CREDENTIALS, 0, 0),
        (DESCRIBE_USER_SCRAM_CREDENTIALS, 0, 0),
        (UNREGISTER_BROKER, 0, 0),
        (DESCRIBE_CLIENT_QUOTAS, 0, 1),
        (ALTER_CLIENT_QUOTAS, 0, 1),
        (ALLOCATE_PRODUCER_IDS, 0, 0),
        (DESCRIBE_TRANSACTIONS, 0, 0),
        (LIST_TRANSACTIONS, 0, 2),
        (INIT_PRODUCER_ID, 0, 4),
        (ADD_PARTITIONS_TO_TXN, 0, 1),
        (ADD_OFFSETS_TO_TXN, 0, 1),
        (END_TXN, 0, 1),
        (TXN_OFFSET_COMMIT, 0, 2),
        (OFFSET_DELETE, 0, 0),
        (OFFSET_FOR_LEADER_EPOCH, 0, 2),
        (DESCRIBE_CONFIGS, 0, 1),
        (SASL_AUTHENTICATE, 0, 1),
    ];
    ApiVersionsResponse {
        error_code: 0,
        api_keys: keys
            .into_iter()
            .map(|(api_key, min_version, max_version)| ApiVersion {
                api_key,
                min_version,
                max_version,
            })
            .collect(),
        throttle_time_ms: 0,
    }
}

fn encode_not_coordinator(api_key: i16, body: &mut BytesMut) {
    const NC: i16 = 16;
    match api_key {
        HEARTBEAT => encode_heartbeat_response(body, NC).unwrap(),
        LEAVE_GROUP => encode_leave_group_response(body, NC).unwrap(),
        JOIN_GROUP => encode_join_group_response(body, NC, -1, "", "", "", &[]).unwrap(),
        SYNC_GROUP => encode_sync_group_response(body, NC, &[]).unwrap(),
        OFFSET_COMMIT => encode_offset_commit_response(
            body,
            &[OffsetTopic {
                topic: "t".into(),
                partitions: vec![OffsetPartition::new(0, -1)],
            }],
            NC,
        )
        .unwrap(),
        OFFSET_FETCH => encode_offset_fetch_response(
            body,
            &[FetchedOffsetTopic {
                topic: "t".into(),
                partitions: vec![FetchedOffset::new(0, -1, NC)],
            }],
        )
        .unwrap(),
        CONSUMER_GROUP_HEARTBEAT => encode_consumer_group_heartbeat_response(
            body,
            &ConsumerGroupHeartbeatResponse {
                error_code: NC,
                error_message: None,
                member_id: None,
                member_epoch: 0,
                heartbeat_interval_ms: 5000,
                assignment: None,
            },
        )
        .unwrap(),
        SHARE_GROUP_HEARTBEAT => encode_share_group_heartbeat_response(
            body,
            &ShareGroupHeartbeatResponse {
                error_code: NC,
                error_message: None,
                member_id: None,
                member_epoch: 0,
                heartbeat_interval_ms: 5000,
                assignment: None,
            },
        )
        .unwrap(),
        SHARE_ACKNOWLEDGE => encode_share_acknowledge_response(body, NC).unwrap(),
        SHARE_FETCH => {
            body.put_i32(0);
            body.put_i16(NC);
            buf::put_compact_string(body, None).unwrap();
            body.put_i32(0);
            buf::put_array_len(body, true, Some(0)).unwrap();
            buf::put_array_len(body, true, Some(0)).unwrap();
            buf::put_empty_tagged_fields(body);
        }
        OFFSET_DELETE => encode_offset_delete_response(body, NC, &[]).unwrap(),
        _ => {}
    }
}

async fn handle_conn<S: AsyncRead + AsyncWrite + Unpin>(
    mut stream: S,
    node_id: i32,
    state: Arc<Mutex<State>>,
) {
    let mut buf = BytesMut::new();
    let mut drop_rx = state.lock().drop_gen.subscribe();
    let mut authed = {
        let st = state.lock();
        st.sasl_user.is_none() && st.scram_user.is_none() && st.oauth_principal.is_none()
    };
    let mut scram_step: Option<(scram::ScramAlg, String, String, String)> = None;
    loop {
        let mut frame = tokio::select! {
            _ = drop_rx.changed() => break,
            frame = read_frame(&mut stream, &mut buf) => match frame {
                Ok(f) => f,
                Err(_) => break,
            },
        };
        let header = match decode_request_header(&mut frame) {
            Ok(h) => h,
            Err(_) => break,
        };
        if !authed
            && !matches!(
                header.api_key,
                API_VERSIONS | SASL_HANDSHAKE | SASL_AUTHENTICATE
            )
        {
            break;
        }
        let mut body = BytesMut::new();
        encode_response_header(
            &mut body,
            header.api_key,
            header.api_version,
            header.correlation_id,
        )
        .unwrap();
        let coord_mismatch = matches!(
            header.api_key,
            JOIN_GROUP
                | SYNC_GROUP
                | HEARTBEAT
                | LEAVE_GROUP
                | OFFSET_COMMIT
                | OFFSET_FETCH
                | OFFSET_DELETE
                | CONSUMER_GROUP_HEARTBEAT
                | SHARE_GROUP_HEARTBEAT
        ) && {
            let st = state.lock();
            st.coord_node != node_id
        };
        if coord_mismatch {
            {
                let mut st = state.lock();
                if header.api_key == OFFSET_COMMIT {
                    st.offset_commit_not_coordinator =
                        st.offset_commit_not_coordinator.saturating_add(1);
                }
                if header.api_key == OFFSET_DELETE {
                    st.offset_delete_not_coordinator =
                        st.offset_delete_not_coordinator.saturating_add(1);
                }
            }
            encode_not_coordinator(header.api_key, &mut body);
            if write_frame(&mut stream, &body).await.is_err() {
                break;
            }
            continue;
        }
        match header.api_key {
            API_VERSIONS => {
                encode_api_versions_response(&mut body, header.api_version, &versions()).unwrap()
            }
            METADATA => {
                let mut st = state.lock();
                st.metadata_calls = st.metadata_calls.saturating_add(1);
                let (_, allow) =
                    decode_metadata_request(&mut frame.clone(), header.api_version).unwrap();
                st.last_metadata_allow_auto = Some(allow);
                let (host, port) = broker_host_port(&st, node_id);
                encode_metadata_response(
                    &mut body,
                    header.api_version,
                    &metadata_for(&st, &host, port),
                )
                .unwrap();
            }
            CREATE_TOPICS => {
                let req = decode_create_topics_request(&mut frame, header.api_version).unwrap();
                let mut results = Vec::new();
                let mut st = state.lock();
                if st.controller_node != node_id {
                    st.create_topics_not_controller =
                        st.create_topics_not_controller.saturating_add(1);
                    for t in req.topics {
                        results.push(TopicResult {
                            name: t.name,
                            error_code: error::NOT_CONTROLLER,
                            error_message: Some("Not controller".into()),
                        });
                    }
                } else {
                    st.last_create_topics_node = Some(node_id);
                    for t in req.topics {
                        if st.created_topics.contains_key(&t.name) {
                            results.push(TopicResult {
                                name: t.name,
                                error_code: 36,
                                error_message: Some("Topic already exists.".into()),
                            });
                            continue;
                        }
                        let npart = if t.assignments.is_empty() {
                            t.num_partitions
                        } else {
                            t.assignments.len() as i32
                        };
                        let mut error_code = 0i16;
                        if npart < 1 {
                            error_code = 37;
                        } else if t.replication_factor < 1 && t.assignments.is_empty() {
                            error_code = 38;
                        }
                        if error_code == 0 && !req.validate_only {
                            let mut configs = HashMap::new();
                            for c in t.configs {
                                configs.insert(c.name, c.value);
                            }
                            st.created_topics.insert(
                                t.name.clone(),
                                CreatedTopic {
                                    num_partitions: npart,
                                    configs,
                                },
                            );
                        }
                        results.push(TopicResult {
                            name: t.name,
                            error_code,
                            error_message: None,
                        });
                    }
                }
                encode_create_topics_response(&mut body, header.api_version, &results).unwrap();
            }
            DELETE_TOPICS => {
                let (names, _timeout) = decode_delete_topics_request(&mut frame).unwrap();
                let mut results = Vec::new();
                let mut st = state.lock();
                if st.controller_node != node_id {
                    st.delete_topics_not_controller =
                        st.delete_topics_not_controller.saturating_add(1);
                    for name in names {
                        results.push(TopicResult {
                            name,
                            error_code: error::NOT_CONTROLLER,
                            error_message: Some("Not controller".into()),
                        });
                    }
                } else {
                    st.last_delete_topics_node = Some(node_id);
                    for name in names {
                        let error_code = if st.created_topics.remove(&name).is_some() {
                            0
                        } else {
                            3
                        };
                        results.push(TopicResult {
                            name,
                            error_code,
                            error_message: None,
                        });
                    }
                }
                encode_delete_topics_response(&mut body, header.api_version, &results).unwrap();
            }
            DESCRIBE_CONFIGS => {
                let (resources, _syn) =
                    decode_describe_configs_request(&mut frame, header.api_version).unwrap();
                let st = state.lock();
                let mut results = Vec::new();
                for r in resources {
                    if r.resource_type == RESOURCE_TOPIC {
                        match st.created_topics.get(&r.name) {
                            None => results.push(DescribeConfigsResult {
                                error_code: 3,
                                error_message: Some("Unknown topic.".into()),
                                resource_type: r.resource_type,
                                name: r.name,
                                entries: Vec::new(),
                            }),
                            Some(spec) => {
                                let mut entries = Vec::new();
                                let mut seen = std::collections::HashSet::new();
                                let mut push = |name: &str, value: Option<String>, source: i8| {
                                    if let Some(keys) = &r.keys {
                                        if !keys.iter().any(|k| k == name) {
                                            return;
                                        }
                                    }
                                    if seen.insert(name.to_string()) {
                                        entries.push(ConfigEntry {
                                            name: name.to_string(),
                                            value,
                                            read_only: false,
                                            source,
                                            is_sensitive: false,
                                            synonyms: Vec::new(),
                                        });
                                    }
                                };
                                push(
                                    "cleanup.policy",
                                    spec.configs
                                        .get("cleanup.policy")
                                        .cloned()
                                        .flatten()
                                        .or_else(|| Some("delete".into())),
                                    if spec.configs.contains_key("cleanup.policy") {
                                        CONFIG_SOURCE_DYNAMIC_TOPIC
                                    } else {
                                        CONFIG_SOURCE_DEFAULT
                                    },
                                );
                                for (k, v) in &spec.configs {
                                    if k == "cleanup.policy" {
                                        continue;
                                    }
                                    push(k, v.clone(), CONFIG_SOURCE_DYNAMIC_TOPIC);
                                }
                                results.push(DescribeConfigsResult {
                                    error_code: 0,
                                    error_message: None,
                                    resource_type: r.resource_type,
                                    name: r.name,
                                    entries,
                                });
                            }
                        }
                    } else if r.resource_type == RESOURCE_BROKER {
                        results.push(DescribeConfigsResult {
                            error_code: 0,
                            error_message: None,
                            resource_type: r.resource_type,
                            name: r.name,
                            entries: vec![ConfigEntry {
                                name: "log.retention.hours".into(),
                                value: Some("168".into()),
                                read_only: true,
                                source: CONFIG_SOURCE_DEFAULT,
                                is_sensitive: false,
                                synonyms: Vec::new(),
                            }],
                        });
                    } else {
                        results.push(DescribeConfigsResult {
                            error_code: 3,
                            error_message: Some("Unknown resource.".into()),
                            resource_type: r.resource_type,
                            name: r.name,
                            entries: Vec::new(),
                        });
                    }
                }
                encode_describe_configs_response(&mut body, header.api_version, &results).unwrap();
            }
            CREATE_PARTITIONS => {
                let (topics, validate_only) = decode_create_partitions_request(&mut frame).unwrap();
                let mut results = Vec::new();
                let mut st = state.lock();
                if st.controller_node != node_id {
                    st.create_partitions_not_controller =
                        st.create_partitions_not_controller.saturating_add(1);
                    for (name, _count) in topics {
                        results.push(TopicResult {
                            name,
                            error_code: error::NOT_CONTROLLER,
                            error_message: Some("Not controller".into()),
                        });
                    }
                } else {
                    st.last_create_partitions_node = Some(node_id);
                    for (name, count) in topics {
                        match st.created_topics.get_mut(&name) {
                            None => results.push(TopicResult {
                                name,
                                error_code: 3,
                                error_message: Some("Unknown topic.".into()),
                            }),
                            Some(spec) => {
                                let mut err = 0i16;
                                if count < spec.num_partitions {
                                    err = 37;
                                } else if !validate_only {
                                    spec.num_partitions = count;
                                }
                                results.push(TopicResult {
                                    name,
                                    error_code: err,
                                    error_message: None,
                                });
                            }
                        }
                    }
                }
                encode_create_partitions_response(&mut body, &results).unwrap();
            }
            INCREMENTAL_ALTER_CONFIGS => {
                let (rt, name, configs, validate_only) =
                    decode_incremental_alter_configs_request(&mut frame).unwrap();
                let mut err = 0i16;
                let mut st = state.lock();
                if st.controller_node != node_id {
                    st.incremental_alter_configs_not_controller = st
                        .incremental_alter_configs_not_controller
                        .saturating_add(1);
                    err = error::NOT_CONTROLLER;
                } else {
                    st.last_incremental_alter_configs_node = Some(node_id);
                    if rt != RESOURCE_TOPIC {
                        err = 3;
                    } else if let Some(spec) = st.created_topics.get_mut(&name) {
                        if !validate_only {
                            for c in configs {
                                if c.op == ALTER_CONFIG_DELETE {
                                    spec.configs.remove(&c.name);
                                } else if c.op == ALTER_CONFIG_SET {
                                    spec.configs.insert(c.name, c.value);
                                }
                            }
                        }
                    } else {
                        err = 3;
                    }
                }
                encode_incremental_alter_configs_response(&mut body, err, &name).unwrap();
            }
            ALTER_CONFIGS => {
                let (rt, name, configs, validate_only) =
                    decode_alter_configs_request(&mut frame).unwrap();
                let mut err = 0i16;
                let mut st = state.lock();
                if rt != RESOURCE_TOPIC {
                    err = 3;
                } else if let Some(spec) = st.created_topics.get_mut(&name) {
                    if !validate_only {
                        for c in configs {
                            if let Some(val) = c.value {
                                spec.configs.insert(c.name, Some(val));
                            } else {
                                spec.configs.remove(&c.name);
                            }
                        }
                    }
                } else {
                    err = 3;
                }
                encode_alter_configs_response(&mut body, header.api_version, err, &name).unwrap();
            }
            DELETE_RECORDS => {
                let (topic, partition, offset, _timeout) =
                    decode_delete_records_request(&mut frame).unwrap();
                let mut st = state.lock();
                let key = (topic.clone(), partition);
                let leader = st.partition_leaders.get(&key).copied().unwrap_or(node_id);
                if leader != node_id {
                    st.delete_records_not_leader = st.delete_records_not_leader.saturating_add(1);
                    encode_delete_records_response(
                        &mut body,
                        header.api_version,
                        &topic,
                        partition,
                        -1,
                        error::NOT_LEADER_OR_FOLLOWER,
                    )
                    .unwrap();
                } else {
                    let (low, err) = if st.created_topics.contains_key(&topic) {
                        let hw = *st.next_offset.get(&key).unwrap_or(&0);
                        let start = *st.log_start.get(&key).unwrap_or(&0);
                        let low = offset.clamp(start, hw);
                        st.log_start.insert(key.clone(), low);
                        if let Some(recs) = st.log.get_mut(&key) {
                            recs.retain(|r| r.offset >= low);
                        }
                        st.last_delete_records_node = Some(node_id);
                        (low, 0i16)
                    } else {
                        (0i64, 3i16)
                    };
                    encode_delete_records_response(
                        &mut body,
                        header.api_version,
                        &topic,
                        partition,
                        low,
                        err,
                    )
                    .unwrap();
                }
            }
            DESCRIBE_PRODUCERS => {
                let (topic, partitions) = decode_describe_producers_request(&mut frame).unwrap();
                let partition = partitions.first().copied().unwrap_or(0);
                let mut st = state.lock();
                let key = (topic.clone(), partition);
                let leader = st.partition_leaders.get(&key).copied().unwrap_or(node_id);
                if leader != node_id {
                    st.describe_producers_not_leader =
                        st.describe_producers_not_leader.saturating_add(1);
                    // Per-partition 6 only. Do not invent a producer store,
                    // a 41 path, or a 16 path.
                    encode_describe_producers_response(
                        &mut body,
                        &DescribeProducersResponse::new(vec![DescribeProducersTopic::new(
                            topic,
                            vec![DescribeProducersPartition::new(
                                partition,
                                error::NOT_LEADER_OR_FOLLOWER,
                                None,
                                vec![],
                            )],
                        )]),
                    )
                    .unwrap();
                } else {
                    st.last_describe_producers_node = Some(node_id);
                    encode_describe_producers_response(
                        &mut body,
                        &DescribeProducersResponse::new(vec![DescribeProducersTopic::new(
                            topic,
                            vec![DescribeProducersPartition::new(
                                partition,
                                0,
                                None,
                                vec![ActiveProducer::new(1000, 1, 7, 1_700_000_000_000, 0, -1)],
                            )],
                        )]),
                    )
                    .unwrap();
                }
            }
            DESCRIBE_CLUSTER => {
                let _include = decode_describe_cluster_request(&mut frame).unwrap();
                let st = state.lock();
                let brokers = if st.brokers.is_empty() {
                    vec![Broker {
                        node_id,
                        host: "127.0.0.1".into(),
                        port: 0,
                        rack: None,
                    }]
                } else {
                    st.brokers.clone()
                };
                let controller_id = brokers.first().map(|b| b.node_id).unwrap_or(node_id);
                encode_describe_cluster_response(
                    &mut body,
                    &ClusterDescription {
                        error_code: 0,
                        error_message: None,
                        cluster_id: Some("mock".into()),
                        controller_id,
                        brokers,
                    },
                )
                .unwrap();
            }
            CREATE_ACLS => {
                let acls = decode_create_acls_request(&mut frame).unwrap();
                let n = acls.len();
                let mut st = state.lock();
                if st.controller_node != node_id {
                    st.create_acls_not_controller = st.create_acls_not_controller.saturating_add(1);
                    encode_create_acls_response(&mut body, &vec![error::NOT_CONTROLLER; n])
                        .unwrap();
                } else {
                    st.last_create_acls_node = Some(node_id);
                    st.acls.extend(acls);
                    encode_create_acls_response(&mut body, &vec![0; n]).unwrap();
                }
            }
            ALTER_PARTITION_REASSIGNMENTS => {
                let (_timeout, topics) =
                    decode_alter_partition_reassignments_request(&mut frame).unwrap();
                let mut st = state.lock();
                if st.controller_node != node_id {
                    st.alter_reassignments_not_controller =
                        st.alter_reassignments_not_controller.saturating_add(1);
                    encode_alter_partition_reassignments_response(
                        &mut body,
                        &AlterPartitionReassignmentsResponse {
                            error_code: error::NOT_CONTROLLER,
                            error_message: Some("Not controller".into()),
                            results: Vec::new(),
                        },
                    )
                    .unwrap();
                } else {
                    st.last_alter_reassignments_node = Some(node_id);
                    let mut results = Vec::new();
                    for t in topics {
                        let mut parts = Vec::new();
                        for p in t.partitions {
                            let err = if st.created_topics.contains_key(&t.name) {
                                st.last_reassignment =
                                    Some((t.name.clone(), p.partition_index, p.replicas.clone()));
                                match p.replicas {
                                    Some(replicas) => {
                                        let _ = st
                                            .reassignments
                                            .insert((t.name.clone(), p.partition_index), replicas);
                                    }
                                    None => {
                                        let _ = st
                                            .reassignments
                                            .remove(&(t.name.clone(), p.partition_index));
                                    }
                                }
                                0
                            } else {
                                3
                            };
                            parts.push(ReassignmentPartitionResult {
                                partition_index: p.partition_index,
                                error_code: err,
                                error_message: None,
                            });
                        }
                        results.push(ReassignmentTopicResult {
                            name: t.name,
                            partitions: parts,
                        });
                    }
                    encode_alter_partition_reassignments_response(
                        &mut body,
                        &AlterPartitionReassignmentsResponse {
                            error_code: 0,
                            error_message: None,
                            results,
                        },
                    )
                    .unwrap();
                }
            }
            LIST_PARTITION_REASSIGNMENTS => {
                let (_timeout, topics) =
                    decode_list_partition_reassignments_request(&mut frame).unwrap();
                let mut st = state.lock();
                if st.controller_node != node_id {
                    st.list_reassignments_not_controller =
                        st.list_reassignments_not_controller.saturating_add(1);
                    // 41 only. Do not invent a replica list on the wrong node.
                    encode_list_partition_reassignments_response(
                        &mut body,
                        &ListPartitionReassignmentsResponse {
                            error_code: error::NOT_CONTROLLER,
                            error_message: Some("Not controller".into()),
                            topics: Vec::new(),
                        },
                    )
                    .unwrap();
                } else {
                    st.last_list_reassignments_node = Some(node_id);
                    let mut by_topic: BTreeMap<String, Vec<OngoingPartitionReassignment>> =
                        BTreeMap::new();
                    for ((name, partition), replicas) in &st.reassignments {
                        let wanted = match &topics {
                            None => true,
                            Some(filter) => filter.iter().any(|t| {
                                t.name == *name
                                    && (t.partition_indexes.is_empty()
                                        || t.partition_indexes.contains(partition))
                            }),
                        };
                        if wanted {
                            by_topic.entry(name.clone()).or_default().push(
                                OngoingPartitionReassignment {
                                    partition_index: *partition,
                                    replicas: replicas.clone(),
                                    adding_replicas: Vec::new(),
                                    removing_replicas: Vec::new(),
                                },
                            );
                        }
                    }
                    let listed: Vec<OngoingTopicReassignment> = by_topic
                        .into_iter()
                        .map(|(name, partitions)| OngoingTopicReassignment { name, partitions })
                        .collect();
                    encode_list_partition_reassignments_response(
                        &mut body,
                        &ListPartitionReassignmentsResponse {
                            error_code: 0,
                            error_message: None,
                            topics: listed,
                        },
                    )
                    .unwrap();
                }
            }
            UPDATE_FEATURES => {
                let (_timeout, updates) = decode_update_features_request(&mut frame).unwrap();
                let mut st = state.lock();
                if st.controller_node != node_id {
                    st.update_features_not_controller =
                        st.update_features_not_controller.saturating_add(1);
                    // 41 only. Do not apply the feature mutation on the wrong node.
                    encode_update_features_response(
                        &mut body,
                        &UpdateFeaturesResponse {
                            error_code: error::NOT_CONTROLLER,
                            error_message: Some("Not controller".into()),
                            results: Vec::new(),
                        },
                    )
                    .unwrap();
                } else {
                    st.last_update_features_node = Some(node_id);
                    let mut results = Vec::new();
                    for u in updates {
                        st.last_feature_update =
                            Some((u.name.clone(), u.max_version_level, u.allow_downgrade));
                        let _ = st.features.insert(u.name.clone(), u.max_version_level);
                        results.push(UpdatableFeatureResult {
                            name: u.name,
                            error_code: 0,
                            error_message: None,
                        });
                    }
                    encode_update_features_response(
                        &mut body,
                        &UpdateFeaturesResponse {
                            error_code: 0,
                            error_message: None,
                            results,
                        },
                    )
                    .unwrap();
                }
            }
            ALTER_USER_SCRAM_CREDENTIALS => {
                let (deletions, upsertions) =
                    decode_alter_user_scram_credentials_request(&mut frame).unwrap();
                let mut st = state.lock();
                if st.controller_node != node_id {
                    st.alter_user_scram_not_controller =
                        st.alter_user_scram_not_controller.saturating_add(1);
                    // 41 only. Do not apply the SCRAM mutation on the wrong node.
                    let mut results = Vec::new();
                    for d in deletions {
                        results.push(AlterUserScramCredentialsResult {
                            user: d.name,
                            error_code: error::NOT_CONTROLLER,
                            error_message: Some("Not controller".into()),
                        });
                    }
                    for u in upsertions {
                        results.push(AlterUserScramCredentialsResult {
                            user: u.name,
                            error_code: error::NOT_CONTROLLER,
                            error_message: Some("Not controller".into()),
                        });
                    }
                    encode_alter_user_scram_credentials_response(&mut body, &results).unwrap();
                } else {
                    st.last_alter_user_scram_node = Some(node_id);
                    let mut results = Vec::new();
                    for d in deletions {
                        let _removed = st.scram_users.remove(&(d.name.clone(), d.mechanism));
                        st.last_scram_delete = Some((d.name.clone(), d.mechanism));
                        results.push(AlterUserScramCredentialsResult {
                            user: d.name,
                            error_code: 0,
                            error_message: None,
                        });
                    }
                    for u in upsertions {
                        // Store name/mechanism/iterations only. Dummy salt
                        // bytes are not kept and are not logged.
                        let _prev = st
                            .scram_users
                            .insert((u.name.clone(), u.mechanism), u.iterations);
                        st.last_scram_upsert = Some((u.name.clone(), u.mechanism, u.iterations));
                        results.push(AlterUserScramCredentialsResult {
                            user: u.name,
                            error_code: 0,
                            error_message: None,
                        });
                    }
                    encode_alter_user_scram_credentials_response(&mut body, &results).unwrap();
                }
            }
            DESCRIBE_USER_SCRAM_CREDENTIALS => {
                let users = decode_describe_user_scram_credentials_request(&mut frame).unwrap();
                let mut st = state.lock();
                if st.controller_node != node_id {
                    st.describe_user_scram_not_controller =
                        st.describe_user_scram_not_controller.saturating_add(1);
                    // 41 only. Do not disclose fixture credential metadata
                    // on the wrong node.
                    encode_describe_user_scram_credentials_response(
                        &mut body,
                        &DescribeUserScramCredentialsResponse {
                            error_code: error::NOT_CONTROLLER,
                            error_message: Some("Not controller".into()),
                            results: Vec::new(),
                        },
                    )
                    .unwrap();
                } else {
                    st.last_describe_user_scram_node = Some(node_id);
                    // Fixture users only. Name/mechanism/iterations; no
                    // salt, no password, nothing logged.
                    let mut by_user: std::collections::BTreeMap<String, Vec<ScramCredentialInfo>> =
                        std::collections::BTreeMap::new();
                    for ((name, mechanism), iterations) in &st.scram_users {
                        by_user
                            .entry(name.clone())
                            .or_default()
                            .push(ScramCredentialInfo {
                                mechanism: *mechanism,
                                iterations: *iterations,
                            });
                    }
                    let names: Vec<String> = match users {
                        None => by_user.keys().cloned().collect(),
                        Some(v) if v.is_empty() => by_user.keys().cloned().collect(),
                        Some(v) => v,
                    };
                    let results = names
                        .into_iter()
                        .map(|user| {
                            let credential_infos = by_user.remove(&user).unwrap_or_default();
                            DescribeUserScramCredentialsResult {
                                user,
                                error_code: 0,
                                error_message: None,
                                credential_infos,
                            }
                        })
                        .collect();
                    encode_describe_user_scram_credentials_response(
                        &mut body,
                        &DescribeUserScramCredentialsResponse {
                            error_code: 0,
                            error_message: None,
                            results,
                        },
                    )
                    .unwrap();
                }
            }
            UNREGISTER_BROKER => {
                let broker_id = decode_unregister_broker_request(&mut frame).unwrap();
                let mut st = state.lock();
                if st.controller_node != node_id {
                    st.unregister_broker_not_controller =
                        st.unregister_broker_not_controller.saturating_add(1);
                    // 41 only. Do not pretend the broker was unregistered
                    // on the wrong node.
                    encode_unregister_broker_response(
                        &mut body,
                        &UnregisterBrokerResponse {
                            error_code: error::NOT_CONTROLLER,
                            error_message: Some("Not controller".into()),
                        },
                    )
                    .unwrap();
                } else {
                    st.last_unregister_broker_node = Some(node_id);
                    st.last_unregistered_broker_id = Some(broker_id);
                    let _ = st.unregistered_brokers.insert(broker_id);
                    encode_unregister_broker_response(
                        &mut body,
                        &UnregisterBrokerResponse {
                            error_code: 0,
                            error_message: None,
                        },
                    )
                    .unwrap();
                }
            }
            DESCRIBE_CLIENT_QUOTAS => {
                let (components, strict) =
                    decode_describe_client_quotas_request(&mut frame).unwrap();
                let mut st = state.lock();
                // Any connected broker answers. Fixture describe only;
                // not a quota store and not a controller hop.
                st.last_describe_client_quotas_node = Some(node_id);
                st.last_describe_client_quotas = Some((components, strict));
                encode_describe_client_quotas_response(
                    &mut body,
                    &DescribeClientQuotasResponse {
                        error_code: 0,
                        error_message: None,
                        entries: Some(vec![ClientQuotaEntry::new(
                            vec![ClientQuotaEntity::new("user", Some("alice".into()))],
                            vec![ClientQuotaValue::new("producer_byte_rate", 1024.0)],
                        )]),
                    },
                )
                .unwrap();
            }
            ALTER_CLIENT_QUOTAS => {
                let (entries, _validate_only) =
                    decode_alter_client_quotas_request(&mut frame).unwrap();
                let mut st = state.lock();
                if st.controller_node != node_id {
                    st.alter_client_quotas_not_controller =
                        st.alter_client_quotas_not_controller.saturating_add(1);
                    // 41 only. Do not apply the quota mutation on the wrong node.
                    let mut results = Vec::new();
                    for e in entries {
                        results.push(ClientQuotaAlterationResult {
                            error_code: error::NOT_CONTROLLER,
                            error_message: Some("Not controller".into()),
                            entity: e.entity,
                        });
                    }
                    encode_alter_client_quotas_response(&mut body, &results).unwrap();
                } else {
                    st.last_alter_client_quotas_node = Some(node_id);
                    let mut results = Vec::new();
                    for e in entries {
                        // Fixture entity/ops only. Not a real cluster quota store.
                        for op in &e.ops {
                            let ent = e.entity.first().cloned();
                            let (entity_type, name) = match ent {
                                Some(ent) => (ent.entity_type, ent.name),
                                None => (String::new(), None),
                            };
                            let key = (entity_type.clone(), name.clone(), op.key.clone());
                            if op.remove {
                                let _removed = st.quota_fixtures.remove(&key);
                                st.last_quota_delete = Some(key);
                            } else {
                                let _prev = st.quota_fixtures.insert(key.clone(), op.value);
                                st.last_quota_upsert =
                                    Some((entity_type, name, op.key.clone(), op.value));
                            }
                        }
                        results.push(ClientQuotaAlterationResult {
                            error_code: 0,
                            error_message: None,
                            entity: e.entity,
                        });
                    }
                    encode_alter_client_quotas_response(&mut body, &results).unwrap();
                }
            }
            ALLOCATE_PRODUCER_IDS => {
                let (broker_id, broker_epoch) =
                    decode_allocate_producer_ids_request(&mut frame).unwrap();
                let mut st = state.lock();
                if st.controller_node != node_id {
                    st.allocate_producer_ids_not_controller =
                        st.allocate_producer_ids_not_controller.saturating_add(1);
                    // 41 only. Do not hand out a PID block on the wrong node.
                    encode_allocate_producer_ids_response(
                        &mut body,
                        &AllocateProducerIdsResponse {
                            error_code: error::NOT_CONTROLLER,
                            producer_id_start: 0,
                            producer_id_len: 0,
                        },
                    )
                    .unwrap();
                } else {
                    st.last_allocate_producer_ids_node = Some(node_id);
                    let start = st.next_producer_id_block_start;
                    let len: i32 = 1000;
                    st.next_producer_id_block_start = start.saturating_add(i64::from(len));
                    st.last_allocate_producer_ids = Some((broker_id, broker_epoch, start, len));
                    encode_allocate_producer_ids_response(
                        &mut body,
                        &AllocateProducerIdsResponse {
                            error_code: 0,
                            producer_id_start: start,
                            producer_id_len: len,
                        },
                    )
                    .unwrap();
                }
            }
            DESCRIBE_TRANSACTIONS => {
                let ids = decode_describe_transactions_request(&mut frame).unwrap();
                let mut st = state.lock();
                if st.txn_coord_node != node_id {
                    st.describe_transactions_not_coordinator =
                        st.describe_transactions_not_coordinator.saturating_add(1);
                    // 16 only. Do not disclose fixture txn state on the wrong node.
                    let results: Vec<TransactionState> = ids
                        .into_iter()
                        .map(|transactional_id| TransactionState {
                            error_code: error::NOT_COORDINATOR,
                            transactional_id,
                            transaction_state: String::new(),
                            transaction_timeout_ms: 0,
                            transaction_start_time_ms: 0,
                            producer_id: 0,
                            producer_epoch: 0,
                            topics: Vec::new(),
                        })
                        .collect();
                    encode_describe_transactions_response(&mut body, &results).unwrap();
                } else {
                    st.last_describe_transactions_node = Some(node_id);
                    // Fixture transactional ids only.
                    const TRANSACTIONAL_ID_NOT_FOUND: i16 = 152;
                    let results: Vec<TransactionState> = ids
                        .into_iter()
                        .map(|transactional_id| {
                            st.txn_fixtures.get(&transactional_id).cloned().unwrap_or(
                                TransactionState {
                                    error_code: TRANSACTIONAL_ID_NOT_FOUND,
                                    transactional_id,
                                    transaction_state: String::new(),
                                    transaction_timeout_ms: 0,
                                    transaction_start_time_ms: 0,
                                    producer_id: 0,
                                    producer_epoch: 0,
                                    topics: Vec::new(),
                                },
                            )
                        })
                        .collect();
                    encode_describe_transactions_response(&mut body, &results).unwrap();
                }
            }
            LIST_TRANSACTIONS => {
                let _filters = decode_list_transactions_request(&mut frame).unwrap();
                let mut st = state.lock();
                if st.txn_coord_node != node_id {
                    st.list_transactions_not_coordinator =
                        st.list_transactions_not_coordinator.saturating_add(1);
                    // 16 only. Do not disclose fixture txn ids on the wrong node.
                    encode_list_transactions_response(
                        &mut body,
                        &ListTransactionsResponse {
                            error_code: error::NOT_COORDINATOR,
                            unknown_state_filters: Vec::new(),
                            transaction_states: Vec::new(),
                        },
                    )
                    .unwrap();
                } else {
                    st.last_list_transactions_node = Some(node_id);
                    // Fixture transactional ids only.
                    let transaction_states: Vec<TransactionListing> = st
                        .txn_fixtures
                        .values()
                        .map(|s| TransactionListing {
                            transactional_id: s.transactional_id.clone(),
                            producer_id: s.producer_id,
                            transaction_state: s.transaction_state.clone(),
                        })
                        .collect();
                    encode_list_transactions_response(
                        &mut body,
                        &ListTransactionsResponse {
                            error_code: 0,
                            unknown_state_filters: Vec::new(),
                            transaction_states,
                        },
                    )
                    .unwrap();
                }
            }
            DESCRIBE_ACLS => {
                let rt = decode_describe_acls_request(&mut frame).unwrap();
                let st = state.lock();
                let acls: Vec<AclBinding> = st
                    .acls
                    .iter()
                    .filter(|a| rt == 1 || a.resource_type == rt)
                    .cloned()
                    .collect();
                encode_describe_acls_response(&mut body, &acls).unwrap();
            }
            DELETE_ACLS => {
                let rt = decode_delete_acls_request(&mut frame).unwrap();
                let mut st = state.lock();
                let before = st.acls.len();
                st.acls.retain(|a| rt != 1 && a.resource_type != rt);
                let removed = i32::try_from(before.saturating_sub(st.acls.len())).unwrap_or(0);
                encode_delete_acls_response(&mut body, removed).unwrap();
            }
            LIST_OFFSETS => {
                let (iso, topic, partition, current_epoch, timestamp) =
                    decode_list_offsets_request(&mut frame, header.api_version).unwrap();
                let _ = iso;
                let mut st = state.lock();
                st.last_list_offsets = Some((topic.clone(), partition, current_epoch));
                let key = (topic.clone(), partition);
                let leader = st.partition_leaders.get(&key).copied().unwrap_or(node_id);
                if leader != node_id {
                    st.list_offsets_not_leader = st.list_offsets_not_leader.saturating_add(1);
                    encode_list_offsets_response(
                        &mut body,
                        header.api_version,
                        &topic,
                        partition,
                        ListOffsetsPartition {
                            error_code: error::NOT_LEADER_OR_FOLLOWER,
                            timestamp,
                            offset: -1,
                            leader_epoch: -1,
                        },
                    )
                    .unwrap();
                } else {
                    st.last_list_offsets_node = Some(node_id);
                    let broker_epoch = st.partition_epochs.get(&key).copied().unwrap_or(0);
                    let error_code = if current_epoch != -1 && current_epoch < broker_epoch {
                        error::FENCED_LEADER_EPOCH
                    } else if current_epoch != -1 && current_epoch > broker_epoch {
                        error::UNKNOWN_LEADER_EPOCH
                    } else {
                        0
                    };
                    let log_start = *st.log_start.get(&key).unwrap_or(&0);
                    let hw = *st.next_offset.get(&key).unwrap_or(&0);
                    let (resp_ts, offset) = if error_code != 0 {
                        (-1, -1)
                    } else if timestamp == EARLIEST_TIMESTAMP {
                        (timestamp, log_start)
                    } else if timestamp == LATEST_TIMESTAMP {
                        (timestamp, hw)
                    } else {
                        st.log
                            .get(&key)
                            .and_then(|recs| recs.iter().find(|r| r.timestamp >= timestamp))
                            .map(|r| (r.timestamp, r.offset))
                            .unwrap_or((-1, -1))
                    };
                    encode_list_offsets_response(
                        &mut body,
                        header.api_version,
                        &topic,
                        partition,
                        ListOffsetsPartition {
                            error_code,
                            timestamp: resp_ts,
                            offset,
                            leader_epoch: if error_code == 0 { broker_epoch } else { -1 },
                        },
                    )
                    .unwrap();
                }
            }
            INIT_PRODUCER_ID => {
                let tid = buf::get_classic_nullable_string(&mut frame).unwrap();
                let txn_timeout = buf::get_i32(&mut frame).unwrap();
                if header.api_version >= 3 {
                    let _ = buf::get_i64(&mut frame).unwrap();
                    let _ = buf::get_i16(&mut frame).unwrap();
                }
                let mut st = state.lock();
                st.last_init_producer_id_timeout = Some(txn_timeout);
                st.init_producer_id_nodes.push(node_id);
                if tid.is_some() && st.txn_coord_node != node_id {
                    st.init_producer_id_not_coordinator =
                        st.init_producer_id_not_coordinator.saturating_add(1);
                    encode_init_producer_id_response(&mut body, header.api_version, 16, -1, -1)
                        .unwrap();
                } else {
                    st.last_init_producer_id_node = Some(node_id);
                    let pid = st.next_pid;
                    st.next_pid += 1;
                    encode_init_producer_id_response(&mut body, header.api_version, 0, pid, 0)
                        .unwrap();
                }
            }
            ADD_PARTITIONS_TO_TXN => {
                let (_tid, _pid, _epoch, topics) =
                    decode_add_partitions_to_txn_request(&mut frame).unwrap();
                let mut st = state.lock();
                if st.txn_coord_node != node_id {
                    encode_add_partitions_to_txn_response(&mut body, &topics, 16).unwrap();
                } else {
                    let n = topics.iter().map(|t| t.partitions.len()).sum();
                    st.in_txn = true;
                    st.add_partitions_to_txn_calls =
                        st.add_partitions_to_txn_calls.saturating_add(1);
                    st.last_add_partitions_to_txn = n;
                    st.last_add_partitions_node = Some(node_id);
                    encode_add_partitions_to_txn_response(&mut body, &topics, 0).unwrap();
                }
            }
            ADD_OFFSETS_TO_TXN => {
                let _ = decode_add_offsets_to_txn_request(&mut frame);
                let mut st = state.lock();
                if st.txn_coord_node != node_id {
                    encode_add_offsets_to_txn_response(&mut body, 16).unwrap();
                } else {
                    st.last_add_offsets_node = Some(node_id);
                    encode_add_offsets_to_txn_response(&mut body, 0).unwrap();
                }
            }
            END_TXN => {
                let (_tid, _pid, _epoch, committed) = decode_end_txn_request(&mut frame).unwrap();
                let mut st = state.lock();
                if st.txn_coord_node != node_id {
                    encode_end_txn_response(&mut body, 16).unwrap();
                } else {
                    if !committed {
                        let pending = std::mem::take(&mut st.txn_pending);
                        for rec in pending {
                            st.txn_aborted.insert(rec);
                        }
                    } else {
                        st.txn_pending.clear();
                    }
                    st.in_txn = false;
                    st.last_end_txn_node = Some(node_id);
                    encode_end_txn_response(&mut body, 0).unwrap();
                }
            }
            TXN_OFFSET_COMMIT => {
                let (_tid, _gid, topics) =
                    decode_txn_offset_commit_request(&mut frame, header.api_version).unwrap();
                let mut st = state.lock();
                if st.coord_node != node_id {
                    encode_txn_offset_commit_response(&mut body, &topics, 16).unwrap();
                } else {
                    st.txn_offset_commit_calls = st.txn_offset_commit_calls.saturating_add(1);
                    let mut nparts = 0usize;
                    let mut epochs = Vec::new();
                    for t in &topics {
                        for p in &t.partitions {
                            nparts = nparts.saturating_add(1);
                            epochs.push(p.leader_epoch);
                            let _ = st.committed.insert(
                                (t.topic.clone(), p.partition),
                                CommittedOffset {
                                    offset: p.offset,
                                    leader_epoch: p.leader_epoch,
                                    metadata: p.metadata.clone(),
                                },
                            );
                        }
                    }
                    st.last_txn_offset_commit_partitions = nparts;
                    st.last_txn_offset_epochs = epochs;
                    st.last_txn_offset_commit_node = Some(node_id);
                    encode_txn_offset_commit_response(&mut body, &topics, 0).unwrap();
                }
            }
            PRODUCE => {
                let decoded = decode_produce_request(&mut frame, header.api_version).unwrap();
                let txn_id = decoded.0;
                let mut parts = Vec::new();
                let mut st = state.lock();
                st.produce_requests.push(node_id);
                let forced = match (st.produce_error, st.produce_error_left) {
                    (Some(_), Some(0)) => {
                        st.produce_error = None;
                        st.produce_error_left = None;
                        None
                    }
                    (Some(c), Some(left)) => {
                        st.produce_error_left = Some(left.saturating_sub(1));
                        if left <= 1 {
                            st.produce_error = None;
                            st.produce_error_left = None;
                        }
                        Some(c)
                    }
                    (Some(c), None) => Some(c),
                    (None, _) => None,
                };
                for topic in decoded.3 {
                    for p in topic.partitions {
                        st.last_producer_id = Some(p.records.producer_id);
                        let key = (topic.topic.clone(), p.index);
                        let nrec = p.records.records.len() as i32;
                        let leader = st
                            .partition_leaders
                            .get(&(topic.topic.clone(), p.index))
                            .copied()
                            .unwrap_or(node_id);
                        let mut error_code = if leader != node_id {
                            6
                        } else if st.in_txn && txn_id.is_none() {
                            error::INVALID_TXN_STATE
                        } else {
                            forced.unwrap_or(0)
                        };
                        if error_code == 0 {
                            let pid = p.records.producer_id;
                            let seq = p.records.base_sequence;
                            if pid >= 0 && seq >= 0 {
                                let skey = (pid, topic.topic.clone(), p.index);
                                let expected = *st.expected_seq.get(&skey).unwrap_or(&0);
                                if seq != expected {
                                    error_code = 45;
                                } else {
                                    st.expected_seq.insert(skey, expected + nrec);
                                }
                            }
                        }
                        let start = *st.next_offset.get(&key).unwrap_or(&0);
                        if error_code == 0 {
                            st.accepted_produce.push(node_id);
                            st.last_produce_txn_id = txn_id.clone();
                            let pid = p.records.producer_id;
                            let mut n = 0i64;
                            for mut rec in p.records.records {
                                rec.offset = start + n;
                                st.log_producer
                                    .insert((topic.topic.clone(), p.index, rec.offset), pid);
                                st.log.entry(key.clone()).or_default().push(rec);
                                n += 1;
                            }
                            st.next_offset.insert(key, start + n);
                            if st.in_txn {
                                for o in 0..n {
                                    st.txn_pending
                                        .push((topic.topic.clone(), p.index, start + o));
                                }
                            }
                            parts.push(ProducePartitionResponse {
                                topic: topic.topic.clone(),
                                partition: p.index,
                                error_code: 0,
                                base_offset: start,
                                log_append_time_ms: -1,
                                log_start_offset: 0,
                            });
                        } else {
                            parts.push(ProducePartitionResponse {
                                topic: topic.topic.clone(),
                                partition: p.index,
                                error_code,
                                base_offset: -1,
                                log_append_time_ms: -1,
                                log_start_offset: 0,
                            });
                        }
                    }
                }
                encode_produce_response(&mut body, header.api_version, &parts).unwrap();
            }
            FETCH => {
                let (iso, max_bytes, req, rack) = decode_fetch_request(&mut frame).unwrap();
                let mut st = state.lock();
                st.last_fetch_isolation = iso;
                st.last_fetch_rack = rack.clone();
                st.last_fetch_max_bytes = max_bytes;
                st.last_fetch_partition_max_bytes = req
                    .first()
                    .and_then(|t| t.partitions.first())
                    .map(|p| p.partition_max_bytes)
                    .unwrap_or(0);
                let mut topics = Vec::new();
                for t in req {
                    let mut parts = Vec::new();
                    for p in t.partitions {
                        let leader = st
                            .partition_leaders
                            .get(&(t.topic.clone(), p.partition))
                            .copied()
                            .unwrap_or(node_id);
                        if leader != node_id && rack.is_empty() {
                            parts.push(FetchedPartition {
                                partition: p.partition,
                                error_code: 6,
                                high_watermark: 0,
                                last_stable_offset: 0,
                                log_start_offset: 0,
                                aborted_transactions: Vec::new(),
                                preferred_read_replica: -1,
                                records: Vec::new(),
                            });
                            continue;
                        }
                        if leader == node_id && !rack.is_empty() {
                            let follower = st.brokers.iter().find(|b| {
                                b.rack.as_deref() == Some(rack.as_str()) && b.node_id != leader
                            });
                            if let Some(f) = follower {
                                parts.push(FetchedPartition {
                                    partition: p.partition,
                                    error_code: 0,
                                    high_watermark: 0,
                                    last_stable_offset: 0,
                                    log_start_offset: 0,
                                    aborted_transactions: Vec::new(),
                                    preferred_read_replica: f.node_id,
                                    records: Vec::new(),
                                });
                                continue;
                            }
                        }
                        let current_epoch = st
                            .partition_epochs
                            .get(&(t.topic.clone(), p.partition))
                            .copied()
                            .unwrap_or(0);
                        if p.current_leader_epoch != -1 && p.current_leader_epoch < current_epoch {
                            parts.push(FetchedPartition {
                                partition: p.partition,
                                error_code: error::FENCED_LEADER_EPOCH,
                                high_watermark: 0,
                                last_stable_offset: 0,
                                log_start_offset: 0,
                                aborted_transactions: Vec::new(),
                                preferred_read_replica: -1,
                                records: Vec::new(),
                            });
                            continue;
                        }
                        if p.current_leader_epoch != -1 && p.current_leader_epoch > current_epoch {
                            parts.push(FetchedPartition {
                                partition: p.partition,
                                error_code: error::UNKNOWN_LEADER_EPOCH,
                                high_watermark: 0,
                                last_stable_offset: 0,
                                log_start_offset: 0,
                                aborted_transactions: Vec::new(),
                                preferred_read_replica: -1,
                                records: Vec::new(),
                            });
                            continue;
                        }
                        st.accepted_fetch.push(node_id);
                        let key = (t.topic.clone(), p.partition);
                        let recs = st
                            .log
                            .get(&key)
                            .map(|v| {
                                v.iter()
                                    .filter(|r| r.offset >= p.fetch_offset)
                                    .cloned()
                                    .collect::<Vec<_>>()
                            })
                            .unwrap_or_default();
                        let hw = *st.next_offset.get(&key).unwrap_or(&0);
                        let log_start = *st.log_start.get(&key).unwrap_or(&0);
                        let lso = if iso == 1 {
                            st.txn_pending
                                .iter()
                                .filter(|(tn, pn, _)| tn == &t.topic && *pn == p.partition)
                                .map(|(_, _, o)| *o)
                                .min()
                                .unwrap_or(hw)
                        } else {
                            hw
                        };
                        let mut aborted_transactions = Vec::new();
                        if iso == 1 {
                            let mut first_off: HashMap<i64, i64> = HashMap::new();
                            for (tn, pn, off) in &st.txn_aborted {
                                if tn == &t.topic && *pn == p.partition {
                                    if let Some(pid) =
                                        st.log_producer.get(&(tn.clone(), *pn, *off)).copied()
                                    {
                                        let e = first_off.entry(pid).or_insert(*off);
                                        if *off < *e {
                                            *e = *off;
                                        }
                                    }
                                }
                            }
                            aborted_transactions = first_off.into_iter().collect();
                        }
                        let error_code = if p.fetch_offset < log_start { 1 } else { 0 };
                        let batches = if error_code != 0 || recs.is_empty() {
                            Vec::new()
                        } else {
                            let first = recs[0].offset;
                            let pid = st
                                .log_producer
                                .get(&(t.topic.clone(), p.partition, first))
                                .copied()
                                .unwrap_or(-1);
                            let mut batch = RecordBatch::from_records(recs);
                            batch.base_offset = first;
                            batch.producer_id = pid;
                            batch.partition_leader_epoch = st
                                .partition_epochs
                                .get(&(t.topic.clone(), p.partition))
                                .copied()
                                .unwrap_or(0);
                            vec![batch]
                        };
                        parts.push(FetchedPartition {
                            partition: p.partition,
                            error_code,
                            high_watermark: hw,
                            last_stable_offset: lso,
                            log_start_offset: log_start,
                            aborted_transactions,
                            preferred_read_replica: -1,
                            records: batches,
                        });
                    }
                    topics.push(FetchedTopic {
                        topic: t.topic,
                        partitions: parts,
                    });
                }
                encode_fetch_response(&mut body, &topics).unwrap();
            }
            OFFSET_FOR_LEADER_EPOCH => {
                let (topic, partition, current, leader_epoch) =
                    decode_offset_for_leader_epoch_request(&mut frame, header.api_version).unwrap();
                let mut st = state.lock();
                st.last_epoch_req = Some((topic.clone(), partition, leader_epoch));
                let key = (topic.clone(), partition);
                let leader = st.partition_leaders.get(&key).copied().unwrap_or(node_id);
                let epoch = st.partition_epochs.get(&key).copied().unwrap_or(0);
                let end = *st.next_offset.get(&key).unwrap_or(&0);
                let error_code = if leader != node_id {
                    st.epoch_not_leader = st.epoch_not_leader.saturating_add(1);
                    error::NOT_LEADER_OR_FOLLOWER
                } else if current != -1 && current < epoch {
                    error::FENCED_LEADER_EPOCH
                } else if current != -1 && current > epoch {
                    error::UNKNOWN_LEADER_EPOCH
                } else {
                    st.last_epoch_node = Some(node_id);
                    0
                };
                encode_offset_for_leader_epoch_response(
                    &mut body,
                    header.api_version,
                    &topic,
                    partition,
                    error_code,
                    epoch,
                    end,
                )
                .unwrap();
            }
            SASL_HANDSHAKE => {
                let _mech = decode_sasl_handshake_request(&mut frame).unwrap_or_default();
                let (scram, oauth) = {
                    let st = state.lock();
                    (st.scram_user.clone(), st.oauth_principal.clone())
                };
                if let Some((alg, _, _)) = scram {
                    encode_sasl_handshake_response(&mut body, 0, &[alg.name()]).unwrap();
                } else if oauth.is_some() {
                    encode_sasl_handshake_response(&mut body, 0, &["OAUTHBEARER"]).unwrap();
                } else {
                    encode_sasl_handshake_response(&mut body, 0, &["PLAIN"]).unwrap();
                }
            }
            SASL_AUTHENTICATE => {
                let bytes = decode_sasl_authenticate_request(&mut frame).unwrap();
                let (scram_user, oauth_principal, sasl_user) = {
                    let st = state.lock();
                    (
                        st.scram_user.clone(),
                        st.oauth_principal.clone(),
                        st.sasl_user.clone(),
                    )
                };
                if let Some((alg, _, pass)) = scram_user {
                    match scram_step.take() {
                        None => {
                            let first = String::from_utf8_lossy(&bytes);
                            match scram::server_first(
                                &first,
                                "SrvNonceMock0001",
                                b"saltsalt16bytes!",
                                4096,
                            ) {
                                Ok((sf, bare)) => {
                                    scram_step = Some((alg, pass, bare, sf.clone()));
                                    encode_sasl_authenticate_response(
                                        &mut body,
                                        0,
                                        None,
                                        sf.as_bytes(),
                                    )
                                    .unwrap();
                                }
                                Err(_) => {
                                    encode_sasl_authenticate_response(
                                        &mut body,
                                        58,
                                        Some("bad scram first"),
                                        &[],
                                    )
                                    .unwrap();
                                }
                            }
                        }
                        Some((alg, pass, bare, sf)) => {
                            let cf = String::from_utf8_lossy(&bytes);
                            match scram::server_final(alg, &pass, &bare, &sf, &cf) {
                                Ok(fin) => {
                                    authed = true;
                                    encode_sasl_authenticate_response(
                                        &mut body,
                                        0,
                                        None,
                                        fin.as_bytes(),
                                    )
                                    .unwrap();
                                }
                                Err(_) => {
                                    encode_sasl_authenticate_response(
                                        &mut body,
                                        58,
                                        Some("bad scram proof"),
                                        &[],
                                    )
                                    .unwrap();
                                }
                            }
                        }
                    }
                } else if let Some(expected) = oauth_principal {
                    let ok = oauth::token_from_initial(&bytes)
                        .and_then(|t| oauth::principal_from_jwt(&t))
                        .map(|p| p == expected)
                        .unwrap_or(false);
                    authed = ok;
                    encode_sasl_authenticate_response(
                        &mut body,
                        if ok { 0 } else { 58 },
                        if ok { None } else { Some("bad oauth token") },
                        &[],
                    )
                    .unwrap();
                } else {
                    let parsed = parse_plain_auth_bytes(&bytes);
                    let ok = match (parsed, sasl_user) {
                        (Some(got), Some(exp)) => got == exp,
                        _ => false,
                    };
                    authed = ok;
                    encode_sasl_authenticate_response(
                        &mut body,
                        if ok { 0 } else { 58 },
                        if ok { None } else { Some("bad credentials") },
                        &[],
                    )
                    .unwrap();
                }
            }
            FIND_COORDINATOR => {
                let (_key, key_type) = decode_find_coordinator_request(&mut frame).unwrap();
                let mut st = state.lock();
                st.find_coordinator_key_types.push(key_type);
                let coord = if key_type == COORDINATOR_TRANSACTION {
                    if st.stale_txn_finds > 0 {
                        st.stale_txn_finds = st.stale_txn_finds.saturating_sub(1);
                        st.brokers
                            .iter()
                            .map(|b| b.node_id)
                            .find(|id| *id != st.txn_coord_node)
                            .unwrap_or(st.txn_coord_node)
                    } else {
                        st.txn_coord_node
                    }
                } else {
                    st.coord_node
                };
                let (host, port) = broker_host_port(&st, coord);
                encode_find_coordinator_response(&mut body, coord, &host, port).unwrap();
            }
            SHARE_GROUP_HEARTBEAT => {
                let req = decode_share_group_heartbeat_request(&mut frame).unwrap();
                let mut st = state.lock();
                st.share_heartbeat_calls = st.share_heartbeat_calls.saturating_add(1);
                let n = st.hb_by_node.entry(node_id).or_insert(0);
                *n = n.saturating_add(1);
                let (member_id, epoch, assignment) = match req.member_epoch.cmp(&0) {
                    std::cmp::Ordering::Less => (req.member_id, -1, None),
                    std::cmp::Ordering::Equal => {
                        let names = match &req.subscribed_topic_names {
                            Some(n) if n.is_empty() => Vec::new(),
                            Some(n) => n.clone(),
                            None => vec!["t".into()],
                        };
                        let assignment = names
                            .iter()
                            .map(|name| {
                                let npart = st
                                    .created_topics
                                    .get(name)
                                    .map(|s| s.num_partitions)
                                    .unwrap_or(1);
                                ShareTopicPartitions {
                                    topic_id: mock_topic_id(name),
                                    partitions: (0..npart).collect(),
                                }
                            })
                            .collect();
                        (req.member_id, 1, Some(assignment))
                    }
                    std::cmp::Ordering::Greater => (req.member_id, req.member_epoch, None),
                };
                encode_share_group_heartbeat_response(
                    &mut body,
                    &ShareGroupHeartbeatResponse {
                        error_code: 0,
                        error_message: None,
                        member_id: Some(member_id),
                        member_epoch: epoch,
                        heartbeat_interval_ms: 5000,
                        assignment,
                    },
                )
                .unwrap();
            }
            SHARE_FETCH => {
                let (_gid, member_id, epoch, max_records, topics) =
                    decode_share_fetch_request(&mut frame).unwrap();
                let mut st = state.lock();
                let tps: Vec<(String, i32)> = topics
                    .iter()
                    .flat_map(|t| {
                        let name = topic_name_for_id(&st, t.topic_id);
                        t.partitions
                            .iter()
                            .map(move |p| (name.clone(), p.partition))
                    })
                    .collect();
                st.share_fetch_calls = st.share_fetch_calls.saturating_add(1);
                st.last_share_fetch_epoch = Some(epoch);
                if !tps.is_empty() && share_wrong_leader(&st, node_id, &tps) {
                    st.share_fetch_not_leader = st.share_fetch_not_leader.saturating_add(1);
                    encode_share_fetch_error(&mut body, error::NOT_LEADER_OR_FOLLOWER).unwrap();
                } else {
                    st.last_share_fetch_node = Some(node_id);
                    let sess = share_session_step(&mut st, &member_id, epoch);
                    if sess != 0 {
                        encode_share_fetch_error(&mut body, sess).unwrap();
                    } else {
                        let cap = usize::try_from(max_records.max(0)).unwrap_or(0);
                        let mut fetched = Vec::new();
                        for t in topics {
                            let name = topic_name_for_id(&st, t.topic_id);
                            let mut parts = Vec::new();
                            for p in t.partitions {
                                apply_share_acks(
                                    &mut st,
                                    &member_id,
                                    &name,
                                    p.partition,
                                    &p.acknowledgements,
                                );
                                let key = (name.clone(), p.partition);
                                let recs = st.log.get(&key).cloned().unwrap_or_default();
                                let recs: Vec<_> = recs
                                    .into_iter()
                                    .filter(|r| {
                                        let k = (name.clone(), p.partition, r.offset);
                                        !st.share_accepted.contains(&k)
                                            && match st.share_acquired.get(&k) {
                                                None => true,
                                                Some(owner) => owner == &member_id,
                                            }
                                    })
                                    .collect();
                                let mut acquired = Vec::new();
                                let mut taken = Vec::new();
                                for r in recs {
                                    if taken.len() >= cap {
                                        break;
                                    }
                                    let k = (name.clone(), p.partition, r.offset);
                                    if let std::collections::hash_map::Entry::Vacant(e) =
                                        st.share_acquired.entry(k)
                                    {
                                        e.insert(member_id.clone());
                                        acquired.push(AcquiredRange {
                                            first_offset: r.offset,
                                            last_offset: r.offset,
                                            delivery_count: 1,
                                        });
                                        taken.push(r);
                                    }
                                }
                                let epoch = st
                                    .partition_epochs
                                    .get(&(name.clone(), p.partition))
                                    .copied()
                                    .unwrap_or(0);
                                parts.push(ShareFetchedPartition {
                                    partition: p.partition,
                                    error_code: 0,
                                    records: share_record_batches(taken, epoch),
                                    acquired,
                                });
                            }
                            fetched.push(ShareFetchedTopic {
                                topic_id: t.topic_id,
                                partitions: parts,
                            });
                        }
                        encode_share_fetch_response(&mut body, &fetched).unwrap();
                    }
                }
            }
            SHARE_ACKNOWLEDGE => {
                let (_gid, member_id, epoch, acks) =
                    decode_share_acknowledge_request(&mut frame).unwrap();
                let mut st = state.lock();
                let tps: Vec<(String, i32)> = acks
                    .iter()
                    .map(|(tid, p, _)| (topic_name_for_id(&st, *tid), *p))
                    .collect();
                st.share_ack_calls = st.share_ack_calls.saturating_add(1);
                st.last_share_ack_epoch = Some(epoch);
                st.last_share_ack_partitions = acks.len();
                if epoch != -1 && !tps.is_empty() && share_wrong_leader(&st, node_id, &tps) {
                    encode_share_acknowledge_response(&mut body, error::NOT_LEADER_OR_FOLLOWER)
                        .unwrap();
                } else {
                    st.last_share_ack_node = Some(node_id);
                    let sess = share_session_step(&mut st, &member_id, epoch);
                    if sess != 0 {
                        encode_share_acknowledge_response(&mut body, sess).unwrap();
                    } else {
                        for (tid, partition, batches) in acks {
                            let name = topic_name_for_id(&st, tid);
                            apply_share_acks(&mut st, &member_id, &name, partition, &batches);
                        }
                        encode_share_acknowledge_response(&mut body, 0).unwrap();
                    }
                }
            }
            CONSUMER_GROUP_HEARTBEAT => {
                let req = decode_consumer_group_heartbeat_request(&mut frame).unwrap();
                let mut st = state.lock();
                st.cg_heartbeat_calls = st.cg_heartbeat_calls.saturating_add(1);
                st.last_group_instance_id = req.instance_id.clone();
                st.last_group_rack = req.rack_id.clone();
                let n = st.hb_by_node.entry(node_id).or_insert(0);
                *n = n.saturating_add(1);
                let (member_id, epoch, assignment) = match req.member_epoch.cmp(&0) {
                    std::cmp::Ordering::Less => {
                        let empty = if let Some(g) = st.kip848_groups.get_mut(&req.group_id) {
                            let _ = g.members.remove(&req.member_id);
                            g.members.is_empty()
                        } else {
                            false
                        };
                        if empty {
                            let _ = st.kip848_groups.remove(&req.group_id);
                        } else {
                            kip848_recompute(&mut st, &req.group_id);
                        }
                        (req.member_id, -1, None)
                    }
                    std::cmp::Ordering::Equal => {
                        st.member_seq += 1;
                        let id = format!("k-{}", st.member_seq);
                        let topic_names = match &req.subscribed_topic_names {
                            Some(n) if n.is_empty() => Vec::new(),
                            Some(n) => n.clone(),
                            None => vec!["t".into()],
                        };
                        let g = st.kip848_groups.entry(req.group_id.clone()).or_default();
                        let _ = g.members.insert(
                            id.clone(),
                            Kip848Member {
                                topics: topic_names,
                                epoch: 0,
                                partitions: Vec::new(),
                                pending: false,
                            },
                        );
                        kip848_recompute(&mut st, &req.group_id);
                        let (epoch, partitions) = st
                            .kip848_groups
                            .get_mut(&req.group_id)
                            .and_then(|g| g.members.get_mut(&id))
                            .map(|m| {
                                m.pending = false;
                                (m.epoch, m.partitions.clone())
                            })
                            .unwrap_or((1, Vec::new()));
                        (id, epoch, Some(kip848_topic_partitions(&partitions)))
                    }
                    std::cmp::Ordering::Greater => {
                        let found = st
                            .kip848_groups
                            .get_mut(&req.group_id)
                            .and_then(|g| g.members.get_mut(&req.member_id));
                        match found {
                            Some(m) if m.pending || req.member_epoch < m.epoch => {
                                m.pending = false;
                                (
                                    req.member_id,
                                    m.epoch,
                                    Some(kip848_topic_partitions(&m.partitions)),
                                )
                            }
                            Some(m) => (req.member_id, m.epoch, None),
                            None => (req.member_id, req.member_epoch, None),
                        }
                    }
                };
                encode_consumer_group_heartbeat_response(
                    &mut body,
                    &ConsumerGroupHeartbeatResponse {
                        error_code: 0,
                        error_message: None,
                        member_id: Some(member_id),
                        member_epoch: epoch,
                        heartbeat_interval_ms: 5000,
                        assignment,
                    },
                )
                .unwrap();
            }
            JOIN_GROUP => {
                let (gid, member_id, instance, metadata) =
                    decode_join_group_request(&mut frame).unwrap();
                let mut st = state.lock();
                st.join_group_calls = st.join_group_calls.saturating_add(1);
                st.last_group_instance_id = instance;
                if member_id.is_empty() {
                    st.member_seq += 1;
                    let assigned = format!("m-{}", st.member_seq);
                    encode_join_group_response(&mut body, 79, -1, "range", "", &assigned, &[])
                        .unwrap();
                } else {
                    let notify = st.assign_notify.clone();
                    let g = st.groups.entry(gid).or_insert_with(|| GroupReg {
                        members: BTreeMap::new(),
                        generation: 0,
                        joined: HashSet::new(),
                        assignments: HashMap::new(),
                        hb_total: 0,
                    });
                    let mut bumped = false;
                    if !g.members.contains_key(&member_id) || g.joined.contains(&member_id) {
                        g.generation += 1;
                        g.joined.clear();
                        g.assignments.clear();
                        bumped = true;
                    }
                    g.members.insert(member_id.clone(), metadata.clone());
                    g.joined.insert(member_id.clone());
                    let leader = g.members.keys().next().cloned().unwrap_or_default();
                    let members: Vec<JoinMember> = g
                        .members
                        .iter()
                        .map(|(id, md)| JoinMember {
                            member_id: id.clone(),
                            metadata: md.clone(),
                        })
                        .collect();
                    let gen = g.generation;
                    drop(st);
                    if bumped {
                        notify.notify_waiters();
                    }
                    encode_join_group_response(
                        &mut body, 0, gen, "range", &leader, &member_id, &members,
                    )
                    .unwrap();
                }
            }
            SYNC_GROUP => {
                {
                    let mut st = state.lock();
                    st.sync_group_calls = st.sync_group_calls.saturating_add(1);
                }
                let (gid, member_id, assignments) = decode_sync_group_request(&mut frame).unwrap();
                let notify = state.lock().assign_notify.clone();
                if !assignments.is_empty() {
                    let mut st = state.lock();
                    if let Some(g) = st.groups.get_mut(&gid) {
                        g.assignments.clear();
                        for (id, bytes) in assignments {
                            g.assignments.insert(id, bytes);
                        }
                    }
                    notify.notify_waiters();
                }
                let mut asg = Vec::new();
                for _ in 0..40 {
                    {
                        let st = state.lock();
                        if let Some(g) = st.groups.get(&gid) {
                            if let Some(b) = g.assignments.get(&member_id) {
                                asg = b.clone();
                                break;
                            }
                        }
                    }
                    tokio::time::sleep(std::time::Duration::from_millis(25)).await;
                }
                encode_sync_group_response(&mut body, 0, &asg).unwrap();
            }
            HEARTBEAT => {
                let (gid, _gen, member_id) = decode_heartbeat_request(&mut frame).unwrap();
                let mut st = state.lock();
                let mut err = 0i16;
                if let Some(g) = st.groups.get_mut(&gid) {
                    g.hb_total += 1;
                    if g.members.contains_key(&member_id) && !g.joined.contains(&member_id) {
                        err = 27;
                    }
                }
                let n = st.hb_by_node.entry(node_id).or_insert(0);
                *n = n.saturating_add(1);
                encode_heartbeat_response(&mut body, err).unwrap();
            }
            LEAVE_GROUP => {
                let (gid, member_id) = decode_leave_group_request(&mut frame).unwrap();
                let mut st = state.lock();
                if let Some(g) = st.groups.get_mut(&gid) {
                    g.members.remove(&member_id);
                    g.joined.remove(&member_id);
                    g.generation += 1;
                    g.joined.clear();
                    g.assignments.clear();
                }
                st.assign_notify.notify_waiters();
                encode_leave_group_response(&mut body, 0).unwrap();
            }
            OFFSET_COMMIT => {
                let (_g, _m, topics) = decode_offset_commit_request(&mut frame).unwrap();
                let mut st = state.lock();
                st.offset_commit_calls = st.offset_commit_calls.saturating_add(1);
                if st.offset_commit_load_left > 0 {
                    st.offset_commit_load_left = st.offset_commit_load_left.saturating_sub(1);
                    st.offset_commit_load_in_progress =
                        st.offset_commit_load_in_progress.saturating_add(1);
                    encode_offset_commit_response(
                        &mut body,
                        &topics,
                        error::COORDINATOR_LOAD_IN_PROGRESS,
                    )
                    .unwrap();
                } else {
                    let mut nparts = 0usize;
                    for t in &topics {
                        nparts = nparts.saturating_add(t.partitions.len());
                        for p in &t.partitions {
                            st.committed.insert(
                                (t.topic.clone(), p.partition),
                                CommittedOffset {
                                    offset: p.offset,
                                    leader_epoch: p.leader_epoch,
                                    metadata: p.metadata.clone(),
                                },
                            );
                        }
                    }
                    st.last_offset_commit_partitions = nparts;
                    st.last_offset_commit_node = Some(node_id);
                    encode_offset_commit_response(&mut body, &topics, 0).unwrap();
                }
            }
            OFFSET_FETCH => {
                let (_g, topics) = decode_offset_fetch_request(&mut frame).unwrap();
                let mut st = state.lock();
                st.offset_fetch_calls = st.offset_fetch_calls.saturating_add(1);
                let mut nparts = 0usize;
                let mut out = Vec::with_capacity(topics.len());
                for t in topics {
                    nparts = nparts.saturating_add(t.partitions.len());
                    let mut parts = Vec::with_capacity(t.partitions.len());
                    for p in t.partitions {
                        let (off, epoch, meta) = st
                            .committed
                            .get(&(t.topic.clone(), p))
                            .map(|c| (c.offset, c.leader_epoch, c.metadata.clone()))
                            .unwrap_or((-1, -1, String::new()));
                        parts.push(FetchedOffset {
                            partition: p,
                            offset: off,
                            leader_epoch: epoch,
                            metadata: meta,
                            error_code: 0,
                        });
                    }
                    out.push(FetchedOffsetTopic {
                        topic: t.topic,
                        partitions: parts,
                    });
                }
                st.last_offset_fetch_partitions = nparts;
                encode_offset_fetch_response(&mut body, &out).unwrap();
            }
            OFFSET_DELETE => {
                let (_gid, topics) = decode_offset_delete_request(&mut frame).unwrap();
                let mut st = state.lock();
                let mut results = Vec::new();
                for t in topics {
                    for p in t.partitions {
                        let _removed = st.committed.remove(&(t.topic.clone(), p));
                        results.push(OffsetDeleteResult {
                            topic: t.topic.clone(),
                            partition: p,
                            error_code: 0,
                        });
                    }
                }
                st.last_offset_delete_node = Some(node_id);
                encode_offset_delete_response(&mut body, 0, &results).unwrap();
            }
            CONSUMER_GROUP_DESCRIBE => {
                let (ids, _include) = decode_consumer_group_describe_request(&mut frame).unwrap();
                let mut st = state.lock();
                if st.coord_node != node_id {
                    st.consumer_group_describe_not_coordinator =
                        st.consumer_group_describe_not_coordinator.saturating_add(1);
                    // Per-group 16 only. Do not invent a member store,
                    // a 41 path, or a 6 path.
                    let results: Vec<DescribedConsumerGroup> = ids
                        .into_iter()
                        .map(|group_id| {
                            DescribedConsumerGroup::new(group_id, error::NOT_COORDINATOR)
                        })
                        .collect();
                    encode_consumer_group_describe_response(&mut body, &results).unwrap();
                } else {
                    st.last_consumer_group_describe_node = Some(node_id);
                    let results: Vec<DescribedConsumerGroup> = ids
                        .into_iter()
                        .map(|group_id| {
                            let mut g = DescribedConsumerGroup::new(group_id, 0);
                            g.group_state = "Stable".into();
                            g.group_epoch = 1;
                            g.assignment_epoch = 1;
                            g.assignor_name = "uniform".into();
                            g
                        })
                        .collect();
                    encode_consumer_group_describe_response(&mut body, &results).unwrap();
                }
            }
            DESCRIBE_GROUPS => {
                let (ids, _include) = decode_describe_groups_request(&mut frame).unwrap();
                let mut st = state.lock();
                if st.coord_node != node_id {
                    st.describe_groups_not_coordinator =
                        st.describe_groups_not_coordinator.saturating_add(1);
                    // Per-group 16 only. Do not invent a member store,
                    // a 41 path, or a 6 path.
                    let results: Vec<DescribedGroup> = ids
                        .into_iter()
                        .map(|group_id| DescribedGroup::new(group_id, error::NOT_COORDINATOR))
                        .collect();
                    encode_describe_groups_response(&mut body, &results).unwrap();
                } else {
                    st.last_describe_groups_node = Some(node_id);
                    let results: Vec<DescribedGroup> = ids
                        .into_iter()
                        .map(|group_id| {
                            let mut g = DescribedGroup::new(group_id, 0);
                            g.group_state = "Stable".into();
                            g.protocol_type = "consumer".into();
                            g
                        })
                        .collect();
                    encode_describe_groups_response(&mut body, &results).unwrap();
                }
            }
            LIST_GROUPS => {
                let (states, types) = decode_list_groups_request(&mut frame).unwrap();
                let mut st = state.lock();
                // Any connected broker answers. Fixture list only; not a
                // group store, not a coordinator hop, not a 41/6 path.
                // Official listed errors do not include NOT_COORDINATOR
                // (16), so the wrong node does not return 16.
                st.last_list_groups_node = Some(node_id);
                st.last_list_groups = Some((states, types));
                encode_list_groups_response(
                    &mut body,
                    &ListGroupsResponse {
                        error_code: 0,
                        groups: vec![ListedGroup {
                            group_id: "g".into(),
                            protocol_type: "consumer".into(),
                            group_state: "Stable".into(),
                            group_type: "classic".into(),
                        }],
                    },
                )
                .unwrap();
            }
            DELETE_GROUPS => {
                let ids = decode_delete_groups_request(&mut frame).unwrap();
                let mut st = state.lock();
                if st.coord_node != node_id {
                    st.delete_groups_not_coordinator =
                        st.delete_groups_not_coordinator.saturating_add(1);
                    // Per-group 16 only. Do not invent a group store,
                    // a 41 path, or a 6 path.
                    let results: Vec<DeletableGroupResult> = ids
                        .into_iter()
                        .map(|group_id| DeletableGroupResult::new(group_id, error::NOT_COORDINATOR))
                        .collect();
                    encode_delete_groups_response(&mut body, &results).unwrap();
                } else {
                    st.last_delete_groups_node = Some(node_id);
                    let results: Vec<DeletableGroupResult> = ids
                        .into_iter()
                        .map(|group_id| DeletableGroupResult::new(group_id, 0))
                        .collect();
                    encode_delete_groups_response(&mut body, &results).unwrap();
                }
            }
            SHARE_GROUP_DESCRIBE => {
                let (ids, _include) = decode_share_group_describe_request(&mut frame).unwrap();
                let mut st = state.lock();
                if st.coord_node != node_id {
                    st.share_group_describe_not_coordinator =
                        st.share_group_describe_not_coordinator.saturating_add(1);
                    // Per-group 16 only. Do not invent a member store,
                    // a 41 path, or a 6 path.
                    let results: Vec<DescribedShareGroup> = ids
                        .into_iter()
                        .map(|group_id| DescribedShareGroup::new(group_id, error::NOT_COORDINATOR))
                        .collect();
                    encode_share_group_describe_response(&mut body, &results).unwrap();
                } else {
                    st.last_share_group_describe_node = Some(node_id);
                    let results: Vec<DescribedShareGroup> = ids
                        .into_iter()
                        .map(|group_id| {
                            let mut g = DescribedShareGroup::new(group_id, 0);
                            g.group_state = "Stable".into();
                            g.group_epoch = 1;
                            g.assignment_epoch = 1;
                            g.assignor_name = "uniform".into();
                            g
                        })
                        .collect();
                    encode_share_group_describe_response(&mut body, &results).unwrap();
                }
            }
            DESCRIBE_SHARE_GROUP_OFFSETS => {
                let groups = decode_describe_share_group_offsets_request(&mut frame).unwrap();
                let mut st = state.lock();
                if st.coord_node != node_id {
                    st.describe_share_group_offsets_not_coordinator = st
                        .describe_share_group_offsets_not_coordinator
                        .saturating_add(1);
                    // Per-group 16 only. Do not invent an offset store,
                    // a 41 path, or a 6 path.
                    let results: Vec<DescribedShareGroupOffsets> = groups
                        .into_iter()
                        .map(|g| {
                            DescribedShareGroupOffsets::new(g.group_id, error::NOT_COORDINATOR)
                        })
                        .collect();
                    encode_describe_share_group_offsets_response(&mut body, &results).unwrap();
                } else {
                    st.last_describe_share_group_offsets_node = Some(node_id);
                    let results: Vec<DescribedShareGroupOffsets> = groups
                        .into_iter()
                        .map(|g| DescribedShareGroupOffsets::new(g.group_id, 0))
                        .collect();
                    encode_describe_share_group_offsets_response(&mut body, &results).unwrap();
                }
            }
            ALTER_SHARE_GROUP_OFFSETS => {
                let (_group_id, _topics) =
                    decode_alter_share_group_offsets_request(&mut frame).unwrap();
                let mut st = state.lock();
                if st.coord_node != node_id {
                    st.alter_share_group_offsets_not_coordinator = st
                        .alter_share_group_offsets_not_coordinator
                        .saturating_add(1);
                    // Top-level 16 only. Do not invent an offset store,
                    // a 41 path, or a 6 path.
                    encode_alter_share_group_offsets_response(
                        &mut body,
                        &AlteredShareGroupOffsets::new(error::NOT_COORDINATOR),
                    )
                    .unwrap();
                } else {
                    st.last_alter_share_group_offsets_node = Some(node_id);
                    encode_alter_share_group_offsets_response(
                        &mut body,
                        &AlteredShareGroupOffsets::new(0),
                    )
                    .unwrap();
                }
            }
            DELETE_SHARE_GROUP_OFFSETS => {
                let (_group_id, _topics) =
                    decode_delete_share_group_offsets_request(&mut frame).unwrap();
                let mut st = state.lock();
                if st.coord_node != node_id {
                    st.delete_share_group_offsets_not_coordinator = st
                        .delete_share_group_offsets_not_coordinator
                        .saturating_add(1);
                    // Top-level 16 only. Do not invent an offset store,
                    // a 41 path, or a 6 path.
                    encode_delete_share_group_offsets_response(
                        &mut body,
                        &DeletedShareGroupOffsets::new(error::NOT_COORDINATOR),
                    )
                    .unwrap();
                } else {
                    st.last_delete_share_group_offsets_node = Some(node_id);
                    encode_delete_share_group_offsets_response(
                        &mut body,
                        &DeletedShareGroupOffsets::new(0),
                    )
                    .unwrap();
                }
            }
            DESCRIBE_TOPIC_PARTITIONS => {
                let (topics, limit, cursor) =
                    decode_describe_topic_partitions_request(&mut frame).unwrap();
                let mut st = state.lock();
                // Any connected broker answers. Fixture topic only; not
                // a metadata store, not a coordinator hop, not a 41/6
                // path. Official JSON lists no error codes; official
                // handler does not use NOT_COORDINATOR (16), so the
                // wrong node does not return 16.
                st.last_describe_topic_partitions_node = Some(node_id);
                st.last_describe_topic_partitions = Some((topics.clone(), limit, cursor));
                let name = topics.first().cloned().unwrap_or_else(|| "t".into());
                let mut topic = DescribedTopicPartitions::new(name, 0);
                topic.partitions = vec![DescribedTopicPartition {
                    error_code: 0,
                    partition_index: 0,
                    leader_id: 1,
                    leader_epoch: 0,
                    replica_nodes: vec![1],
                    isr_nodes: vec![1],
                    eligible_leader_replicas: None,
                    last_known_elr: None,
                    offline_replicas: Vec::new(),
                }];
                encode_describe_topic_partitions_response(
                    &mut body,
                    &DescribeTopicPartitionsResponse::new(vec![topic]),
                )
                .unwrap();
            }
            LIST_CONFIG_RESOURCES => {
                let types = decode_list_config_resources_request(&mut frame).unwrap();
                let mut st = state.lock();
                // Any connected broker answers. Fixture resource only;
                // not a config store, not a coordinator hop, not a
                // 41/6 path. Official JSON lists no error codes;
                // official handler does not use NOT_COORDINATOR (16),
                // so the wrong node does not return 16.
                st.last_list_config_resources_node = Some(node_id);
                st.last_list_config_resources = Some(types);
                encode_list_config_resources_response(
                    &mut body,
                    &ListConfigResourcesResponse::new(
                        0,
                        vec![ListedConfigResource::new("r", RESOURCE_CLIENT_METRICS)],
                    ),
                )
                .unwrap();
            }
            GET_TELEMETRY_SUBSCRIPTIONS => {
                let client_instance_id =
                    decode_get_telemetry_subscriptions_request(&mut frame).unwrap();
                let mut st = state.lock();
                // Any connected broker answers. Fixture subscription
                // only; not a telemetry store, not a coordinator hop,
                // not a 41/6 path. Official JSON lists no error codes;
                // official handler does not use NOT_COORDINATOR (16),
                // so the wrong node does not return 16.
                st.last_get_telemetry_subscriptions_node = Some(node_id);
                st.last_get_telemetry_subscriptions = Some(client_instance_id);
                let assigned = if client_instance_id == [0; 16] {
                    [0x11; 16]
                } else {
                    client_instance_id
                };
                encode_get_telemetry_subscriptions_response(
                    &mut body,
                    &GetTelemetrySubscriptionsResponse::new(
                        0,
                        assigned,
                        1,
                        vec![1],
                        1000,
                        100,
                        true,
                        vec!["m".into()],
                    ),
                )
                .unwrap();
            }
            PUSH_TELEMETRY => {
                let req = decode_push_telemetry_request(&mut frame).unwrap();
                let mut st = state.lock();
                // Any connected broker answers. Fixture ack only; not
                // a telemetry store, not a coordinator hop, not a
                // 41/6 path. Official JSON lists no error codes;
                // official handler does not use NOT_COORDINATOR (16),
                // so the wrong node does not return 16.
                st.last_push_telemetry_node = Some(node_id);
                st.last_push_telemetry = Some(LastPushTelemetry {
                    client_instance_id: req.client_instance_id,
                    subscription_id: req.subscription_id,
                    terminating: req.terminating,
                    compression_type: req.compression_type,
                    metrics: req.metrics,
                });
                encode_push_telemetry_response(&mut body, &PushTelemetryResponse::new(0)).unwrap();
            }
            ASSIGN_REPLICAS_TO_DIRS => {
                let req = decode_assign_replicas_to_dirs_request(&mut frame).unwrap();
                let mut st = state.lock();
                if st.controller_node != node_id {
                    st.assign_replicas_to_dirs_not_controller =
                        st.assign_replicas_to_dirs_not_controller.saturating_add(1);
                    // 41 only. Do not invent a replica-dir store on
                    // the wrong node.
                    encode_assign_replicas_to_dirs_response(
                        &mut body,
                        &AssignReplicasToDirsResponse::new(error::NOT_CONTROLLER, vec![]),
                    )
                    .unwrap();
                } else {
                    st.last_assign_replicas_to_dirs_node = Some(node_id);
                    // Echo the request directories with per-partition
                    // error 0. Fixture ack only; not a replica-dir
                    // store.
                    let directories = req
                        .directories
                        .iter()
                        .map(|d| {
                            AssignReplicasToDirsResponseDirectory::new(
                                d.id,
                                d.topics
                                    .iter()
                                    .map(|t| {
                                        AssignReplicasToDirsResponseTopic::new(
                                            t.topic_id,
                                            t.partitions
                                                .iter()
                                                .map(|p| {
                                                    AssignReplicasToDirsResponsePartition::new(
                                                        p.partition_index,
                                                        0,
                                                    )
                                                })
                                                .collect(),
                                        )
                                    })
                                    .collect(),
                            )
                        })
                        .collect();
                    st.last_assign_replicas_to_dirs = Some(req);
                    encode_assign_replicas_to_dirs_response(
                        &mut body,
                        &AssignReplicasToDirsResponse::new(0, directories),
                    )
                    .unwrap();
                }
            }
            ALTER_REPLICA_LOG_DIRS => {
                let req = decode_alter_replica_log_dirs_request(&mut frame).unwrap();
                let mut st = state.lock();
                // Any connected broker answers. Fixture ack only; not
                // a log-dir store, not a coordinator hop, not a
                // 41/6 path. Official JSON lists no error codes;
                // official handler does not use NOT_COORDINATOR (16)
                // or NOT_CONTROLLER (41), so the wrong node does not
                // return 16 or 41.
                st.last_alter_replica_log_dirs_node = Some(node_id);
                let results = req
                    .dirs
                    .iter()
                    .flat_map(|d| d.topics.iter())
                    .map(|t| {
                        AlterReplicaLogDirsResponseTopic::new(
                            t.name.clone(),
                            t.partitions
                                .iter()
                                .map(|p| AlterReplicaLogDirsResponsePartition::new(*p, 0))
                                .collect(),
                        )
                    })
                    .collect();
                st.last_alter_replica_log_dirs = Some(req);
                encode_alter_replica_log_dirs_response(
                    &mut body,
                    &AlterReplicaLogDirsResponse::new(results),
                )
                .unwrap();
            }
            DESCRIBE_LOG_DIRS => {
                let req = decode_describe_log_dirs_request(&mut frame).unwrap();
                let mut st = state.lock();
                // Any connected broker answers. Fixture ack only; not
                // a log-dir store, not a coordinator hop, not a
                // 41/6 path. Official JSON lists no error codes;
                // official handler does not use NOT_COORDINATOR (16)
                // or NOT_CONTROLLER (41), so the wrong node does not
                // return 16 or 41.
                st.last_describe_log_dirs_node = Some(node_id);
                let topics = req
                    .topics
                    .as_ref()
                    .map(|topics| {
                        topics
                            .iter()
                            .map(|t| {
                                DescribeLogDirsTopic::new(
                                    t.name.clone(),
                                    t.partitions
                                        .iter()
                                        .map(|p| DescribeLogDirsPartition::new(*p, 0, 0, false))
                                        .collect(),
                                )
                            })
                            .collect()
                    })
                    .unwrap_or_default();
                st.last_describe_log_dirs = Some(req);
                encode_describe_log_dirs_response(
                    &mut body,
                    &DescribeLogDirsResponse::new(
                        0,
                        vec![DescribeLogDirsResult::new(0, "/d", topics, -1, -1)],
                    ),
                )
                .unwrap();
            }
            CREATE_DELEGATION_TOKEN => {
                let req = decode_create_delegation_token_request(&mut frame).unwrap();
                let mut st = state.lock();
                // Any connected broker answers. Fixture ack only; not
                // a token store, not a coordinator hop, not a 41/6
                // path. Official JSON lists no error codes; official
                // handler forwards to the controller internally after
                // local validation. Official Java AdminClient uses
                // LeastLoadedNodeProvider. NOT_COORDINATOR (16) and
                // NOT_CONTROLLER (41) are not listed, so the wrong
                // node does not return 16 or 41.
                st.last_create_delegation_token_node = Some(node_id);
                let owner_type = req.owner_principal_type.clone().unwrap_or_default();
                let owner_name = req.owner_principal_name.clone().unwrap_or_default();
                st.last_create_delegation_token = Some(req);
                encode_create_delegation_token_response(
                    &mut body,
                    &CreateDelegationTokenResponse::new(
                        0,
                        owner_type,
                        owner_name,
                        String::new(),
                        String::new(),
                        0,
                        0,
                        0,
                        String::new(),
                        Vec::new(),
                    ),
                )
                .unwrap();
            }
            RENEW_DELEGATION_TOKEN => {
                let req = decode_renew_delegation_token_request(&mut frame).unwrap();
                let mut st = state.lock();
                // Any connected broker answers. Fixture ack only; not
                // a token store, not a coordinator hop, not a 41/6
                // path. Official JSON lists no error codes; official
                // handler forwards to the controller internally after
                // local validation. Official Java AdminClient uses
                // LeastLoadedNodeProvider. NOT_COORDINATOR (16) and
                // NOT_CONTROLLER (41) are not listed, so the wrong
                // node does not return 16 or 41.
                st.last_renew_delegation_token_node = Some(node_id);
                st.last_renew_delegation_token = Some(req);
                encode_renew_delegation_token_response(
                    &mut body,
                    &RenewDelegationTokenResponse::new(0, 0),
                )
                .unwrap();
            }
            EXPIRE_DELEGATION_TOKEN => {
                let req = decode_expire_delegation_token_request(&mut frame).unwrap();
                let mut st = state.lock();
                // Any connected broker answers. Fixture ack only; not
                // a token store, not a coordinator hop, not a 41/6
                // path. Official JSON lists no error codes; official
                // handler forwards to the controller internally after
                // local validation. Official Java AdminClient uses
                // LeastLoadedNodeProvider. NOT_COORDINATOR (16) and
                // NOT_CONTROLLER (41) are not listed, so the wrong
                // node does not return 16 or 41.
                st.last_expire_delegation_token_node = Some(node_id);
                st.last_expire_delegation_token = Some(req);
                encode_expire_delegation_token_response(
                    &mut body,
                    &ExpireDelegationTokenResponse::new(0, 0),
                )
                .unwrap();
            }
            DESCRIBE_DELEGATION_TOKEN => {
                let req = decode_describe_delegation_token_request(&mut frame).unwrap();
                let mut st = state.lock();
                // Any connected broker answers. Fixture ack only; not
                // a token store, not a coordinator hop, not a 41/6
                // path. Official JSON lists no error codes; official
                // handleDescribeTokensRequest answers locally (no
                // forwardToController). Official Java AdminClient uses
                // LeastLoadedNodeProvider. NOT_COORDINATOR (16) and
                // NOT_CONTROLLER (41) are not listed, so the wrong
                // node does not return 16 or 41. apiKey 41 is not
                // error code 41.
                st.last_describe_delegation_token_node = Some(node_id);
                st.last_describe_delegation_token = Some(req);
                encode_describe_delegation_token_response(
                    &mut body,
                    &DescribeDelegationTokenResponse::new(0, vec![]),
                )
                .unwrap();
            }
            _ => break,
        }
        if write_frame(&mut stream, &body).await.is_err() {
            break;
        }
    }
}

/// RFC 6749 token endpoint. Valid Basic credentials get an unsecured JWT for `principal`.
pub async fn start_oidc_token_endpoint(
    client_id: String,
    client_secret: String,
    principal: String,
) -> String {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        loop {
            let Ok((sock, _)) = listener.accept().await else {
                break;
            };
            let id = client_id.clone();
            let secret = client_secret.clone();
            let principal = principal.clone();
            tokio::spawn(async move {
                serve_oidc_token(sock, &id, &secret, &principal).await;
            });
        }
    });
    format!("http://{addr}/oauth/token")
}

pub async fn start_oidc_token_endpoint_tls(
    client_id: String,
    client_secret: String,
    principal: String,
) -> (String, partitionline::TlsConfig) {
    partitionline::net::install_crypto_provider();
    let (server, ca_pem) = tls_server_identity();
    let acceptor = tokio_rustls::TlsAcceptor::from(std::sync::Arc::new(server));
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        loop {
            let Ok((sock, _)) = listener.accept().await else {
                break;
            };
            let acceptor = acceptor.clone();
            let id = client_id.clone();
            let secret = client_secret.clone();
            let principal = principal.clone();
            tokio::spawn(async move {
                let Ok(stream) = acceptor.accept(sock).await else {
                    return;
                };
                serve_oidc_token(stream, &id, &secret, &principal).await;
            });
        }
    });
    let tls = partitionline::TlsConfig {
        ca_pem: Some(ca_pem),
        client_cert_pem: None,
        client_key_pem: None,
        server_name: Some("localhost".into()),
    };
    (
        format!("https://127.0.0.1:{}/oauth/token", addr.port()),
        tls,
    )
}

async fn serve_oidc_token<S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin>(
    mut sock: S,
    client_id: &str,
    client_secret: &str,
    principal: &str,
) {
    let mut buf = Vec::new();
    let mut tmp = [0u8; 2048];
    loop {
        let n = match sock.read(&mut tmp).await {
            Ok(0) => break,
            Ok(n) => n,
            Err(_) => return,
        };
        buf.extend_from_slice(tmp.get(..n).unwrap_or(&[]));
        if buf.windows(4).any(|w| w == b"\r\n\r\n") || buf.len() > 16 * 1024 {
            break;
        }
    }
    let req = String::from_utf8_lossy(&buf);
    let expected = {
        let raw = format!("{client_id}:{client_secret}");
        base64::Engine::encode(&base64::engine::general_purpose::STANDARD, raw.as_bytes())
    };
    let auth_ok = req.lines().any(|l| {
        let line = l.trim_end_matches('\r');
        let Some((k, v)) = line.split_once(':') else {
            return false;
        };
        k.eq_ignore_ascii_case("authorization") && v.trim() == format!("Basic {expected}")
    });
    let ok = auth_ok && req.contains("grant_type=client_credentials");
    let (status, body) = if ok {
        let token = oauth::unsecured_jwt_now(principal);
        (
            "200 OK",
            format!("{{\"access_token\":\"{token}\",\"token_type\":\"Bearer\"}}"),
        )
    } else {
        ("401 Unauthorized", "{\"error\":\"invalid_client\"}".into())
    };
    let resp = format!(
        "HTTP/1.1 {status}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
        body.len()
    );
    let _ = sock.write_all(resp.as_bytes()).await;
}

//! Mock-broker coverage of produce, fetch, SASL, admin, and compression.
#![expect(
    unused_results,
    reason = "tests often discard RecordMetadata and admin delete results"
)]
#![expect(
    clippy::let_underscore_must_use,
    reason = "tests discard admin delete results"
)]

mod common;

use partitionline::protocol::api_keys::{
    ALTER_CLIENT_QUOTAS, ALTER_CONFIGS, ALTER_PARTITION_REASSIGNMENTS, ALTER_REPLICA_LOG_DIRS,
    ALTER_USER_SCRAM_CREDENTIALS, API_VERSIONS, CONSUMER_GROUP_DESCRIBE, CONSUMER_GROUP_HEARTBEAT,
    CREATE_ACLS, CREATE_DELEGATION_TOKEN, CREATE_PARTITIONS, CREATE_TOPICS, DELETE_ACLS,
    DELETE_GROUPS, DELETE_RECORDS, DELETE_TOPICS, DESCRIBE_ACLS, DESCRIBE_CLIENT_QUOTAS,
    DESCRIBE_CLUSTER, DESCRIBE_CONFIGS, DESCRIBE_DELEGATION_TOKEN, DESCRIBE_GROUPS,
    DESCRIBE_LOG_DIRS, DESCRIBE_PRODUCERS, DESCRIBE_TOPIC_PARTITIONS, DESCRIBE_TRANSACTIONS,
    DESCRIBE_USER_SCRAM_CREDENTIALS, END_TXN, EXPIRE_DELEGATION_TOKEN, FIND_COORDINATOR, HEARTBEAT,
    INCREMENTAL_ALTER_CONFIGS, JOIN_GROUP, LEAVE_GROUP, LIST_CONFIG_RESOURCES, LIST_GROUPS,
    LIST_PARTITION_REASSIGNMENTS, LIST_TRANSACTIONS, METADATA, OFFSET_COMMIT, OFFSET_DELETE,
    OFFSET_FETCH, OFFSET_FOR_LEADER_EPOCH, RENEW_DELEGATION_TOKEN, SASL_AUTHENTICATE,
    SASL_HANDSHAKE, SHARE_ACKNOWLEDGE, SHARE_FETCH, SHARE_GROUP_DESCRIBE, SHARE_GROUP_HEARTBEAT,
    SYNC_GROUP, UNREGISTER_BROKER, UPDATE_FEATURES,
};
use partitionline::protocol::group::{COORDINATOR_GROUP, COORDINATOR_TRANSACTION};
use partitionline::{
    error, AbortTransactionSpec, AclBinding, AclBindingFilter, AclResourceType, Admin, AdminConfig,
    AlterConfig, AlterConfigOpType, AlterReplicaLogDirsDirectory, AlterReplicaLogDirsRequest,
    AlterReplicaLogDirsTopic, AlterShareGroupOffsetsTopic, AssignReplicasToDirsDirectory,
    AssignReplicasToDirsPartition, AssignReplicasToDirsRequest, AssignReplicasToDirsTopic,
    ClientQuotaAlteration, ClientQuotaEntity, ClientQuotaFilter, ClientQuotaFilterComponent,
    ClientQuotaOp, Compression, Config, ConfigEntry, ConfigReplacement, ConfigResource,
    ConfigResourceType, ConfigResourceUpdate, ConfigSource, ConfigType, Consumer, ConsumerConfig,
    ConsumerGroup, CreatableRenewer, CreateDelegationTokenRequest, DeleteShareGroupOffsetsTopic,
    DeletedRecords, DescribableLogDirTopic, DescribeClusterBroker, DescribeDelegationTokenOwner,
    DescribeDelegationTokenRequest, DescribeLogDirsRequest, DescribeShareGroupOffsetsGroup,
    EndpointType, Error, ExpireDelegationTokenRequest, FeatureUpdate, GroupState, GroupType,
    IsolationLevel, ListConsumerGroupOffsetsSpec, NewPartitionReassignment, NewPartitions,
    NewTopic, Node, OffsetAndMetadata, OffsetSpec, OidcConfig, OngoingReassignment,
    PartitionReassignment, ProduceRecord, Producer, ProducerConfig, RecordsToDelete,
    RenewDelegationTokenRequest, ReplicaLogDirInfo, ScramMechanism, ShareGroup, TimestampType,
    TopicCollection, TopicPartition, TopicPartitionReplica, TransactionState, TransactionTopic,
    UpgradeType, UserScramCredentialAlteration, UserScramCredentialDeletion,
    UserScramCredentialUpsertion, Uuid, AUTHORIZED_OPERATIONS_OMITTED,
    CONFIG_RESOURCE_CLIENT_METRICS, DEFAULT_LEAVE_GROUP_REASON, EARLIEST_TIMESTAMP,
    LATEST_TIMESTAMP, SCRAM_SHA_256, SCRAM_SHA_512,
};
use std::time::{Duration, Instant};

#[tokio::test]
async fn try_send_flush_writes_record() {
    let mock = common::Mock::start().await;
    let mut pcfg = ProducerConfig::bootstrap([mock.addr.clone()]);
    pcfg.linger = Duration::ZERO;
    let producer = Producer::new(pcfg).await.unwrap();
    assert_eq!(
        mock.last_api_versions_version(),
        Some(4),
        "Producer must prefer ApiVersions v4 when the broker advertises it"
    );
    let rec = ProduceRecord::to("t").value(&b"try-send"[..]);
    let deadline = std::time::Instant::now() + Duration::from_secs(2);
    loop {
        match producer.try_send(rec.clone()) {
            Ok(()) => break,
            Err(Error::QueueFull) => {
                assert!(
                    std::time::Instant::now() < deadline,
                    "try_send never left QueueFull"
                );
                tokio::task::yield_now().await;
            }
            Err(e) => panic!("try_send: {e}"),
        }
    }
    producer.flush().await.unwrap();
    assert_eq!(
        mock.last_produce_version(),
        Some(12),
        "Producer must prefer Produce v12 when the broker advertises it"
    );
    producer.close().await.unwrap();
    let mut ccfg = ConsumerConfig::bootstrap([mock.addr.clone()]);
    ccfg.max_wait_ms = 10;
    let mut consumer = Consumer::new(ccfg).await.unwrap();
    consumer.assign("t", 0, 0).await.unwrap();
    let recs = consumer.fetch().await.unwrap();
    assert_eq!(recs[0].value.as_deref(), Some(&b"try-send"[..]));
    assert_eq!(recs[0].timestamp_type(), TimestampType::CreateTime);
    assert_eq!(recs[0].timestamp_type, TimestampType::CreateTime);
}

#[tokio::test]
async fn api_versions_retries_v3_on_unsupported_version() {
    let mock = common::Mock::start().await;
    mock.set_api_max(API_VERSIONS, 3);
    let mut pcfg = ProducerConfig::bootstrap([mock.addr.clone()]);
    pcfg.linger = Duration::ZERO;
    let producer = Producer::new(pcfg).await.unwrap();
    assert_eq!(
        mock.api_versions_versions(),
        vec![4, 3],
        "KIP-511 retries ApiVersions at the advertised max"
    );
    assert_eq!(mock.last_api_versions_version(), Some(3));
    producer.close().await.unwrap();
}

#[tokio::test]
async fn api_versions_retries_v0_on_unsupported_version() {
    let mock = common::Mock::start().await;
    mock.set_api_max(API_VERSIONS, 0);
    let mut pcfg = ProducerConfig::bootstrap([mock.addr.clone()]);
    pcfg.linger = Duration::ZERO;
    let producer = Producer::new(pcfg).await.unwrap();
    assert_eq!(
        mock.api_versions_versions(),
        vec![4, 0],
        "KIP-511 falls back to v0 when the advertised max is 0"
    );
    assert_eq!(mock.last_api_versions_version(), Some(0));
    producer.close().await.unwrap();
}

#[tokio::test]
async fn metadata_negotiates_v13_when_advertised() {
    let mock = common::Mock::start().await;
    let mut pcfg = ProducerConfig::bootstrap([mock.addr.clone()]);
    pcfg.linger = Duration::ZERO;
    let producer = Producer::new(pcfg).await.unwrap();
    let md = producer
        .send(ProduceRecord::to("t").value(&b"md13"[..]))
        .await
        .unwrap();
    assert_eq!(md.serialized_key_size(), -1);
    assert_eq!(md.serialized_value_size(), 4);
    assert!(md.has_timestamp());
    assert_eq!(md.to_string(), format!("t-0@{}", md.offset()));
    assert_eq!(
        mock.last_metadata_version(),
        Some(13),
        "Producer must prefer Metadata v13 when the broker advertises it"
    );
    producer.close().await.unwrap();

    let mock = common::Mock::start().await;
    mock.set_api_max(METADATA, 12);
    let mut pcfg = ProducerConfig::bootstrap([mock.addr.clone()]);
    pcfg.linger = Duration::ZERO;
    let producer = Producer::new(pcfg).await.unwrap();
    producer
        .send(ProduceRecord::to("t").value(&b"md12"[..]))
        .await
        .unwrap();
    assert_eq!(
        mock.last_metadata_version(),
        Some(12),
        "client must speak Metadata v12 when the broker max is 12"
    );
    producer.close().await.unwrap();
}

#[tokio::test]
async fn fetch_uses_base_offset_plus_record_delta() {
    let mock = common::Mock::start().await;
    let mut pcfg = ProducerConfig::bootstrap([mock.addr.clone()]);
    pcfg.linger = Duration::ZERO;
    let producer = Producer::new(pcfg).await.unwrap();
    for value in [b"a" as &[u8], b"b", b"c"] {
        producer
            .send(ProduceRecord::to("t").value(value))
            .await
            .unwrap();
    }
    producer.close().await.unwrap();

    let mut ccfg = ConsumerConfig::bootstrap([mock.addr.clone()]);
    ccfg.max_wait_ms = 10;
    let mut consumer = Consumer::new(ccfg).await.unwrap();
    consumer.assign("t", 0, 0).await.unwrap();
    let recs = consumer.fetch().await.unwrap();
    let offsets: Vec<i64> = recs.iter().map(|r| r.offset).collect();
    assert_eq!(offsets, vec![0, 1, 2]);
    assert_eq!(
        recs.iter()
            .map(|r| r.value.as_deref().unwrap())
            .collect::<Vec<_>>(),
        [b"a" as &[u8], b"b", b"c"]
    );
    assert_eq!(consumer.positions(), vec![(TopicPartition::new("t", 0), 3)]);

    consumer.seek("t", 0, 1).unwrap();
    let recs = consumer.fetch().await.unwrap();
    assert_eq!(
        recs.iter().map(|r| r.offset).collect::<Vec<_>>(),
        vec![1, 2]
    );
    assert_eq!(consumer.positions(), vec![(TopicPartition::new("t", 0), 3)]);
}

#[expect(
    clippy::panic,
    reason = "test helper surfaces try_send errors like the in-test loops"
)]
async fn try_send_n(producer: &Producer, n: usize, value: &'static [u8]) {
    let mut queued = 0usize;
    let deadline = std::time::Instant::now() + Duration::from_secs(2);
    while queued < n {
        match producer.try_send(ProduceRecord::to("t").value(value)) {
            Ok(()) => queued += 1,
            Err(Error::QueueFull) => {
                assert!(
                    std::time::Instant::now() < deadline,
                    "try_send never left QueueFull"
                );
                tokio::task::yield_now().await;
            }
            Err(e) => panic!("try_send: {e}"),
        }
    }
}

#[tokio::test]
async fn try_send_follows_moved_leader() {
    let mock = common::Mock::start_two_node().await;
    let mut pcfg = ProducerConfig::bootstrap([mock.addr.clone()]);
    pcfg.linger = Duration::ZERO;
    let producer = Producer::new(pcfg).await.unwrap();

    const FIRST: usize = 8;
    const SECOND: usize = 8;
    const THIRD: usize = 8;

    try_send_n(&producer, FIRST, b"a").await;
    producer.flush().await.unwrap();
    assert_eq!(mock.log_len("t", 0), FIRST);
    let first_nodes = mock.produce_nodes();
    assert!(
        !first_nodes.is_empty() && first_nodes.iter().all(|&n| n == 2),
        "cache populate must produce to leader node 2, got {first_nodes:?}"
    );

    mock.set_partition_leader("t", 0, 1);

    try_send_n(&producer, SECOND, b"b").await;
    producer.flush().await.unwrap();
    assert_eq!(mock.log_len("t", 0), FIRST + SECOND);
    let after_move = mock.produce_nodes();
    assert!(
        after_move.contains(&1),
        "retry after leader move must land on node 1, got {after_move:?}"
    );

    let reqs_after_move = mock.produce_request_nodes().len();
    try_send_n(&producer, THIRD, b"c").await;
    producer.flush().await.unwrap();
    assert_eq!(mock.log_len("t", 0), FIRST + SECOND + THIRD);
    let later: Vec<i32> = mock
        .produce_request_nodes()
        .into_iter()
        .skip(reqs_after_move)
        .collect();
    assert!(
        !later.is_empty() && later.iter().all(|&n| n == 1),
        "try_send after FastRoute rebuild must hit new leader only, got {later:?}"
    );
    producer.close().await.unwrap();
}

#[tokio::test]
async fn produce_follows_node_endpoints_without_metadata() {
    let mock = common::Mock::start_two_node().await;
    mock.hide_broker_from_metadata(1);
    let mut pcfg = ProducerConfig::bootstrap([mock.addr.clone()]);
    pcfg.linger = Duration::ZERO;
    let producer = Producer::new(pcfg).await.unwrap();

    producer
        .send(ProduceRecord::to("t").value(&b"a"[..]))
        .await
        .unwrap();
    assert!(
        mock.produce_nodes().contains(&2),
        "first produce must land on Metadata leader node 2, got {:?}",
        mock.produce_nodes()
    );
    let after_first = mock.metadata_calls();

    mock.set_partition_leader("t", 0, 1);
    producer
        .send(ProduceRecord::to("t").value(&b"b"[..]))
        .await
        .unwrap();
    assert!(
        mock.produce_nodes().contains(&1),
        "NodeEndpoints must route produce to hidden broker 1, got {:?}",
        mock.produce_nodes()
    );
    assert_eq!(
        mock.metadata_calls(),
        after_first,
        "unknown CurrentLeader plus NodeEndpoints must skip Metadata"
    );
    producer.close().await.unwrap();
}

#[tokio::test]
async fn fetch_follows_node_endpoints_without_metadata() {
    let mock = common::Mock::start_two_node().await;
    mock.hide_broker_from_metadata(1);
    let mut pcfg = ProducerConfig::bootstrap([mock.addr.clone()]);
    pcfg.linger = Duration::ZERO;
    let producer = Producer::new(pcfg).await.unwrap();
    producer
        .send(ProduceRecord::to("t").value(&b"ne"[..]))
        .await
        .unwrap();
    producer.close().await.unwrap();

    let mut ccfg = ConsumerConfig::bootstrap([mock.addr.clone()]);
    ccfg.max_wait_ms = 10;
    let mut consumer = Consumer::new(ccfg).await.unwrap();
    consumer.assign("t", 0, 0).await.unwrap();
    let recs = consumer.fetch().await.unwrap();
    assert_eq!(recs[0].value.as_deref(), Some(&b"ne"[..]));
    assert!(
        mock.fetch_nodes().contains(&2),
        "first fetch must hit Metadata leader node 2, got {:?}",
        mock.fetch_nodes()
    );
    let after_first = mock.metadata_calls();

    mock.set_partition_leader("t", 0, 1);
    let _ = consumer.fetch().await.unwrap();
    assert!(
        mock.fetch_nodes().contains(&1),
        "NodeEndpoints must route fetch to hidden broker 1, got {:?}",
        mock.fetch_nodes()
    );
    assert_eq!(
        mock.metadata_calls(),
        after_first,
        "unknown CurrentLeader plus NodeEndpoints must skip Metadata"
    );
}

#[tokio::test]
async fn idempotent_produce_gets_pid_and_offset() {
    let mock = common::Mock::start().await;
    let mut pcfg = ProducerConfig::bootstrap([mock.addr.clone()]);
    pcfg.linger = Duration::ZERO;
    pcfg.enable_idempotence = true;
    let producer = Producer::new(pcfg).await.unwrap();
    let md = producer
        .send(ProduceRecord::to("t").value(&b"idem-hello"[..]))
        .await
        .unwrap();
    assert_eq!(md.offset, 0);
    producer.close().await.unwrap();
    let pid = mock.last_producer_id().expect("mock saw a produce batch");
    assert!(
        pid >= 0,
        "idempotent produce must set producer_id, got {pid}"
    );
    assert_ne!(pid, -1);
    assert_eq!(
        mock.last_init_producer_id_version(),
        Some(5),
        "Producer must prefer InitProducerId v5 when the broker advertises it"
    );
    assert_eq!(
        mock.last_init_producer_id_producer_id(),
        Some(-1),
        "first InitProducerId must send ProducerId -1"
    );
    assert_eq!(
        mock.last_init_producer_id_producer_epoch(),
        Some(-1),
        "first InitProducerId must send ProducerEpoch -1"
    );
}

#[tokio::test]
async fn idempotent_unknown_producer_id_bumps_epoch_and_retries() {
    let mock = common::Mock::start().await;
    mock.set_produce_error_times(error::UNKNOWN_PRODUCER_ID, 1);
    let mut pcfg = ProducerConfig::bootstrap([mock.addr.clone()]);
    pcfg.linger = Duration::ZERO;
    pcfg.enable_idempotence = true;
    let producer = Producer::new(pcfg).await.unwrap();
    let md = producer
        .send(ProduceRecord::to("t").value(&b"unk-pid"[..]))
        .await
        .unwrap();
    assert_eq!(md.offset, 0);
    assert_eq!(
        mock.last_produce_producer_epoch(),
        Some(1),
        "idempotent UNKNOWN_PRODUCER_ID must bump epoch locally and retry"
    );
    producer.close().await.unwrap();
}

#[tokio::test]
async fn transactional_abort_reinit_sends_last_pid_epoch() {
    let mock = common::Mock::start().await;
    mock.set_api_max(END_TXN, 4);
    let mut pcfg = ProducerConfig::bootstrap([mock.addr.clone()]);
    pcfg.linger = Duration::ZERO;
    pcfg.transactional_id = Some("tx-bump".into());
    let producer = Producer::new(pcfg).await.unwrap();
    assert_eq!(mock.last_init_producer_id_producer_id(), Some(-1));
    assert_eq!(mock.last_init_producer_id_producer_epoch(), Some(-1));
    producer.begin_transaction().await.unwrap();
    mock.set_produce_error_times(error::UNKNOWN_PRODUCER_ID, 1);
    let err = producer
        .send(ProduceRecord::to("t").value(&b"fenced"[..]))
        .await
        .unwrap_err();
    assert_eq!(err.broker_code(), Some(error::UNKNOWN_PRODUCER_ID));
    producer.abort_transaction().await.unwrap();
    assert_eq!(
        mock.last_init_producer_id_producer_id(),
        Some(1000),
        "KIP-360 resume must send the last producer id, not -1"
    );
    assert_eq!(
        mock.last_init_producer_id_producer_epoch(),
        Some(0),
        "KIP-360 resume must send the last producer epoch"
    );
    assert_eq!(
        mock.last_end_txn_version(),
        Some(4),
        "test fixture advertises EndTxn max 4 so InitProducerId performs the bump"
    );
    producer.begin_transaction().await.unwrap();
    let md = producer
        .send(ProduceRecord::to("t").value(&b"after-bump"[..]))
        .await
        .unwrap();
    assert_eq!(md.offset, 0);
    assert_eq!(
        mock.last_produce_producer_epoch(),
        Some(1),
        "InitProducerId epoch bump must apply on the next Produce"
    );
    producer.abort_transaction().await.unwrap();
    producer.close().await.unwrap();
}

#[tokio::test]
async fn transactional_commit_after_unknown_pid_still_fails() {
    let mock = common::Mock::start().await;
    let mut pcfg = ProducerConfig::bootstrap([mock.addr.clone()]);
    pcfg.linger = Duration::ZERO;
    pcfg.transactional_id = Some("tx-commit-fail".into());
    let producer = Producer::new(pcfg).await.unwrap();
    producer.begin_transaction().await.unwrap();
    mock.set_produce_error_times(error::UNKNOWN_PRODUCER_ID, 1);
    let err = producer
        .send(ProduceRecord::to("t").value(&b"fenced"[..]))
        .await
        .unwrap_err();
    assert_eq!(err.broker_code(), Some(error::UNKNOWN_PRODUCER_ID));
    let commit_err = producer.commit_transaction().await.unwrap_err();
    assert_eq!(
        commit_err.broker_code(),
        Some(error::UNKNOWN_PRODUCER_ID),
        "commit must still fail flush after a failed Produce; only abort ignores it"
    );
    producer.abort_transaction().await.unwrap();
    producer.close().await.unwrap();
}

#[tokio::test]
async fn idempotent_unkeyed_multi_conn_stays_in_order() {
    let mock = common::Mock::start().await;
    let mut pcfg = ProducerConfig::bootstrap([mock.addr.clone()]);
    pcfg.linger = Duration::ZERO;
    pcfg.enable_idempotence = true;
    pcfg.connections = 8;
    let producer = Producer::new(pcfg).await.unwrap();
    const N: usize = 1024;
    let mut queued = 0usize;
    while queued < N {
        match producer.try_send(ProduceRecord::to("t").value(&b"seq"[..])) {
            Ok(()) => queued += 1,
            Err(Error::QueueFull) => tokio::task::yield_now().await,
            Err(e) => panic!("try_send: {e}"),
        }
    }
    producer.flush().await.unwrap();
    assert_eq!(
        mock.log_len("t", 0),
        N,
        "broker must append every record (error 45 means sequences arrived out of order)"
    );
    producer.close().await.unwrap();
}

#[tokio::test]
async fn produce_fetch_follow_metadata_leader() {
    let mock = common::Mock::start_two_node().await;
    let mut pcfg = ProducerConfig::bootstrap([mock.addr.clone()]);
    pcfg.linger = Duration::ZERO;
    let producer = Producer::new(pcfg).await.unwrap();
    let md = producer
        .send(ProduceRecord::to("t").value(&b"leader"[..]))
        .await
        .unwrap();
    assert_eq!(md.partition, 0);
    assert_eq!(md.offset, 0);
    producer.close().await.unwrap();
    let produced = mock.produce_nodes();
    assert!(
        produced.contains(&2),
        "successful produce must land on leader node 2, got {produced:?}"
    );
    assert_eq!(mock.log_len("t", 0), 1);

    let mut ccfg = ConsumerConfig::bootstrap([mock.addr.clone()]);
    ccfg.max_wait_ms = 10;
    let mut consumer = Consumer::new(ccfg).await.unwrap();
    consumer.assign("t", 0, 0).await.unwrap();
    let recs = consumer.fetch().await.unwrap();
    assert_eq!(recs[0].value.as_deref(), Some(&b"leader"[..]));
    let fetched = mock.fetch_nodes();
    assert!(
        fetched.contains(&2),
        "successful fetch must hit leader node 2, got {fetched:?}"
    );
}

#[tokio::test]
async fn fetch_from_follower_when_rack_matches() {
    let mock = common::Mock::start_two_node().await;
    let mut pcfg = ProducerConfig::bootstrap([mock.addr.clone()]);
    pcfg.linger = Duration::ZERO;
    let producer = Producer::new(pcfg).await.unwrap();
    let md = producer
        .send(ProduceRecord::to("t").value(&b"rack"[..]))
        .await
        .unwrap();
    producer.close().await.unwrap();
    let mut ccfg = ConsumerConfig::bootstrap([mock.addr.clone()]);
    ccfg.max_wait_ms = 10;
    ccfg.rack = Some("r1".into());
    let mut consumer = Consumer::new(ccfg).await.unwrap();
    consumer.assign("t", md.partition, md.offset).await.unwrap();
    let recs = consumer.fetch().await.unwrap();
    assert_eq!(recs[0].value.as_deref(), Some(&b"rack"[..]));
    let fetched = mock.fetch_nodes();
    assert!(
        fetched.contains(&1),
        "racked consumer must fetch follower node 1, got {fetched:?}"
    );
    assert_eq!(mock.last_fetch_rack(), "r1");
}

#[tokio::test]
async fn offset_for_leader_epoch_follows_partition_leader() {
    let mock = common::Mock::start_two_node().await;
    let mut pcfg = ProducerConfig::bootstrap([mock.addr.clone()]);
    pcfg.linger = Duration::ZERO;
    let producer = Producer::new(pcfg).await.unwrap();
    let md = producer
        .send(ProduceRecord::to("t").value(&b"ofle-lead"[..]))
        .await
        .unwrap();

    let mut ccfg = ConsumerConfig::bootstrap([mock.addr.clone()]);
    ccfg.max_wait_ms = 10;
    ccfg.rack = Some("r1".into());
    let mut consumer = Consumer::new(ccfg).await.unwrap();
    consumer.assign("t", md.partition, md.offset).await.unwrap();
    let recs = consumer.fetch().await.unwrap();
    assert_eq!(recs[0].value.as_deref(), Some(&b"ofle-lead"[..]));
    let fetched = mock.fetch_nodes();
    assert!(
        fetched.contains(&1),
        "racked consumer must fetch follower node 1 before the epoch bump, got {fetched:?}"
    );
    assert!(
        mock.last_offset_for_leader_epoch().is_none(),
        "unfenced follower fetch must not speak OffsetForLeaderEpoch"
    );

    producer
        .send(ProduceRecord::to("t").value(&b"ofle-lead-2"[..]))
        .await
        .unwrap();
    producer.close().await.unwrap();
    let bumped = mock.bump_leader_epoch("t", md.partition);
    assert!(bumped > 0);
    let recs = consumer.fetch().await.unwrap();
    assert_eq!(recs[0].value.as_deref(), Some(&b"ofle-lead-2"[..]));
    let ofle = mock
        .last_offset_for_leader_epoch()
        .expect("fenced follower fetch must speak OffsetForLeaderEpoch");
    assert_eq!(ofle.0, "t");
    assert_eq!(ofle.1, md.partition);
    assert_eq!(ofle.2, bumped);
    assert_eq!(
        mock.last_offset_for_leader_epoch_node(),
        Some(2),
        "OffsetForLeaderEpoch must land on the partition leader, not the fenced follower"
    );
    assert_eq!(
        mock.offset_for_leader_epoch_not_leader(),
        0,
        "Metadata refresh must send OffsetForLeaderEpoch to the leader without a follower 6"
    );
    assert_eq!(
        mock.last_offset_for_leader_epoch_version(),
        Some(4),
        "Consumer must prefer OffsetForLeaderEpoch v4 when the broker advertises it"
    );
}

#[tokio::test]
async fn fetch_without_rack_stays_on_leader() {
    let mock = common::Mock::start_two_node().await;
    let mut pcfg = ProducerConfig::bootstrap([mock.addr.clone()]);
    pcfg.linger = Duration::ZERO;
    let producer = Producer::new(pcfg).await.unwrap();
    let md = producer
        .send(ProduceRecord::to("t").value(&b"lead"[..]))
        .await
        .unwrap();
    producer.close().await.unwrap();
    let mut ccfg = ConsumerConfig::bootstrap([mock.addr.clone()]);
    ccfg.max_wait_ms = 10;
    let mut consumer = Consumer::new(ccfg).await.unwrap();
    consumer.assign("t", md.partition, md.offset).await.unwrap();
    let recs = consumer.fetch().await.unwrap();
    assert_eq!(recs[0].value.as_deref(), Some(&b"lead"[..]));
    let fetched = mock.fetch_nodes();
    assert!(
        fetched.contains(&2),
        "no-rack fetch must hit leader node 2, got {fetched:?}"
    );
    assert!(
        !fetched.contains(&1),
        "no-rack fetch must not hit follower, got {fetched:?}"
    );
    assert!(mock.last_fetch_rack().is_empty());
}

#[tokio::test]
async fn fetch_recovers_from_fenced_leader_epoch() {
    let mock = common::Mock::start().await;
    let mut pcfg = ProducerConfig::bootstrap([mock.addr.clone()]);
    pcfg.linger = Duration::ZERO;
    let producer = Producer::new(pcfg).await.unwrap();
    let md = producer
        .send(ProduceRecord::to("t").value(&b"epoch"[..]))
        .await
        .unwrap();
    producer.close().await.unwrap();
    let mut ccfg = ConsumerConfig::bootstrap([mock.addr.clone()]);
    ccfg.max_wait_ms = 10;
    let mut consumer = Consumer::new(ccfg).await.unwrap();
    consumer.assign("t", md.partition, md.offset).await.unwrap();
    let bumped = mock.bump_leader_epoch("t", md.partition);
    assert!(bumped > 0);
    let recs = consumer.fetch().await.unwrap();
    assert_eq!(recs[0].value.as_deref(), Some(&b"epoch"[..]));
    let ofle = mock
        .last_offset_for_leader_epoch()
        .expect("fenced fetch must speak OffsetForLeaderEpoch");
    assert_eq!(ofle.0, "t");
    assert_eq!(ofle.1, md.partition);
}

#[tokio::test]
async fn offset_for_leader_epoch_batches_fenced_partitions() {
    let mock = common::Mock::start().await;
    let mut admin = Admin::connect(mock.addr.clone()).await.unwrap();
    let created = admin
        .create_topics(
            &[NewTopic::new("ofle-a", 1, 1), NewTopic::new("ofle-b", 2, 1)],
            10_000,
            false,
        )
        .await
        .unwrap();
    assert_eq!(created[0].error_code, 0);
    assert_eq!(created[1].error_code, 0);

    let mut pcfg = ProducerConfig::bootstrap([mock.addr.clone()]);
    pcfg.linger = Duration::ZERO;
    let producer = Producer::new(pcfg).await.unwrap();
    producer
        .send(ProduceRecord::to("ofle-a").partition(0).value(&b"a0"[..]))
        .await
        .unwrap();
    producer
        .send(ProduceRecord::to("ofle-b").partition(0).value(&b"b0"[..]))
        .await
        .unwrap();
    producer
        .send(ProduceRecord::to("ofle-b").partition(1).value(&b"b1"[..]))
        .await
        .unwrap();
    producer.close().await.unwrap();

    let mut ccfg = ConsumerConfig::bootstrap([mock.addr.clone()]);
    ccfg.max_wait_ms = 10;
    let mut consumer = Consumer::new(ccfg).await.unwrap();
    consumer.assign("ofle-a", 0, 0).await.unwrap();
    consumer.assign("ofle-b", 0, 0).await.unwrap();
    consumer.assign("ofle-b", 1, 0).await.unwrap();
    let _ = mock.bump_leader_epoch("ofle-a", 0);
    let _ = mock.bump_leader_epoch("ofle-b", 0);
    let _ = mock.bump_leader_epoch("ofle-b", 1);
    let recs = consumer.fetch().await.unwrap();
    assert_eq!(recs.len(), 3);
    assert_eq!(
        mock.offset_for_leader_epoch_calls(),
        1,
        "one OffsetForLeaderEpoch RPC per leader, not one per fenced partition"
    );
    assert_eq!(
        mock.last_offset_for_leader_epoch_n(),
        Some(3),
        "fenced Fetch partitions must recover with Topics/Partitions of N"
    );
}

#[tokio::test]
async fn offset_for_leader_epoch_negotiates_v3_when_broker_caps() {
    let mock = common::Mock::start().await;
    mock.set_api_max(OFFSET_FOR_LEADER_EPOCH, 3);
    let mut pcfg = ProducerConfig::bootstrap([mock.addr.clone()]);
    pcfg.linger = Duration::ZERO;
    let producer = Producer::new(pcfg).await.unwrap();
    let md = producer
        .send(ProduceRecord::to("t").value(&b"ofle3"[..]))
        .await
        .unwrap();
    producer.close().await.unwrap();
    let mut ccfg = ConsumerConfig::bootstrap([mock.addr.clone()]);
    ccfg.max_wait_ms = 10;
    let mut consumer = Consumer::new(ccfg).await.unwrap();
    consumer.assign("t", md.partition, md.offset).await.unwrap();
    let _ = mock.bump_leader_epoch("t", md.partition);
    let recs = consumer.fetch().await.unwrap();
    assert_eq!(recs[0].value.as_deref(), Some(&b"ofle3"[..]));
    assert_eq!(
        mock.last_offset_for_leader_epoch_version(),
        Some(3),
        "client must speak OffsetForLeaderEpoch v3 when the broker max is 3"
    );
}

#[tokio::test]
async fn offset_for_leader_epoch_negotiates_v2_when_broker_caps() {
    let mock = common::Mock::start().await;
    mock.set_api_max(OFFSET_FOR_LEADER_EPOCH, 2);
    let mut pcfg = ProducerConfig::bootstrap([mock.addr.clone()]);
    pcfg.linger = Duration::ZERO;
    let producer = Producer::new(pcfg).await.unwrap();
    let md = producer
        .send(ProduceRecord::to("t").value(&b"ofle2"[..]))
        .await
        .unwrap();
    producer.close().await.unwrap();
    let mut ccfg = ConsumerConfig::bootstrap([mock.addr.clone()]);
    ccfg.max_wait_ms = 10;
    let mut consumer = Consumer::new(ccfg).await.unwrap();
    consumer.assign("t", md.partition, md.offset).await.unwrap();
    let _ = mock.bump_leader_epoch("t", md.partition);
    let recs = consumer.fetch().await.unwrap();
    assert_eq!(recs[0].value.as_deref(), Some(&b"ofle2"[..]));
    assert_eq!(
        mock.last_offset_for_leader_epoch_version(),
        Some(2),
        "client must speak OffsetForLeaderEpoch v2 when the broker max is 2"
    );
}

#[tokio::test]
async fn fetch_unfenced_does_not_speak_offset_for_leader_epoch() {
    let mock = common::Mock::start().await;
    let mut pcfg = ProducerConfig::bootstrap([mock.addr.clone()]);
    pcfg.linger = Duration::ZERO;
    let producer = Producer::new(pcfg).await.unwrap();
    let md = producer
        .send(ProduceRecord::to("t").value(&b"plain"[..]))
        .await
        .unwrap();
    producer.close().await.unwrap();
    let mut ccfg = ConsumerConfig::bootstrap([mock.addr.clone()]);
    ccfg.max_wait_ms = 10;
    let mut consumer = Consumer::new(ccfg).await.unwrap();
    consumer.assign("t", md.partition, md.offset).await.unwrap();
    let recs = consumer.fetch().await.unwrap();
    assert_eq!(recs[0].value.as_deref(), Some(&b"plain"[..]));
    assert!(
        mock.last_offset_for_leader_epoch().is_none(),
        "unfenced fetch must not send OffsetForLeaderEpoch"
    );
}

#[tokio::test]
async fn produce_retries_retriable_then_succeeds() {
    let mock = common::Mock::start().await;
    mock.set_produce_error_times(error::LEADER_NOT_AVAILABLE, 1);
    let mut pcfg = ProducerConfig::bootstrap([mock.addr.clone()]);
    pcfg.linger = Duration::ZERO;
    let producer = Producer::new(pcfg).await.unwrap();
    let md = producer
        .send(ProduceRecord::to("t").value(&b"retry"[..]))
        .await
        .unwrap();
    assert_eq!(md.offset, 0);
    producer.flush().await.unwrap();
    assert_eq!(mock.log_len("t", 0), 1);
    producer.close().await.unwrap();
}

#[tokio::test]
async fn produce_retry_honors_backoff() {
    let mock = common::Mock::start().await;
    mock.set_produce_error_times(error::LEADER_NOT_AVAILABLE, 1);
    let mut pcfg = ProducerConfig::bootstrap([mock.addr.clone()]);
    pcfg.linger = Duration::ZERO;
    pcfg.retry_backoff = Duration::from_millis(50);
    pcfg.retry_backoff_max = Duration::from_millis(50);
    let producer = Producer::new(pcfg).await.unwrap();
    let start = Instant::now();
    let md = producer
        .send(ProduceRecord::to("t").value(&b"retry-backoff"[..]))
        .await
        .unwrap();
    assert_eq!(md.offset, 0);
    assert!(
        start.elapsed() >= Duration::from_millis(50),
        "retriable Produce must wait retry.backoff.ms, elapsed {:?}",
        start.elapsed()
    );
    producer.close().await.unwrap();
}

#[tokio::test]
async fn produce_reconnect_honors_backoff() {
    let mock = common::Mock::start().await;
    let mut pcfg = ProducerConfig::bootstrap([mock.addr.clone()]);
    pcfg.linger = Duration::ZERO;
    pcfg.connections = 1;
    pcfg.reconnect_backoff = Duration::from_millis(50);
    pcfg.reconnect_backoff_max = Duration::from_millis(50);
    let producer = Producer::new(pcfg).await.unwrap();
    mock.refuse_connections(1);
    let start = Instant::now();
    let md = producer
        .send(ProduceRecord::to("t").value(&b"reconnect-backoff"[..]))
        .await
        .unwrap();
    assert_eq!(md.offset, 0);
    assert!(
        start.elapsed() >= Duration::from_millis(50),
        "failed broker connect must wait reconnect.backoff.ms, elapsed {:?}",
        start.elapsed()
    );
    producer.close().await.unwrap();
}

#[tokio::test]
async fn fetch_reconnect_honors_backoff() {
    let mock = common::Mock::start().await;
    let mut pcfg = ProducerConfig::bootstrap([mock.addr.clone()]);
    pcfg.linger = Duration::ZERO;
    let producer = Producer::new(pcfg).await.unwrap();
    producer
        .send(ProduceRecord::to("t").value(&b"fetch-reconnect"[..]))
        .await
        .unwrap();
    producer.close().await.unwrap();

    let mut ccfg = ConsumerConfig::bootstrap([mock.addr.clone()]);
    ccfg.max_wait_ms = 10;
    ccfg.retry_backoff = Duration::ZERO;
    ccfg.reconnect_backoff = Duration::from_millis(50);
    ccfg.reconnect_backoff_max = Duration::from_millis(50);
    let mut consumer = Consumer::new(ccfg).await.unwrap();
    consumer.assign("t", 0, 0).await.unwrap();
    mock.refuse_connections(1);
    let start = Instant::now();
    let recs = consumer.fetch().await.unwrap();
    assert_eq!(recs.len(), 1);
    assert!(
        start.elapsed() >= Duration::from_millis(50),
        "failed broker connect must wait reconnect.backoff.ms, elapsed {:?}",
        start.elapsed()
    );
}

#[tokio::test]
async fn fetch_reconnects_when_connection_idle() {
    let mock = common::Mock::start().await;
    let mut consumer = Consumer::new(
        ConsumerConfig::bootstrap([mock.addr.clone()])
            .max_wait_ms(10)
            .connections_max_idle(Duration::from_millis(30)),
    )
    .await
    .unwrap();
    consumer.assign("t", 0, 0).await.unwrap();
    let _ = consumer.fetch().await.unwrap();
    let after_fetch = mock.accept_count();
    tokio::time::sleep(Duration::from_millis(50)).await;
    let _ = consumer.fetch().await.unwrap();
    let after_idle = mock.accept_count();
    assert!(
        after_idle > after_fetch,
        "idle Fetch must open a new TCP connection (before {after_fetch}, after {after_idle})"
    );
}

#[tokio::test]
async fn produce_reconnects_when_connection_idle() {
    let mock = common::Mock::start().await;
    let producer = Producer::new(
        ProducerConfig::bootstrap([mock.addr.clone()])
            .linger(Duration::ZERO)
            .connections_max_idle(Duration::from_millis(30)),
    )
    .await
    .unwrap();
    producer
        .send(ProduceRecord::to("t").value(&b"idle-a"[..]))
        .await
        .unwrap();
    let after_send = mock.accept_count();
    tokio::time::sleep(Duration::from_millis(50)).await;
    producer
        .send(ProduceRecord::to("t").value(&b"idle-b"[..]))
        .await
        .unwrap();
    let after_idle = mock.accept_count();
    assert!(
        after_idle > after_send,
        "idle Produce must open a new TCP connection (before {after_send}, after {after_idle})"
    );
    producer.close().await.unwrap();
}

#[tokio::test]
async fn admin_reconnects_when_connection_idle() {
    let mock = common::Mock::start().await;
    let mut admin = Admin::new(
        AdminConfig::bootstrap([mock.addr.clone()]).connections_max_idle(Duration::from_millis(30)),
    )
    .await
    .unwrap();
    admin.describe_cluster().await.unwrap();
    let after_first = mock.accept_count();
    tokio::time::sleep(Duration::from_millis(50)).await;
    admin.describe_cluster().await.unwrap();
    let after_idle = mock.accept_count();
    assert!(
        after_idle > after_first,
        "idle DescribeCluster must open a new TCP connection (before {after_first}, after {after_idle})"
    );
}

#[tokio::test]
async fn admin_list_groups_reconnects_when_connection_idle() {
    let mock = common::Mock::start().await;
    let mut admin = Admin::new(
        AdminConfig::bootstrap([mock.addr.clone()]).connections_max_idle(Duration::from_millis(30)),
    )
    .await
    .unwrap();
    let _ = admin.list_groups(&[], &[]).await.unwrap();
    let after_first = mock.accept_count();
    tokio::time::sleep(Duration::from_millis(50)).await;
    let _ = admin.list_groups(&[], &[]).await.unwrap();
    let after_idle = mock.accept_count();
    assert!(
        after_idle > after_first,
        "idle ListGroups must open a new TCP connection (before {after_first}, after {after_idle})"
    );
}

#[tokio::test]
async fn group_coord_reconnects_when_connection_idle() {
    let mock = common::Mock::start().await;
    let mut group = ConsumerGroup::join(
        ConsumerConfig::bootstrap([mock.addr.clone()])
            .max_wait_ms(10)
            .heartbeat_interval(Duration::from_secs(60))
            .connections_max_idle(Duration::from_millis(30)),
        "idle-coord",
        "t",
    )
    .await
    .unwrap();
    let _ = group.committed().await.unwrap();
    let after_first = mock.accept_count();
    tokio::time::sleep(Duration::from_millis(50)).await;
    let _ = group.committed().await.unwrap();
    let after_idle = mock.accept_count();
    assert!(
        after_idle > after_first,
        "idle OffsetFetch must open a new coordinator TCP connection (before {after_first}, after {after_idle})"
    );
    group.close().await.unwrap();
}

#[tokio::test]
async fn admin_reconnect_honors_backoff() {
    let mock = common::Mock::start().await;
    let mut acfg = AdminConfig::bootstrap([mock.addr.clone()]);
    acfg.reconnect_backoff = Duration::from_millis(50);
    acfg.reconnect_backoff_max = Duration::from_millis(50);
    let mut admin = Admin::new(acfg).await.unwrap();
    mock.refuse_connections(1);
    let start = Instant::now();
    let results = admin
        .create_topics(&[NewTopic::new("reconnect-t", 1, 1)], 5_000, false)
        .await
        .unwrap();
    assert_eq!(results[0].error_code, 0);
    assert!(
        start.elapsed() >= Duration::from_millis(50),
        "failed broker connect must wait reconnect.backoff.ms, elapsed {:?}",
        start.elapsed()
    );
}

#[tokio::test]
async fn admin_retry_honors_backoff() {
    let mock = common::Mock::start_two_node().await;
    mock.set_controller(2);
    let mut acfg = AdminConfig::bootstrap([mock.addr.clone()]);
    acfg.retry_backoff = Duration::from_millis(50);
    acfg.retry_backoff_max = Duration::from_millis(50);
    let mut admin = Admin::new(acfg).await.unwrap();
    admin
        .create_topics(&[NewTopic::new("retry-a", 1, 1)], 10_000, false)
        .await
        .unwrap();
    mock.set_controller(1);
    let start = Instant::now();
    let created = admin
        .create_topics(&[NewTopic::new("retry-b", 1, 1)], 10_000, false)
        .await
        .unwrap();
    assert_eq!(created[0].error_code, 0);
    assert_eq!(mock.create_topics_not_controller(), 1);
    assert!(
        start.elapsed() >= Duration::from_millis(50),
        "NOT_CONTROLLER must wait retry.backoff.ms, elapsed {:?}",
        start.elapsed()
    );
}

#[tokio::test]
async fn produce_refreshes_metadata_after_max_age() {
    let mock = common::Mock::start().await;
    let mut pcfg = ProducerConfig::bootstrap([mock.addr.clone()]);
    pcfg.linger = Duration::ZERO;
    pcfg.metadata_max_age = Duration::from_millis(40);
    let producer = Producer::new(pcfg).await.unwrap();
    producer
        .send(ProduceRecord::to("t").value(&b"a"[..]))
        .await
        .unwrap();
    let after_first = mock.metadata_calls();
    assert!(
        after_first >= 1,
        "first send must fetch Metadata, got {after_first}"
    );
    assert_eq!(
        mock.last_metadata_allow_auto(),
        Some(false),
        "producer default allow.auto.create.topics is false"
    );
    producer
        .send(ProduceRecord::to("t").value(&b"b"[..]))
        .await
        .unwrap();
    assert_eq!(
        mock.metadata_calls(),
        after_first,
        "fresh metadata.max.age.ms cache must not Metadata again"
    );
    tokio::time::sleep(Duration::from_millis(50)).await;
    producer
        .send(ProduceRecord::to("t").value(&b"c"[..]))
        .await
        .unwrap();
    assert!(
        mock.metadata_calls() > after_first,
        "stale metadata.max.age.ms must Metadata again, first {after_first} now {}",
        mock.metadata_calls()
    );
    producer.close().await.unwrap();
}

#[tokio::test]
async fn flush_fails_on_broker_produce_error() {
    let mock = common::Mock::start().await;
    mock.set_produce_error(error::OUT_OF_ORDER_SEQUENCE_NUMBER);
    let mut pcfg = ProducerConfig::bootstrap([mock.addr.clone()]);
    pcfg.linger = Duration::ZERO;
    pcfg.connections = 8;
    let producer = Producer::new(pcfg).await.unwrap();
    loop {
        match producer.try_send(ProduceRecord::to("t").value(&b"x"[..])) {
            Ok(()) => break,
            Err(Error::QueueFull) => tokio::task::yield_now().await,
            Err(e) => panic!("try_send: {e}"),
        }
    }
    let err = producer
        .flush()
        .await
        .expect_err("flush must surface broker error");
    match err {
        Error::Broker { code, .. } => assert_eq!(code, error::OUT_OF_ORDER_SEQUENCE_NUMBER),
        other => panic!("expected broker error 45, got {other}"),
    }
    assert_eq!(mock.log_len("t", 0), 0);
}

#[tokio::test]
async fn tls_produce_fetch() {
    let (mock, tls) = common::Mock::start_tls().await;
    let mut pcfg = ProducerConfig::bootstrap([mock.addr.clone()]);
    pcfg.linger = Duration::ZERO;
    pcfg.tls = Some(tls.clone());
    let producer = Producer::new(pcfg).await.unwrap();
    let _md = producer
        .send(ProduceRecord::to("t").value(&b"tls-hello"[..]))
        .await
        .unwrap();
    producer.close().await.unwrap();

    let mut ccfg = ConsumerConfig::bootstrap([mock.addr.clone()]);
    ccfg.max_wait_ms = 10;
    ccfg.tls = Some(tls);
    let mut consumer = Consumer::new(ccfg).await.unwrap();
    consumer.assign("t", 0, 0).await.unwrap();
    let recs = consumer.fetch().await.unwrap();
    assert_eq!(recs[0].value.as_deref(), Some(&b"tls-hello"[..]));
}

#[tokio::test]
async fn transactional_commit_visible_abort_hidden() {
    let mock = common::Mock::start().await;
    let mut pcfg = ProducerConfig::bootstrap([mock.addr.clone()]);
    pcfg.linger = Duration::ZERO;
    pcfg.transactional_id = Some("tx-1".into());
    let producer = Producer::new(pcfg).await.unwrap();
    producer.begin_transaction().await.unwrap();
    producer
        .send(ProduceRecord::to("t").value(&b"committed"[..]))
        .await
        .unwrap();
    producer
        .send_offsets_to_transaction("g", [(TopicPartition::new("t", 0), 1)])
        .await
        .unwrap();
    producer.commit_transaction().await.unwrap();
    producer.begin_transaction().await.unwrap();
    producer
        .send(ProduceRecord::to("t").value(&b"aborted"[..]))
        .await
        .unwrap();
    producer.abort_transaction().await.unwrap();
    producer.close().await.unwrap();
    assert_eq!(
        mock.last_produce_txn_id().as_deref(),
        Some("tx-1"),
        "Produce body must carry transactional_id, not null"
    );

    let mut ccfg0 = ConsumerConfig::bootstrap([mock.addr.clone()]);
    ccfg0.max_wait_ms = 10;
    ccfg0.isolation_level = IsolationLevel::ReadUncommitted;
    let mut uncommitted = Consumer::new(ccfg0).await.unwrap();
    uncommitted.assign("t", 0, 0).await.unwrap();
    let all = uncommitted.fetch().await.unwrap();
    let all_vals: Vec<&[u8]> = all.iter().filter_map(|r| r.value.as_deref()).collect();
    assert!(
        all_vals.iter().any(|v| *v == b"aborted"),
        "mock must return aborted records so the client, not the broker, filters them; got {all_vals:?}"
    );

    let mut ccfg = ConsumerConfig::bootstrap([mock.addr.clone()]);
    ccfg.max_wait_ms = 10;
    ccfg.isolation_level = IsolationLevel::ReadCommitted;
    let mut consumer = Consumer::new(ccfg).await.unwrap();
    consumer.assign("t", 0, 0).await.unwrap();
    let recs = consumer.fetch().await.unwrap();
    let vals: Vec<&[u8]> = recs.iter().filter_map(|r| r.value.as_deref()).collect();
    assert_eq!(vals, vec![&b"committed"[..]]);
    assert!(!vals.iter().any(|v| *v == b"aborted"));
}

#[tokio::test]
async fn transactional_offsets_and_partitions_one_rpc() {
    let mock = common::Mock::start().await;
    let mut admin = Admin::new(AdminConfig::bootstrap([mock.addr.clone()]))
        .await
        .unwrap();
    assert_eq!(
        admin
            .create_topics(&[NewTopic::new("txn3", 3, 1)], 10_000, false)
            .await
            .unwrap()[0]
            .error_code,
        0
    );
    let e0 = mock.bump_leader_epoch("txn3", 0);
    let e1 = mock.bump_leader_epoch("txn3", 1);
    let e2 = mock.bump_leader_epoch("txn3", 2);

    let mut pcfg = ProducerConfig::bootstrap([mock.addr.clone()]);
    pcfg.linger = Duration::from_millis(5);
    pcfg.connections = 1;
    pcfg.transactional_id = Some("tx-batch".into());
    let producer = Producer::new(pcfg).await.unwrap();
    producer.begin_transaction().await.unwrap();
    let deadline = std::time::Instant::now() + Duration::from_secs(2);
    for p in 0..3 {
        let rec = ProduceRecord::to("txn3")
            .partition(p)
            .value(format!("txn-{p}").into_bytes());
        loop {
            match producer.try_send(rec.clone()) {
                Ok(()) => break,
                Err(Error::QueueFull) => {
                    assert!(
                        std::time::Instant::now() < deadline,
                        "try_send never left QueueFull"
                    );
                    tokio::task::yield_now().await;
                }
                Err(e) => panic!("try_send: {e}"),
            }
        }
    }
    producer.flush().await.unwrap();
    assert_eq!(
        mock.add_partitions_to_txn_calls(),
        0,
        "Produce v12 skips AddPartitionsToTxn (transaction V2), got {} calls",
        mock.add_partitions_to_txn_calls()
    );
    assert_eq!(mock.last_add_partitions_to_txn(), 0);
    assert_eq!(
        mock.last_add_partitions_to_txn_version(),
        None,
        "Produce v12 skips AddPartitionsToTxn (transaction V2)"
    );

    producer
        .send_offsets_to_transaction(
            "g",
            [
                (TopicPartition::new("txn3", 0), 1),
                (TopicPartition::new("txn3", 1), 1),
                (TopicPartition::new("txn3", 2), 1),
            ],
        )
        .await
        .unwrap();
    assert_eq!(
        mock.txn_offset_commit_calls(),
        1,
        "TxnOffsetCommit must be one RPC, got {}",
        mock.txn_offset_commit_calls()
    );
    assert_eq!(mock.last_txn_offset_commit_partitions(), 3);
    assert_eq!(
        mock.last_txn_offset_commit_version(),
        Some(5),
        "Producer must prefer TxnOffsetCommit v5 when the broker advertises it"
    );
    assert_eq!(
        mock.last_txn_offset_generation(),
        Some(-1),
        "send_offsets_to_transaction must send generation -1"
    );
    assert_eq!(
        mock.last_txn_offset_member_id().as_deref(),
        Some(""),
        "send_offsets_to_transaction must send empty member id"
    );
    assert_eq!(
        mock.last_add_offsets_to_txn_version(),
        None,
        "TxnOffsetCommit v5 skips AddOffsetsToTxn (transaction V2)"
    );
    assert_eq!(
        mock.last_txn_offset_epochs(),
        vec![e0, e1, e2],
        "TxnOffsetCommit v2+ must send Metadata current_leader_epoch"
    );
    producer.commit_transaction().await.unwrap();
    assert_eq!(
        mock.last_end_txn_version(),
        Some(5),
        "Producer must prefer EndTxn v5 when the broker advertises it"
    );
    producer.begin_transaction().await.unwrap();
    producer
        .send(
            ProduceRecord::to("txn3")
                .partition(0)
                .value(&b"after-v5"[..]),
        )
        .await
        .unwrap();
    producer.flush().await.unwrap();
    assert_eq!(
        mock.last_add_partitions_to_txn_version(),
        None,
        "Produce v12 skips AddPartitionsToTxn (transaction V2)"
    );
    assert_eq!(
        mock.last_add_partitions_producer_epoch(),
        None,
        "Produce v12 skips AddPartitionsToTxn (transaction V2)"
    );
    assert_eq!(
        mock.last_produce_producer_epoch(),
        Some(1),
        "EndTxn v5 must apply the bumped producer epoch on the next Produce"
    );
    producer.close().await.unwrap();
}

#[tokio::test]
async fn transactional_producer_finds_txn_coordinator() {
    let mock = common::Mock::start_two_node().await;
    mock.set_txn_coordinator(2);
    let mut pcfg = ProducerConfig::bootstrap([mock.addr.clone()]);
    pcfg.linger = Duration::ZERO;
    pcfg.transactional_id = Some("tx-coord".into());
    pcfg.transaction_timeout = Duration::from_secs(45);
    let producer = Producer::new(pcfg).await.unwrap();
    assert!(
        mock.find_coordinator_key_types()
            .contains(&COORDINATOR_TRANSACTION),
        "InitProducerId with transactional.id must FindCoordinator key_type=1"
    );
    assert_eq!(
        mock.last_find_coordinator_version(),
        Some(6),
        "Producer must prefer FindCoordinator v6 when the broker advertises it"
    );
    assert_eq!(
        mock.last_init_producer_id_node(),
        Some(2),
        "InitProducerId must land on the transaction coordinator, not bootstrap"
    );
    assert_eq!(
        mock.last_init_producer_id_timeout(),
        Some(45_000),
        "InitProducerId must send transaction.timeout.ms"
    );

    producer.begin_transaction().await.unwrap();
    producer
        .send(ProduceRecord::to("t").value(&b"coord"[..]))
        .await
        .unwrap();
    producer.flush().await.unwrap();
    assert_eq!(
        mock.last_add_partitions_node(),
        None,
        "Produce v12 skips AddPartitionsToTxn (transaction V2)"
    );
    producer
        .send_offsets_to_transaction("g", [(TopicPartition::new("t", 0), 1)])
        .await
        .unwrap();
    assert_eq!(
        mock.last_add_offsets_node(),
        None,
        "TxnOffsetCommit v5 skips AddOffsetsToTxn (transaction V2)"
    );
    assert!(
        mock.find_coordinator_key_types()
            .contains(&COORDINATOR_GROUP),
        "TxnOffsetCommit must FindCoordinator key_type=0"
    );
    assert_eq!(
        mock.last_txn_offset_commit_node(),
        Some(1),
        "TxnOffsetCommit must land on the group coordinator"
    );
    producer.commit_transaction().await.unwrap();
    assert_eq!(mock.last_end_txn_node(), Some(2));
    assert_eq!(
        mock.last_end_txn_version(),
        Some(5),
        "Producer must prefer EndTxn v5 when the broker advertises it"
    );

    mock.move_txn_coordinator();
    producer.begin_transaction().await.unwrap();
    producer
        .send(ProduceRecord::to("t").value(&b"moved"[..]))
        .await
        .unwrap();
    producer.commit_transaction().await.unwrap();
    assert_eq!(
        mock.last_end_txn_node(),
        Some(1),
        "EndTxn must rediscover after NOT_COORDINATOR"
    );
    producer.close().await.unwrap();
}

#[tokio::test]
async fn init_producer_id_rediscovers_after_not_coordinator() {
    let mock = common::Mock::start_two_node().await;
    mock.set_txn_coordinator(2);
    mock.stale_txn_find_once();
    let mut pcfg = ProducerConfig::bootstrap([mock.addr.clone()]);
    pcfg.linger = Duration::ZERO;
    pcfg.transactional_id = Some("tx-stale".into());
    let producer = Producer::new(pcfg).await.unwrap();
    assert_eq!(
        mock.init_producer_id_nodes(),
        vec![1, 2],
        "InitProducerId must hit the stale node then the coordinator"
    );
    assert_eq!(
        mock.init_producer_id_not_coordinator(),
        1,
        "wrong node must return NOT_COORDINATOR (16)"
    );
    assert_eq!(
        mock.last_init_producer_id_node(),
        Some(2),
        "InitProducerId must rediscover and land on the transaction coordinator"
    );
    producer.close().await.unwrap();
}

#[tokio::test]
async fn list_offsets_seek_and_read_committed_isolation() {
    let mock = common::Mock::start().await;
    let mut pcfg = ProducerConfig::bootstrap([mock.addr.clone()]);
    pcfg.linger = Duration::ZERO;
    let producer = Producer::new(pcfg).await.unwrap();
    for i in 0..3 {
        let v = format!("v{i}");
        producer
            .send(ProduceRecord::to("t").value(v.into_bytes()))
            .await
            .unwrap();
    }
    producer.close().await.unwrap();

    let mut ccfg = ConsumerConfig::bootstrap([mock.addr.clone()]);
    ccfg.max_wait_ms = 10;
    ccfg.isolation_level = IsolationLevel::ReadCommitted;
    let mut consumer = Consumer::new(ccfg).await.unwrap();
    let earliest = consumer
        .list_offsets("t", 0, EARLIEST_TIMESTAMP)
        .await
        .unwrap();
    let latest = consumer
        .list_offsets("t", 0, LATEST_TIMESTAMP)
        .await
        .unwrap();
    assert_eq!(earliest, 0);
    assert_eq!(latest, 3);
    let by_ts = consumer.list_offsets("t", 0, 0).await.unwrap();
    assert_eq!(by_ts, 0);

    consumer.assign("t", 0, 0).await.unwrap();
    consumer.seek("t", 0, 1).unwrap();
    let recs = consumer.fetch().await.unwrap();
    assert_eq!(recs[0].offset, 1);
    assert_eq!(mock.last_fetch_isolation(), 1);
}

#[tokio::test]
async fn fetch_sends_split_max_bytes() {
    let mock = common::Mock::start().await;
    let producer =
        Producer::new(ProducerConfig::bootstrap([mock.addr.clone()]).linger(Duration::ZERO))
            .await
            .unwrap();
    producer
        .send(ProduceRecord::to("t").value(&b"a"[..]))
        .await
        .unwrap();
    producer.close().await.unwrap();

    let mut consumer = Consumer::new(
        ConsumerConfig::bootstrap([mock.addr.clone()])
            .max_wait_ms(10)
            .fetch_max_bytes(4096)
            .max_partition_fetch_bytes(2048),
    )
    .await
    .unwrap();
    consumer.assign("t", 0, 0).await.unwrap();
    let recs = consumer.fetch().await.unwrap();
    assert_eq!(recs.len(), 1);
    assert_eq!(mock.last_fetch_max_bytes(), 4096);
    assert_eq!(mock.last_fetch_partition_max_bytes(), 2048);
    assert_eq!(
        mock.last_fetch_version(),
        Some(17),
        "Consumer must prefer Fetch v17 when the broker advertises it"
    );
    consumer.close().await.unwrap();
}

#[tokio::test]
async fn fetch_sends_last_fetched_epoch_after_records() {
    let mock = common::Mock::start().await;
    let producer =
        Producer::new(ProducerConfig::bootstrap([mock.addr.clone()]).linger(Duration::ZERO))
            .await
            .unwrap();
    producer
        .send(ProduceRecord::to("t").value(&b"a"[..]))
        .await
        .unwrap();
    producer.close().await.unwrap();

    let mut consumer =
        Consumer::new(ConsumerConfig::bootstrap([mock.addr.clone()]).max_wait_ms(10))
            .await
            .unwrap();
    consumer.assign("t", 0, 0).await.unwrap();
    let recs = consumer.fetch().await.unwrap();
    assert_eq!(recs.len(), 1);
    assert_eq!(
        mock.last_fetched_epoch(),
        Some(-1),
        "first Fetch has no last consumed batch"
    );

    let recs = consumer.fetch().await.unwrap();
    assert!(recs.is_empty());
    assert_eq!(
        mock.last_fetched_epoch(),
        Some(0),
        "Fetch v12+ must send LastFetchedEpoch from the last consumed batch"
    );

    consumer.seek("t", 0, 0).unwrap();
    let recs = consumer.fetch().await.unwrap();
    assert_eq!(recs.len(), 1);
    assert_eq!(
        mock.last_fetched_epoch(),
        Some(-1),
        "seek must clear LastFetchedEpoch"
    );

    consumer
        .seek_with_metadata(("t", 0), OffsetAndMetadata::new(0).with_leader_epoch(7))
        .unwrap();
    let recs = consumer.fetch().await.unwrap();
    assert_eq!(recs.len(), 1);
    assert_eq!(
        mock.last_fetched_epoch(),
        Some(7),
        "seek_with_metadata must send OffsetAndMetadata leader epoch as LastFetchedEpoch"
    );
    consumer.close().await.unwrap();
}

#[tokio::test]
async fn fetch_truncates_on_diverging_epoch() {
    let mock = common::Mock::start().await;
    let producer =
        Producer::new(ProducerConfig::bootstrap([mock.addr.clone()]).linger(Duration::ZERO))
            .await
            .unwrap();
    producer
        .send_all([
            ProduceRecord::to("t").value(&b"a"[..]),
            ProduceRecord::to("t").value(&b"b"[..]),
            ProduceRecord::to("t").value(&b"c"[..]),
        ])
        .await
        .unwrap();
    producer.close().await.unwrap();

    let mut consumer =
        Consumer::new(ConsumerConfig::bootstrap([mock.addr.clone()]).max_wait_ms(10))
            .await
            .unwrap();
    consumer.assign("t", 0, 0).await.unwrap();
    let recs = consumer.fetch().await.unwrap();
    assert_eq!(recs.len(), 3);
    assert_eq!(consumer.positions(), vec![(TopicPartition::new("t", 0), 3)]);

    mock.set_next_diverging_epoch("t", 0, 0, 1);
    let recs = consumer.fetch().await.unwrap();
    assert_eq!(
        recs.iter().map(|r| r.offset).collect::<Vec<_>>(),
        vec![1, 2],
        "DivergingEpoch must seek to EndOffset and refetch"
    );
    assert_eq!(consumer.positions(), vec![(TopicPartition::new("t", 0), 3)]);
    consumer.close().await.unwrap();
}

#[tokio::test]
async fn list_offsets_sends_current_leader_epoch() {
    let mock = common::Mock::start().await;
    let bumped = mock.bump_leader_epoch("t", 0);
    let mut ccfg = ConsumerConfig::bootstrap([mock.addr.clone()]);
    ccfg.max_wait_ms = 10;
    let mut consumer = Consumer::new(ccfg).await.unwrap();
    let latest = consumer
        .list_offsets("t", 0, LATEST_TIMESTAMP)
        .await
        .unwrap();
    assert_eq!(latest, 0);
    let got = mock
        .last_list_offsets()
        .expect("ListOffsets must send current_leader_epoch");
    assert_eq!(got.0, "t");
    assert_eq!(got.1, 0);
    assert_eq!(
        mock.last_list_offsets_version(),
        Some(10),
        "Consumer must prefer ListOffsets v10 when the broker advertises it"
    );
    assert_eq!(got.2, bumped);
    assert!(
        mock.last_offset_for_leader_epoch().is_none(),
        "fresh metadata epoch must not speak OffsetForLeaderEpoch"
    );
}

#[tokio::test]
async fn list_offsets_recovers_from_fenced_leader_epoch() {
    let mock = common::Mock::start().await;
    let mut ccfg = ConsumerConfig::bootstrap([mock.addr.clone()]);
    ccfg.max_wait_ms = 10;
    let mut consumer = Consumer::new(ccfg).await.unwrap();
    let first = consumer
        .list_offsets("t", 0, LATEST_TIMESTAMP)
        .await
        .unwrap();
    assert_eq!(first, 0);
    let bumped = mock.bump_leader_epoch("t", 0);
    assert!(bumped > 0);
    let again = consumer
        .list_offsets("t", 0, LATEST_TIMESTAMP)
        .await
        .unwrap();
    assert_eq!(again, 0);
    let ofle = mock
        .last_offset_for_leader_epoch()
        .expect("fenced ListOffsets must speak OffsetForLeaderEpoch");
    assert_eq!(ofle.0, "t");
    assert_eq!(ofle.1, 0);
    let sent = mock
        .last_list_offsets()
        .expect("retry ListOffsets after recover");
    assert_eq!(sent.2, bumped);
}

#[tokio::test]
async fn list_offsets_follows_partition_leader() {
    let mock = common::Mock::start_two_node().await;
    let mut pcfg = ProducerConfig::bootstrap([mock.addr.clone()]);
    pcfg.linger = Duration::ZERO;
    let producer = Producer::new(pcfg).await.unwrap();
    producer
        .send(ProduceRecord::to("t").value(&b"lo-lead"[..]))
        .await
        .unwrap();
    producer.close().await.unwrap();

    let mut ccfg = ConsumerConfig::bootstrap([mock.addr.clone()]);
    ccfg.max_wait_ms = 10;
    let mut consumer = Consumer::new(ccfg).await.unwrap();
    let latest = consumer
        .list_offsets("t", 0, LATEST_TIMESTAMP)
        .await
        .unwrap();
    assert_eq!(latest, 1);
    assert_eq!(
        mock.last_list_offsets_node(),
        Some(2),
        "ListOffsets must land on the partition leader, not a follower"
    );

    mock.set_partition_leader("t", 0, 1);
    let again = consumer
        .list_offsets("t", 0, LATEST_TIMESTAMP)
        .await
        .unwrap();
    assert_eq!(again, 1);
    assert_eq!(
        mock.list_offsets_not_leader(),
        1,
        "stale leader must return NOT_LEADER_OR_FOLLOWER (6) once"
    );
    assert_eq!(
        mock.last_list_offsets_node(),
        Some(1),
        "ListOffsets must follow Metadata after NOT_LEADER"
    );
}

#[tokio::test]
async fn admin_list_offsets_batches_by_leader() {
    let mock = common::Mock::start_two_node().await;
    let mut admin = Admin::connect(mock.addr.clone()).await.unwrap();
    let created = admin
        .create_topics(&[NewTopic::new("u", 2, 1)], 10_000, false)
        .await
        .unwrap();
    assert_eq!(created[0].error_code, 0);

    let listed = admin
        .list_offsets([
            (("t", 0), LATEST_TIMESTAMP),
            (("u", 0), LATEST_TIMESTAMP),
            (("u", 1), LATEST_TIMESTAMP),
        ])
        .await
        .unwrap();
    assert_eq!(listed.len(), 3);
    assert_eq!(listed[0].0, TopicPartition::new("t", 0));
    assert_eq!(listed[0].0.topic(), "t");
    assert_eq!(listed[0].0.partition(), 0);
    assert_eq!(listed[0].1.offset(), listed[0].1.offset);
    assert_eq!(listed[0].1.timestamp(), listed[0].1.timestamp);
    assert_eq!(listed[1].0, TopicPartition::new("u", 0));
    assert_eq!(listed[2].0, TopicPartition::new("u", 1));
    assert_eq!(
        mock.list_offsets_calls(),
        2,
        "t-0 is on node 2; u-0 and u-1 share node 1"
    );
    assert_eq!(
        mock.last_list_offsets_n(),
        Some(2),
        "last ListOffsets must carry both u partitions"
    );
    assert_eq!(
        mock.last_list_offsets_node(),
        Some(1),
        "u partitions must land on the default leader, not t's leader"
    );
    assert_eq!(
        mock.last_list_offsets_isolation(),
        Some(0),
        "list_offsets defaults to read-uncommitted"
    );
    assert_eq!(
        mock.last_list_offsets_timeout(),
        Some(30_000),
        "list_offsets must send request_timeout as ListOffsets v10 TimeoutMs"
    );

    let committed = admin
        .list_offsets_with_isolation(
            [(("t", 0), LATEST_TIMESTAMP)],
            IsolationLevel::ReadCommitted,
        )
        .await
        .unwrap();
    assert_eq!(committed.len(), 1);
    assert_eq!(
        mock.last_list_offsets_isolation(),
        Some(1),
        "list_offsets_with_isolation must send isolation=1"
    );
    let timed = admin
        .list_offsets_timeout([(("t", 0), LATEST_TIMESTAMP)], Duration::from_secs(5))
        .await
        .unwrap();
    assert_eq!(timed.len(), 1);
    assert_eq!(
        mock.last_list_offsets_timeout(),
        Some(5_000),
        "list_offsets_timeout must send ListOffsets v10 TimeoutMs"
    );
    let timed_iso = admin
        .list_offsets_with_isolation_timeout(
            [(("t", 0), LATEST_TIMESTAMP)],
            IsolationLevel::ReadCommitted,
            Duration::from_secs(8),
        )
        .await
        .unwrap();
    assert_eq!(timed_iso.len(), 1);
    assert_eq!(mock.last_list_offsets_isolation(), Some(1));
    assert_eq!(
        mock.last_list_offsets_timeout(),
        Some(8_000),
        "list_offsets_with_isolation_timeout must send isolation and TimeoutMs"
    );
    let spec = admin
        .list_offsets([(("t", 0), OffsetSpec::latest())])
        .await
        .unwrap();
    assert_eq!(spec.len(), 1);
    assert_eq!(spec[0].0, TopicPartition::new("t", 0));
    assert_eq!(
        mock.last_list_offsets_isolation(),
        Some(0),
        "OffsetSpec::latest uses list_offsets default isolation"
    );
}

#[tokio::test]
async fn fetch_offset_out_of_range_jumps_to_log_start() {
    let mock = common::Mock::start().await;
    mock.set_log_start("t", 0, 10);
    let mut ccfg = ConsumerConfig::bootstrap([mock.addr.clone()]);
    ccfg.max_wait_ms = 10;
    let mut consumer = Consumer::new(ccfg).await.unwrap();
    consumer.assign("t", 0, 0).await.unwrap();
    let recs = consumer.fetch().await.unwrap();
    assert!(recs.is_empty());
    assert_eq!(
        consumer.positions(),
        vec![(TopicPartition::new("t", 0), 10)]
    );
}

#[tokio::test]
async fn gzip_produce_fetch() {
    let mock = common::Mock::start().await;
    let mut pcfg = ProducerConfig::bootstrap([mock.addr.clone()]);
    pcfg.linger = Duration::ZERO;
    pcfg.compression = Compression::Gzip;
    let producer = Producer::new(pcfg).await.unwrap();
    producer
        .send(ProduceRecord::to("t").value(&b"gzip-hello"[..]))
        .await
        .unwrap();
    producer.close().await.unwrap();

    let mut ccfg = ConsumerConfig::bootstrap([mock.addr.clone()]);
    ccfg.max_wait_ms = 10;
    let mut consumer = Consumer::new(ccfg).await.unwrap();
    consumer.assign("t", 0, 0).await.unwrap();
    let recs = consumer.fetch().await.unwrap();
    assert_eq!(recs[0].value.as_deref(), Some(&b"gzip-hello"[..]));
}

#[tokio::test]
async fn snappy_produce_fetch() {
    let mock = common::Mock::start().await;
    let mut pcfg = ProducerConfig::bootstrap([mock.addr.clone()]);
    pcfg.linger = Duration::ZERO;
    pcfg.compression = Compression::Snappy;
    let producer = Producer::new(pcfg).await.unwrap();
    producer
        .send(ProduceRecord::to("t").value(&b"snappy-hello"[..]))
        .await
        .unwrap();
    producer.close().await.unwrap();

    let mut ccfg = ConsumerConfig::bootstrap([mock.addr.clone()]);
    ccfg.max_wait_ms = 10;
    let mut consumer = Consumer::new(ccfg).await.unwrap();
    consumer.assign("t", 0, 0).await.unwrap();
    let recs = consumer.fetch().await.unwrap();
    assert_eq!(recs[0].value.as_deref(), Some(&b"snappy-hello"[..]));
}

#[tokio::test]
async fn lz4_produce_fetch() {
    let mock = common::Mock::start().await;
    let mut pcfg = ProducerConfig::bootstrap([mock.addr.clone()]);
    pcfg.linger = Duration::ZERO;
    pcfg.compression = Compression::Lz4;
    let producer = Producer::new(pcfg).await.unwrap();
    producer
        .send(ProduceRecord::to("t").value(&b"lz4-hello"[..]))
        .await
        .unwrap();
    producer.close().await.unwrap();

    let mut ccfg = ConsumerConfig::bootstrap([mock.addr.clone()]);
    ccfg.max_wait_ms = 10;
    let mut consumer = Consumer::new(ccfg).await.unwrap();
    consumer.assign("t", 0, 0).await.unwrap();
    let recs = consumer.fetch().await.unwrap();
    assert_eq!(recs[0].value.as_deref(), Some(&b"lz4-hello"[..]));
}

#[tokio::test]
async fn sasl_scram_sha256_then_produce() {
    let mock = common::Mock::start_with_scram(("alice".into(), "secret".into())).await;
    let mut pcfg = ProducerConfig::bootstrap([mock.addr.clone()]);
    pcfg.linger = Duration::ZERO;
    pcfg.sasl_scram = Some(("alice".into(), "secret".into()));
    let producer = Producer::new(pcfg).await.unwrap();
    let md = producer
        .send(ProduceRecord::to("t").value(&b"scram-ok"[..]))
        .await
        .unwrap();
    assert_eq!(md.offset, 0);
    producer.close().await.unwrap();
}

#[tokio::test]
async fn sasl_scram_sha512_then_produce() {
    let mock = common::Mock::start_with_scram_sha512(("alice".into(), "secret".into())).await;
    let mut pcfg = ProducerConfig::bootstrap([mock.addr.clone()]);
    pcfg.linger = Duration::ZERO;
    pcfg.sasl_scram_sha512 = Some(("alice".into(), "secret".into()));
    let producer = Producer::new(pcfg).await.unwrap();
    let md = producer
        .send(ProduceRecord::to("t").value(&b"scram512-ok"[..]))
        .await
        .unwrap();
    assert_eq!(md.offset, 0);
    producer.close().await.unwrap();
}

#[tokio::test]
async fn sasl_oauthbearer_then_produce() {
    let mock = common::Mock::start_with_oauthbearer("alice".into()).await;
    let mut pcfg = ProducerConfig::bootstrap([mock.addr.clone()]);
    pcfg.linger = Duration::ZERO;
    pcfg.sasl_oauthbearer = Some("alice".into());
    let producer = Producer::new(pcfg).await.unwrap();
    let md = producer
        .send(ProduceRecord::to("t").value(&b"oauth-ok"[..]))
        .await
        .unwrap();
    assert_eq!(md.offset, 0);
    producer.close().await.unwrap();
}

#[tokio::test]
async fn sasl_oidc_then_produce() {
    let token_url =
        common::start_oidc_token_endpoint("cid".into(), "csecret".into(), "alice".into()).await;
    let mock = common::Mock::start_with_oauthbearer("alice".into()).await;
    let mut pcfg = ProducerConfig::bootstrap([mock.addr.clone()]);
    pcfg.linger = Duration::ZERO;
    pcfg.sasl_oauthbearer_oidc = Some(OidcConfig::new(token_url, "cid", "csecret"));
    let producer = Producer::new(pcfg).await.unwrap();
    let md = producer
        .send(ProduceRecord::to("t").value(&b"oidc-ok"[..]))
        .await
        .unwrap();
    assert_eq!(md.offset, 0);
    producer.close().await.unwrap();
}

#[tokio::test]
async fn sasl_oidc_https_then_produce() {
    let (token_url, tls) =
        common::start_oidc_token_endpoint_tls("cid".into(), "csecret".into(), "alice".into()).await;
    let mock = common::Mock::start_with_oauthbearer("alice".into()).await;
    let mut pcfg = ProducerConfig::bootstrap([mock.addr.clone()]);
    pcfg.linger = Duration::ZERO;
    let mut oidc = OidcConfig::new(token_url, "cid", "csecret");
    oidc.tls = Some(tls);
    pcfg.sasl_oauthbearer_oidc = Some(oidc);
    let producer = Producer::new(pcfg).await.unwrap();
    let md = producer
        .send(ProduceRecord::to("t").value(&b"oidc-https"[..]))
        .await
        .unwrap();
    assert_eq!(md.offset, 0);
    producer.close().await.unwrap();
}

#[tokio::test]
async fn sasl_oidc_https_without_trust_fails() {
    let (token_url, _tls) =
        common::start_oidc_token_endpoint_tls("cid".into(), "csecret".into(), "alice".into()).await;
    let mock = common::Mock::start_with_oauthbearer("alice".into()).await;
    let mut pcfg = ProducerConfig::bootstrap([mock.addr.clone()]);
    pcfg.sasl_oauthbearer_oidc = Some(OidcConfig::new(token_url, "cid", "csecret"));
    let err = match Producer::new(pcfg).await {
        Err(e) => e,
        Ok(_) => panic!("https oidc without trusted CA must fail"),
    };
    match err {
        Error::Io(_) | Error::Protocol(_) | Error::Timeout => {}
        other => panic!("expected tls failure, got {other}"),
    }
}

#[tokio::test]
async fn sasl_oidc_then_fetch() {
    let token_url =
        common::start_oidc_token_endpoint("cid".into(), "csecret".into(), "alice".into()).await;
    let mock = common::Mock::start_with_oauthbearer("alice".into()).await;
    let mut pcfg = ProducerConfig::bootstrap([mock.addr.clone()]);
    pcfg.linger = Duration::ZERO;
    pcfg.sasl_oauthbearer_oidc = Some(OidcConfig::new(token_url.clone(), "cid", "csecret"));
    let producer = Producer::new(pcfg).await.unwrap();
    let md = producer
        .send(ProduceRecord::to("t").value(&b"oidc-fetch"[..]))
        .await
        .unwrap();
    producer.close().await.unwrap();
    let mut ccfg = ConsumerConfig::bootstrap([mock.addr.clone()]);
    ccfg.sasl_oauthbearer_oidc = Some(OidcConfig::new(token_url, "cid", "csecret"));
    let mut consumer = Consumer::new(ccfg).await.unwrap();
    consumer.assign("t", md.partition, md.offset).await.unwrap();
    let recs = consumer.fetch().await.unwrap();
    assert_eq!(recs[0].value.as_deref(), Some(&b"oidc-fetch"[..]));
}

#[tokio::test]
async fn sasl_oidc_bad_secret_fails() {
    let token_url =
        common::start_oidc_token_endpoint("cid".into(), "csecret".into(), "alice".into()).await;
    let mock = common::Mock::start_with_oauthbearer("alice".into()).await;
    let mut pcfg = ProducerConfig::bootstrap([mock.addr.clone()]);
    pcfg.sasl_oauthbearer_oidc = Some(OidcConfig::new(token_url, "cid", "wrong"));
    let err = match Producer::new(pcfg).await {
        Err(e) => e,
        Ok(_) => panic!("bad oidc secret must fail"),
    };
    match err {
        Error::Protocol(m) => assert!(m.contains("401") || m.contains("oidc"), "{m}"),
        other => panic!("expected oidc HTTP failure, got {other}"),
    }
}

#[tokio::test]
async fn sasl_oidc_bad_url_fails() {
    let bound = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = bound.local_addr().unwrap();
    drop(bound);
    let mock = common::Mock::start_with_oauthbearer("alice".into()).await;
    let mut pcfg = ProducerConfig::bootstrap([mock.addr.clone()]);
    pcfg.sasl_oauthbearer_oidc = Some(OidcConfig::new(
        format!("http://{addr}/oauth/token"),
        "cid",
        "csecret",
    ));
    let err = match Producer::new(pcfg).await {
        Err(e) => e,
        Ok(_) => panic!("closed token URL must fail"),
    };
    match err {
        Error::Io(_) | Error::Timeout | Error::Protocol(_) => {}
        other => panic!("expected token URL failure, got {other}"),
    }
}

#[tokio::test]
async fn sasl_oauthbearer_bad_principal_fails() {
    let mock = common::Mock::start_with_oauthbearer("alice".into()).await;
    let mut pcfg = ProducerConfig::bootstrap([mock.addr.clone()]);
    pcfg.sasl_oauthbearer = Some("eve".into());
    let err = match Producer::new(pcfg).await {
        Err(e) => e,
        Ok(_) => panic!("bad oauth principal must fail"),
    };
    match err {
        Error::Broker { code, .. } => assert_eq!(code, error::SASL_AUTHENTICATION_FAILED),
        other => panic!("expected broker SASL_AUTHENTICATION_FAILED, got {other}"),
    }
}

#[tokio::test]
async fn sasl_scram_bad_password_fails() {
    let mock = common::Mock::start_with_scram(("alice".into(), "secret".into())).await;
    let mut pcfg = ProducerConfig::bootstrap([mock.addr.clone()]);
    pcfg.sasl_scram = Some(("alice".into(), "wrong".into()));
    let err = match Producer::new(pcfg).await {
        Err(e) => e,
        Ok(_) => panic!("bad scram password must fail"),
    };
    match err {
        Error::Broker { code, .. } => assert_eq!(code, error::SASL_AUTHENTICATION_FAILED),
        other => panic!("expected broker SASL_AUTHENTICATION_FAILED, got {other}"),
    }
}

#[tokio::test]
async fn sasl_plain_then_produce() {
    let mock = common::Mock::start_with_sasl(Some(("alice".into(), "secret".into()))).await;
    let mut pcfg = ProducerConfig::bootstrap([mock.addr.clone()]);
    pcfg.linger = Duration::ZERO;
    pcfg.sasl_plain = Some(("alice".into(), "secret".into()));
    let producer = Producer::new(pcfg).await.unwrap();
    assert_eq!(
        mock.last_sasl_handshake_version(),
        Some(1),
        "Producer must prefer SaslHandshake v1 when the broker advertises it"
    );
    assert_eq!(
        mock.last_sasl_authenticate_version(),
        Some(2),
        "Producer must prefer SaslAuthenticate v2 when the broker advertises it"
    );
    let md = producer
        .send(ProduceRecord::to("t").value(&b"sasl-ok"[..]))
        .await
        .unwrap();
    assert_eq!(md.offset, 0);
    producer.close().await.unwrap();
}

#[tokio::test]
async fn sasl_authenticate_negotiates_v0_when_broker_caps() {
    let mock = common::Mock::start_with_sasl(Some(("alice".into(), "secret".into()))).await;
    mock.set_api_max(SASL_AUTHENTICATE, 0);
    let mut pcfg = ProducerConfig::bootstrap([mock.addr.clone()]);
    pcfg.linger = Duration::ZERO;
    pcfg.sasl_plain = Some(("alice".into(), "secret".into()));
    let producer = Producer::new(pcfg).await.unwrap();
    assert_eq!(
        mock.last_sasl_handshake_version(),
        Some(1),
        "handshake stays v1 when only authenticate is capped"
    );
    assert_eq!(
        mock.last_sasl_authenticate_version(),
        Some(0),
        "client must speak SaslAuthenticate v0 when the broker max is 0"
    );
    producer.close().await.unwrap();
}

#[tokio::test]
async fn sasl_authenticate_negotiates_v1_when_broker_caps() {
    let mock = common::Mock::start_with_sasl(Some(("alice".into(), "secret".into()))).await;
    mock.set_api_max(SASL_AUTHENTICATE, 1);
    let mut pcfg = ProducerConfig::bootstrap([mock.addr.clone()]);
    pcfg.linger = Duration::ZERO;
    pcfg.sasl_plain = Some(("alice".into(), "secret".into()));
    let producer = Producer::new(pcfg).await.unwrap();
    assert_eq!(
        mock.last_sasl_authenticate_version(),
        Some(1),
        "client must speak SaslAuthenticate v1 when the broker max is 1"
    );
    producer.close().await.unwrap();
}

#[tokio::test]
async fn sasl_handshake_negotiates_v0_when_broker_caps() {
    let mock = common::Mock::start_with_sasl(Some(("alice".into(), "secret".into()))).await;
    mock.set_api_max(SASL_HANDSHAKE, 0);
    let mut pcfg = ProducerConfig::bootstrap([mock.addr.clone()]);
    pcfg.linger = Duration::ZERO;
    pcfg.sasl_plain = Some(("alice".into(), "secret".into()));
    let producer = Producer::new(pcfg).await.unwrap();
    assert_eq!(
        mock.last_sasl_handshake_version(),
        Some(0),
        "client must speak SaslHandshake v0 when the broker max is 0"
    );
    producer.close().await.unwrap();
}

#[tokio::test]
async fn two_members_range_partition_all() {
    let mock = common::Mock::start().await;
    let mut admin = Admin::new(AdminConfig::bootstrap([mock.addr.clone()]))
        .await
        .unwrap();
    let created = admin
        .create_topics(&[NewTopic::new("g4", 4, 1)], 10_000, false)
        .await
        .unwrap();
    assert_eq!(created[0].error_code, 0);

    let mut ccfg = ConsumerConfig::bootstrap([mock.addr.clone()]);
    ccfg.max_wait_ms = 10;
    let mut a = ConsumerGroup::join(ccfg.clone(), "rg", "g4").await.unwrap();
    assert_eq!(a.assignment().len(), 4, "solo member gets every partition");

    let b_join = tokio::spawn({
        let ccfg = ccfg.clone();
        async move { ConsumerGroup::join(ccfg, "rg", "g4").await }
    });
    tokio::time::sleep(Duration::from_millis(350)).await;
    drop(a.poll().await);
    let mut b = b_join.await.unwrap().unwrap();
    let a_parts: std::collections::HashSet<i32> =
        a.assignment().iter().map(|tp| tp.partition).collect();
    let b_parts: std::collections::HashSet<i32> =
        b.assignment().iter().map(|tp| tp.partition).collect();
    assert!(a_parts.is_disjoint(&b_parts), "range must not overlap");
    let union: std::collections::HashSet<i32> = a_parts.union(&b_parts).copied().collect();
    assert_eq!(union.len(), 4, "union of assignments is all partitions");
    tokio::time::sleep(Duration::from_millis(350)).await;
    assert!(
        mock.heartbeat_total("rg") >= 2,
        "heartbeat loop must run after join, got {}",
        mock.heartbeat_total("rg")
    );

    a.leave().await.unwrap();
    tokio::time::sleep(Duration::from_millis(350)).await;
    drop(b.poll().await);
    assert_eq!(
        b.assignment().len(),
        4,
        "remaining member covers all partitions after leave"
    );
}

#[tokio::test]
async fn two_members_sticky_partition_all() {
    let mock = common::Mock::start().await;
    let mut admin = Admin::new(AdminConfig::bootstrap([mock.addr.clone()]))
        .await
        .unwrap();
    assert_eq!(
        admin
            .create_topics(&[NewTopic::new("s4", 4, 1)], 10_000, false)
            .await
            .unwrap()[0]
            .error_code,
        0
    );
    let mut ccfg = ConsumerConfig::bootstrap([mock.addr.clone()]);
    ccfg.max_wait_ms = 10;
    let mut a = ConsumerGroup::join_sticky(ccfg.clone(), "sg", "s4")
        .await
        .unwrap();
    let b_join = tokio::spawn({
        let ccfg = ccfg.clone();
        async move { ConsumerGroup::join_sticky(ccfg, "sg", "s4").await }
    });
    tokio::time::sleep(Duration::from_millis(350)).await;
    drop(a.poll().await);
    let b = b_join.await.unwrap().unwrap();
    let a_parts: std::collections::HashSet<i32> =
        a.assignment().iter().map(|tp| tp.partition).collect();
    let b_parts: std::collections::HashSet<i32> =
        b.assignment().iter().map(|tp| tp.partition).collect();
    assert!(a_parts.is_disjoint(&b_parts));
    assert_eq!(a_parts.len(), 2);
    assert_eq!(b_parts.len(), 2);
}

#[tokio::test]
async fn consumer_group_join_fetch_commit() {
    let mock = common::Mock::start().await;
    let mut pcfg = ProducerConfig::bootstrap([mock.addr.clone()]);
    pcfg.linger = Duration::ZERO;
    let producer = Producer::new(pcfg).await.unwrap();
    producer
        .send(ProduceRecord::to("t").value(&b"grouped"[..]))
        .await
        .unwrap();
    producer.close().await.unwrap();

    let mut ccfg = ConsumerConfig::bootstrap([mock.addr.clone()]);
    ccfg.max_wait_ms = 10;
    let mut group = ConsumerGroup::join(ccfg, "g1", "t").await.unwrap();
    assert_eq!(mock.last_group_instance_id(), None);
    assert_eq!(
        mock.last_find_coordinator_version(),
        Some(6),
        "ConsumerGroup must prefer FindCoordinator v6 when the broker advertises it"
    );
    assert_eq!(
        mock.last_offset_fetch_version(),
        Some(9),
        "ConsumerGroup must prefer OffsetFetch v9 when the broker advertises it"
    );
    assert_eq!(
        mock.last_offset_fetch_require_stable(),
        Some(false),
        "ReadUncommitted OffsetFetch must send RequireStable false"
    );
    assert_eq!(
        mock.last_sync_group_version(),
        Some(5),
        "ConsumerGroup must prefer SyncGroup v5 when the broker advertises it"
    );
    assert_eq!(
        mock.last_join_group_version(),
        Some(9),
        "ConsumerGroup must prefer JoinGroup v9 when the broker advertises it"
    );
    assert_eq!(
        mock.last_join_group_reason(),
        None,
        "first JoinGroup must send a null Reason"
    );
    assert_eq!(
        mock.last_join_protocols_n(),
        Some(1),
        "join() must send JoinGroup Protocols of 1"
    );
    let recs = group.poll().await.unwrap();
    assert_eq!(recs.len(), 1);
    assert_eq!(recs[0].value.as_deref(), Some(&b"grouped"[..]));
    group.commit().await.unwrap();
    assert_eq!(
        mock.last_offset_commit_version(),
        Some(9),
        "ConsumerGroup must prefer OffsetCommit v9 when the broker advertises it"
    );
}

#[tokio::test]
async fn join_with_assignors_sends_protocols_of_n() {
    let mock = common::Mock::start().await;
    let mut pcfg = ProducerConfig::bootstrap([mock.addr.clone()]);
    pcfg.linger = Duration::ZERO;
    let producer = Producer::new(pcfg).await.unwrap();
    producer
        .send(ProduceRecord::to("t").value(&b"assignors"[..]))
        .await
        .unwrap();
    producer.close().await.unwrap();

    let mut ccfg = ConsumerConfig::bootstrap([mock.addr.clone()]);
    ccfg.max_wait_ms = 10;
    let mut group = ConsumerGroup::join_with_assignors(
        ccfg,
        "g-assignors",
        "t",
        ["range", "cooperative-sticky"],
    )
    .await
    .unwrap();
    assert_eq!(
        mock.last_join_protocols_n(),
        Some(2),
        "join_with_assignors must send JoinGroup Protocols of N"
    );
    let recs = group.poll().await.unwrap();
    assert_eq!(recs.len(), 1);
    assert_eq!(recs[0].value.as_deref(), Some(&b"assignors"[..]));
    group.leave().await.unwrap();
}

#[tokio::test]
async fn find_coordinator_negotiates_below_v6_when_broker_caps() {
    let mock = common::Mock::start().await;
    mock.set_api_max(FIND_COORDINATOR, 4);
    let mut ccfg = ConsumerConfig::bootstrap([mock.addr.clone()]);
    ccfg.max_wait_ms = 10;
    let _group = ConsumerGroup::join(ccfg, "g4", "t").await.unwrap();
    assert_eq!(
        mock.last_find_coordinator_version(),
        Some(4),
        "client must speak FindCoordinator v4 when the broker max is 4"
    );

    let mock = common::Mock::start().await;
    mock.set_api_max(FIND_COORDINATOR, 3);
    let mut ccfg = ConsumerConfig::bootstrap([mock.addr.clone()]);
    ccfg.max_wait_ms = 10;
    let _group = ConsumerGroup::join(ccfg, "g3", "t").await.unwrap();
    assert_eq!(
        mock.last_find_coordinator_version(),
        Some(3),
        "client must speak FindCoordinator v3 when the broker max is 3"
    );
}

#[tokio::test]
async fn offset_commit_negotiates_below_v9_when_broker_caps() {
    let mock = common::Mock::start().await;
    mock.set_api_max(OFFSET_COMMIT, 8);
    let mut ccfg = ConsumerConfig::bootstrap([mock.addr.clone()]);
    ccfg.max_wait_ms = 10;
    let mut group = ConsumerGroup::join(ccfg, "oc8", "t").await.unwrap();
    group.commit().await.unwrap();
    assert_eq!(
        mock.last_offset_commit_version(),
        Some(8),
        "client must speak OffsetCommit v8 when the broker max is 8"
    );
    group.leave().await.unwrap();

    let mock = common::Mock::start().await;
    mock.set_api_max(OFFSET_COMMIT, 7);
    let mut ccfg = ConsumerConfig::bootstrap([mock.addr.clone()]);
    ccfg.max_wait_ms = 10;
    let mut group = ConsumerGroup::join(ccfg, "oc7", "t").await.unwrap();
    group.commit().await.unwrap();
    assert_eq!(
        mock.last_offset_commit_version(),
        Some(7),
        "client must speak OffsetCommit v7 when the broker max is 7"
    );
    group.leave().await.unwrap();

    let mock = common::Mock::start().await;
    mock.set_api_max(OFFSET_COMMIT, 6);
    let mut ccfg = ConsumerConfig::bootstrap([mock.addr.clone()]);
    ccfg.max_wait_ms = 10;
    ccfg.group_instance_id = Some("worker-oc6".into());
    let mut group = ConsumerGroup::join(ccfg, "oc6", "t").await.unwrap();
    group.commit().await.unwrap();
    assert_eq!(
        mock.last_offset_commit_version(),
        Some(6),
        "client must speak OffsetCommit v6 when the broker max is 6"
    );
    group.leave().await.unwrap();

    let mock = common::Mock::start().await;
    mock.set_api_max(OFFSET_COMMIT, 5);
    let mut ccfg = ConsumerConfig::bootstrap([mock.addr.clone()]);
    ccfg.max_wait_ms = 10;
    let mut group = ConsumerGroup::join(ccfg, "oc5", "t").await.unwrap();
    group.commit().await.unwrap();
    assert_eq!(
        mock.last_offset_commit_version(),
        Some(5),
        "client must speak OffsetCommit v5 when the broker max is 5"
    );
    group.leave().await.unwrap();

    let mock = common::Mock::start().await;
    mock.set_api_max(OFFSET_COMMIT, 2);
    let mut ccfg = ConsumerConfig::bootstrap([mock.addr.clone()]);
    ccfg.max_wait_ms = 10;
    let mut group = ConsumerGroup::join(ccfg, "oc2", "t").await.unwrap();
    group.commit().await.unwrap();
    assert_eq!(
        mock.last_offset_commit_version(),
        Some(2),
        "client must speak OffsetCommit v2 when the broker max is 2"
    );
    group.leave().await.unwrap();
}

#[tokio::test]
async fn offset_fetch_negotiates_below_v9_when_broker_caps() {
    let mock = common::Mock::start().await;
    mock.set_api_max(OFFSET_FETCH, 7);
    let mut ccfg = ConsumerConfig::bootstrap([mock.addr.clone()]);
    ccfg.max_wait_ms = 10;
    let _group = ConsumerGroup::join(ccfg, "of7", "t").await.unwrap();
    assert_eq!(
        mock.last_offset_fetch_version(),
        Some(7),
        "client must speak OffsetFetch v7 when the broker max is 7"
    );

    let mock = common::Mock::start().await;
    mock.set_api_max(OFFSET_FETCH, 5);
    let mut ccfg = ConsumerConfig::bootstrap([mock.addr.clone()]);
    ccfg.max_wait_ms = 10;
    let _group = ConsumerGroup::join(ccfg, "of5", "t").await.unwrap();
    assert_eq!(
        mock.last_offset_fetch_version(),
        Some(5),
        "client must speak OffsetFetch v5 when the broker max is 5"
    );

    let mock = common::Mock::start().await;
    mock.set_api_max(OFFSET_FETCH, 4);
    let mut ccfg = ConsumerConfig::bootstrap([mock.addr.clone()]);
    ccfg.max_wait_ms = 10;
    let _group = ConsumerGroup::join(ccfg, "of4", "t").await.unwrap();
    assert_eq!(
        mock.last_offset_fetch_version(),
        Some(4),
        "client must speak OffsetFetch v4 when the broker max is 4"
    );

    let mock = common::Mock::start().await;
    mock.set_api_max(OFFSET_FETCH, 2);
    let mut ccfg = ConsumerConfig::bootstrap([mock.addr.clone()]);
    ccfg.max_wait_ms = 10;
    let _group = ConsumerGroup::join(ccfg, "of2", "t").await.unwrap();
    assert_eq!(
        mock.last_offset_fetch_version(),
        Some(2),
        "client must speak OffsetFetch v2 when the broker max is 2"
    );

    let mock = common::Mock::start().await;
    mock.set_api_max(OFFSET_FETCH, 1);
    let mut ccfg = ConsumerConfig::bootstrap([mock.addr.clone()]);
    ccfg.max_wait_ms = 10;
    let _group = ConsumerGroup::join(ccfg, "of1", "t").await.unwrap();
    assert_eq!(
        mock.last_offset_fetch_version(),
        Some(1),
        "client must speak OffsetFetch v1 when the broker max is 1"
    );
}

#[tokio::test]
async fn offset_fetch_read_committed_sets_require_stable() {
    let mock = common::Mock::start().await;
    let mut ccfg = ConsumerConfig::bootstrap([mock.addr.clone()]);
    ccfg.max_wait_ms = 10;
    ccfg.isolation_level = IsolationLevel::ReadCommitted;
    let _group = ConsumerGroup::join(ccfg, "of-rs", "t").await.unwrap();
    assert_eq!(
        mock.last_offset_fetch_require_stable(),
        Some(true),
        "ReadCommitted OffsetFetch must send RequireStable true"
    );
    assert_eq!(
        mock.last_offset_fetch_null_topics(),
        Some(false),
        "group assign OffsetFetch names assigned partitions"
    );
    assert_eq!(
        mock.last_offset_fetch_group_count(),
        1,
        "classic group assign OffsetFetch is one group"
    );
}

#[tokio::test]
async fn admin_list_consumer_group_offsets_for_groups_falls_back_below_v8() {
    let mock = common::Mock::start().await;
    mock.set_api_max(OFFSET_FETCH, 7);
    let mut admin = Admin::new(AdminConfig::bootstrap([mock.addr.clone()]))
        .await
        .unwrap();
    admin
        .alter_consumer_group_offsets(
            "g1",
            [(TopicPartition::new("t", 0), OffsetAndMetadata::new(1))],
        )
        .await
        .unwrap();
    admin
        .alter_consumer_group_offsets(
            "g2",
            [(TopicPartition::new("t", 0), OffsetAndMetadata::new(2))],
        )
        .await
        .unwrap();
    let before = mock.offset_fetch_calls();
    let listed = admin
        .list_consumer_group_offsets_for_groups([
            (
                "g1",
                ListConsumerGroupOffsetsSpec::topic_partitions([TopicPartition::new("t", 0)]),
            ),
            (
                "g2",
                ListConsumerGroupOffsetsSpec::topic_partitions([TopicPartition::new("t", 0)]),
            ),
        ])
        .await
        .unwrap();
    assert_eq!(listed.len(), 2);
    assert_eq!(listed[0].1[0].1.offset, 1);
    assert_eq!(listed[1].1[0].1.offset, 2);
    assert_eq!(
        mock.last_offset_fetch_version(),
        Some(7),
        "OffsetFetch max 7 must not speak Groups"
    );
    assert_eq!(
        mock.last_offset_fetch_group_count(),
        1,
        "v1–v7 fallback is one group per RPC"
    );
    assert_eq!(
        mock.offset_fetch_calls().saturating_sub(before),
        2,
        "v1–v7 must send one OffsetFetch per group"
    );
    admin.close().await.unwrap();
}

#[tokio::test]
async fn admin_list_consumer_group_offsets_for_groups_batches_find_coordinator() {
    let mock = common::Mock::start().await;
    let mut admin = Admin::new(AdminConfig::bootstrap([mock.addr.clone()]))
        .await
        .unwrap();
    admin
        .alter_consumer_group_offsets(
            "g1",
            [(TopicPartition::new("t", 0), OffsetAndMetadata::new(1))],
        )
        .await
        .unwrap();
    admin
        .alter_consumer_group_offsets(
            "g2",
            [(TopicPartition::new("t", 0), OffsetAndMetadata::new(2))],
        )
        .await
        .unwrap();
    let before_find = mock.find_coordinator_calls();
    let before_fetch = mock.offset_fetch_calls();
    let listed = admin
        .list_consumer_group_offsets_for_groups([
            (
                "g1",
                ListConsumerGroupOffsetsSpec::topic_partitions([TopicPartition::new("t", 0)]),
            ),
            (
                "g2",
                ListConsumerGroupOffsetsSpec::topic_partitions([TopicPartition::new("t", 0)]),
            ),
        ])
        .await
        .unwrap();
    assert_eq!(listed.len(), 2);
    assert_eq!(
        mock.last_find_coordinator_key_count(),
        2,
        "listConsumerGroupOffsets(Map) must send CoordinatorKeys of N on v4+"
    );
    assert_eq!(
        mock.find_coordinator_calls().saturating_sub(before_find),
        1,
        "groups that share a coordinator must be one FindCoordinator"
    );
    assert_eq!(
        mock.offset_fetch_calls().saturating_sub(before_fetch),
        1,
        "groups that share a coordinator must be one OffsetFetch"
    );
    admin.close().await.unwrap();

    let mock = common::Mock::start().await;
    mock.set_api_max(FIND_COORDINATOR, 3);
    let mut admin = Admin::new(AdminConfig::bootstrap([mock.addr.clone()]))
        .await
        .unwrap();
    admin
        .alter_consumer_group_offsets(
            "g1",
            [(TopicPartition::new("t", 0), OffsetAndMetadata::new(1))],
        )
        .await
        .unwrap();
    admin
        .alter_consumer_group_offsets(
            "g2",
            [(TopicPartition::new("t", 0), OffsetAndMetadata::new(2))],
        )
        .await
        .unwrap();
    let before_find = mock.find_coordinator_calls();
    let before_fetch = mock.offset_fetch_calls();
    let listed = admin
        .list_consumer_group_offsets_for_groups([
            (
                "g1",
                ListConsumerGroupOffsetsSpec::topic_partitions([TopicPartition::new("t", 0)]),
            ),
            (
                "g2",
                ListConsumerGroupOffsetsSpec::topic_partitions([TopicPartition::new("t", 0)]),
            ),
        ])
        .await
        .unwrap();
    assert_eq!(listed.len(), 2);
    assert_eq!(
        mock.last_find_coordinator_version(),
        Some(3),
        "client must speak FindCoordinator v3 when the broker max is 3"
    );
    assert_eq!(
        mock.last_find_coordinator_key_count(),
        1,
        "FindCoordinator v1–v3 is one key per RPC"
    );
    assert_eq!(
        mock.find_coordinator_calls().saturating_sub(before_find),
        2,
        "v1–v3 must send one FindCoordinator per group"
    );
    assert_eq!(
        mock.last_offset_fetch_group_count(),
        2,
        "OffsetFetch v8+ Groups of N still batches when FindCoordinator is v3"
    );
    assert_eq!(
        mock.offset_fetch_calls().saturating_sub(before_fetch),
        1,
        "OffsetFetch v8+ is still one RPC per coordinator"
    );
    admin.close().await.unwrap();
}

#[tokio::test]
async fn heartbeat_negotiates_v4_when_broker_advertises() {
    let mock = common::Mock::start().await;
    let mut ccfg = ConsumerConfig::bootstrap([mock.addr.clone()]);
    ccfg.max_wait_ms = 10;
    let group = ConsumerGroup::join(ccfg, "hb4", "t").await.unwrap();
    common::wait_pred("classic Heartbeat v4", || mock.heartbeat_total("hb4") >= 1).await;
    assert_eq!(
        mock.last_heartbeat_version(),
        Some(4),
        "ConsumerGroup must prefer Heartbeat v4 when the broker advertises it"
    );
    group.leave().await.unwrap();
    assert_eq!(
        mock.last_leave_group_version(),
        Some(5),
        "ConsumerGroup must prefer LeaveGroup v5 when the broker advertises it"
    );
}

#[tokio::test]
async fn leave_group_negotiates_v0_when_broker_caps() {
    let mock = common::Mock::start().await;
    mock.set_api_max(LEAVE_GROUP, 0);
    let mut ccfg = ConsumerConfig::bootstrap([mock.addr.clone()]);
    ccfg.max_wait_ms = 10;
    let group = ConsumerGroup::join(ccfg, "lg0", "t").await.unwrap();
    group.leave().await.unwrap();
    assert_eq!(
        mock.last_leave_group_version(),
        Some(0),
        "client must speak LeaveGroup v0 when the broker max is 0"
    );
}

#[tokio::test]
async fn leave_group_negotiates_v3_when_broker_caps() {
    let mock = common::Mock::start().await;
    mock.set_api_max(LEAVE_GROUP, 3);
    let mut ccfg = ConsumerConfig::bootstrap([mock.addr.clone()]);
    ccfg.max_wait_ms = 10;
    ccfg.group_instance_id = Some("worker-lg3".into());
    let group = ConsumerGroup::join(ccfg, "lg3", "t").await.unwrap();
    group.leave().await.unwrap();
    assert_eq!(
        mock.last_leave_group_version(),
        Some(3),
        "client must speak LeaveGroup v3 when the broker max is 3"
    );
    let members = mock.last_leave_group_members().expect("LeaveGroup members");
    assert_eq!(members[0].group_instance_id.as_deref(), Some("worker-lg3"));
    assert_eq!(members[0].reason, None, "v3 omits Reason");
}

#[tokio::test]
async fn heartbeat_negotiates_below_v4_when_broker_caps() {
    let mock = common::Mock::start().await;
    mock.set_api_max(HEARTBEAT, 3);
    let mut ccfg = ConsumerConfig::bootstrap([mock.addr.clone()]);
    ccfg.max_wait_ms = 10;
    let group = ConsumerGroup::join(ccfg, "hb3", "t").await.unwrap();
    common::wait_pred("classic Heartbeat v3", || mock.heartbeat_total("hb3") >= 1).await;
    assert_eq!(
        mock.last_heartbeat_version(),
        Some(3),
        "client must speak Heartbeat v3 when the broker max is 3"
    );
    group.leave().await.unwrap();
}

#[tokio::test]
async fn heartbeat_negotiates_v0_when_broker_caps() {
    let mock = common::Mock::start().await;
    mock.set_api_max(HEARTBEAT, 0);
    let mut ccfg = ConsumerConfig::bootstrap([mock.addr.clone()]);
    ccfg.max_wait_ms = 10;
    let group = ConsumerGroup::join(ccfg, "hb0", "t").await.unwrap();
    common::wait_pred("classic Heartbeat v0", || mock.heartbeat_total("hb0") >= 1).await;
    assert_eq!(
        mock.last_heartbeat_version(),
        Some(0),
        "client must speak Heartbeat v0 when the broker max is 0"
    );
    group.leave().await.unwrap();
}

#[tokio::test]
async fn heartbeat_negotiates_v2_when_broker_caps() {
    let mock = common::Mock::start().await;
    mock.set_api_max(HEARTBEAT, 2);
    let mut ccfg = ConsumerConfig::bootstrap([mock.addr.clone()]);
    ccfg.max_wait_ms = 10;
    ccfg.group_instance_id = Some("worker-hb2".into());
    let group = ConsumerGroup::join(ccfg, "hb2", "t").await.unwrap();
    common::wait_pred("classic Heartbeat v2", || mock.heartbeat_total("hb2") >= 1).await;
    assert_eq!(
        mock.last_heartbeat_version(),
        Some(2),
        "client must speak Heartbeat v2 when the broker max is 2"
    );
    group.leave().await.unwrap();
}

#[tokio::test]
async fn sync_group_negotiates_below_v5_when_broker_caps() {
    let mock = common::Mock::start().await;
    mock.set_api_max(SYNC_GROUP, 4);
    let mut ccfg = ConsumerConfig::bootstrap([mock.addr.clone()]);
    ccfg.max_wait_ms = 10;
    let _group = ConsumerGroup::join(ccfg, "sg4", "t").await.unwrap();
    assert_eq!(
        mock.last_sync_group_version(),
        Some(4),
        "client must speak SyncGroup v4 when the broker max is 4"
    );

    let mock = common::Mock::start().await;
    mock.set_api_max(SYNC_GROUP, 3);
    let mut ccfg = ConsumerConfig::bootstrap([mock.addr.clone()]);
    ccfg.max_wait_ms = 10;
    let _group = ConsumerGroup::join(ccfg, "sg3", "t").await.unwrap();
    assert_eq!(
        mock.last_sync_group_version(),
        Some(3),
        "client must speak SyncGroup v3 when the broker max is 3"
    );
}

#[tokio::test]
async fn sync_group_negotiates_v0_when_broker_caps() {
    let mock = common::Mock::start().await;
    mock.set_api_max(SYNC_GROUP, 0);
    let mut ccfg = ConsumerConfig::bootstrap([mock.addr.clone()]);
    ccfg.max_wait_ms = 10;
    let _group = ConsumerGroup::join(ccfg, "sg0", "t").await.unwrap();
    assert_eq!(
        mock.last_sync_group_version(),
        Some(0),
        "client must speak SyncGroup v0 when the broker max is 0"
    );
}

#[tokio::test]
async fn sync_group_negotiates_v2_when_broker_caps() {
    let mock = common::Mock::start().await;
    mock.set_api_max(SYNC_GROUP, 2);
    let mut ccfg = ConsumerConfig::bootstrap([mock.addr.clone()]);
    ccfg.max_wait_ms = 10;
    ccfg.group_instance_id = Some("worker-sg2".into());
    let _group = ConsumerGroup::join(ccfg, "sg2", "t").await.unwrap();
    assert_eq!(
        mock.last_sync_group_version(),
        Some(2),
        "client must speak SyncGroup v2 when the broker max is 2"
    );
}

#[tokio::test]
async fn join_group_negotiates_below_v9_when_broker_caps() {
    let mock = common::Mock::start().await;
    mock.set_api_max(JOIN_GROUP, 8);
    let mut ccfg = ConsumerConfig::bootstrap([mock.addr.clone()]);
    ccfg.max_wait_ms = 10;
    let _group = ConsumerGroup::join(ccfg, "jg8", "t").await.unwrap();
    assert_eq!(
        mock.last_join_group_version(),
        Some(8),
        "client must speak JoinGroup v8 when the broker max is 8"
    );

    let mock = common::Mock::start().await;
    mock.set_api_max(JOIN_GROUP, 6);
    let mut ccfg = ConsumerConfig::bootstrap([mock.addr.clone()]);
    ccfg.max_wait_ms = 10;
    let _group = ConsumerGroup::join(ccfg, "jg6", "t").await.unwrap();
    assert_eq!(
        mock.last_join_group_version(),
        Some(6),
        "client must speak JoinGroup v6 when the broker max is 6"
    );

    let mock = common::Mock::start().await;
    mock.set_api_max(JOIN_GROUP, 5);
    let mut ccfg = ConsumerConfig::bootstrap([mock.addr.clone()]);
    ccfg.max_wait_ms = 10;
    let _group = ConsumerGroup::join(ccfg, "jg5", "t").await.unwrap();
    assert_eq!(
        mock.last_join_group_version(),
        Some(5),
        "client must speak JoinGroup v5 when the broker max is 5"
    );

    let mock = common::Mock::start().await;
    mock.set_api_max(JOIN_GROUP, 4);
    let mut ccfg = ConsumerConfig::bootstrap([mock.addr.clone()]);
    ccfg.max_wait_ms = 10;
    ccfg.group_instance_id = Some("worker-jg4".into());
    let _group = ConsumerGroup::join(ccfg, "jg4", "t").await.unwrap();
    assert_eq!(
        mock.last_join_group_version(),
        Some(4),
        "client must speak JoinGroup v4 when the broker max is 4"
    );

    let mock = common::Mock::start().await;
    mock.set_api_max(JOIN_GROUP, 2);
    let mut ccfg = ConsumerConfig::bootstrap([mock.addr.clone()]);
    ccfg.max_wait_ms = 10;
    let _group = ConsumerGroup::join(ccfg, "jg2", "t").await.unwrap();
    assert_eq!(
        mock.last_join_group_version(),
        Some(2),
        "client must speak JoinGroup v2 when the broker max is 2"
    );
}

#[tokio::test]
async fn consumer_group_commit_one_rpc_then_resume() {
    let mock = common::Mock::start().await;
    let mut admin = Admin::new(AdminConfig::bootstrap([mock.addr.clone()]))
        .await
        .unwrap();
    assert_eq!(
        admin
            .create_topics(&[NewTopic::new("off3", 3, 1)], 10_000, false)
            .await
            .unwrap()[0]
            .error_code,
        0
    );

    let mut pcfg = ProducerConfig::bootstrap([mock.addr.clone()]);
    pcfg.linger = Duration::ZERO;
    let producer = Producer::new(pcfg).await.unwrap();
    for p in 0..3 {
        producer
            .send(
                ProduceRecord::to("off3")
                    .partition(p)
                    .value(format!("first-{p}").into_bytes()),
            )
            .await
            .unwrap();
    }
    producer.flush().await.unwrap();

    let mut ccfg = ConsumerConfig::bootstrap([mock.addr.clone()]);
    ccfg.max_wait_ms = 10;
    let mut group = ConsumerGroup::join(ccfg.clone(), "offg", "off3")
        .await
        .unwrap();
    assert_eq!(group.assignment().len(), 3);
    assert_eq!(
        mock.offset_fetch_calls(),
        1,
        "join OffsetFetch must be one RPC, got {}",
        mock.offset_fetch_calls()
    );
    assert_eq!(mock.last_offset_fetch_partitions(), 3);

    let first = group.poll().await.unwrap();
    assert_eq!(first.len(), 3);
    group.commit().await.unwrap();
    assert_eq!(
        mock.offset_commit_calls(),
        1,
        "commit must be one OffsetCommit, got {}",
        mock.offset_commit_calls()
    );
    assert_eq!(mock.last_offset_commit_partitions(), 3);
    group.leave().await.unwrap();

    for p in 0..3 {
        producer
            .send(
                ProduceRecord::to("off3")
                    .partition(p)
                    .value(format!("second-{p}").into_bytes()),
            )
            .await
            .unwrap();
    }
    producer.close().await.unwrap();

    let mut group = ConsumerGroup::join(ccfg, "offg", "off3").await.unwrap();
    assert_eq!(
        mock.offset_fetch_calls(),
        2,
        "rejoin OffsetFetch must be one more RPC, got {}",
        mock.offset_fetch_calls()
    );
    assert_eq!(mock.last_offset_fetch_partitions(), 3);
    let second = group.poll().await.unwrap();
    let vals: Vec<Vec<u8>> = second
        .iter()
        .filter_map(|r| r.value.as_ref().map(|v| v.to_vec()))
        .collect();
    assert_eq!(second.len(), 3, "must not re-read committed records");
    for p in 0..3 {
        assert!(
            vals.iter().any(|v| v == format!("second-{p}").as_bytes()),
            "missing second-{p}"
        );
        assert!(
            vals.iter().all(|v| v != format!("first-{p}").as_bytes()),
            "replayed first-{p}"
        );
    }
    group.leave().await.unwrap();
}

#[tokio::test]
async fn kip848_join_resumes_committed_offset() {
    let mock = common::Mock::start().await;
    let mut pcfg = ProducerConfig::bootstrap([mock.addr.clone()]);
    pcfg.linger = Duration::ZERO;
    let producer = Producer::new(pcfg).await.unwrap();
    producer
        .send(ProduceRecord::to("t").value(&b"old"[..]))
        .await
        .unwrap();
    producer.flush().await.unwrap();

    let mut ccfg = ConsumerConfig::bootstrap([mock.addr.clone()]);
    ccfg.max_wait_ms = 10;
    let mut group = ConsumerGroup::join_consumer(ccfg.clone(), "g848-off", "t")
        .await
        .unwrap();
    let recs = group.poll().await.unwrap();
    assert_eq!(recs[0].value.as_deref(), Some(&b"old"[..]));
    group.commit().await.unwrap();
    assert_eq!(mock.offset_commit_calls(), 1);
    group.leave().await.unwrap();

    producer
        .send(ProduceRecord::to("t").value(&b"new"[..]))
        .await
        .unwrap();
    producer.close().await.unwrap();

    let mut group = ConsumerGroup::join_consumer(ccfg, "g848-off", "t")
        .await
        .unwrap();
    let recs = group.poll().await.unwrap();
    assert_eq!(
        recs.iter().map(|r| r.value.as_deref()).collect::<Vec<_>>(),
        vec![Some(&b"new"[..])],
        "KIP-848 rejoin must OffsetFetch committed offsets, not restart at 0"
    );
    group.leave().await.unwrap();
}

#[tokio::test]
async fn two_members_kip848_partition_all() {
    let mock = common::Mock::start().await;
    let mut admin = Admin::new(AdminConfig::bootstrap([mock.addr.clone()]))
        .await
        .unwrap();
    let created = admin
        .create_topics(&[NewTopic::new("k4", 4, 1)], 10_000, false)
        .await
        .unwrap();
    assert_eq!(created[0].error_code, 0);

    let mut pcfg = ProducerConfig::bootstrap([mock.addr.clone()]);
    pcfg.linger = Duration::ZERO;
    let producer = Producer::new(pcfg).await.unwrap();
    for p in 0..4 {
        producer
            .send(
                ProduceRecord::to("k4")
                    .partition(p)
                    .value(format!("first-{p}").into_bytes()),
            )
            .await
            .unwrap();
    }
    producer.flush().await.unwrap();

    let mut ccfg = ConsumerConfig::bootstrap([mock.addr.clone()]);
    ccfg.max_wait_ms = 10;
    let mut a = ConsumerGroup::join_consumer(ccfg.clone(), "kg", "k4")
        .await
        .unwrap();
    assert_eq!(
        a.assignment().len(),
        4,
        "solo KIP-848 member gets every partition"
    );
    let first = a.poll().await.unwrap();
    assert_eq!(first.len(), 4, "solo member reads every partition");
    let seen: std::collections::HashSet<(i32, i64)> =
        first.iter().map(|r| (r.partition, r.offset)).collect();

    let mut b = ConsumerGroup::join_consumer(ccfg, "kg", "k4")
        .await
        .unwrap();
    assert_eq!(
        b.assignment().len(),
        2,
        "second member gets a range slice, not every partition"
    );
    assert_eq!(
        mock.join_group_calls(),
        0,
        "KIP-848 two-member must not use JoinGroup"
    );

    let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
    loop {
        let recs = a.poll().await.unwrap();
        for r in recs {
            assert!(
                !seen.contains(&(r.partition, r.offset)),
                "kept partitions must not rewind to OffsetFetch 0 after revoke"
            );
        }
        if a.assignment().len() == 2 {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "first member never applied heartbeat assignment revoke"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    let a_parts: std::collections::HashSet<i32> =
        a.assignment().iter().map(|tp| tp.partition).collect();
    let b_parts: std::collections::HashSet<i32> =
        b.assignment().iter().map(|tp| tp.partition).collect();
    assert!(
        a_parts.is_disjoint(&b_parts),
        "KIP-848 assignment must not overlap"
    );
    let union: std::collections::HashSet<i32> = a_parts.union(&b_parts).copied().collect();
    assert_eq!(union.len(), 4, "union of assignments is all partitions");

    a.leave().await.unwrap();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
    loop {
        drop(b.poll().await);
        if b.assignment().len() == 4 {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "remaining member never covered all partitions after leave"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    b.leave().await.unwrap();
    producer.close().await.unwrap();
}

#[tokio::test]
async fn kip848_join_fetch_leave_without_classic_join() {
    let mock = common::Mock::start().await;
    let mut pcfg = ProducerConfig::bootstrap([mock.addr.clone()]);
    pcfg.linger = Duration::ZERO;
    let producer = Producer::new(pcfg).await.unwrap();
    producer
        .send(ProduceRecord::to("t").value(&b"kip848"[..]))
        .await
        .unwrap();
    producer.close().await.unwrap();

    let mut ccfg = ConsumerConfig::bootstrap([mock.addr.clone()]);
    ccfg.max_wait_ms = 10;
    let mut group = ConsumerGroup::join_consumer(ccfg, "g848", "t")
        .await
        .unwrap();
    let recs = group.poll().await.unwrap();
    assert_eq!(recs[0].value.as_deref(), Some(&b"kip848"[..]));
    assert!(
        mock.cg_heartbeat_calls() >= 1,
        "must speak ConsumerGroupHeartbeat, got {}",
        mock.cg_heartbeat_calls()
    );
    assert_eq!(
        mock.join_group_calls(),
        0,
        "KIP-848 path must not send JoinGroup"
    );
    assert_eq!(mock.last_group_instance_id(), None);
    assert_eq!(mock.last_group_rack(), None);
    group.leave().await.unwrap();
}

#[tokio::test]
async fn consumer_group_heartbeat_negotiates_v1_when_broker_advertises() {
    let mock = common::Mock::start().await;
    let mut ccfg = ConsumerConfig::bootstrap([mock.addr.clone()]);
    ccfg.max_wait_ms = 10;
    let group = ConsumerGroup::join_consumer(ccfg, "cgh1", "t")
        .await
        .unwrap();
    assert_eq!(
        mock.last_consumer_group_heartbeat_version(),
        Some(1),
        "ConsumerGroup must prefer ConsumerGroupHeartbeat v1 when the broker advertises it"
    );
    let join_id = mock
        .last_consumer_group_heartbeat_join_member_id()
        .expect("join member id");
    assert!(
        !join_id.is_empty(),
        "ConsumerGroupHeartbeat v1 must send a client-generated MemberId (KIP-1082), got {join_id:?}"
    );
    assert_eq!(group.member_id(), join_id);
    group.leave().await.unwrap();
}

#[tokio::test]
async fn consumer_group_heartbeat_negotiates_v0_when_broker_caps() {
    let mock = common::Mock::start().await;
    mock.set_api_max(CONSUMER_GROUP_HEARTBEAT, 0);
    let mut ccfg = ConsumerConfig::bootstrap([mock.addr.clone()]);
    ccfg.max_wait_ms = 10;
    let group = ConsumerGroup::join_consumer(ccfg, "cgh0", "t")
        .await
        .unwrap();
    assert_eq!(
        mock.last_consumer_group_heartbeat_version(),
        Some(0),
        "client must speak ConsumerGroupHeartbeat v0 when the broker max is 0"
    );
    assert_eq!(
        mock.last_consumer_group_heartbeat_join_member_id()
            .as_deref(),
        Some(""),
        "ConsumerGroupHeartbeat v0 join must send empty MemberId"
    );
    group.leave().await.unwrap();
}

#[tokio::test]
async fn share_group_join_leave_without_classic_or_kip848() {
    let mock = common::Mock::start().await;
    let mut ccfg = ConsumerConfig::bootstrap([mock.addr.clone()]);
    ccfg.max_wait_ms = 10;
    let group = ShareGroup::join(ccfg, "sg1", "t").await.unwrap();
    assert!(
        mock.share_heartbeat_calls() >= 1,
        "must speak ShareGroupHeartbeat, got {}",
        mock.share_heartbeat_calls()
    );
    assert_eq!(mock.join_group_calls(), 0);
    assert_eq!(mock.sync_group_calls(), 0);
    assert_eq!(mock.cg_heartbeat_calls(), 0);
    assert_eq!(
        mock.last_share_group_heartbeat_version(),
        Some(1),
        "ShareGroup must prefer ShareGroupHeartbeat v1 when the broker advertises it"
    );
    group.leave().await.unwrap();
}

#[tokio::test]
async fn share_group_heartbeat_negotiates_v0_when_broker_caps() {
    let mock = common::Mock::start().await;
    mock.set_api_max(SHARE_GROUP_HEARTBEAT, 0);
    let mut ccfg = ConsumerConfig::bootstrap([mock.addr.clone()]);
    ccfg.max_wait_ms = 10;
    let group = ShareGroup::join(ccfg, "sg0", "t").await.unwrap();
    assert_eq!(
        mock.last_share_group_heartbeat_version(),
        Some(0),
        "client must speak ShareGroupHeartbeat v0 when the broker max is 0"
    );
    group.leave().await.unwrap();
}

#[tokio::test]
async fn share_fetch_negotiates_v0_when_broker_caps() {
    let mock = common::Mock::start().await;
    mock.set_api_max(SHARE_FETCH, 0);
    let mut pcfg = ProducerConfig::bootstrap([mock.addr.clone()]);
    pcfg.linger = Duration::ZERO;
    let producer = Producer::new(pcfg).await.unwrap();
    producer
        .send(ProduceRecord::to("t").value(&b"share-v0"[..]))
        .await
        .unwrap();
    producer.close().await.unwrap();

    let mut ccfg = ConsumerConfig::bootstrap([mock.addr.clone()]);
    ccfg.max_wait_ms = 10;
    let mut g = ShareGroup::join(ccfg, "sg-sf0", "t").await.unwrap();
    let recs = g.poll().await.unwrap();
    assert_eq!(recs[0].value.as_deref(), Some(&b"share-v0"[..]));
    assert_eq!(
        mock.last_share_fetch_version(),
        Some(0),
        "client must speak ShareFetch v0 when the broker max is 0"
    );
    g.leave().await.unwrap();
}

#[tokio::test]
async fn share_acknowledge_negotiates_v0_when_broker_caps() {
    let mock = common::Mock::start().await;
    mock.set_api_max(SHARE_ACKNOWLEDGE, 0);
    let mut pcfg = ProducerConfig::bootstrap([mock.addr.clone()]);
    pcfg.linger = Duration::ZERO;
    let producer = Producer::new(pcfg).await.unwrap();
    producer
        .send(ProduceRecord::to("t").value(&b"share-ack0"[..]))
        .await
        .unwrap();
    producer.close().await.unwrap();

    let mut ccfg = ConsumerConfig::bootstrap([mock.addr.clone()]);
    ccfg.max_wait_ms = 10;
    let mut g = ShareGroup::join(ccfg, "sg-ack0", "t").await.unwrap();
    let recs = g.poll().await.unwrap();
    assert_eq!(recs[0].value.as_deref(), Some(&b"share-ack0"[..]));
    g.accept(&recs).await.unwrap();
    assert_eq!(
        mock.last_share_ack_version(),
        Some(0),
        "client must speak ShareAcknowledge v0 when the broker max is 0"
    );
    g.leave().await.unwrap();
}

#[tokio::test]
async fn share_fetch_accept_then_release() {
    let mock = common::Mock::start().await;
    let mut pcfg = ProducerConfig::bootstrap([mock.addr.clone()]);
    pcfg.linger = Duration::ZERO;
    let producer = Producer::new(pcfg).await.unwrap();
    producer
        .send(ProduceRecord::to("t").value(&b"share-a"[..]))
        .await
        .unwrap();
    producer.close().await.unwrap();

    let mut ccfg = ConsumerConfig::bootstrap([mock.addr.clone()]);
    ccfg.max_wait_ms = 10;
    let mut g = ShareGroup::join(ccfg, "sg-ack", "t").await.unwrap();
    let recs = g.poll().await.unwrap();
    assert!(mock.share_fetch_calls() >= 1);
    assert_eq!(
        mock.last_share_fetch_version(),
        Some(1),
        "ShareGroup must prefer ShareFetch v1 when the broker advertises it"
    );
    assert_eq!(recs[0].value.as_deref(), Some(&b"share-a"[..]));
    assert_eq!(recs[0].timestamp_type(), TimestampType::CreateTime);
    assert_eq!(recs[0].timestamp_type, TimestampType::CreateTime);
    assert!(recs[0].headers().is_empty());
    assert!(recs[0].headers.is_empty());
    assert!(recs[0].last_header("k").is_none());
    assert!(recs[0].to_string().starts_with("ConsumerRecord(topic = t"));
    let off = recs[0].offset;
    g.accept(&recs).await.unwrap();
    assert_eq!(
        mock.share_ack_calls(),
        1,
        "accept must be one ShareAcknowledge, not one RPC per record"
    );
    assert_eq!(mock.last_share_ack_epoch(), Some(1));
    assert_eq!(
        mock.last_share_ack_version(),
        Some(1),
        "ShareGroup must prefer ShareAcknowledge v1 when the broker advertises it"
    );
    let again = g.poll().await.unwrap();
    assert!(
        again.iter().all(|r| r.offset != off),
        "accepted record must not be redelivered, got {again:?}"
    );

    let mut pcfg = ProducerConfig::bootstrap([mock.addr.clone()]);
    pcfg.linger = Duration::ZERO;
    let producer = Producer::new(pcfg).await.unwrap();
    producer
        .send(ProduceRecord::to("t").value(&b"share-b"[..]))
        .await
        .unwrap();
    producer.close().await.unwrap();
    let recs = g.poll().await.unwrap();
    let b = recs
        .iter()
        .find(|r| r.value.as_deref() == Some(&b"share-b"[..]))
        .expect("acquired share-b");
    let off_b = b.offset;
    g.release(&recs).await.unwrap();
    let recs = g.poll().await.unwrap();
    assert!(
        recs.iter().any(|r| r.offset == off_b),
        "released record must be acquirable again, got {recs:?}"
    );
    g.leave().await.unwrap();
}

#[tokio::test]
async fn share_two_members_same_partition() {
    let mock = common::Mock::start().await;
    let mut pcfg = ProducerConfig::bootstrap([mock.addr.clone()]);
    pcfg.linger = Duration::ZERO;
    let producer = Producer::new(pcfg).await.unwrap();
    for i in 0..20u8 {
        producer
            .send(ProduceRecord::to("t").value(vec![i]))
            .await
            .unwrap();
    }
    producer.close().await.unwrap();

    let mut c1 = ConsumerConfig::bootstrap([mock.addr.clone()]);
    c1.max_wait_ms = 10;
    let mut g1 = ShareGroup::join(c1, "sg-two", "t").await.unwrap();
    let mut c2 = ConsumerConfig::bootstrap([mock.addr.clone()]);
    c2.max_wait_ms = 10;
    let mut g2 = ShareGroup::join(c2, "sg-two", "t").await.unwrap();
    let r1 = g1.poll().await.unwrap();
    let r2 = g2.poll().await.unwrap();
    assert!(!r1.is_empty(), "member 1 must acquire a record");
    assert!(!r2.is_empty(), "member 2 must acquire a record");
    assert_eq!(r1[0].partition, r2[0].partition);
    assert_ne!(
        r1[0].offset, r2[0].offset,
        "members must not get the same acquired offset"
    );
    g1.leave().await.unwrap();
    g2.leave().await.unwrap();
}

#[tokio::test]
async fn share_accept_batches_one_rpc_and_advances_epoch() {
    let mock = common::Mock::start().await;
    let mut pcfg = ProducerConfig::bootstrap([mock.addr.clone()]);
    pcfg.linger = Duration::ZERO;
    let producer = Producer::new(pcfg).await.unwrap();
    for i in 0..5u8 {
        producer
            .send(ProduceRecord::to("t").value(vec![i]))
            .await
            .unwrap();
    }
    producer.close().await.unwrap();

    let mut ccfg = ConsumerConfig::bootstrap([mock.addr.clone()]);
    ccfg.max_wait_ms = 10;
    let mut g = ShareGroup::join(ccfg, "sg-batch", "t").await.unwrap();
    let recs = g.poll().await.unwrap();
    assert!(
        recs.len() >= 2,
        "need several acquired records, got {recs:?}"
    );
    assert_eq!(mock.last_share_fetch_epoch(), Some(0));
    let offs: Vec<i64> = recs.iter().map(|r| r.offset).collect();
    g.accept(&recs).await.unwrap();
    assert_eq!(mock.share_ack_calls(), 1);
    assert_eq!(mock.last_share_ack_epoch(), Some(1));
    assert_eq!(mock.last_share_ack_partitions(), 1);
    let again = g.poll().await.unwrap();
    assert_eq!(
        mock.last_share_fetch_epoch(),
        Some(2),
        "ShareAcknowledge must increment the share session epoch"
    );
    assert!(
        again.iter().all(|r| !offs.contains(&r.offset)),
        "batched accept must not redeliver, got {again:?}"
    );
    g.leave().await.unwrap();
    assert_eq!(mock.last_share_ack_epoch(), Some(-1));
}

#[tokio::test]
async fn share_reject_does_not_redeliver() {
    let mock = common::Mock::start().await;
    let mut pcfg = ProducerConfig::bootstrap([mock.addr.clone()]);
    pcfg.linger = Duration::ZERO;
    let producer = Producer::new(pcfg).await.unwrap();
    producer
        .send(ProduceRecord::to("t").value(&b"rej"[..]))
        .await
        .unwrap();
    producer.close().await.unwrap();

    let mut ccfg = ConsumerConfig::bootstrap([mock.addr.clone()]);
    ccfg.max_wait_ms = 10;
    let mut g = ShareGroup::join(ccfg, "sg-rej", "t").await.unwrap();
    let recs = g.poll().await.unwrap();
    let off = recs[0].offset;
    g.reject(&recs).await.unwrap();
    assert_eq!(mock.share_ack_calls(), 1);
    let again = g.poll().await.unwrap();
    assert!(
        again.iter().all(|r| r.offset != off),
        "rejected record must not be redelivered, got {again:?}"
    );
    g.leave().await.unwrap();
}

#[tokio::test]
async fn share_leave_without_poll_does_not_acknowledge() {
    let mock = common::Mock::start().await;
    let mut ccfg = ConsumerConfig::bootstrap([mock.addr.clone()]);
    ccfg.max_wait_ms = 10;
    let g = ShareGroup::join(ccfg, "sg-nopoll", "t").await.unwrap();
    g.leave().await.unwrap();
    assert_eq!(mock.share_ack_calls(), 0);
    assert_eq!(mock.last_share_ack_epoch(), None);
}

#[tokio::test]
async fn tls_classic_group_post_join_heartbeat() {
    let (mock, tls) = common::Mock::start_tls().await;
    let mut ccfg = ConsumerConfig::bootstrap([mock.addr.clone()]);
    ccfg.max_wait_ms = 10;
    ccfg.tls = Some(tls);
    let mut g = ConsumerGroup::join(ccfg, "tls-classic", "t").await.unwrap();
    common::wait_pred("classic TLS heartbeat", || {
        mock.heartbeat_total("tls-classic") >= 1
    })
    .await;
    let _recs = g.poll().await.unwrap();
    g.leave().await.unwrap();
}

#[tokio::test]
async fn tls_kip848_post_join_heartbeat() {
    let (mock, tls) = common::Mock::start_tls().await;
    let mut ccfg = ConsumerConfig::bootstrap([mock.addr.clone()]);
    ccfg.max_wait_ms = 10;
    ccfg.tls = Some(tls);
    let mut g = ConsumerGroup::join_consumer(ccfg, "tls-848", "t")
        .await
        .unwrap();
    let join_hbs = mock.cg_heartbeat_calls();
    assert!(join_hbs >= 1);
    common::wait_pred("KIP-848 TLS membership heartbeat", || {
        mock.cg_heartbeat_calls() > join_hbs
    })
    .await;
    let _recs = g.poll().await.unwrap();
    g.leave().await.unwrap();
}

#[tokio::test]
async fn tls_share_group_post_join_heartbeat() {
    let (mock, tls) = common::Mock::start_tls().await;
    let mut ccfg = ConsumerConfig::bootstrap([mock.addr.clone()]);
    ccfg.max_wait_ms = 10;
    ccfg.tls = Some(tls);
    let mut g = ShareGroup::join(ccfg, "tls-share", "t").await.unwrap();
    let join_hbs = mock.share_heartbeat_calls();
    assert!(join_hbs >= 1);
    common::wait_pred("share TLS membership heartbeat", || {
        mock.share_heartbeat_calls() > join_hbs
    })
    .await;
    let _recs = g.poll().await.unwrap();
    g.leave().await.unwrap();
}

#[tokio::test]
async fn sasl_classic_group_post_join_heartbeat() {
    let mock = common::Mock::start_with_sasl(Some(("alice".into(), "secret".into()))).await;
    let mut ccfg = ConsumerConfig::bootstrap([mock.addr.clone()]);
    ccfg.max_wait_ms = 10;
    ccfg.sasl_plain = Some(("alice".into(), "secret".into()));
    let mut g = ConsumerGroup::join(ccfg, "sasl-classic", "t")
        .await
        .unwrap();
    common::wait_pred("classic SASL heartbeat", || {
        mock.heartbeat_total("sasl-classic") >= 1
    })
    .await;
    let _recs = g.poll().await.unwrap();
    g.leave().await.unwrap();
}

#[tokio::test]
async fn sasl_kip848_post_join_heartbeat() {
    let mock = common::Mock::start_with_sasl(Some(("alice".into(), "secret".into()))).await;
    let mut ccfg = ConsumerConfig::bootstrap([mock.addr.clone()]);
    ccfg.max_wait_ms = 10;
    ccfg.sasl_plain = Some(("alice".into(), "secret".into()));
    let mut g = ConsumerGroup::join_consumer(ccfg, "sasl-848", "t")
        .await
        .unwrap();
    let join_hbs = mock.cg_heartbeat_calls();
    assert!(join_hbs >= 1);
    common::wait_pred("KIP-848 SASL membership heartbeat", || {
        mock.cg_heartbeat_calls() > join_hbs
    })
    .await;
    let _recs = g.poll().await.unwrap();
    g.leave().await.unwrap();
}

#[tokio::test]
async fn sasl_share_group_post_join_heartbeat() {
    let mock = common::Mock::start_with_sasl(Some(("alice".into(), "secret".into()))).await;
    let mut ccfg = ConsumerConfig::bootstrap([mock.addr.clone()]);
    ccfg.max_wait_ms = 10;
    ccfg.sasl_plain = Some(("alice".into(), "secret".into()));
    let mut g = ShareGroup::join(ccfg, "sasl-share", "t").await.unwrap();
    let join_hbs = mock.share_heartbeat_calls();
    assert!(join_hbs >= 1);
    common::wait_pred("share SASL membership heartbeat", || {
        mock.share_heartbeat_calls() > join_hbs
    })
    .await;
    let _recs = g.poll().await.unwrap();
    g.leave().await.unwrap();
}

#[tokio::test]
async fn classic_group_recovers_after_coordinator_drop() {
    let mock = common::Mock::start().await;
    let mut ccfg = ConsumerConfig::bootstrap([mock.addr.clone()]);
    ccfg.max_wait_ms = 10;
    let mut g = ConsumerGroup::join(ccfg, "re-classic", "t").await.unwrap();
    common::wait_pred("classic hb before drop", || {
        mock.heartbeat_total("re-classic") >= 1
    })
    .await;
    let before = mock.heartbeat_total("re-classic");
    mock.drop_connections();
    common::wait_pred("classic hb after reconnect", || {
        mock.heartbeat_total("re-classic") > before
    })
    .await;
    let _recs = g.poll().await.unwrap();
    g.leave().await.unwrap();
}

#[tokio::test]
async fn kip848_recovers_after_coordinator_drop() {
    let mock = common::Mock::start().await;
    let mut ccfg = ConsumerConfig::bootstrap([mock.addr.clone()]);
    ccfg.max_wait_ms = 10;
    let mut g = ConsumerGroup::join_consumer(ccfg, "re-848", "t")
        .await
        .unwrap();
    common::wait_pred("kip848 hb before drop", || mock.cg_heartbeat_calls() >= 2).await;
    let before = mock.cg_heartbeat_calls();
    mock.drop_connections();
    common::wait_pred("kip848 hb after reconnect", || {
        mock.cg_heartbeat_calls() > before
    })
    .await;
    let _recs = g.poll().await.unwrap();
    g.leave().await.unwrap();
}

#[tokio::test]
async fn share_group_recovers_after_coordinator_drop() {
    let mock = common::Mock::start().await;
    let mut ccfg = ConsumerConfig::bootstrap([mock.addr.clone()]);
    ccfg.max_wait_ms = 10;
    let mut g = ShareGroup::join(ccfg, "re-share", "t").await.unwrap();
    common::wait_pred("share hb before drop", || mock.share_heartbeat_calls() >= 2).await;
    let before = mock.share_heartbeat_calls();
    mock.drop_connections();
    common::wait_pred("share hb after reconnect", || {
        mock.share_heartbeat_calls() > before
    })
    .await;
    let _recs = g.poll().await.unwrap();
    g.leave().await.unwrap();
}

#[tokio::test]
async fn classic_group_follows_moved_coordinator() {
    let mock = common::Mock::start_two_node().await;
    let mut ccfg = ConsumerConfig::bootstrap([mock.addr.clone()]);
    ccfg.max_wait_ms = 10;
    let mut g = ConsumerGroup::join(ccfg, "mv-classic", "t").await.unwrap();
    common::wait_pred("classic hb on node 1", || {
        mock.membership_heartbeats_on(1) >= 1
    })
    .await;
    mock.move_coordinator();
    common::wait_pred("classic hb on node 2 after move", || {
        mock.membership_heartbeats_on(2) >= 1
    })
    .await;
    let _recs = g.poll().await.unwrap();
    g.leave().await.unwrap();
}

#[tokio::test]
async fn offset_commit_rediscovers_after_not_coordinator() {
    let mock = common::Mock::start_two_node().await;
    let mut pcfg = ProducerConfig::bootstrap([mock.addr.clone()]);
    pcfg.linger = Duration::ZERO;
    let producer = Producer::new(pcfg).await.unwrap();
    producer
        .send(ProduceRecord::to("t").value(&b"oc-nc"[..]))
        .await
        .unwrap();
    producer.close().await.unwrap();

    let mut ccfg = ConsumerConfig::bootstrap([mock.addr.clone()]);
    ccfg.max_wait_ms = 10;
    let mut g = ConsumerGroup::join(ccfg, "oc-nc", "t").await.unwrap();
    let recs = g.poll().await.unwrap();
    assert_eq!(recs[0].value.as_deref(), Some(&b"oc-nc"[..]));
    g.commit().await.unwrap();
    assert_eq!(
        mock.last_offset_commit_node(),
        Some(1),
        "first OffsetCommit must land on the group coordinator"
    );

    mock.move_coordinator();
    g.commit().await.unwrap();
    assert_eq!(
        mock.offset_commit_not_coordinator(),
        1,
        "stale coordinator must return NOT_COORDINATOR (16) on OffsetCommit"
    );
    assert_eq!(
        mock.last_offset_commit_node(),
        Some(2),
        "OffsetCommit must rediscover and land on the new coordinator"
    );
    g.leave().await.unwrap();
}

#[tokio::test]
async fn offset_commit_rediscovers_after_coordinator_load_in_progress() {
    let mock = common::Mock::start().await;
    let mut pcfg = ProducerConfig::bootstrap([mock.addr.clone()]);
    pcfg.linger = Duration::ZERO;
    let producer = Producer::new(pcfg).await.unwrap();
    producer
        .send(ProduceRecord::to("t").value(&b"oc-14"[..]))
        .await
        .unwrap();
    producer.close().await.unwrap();

    let mut ccfg = ConsumerConfig::bootstrap([mock.addr.clone()]);
    ccfg.max_wait_ms = 10;
    let mut g = ConsumerGroup::join(ccfg.clone(), "oc-14", "t")
        .await
        .unwrap();
    let recs = g.poll().await.unwrap();
    assert_eq!(recs[0].value.as_deref(), Some(&b"oc-14"[..]));
    mock.offset_commit_load_once();
    g.commit().await.unwrap();
    assert_eq!(
        mock.offset_commit_load_in_progress(),
        1,
        "coordinator must return COORDINATOR_LOAD_IN_PROGRESS (14) once"
    );
    assert_eq!(
        mock.last_offset_commit_node(),
        Some(1),
        "OffsetCommit must retry after 14 and land on the coordinator"
    );
    g.leave().await.unwrap();

    let mut g2 = ConsumerGroup::join(ccfg, "oc-14", "t").await.unwrap();
    let recs = g2.poll().await.unwrap();
    assert!(
        recs.is_empty(),
        "successful commit after 14 must store the offset"
    );
    g2.leave().await.unwrap();
}

#[tokio::test]
async fn kip848_follows_moved_coordinator() {
    let mock = common::Mock::start_two_node().await;
    let mut ccfg = ConsumerConfig::bootstrap([mock.addr.clone()]);
    ccfg.max_wait_ms = 10;
    let mut g = ConsumerGroup::join_consumer(ccfg, "mv-848", "t")
        .await
        .unwrap();
    common::wait_pred("kip848 hb on node 1", || {
        mock.membership_heartbeats_on(1) >= 1
    })
    .await;
    mock.move_coordinator();
    common::wait_pred("kip848 hb on node 2 after move", || {
        mock.membership_heartbeats_on(2) >= 1
    })
    .await;
    let _recs = g.poll().await.unwrap();
    g.leave().await.unwrap();
}

#[tokio::test]
async fn share_fetch_follows_partition_leader() {
    let mock = common::Mock::start_two_node().await;
    let mut pcfg = ProducerConfig::bootstrap([mock.addr.clone()]);
    pcfg.linger = Duration::ZERO;
    let producer = Producer::new(pcfg).await.unwrap();
    producer
        .send(ProduceRecord::to("t").value(&b"share-lead"[..]))
        .await
        .unwrap();
    producer.close().await.unwrap();

    let mut ccfg = ConsumerConfig::bootstrap([mock.addr.clone()]);
    ccfg.max_wait_ms = 10;
    let mut g = ShareGroup::join(ccfg, "sg-lead", "t").await.unwrap();
    common::wait_pred("share hb on coordinator 1", || {
        mock.membership_heartbeats_on(1) >= 1
    })
    .await;

    let recs = g.poll().await.unwrap();
    assert_eq!(recs[0].value.as_deref(), Some(&b"share-lead"[..]));
    assert_eq!(
        mock.last_share_fetch_node(),
        Some(2),
        "ShareFetch must land on the share-partition leader, not the coordinator"
    );
    assert_eq!(
        mock.membership_heartbeats_on(2),
        0,
        "ShareGroupHeartbeat must stay on the share coordinator"
    );
    g.accept(&recs).await.unwrap();
    assert_eq!(
        mock.last_share_ack_node(),
        Some(2),
        "ShareAcknowledge must land on the share-partition leader"
    );

    mock.set_partition_leader("t", 0, 1);
    let recs = g.poll().await.unwrap();
    assert!(
        recs.iter()
            .all(|r| r.value.as_deref() != Some(&b"share-lead"[..])),
        "accepted record must stay accepted after the leader hop"
    );
    assert_eq!(
        mock.share_fetch_not_leader(),
        1,
        "stale leader must return NOT_LEADER_OR_FOLLOWER (6) once"
    );
    assert_eq!(
        mock.last_share_fetch_node(),
        Some(1),
        "ShareFetch must follow Metadata after NOT_LEADER"
    );
    g.leave().await.unwrap();
}

#[tokio::test]
async fn share_group_follows_moved_coordinator() {
    let mock = common::Mock::start_two_node().await;
    let mut ccfg = ConsumerConfig::bootstrap([mock.addr.clone()]);
    ccfg.max_wait_ms = 10;
    let mut g = ShareGroup::join(ccfg, "mv-share", "t").await.unwrap();
    common::wait_pred("share hb on node 1", || {
        mock.membership_heartbeats_on(1) >= 1
    })
    .await;
    mock.move_coordinator();
    common::wait_pred("share hb on node 2 after move", || {
        mock.membership_heartbeats_on(2) >= 1
    })
    .await;
    let _recs = g.poll().await.unwrap();
    g.leave().await.unwrap();
}

#[tokio::test]
async fn producer_skips_dead_bootstrap() {
    let mock = common::Mock::start().await;
    let dead = common::closed_tcp_addr().await;
    let mut pcfg = ProducerConfig::bootstrap([dead, mock.addr.clone()]);
    pcfg.linger = Duration::ZERO;
    let producer = Producer::new(pcfg).await.unwrap();
    producer
        .send(ProduceRecord::to("t").value(&b"boot-p"[..]))
        .await
        .unwrap();
    producer.close().await.unwrap();
}

#[tokio::test]
async fn consumer_skips_dead_bootstrap() {
    let mock = common::Mock::start().await;
    let mut pcfg = ProducerConfig::bootstrap([mock.addr.clone()]);
    pcfg.linger = Duration::ZERO;
    let producer = Producer::new(pcfg).await.unwrap();
    producer
        .send(ProduceRecord::to("t").value(&b"boot-c"[..]))
        .await
        .unwrap();
    producer.close().await.unwrap();

    let dead = common::closed_tcp_addr().await;
    let mut ccfg = ConsumerConfig::bootstrap([dead, mock.addr.clone()]);
    ccfg.max_wait_ms = 10;
    let mut consumer = Consumer::new(ccfg).await.unwrap();
    consumer.assign("t", 0, 0).await.unwrap();
    let recs = consumer.fetch().await.unwrap();
    assert_eq!(recs[0].value.as_deref(), Some(&b"boot-c"[..]));
}

#[tokio::test]
async fn admin_skips_dead_bootstrap() {
    let mock = common::Mock::start().await;
    let dead = common::closed_tcp_addr().await;
    let mut admin = Admin::new(AdminConfig::bootstrap([dead, mock.addr.clone()]))
        .await
        .unwrap();
    let cluster = admin.describe_cluster().await.unwrap();
    assert!(
        !cluster.brokers.is_empty(),
        "admin RPC after bootstrap failover must return brokers, got {cluster:?}"
    );
}

#[tokio::test]
async fn admin_create_then_produce_fetch() {
    let mock = common::Mock::start().await;
    let mut admin = Admin::new(AdminConfig::bootstrap([mock.addr.clone()]))
        .await
        .unwrap();
    let created = admin
        .create_topics(&[NewTopic::new("orders", 3, 1)], 10_000, false)
        .await
        .unwrap();
    assert_eq!(created.len(), 1);
    assert_eq!(created[0].error_code, 0);
    assert_eq!(created[0].name, "orders");

    let mut pcfg = ProducerConfig::bootstrap([mock.addr.clone()]);
    pcfg.linger = Duration::ZERO;
    let producer = Producer::new(pcfg).await.unwrap();
    producer
        .send(
            ProduceRecord::to("orders")
                .value(&b"admin-hello"[..])
                .partition(1),
        )
        .await
        .unwrap();
    producer.close().await.unwrap();

    let mut ccfg = ConsumerConfig::bootstrap([mock.addr.clone()]);
    ccfg.max_wait_ms = 10;
    let mut consumer = Consumer::new(ccfg).await.unwrap();
    consumer.assign_topic("orders", 0).await.unwrap();
    assert_eq!(consumer.assignment().len(), 3);
    let recs = consumer.fetch().await.unwrap();
    assert_eq!(recs.len(), 1);
    assert_eq!(recs[0].partition, 1);
    assert_eq!(recs[0].value.as_deref(), Some(&b"admin-hello"[..]));
}

#[tokio::test]
async fn admin_create_duplicate_is_already_exists() {
    let mock = common::Mock::start().await;
    let mut admin = Admin::connect(mock.addr.clone()).await.unwrap();
    let dup = admin
        .create_topics(&[NewTopic::new("t", 1, 1)], 10_000, false)
        .await
        .unwrap();
    assert_eq!(dup[0].error_code, error::TOPIC_ALREADY_EXISTS);
}

#[tokio::test]
async fn admin_validate_only_does_not_create() {
    let mock = common::Mock::start().await;
    let mut admin = Admin::connect(mock.addr.clone()).await.unwrap();
    let created = admin
        .create_topics(&[NewTopic::new("ghost", 1, 1)], 10_000, true)
        .await
        .unwrap();
    assert_eq!(created[0].error_code, 0);

    let mut pcfg = ProducerConfig::bootstrap([mock.addr.clone()]);
    pcfg.linger = Duration::ZERO;
    let producer = Producer::new(pcfg).await.unwrap();
    let err = producer
        .send(ProduceRecord::to("ghost").value(&b"x"[..]))
        .await
        .expect_err("validate_only must not create the topic");
    match err {
        Error::UnknownTopic(t) => assert_eq!(t, "ghost"),
        other => panic!("expected UnknownTopic, got {other}"),
    }
    producer.close().await.unwrap();
}

#[tokio::test]
async fn admin_delete_and_describe() {
    let mock = common::Mock::start().await;
    let mut admin = Admin::connect(mock.addr.clone()).await.unwrap();
    let created = admin
        .create_topics(
            &[NewTopic::new("orders", 2, 1).config("cleanup.policy", "compact")],
            10_000,
            false,
        )
        .await
        .unwrap();
    assert_eq!(created[0].error_code, 0);

    let described = admin
        .describe_configs(
            &[ConfigResource::topic("orders").keys(["cleanup.policy"])],
            false,
        )
        .await
        .unwrap();
    assert_eq!(described[0].error_code, 0);
    let entry = described[0]
        .entries
        .iter()
        .find(|e| e.name == "cleanup.policy")
        .expect("cleanup.policy");
    assert_eq!(entry.value.as_deref(), Some("compact"));
    assert_eq!(
        mock.last_describe_configs_version(),
        Some(4),
        "Admin must prefer DescribeConfigs v4 when the broker advertises it"
    );
    assert_eq!(entry.config_type, partitionline::CONFIG_TYPE_STRING);
    assert_eq!(entry.config_type(), ConfigType::String);
    assert_eq!(entry.source(), ConfigSource::DynamicTopic);
    assert!(!entry.is_default());
    assert_eq!(entry.name(), "cleanup.policy");
    assert_eq!(entry.value(), Some("compact"));
    assert!(!entry.is_sensitive());
    assert!(entry.documentation().is_none());
    assert_eq!(entry.documentation, None);

    let resource = ConfigResource::topic("orders");
    assert!(!resource.is_default());
    assert_eq!(
        format!("{resource}"),
        "ConfigResource(type=TOPIC, name='orders')"
    );
    assert_eq!(
        format!("{entry}"),
        format!(
            "ConfigEntry(name={}, value={}, source={}, isSensitive={}, isReadOnly={}, synonyms=[], type={}, documentation=null)",
            entry.name(),
            entry.value().unwrap(),
            entry.source(),
            entry.is_sensitive(),
            entry.is_read_only(),
            entry.config_type(),
        )
    );

    let mapped = admin
        .create_topics(
            &[NewTopic::new("orders-map", 1, 1)
                .configs([("cleanup.policy", "compact"), ("retention.ms", "1000")])],
            10_000,
            false,
        )
        .await
        .unwrap();
    assert_eq!(mapped[0].error_code, 0);
    let mapped_cfg = admin
        .describe_configs(
            &[ConfigResource::topic("orders-map").keys(["cleanup.policy", "retention.ms"])],
            false,
        )
        .await
        .unwrap();
    assert_eq!(mapped_cfg[0].error_code, 0);
    let policy = mapped_cfg[0]
        .entries
        .iter()
        .find(|e| e.name == "cleanup.policy")
        .expect("cleanup.policy");
    assert_eq!(policy.value.as_deref(), Some("compact"));
    let retention = mapped_cfg[0]
        .entries
        .iter()
        .find(|e| e.name == "retention.ms")
        .expect("retention.ms");
    assert_eq!(retention.value.as_deref(), Some("1000"));

    let timed = admin
        .describe_configs_timeout(
            &[ConfigResource::topic("orders").keys(["cleanup.policy"])],
            false,
            Duration::from_secs(5),
        )
        .await
        .unwrap();
    assert_eq!(timed[0].error_code, 0);

    let missing = admin
        .describe_configs(&[ConfigResource::topic("nope")], false)
        .await
        .unwrap();
    assert_eq!(missing[0].error_code, error::UNKNOWN_TOPIC_OR_PARTITION);

    let deleted = admin.delete_topics(&["orders"], 10_000).await.unwrap();
    assert_eq!(deleted[0].error_code, 0);
    let gone = admin.delete_topics(&["orders"], 10_000).await.unwrap();
    assert_eq!(gone[0].error_code, error::UNKNOWN_TOPIC_OR_PARTITION);
}

#[tokio::test]
async fn admin_partitions_alter_configs_and_acls() {
    let mock = common::Mock::start().await;
    let mut admin = Admin::connect(mock.addr.clone()).await.unwrap();
    assert_eq!(
        admin
            .create_topics(&[NewTopic::new("acl-t", 1, 1)], 10_000, false)
            .await
            .unwrap()[0]
            .error_code,
        0
    );
    let parts = admin
        .create_partitions(&[NewPartitions::increase_to("acl-t", 3)], 10_000, false)
        .await
        .unwrap();
    assert_eq!(parts[0].error_code, 0);
    let err = admin
        .incremental_alter_configs(
            &ConfigResource::topic("acl-t"),
            &[AlterConfig::set("retention.ms", "1000")],
            false,
        )
        .await
        .unwrap();
    assert_eq!(err, 0);
    let err = admin
        .incremental_alter_configs_timeout(
            &ConfigResource::topic("acl-t"),
            &[AlterConfig::set("retention.ms", "1000")],
            Duration::from_secs(5),
            false,
        )
        .await
        .unwrap();
    assert_eq!(err, 0);
    let described = admin
        .describe_configs(
            &[ConfigResource::topic("acl-t").keys(["retention.ms"])],
            false,
        )
        .await
        .unwrap();
    assert_eq!(
        described[0]
            .entries
            .iter()
            .find(|e| e.name == "retention.ms")
            .and_then(|e| e.value.as_deref()),
        Some("1000")
    );
    let err = admin
        .incremental_alter_configs(
            &ConfigResource::topic("acl-t"),
            &[AlterConfig::set("cleanup.policy", "delete")],
            false,
        )
        .await
        .unwrap();
    assert_eq!(err, 0);
    let err = admin
        .incremental_alter_configs(
            &ConfigResource::topic("acl-t"),
            &[AlterConfig::append("cleanup.policy", "compact")],
            false,
        )
        .await
        .unwrap();
    assert_eq!(err, 0);
    let described = admin
        .describe_configs(
            &[ConfigResource::topic("acl-t").keys(["cleanup.policy"])],
            false,
        )
        .await
        .unwrap();
    assert_eq!(
        described[0]
            .entries
            .iter()
            .find(|e| e.name == "cleanup.policy")
            .and_then(|e| e.value.as_deref()),
        Some("delete,compact")
    );
    let err = admin
        .incremental_alter_configs(
            &ConfigResource::topic("acl-t"),
            &[AlterConfig::append("cleanup.policy", "compact")],
            false,
        )
        .await
        .unwrap();
    assert_eq!(err, 0);
    let described = admin
        .describe_configs(
            &[ConfigResource::topic("acl-t").keys(["cleanup.policy"])],
            false,
        )
        .await
        .unwrap();
    assert_eq!(
        described[0]
            .entries
            .iter()
            .find(|e| e.name == "cleanup.policy")
            .and_then(|e| e.value.as_deref()),
        Some("delete,compact"),
        "APPEND must not duplicate an existing LIST entry"
    );
    let err = admin
        .incremental_alter_configs(
            &ConfigResource::topic("acl-t"),
            &[AlterConfig::subtract("cleanup.policy", "delete")],
            false,
        )
        .await
        .unwrap();
    assert_eq!(err, 0);
    let described = admin
        .describe_configs(
            &[ConfigResource::topic("acl-t").keys(["cleanup.policy"])],
            false,
        )
        .await
        .unwrap();
    assert_eq!(
        described[0]
            .entries
            .iter()
            .find(|e| e.name == "cleanup.policy")
            .and_then(|e| e.value.as_deref()),
        Some("compact")
    );
    let created = admin
        .create_acls(&[AclBinding::allow_topic("acl-t", "User:alice")])
        .await
        .unwrap();
    assert_eq!(created, vec![0]);
    assert_eq!(
        mock.last_create_acls_version(),
        Some(3),
        "Admin must prefer CreateAcls v3 when the broker advertises it"
    );
    let listed = admin.describe_acls(AclResourceType::Topic).await.unwrap();
    assert_eq!(listed.len(), 1);
    assert_eq!(listed[0].principal, "User:alice");
    assert_eq!(listed[0].entry().principal(), "User:alice");
    assert_eq!(listed[0].pattern().name(), "acl-t");
    assert_eq!(listed[0].pattern().resource_type(), AclResourceType::Topic);
    assert!(!listed[0].is_unknown());
    assert_eq!(
        format!("{}", listed[0]),
        "(pattern=ResourcePattern(resourceType=TOPIC, name=acl-t, patternType=LITERAL), entry=(principal=User:alice, host=*, operation=ALL, permissionType=ALLOW))"
    );
    assert_eq!(
        format!("{}", AclBindingFilter::any()),
        "(patternFilter=ResourcePattern(resourceType=ANY, name= , patternType=ANY), entryFilter=(principal= , host= , operation=ANY, permissionType=ANY))"
    );
    assert!(listed[0].to_filter().matches(&listed[0]));
    assert_eq!(listed[0].pattern_type, partitionline::ACL_PATTERN_LITERAL);
    assert_eq!(
        mock.last_describe_acls_version(),
        Some(3),
        "Admin must prefer DescribeAcls v3 when the broker advertises it"
    );
    assert_eq!(admin.delete_acls(AclResourceType::Topic).await.unwrap(), 0);
    assert_eq!(
        mock.last_delete_acls_version(),
        Some(3),
        "Admin must prefer DeleteAcls v3 when the broker advertises it"
    );
    assert!(admin
        .describe_acls(AclResourceType::Topic)
        .await
        .unwrap()
        .is_empty());
    let created = admin
        .create_acls_timeout(
            &[AclBinding::allow_topic("acl-t", "User:alice")],
            Duration::from_secs(5),
        )
        .await
        .unwrap();
    assert_eq!(created, vec![0]);
    let listed = admin
        .describe_acls_timeout(AclResourceType::Topic, Duration::from_secs(5))
        .await
        .unwrap();
    assert_eq!(listed.len(), 1);
    assert_eq!(
        admin
            .delete_acls_timeout(AclResourceType::Topic, Duration::from_secs(5))
            .await
            .unwrap(),
        0
    );
}

#[tokio::test]
async fn admin_describe_delete_acls_with_filter() {
    let mock = common::Mock::start().await;
    let mut admin = Admin::connect(mock.addr.clone()).await.unwrap();
    let created = admin
        .create_acls(&[
            AclBinding::allow_topic("acl-f", "User:alice"),
            AclBinding::allow_topic("acl-f", "User:bob"),
        ])
        .await
        .unwrap();
    assert_eq!(created, vec![0, 0]);

    let all = admin.describe_acls_any().await.unwrap();
    assert_eq!(all.len(), 2);
    assert_eq!(
        mock.last_describe_acls_filter(),
        Some(AclBindingFilter::any()),
        "describeAcls(AclBindingFilter.ANY) sends ResourceType ANY"
    );
    let timed_any = admin
        .describe_acls_any_timeout(Duration::from_secs(5))
        .await
        .unwrap();
    assert_eq!(timed_any.len(), 2);

    let listed = admin
        .describe_acls_with(
            &AclBindingFilter::resource_type(AclResourceType::Topic).principal("User:alice"),
        )
        .await
        .unwrap();
    assert_eq!(listed.len(), 1);
    assert_eq!(listed[0].principal, "User:alice");
    let filter = mock.last_describe_acls_filter().unwrap();
    assert_eq!(filter.principal.as_deref(), Some("User:alice"));
    assert_eq!(filter.operation, partitionline::ACL_OPERATION_ANY);
    assert_eq!(filter.permission, partitionline::ACL_PERMISSION_ANY);

    let listed = admin
        .describe_acls_with_timeout(
            &AclBindingFilter::resource_type(AclResourceType::Topic).principal("User:alice"),
            Duration::from_secs(5),
        )
        .await
        .unwrap();
    assert_eq!(listed.len(), 1);

    let results = admin
        .delete_acls_with(&[
            AclBindingFilter::resource_type(AclResourceType::Topic).principal("User:alice"),
            AclBindingFilter::resource_type(AclResourceType::Topic).principal("User:bob"),
        ])
        .await
        .unwrap();
    assert_eq!(results.len(), 2);
    assert_eq!(results[0].error_code, 0);
    assert_eq!(results[0].matching.len(), 1);
    assert_eq!(results[1].matching.len(), 1);
    assert_eq!(mock.last_delete_acls_n(), Some(2));
    let empty = admin
        .delete_acls_with_timeout(&[], Duration::from_secs(5))
        .await
        .unwrap();
    assert!(empty.is_empty());
    assert_eq!(mock.last_delete_acls_n(), Some(2));
    assert!(admin
        .describe_acls(AclResourceType::Topic)
        .await
        .unwrap()
        .is_empty());
    admin.close().await.unwrap();
}

#[tokio::test]
async fn admin_alter_configs_delete_records_describe_cluster() {
    let mock = common::Mock::start().await;
    let mut admin = Admin::connect(mock.addr.clone()).await.unwrap();
    assert_eq!(
        admin
            .create_topics(&[NewTopic::new("rest", 1, 1)], 10_000, false)
            .await
            .unwrap()[0]
            .error_code,
        0
    );
    let err = admin
        .alter_configs(
            &ConfigResource::topic("rest"),
            &[("retention.ms".into(), Some("2000".into()))],
            false,
        )
        .await
        .unwrap();
    assert_eq!(err, 0);
    let err = admin
        .alter_configs_timeout(
            &ConfigResource::topic("rest"),
            &[("retention.ms".into(), Some("2000".into()))],
            Duration::from_secs(5),
            false,
        )
        .await
        .unwrap();
    assert_eq!(err, 0);
    assert_eq!(
        mock.last_alter_configs_version(),
        Some(2),
        "Admin must prefer AlterConfigs v2 when the broker advertises it"
    );
    let described = admin
        .describe_configs(
            &[ConfigResource::topic("rest").keys(["retention.ms"])],
            false,
        )
        .await
        .unwrap();
    assert_eq!(
        described[0]
            .entries
            .iter()
            .find(|e| e.name == "retention.ms")
            .and_then(|e| e.value.as_deref()),
        Some("2000")
    );

    let mut pcfg = ProducerConfig::bootstrap([mock.addr.clone()]);
    pcfg.linger = Duration::ZERO;
    let producer = Producer::new(pcfg).await.unwrap();
    let md0 = producer
        .send(ProduceRecord::to("rest").value(&b"a"[..]))
        .await
        .unwrap();
    let _md1 = producer
        .send(ProduceRecord::to("rest").value(&b"b"[..]))
        .await
        .unwrap();
    producer.close().await.unwrap();
    let deleted = admin
        .delete_records(("rest", md0.partition), md0.offset + 1, 10_000)
        .await
        .unwrap();
    assert_eq!(deleted.error_code(), 0);
    assert_eq!(deleted.low_watermark(), md0.offset + 1);
    assert_eq!(
        mock.last_delete_records_version(),
        Some(2),
        "Admin must prefer DeleteRecords v2 when the broker advertises it"
    );
    assert_eq!(mock.last_delete_records_partitions(), 1);
    assert_eq!(mock.last_delete_records_timeout(), Some(10_000));
    let timed_deleted = admin
        .delete_records_timeout(
            ("rest", md0.partition),
            md0.offset + 1,
            Duration::from_millis(1500),
        )
        .await
        .unwrap();
    assert_eq!(timed_deleted.error_code(), 0);
    assert_eq!(timed_deleted.low_watermark(), md0.offset + 1);
    assert_eq!(mock.last_delete_records_timeout(), Some(1500));

    let cluster = admin.describe_cluster().await.unwrap();
    assert_eq!(cluster.error_code, 0);
    assert_eq!(cluster.error_code(), 0);
    assert!(cluster.error_message.is_none());
    assert!(cluster.error_message().is_none());
    assert!(!cluster.brokers.is_empty());
    assert!(!cluster.brokers().is_empty());
    assert_eq!(cluster.nodes().len(), cluster.brokers().len());
    assert_eq!(cluster.cluster_id.as_deref(), Some("mock"));
    assert_eq!(cluster.cluster_id(), Some("mock"));
    assert_eq!(
        cluster.controller().map(DescribeClusterBroker::id),
        Some(cluster.controller_id())
    );
    assert_eq!(
        cluster.controller().map(Node::id),
        cluster.controller().map(DescribeClusterBroker::id)
    );
    assert_eq!(
        cluster.controller().map(DescribeClusterBroker::id_string),
        Some(cluster.controller_id().to_string())
    );
    assert!(!cluster.nodes().iter().any(DescribeClusterBroker::is_empty));
    assert_eq!(
        cluster.authorized_operations(),
        AUTHORIZED_OPERATIONS_OMITTED
    );
    assert_eq!(
        cluster.cluster_authorized_operations(),
        AUTHORIZED_OPERATIONS_OMITTED
    );
    assert_eq!(
        mock.last_describe_cluster_version(),
        Some(2),
        "Admin must prefer DescribeCluster v2 when the broker advertises it"
    );
    assert_eq!(cluster.endpoint_type, 1);
    assert_eq!(cluster.endpoint_type(), 1);
    assert!(!cluster.brokers[0].is_fenced);
    assert!(!cluster.brokers()[0].is_fenced());
}

#[tokio::test]
async fn alter_configs_negotiates_v1_when_broker_caps() {
    let mock = common::Mock::start().await;
    mock.set_api_max(ALTER_CONFIGS, 1);
    let mut admin = Admin::connect(mock.addr.clone()).await.unwrap();
    assert_eq!(
        admin
            .create_topics(&[NewTopic::new("ac1", 1, 1)], 10_000, false)
            .await
            .unwrap()[0]
            .error_code,
        0
    );
    let err = admin
        .alter_configs(
            &ConfigResource::topic("ac1"),
            &[("retention.ms".into(), Some("2000".into()))],
            false,
        )
        .await
        .unwrap();
    assert_eq!(err, 0);
    assert_eq!(
        mock.last_alter_configs_version(),
        Some(1),
        "client must speak AlterConfigs v1 when the broker max is 1"
    );
}

#[tokio::test]
async fn admin_alter_configs_for() {
    let mock = common::Mock::start().await;
    let mut admin = Admin::connect(mock.addr.clone()).await.unwrap();
    let created = admin
        .create_topics(
            &[NewTopic::new("ac-a", 1, 1), NewTopic::new("ac-b", 1, 1)],
            10_000,
            false,
        )
        .await
        .unwrap();
    assert_eq!(created[0].error_code, 0);
    assert_eq!(created[1].error_code, 0);

    let empty = admin.alter_configs_for(&[], false).await.unwrap();
    assert!(empty.is_empty());

    let results = admin
        .alter_configs_for(
            &[
                ConfigReplacement::new(
                    ConfigResource::topic("ac-a"),
                    [("retention.ms".into(), Some("1000".into()))],
                ),
                ConfigReplacement::new(
                    ConfigResource::topic("ac-b"),
                    [("retention.ms".into(), Some("2000".into()))],
                ),
            ],
            false,
        )
        .await
        .unwrap();
    assert_eq!(results.len(), 2);
    assert_eq!(results[0].error_code, 0);
    assert_eq!(results[0].name, "ac-a");
    assert_eq!(results[1].error_code, 0);
    assert_eq!(results[1].name, "ac-b");
    assert_eq!(
        mock.last_alter_configs_n(),
        Some(2),
        "alterConfigs(Map) must send Resources of 2 in one RPC"
    );
    let timed = admin
        .alter_configs_for_timeout(
            &[ConfigReplacement::new(
                ConfigResource::topic("ac-a"),
                [("retention.ms".into(), Some("1000".into()))],
            )],
            Duration::from_secs(5),
            false,
        )
        .await
        .unwrap();
    assert_eq!(timed[0].error_code, 0);

    let described = admin
        .describe_configs(
            &[
                ConfigResource::topic("ac-a").keys(["retention.ms"]),
                ConfigResource::topic("ac-b").keys(["retention.ms"]),
            ],
            false,
        )
        .await
        .unwrap();
    assert_eq!(
        described[0]
            .entries
            .iter()
            .find(|e| e.name == "retention.ms")
            .and_then(|e| e.value.as_deref()),
        Some("1000")
    );
    assert_eq!(
        described[1]
            .entries
            .iter()
            .find(|e| e.name == "retention.ms")
            .and_then(|e| e.value.as_deref()),
        Some("2000")
    );
    assert_eq!(
        described[0]
            .config()
            .get("retention.ms")
            .and_then(|e| e.value.as_deref()),
        Some("1000")
    );
    assert_eq!(
        described[1]
            .config()
            .get("retention.ms")
            .and_then(|e| e.value.as_deref()),
        Some("2000")
    );

    let cfg_a = Config::new([ConfigEntry::new("retention.ms", Some("3000".into()))]);
    let err = admin
        .alter_configs_with(&ConfigResource::topic("ac-a"), &cfg_a, false)
        .await
        .unwrap();
    assert_eq!(err, 0);
    let err = admin
        .alter_configs_with_timeout(
            &ConfigResource::topic("ac-a"),
            &cfg_a,
            Duration::from_secs(5),
            false,
        )
        .await
        .unwrap();
    assert_eq!(err, 0);
    let via_config = admin
        .alter_configs_for(
            &[ConfigReplacement::from_config(
                ConfigResource::topic("ac-b"),
                &Config::new([ConfigEntry::new("retention.ms", Some("4000".into()))]),
            )],
            false,
        )
        .await
        .unwrap();
    assert_eq!(via_config[0].error_code, 0);
    let described = admin
        .describe_configs(
            &[
                ConfigResource::topic("ac-a").keys(["retention.ms"]),
                ConfigResource::topic("ac-b").keys(["retention.ms"]),
            ],
            false,
        )
        .await
        .unwrap();
    assert_eq!(
        described[0]
            .config()
            .get("retention.ms")
            .and_then(|e| e.value.as_deref()),
        Some("3000")
    );
    assert_eq!(
        described[1]
            .config()
            .get("retention.ms")
            .and_then(|e| e.value.as_deref()),
        Some("4000")
    );
    admin.close().await.unwrap();
}

#[tokio::test]
async fn delete_records_follows_partition_leader() {
    let mock = common::Mock::start_two_node().await;
    let mut pcfg = ProducerConfig::bootstrap([mock.addr.clone()]);
    pcfg.linger = Duration::ZERO;
    let producer = Producer::new(pcfg).await.unwrap();
    let md = producer
        .send(ProduceRecord::to("t").value(&b"del-lead"[..]))
        .await
        .unwrap();
    producer.close().await.unwrap();

    let mut admin = Admin::connect(mock.addr.clone()).await.unwrap();
    let deleted = admin
        .delete_records(("t", md.partition), md.offset + 1, 10_000)
        .await
        .unwrap();
    assert_eq!(deleted, DeletedRecords::with_error_code(md.offset + 1, 0));
    assert_eq!(
        mock.last_delete_records_version(),
        Some(2),
        "Admin must prefer DeleteRecords v2 when the broker advertises it"
    );
    assert_eq!(
        mock.last_delete_records_node(),
        Some(2),
        "DeleteRecords must land on the partition leader, not a follower"
    );
    assert_eq!(mock.log_len("t", md.partition), 0);

    mock.set_partition_leader("t", md.partition, 1);
    let again = admin
        .delete_records(("t", md.partition), md.offset + 1, 10_000)
        .await
        .unwrap();
    assert_eq!(again.error_code(), 0);
    assert_eq!(again.low_watermark(), md.offset + 1);
    assert_eq!(
        mock.delete_records_not_leader(),
        1,
        "stale leader must return NOT_LEADER_OR_FOLLOWER (6) once"
    );
    assert_eq!(
        mock.last_delete_records_node(),
        Some(1),
        "DeleteRecords must follow Metadata after NOT_LEADER"
    );
}

#[tokio::test]
async fn delete_records_negotiates_v1_when_broker_caps() {
    let mock = common::Mock::start().await;
    mock.set_api_max(DELETE_RECORDS, 1);
    let mut pcfg = ProducerConfig::bootstrap([mock.addr.clone()]);
    pcfg.linger = Duration::ZERO;
    let producer = Producer::new(pcfg).await.unwrap();
    let md = producer
        .send(ProduceRecord::to("t").value(&b"x"[..]))
        .await
        .unwrap();
    producer.close().await.unwrap();
    let mut admin = Admin::connect(mock.addr.clone()).await.unwrap();
    let deleted = admin
        .delete_records(("t", md.partition), md.offset + 1, 10_000)
        .await
        .unwrap();
    assert_eq!(deleted.error_code(), 0);
    assert_eq!(deleted.low_watermark(), md.offset + 1);
    assert_eq!(
        mock.last_delete_records_version(),
        Some(1),
        "client must speak DeleteRecords v1 when the broker max is 1"
    );
}

#[tokio::test]
async fn admin_delete_records_for_batches_partitions() {
    let mock = common::Mock::start().await;
    let mut admin = Admin::connect(mock.addr.clone()).await.unwrap();
    assert_eq!(
        admin
            .create_topics(&[NewTopic::new("dr2", 2, 1)], 10_000, false)
            .await
            .unwrap()[0]
            .error_code,
        0
    );
    let mut pcfg = ProducerConfig::bootstrap([mock.addr.clone()]);
    pcfg.linger = Duration::ZERO;
    let producer = Producer::new(pcfg).await.unwrap();
    let md0 = producer
        .send(ProduceRecord::to("dr2").partition(0).value(&b"a"[..]))
        .await
        .unwrap();
    let md1 = producer
        .send(ProduceRecord::to("dr2").partition(1).value(&b"b"[..]))
        .await
        .unwrap();
    producer.close().await.unwrap();
    let before = mock.delete_records_calls();
    let listed = admin
        .delete_records_for([
            (TopicPartition::new("dr2", md0.partition), md0.offset + 1),
            (TopicPartition::new("dr2", md1.partition), md1.offset + 1),
        ])
        .await
        .unwrap();
    assert_eq!(listed.len(), 2);
    assert_eq!(listed[0].1.error_code(), 0);
    assert_eq!(listed[1].1.error_code(), 0);
    assert_eq!(listed[0].1.low_watermark(), md0.offset + 1);
    assert_eq!(listed[1].1.low_watermark(), md1.offset + 1);
    assert_eq!(
        mock.last_delete_records_partitions(),
        2,
        "deleteRecords(Map) must send Topics/Partitions of N on one leader"
    );
    assert_eq!(
        mock.delete_records_calls().saturating_sub(before),
        1,
        "partitions that share a leader must be one DeleteRecords"
    );
    assert_eq!(
        mock.last_delete_records_timeout(),
        Some(30_000),
        "delete_records_for TimeoutMs is AdminConfig::request_timeout"
    );
    let timed = admin
        .delete_records_for_timeout(
            [(TopicPartition::new("dr2", md0.partition), md0.offset + 1)],
            Duration::from_millis(2_500),
        )
        .await
        .unwrap();
    assert_eq!(timed.len(), 1);
    assert_eq!(timed[0].1.error_code(), 0);
    assert_eq!(mock.last_delete_records_timeout(), Some(2_500));
    let specced = admin
        .delete_records(
            ("dr2", md0.partition),
            RecordsToDelete::before_offset(md0.offset + 1),
            10_000,
        )
        .await
        .unwrap();
    assert_eq!(specced.error_code(), 0);
    assert_eq!(specced.low_watermark(), md0.offset + 1);
    let spec_listed = admin
        .delete_records_for([(
            TopicPartition::new("dr2", md1.partition),
            RecordsToDelete::before_offset(md1.offset + 1),
        )])
        .await
        .unwrap();
    assert_eq!(spec_listed.len(), 1);
    assert_eq!(spec_listed[0].1.error_code(), 0);
    assert_eq!(spec_listed[0].1.low_watermark(), md1.offset + 1);
    let after_timeout = mock.delete_records_calls();
    let empty = admin
        .delete_records_for(Vec::<(TopicPartition, i64)>::new())
        .await
        .unwrap();
    assert!(empty.is_empty());
    assert_eq!(
        mock.delete_records_calls(),
        after_timeout,
        "empty deleteRecords(Map) is a no-op"
    );
    admin.close().await.unwrap();
}

#[tokio::test]
async fn describe_cluster_negotiates_v0_when_broker_caps() {
    let mock = common::Mock::start().await;
    mock.set_api_max(DESCRIBE_CLUSTER, 0);
    let mut admin = Admin::connect(mock.addr.clone()).await.unwrap();
    let cluster = admin.describe_cluster().await.unwrap();
    assert_eq!(cluster.error_code, 0);
    assert!(!cluster.brokers.is_empty());
    assert_eq!(cluster.endpoint_type, 1);
    assert!(!cluster.brokers[0].is_fenced);
    assert_eq!(
        mock.last_describe_cluster_version(),
        Some(0),
        "client must speak DescribeCluster v0 when the broker max is 0"
    );
}

#[tokio::test]
async fn describe_cluster_negotiates_v1_when_broker_caps() {
    let mock = common::Mock::start().await;
    mock.set_api_max(DESCRIBE_CLUSTER, 1);
    let mut admin = Admin::connect(mock.addr.clone()).await.unwrap();
    let cluster = admin.describe_cluster().await.unwrap();
    assert_eq!(cluster.error_code, 0);
    assert_eq!(cluster.endpoint_type, 1);
    assert!(!cluster.brokers[0].is_fenced);
    assert_eq!(
        mock.last_describe_cluster_version(),
        Some(1),
        "client must speak DescribeCluster v1 when the broker max is 1"
    );
    assert_eq!(
        mock.last_describe_cluster_endpoint_type(),
        Some(1),
        "DescribeCluster v1 must send EndpointType brokers"
    );
    assert_eq!(
        mock.last_describe_cluster_include_fenced(),
        Some(false),
        "DescribeCluster v1 has no IncludeFencedBrokers"
    );
}

#[tokio::test]
async fn describe_cluster_with_sends_endpoint_type_and_fenced() {
    let mock = common::Mock::start().await;
    let mut admin = Admin::connect(mock.addr.clone()).await.unwrap();
    let cluster = admin
        .describe_cluster_with(true, EndpointType::Controllers, true)
        .await
        .unwrap();
    assert_eq!(cluster.error_code, 0);
    assert_eq!(cluster.endpoint_type, i8::from(EndpointType::Controllers));
    assert_eq!(
        mock.last_describe_cluster_version(),
        Some(2),
        "describe_cluster_with must keep DescribeCluster v2"
    );
    assert_eq!(
        mock.last_describe_cluster_endpoint_type(),
        Some(2),
        "describe_cluster_with must send EndpointType controllers"
    );
    assert_eq!(
        mock.last_describe_cluster_include_fenced(),
        Some(true),
        "describe_cluster_with must send IncludeFencedBrokers on v2"
    );
    let timed = admin
        .describe_cluster_timeout(Duration::from_secs(5))
        .await
        .unwrap();
    assert_eq!(timed.error_code, 0);
    let timed_with = admin
        .describe_cluster_with_timeout(
            true,
            EndpointType::Controllers,
            true,
            Duration::from_secs(5),
        )
        .await
        .unwrap();
    assert_eq!(
        timed_with.endpoint_type,
        i8::from(EndpointType::Controllers)
    );
    admin.close().await.unwrap();
    mock.hide_api(DESCRIBE_CLUSTER);
    let mut admin = Admin::connect(mock.addr.clone()).await.unwrap();
    let err = admin.describe_cluster().await.unwrap_err();
    assert!(
        err.to_string().contains("unsupported"),
        "DescribeCluster is optional at connect: {err}"
    );
    let classic = admin
        .describe_classic_groups(&["g-classic"], false)
        .await
        .unwrap();
    assert_eq!(classic[0].group_id, "g-classic");
    admin.close().await.unwrap();
}

#[tokio::test]
async fn describe_producers_follows_partition_leader() {
    let mock = common::Mock::start_two_node().await;
    let mut admin = Admin::connect(mock.addr.clone()).await.unwrap();
    let first = admin.describe_producers(("t", 0)).await.unwrap();
    assert_eq!(first.error_code, 0);
    assert_eq!(first.partition_index, 0);
    assert_eq!(first.active_producers.len(), 1);
    assert_eq!(first.active_producers[0].producer_id(), 1000);
    assert_eq!(first.active_producers()[0].coordinator_epoch(), Some(0));
    assert!(first.active_producers()[0]
        .current_txn_start_offset()
        .is_none());
    assert_eq!(
        first.active_producers[0].to_string(),
        "ProducerState(producerId=1000, producerEpoch=1, lastSequence=7, lastTimestamp=1700000000000, coordinatorEpoch=OptionalInt[0], currentTransactionStartOffset=OptionalLong.empty)"
    );
    assert_eq!(first.error_code(), 0);
    assert_eq!(first.partition_index(), 0);
    assert_eq!(
        mock.last_describe_producers_node(),
        Some(2),
        "DescribeProducers must land on the partition leader, not a follower"
    );

    mock.set_partition_leader("t", 0, 1);
    let again = admin.describe_producers(("t", 0)).await.unwrap();
    assert_eq!(again.error_code, 0);
    assert_eq!(again.active_producers.len(), 1);
    assert_eq!(
        mock.describe_producers_not_leader(),
        1,
        "stale leader must return NOT_LEADER_OR_FOLLOWER (6) once"
    );
    assert_eq!(
        mock.last_describe_producers_node(),
        Some(1),
        "DescribeProducers must follow Metadata after NOT_LEADER"
    );
    assert_eq!(
        mock.last_describe_producers_topics(),
        Some(1),
        "single-partition describe_producers sends Topics array of 1"
    );
    let timed = admin
        .describe_producers_timeout(("t", 0), Duration::from_secs(5))
        .await
        .unwrap();
    assert_eq!(timed.error_code, 0);
    assert_eq!(timed.partition_index, 0);

    let not_leader = mock.describe_producers_not_leader();
    let pinned_fail = admin
        .describe_producers_for_on_broker([("t", 0)], 2)
        .await
        .unwrap();
    assert_eq!(pinned_fail.len(), 1);
    assert_eq!(
        pinned_fail[0].partitions[0].error_code,
        error::NOT_LEADER_OR_FOLLOWER,
        "brokerId pin must not retry NOT_LEADER onto the Metadata leader"
    );
    assert_eq!(
        mock.last_describe_producers_node(),
        Some(2),
        "DescribeProducersOptions.brokerId must land on that broker"
    );
    assert_eq!(
        mock.describe_producers_not_leader(),
        not_leader + 1,
        "pinned follower must return NOT_LEADER_OR_FOLLOWER once"
    );
    let pinned_ok = admin
        .describe_producers_for_on_broker_timeout([("t", 0)], 1, Duration::from_secs(5))
        .await
        .unwrap();
    assert_eq!(pinned_ok[0].partitions[0].error_code, 0);
    assert_eq!(mock.last_describe_producers_node(), Some(1));
    admin.close().await.unwrap();
    mock.hide_api(DESCRIBE_PRODUCERS);
    let mut admin = Admin::connect(mock.addr.clone()).await.unwrap();
    let err = admin.describe_producers(("t", 0)).await.unwrap_err();
    assert!(
        err.to_string().contains("unsupported"),
        "DescribeProducers is optional at connect: {err}"
    );
    let classic = admin
        .describe_classic_groups(&["g-classic"], false)
        .await
        .unwrap();
    assert_eq!(classic[0].group_id, "g-classic");
    admin.close().await.unwrap();
}

/// Java `describeProducers(Collection<TopicPartition>)` sends DescribeProducers Topics of N
/// grouped by partition leader (same class as `deleteRecords`).
#[tokio::test]
async fn admin_describe_producers_for() {
    let mock = common::Mock::start().await;
    let mut admin = Admin::connect(mock.addr.clone()).await.unwrap();
    admin
        .create_topics(
            &[NewTopic::new("dp-a", 1, 1), NewTopic::new("dp-b", 1, 1)],
            10_000,
            false,
        )
        .await
        .unwrap();
    let empty = admin
        .describe_producers_for(Vec::<TopicPartition>::new())
        .await
        .unwrap();
    assert!(empty.is_empty());
    assert_eq!(
        mock.last_describe_producers_topics(),
        None,
        "empty describe_producers_for is a no-op"
    );
    let topics = admin
        .describe_producers_for([("dp-a", 0), ("dp-b", 0)])
        .await
        .unwrap();
    assert_eq!(topics.len(), 2);
    assert_eq!(topics[0].name, "dp-a");
    assert_eq!(topics[0].partitions.len(), 1);
    assert_eq!(topics[0].partitions[0].partition_index, 0);
    assert_eq!(topics[0].partitions[0].error_code, 0);
    assert_eq!(topics[1].name, "dp-b");
    assert_eq!(topics[1].partitions.len(), 1);
    assert_eq!(topics[1].partitions[0].error_code, 0);
    assert_eq!(
        mock.last_describe_producers_topics(),
        Some(2),
        "describe_producers_for sends Topics array of N when partitions share a leader"
    );
    let timed = admin
        .describe_producers_for_timeout([("dp-a", 0), ("dp-b", 0)], Duration::from_secs(5))
        .await
        .unwrap();
    assert_eq!(timed.len(), 2);
    assert_eq!(timed[0].name, "dp-a");
    assert_eq!(timed[1].name, "dp-b");
}

#[tokio::test]
async fn abort_transaction_follows_partition_leader() {
    let mock = common::Mock::start_two_node().await;
    let mut admin = Admin::connect(mock.addr.clone()).await.unwrap();
    let spec = AbortTransactionSpec::new(("t", 0), 1000, 0, 1);
    assert_eq!(
        spec.to_string(),
        "AbortTransactionSpec(topicPartition=t-0, producerId=1000, producerEpoch=0, coordinatorEpoch=1)"
    );
    admin.abort_transaction(spec).await.unwrap();
    let marker = mock.last_write_txn_markers().expect("WriteTxnMarkers sent");
    assert_eq!(marker.producer_id, 1000);
    assert!(!marker.transaction_result);
    assert_eq!(
        mock.last_write_txn_markers_version(),
        Some(1),
        "Admin must prefer WriteTxnMarkers v1 when the broker advertises it"
    );
    assert_eq!(
        mock.last_write_txn_markers_node(),
        Some(2),
        "WriteTxnMarkers must land on the partition leader, not a follower"
    );

    mock.set_partition_leader("t", 0, 1);
    admin
        .abort_transaction(AbortTransactionSpec::new(("t", 0), 1000, 0, 1))
        .await
        .unwrap();
    assert_eq!(
        mock.write_txn_markers_not_leader(),
        1,
        "stale leader must return NOT_LEADER_OR_FOLLOWER (6) once"
    );
    assert_eq!(
        mock.last_write_txn_markers_node(),
        Some(1),
        "WriteTxnMarkers must follow Metadata after NOT_LEADER"
    );
    admin
        .abort_transaction_timeout(
            AbortTransactionSpec::new(("t", 0), 1000, 0, 1),
            Duration::from_secs(5),
        )
        .await
        .unwrap();
    assert_eq!(mock.last_write_txn_markers_node(), Some(1));
}

#[tokio::test]
async fn admin_list_and_describe_topics_on_bootstrap() {
    let mock = common::Mock::start().await;
    let mut admin = Admin::connect(mock.addr.clone()).await.unwrap();
    let listed = admin.list_topics().await.unwrap();
    assert!(listed.iter().any(|t| t.name == "t" && !t.is_internal));
    let t = listed.iter().find(|x| x.name() == "t").unwrap();
    assert_eq!(t.name(), "t");
    assert!(!t.is_internal());
    assert_eq!(t.topic_id().to_bytes(), t.topic_id);
    assert_eq!(
        format!("{t}"),
        format!(
            "(name={}, topicId={}, internal={})",
            t.name(),
            t.topic_id(),
            t.is_internal()
        )
    );
    assert_eq!(
        mock.last_metadata_topics(),
        Some(None),
        "list_topics must send Metadata with a null topic array"
    );
    let created_internal = admin
        .create_topics(&[NewTopic::new("lt-internal", 1, 1)], 10_000, false)
        .await
        .unwrap();
    assert_eq!(created_internal[0].error_code, 0);
    mock.set_topic_internal("lt-internal", true);
    assert_eq!(mock.topic_is_internal("lt-internal"), Some(true));
    let listed = admin.list_topics().await.unwrap();
    assert!(listed
        .iter()
        .any(|t| t.name == "lt-internal" && t.is_internal));
    let listed = admin.list_topics_with(false).await.unwrap();
    assert!(
        listed.iter().all(|t| !t.is_internal),
        "list_topics_with(false) drops IsInternal rows"
    );
    assert!(!listed.iter().any(|t| t.name == "lt-internal"));
    assert!(listed.iter().any(|t| t.name == "t"));
    assert_eq!(
        mock.last_metadata_topics(),
        Some(None),
        "list_topics_with still sends Metadata with a null topic array"
    );
    let listed = admin
        .list_topics_timeout(Duration::from_secs(5))
        .await
        .unwrap();
    assert!(listed
        .iter()
        .any(|t| t.name == "lt-internal" && t.is_internal));
    let listed = admin
        .list_topics_with_timeout(false, Duration::from_secs(5))
        .await
        .unwrap();
    assert!(!listed.iter().any(|t| t.name == "lt-internal"));
    let described_internal = admin.describe_topics(["lt-internal"]).await.unwrap();
    assert_eq!(described_internal.len(), 1);
    assert!(described_internal[0].is_internal);
    assert!(described_internal[0].is_internal());
    assert_eq!(described_internal[0].name(), "lt-internal");
    let described = admin.describe_topics(["t"]).await.unwrap();
    assert_eq!(described.len(), 1);
    assert_eq!(described[0].name(), "t");
    assert!(!described[0].is_internal());
    assert_eq!(described[0].topic_id().to_bytes(), described[0].topic_id);
    assert_eq!(described[0].partitions.len(), 1);
    assert_eq!(described[0].partitions().len(), 1);
    assert_eq!(described[0].error_code(), 0);
    assert_eq!(described[0].partitions[0].leader, 1);
    assert_eq!(
        described[0].authorized_operations, AUTHORIZED_OPERATIONS_OMITTED,
        "describe_topics must leave TopicAuthorizedOperations unset"
    );
    assert_eq!(
        described[0].authorized_operations(),
        AUTHORIZED_OPERATIONS_OMITTED
    );
    assert_eq!(
        mock.last_describe_topic_partitions(),
        Some((vec!["t".into()], 2000, None)),
        "describe_topics must send DescribeTopicPartitions for the named topics"
    );
    assert_eq!(
        mock.last_metadata_topics(),
        Some(None),
        "name-based describeTopics uses DescribeTopicPartitions, not Metadata"
    );
    let with_ops = admin.describe_topics_with(["t"], true).await.unwrap();
    assert_eq!(with_ops.len(), 1);
    assert_eq!(with_ops[0].authorized_operations, 4);
    assert_eq!(with_ops[0].authorized_operations(), 4);
    assert_eq!(
        mock.last_describe_topic_partitions(),
        Some((vec!["t".into()], 2000, None)),
        "describe_topics_with(true) still uses DescribeTopicPartitions"
    );
    let timed = admin
        .describe_topics_timeout(["t"], Duration::from_secs(5))
        .await
        .unwrap();
    assert_eq!(timed.len(), 1);
    assert_eq!(timed[0].name, "t");
    assert_eq!(
        mock.last_describe_topic_partitions(),
        Some((vec!["t".into()], 2000, None)),
        "describe_topics_timeout still uses DescribeTopicPartitions"
    );
    let named = admin
        .describe_topics_for(&TopicCollection::of_topic_names(["t"]))
        .await
        .unwrap();
    assert_eq!(named[0].name(), "t");
    assert_eq!(
        mock.last_describe_topic_partitions(),
        Some((vec!["t".into()], 2000, None)),
        "describeTopics(TopicCollection.ofTopicNames) uses DescribeTopicPartitions"
    );
    let named_ops = admin
        .describe_topics_for_with(&TopicCollection::of_topic_names(["t"]), true)
        .await
        .unwrap();
    assert_eq!(named_ops[0].authorized_operations, 4);
    let named_timed = admin
        .describe_topics_for_timeout(
            &TopicCollection::of_topic_names(["t"]),
            Duration::from_secs(5),
        )
        .await
        .unwrap();
    assert_eq!(named_timed[0].name(), "t");
    let timed_ops = admin
        .describe_topics_with_timeout(["t"], true, Duration::from_secs(5))
        .await
        .unwrap();
    assert_eq!(timed_ops[0].authorized_operations, 4);
    let limited = admin
        .describe_topics_with_partition_limit(["t"], false, 1)
        .await
        .unwrap();
    assert_eq!(limited.len(), 1);
    assert_eq!(
        mock.last_describe_topic_partitions(),
        Some((vec!["t".into()], 1, None)),
        "describe_topics_with_partition_limit sends ResponsePartitionLimit"
    );
    let timed_limit = admin
        .describe_topics_with_partition_limit_timeout(["t"], true, 7, Duration::from_secs(5))
        .await
        .unwrap();
    assert_eq!(timed_limit[0].authorized_operations, 4);
    assert_eq!(
        mock.last_describe_topic_partitions(),
        Some((vec!["t".into()], 7, None)),
        "describe_topics_with_partition_limit_timeout sends ResponsePartitionLimit"
    );
    let created = admin
        .create_topics(
            &[NewTopic::new("dtn-a", 1, 1), NewTopic::new("dtn-b", 1, 1)],
            10_000,
            false,
        )
        .await
        .unwrap();
    assert_eq!(created[0].error_code, 0);
    assert_eq!(created[1].error_code, 0);
    let two = admin.describe_topics(["dtn-a", "dtn-b"]).await.unwrap();
    assert_eq!(two.len(), 2);
    assert_eq!(two[0].name, "dtn-a");
    assert_eq!(two[1].name, "dtn-b");
    assert_eq!(two[0].error_code, 0);
    assert_eq!(two[1].error_code, 0);
    assert_eq!(
        mock.last_describe_topic_partitions(),
        Some((vec!["dtn-a".into(), "dtn-b".into()], 2000, None)),
        "describe_topics sends Topics of N"
    );
    let missing = admin.describe_topics(["no-such-topic"]).await.unwrap();
    assert_eq!(missing.len(), 1);
    assert_eq!(missing[0].error_code, error::UNKNOWN_TOPIC_OR_PARTITION);
    let calls = mock.metadata_calls();
    let dtp = mock.last_describe_topic_partitions();
    let empty = admin.describe_topics(Vec::<&str>::new()).await.unwrap();
    assert!(empty.is_empty());
    assert_eq!(
        mock.metadata_calls(),
        calls,
        "empty describe_topics is a no-op"
    );
    assert_eq!(
        mock.last_describe_topic_partitions(),
        dtp,
        "empty describe_topics does not send DescribeTopicPartitions"
    );
}

/// Name-based describe_topics falls back to Metadata when the broker
/// does not advertise DescribeTopicPartitions (api 75).
#[tokio::test]
async fn admin_describe_topics_falls_back_to_metadata_without_dtp() {
    let mock = common::Mock::start().await;
    mock.hide_api(DESCRIBE_TOPIC_PARTITIONS);
    assert!(mock.api_hidden(DESCRIBE_TOPIC_PARTITIONS));
    let mut admin = Admin::connect(mock.addr.clone()).await.unwrap();
    let dtp = mock.last_describe_topic_partitions();
    let described = admin.describe_topics(["t"]).await.unwrap();
    assert_eq!(described.len(), 1);
    assert_eq!(described[0].name, "t");
    assert_eq!(described[0].error_code, 0);
    assert_eq!(described[0].partitions.len(), 1);
    assert_eq!(
        described[0].authorized_operations,
        AUTHORIZED_OPERATIONS_OMITTED
    );
    assert_eq!(
        mock.last_describe_topic_partitions(),
        dtp,
        "describe_topics must not send DescribeTopicPartitions when hidden"
    );
    assert_eq!(
        mock.last_metadata_topics(),
        Some(Some(vec!["t".into()])),
        "Metadata fallback sends named Topics, not a null array"
    );
    assert_eq!(mock.last_metadata_allow_auto(), Some(false));
    let missing = admin.describe_topics(["no-such-topic"]).await.unwrap();
    assert_eq!(missing.len(), 1);
    assert_eq!(missing[0].error_code, error::UNKNOWN_TOPIC_OR_PARTITION);
    let with_ops = admin.describe_topics_with(["t"], true).await.unwrap();
    assert_eq!(with_ops.len(), 1);
    assert_eq!(with_ops[0].authorized_operations, 4);
    assert_eq!(with_ops[0].authorized_operations(), 4);
    assert_eq!(
        mock.last_metadata_include_topic_authorized(),
        Some(true),
        "Metadata fallback sends IncludeTopicAuthorizedOperations"
    );
    let timed = admin
        .describe_topics_timeout(["t"], Duration::from_secs(5))
        .await
        .unwrap();
    assert_eq!(timed.len(), 1);
    assert_eq!(timed[0].name, "t");
    assert_eq!(
        mock.last_describe_topic_partitions(),
        dtp,
        "describe_topics_timeout must not send DescribeTopicPartitions when hidden"
    );
    let limited = admin
        .describe_topics_with_partition_limit(["t"], false, 1)
        .await
        .unwrap();
    assert_eq!(limited.len(), 1);
    assert_eq!(limited[0].name, "t");
    assert_eq!(
        mock.last_describe_topic_partitions(),
        dtp,
        "partitionSizeLimitPerResponse is ignored on Metadata fallback"
    );
    let err = admin
        .describe_topic_partitions(&["t"], 2000, None)
        .await
        .unwrap_err();
    assert!(
        err.to_string().contains("unsupported"),
        "raw DescribeTopicPartitions is unsupported when hidden: {err}"
    );
    admin.close().await.unwrap();
}

/// Java `describeTopics(TopicCollection.ofTopicIds)` sends Metadata v10+
/// Topics of null Name + TopicId.
#[tokio::test]
async fn admin_describe_topics_by_id() {
    let mock = common::Mock::start().await;
    let mut admin = Admin::connect(mock.addr.clone()).await.unwrap();
    let created = admin
        .create_topics(
            &[NewTopic::new("dtid-a", 1, 1), NewTopic::new("dtid-b", 1, 1)],
            10_000,
            false,
        )
        .await
        .unwrap();
    assert_eq!(created[0].error_code, 0);
    assert_eq!(created[1].error_code, 0);
    assert_ne!(created[0].topic_id, [0u8; 16]);
    assert_ne!(created[1].topic_id, [0u8; 16]);
    let calls = mock.metadata_calls();
    let empty = admin.describe_topics_by_id(&[]).await.unwrap();
    assert!(empty.is_empty());
    assert_eq!(
        mock.metadata_calls(),
        calls,
        "empty describe_topics_by_id is a no-op"
    );
    let described = admin
        .describe_topics_by_id(&[created[0].topic_id, created[1].topic_id])
        .await
        .unwrap();
    assert_eq!(described.len(), 2);
    assert_eq!(described[0].error_code, 0);
    assert_eq!(described[1].error_code, 0);
    assert_eq!(described[0].name, "dtid-a");
    assert_eq!(described[1].name, "dtid-b");
    assert_eq!(described[0].topic_id, created[0].topic_id);
    assert_eq!(described[1].topic_id, created[1].topic_id);
    assert_eq!(described[0].partitions.len(), 1);
    assert_eq!(
        described[0].authorized_operations, AUTHORIZED_OPERATIONS_OMITTED,
        "describe_topics_by_id must leave IncludeTopicAuthorizedOperations unset"
    );
    assert_eq!(
        mock.last_metadata_topic_ids(),
        Some(2),
        "describe_topics_by_id sends Topics of null Name + TopicId"
    );
    assert_eq!(
        mock.last_metadata_allow_auto(),
        Some(false),
        "describeTopics sets AllowAutoTopicCreation false"
    );
    let via = admin
        .describe_topics_for(&TopicCollection::of_topic_ids([
            Uuid::from(created[0].topic_id),
            Uuid::from(created[1].topic_id),
        ]))
        .await
        .unwrap();
    assert_eq!(via.len(), 2);
    assert_eq!(via[0].topic_id, created[0].topic_id);
    assert_eq!(via[1].topic_id, created[1].topic_id);
    assert_eq!(
        mock.last_metadata_topic_ids(),
        Some(2),
        "describeTopics(TopicCollection.ofTopicIds) sends Topics of null Name + TopicId"
    );
    let via_ops = admin
        .describe_topics_for_with(
            &TopicCollection::of_topic_ids([Uuid::from(created[0].topic_id)]),
            true,
        )
        .await
        .unwrap();
    assert_eq!(via_ops[0].authorized_operations, 4);
    let via_timed = admin
        .describe_topics_for_timeout(
            &TopicCollection::of_topic_ids([Uuid::from(created[0].topic_id)]),
            Duration::from_secs(5),
        )
        .await
        .unwrap();
    assert_eq!(via_timed[0].name, "dtid-a");
    let empty_ids = admin
        .describe_topics_for(&TopicCollection::of_topic_ids(Vec::<Uuid>::new()))
        .await
        .unwrap();
    assert!(empty_ids.is_empty());
    let missing = admin.describe_topics_by_id(&[[0xff; 16]]).await.unwrap();
    assert_eq!(missing.len(), 1);
    assert_eq!(missing[0].error_code, error::UNKNOWN_TOPIC_ID);
    assert!(missing[0].name.is_empty());
    assert_eq!(missing[0].topic_id, [0xff; 16]);
    let with_ops = admin
        .describe_topics_by_id_with(&[created[0].topic_id], true)
        .await
        .unwrap();
    assert_eq!(with_ops.len(), 1);
    assert_eq!(with_ops[0].authorized_operations, 4);
    assert_eq!(
        mock.last_metadata_include_topic_authorized(),
        Some(true),
        "describe_topics_by_id_with(true) must send IncludeTopicAuthorizedOperations"
    );
    let timed = admin
        .describe_topics_by_id_timeout(&[created[0].topic_id], Duration::from_secs(5))
        .await
        .unwrap();
    assert_eq!(timed.len(), 1);
    assert_eq!(timed[0].name, "dtid-a");
    let timed_ops = admin
        .describe_topics_by_id_with_timeout(&[created[0].topic_id], true, Duration::from_secs(5))
        .await
        .unwrap();
    assert_eq!(timed_ops[0].authorized_operations, 4);
    admin.close().await.unwrap();

    let mock = common::Mock::start().await;
    mock.set_api_max(METADATA, 9);
    let mut admin = Admin::connect(mock.addr.clone()).await.unwrap();
    let err = admin.describe_topics_by_id(&[[1u8; 16]]).await.unwrap_err();
    assert!(
        err.to_string().contains("unsupported"),
        "Metadata below v10 cannot describe by TopicId: {err}"
    );
    admin.close().await.unwrap();
}

#[tokio::test]
async fn create_topics_follows_controller() {
    let mock = common::Mock::start_two_node().await;
    mock.set_controller(2);
    let mut admin = Admin::connect(mock.addr.clone()).await.unwrap();
    let created = admin
        .create_topics(&[NewTopic::new("ctrl2", 1, 1)], 10_000, false)
        .await
        .unwrap();
    assert_eq!(created[0].error_code, 0);
    assert_eq!(
        mock.last_create_topics_node(),
        Some(2),
        "CreateTopics must land on the controller, not bootstrap"
    );
    assert_eq!(
        mock.last_create_topics_version(),
        Some(7),
        "Admin must prefer CreateTopics v7 when the broker advertises it"
    );
    assert_ne!(
        created[0].topic_id, [0u8; 16],
        "CreateTopics v7 must return TopicId"
    );
    assert_ne!(created[0].topic_id(), Uuid::ZERO);
    assert_eq!(created[0].name(), "ctrl2");
    assert_eq!(created[0].error_code(), 0);
    assert_eq!(created[0].num_partitions, 1);
    assert_eq!(created[0].num_partitions(), 1);
    assert_eq!(created[0].replication_factor, 1);
    assert_eq!(created[0].replication_factor(), 1);

    mock.set_controller(1);
    let again = admin
        .create_topics(&[NewTopic::new("ctrl1", 1, 1)], 10_000, false)
        .await
        .unwrap();
    assert_eq!(again[0].error_code, 0);
    assert_eq!(
        mock.create_topics_not_controller(),
        1,
        "stale controller must return NOT_CONTROLLER (41) once"
    );
    assert_eq!(
        mock.last_create_topics_node(),
        Some(1),
        "CreateTopics must follow Metadata after NOT_CONTROLLER"
    );
    assert_eq!(
        mock.last_create_topics_replica_assignments(),
        Some(Vec::new()),
        "NewTopic::new sends an empty Assignments array"
    );
    let assigned = admin
        .create_topics(
            &[NewTopic::with_assignments(
                "ctrl-as",
                [(0, [1, 2]), (1, [2, 1])],
            )],
            10_000,
            false,
        )
        .await
        .unwrap();
    assert_eq!(assigned[0].error_code, 0);
    assert_eq!(assigned[0].num_partitions, 2);
    assert_eq!(
        mock.last_create_topics_replica_assignments(),
        Some(vec![(0, vec![1, 2]), (1, vec![2, 1])])
    );
    let defaulted = admin
        .create_topics(&[NewTopic::broker_defaults("ctrl-def")], 10_000, false)
        .await
        .unwrap();
    assert_eq!(defaulted[0].error_code, 0);
    assert_eq!(defaulted[0].num_partitions, 1);
    assert_eq!(defaulted[0].replication_factor, 1);
    assert_eq!(
        mock.last_create_topics_num_partitions(),
        Some(-1),
        "broker_defaults sends NumPartitions -1"
    );
    assert_eq!(
        mock.last_create_topics_replication_factor(),
        Some(-1),
        "broker_defaults sends ReplicationFactor -1"
    );
    assert_eq!(
        mock.last_create_topics_replica_assignments(),
        Some(Vec::new()),
        "broker_defaults sends an empty Assignments array"
    );
    let configured = admin
        .create_topics(
            &[NewTopic::new("ctrl-cfg", 1, 1).config("cleanup.policy", "compact")],
            10_000,
            false,
        )
        .await
        .unwrap();
    assert_eq!(configured[0].error_code(), 0);
    let cfg_entry = configured[0]
        .config()
        .get("cleanup.policy")
        .cloned()
        .expect("CreateTopics v5+ echoes requested configs");
    assert_eq!(cfg_entry.value(), Some("compact"));
    assert_eq!(cfg_entry.source(), ConfigSource::DynamicTopic);
    let first_cfg = configured[0]
        .configs()
        .first()
        .expect("CreateTopics v5+ Configs is not empty");
    assert_eq!(first_cfg.name(), "cleanup.policy");
    assert!(!first_cfg.is_read_only());
    assert!(!first_cfg.is_sensitive());
}

#[tokio::test]
async fn create_topics_negotiates_below_v7_when_broker_caps() {
    let mock = common::Mock::start().await;
    mock.set_api_max(CREATE_TOPICS, 5);
    let mut admin = Admin::connect(mock.addr.clone()).await.unwrap();
    let created = admin
        .create_topics(&[NewTopic::new("ct5", 1, 1)], 10_000, false)
        .await
        .unwrap();
    assert_eq!(created[0].error_code, 0);
    assert_eq!(
        mock.last_create_topics_version(),
        Some(5),
        "client must speak CreateTopics v5 when the broker max is 5"
    );
    assert_eq!(
        created[0].topic_id, [0u8; 16],
        "CreateTopics v5 has no TopicId"
    );
    assert_eq!(created[0].num_partitions, 1);

    let mock = common::Mock::start().await;
    mock.set_api_max(CREATE_TOPICS, 4);
    let mut admin = Admin::connect(mock.addr.clone()).await.unwrap();
    let created = admin
        .create_topics(&[NewTopic::new("ct4", 1, 1)], 10_000, false)
        .await
        .unwrap();
    assert_eq!(created[0].error_code, 0);
    assert_eq!(
        mock.last_create_topics_version(),
        Some(4),
        "client must speak CreateTopics v4 when the broker max is 4"
    );
    assert_eq!(
        created[0].num_partitions, -1,
        "CreateTopics v4 omits NumPartitions"
    );
}

#[tokio::test]
async fn delete_topics_follows_controller() {
    let mock = common::Mock::start_two_node().await;
    mock.set_controller(2);
    let mut admin = Admin::connect(mock.addr.clone()).await.unwrap();
    let created = admin
        .create_topics(
            &[NewTopic::new("del2", 1, 1), NewTopic::new("del1", 1, 1)],
            10_000,
            false,
        )
        .await
        .unwrap();
    assert_eq!(created[0].error_code, 0);
    assert_eq!(created[1].error_code, 0);

    let deleted = admin.delete_topics(&["del2"], 10_000).await.unwrap();
    assert_eq!(deleted[0].error_code, 0);
    assert_eq!(
        mock.last_delete_topics_version(),
        Some(6),
        "Admin must prefer DeleteTopics v6 when the broker advertises it"
    );
    assert_ne!(
        deleted[0].topic_id, [0u8; 16],
        "DeleteTopics v6 must return TopicId"
    );
    assert_eq!(
        mock.last_delete_topics_ids(),
        Some(0),
        "name-based deleteTopics sends TopicId zero, not ofTopicIds"
    );
    assert_eq!(
        mock.last_delete_topics_node(),
        Some(2),
        "DeleteTopics must land on the controller, not bootstrap"
    );

    mock.set_controller(1);
    let again = admin.delete_topics(&["del1"], 10_000).await.unwrap();
    assert_eq!(again[0].error_code, 0);
    assert_eq!(
        mock.delete_topics_not_controller(),
        1,
        "stale controller must return NOT_CONTROLLER (41) once"
    );
    assert_eq!(
        mock.last_delete_topics_node(),
        Some(1),
        "DeleteTopics must follow Metadata after NOT_CONTROLLER"
    );
}

#[tokio::test]
async fn delete_topics_negotiates_below_v6_when_broker_caps() {
    let mock = common::Mock::start().await;
    mock.set_api_max(DELETE_TOPICS, 5);
    let mut admin = Admin::connect(mock.addr.clone()).await.unwrap();
    let created = admin
        .create_topics(&[NewTopic::new("dt5", 1, 1)], 10_000, false)
        .await
        .unwrap();
    assert_eq!(created[0].error_code, 0);
    let deleted = admin.delete_topics(&["dt5"], 10_000).await.unwrap();
    assert_eq!(deleted[0].error_code, 0);
    assert_eq!(
        mock.last_delete_topics_version(),
        Some(5),
        "client must speak DeleteTopics v5 when the broker max is 5"
    );
    assert_eq!(
        deleted[0].topic_id, [0u8; 16],
        "DeleteTopics v5 has no TopicId"
    );
    let err = admin
        .delete_topics_by_id(&[[1u8; 16]], 10_000)
        .await
        .unwrap_err();
    assert!(
        err.to_string().contains("unsupported"),
        "DeleteTopics below v6 cannot delete by TopicId: {err}"
    );

    let mock = common::Mock::start().await;
    mock.set_api_max(DELETE_TOPICS, 4);
    let mut admin = Admin::connect(mock.addr.clone()).await.unwrap();
    let created = admin
        .create_topics(&[NewTopic::new("dt4", 1, 1)], 10_000, false)
        .await
        .unwrap();
    assert_eq!(created[0].error_code, 0);
    let deleted = admin.delete_topics(&["dt4"], 10_000).await.unwrap();
    assert_eq!(deleted[0].error_code, 0);
    assert_eq!(
        mock.last_delete_topics_version(),
        Some(4),
        "client must speak DeleteTopics v4 when the broker max is 4"
    );

    let mock = common::Mock::start().await;
    mock.set_api_max(DELETE_TOPICS, 3);
    let mut admin = Admin::connect(mock.addr.clone()).await.unwrap();
    let created = admin
        .create_topics(&[NewTopic::new("dt3", 1, 1)], 10_000, false)
        .await
        .unwrap();
    assert_eq!(created[0].error_code, 0);
    let deleted = admin.delete_topics(&["dt3"], 10_000).await.unwrap();
    assert_eq!(deleted[0].error_code, 0);
    assert_eq!(
        mock.last_delete_topics_version(),
        Some(3),
        "client must speak DeleteTopics v3 when the broker max is 3"
    );
}

#[tokio::test]
async fn describe_configs_negotiates_below_v4_when_broker_caps() {
    let mock = common::Mock::start().await;
    mock.set_api_max(DESCRIBE_CONFIGS, 3);
    let mut admin = Admin::connect(mock.addr.clone()).await.unwrap();
    let created = admin
        .create_topics(&[NewTopic::new("dc3", 1, 1)], 10_000, false)
        .await
        .unwrap();
    assert_eq!(created[0].error_code, 0);
    let described = admin
        .describe_configs_with_documentation(&[ConfigResource::topic("dc3")], false, true)
        .await
        .unwrap();
    assert_eq!(described[0].error_code, 0);
    assert_eq!(
        mock.last_describe_configs_version(),
        Some(3),
        "client must speak DescribeConfigs v3 when the broker max is 3"
    );
    assert_eq!(
        mock.last_describe_configs_documentation(),
        Some(true),
        "v3 must send IncludeDocumentation"
    );
    assert_eq!(
        described[0].entries[0].config_type,
        partitionline::CONFIG_TYPE_STRING
    );
    assert!(
        described[0].entries[0].documentation.is_some(),
        "DescribeConfigs v3 with documentation must fill Documentation"
    );
    let timed = admin
        .describe_configs_with_documentation_timeout(
            &[ConfigResource::topic("dc3")],
            false,
            true,
            Duration::from_secs(5),
        )
        .await
        .unwrap();
    assert_eq!(timed[0].error_code, 0);

    let mock = common::Mock::start().await;
    mock.set_api_max(DESCRIBE_CONFIGS, 1);
    let mut admin = Admin::connect(mock.addr.clone()).await.unwrap();
    let created = admin
        .create_topics(&[NewTopic::new("dc1", 1, 1)], 10_000, false)
        .await
        .unwrap();
    assert_eq!(created[0].error_code, 0);
    let described = admin
        .describe_configs_with_documentation(&[ConfigResource::topic("dc1")], false, true)
        .await
        .unwrap();
    assert_eq!(described[0].error_code, 0);
    assert_eq!(
        mock.last_describe_configs_version(),
        Some(1),
        "client must speak DescribeConfigs v1 when the broker max is 1"
    );
    assert_eq!(
        mock.last_describe_configs_documentation(),
        Some(false),
        "v1 omits IncludeDocumentation even when the caller asked for it"
    );
    assert_eq!(
        described[0].entries[0].config_type,
        partitionline::CONFIG_TYPE_UNKNOWN
    );
    assert_eq!(described[0].entries[0].documentation, None);
}

#[tokio::test]
async fn create_partitions_follows_controller() {
    let mock = common::Mock::start_two_node().await;
    mock.set_controller(2);
    let mut admin = Admin::connect(mock.addr.clone()).await.unwrap();
    let created = admin
        .create_topics(
            &[NewTopic::new("parts2", 1, 1), NewTopic::new("parts1", 1, 1)],
            10_000,
            false,
        )
        .await
        .unwrap();
    assert_eq!(created[0].error_code, 0);
    assert_eq!(created[1].error_code, 0);

    let parts = admin
        .create_partitions(&[NewPartitions::increase_to("parts2", 3)], 10_000, false)
        .await
        .unwrap();
    assert_eq!(parts[0].error_code, 0);
    assert_eq!(
        mock.last_create_partitions_version(),
        Some(3),
        "Admin must prefer CreatePartitions v3 when the broker advertises it"
    );
    assert_eq!(
        mock.last_create_partitions_node(),
        Some(2),
        "CreatePartitions must land on the controller, not bootstrap"
    );
    assert_eq!(
        mock.last_create_partitions_null_assignments(),
        Some(true),
        "increaseTo(int) sends a null Assignments array"
    );

    mock.set_controller(1);
    let again = admin
        .create_partitions(&[NewPartitions::increase_to("parts1", 2)], 10_000, false)
        .await
        .unwrap();
    assert_eq!(again[0].error_code, 0);
    assert_eq!(
        mock.create_partitions_not_controller(),
        1,
        "stale controller must return NOT_CONTROLLER (41) once"
    );
    assert_eq!(
        mock.last_create_partitions_node(),
        Some(1),
        "CreatePartitions must follow Metadata after NOT_CONTROLLER"
    );
    let assigned = admin
        .create_partitions(
            &[NewPartitions::increase_to("parts2", 4).with_assignments([[1, 2]])],
            10_000,
            false,
        )
        .await
        .unwrap();
    assert_eq!(assigned[0].error_code, 0);
    assert_eq!(
        mock.last_create_partitions_null_assignments(),
        Some(false),
        "increaseTo(int, List) sends Assignments"
    );
    assert_eq!(
        mock.last_create_partitions_replica_assignments(),
        Some(vec![vec![1, 2]])
    );
}

#[tokio::test]
async fn create_partitions_negotiates_below_v3_when_broker_caps() {
    let mock = common::Mock::start().await;
    mock.set_api_max(CREATE_PARTITIONS, 2);
    let mut admin = Admin::connect(mock.addr.clone()).await.unwrap();
    let created = admin
        .create_topics(&[NewTopic::new("cp2", 1, 1)], 10_000, false)
        .await
        .unwrap();
    assert_eq!(created[0].error_code, 0);
    let parts = admin
        .create_partitions(&[NewPartitions::increase_to("cp2", 3)], 10_000, false)
        .await
        .unwrap();
    assert_eq!(parts[0].error_code, 0);
    assert_eq!(
        mock.last_create_partitions_version(),
        Some(2),
        "client must speak CreatePartitions v2 when the broker max is 2"
    );

    let mock = common::Mock::start().await;
    mock.set_api_max(CREATE_PARTITIONS, 1);
    let mut admin = Admin::connect(mock.addr.clone()).await.unwrap();
    let created = admin
        .create_topics(&[NewTopic::new("cp1", 1, 1)], 10_000, false)
        .await
        .unwrap();
    assert_eq!(created[0].error_code, 0);
    let parts = admin
        .create_partitions(&[NewPartitions::increase_to("cp1", 2)], 10_000, false)
        .await
        .unwrap();
    assert_eq!(parts[0].error_code, 0);
    assert_eq!(
        mock.last_create_partitions_version(),
        Some(1),
        "client must speak CreatePartitions v1 when the broker max is 1"
    );
}

#[tokio::test]
async fn admin_create_delete_topics_partitions_timeout() {
    let mock = common::Mock::start().await;
    let mut admin = Admin::connect(mock.addr.clone()).await.unwrap();
    let created = admin
        .create_topics(&[NewTopic::new("cto", 1, 1)], 10_000, false)
        .await
        .unwrap();
    assert_eq!(created[0].error_code, 0);
    assert_eq!(mock.last_create_topics_timeout(), Some(10_000));
    let created = admin
        .create_topics_timeout(
            &[NewTopic::new("cto-d", 1, 1)],
            Duration::from_millis(1_500),
            false,
        )
        .await
        .unwrap();
    assert_eq!(created[0].error_code, 0);
    assert_eq!(mock.last_create_topics_timeout(), Some(1_500));

    let parts = admin
        .create_partitions(&[NewPartitions::increase_to("cto", 2)], 10_000, false)
        .await
        .unwrap();
    assert_eq!(parts[0].error_code, 0);
    assert_eq!(mock.last_create_partitions_timeout(), Some(10_000));
    let parts = admin
        .create_partitions_timeout(
            &[NewPartitions::increase_to("cto", 3)],
            Duration::from_millis(2_500),
            false,
        )
        .await
        .unwrap();
    assert_eq!(parts[0].error_code, 0);
    assert_eq!(mock.last_create_partitions_timeout(), Some(2_500));

    let deleted = admin.delete_topics(&["cto-d"], 10_000).await.unwrap();
    assert_eq!(deleted[0].error_code, 0);
    assert_eq!(mock.last_delete_topics_timeout(), Some(10_000));
    let deleted = admin
        .delete_topics_timeout(&["cto"], Duration::from_millis(1_500))
        .await
        .unwrap();
    assert_eq!(deleted[0].error_code, 0);
    assert_eq!(mock.last_delete_topics_timeout(), Some(1_500));

    mock.create_topics_quota_once("cto-q");
    let created = admin
        .create_topics(&[NewTopic::new("cto-q", 1, 1)], 10_000, false)
        .await
        .unwrap();
    assert_eq!(created[0].error_code, 0);
    assert_eq!(
        mock.create_topics_quota_hits(),
        1,
        "create_topics retries THROTTLING_QUOTA_EXCEEDED by default"
    );

    mock.create_topics_quota_once("cto-qn");
    let created = admin
        .create_topics_with_quota_retry(&[NewTopic::new("cto-qn", 1, 1)], 10_000, false, false)
        .await
        .unwrap();
    assert_eq!(created[0].error_code, error::THROTTLING_QUOTA_EXCEEDED);
    assert_eq!(mock.create_topics_quota_hits(), 2);
    let listed = admin.list_topics().await.unwrap();
    assert!(
        !listed.iter().any(|t| t.name == "cto-qn"),
        "quota retry disabled must not create the topic"
    );

    mock.create_topics_quota_once("cto-mix-q");
    let mix_ok = NewTopic::new("cto-mix-ok", 1, 1);
    let mix_q = NewTopic::new("cto-mix-q", 1, 1);
    let created = admin
        .create_topics_timeout_with_quota_retry(
            &[mix_ok, mix_q],
            Duration::from_secs(10),
            false,
            true,
        )
        .await
        .unwrap();
    assert_eq!(created[0].error_code, 0);
    assert_eq!(created[1].error_code, 0);
    assert_eq!(created[0].name, "cto-mix-ok");
    assert_eq!(created[1].name, "cto-mix-q");
    assert_eq!(
        mock.last_create_topics_names(),
        Some(vec!["cto-mix-q".to_string()]),
        "quota retry resends only THROTTLING_QUOTA_EXCEEDED topics"
    );
    assert_eq!(mock.create_topics_quota_hits(), 3);

    let created = admin
        .create_topics(&[NewTopic::new("dto-q", 1, 1)], 10_000, false)
        .await
        .unwrap();
    assert_eq!(created[0].error_code, 0);
    mock.delete_topics_quota_once("dto-q");
    let deleted = admin.delete_topics(&["dto-q"], 10_000).await.unwrap();
    assert_eq!(deleted[0].error_code, 0);
    assert_eq!(
        mock.delete_topics_quota_hits(),
        1,
        "delete_topics retries THROTTLING_QUOTA_EXCEEDED by default"
    );

    let created = admin
        .create_topics(&[NewTopic::new("dto-qn", 1, 1)], 10_000, false)
        .await
        .unwrap();
    assert_eq!(created[0].error_code, 0);
    mock.delete_topics_quota_once("dto-qn");
    let deleted = admin
        .delete_topics_with_quota_retry(&["dto-qn"], 10_000, false)
        .await
        .unwrap();
    assert_eq!(deleted[0].error_code, error::THROTTLING_QUOTA_EXCEEDED);
    assert_eq!(mock.delete_topics_quota_hits(), 2);
    let listed = admin.list_topics().await.unwrap();
    assert!(
        listed.iter().any(|t| t.name == "dto-qn"),
        "quota retry disabled must not delete the topic"
    );

    let created = admin
        .create_topics(
            &[
                NewTopic::new("dto-mix-ok", 1, 1),
                NewTopic::new("dto-mix-q", 1, 1),
            ],
            10_000,
            false,
        )
        .await
        .unwrap();
    assert_eq!(created[0].error_code, 0);
    assert_eq!(created[1].error_code, 0);
    mock.delete_topics_quota_once("dto-mix-q");
    let deleted = admin
        .delete_topics_timeout_with_quota_retry(
            &["dto-mix-ok", "dto-mix-q"],
            Duration::from_secs(10),
            true,
        )
        .await
        .unwrap();
    assert_eq!(deleted[0].error_code, 0);
    assert_eq!(deleted[1].error_code, 0);
    assert_eq!(deleted[0].name, "dto-mix-ok");
    assert_eq!(deleted[1].name, "dto-mix-q");
    assert_eq!(
        mock.last_delete_topics_names(),
        Some(vec!["dto-mix-q".to_string()]),
        "quota retry resends only THROTTLING_QUOTA_EXCEEDED topics"
    );
    assert_eq!(mock.delete_topics_quota_hits(), 3);

    let created = admin
        .create_topics(&[NewTopic::new("cpo-q", 1, 1)], 10_000, false)
        .await
        .unwrap();
    assert_eq!(created[0].error_code, 0);
    mock.create_partitions_quota_once("cpo-q");
    let parts = admin
        .create_partitions(&[NewPartitions::increase_to("cpo-q", 2)], 10_000, false)
        .await
        .unwrap();
    assert_eq!(parts[0].error_code, 0);
    assert_eq!(
        mock.create_partitions_quota_hits(),
        1,
        "create_partitions retries THROTTLING_QUOTA_EXCEEDED by default"
    );

    let created = admin
        .create_topics(&[NewTopic::new("cpo-qn", 1, 1)], 10_000, false)
        .await
        .unwrap();
    assert_eq!(created[0].error_code, 0);
    mock.create_partitions_quota_once("cpo-qn");
    let parts = admin
        .create_partitions_with_quota_retry(
            &[NewPartitions::increase_to("cpo-qn", 2)],
            10_000,
            false,
            false,
        )
        .await
        .unwrap();
    assert_eq!(parts[0].error_code, error::THROTTLING_QUOTA_EXCEEDED);
    assert_eq!(mock.create_partitions_quota_hits(), 2);
    let described = admin.describe_topics(&["cpo-qn"]).await.unwrap();
    assert_eq!(
        described[0].partitions.len(),
        1,
        "quota retry disabled must not create partitions"
    );

    let created = admin
        .create_topics(
            &[
                NewTopic::new("cpo-mix-ok", 1, 1),
                NewTopic::new("cpo-mix-q", 1, 1),
            ],
            10_000,
            false,
        )
        .await
        .unwrap();
    assert_eq!(created[0].error_code, 0);
    assert_eq!(created[1].error_code, 0);
    mock.create_partitions_quota_once("cpo-mix-q");
    let mix_ok = NewPartitions::increase_to("cpo-mix-ok", 2);
    let mix_q = NewPartitions::increase_to("cpo-mix-q", 2);
    let parts = admin
        .create_partitions_timeout_with_quota_retry(
            &[mix_ok, mix_q],
            Duration::from_secs(10),
            false,
            true,
        )
        .await
        .unwrap();
    assert_eq!(parts[0].error_code, 0);
    assert_eq!(parts[1].error_code, 0);
    assert_eq!(parts[0].name, "cpo-mix-ok");
    assert_eq!(parts[1].name, "cpo-mix-q");
    assert_eq!(
        mock.last_create_partitions_names(),
        Some(vec!["cpo-mix-q".to_string()]),
        "quota retry resends only THROTTLING_QUOTA_EXCEEDED topics"
    );
    assert_eq!(mock.create_partitions_quota_hits(), 3);
    admin.close().await.unwrap();
}

/// Java `deleteTopics(TopicCollection.ofTopicIds)` sends DeleteTopics v6
/// Topics of null Name + TopicId.
#[tokio::test]
async fn admin_delete_topics_by_id() {
    let mock = common::Mock::start().await;
    let mut admin = Admin::connect(mock.addr.clone()).await.unwrap();
    let created = admin
        .create_topics(
            &[NewTopic::new("dti-a", 1, 1), NewTopic::new("dti-b", 1, 1)],
            10_000,
            false,
        )
        .await
        .unwrap();
    assert_eq!(created[0].error_code, 0);
    assert_eq!(created[1].error_code, 0);
    assert_ne!(created[0].topic_id, [0u8; 16]);
    assert_ne!(created[1].topic_id, [0u8; 16]);
    let empty = admin.delete_topics_by_id(&[], 10_000).await.unwrap();
    assert!(empty.is_empty());
    assert_eq!(
        mock.last_delete_topics_ids(),
        None,
        "empty delete_topics_by_id is a no-op"
    );
    let deleted = admin
        .delete_topics_by_id(&[created[0].topic_id, created[1].topic_id], 10_000)
        .await
        .unwrap();
    assert_eq!(deleted.len(), 2);
    assert_eq!(deleted[0].error_code, 0);
    assert_eq!(deleted[1].error_code, 0);
    assert_eq!(deleted[0].topic_id, created[0].topic_id);
    assert_eq!(deleted[1].topic_id, created[1].topic_id);
    assert_eq!(deleted[0].name, "dti-a");
    assert_eq!(deleted[1].name, "dti-b");
    assert_eq!(
        mock.last_delete_topics_ids(),
        Some(2),
        "delete_topics_by_id sends Topics of null Name + TopicId"
    );
    assert_eq!(mock.last_delete_topics_version(), Some(6));
    let listed = admin.list_topics().await.unwrap();
    assert!(
        !listed
            .iter()
            .any(|t| t.name == "dti-a" || t.name == "dti-b"),
        "TopicId deletes must remove the topics"
    );
    let created_col = admin
        .create_topics(&[NewTopic::new("dti-col", 1, 1)], 10_000, false)
        .await
        .unwrap();
    assert_eq!(created_col[0].error_code, 0);
    let deleted_col = admin
        .delete_topics_for(
            &TopicCollection::of_topic_ids([Uuid::from(created_col[0].topic_id)]),
            10_000,
        )
        .await
        .unwrap();
    assert_eq!(deleted_col[0].error_code, 0);
    assert_eq!(
        mock.last_delete_topics_ids(),
        Some(1),
        "deleteTopics(TopicCollection.ofTopicIds) sends Topics of null Name + TopicId"
    );
    let created_name = admin
        .create_topics(&[NewTopic::new("dti-col-n", 1, 1)], 10_000, false)
        .await
        .unwrap();
    assert_eq!(created_name[0].error_code, 0);
    let deleted_name = admin
        .delete_topics_for(&TopicCollection::of_topic_names(["dti-col-n"]), 10_000)
        .await
        .unwrap();
    assert_eq!(deleted_name[0].error_code, 0);
    assert_eq!(deleted_name[0].name, "dti-col-n");
    let created_to = admin
        .create_topics(&[NewTopic::new("dti-col-t", 1, 1)], 10_000, false)
        .await
        .unwrap();
    assert_eq!(created_to[0].error_code, 0);
    let timed_col = admin
        .delete_topics_for_timeout(
            &TopicCollection::of_topic_ids([Uuid::from(created_to[0].topic_id)]),
            Duration::from_millis(1_500),
        )
        .await
        .unwrap();
    assert_eq!(timed_col[0].error_code, 0);
    assert_eq!(mock.last_delete_topics_timeout(), Some(1_500));
    let missing = admin
        .delete_topics_by_id(&[[0xff; 16]], 10_000)
        .await
        .unwrap();
    assert_eq!(missing[0].error_code, error::UNKNOWN_TOPIC_ID);
    assert_eq!(missing[0].topic_id, [0xff; 16]);
    let created = admin
        .create_topics(&[NewTopic::new("dti-c", 1, 1)], 10_000, false)
        .await
        .unwrap();
    assert_eq!(created[0].error_code, 0);
    let timed = admin
        .delete_topics_by_id_timeout(&[created[0].topic_id], Duration::from_millis(1_500))
        .await
        .unwrap();
    assert_eq!(timed[0].error_code, 0);
    assert_eq!(mock.last_delete_topics_timeout(), Some(1_500));

    let created = admin
        .create_topics(&[NewTopic::new("dti-q", 1, 1)], 10_000, false)
        .await
        .unwrap();
    assert_eq!(created[0].error_code, 0);
    let dti_q = created[0].topic_id;
    mock.delete_topics_quota_once("dti-q");
    let deleted = admin.delete_topics_by_id(&[dti_q], 10_000).await.unwrap();
    assert_eq!(deleted[0].error_code, 0);
    assert_eq!(deleted[0].topic_id, dti_q);
    assert_eq!(
        mock.delete_topics_quota_hits(),
        1,
        "delete_topics_by_id retries THROTTLING_QUOTA_EXCEEDED by default"
    );

    let created = admin
        .create_topics(&[NewTopic::new("dti-qn", 1, 1)], 10_000, false)
        .await
        .unwrap();
    assert_eq!(created[0].error_code, 0);
    let dti_qn = created[0].topic_id;
    mock.delete_topics_quota_once("dti-qn");
    let deleted = admin
        .delete_topics_by_id_with_quota_retry(&[dti_qn], 10_000, false)
        .await
        .unwrap();
    assert_eq!(deleted[0].error_code, error::THROTTLING_QUOTA_EXCEEDED);
    assert_eq!(deleted[0].topic_id, dti_qn);
    assert_eq!(mock.delete_topics_quota_hits(), 2);
    let listed = admin.list_topics().await.unwrap();
    assert!(
        listed.iter().any(|t| t.name == "dti-qn"),
        "quota retry disabled must not delete the topic"
    );

    let created = admin
        .create_topics(
            &[
                NewTopic::new("dti-mix-ok", 1, 1),
                NewTopic::new("dti-mix-q", 1, 1),
            ],
            10_000,
            false,
        )
        .await
        .unwrap();
    assert_eq!(created[0].error_code, 0);
    assert_eq!(created[1].error_code, 0);
    let dti_mix_ok = created[0].topic_id;
    let dti_mix_q = created[1].topic_id;
    mock.delete_topics_quota_once("dti-mix-q");
    let deleted = admin
        .delete_topics_by_id_timeout_with_quota_retry(
            &[dti_mix_ok, dti_mix_q],
            Duration::from_secs(10),
            true,
        )
        .await
        .unwrap();
    assert_eq!(deleted[0].error_code, 0);
    assert_eq!(deleted[1].error_code, 0);
    assert_eq!(deleted[0].topic_id, dti_mix_ok);
    assert_eq!(deleted[1].topic_id, dti_mix_q);
    assert_eq!(
        mock.last_delete_topics_ids(),
        Some(1),
        "quota retry resends only THROTTLING_QUOTA_EXCEEDED topic ids"
    );
    assert_eq!(mock.delete_topics_quota_hits(), 3);
    admin.close().await.unwrap();
}

#[tokio::test]
async fn incremental_alter_configs_follows_controller() {
    let mock = common::Mock::start_two_node().await;
    mock.set_controller(2);
    let mut admin = Admin::connect(mock.addr.clone()).await.unwrap();
    let created = admin
        .create_topics(
            &[NewTopic::new("iac2", 1, 1), NewTopic::new("iac1", 1, 1)],
            10_000,
            false,
        )
        .await
        .unwrap();
    assert_eq!(created[0].error_code, 0);
    assert_eq!(created[1].error_code, 0);

    let err = admin
        .incremental_alter_configs(
            &ConfigResource::topic("iac2"),
            &[AlterConfig::set("retention.ms", "1000")],
            false,
        )
        .await
        .unwrap();
    assert_eq!(err, 0);
    assert_eq!(
        mock.last_incremental_alter_configs_version(),
        Some(1),
        "Admin must prefer IncrementalAlterConfigs v1 when the broker advertises it"
    );
    assert_eq!(
        mock.last_incremental_alter_configs_node(),
        Some(2),
        "IncrementalAlterConfigs must land on the controller, not bootstrap"
    );

    mock.set_controller(1);
    let again = admin
        .incremental_alter_configs(
            &ConfigResource::topic("iac1"),
            &[AlterConfig::set("retention.ms", "2000")],
            false,
        )
        .await
        .unwrap();
    assert_eq!(again, 0);
    assert_eq!(
        mock.incremental_alter_configs_not_controller(),
        1,
        "stale controller must return NOT_CONTROLLER (41) once"
    );
    assert_eq!(
        mock.last_incremental_alter_configs_node(),
        Some(1),
        "IncrementalAlterConfigs must follow Metadata after NOT_CONTROLLER"
    );
    admin.close().await.unwrap();
    mock.hide_api(INCREMENTAL_ALTER_CONFIGS);
    let mut admin = Admin::connect(mock.addr.clone()).await.unwrap();
    let err = admin
        .incremental_alter_configs(
            &ConfigResource::topic("iac2"),
            &[AlterConfig::set("retention.ms", "1000")],
            false,
        )
        .await
        .unwrap_err();
    assert!(
        err.to_string().contains("unsupported"),
        "IncrementalAlterConfigs is optional at connect: {err}"
    );
    let classic = admin
        .describe_classic_groups(&["g-classic"], false)
        .await
        .unwrap();
    assert_eq!(classic[0].group_id, "g-classic");
    admin.close().await.unwrap();
}

#[tokio::test]
async fn incremental_alter_configs_negotiates_v0_when_broker_caps() {
    let mock = common::Mock::start().await;
    mock.set_api_max(INCREMENTAL_ALTER_CONFIGS, 0);
    let mut admin = Admin::connect(mock.addr.clone()).await.unwrap();
    let created = admin
        .create_topics(&[NewTopic::new("iac0", 1, 1)], 10_000, false)
        .await
        .unwrap();
    assert_eq!(created[0].error_code, 0);
    let err = admin
        .incremental_alter_configs(
            &ConfigResource::topic("iac0"),
            &[AlterConfig::set("retention.ms", "1000")],
            false,
        )
        .await
        .unwrap();
    assert_eq!(err, 0);
    assert_eq!(
        mock.last_incremental_alter_configs_version(),
        Some(0),
        "client must speak IncrementalAlterConfigs v0 when the broker max is 0"
    );
}

#[tokio::test]
async fn admin_incremental_alter_configs_for() {
    let mock = common::Mock::start().await;
    let mut admin = Admin::connect(mock.addr.clone()).await.unwrap();
    let created = admin
        .create_topics(
            &[NewTopic::new("iac-a", 1, 1), NewTopic::new("iac-b", 1, 1)],
            10_000,
            false,
        )
        .await
        .unwrap();
    assert_eq!(created[0].error_code, 0);
    assert_eq!(created[1].error_code, 0);

    let empty = admin
        .incremental_alter_configs_for(&[], false)
        .await
        .unwrap();
    assert!(empty.is_empty());

    let results = admin
        .incremental_alter_configs_for(
            &[
                ConfigResourceUpdate::new(
                    ConfigResource::topic("iac-a"),
                    [AlterConfig::set("retention.ms", "1000")],
                ),
                ConfigResourceUpdate::new(
                    ConfigResource::topic("iac-b"),
                    [AlterConfig::set("retention.ms", "2000")],
                ),
            ],
            false,
        )
        .await
        .unwrap();
    assert_eq!(results.len(), 2);
    assert_eq!(results[0].error_code, 0);
    assert_eq!(results[0].name, "iac-a");
    assert_eq!(results[1].error_code, 0);
    assert_eq!(results[1].name, "iac-b");
    assert_eq!(
        mock.last_incremental_alter_configs_n(),
        Some(2),
        "incrementalAlterConfigs(Map) must send Resources of 2 in one RPC"
    );
    let timed = admin
        .incremental_alter_configs_for_timeout(
            &[ConfigResourceUpdate::new(
                ConfigResource::topic("iac-a"),
                [AlterConfig::set("retention.ms", "1000")],
            )],
            Duration::from_secs(5),
            false,
        )
        .await
        .unwrap();
    assert_eq!(timed[0].error_code, 0);

    let described = admin
        .describe_configs(
            &[
                ConfigResource::topic("iac-a").keys(["retention.ms"]),
                ConfigResource::topic("iac-b").keys(["retention.ms"]),
            ],
            false,
        )
        .await
        .unwrap();
    assert_eq!(
        described[0]
            .entries
            .iter()
            .find(|e| e.name == "retention.ms")
            .and_then(|e| e.value.as_deref()),
        Some("1000")
    );
    assert_eq!(
        described[1]
            .entries
            .iter()
            .find(|e| e.name == "retention.ms")
            .and_then(|e| e.value.as_deref()),
        Some("2000")
    );

    let from_entry = AlterConfig::from_entry(
        &ConfigEntry::new("retention.ms", Some("5000".into())),
        AlterConfigOpType::Set,
    );
    assert_eq!(from_entry.op_type(), Some(AlterConfigOpType::Set));
    assert_eq!(from_entry.config_entry().value.as_deref(), Some("5000"));
    let via_entry = admin
        .incremental_alter_configs_for(
            &[ConfigResourceUpdate::new(
                ConfigResource::topic("iac-a"),
                [from_entry],
            )],
            false,
        )
        .await
        .unwrap();
    assert_eq!(via_entry[0].error_code, 0);
    let described = admin
        .describe_configs(
            &[ConfigResource::topic("iac-a").keys(["retention.ms"])],
            false,
        )
        .await
        .unwrap();
    assert_eq!(
        described[0]
            .config()
            .get("retention.ms")
            .and_then(|e| e.value.as_deref()),
        Some("5000")
    );
    admin.close().await.unwrap();
}

#[tokio::test]
async fn create_acls_follows_controller() {
    let mock = common::Mock::start_two_node().await;
    mock.set_controller(2);
    let mut admin = Admin::connect(mock.addr.clone()).await.unwrap();
    let created = admin
        .create_acls(&[AclBinding::allow_topic("acl2", "User:alice")])
        .await
        .unwrap();
    assert_eq!(created, vec![0]);
    assert_eq!(
        mock.last_create_acls_version(),
        Some(3),
        "Admin must prefer CreateAcls v3 when the broker advertises it"
    );
    assert_eq!(
        mock.last_create_acls_node(),
        Some(2),
        "CreateAcls must land on the controller, not bootstrap"
    );

    mock.set_controller(1);
    let again = admin
        .create_acls(&[AclBinding::allow_topic("acl1", "User:bob")])
        .await
        .unwrap();
    assert_eq!(again, vec![0]);
    assert_eq!(
        mock.create_acls_not_controller(),
        1,
        "stale controller must return NOT_CONTROLLER (41) once"
    );
    assert_eq!(
        mock.last_create_acls_node(),
        Some(1),
        "CreateAcls must follow Metadata after NOT_CONTROLLER"
    );
}

#[tokio::test]
async fn acl_apis_negotiate_v0_when_broker_caps() {
    let mock = common::Mock::start().await;
    mock.set_api_max(CREATE_ACLS, 0);
    mock.set_api_max(DESCRIBE_ACLS, 0);
    mock.set_api_max(DELETE_ACLS, 0);
    let mut admin = Admin::connect(mock.addr.clone()).await.unwrap();
    let created = admin
        .create_acls(&[AclBinding::allow_topic("acl0", "User:alice")])
        .await
        .unwrap();
    assert_eq!(created, vec![0]);
    assert_eq!(
        mock.last_create_acls_version(),
        Some(0),
        "client must speak CreateAcls v0 when the broker max is 0"
    );
    let listed = admin.describe_acls(AclResourceType::Topic).await.unwrap();
    assert_eq!(listed.len(), 1);
    assert_eq!(
        mock.last_describe_acls_version(),
        Some(0),
        "client must speak DescribeAcls v0 when the broker max is 0"
    );
    assert_eq!(admin.delete_acls(AclResourceType::Topic).await.unwrap(), 0);
    assert_eq!(
        mock.last_delete_acls_version(),
        Some(0),
        "client must speak DeleteAcls v0 when the broker max is 0"
    );
}

#[tokio::test]
async fn alter_partition_reassignments_follows_controller() {
    let mock = common::Mock::start_two_node().await;
    mock.set_controller(2);
    let mut admin = Admin::connect(mock.addr.clone()).await.unwrap();
    let created = admin
        .create_topics(
            &[NewTopic::new("re2", 1, 1), NewTopic::new("re1", 1, 1)],
            10_000,
            false,
        )
        .await
        .unwrap();
    assert_eq!(created[0].error_code, 0);
    assert_eq!(created[1].error_code, 0);

    let results = admin
        .alter_partition_reassignments(
            &[PartitionReassignment::assign(
                TopicPartition::new("re2", 0),
                [2, 1],
            )],
            10_000,
        )
        .await
        .unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].error_code, 0);
    assert_eq!(
        mock.last_alter_reassignments_node(),
        Some(2),
        "AlterPartitionReassignments must land on the controller, not bootstrap"
    );
    assert_eq!(
        mock.last_reassignment(),
        Some(("re2".into(), 0, Some(vec![2, 1]))),
        "controller must store the replica list"
    );

    mock.set_controller(1);
    let again = admin
        .alter_partition_reassignments(
            &[PartitionReassignment::assign(
                TopicPartition::new("re1", 0),
                [1, 2],
            )],
            10_000,
        )
        .await
        .unwrap();
    assert_eq!(again.len(), 1);
    assert_eq!(again[0].error_code, 0);
    assert_eq!(
        mock.alter_reassignments_not_controller(),
        1,
        "stale controller must return NOT_CONTROLLER (41) once"
    );
    assert_eq!(
        mock.last_alter_reassignments_node(),
        Some(1),
        "AlterPartitionReassignments must follow Metadata after NOT_CONTROLLER"
    );
    assert_eq!(
        mock.last_reassignment(),
        Some(("re1".into(), 0, Some(vec![1, 2]))),
        "retry on the new controller must store the replica list"
    );
    let mapped = NewPartitionReassignment::new([1, 2]).unwrap();
    assert_eq!(mapped.target_replicas(), &[1, 2]);
    let from_map = admin
        .alter_partition_reassignments_for([(TopicPartition::new("re1", 0), Some(mapped))], 10_000)
        .await
        .unwrap();
    assert_eq!(from_map.len(), 1);
    assert_eq!(from_map[0].error_code(), 0);
    assert_eq!(from_map[0].topic(), "re1");
    assert_eq!(from_map[0].partition(), 0);
    assert_eq!(
        mock.last_reassignment(),
        Some(("re1".into(), 0, Some(vec![1, 2]))),
        "alterPartitionReassignments(Map) must send the target replicas"
    );
    let cancelled = admin
        .alter_partition_reassignments_for([(TopicPartition::new("re1", 0), None)], 10_000)
        .await
        .unwrap();
    assert_eq!(cancelled[0].error_code(), 0);
    assert_eq!(
        mock.last_reassignment(),
        Some(("re1".into(), 0, None)),
        "Optional.empty() cancels the reassignment"
    );
    admin.close().await.unwrap();
    mock.hide_api(ALTER_PARTITION_REASSIGNMENTS);
    let mut admin = Admin::connect(mock.addr.clone()).await.unwrap();
    let err = admin
        .alter_partition_reassignments(
            &[PartitionReassignment::assign(
                TopicPartition::new("re2", 0),
                [2, 1],
            )],
            10_000,
        )
        .await
        .unwrap_err();
    assert!(
        err.to_string().contains("unsupported"),
        "AlterPartitionReassignments is optional at connect: {err}"
    );
    let classic = admin
        .describe_classic_groups(&["g-classic"], false)
        .await
        .unwrap();
    assert_eq!(classic[0].group_id, "g-classic");
    admin.close().await.unwrap();
}

#[tokio::test]
async fn list_partition_reassignments_follows_controller() {
    let mock = common::Mock::start_two_node().await;
    mock.set_controller(2);
    let mut admin = Admin::connect(mock.addr.clone()).await.unwrap();
    let created = admin
        .create_topics(&[NewTopic::new("lr2", 1, 1)], 10_000, false)
        .await
        .unwrap();
    assert_eq!(created[0].error_code, 0);

    let assigned = admin
        .alter_partition_reassignments(
            &[PartitionReassignment::assign(
                TopicPartition::new("lr2", 0),
                [2, 1],
            )],
            10_000,
        )
        .await
        .unwrap();
    assert_eq!(assigned[0].error_code, 0);

    let listed = admin
        .list_partition_reassignments(Some(&[TopicPartition::new("lr2", 0)]), 10_000)
        .await
        .unwrap();
    assert_eq!(
        listed,
        vec![OngoingReassignment {
            topic: "lr2".into(),
            partition: 0,
            replicas: vec![2, 1],
            adding_replicas: vec![],
            removing_replicas: vec![],
        }]
    );
    assert_eq!(listed[0].topic(), "lr2");
    assert_eq!(listed[0].partition(), 0);
    assert_eq!(listed[0].replicas(), &[2, 1]);
    assert!(listed[0].adding_replicas().is_empty());
    assert!(listed[0].removing_replicas().is_empty());
    assert_eq!(
        listed[0].to_string(),
        "PartitionReassignment(replicas=[2, 1], addingReplicas=[], removingReplicas=[])"
    );
    assert_eq!(
        mock.last_list_reassignments_node(),
        Some(2),
        "ListPartitionReassignments must land on the controller, not bootstrap"
    );

    mock.set_controller(1);
    let again = admin
        .list_partition_reassignments(Some(&[TopicPartition::new("lr2", 0)]), 10_000)
        .await
        .unwrap();
    assert_eq!(
        again,
        vec![OngoingReassignment {
            topic: "lr2".into(),
            partition: 0,
            replicas: vec![2, 1],
            adding_replicas: vec![],
            removing_replicas: vec![],
        }]
    );
    assert_eq!(
        mock.list_reassignments_not_controller(),
        1,
        "stale controller must return NOT_CONTROLLER (41) once"
    );
    assert_eq!(
        mock.last_list_reassignments_node(),
        Some(1),
        "ListPartitionReassignments must follow Metadata after NOT_CONTROLLER"
    );
    let for_parts = admin
        .list_partition_reassignments_for(&[TopicPartition::new("lr2", 0)])
        .await
        .unwrap();
    assert_eq!(for_parts.len(), 1);
    assert_eq!(
        mock.last_list_reassignments_topics(),
        Some(Some(vec![("lr2".into(), vec![0])])),
        "listPartitionReassignments(Set) sends those Topics"
    );
    assert_eq!(
        mock.last_list_reassignments_timeout(),
        Some(30_000),
        "listPartitionReassignments(Set) uses request_timeout for TimeoutMs"
    );
    let all = admin.list_partition_reassignments_all().await.unwrap();
    assert_eq!(all.len(), 1);
    assert_eq!(
        mock.last_list_reassignments_topics(),
        Some(None),
        "listPartitionReassignments() sends Topics null"
    );
    assert_eq!(
        mock.last_list_reassignments_timeout(),
        Some(30_000),
        "listPartitionReassignments() uses request_timeout for TimeoutMs"
    );
    let timed_all = admin
        .list_partition_reassignments_all_timeout(Duration::from_secs(5))
        .await
        .unwrap();
    assert_eq!(timed_all.len(), 1);
    assert_eq!(mock.last_list_reassignments_timeout(), Some(5_000));
    admin.close().await.unwrap();
    mock.hide_api(LIST_PARTITION_REASSIGNMENTS);
    let mut admin = Admin::connect(mock.addr.clone()).await.unwrap();
    let err = admin.list_partition_reassignments_all().await.unwrap_err();
    assert!(
        err.to_string().contains("unsupported"),
        "ListPartitionReassignments is optional at connect: {err}"
    );
    let classic = admin
        .describe_classic_groups(&["g-classic"], false)
        .await
        .unwrap();
    assert_eq!(classic[0].group_id, "g-classic");
    admin.close().await.unwrap();
}

#[tokio::test]
async fn admin_alter_list_partition_reassignments_timeout() {
    let mock = common::Mock::start().await;
    let mut admin = Admin::connect(mock.addr.clone()).await.unwrap();
    let created = admin
        .create_topics(&[NewTopic::new("re-to", 1, 1)], 10_000, false)
        .await
        .unwrap();
    assert_eq!(created[0].error_code, 0);

    let assigned = admin
        .alter_partition_reassignments(
            &[PartitionReassignment::assign(
                TopicPartition::new("re-to", 0),
                [1],
            )],
            10_000,
        )
        .await
        .unwrap();
    assert_eq!(assigned[0].error_code, 0);
    assert_eq!(mock.last_alter_reassignments_timeout(), Some(10_000));
    let assigned = admin
        .alter_partition_reassignments_timeout(
            &[PartitionReassignment::assign(
                TopicPartition::new("re-to", 0),
                [1],
            )],
            Duration::from_millis(1_500),
        )
        .await
        .unwrap();
    assert_eq!(assigned[0].error_code, 0);
    assert_eq!(mock.last_alter_reassignments_timeout(), Some(1_500));
    let assigned = admin
        .alter_partition_reassignments_for_timeout(
            [(
                TopicPartition::new("re-to", 0),
                Some(NewPartitionReassignment::new([1]).unwrap()),
            )],
            Duration::from_millis(1_750),
        )
        .await
        .unwrap();
    assert_eq!(assigned[0].error_code(), 0);
    assert_eq!(mock.last_alter_reassignments_timeout(), Some(1_750));

    let listed = admin
        .list_partition_reassignments(Some(&[TopicPartition::new("re-to", 0)]), 10_000)
        .await
        .unwrap();
    assert_eq!(listed.len(), 1);
    assert_eq!(mock.last_list_reassignments_timeout(), Some(10_000));
    let listed = admin
        .list_partition_reassignments_timeout(
            Some(&[TopicPartition::new("re-to", 0)]),
            Duration::from_millis(2_500),
        )
        .await
        .unwrap();
    assert_eq!(listed.len(), 1);
    assert_eq!(mock.last_list_reassignments_timeout(), Some(2_500));
    let all = admin
        .list_partition_reassignments_all_timeout(Duration::from_millis(3_500))
        .await
        .unwrap();
    assert_eq!(all.len(), 1);
    assert_eq!(mock.last_list_reassignments_timeout(), Some(3_500));
    assert_eq!(
        mock.last_list_reassignments_topics(),
        Some(None),
        "listPartitionReassignments() sends Topics null"
    );
    admin.close().await.unwrap();
}

#[tokio::test]
async fn update_features_follows_controller() {
    let mock = common::Mock::start_two_node().await;
    mock.set_controller(2);
    let mut admin = Admin::connect(mock.addr.clone()).await.unwrap();

    let update = FeatureUpdate::new("metadata.version", 17);
    assert_eq!(
        update.to_string(),
        "FeatureUpdate{maxVersionLevel:17, upgradeType:UPGRADE}"
    );
    let results = admin.update_features(&[update], 10_000).await.unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].error_code, 0);
    assert_eq!(
        mock.last_update_features_node(),
        Some(2),
        "UpdateFeatures must land on the controller, not bootstrap"
    );
    assert_eq!(
        mock.last_update_features_version(),
        Some(2),
        "Admin must prefer UpdateFeatures v2 when the broker advertises it"
    );
    assert_eq!(
        mock.last_feature_update(),
        Some(("metadata.version".into(), 17, false)),
        "controller must store the feature update"
    );
    assert_eq!(mock.feature_level("metadata.version"), Some(17));

    mock.set_controller(1);
    let again = admin
        .update_features(&[FeatureUpdate::new("group.version", 1)], 10_000)
        .await
        .unwrap();
    assert_eq!(again.len(), 1);
    assert_eq!(again[0].error_code, 0);
    assert_eq!(
        mock.update_features_not_controller(),
        1,
        "stale controller must return NOT_CONTROLLER (41) once"
    );
    assert_eq!(
        mock.last_update_features_node(),
        Some(1),
        "UpdateFeatures must follow Metadata after NOT_CONTROLLER"
    );
    assert_eq!(
        mock.last_feature_update(),
        Some(("group.version".into(), 1, false)),
        "retry on the new controller must store the feature update"
    );
    assert_eq!(
        mock.feature_level("metadata.version"),
        Some(17),
        "first hop mutation must stay"
    );
    assert_eq!(mock.feature_level("group.version"), Some(1));
    admin.close().await.unwrap();
    mock.hide_api(UPDATE_FEATURES);
    let mut admin = Admin::connect(mock.addr.clone()).await.unwrap();
    let err = admin
        .update_features(&[FeatureUpdate::new("metadata.version", 17)], 10_000)
        .await
        .unwrap_err();
    assert!(
        err.to_string().contains("unsupported"),
        "UpdateFeatures is optional at connect: {err}"
    );
    let classic = admin
        .describe_classic_groups(&["g-classic"], false)
        .await
        .unwrap();
    assert_eq!(classic[0].group_id, "g-classic");
    admin.close().await.unwrap();
}

#[tokio::test]
async fn update_features_negotiates_v0_when_broker_caps() {
    let mock = common::Mock::start().await;
    mock.set_api_max(UPDATE_FEATURES, 0);
    let mut admin = Admin::connect(mock.addr.clone()).await.unwrap();
    let results = admin
        .update_features(&[FeatureUpdate::new("metadata.version", 17)], 10_000)
        .await
        .unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].error_code, 0);
    assert_eq!(
        mock.last_update_features_version(),
        Some(0),
        "client must speak UpdateFeatures v0 when the broker max is 0"
    );
    assert_eq!(
        mock.last_update_features_validate_only(),
        Some(false),
        "UpdateFeatures v0 has no ValidateOnly"
    );
}

#[tokio::test]
async fn update_features_negotiates_v1_when_broker_caps() {
    let mock = common::Mock::start().await;
    mock.set_api_max(UPDATE_FEATURES, 1);
    let mut admin = Admin::connect(mock.addr.clone()).await.unwrap();
    let results = admin
        .update_features(&[FeatureUpdate::new("metadata.version", 17)], 10_000)
        .await
        .unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].error_code, 0);
    assert_eq!(
        mock.last_update_features_version(),
        Some(1),
        "client must speak UpdateFeatures v1 when the broker max is 1"
    );
    assert_eq!(
        mock.last_update_features_upgrade_type(),
        Some(1),
        "UpdateFeatures v1 must send UpgradeType upgrade"
    );
}

#[tokio::test]
async fn update_features_with_sends_validate_only_and_upgrade_type() {
    let mock = common::Mock::start().await;
    let mut admin = Admin::connect(mock.addr.clone()).await.unwrap();
    let safe = FeatureUpdate::new("metadata.version", 17).upgrade_type(UpgradeType::SafeDowngrade);
    assert_eq!(
        safe.to_string(),
        "FeatureUpdate{maxVersionLevel:17, upgradeType:SAFE_DOWNGRADE}"
    );
    let results = admin
        .update_features_with(&[safe], 10_000, true)
        .await
        .unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].error_code, 0);
    assert_eq!(
        mock.last_update_features_version(),
        Some(2),
        "update_features_with must keep UpdateFeatures v2"
    );
    assert_eq!(
        mock.last_update_features_validate_only(),
        Some(true),
        "update_features_with must send ValidateOnly on v1+"
    );
    assert_eq!(
        mock.last_update_features_upgrade_type(),
        Some(2),
        "update_features_with must send UpgradeType safe downgrade"
    );
    assert_eq!(
        mock.feature_level("metadata.version"),
        None,
        "validate_only must not apply the feature mutation"
    );
}

#[tokio::test]
async fn admin_update_features_timeout() {
    let mock = common::Mock::start().await;
    let mut admin = Admin::connect(mock.addr.clone()).await.unwrap();
    let results = admin
        .update_features(&[FeatureUpdate::new("metadata.version", 17)], 10_000)
        .await
        .unwrap();
    assert_eq!(results[0].error_code, 0);
    assert_eq!(mock.last_update_features_timeout(), Some(10_000));
    let results = admin
        .update_features_timeout(
            &[FeatureUpdate::new("group.version", 1)],
            Duration::from_millis(1_500),
        )
        .await
        .unwrap();
    assert_eq!(results[0].error_code, 0);
    assert_eq!(mock.last_update_features_timeout(), Some(1_500));
    let results = admin
        .update_features_with_timeout(
            &[FeatureUpdate::new("transaction.version", 2)],
            Duration::from_millis(2_500),
            true,
        )
        .await
        .unwrap();
    assert_eq!(results[0].error_code, 0);
    assert_eq!(mock.last_update_features_timeout(), Some(2_500));
    assert_eq!(mock.last_update_features_validate_only(), Some(true));
    assert_eq!(
        mock.feature_level("transaction.version"),
        None,
        "update_features_with_timeout validate_only must not apply the mutation"
    );
}

#[tokio::test]
async fn alter_user_scram_credentials_follows_controller() {
    let mock = common::Mock::start_two_node().await;
    mock.set_controller(2);
    let mut admin = Admin::connect(mock.addr.clone()).await.unwrap();

    let alice = UserScramCredentialUpsertion::new(
        "alice",
        ScramMechanism::Sha256,
        4096,
        b"dummy-salt".to_vec(),
        b"dummy-salted".to_vec(),
    );
    let results = admin
        .alter_user_scram_credentials(&[], &[alice])
        .await
        .unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].error_code, 0);
    assert_eq!(
        mock.last_alter_user_scram_node(),
        Some(2),
        "AlterUserScramCredentials must land on the controller, not bootstrap"
    );
    assert_eq!(
        mock.last_scram_upsert(),
        Some(("alice".into(), SCRAM_SHA_256, 4096)),
        "controller must store the SCRAM upsert"
    );
    assert!(mock.has_scram_credential("alice", SCRAM_SHA_256));
    assert_eq!(mock.scram_iterations("alice", SCRAM_SHA_256), Some(4096));

    mock.set_controller(1);
    let bob = UserScramCredentialUpsertion::new(
        "bob",
        SCRAM_SHA_512,
        4096,
        b"dummy-salt-b".to_vec(),
        b"dummy-salted-b".to_vec(),
    );
    let delete_carol = UserScramCredentialDeletion::new("carol", ScramMechanism::Sha256);
    let again = admin
        .alter_user_scram_credentials(&[delete_carol], &[bob])
        .await
        .unwrap();
    assert_eq!(again.len(), 2);
    assert_eq!(again[0].error_code, 0);
    assert_eq!(again[1].error_code, 0);
    assert_eq!(
        mock.alter_user_scram_not_controller(),
        1,
        "stale controller must return NOT_CONTROLLER (41) once"
    );
    assert_eq!(
        mock.last_alter_user_scram_node(),
        Some(1),
        "AlterUserScramCredentials must follow Metadata after NOT_CONTROLLER"
    );
    assert_eq!(
        mock.last_scram_upsert(),
        Some(("bob".into(), SCRAM_SHA_512, 4096)),
        "retry on the new controller must store the SCRAM upsert"
    );
    assert_eq!(
        mock.last_scram_delete(),
        Some(("carol".into(), SCRAM_SHA_256)),
        "retry on the new controller must apply the SCRAM delete"
    );
    assert!(
        mock.has_scram_credential("alice", SCRAM_SHA_256),
        "first hop mutation must stay"
    );
    assert!(mock.has_scram_credential("bob", SCRAM_SHA_512));
    assert!(!mock.has_scram_credential("carol", SCRAM_SHA_256));
    let dave = UserScramCredentialUpsertion::new(
        "dave",
        ScramMechanism::Sha256,
        4096,
        b"dummy-salt-d".to_vec(),
        b"dummy-salted-d".to_vec(),
    );
    let timed = admin
        .alter_user_scram_credentials_timeout(&[], &[dave], Duration::from_secs(5))
        .await
        .unwrap();
    assert_eq!(timed.len(), 1);
    assert_eq!(timed[0].error_code, 0);
    assert!(mock.has_scram_credential("dave", SCRAM_SHA_256));
    let frank = UserScramCredentialUpsertion::new(
        "frank",
        ScramMechanism::Sha256,
        4096,
        b"dummy-salt-f".to_vec(),
        b"dummy-salted-f".to_vec(),
    );
    let delete_gina = UserScramCredentialDeletion::new("gina", ScramMechanism::Sha256);
    let mixed = admin
        .alter_user_scram_credentials_with([
            UserScramCredentialAlteration::Deletion(delete_gina),
            UserScramCredentialAlteration::Upsertion(frank),
        ])
        .await
        .unwrap();
    assert_eq!(mixed.len(), 2);
    assert_eq!(mixed[0].error_code, 0);
    assert_eq!(mixed[1].error_code, 0);
    assert_eq!(
        mock.last_scram_upsert(),
        Some(("frank".into(), SCRAM_SHA_256, 4096)),
        "alterUserScramCredentials(List) must split Upsertions on the wire"
    );
    assert_eq!(
        mock.last_scram_delete(),
        Some(("gina".into(), SCRAM_SHA_256)),
        "alterUserScramCredentials(List) must split Deletions on the wire"
    );
    assert!(mock.has_scram_credential("frank", SCRAM_SHA_256));
    assert!(!mock.has_scram_credential("gina", SCRAM_SHA_256));
    let hank = UserScramCredentialUpsertion::new(
        "hank",
        ScramMechanism::Sha256,
        4096,
        b"dummy-salt-h".to_vec(),
        b"dummy-salted-h".to_vec(),
    );
    let timed_with = admin
        .alter_user_scram_credentials_with_timeout(
            [UserScramCredentialAlteration::from(hank)],
            Duration::from_secs(5),
        )
        .await
        .unwrap();
    assert_eq!(timed_with.len(), 1);
    assert_eq!(timed_with[0].error_code, 0);
    assert!(mock.has_scram_credential("hank", SCRAM_SHA_256));
    admin.close().await.unwrap();
    mock.hide_api(ALTER_USER_SCRAM_CREDENTIALS);
    let mut admin = Admin::connect(mock.addr.clone()).await.unwrap();
    let eve = UserScramCredentialUpsertion::new(
        "eve",
        ScramMechanism::Sha256,
        4096,
        b"dummy-salt-e".to_vec(),
        b"dummy-salted-e".to_vec(),
    );
    let err = admin
        .alter_user_scram_credentials(&[], &[eve])
        .await
        .unwrap_err();
    assert!(
        err.to_string().contains("unsupported"),
        "AlterUserScramCredentials is optional at connect: {err}"
    );
    let classic = admin
        .describe_classic_groups(&["g-classic"], false)
        .await
        .unwrap();
    assert_eq!(classic[0].group_id, "g-classic");
    admin.close().await.unwrap();
}

#[tokio::test]
async fn describe_user_scram_credentials_follows_controller() {
    let mock = common::Mock::start_two_node().await;
    mock.set_controller(2);
    mock.set_scram_fixture("alice", SCRAM_SHA_256, 4096);
    mock.set_scram_fixture("bob", SCRAM_SHA_512, 4096);
    let mut admin = Admin::connect(mock.addr.clone()).await.unwrap();

    let first = admin
        .describe_user_scram_credentials(&["alice"])
        .await
        .unwrap();
    assert_eq!(first.len(), 1);
    assert_eq!(first[0].user, "alice");
    assert_eq!(first[0].error_code, 0);
    assert_eq!(first[0].credential_infos.len(), 1);
    assert_eq!(first[0].credential_infos[0].mechanism, SCRAM_SHA_256);
    assert_eq!(first[0].credential_infos[0].iterations, 4096);
    assert_eq!(
        first[0].credential_infos[0].to_string(),
        "ScramCredentialInfo{mechanism=SCRAM_SHA_256, iterations=4096}"
    );
    assert_eq!(
        first[0].to_string(),
        "UserScramCredentialsDescription{name='alice', credentialInfos=[ScramCredentialInfo{mechanism=SCRAM_SHA_256, iterations=4096}]}"
    );
    assert_eq!(
        mock.last_describe_user_scram_node(),
        Some(2),
        "DescribeUserScramCredentials must land on the controller, not bootstrap"
    );
    assert_eq!(
        mock.last_describe_user_scram_users(),
        Some(Some(vec!["alice".into()])),
        "named describeUserScramCredentials sends Users of those names"
    );

    mock.set_controller(1);
    let again = admin
        .describe_user_scram_credentials(&["alice", "bob"])
        .await
        .unwrap();
    assert_eq!(again.len(), 2);
    assert_eq!(again[0].user, "alice");
    assert_eq!(again[0].error_code, 0);
    assert_eq!(again[0].credential_infos[0].iterations, 4096);
    assert_eq!(again[1].user, "bob");
    assert_eq!(again[1].credential_infos[0].mechanism, SCRAM_SHA_512);
    assert_eq!(
        mock.describe_user_scram_not_controller(),
        1,
        "stale controller must return NOT_CONTROLLER (41) once"
    );
    assert_eq!(
        mock.last_describe_user_scram_node(),
        Some(1),
        "DescribeUserScramCredentials must follow Metadata after NOT_CONTROLLER"
    );
    let timed = admin
        .describe_user_scram_credentials_timeout(&["alice"], Duration::from_secs(5))
        .await
        .unwrap();
    assert_eq!(timed.len(), 1);
    assert_eq!(timed[0].user, "alice");
    assert_eq!(timed[0].error_code, 0);
    let all = admin.describe_user_scram_credentials_all().await.unwrap();
    assert_eq!(all.len(), 2);
    assert_eq!(all[0].user, "alice");
    assert_eq!(all[1].user, "bob");
    assert_eq!(
        mock.last_describe_user_scram_users(),
        Some(None),
        "describeUserScramCredentials() sends Users null"
    );
    let timed_all = admin
        .describe_user_scram_credentials_all_timeout(Duration::from_secs(5))
        .await
        .unwrap();
    assert_eq!(timed_all.len(), 2);
    admin.close().await.unwrap();
    mock.hide_api(DESCRIBE_USER_SCRAM_CREDENTIALS);
    let mut admin = Admin::connect(mock.addr.clone()).await.unwrap();
    let err = admin
        .describe_user_scram_credentials(&["alice"])
        .await
        .unwrap_err();
    assert!(
        err.to_string().contains("unsupported"),
        "DescribeUserScramCredentials is optional at connect: {err}"
    );
    let classic = admin
        .describe_classic_groups(&["g-classic"], false)
        .await
        .unwrap();
    assert_eq!(classic[0].group_id, "g-classic");
    admin.close().await.unwrap();
}

#[tokio::test]
async fn unregister_broker_follows_controller() {
    let mock = common::Mock::start_two_node().await;
    mock.set_controller(2);
    let mut admin = Admin::connect(mock.addr.clone()).await.unwrap();

    admin.unregister_broker(3).await.unwrap();
    assert_eq!(
        mock.last_unregister_broker_node(),
        Some(2),
        "UnregisterBroker must land on the controller, not bootstrap"
    );
    assert_eq!(mock.last_unregistered_broker_id(), Some(3));
    assert!(
        mock.has_unregistered_broker(3),
        "controller must record the fixture unregistration"
    );

    mock.set_controller(1);
    admin.unregister_broker(4).await.unwrap();
    assert_eq!(
        mock.unregister_broker_not_controller(),
        1,
        "stale controller must return NOT_CONTROLLER (41) once"
    );
    assert_eq!(
        mock.last_unregister_broker_node(),
        Some(1),
        "UnregisterBroker must follow Metadata after NOT_CONTROLLER"
    );
    assert_eq!(mock.last_unregistered_broker_id(), Some(4));
    assert!(
        mock.has_unregistered_broker(3),
        "first hop unregistration must stay"
    );
    assert!(
        mock.has_unregistered_broker(4),
        "retry on the new controller must record the fixture unregistration"
    );
    admin
        .unregister_broker_timeout(5, Duration::from_secs(5))
        .await
        .unwrap();
    assert_eq!(mock.last_unregistered_broker_id(), Some(5));
    assert!(
        mock.has_unregistered_broker(5),
        "timeout overload must record the fixture unregistration"
    );
    admin.close().await.unwrap();
    mock.hide_api(UNREGISTER_BROKER);
    let mut admin = Admin::connect(mock.addr.clone()).await.unwrap();
    let err = admin.unregister_broker(6).await.unwrap_err();
    assert!(
        err.to_string().contains("unsupported"),
        "UnregisterBroker is optional at connect: {err}"
    );
    let classic = admin
        .describe_classic_groups(&["g-classic"], false)
        .await
        .unwrap();
    assert_eq!(classic[0].group_id, "g-classic");
    admin.close().await.unwrap();
}

#[tokio::test]
async fn describe_client_quotas_follows_broker() {
    let mock = common::Mock::start_two_node().await;
    mock.set_controller(2);
    let mut admin = Admin::connect(mock.addr.clone()).await.unwrap();

    let filter = ClientQuotaFilterComponent::of_entity(ClientQuotaEntity::USER, "alice");
    assert_eq!(
        filter.to_string(),
        "ClientQuotaFilterComponent(entityType=user, match=Optional[alice])"
    );
    let entries = admin
        .describe_client_quotas(std::slice::from_ref(&filter), false)
        .await
        .unwrap();
    assert_eq!(entries.len(), 1);
    assert_eq!(
        entries[0].entity(),
        [ClientQuotaEntity::new(
            ClientQuotaEntity::USER,
            Some("alice".into())
        )]
    );
    assert_eq!(
        entries[0].entity()[0].to_string(),
        "ClientQuotaEntity(entries={user=alice})"
    );
    assert_eq!(entries[0].values().len(), 1);
    assert_eq!(entries[0].values()[0].key(), "producer_byte_rate");
    assert_eq!(entries[0].values()[0].value(), 1024.0);
    assert_eq!(
        mock.last_describe_client_quotas_node(),
        Some(1),
        "DescribeClientQuotas must land on the connected broker, not the controller"
    );
    assert_eq!(
        mock.last_describe_client_quotas_version(),
        Some(1),
        "Admin must prefer DescribeClientQuotas v1 when the broker advertises it"
    );
    assert_eq!(
        mock.last_describe_client_quotas(),
        Some((vec![filter], false))
    );
    assert_eq!(
        mock.last_alter_client_quotas_node(),
        None,
        "DescribeClientQuotas must not hop via AlterClientQuotas or Metadata controller_id"
    );
    let timed_filter = ClientQuotaFilterComponent::of_entity(ClientQuotaEntity::USER, "alice");
    let timed = admin
        .describe_client_quotas_timeout(
            std::slice::from_ref(&timed_filter),
            false,
            Duration::from_secs(5),
        )
        .await
        .unwrap();
    assert_eq!(timed.len(), 1);
    assert_eq!(timed[0].values()[0].key(), "producer_byte_rate");
    let all = admin.describe_client_quotas_all().await.unwrap();
    assert_eq!(all.len(), 1);
    assert_eq!(
        ClientQuotaFilter::all().to_string(),
        "ClientQuotaFilter(components=[], strict=false)"
    );
    assert_eq!(
        mock.last_describe_client_quotas(),
        Some((vec![], false)),
        "ClientQuotaFilter.all() sends empty components and strict false"
    );
    let timed_all = admin
        .describe_client_quotas_all_timeout(Duration::from_secs(5))
        .await
        .unwrap();
    assert_eq!(timed_all.len(), 1);
    let contains_comp = ClientQuotaFilterComponent::of_entity(ClientQuotaEntity::USER, "alice");
    let contains = admin
        .describe_client_quotas_with(&ClientQuotaFilter::contains([contains_comp.clone()]))
        .await
        .unwrap();
    assert_eq!(contains.len(), 1);
    assert_eq!(
        mock.last_describe_client_quotas(),
        Some((vec![contains_comp.clone()], false)),
        "ClientQuotaFilter.contains sends components and strict false"
    );
    let only = admin
        .describe_client_quotas_with(&ClientQuotaFilter::contains_only([contains_comp.clone()]))
        .await
        .unwrap();
    assert_eq!(only.len(), 1);
    assert_eq!(
        mock.last_describe_client_quotas(),
        Some((vec![contains_comp.clone()], true)),
        "ClientQuotaFilter.containsOnly sends strict true"
    );
    let timed_with = admin
        .describe_client_quotas_with_timeout(
            &ClientQuotaFilter::contains([contains_comp]),
            Duration::from_secs(5),
        )
        .await
        .unwrap();
    assert_eq!(timed_with.len(), 1);
    let default = ClientQuotaFilterComponent::of_default_entity(ClientQuotaEntity::USER);
    let defaulted = admin
        .describe_client_quotas_with(&ClientQuotaFilter::contains([default.clone()]))
        .await
        .unwrap();
    assert_eq!(defaulted.len(), 1);
    assert_eq!(
        mock.last_describe_client_quotas(),
        Some((vec![default], false)),
        "ofDefaultEntity sends MatchType default and a null match"
    );
    let any = ClientQuotaFilterComponent::of_entity_type(ClientQuotaEntity::CLIENT_ID);
    let any_listed = admin
        .describe_client_quotas_with(&ClientQuotaFilter::contains([any.clone()]))
        .await
        .unwrap();
    assert_eq!(any_listed.len(), 1);
    assert_eq!(
        mock.last_describe_client_quotas(),
        Some((vec![any], false)),
        "ofEntityType sends MatchType any and a null match"
    );
    admin.close().await.unwrap();
    mock.hide_api(DESCRIBE_CLIENT_QUOTAS);
    let mut admin = Admin::connect(mock.addr.clone()).await.unwrap();
    let hidden = ClientQuotaFilterComponent::of_entity(ClientQuotaEntity::USER, "alice");
    let err = admin
        .describe_client_quotas(std::slice::from_ref(&hidden), false)
        .await
        .unwrap_err();
    assert!(
        err.to_string().contains("unsupported"),
        "DescribeClientQuotas is optional at connect: {err}"
    );
    let classic = admin
        .describe_classic_groups(&["g-classic"], false)
        .await
        .unwrap();
    assert_eq!(classic[0].group_id, "g-classic");
    admin.close().await.unwrap();
}

#[tokio::test]
async fn describe_client_quotas_negotiates_v0_when_broker_caps() {
    let mock = common::Mock::start().await;
    mock.set_api_max(DESCRIBE_CLIENT_QUOTAS, 0);
    let mut admin = Admin::connect(mock.addr.clone()).await.unwrap();
    let filter = ClientQuotaFilterComponent::of_entity(ClientQuotaEntity::USER, "alice");
    let entries = admin
        .describe_client_quotas(std::slice::from_ref(&filter), false)
        .await
        .unwrap();
    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0].values()[0].key(), "producer_byte_rate");
    assert_eq!(
        mock.last_describe_client_quotas_version(),
        Some(0),
        "client must speak DescribeClientQuotas v0 when the broker max is 0"
    );
}

#[tokio::test]
async fn alter_client_quotas_follows_controller() {
    let mock = common::Mock::start_two_node().await;
    mock.set_controller(2);
    let mut admin = Admin::connect(mock.addr.clone()).await.unwrap();

    let alice = ClientQuotaAlteration::new(
        vec![ClientQuotaEntity::new(
            ClientQuotaEntity::USER,
            Some("alice".into()),
        )],
        vec![ClientQuotaOp::set("producer_byte_rate", 1024.0)],
    );
    assert_eq!(alice.entity()[0].entity_type(), ClientQuotaEntity::USER);
    assert_eq!(alice.ops()[0].key(), "producer_byte_rate");
    assert_eq!(alice.ops()[0].value(), Some(1024.0));
    let results = admin.alter_client_quotas(&[alice], false).await.unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].error_code(), 0);
    assert_eq!(
        mock.last_alter_client_quotas_node(),
        Some(2),
        "AlterClientQuotas must land on the controller, not bootstrap"
    );
    assert_eq!(
        mock.last_alter_client_quotas_version(),
        Some(1),
        "Admin must prefer AlterClientQuotas v1 when the broker advertises it"
    );
    assert_eq!(
        mock.last_quota_upsert(),
        Some((
            "user".into(),
            Some("alice".into()),
            "producer_byte_rate".into(),
            1024.0
        )),
        "controller must store the quota upsert"
    );
    assert!(mock.has_quota_fixture("user", Some("alice"), "producer_byte_rate"));

    mock.set_controller(1);
    let bob = ClientQuotaAlteration::new(
        vec![ClientQuotaEntity::new("user", Some("bob".into()))],
        vec![ClientQuotaOp::set("consumer_byte_rate", 2048.0)],
    );
    let delete_carol = ClientQuotaAlteration::new(
        vec![ClientQuotaEntity::new("user", Some("carol".into()))],
        vec![ClientQuotaOp::remove("producer_byte_rate")],
    );
    assert!(delete_carol.ops()[0].value().is_none());
    let again = admin
        .alter_client_quotas(&[bob, delete_carol], false)
        .await
        .unwrap();
    assert_eq!(again.len(), 2);
    assert_eq!(again[0].error_code(), 0);
    assert_eq!(again[1].error_code(), 0);
    assert_eq!(
        mock.alter_client_quotas_not_controller(),
        1,
        "stale controller must return NOT_CONTROLLER (41) once"
    );
    assert_eq!(
        mock.last_alter_client_quotas_node(),
        Some(1),
        "AlterClientQuotas must follow Metadata after NOT_CONTROLLER"
    );
    assert_eq!(
        mock.last_quota_upsert(),
        Some((
            "user".into(),
            Some("bob".into()),
            "consumer_byte_rate".into(),
            2048.0
        )),
        "retry on the new controller must store the quota upsert"
    );
    assert_eq!(
        mock.last_quota_delete(),
        Some((
            "user".into(),
            Some("carol".into()),
            "producer_byte_rate".into()
        )),
        "retry on the new controller must apply the quota delete"
    );
    assert!(
        mock.has_quota_fixture("user", Some("alice"), "producer_byte_rate"),
        "first hop mutation must stay"
    );
    assert!(mock.has_quota_fixture("user", Some("bob"), "consumer_byte_rate"));
    assert!(!mock.has_quota_fixture("user", Some("carol"), "producer_byte_rate"));
    let dave = ClientQuotaAlteration::new(
        vec![ClientQuotaEntity::new("user", Some("dave".into()))],
        vec![ClientQuotaOp::set("producer_byte_rate", 4096.0)],
    );
    let timed = admin
        .alter_client_quotas_timeout(&[dave], Duration::from_secs(5), false)
        .await
        .unwrap();
    assert_eq!(timed.len(), 1);
    assert_eq!(timed[0].error_code, 0);
    assert!(mock.has_quota_fixture("user", Some("dave"), "producer_byte_rate"));
    admin.close().await.unwrap();
    mock.hide_api(ALTER_CLIENT_QUOTAS);
    let mut admin = Admin::connect(mock.addr.clone()).await.unwrap();
    let eve = ClientQuotaAlteration::new(
        vec![ClientQuotaEntity::new("user", Some("eve".into()))],
        vec![ClientQuotaOp::set("producer_byte_rate", 1.0)],
    );
    let err = admin.alter_client_quotas(&[eve], false).await.unwrap_err();
    assert!(
        err.to_string().contains("unsupported"),
        "AlterClientQuotas is optional at connect: {err}"
    );
    let classic = admin
        .describe_classic_groups(&["g-classic"], false)
        .await
        .unwrap();
    assert_eq!(classic[0].group_id, "g-classic");
    admin.close().await.unwrap();
}

#[tokio::test]
async fn alter_client_quotas_negotiates_v0_when_broker_caps() {
    let mock = common::Mock::start().await;
    mock.set_api_max(ALTER_CLIENT_QUOTAS, 0);
    let mut admin = Admin::connect(mock.addr.clone()).await.unwrap();
    let alice = ClientQuotaAlteration::new(
        vec![ClientQuotaEntity::new("user", Some("alice".into()))],
        vec![ClientQuotaOp::set("producer_byte_rate", 1024.0)],
    );
    let results = admin.alter_client_quotas(&[alice], false).await.unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].error_code, 0);
    assert_eq!(
        mock.last_alter_client_quotas_version(),
        Some(0),
        "client must speak AlterClientQuotas v0 when the broker max is 0"
    );
}

#[tokio::test]
async fn allocate_producer_ids_follows_controller() {
    let mock = common::Mock::start_two_node().await;
    mock.set_controller(2);
    let mut admin = Admin::connect(mock.addr.clone()).await.unwrap();

    let first = admin.allocate_producer_ids(7, 42).await.unwrap();
    assert_eq!(first.producer_id_start, 1000);
    assert_eq!(first.producer_id_len, 1000);
    assert_eq!(first.producer_id_start(), 1000);
    assert_eq!(first.producer_id_len(), 1000);
    assert_eq!(
        mock.last_allocate_producer_ids_node(),
        Some(2),
        "AllocateProducerIds must land on the controller, not bootstrap"
    );
    assert_eq!(
        mock.last_allocate_producer_ids(),
        Some((7, 42, 1000, 1000)),
        "controller must store/hand the first fixture PID block"
    );

    mock.set_controller(1);
    let again = admin.allocate_producer_ids(7, 42).await.unwrap();
    assert_eq!(again.producer_id_start, 2000);
    assert_eq!(again.producer_id_len, 1000);
    assert_eq!(
        mock.allocate_producer_ids_not_controller(),
        1,
        "stale controller must return NOT_CONTROLLER (41) once"
    );
    assert_eq!(
        mock.last_allocate_producer_ids_node(),
        Some(1),
        "AllocateProducerIds must follow Metadata after NOT_CONTROLLER"
    );
    assert_eq!(
        mock.last_allocate_producer_ids(),
        Some((7, 42, 2000, 1000)),
        "retry on the new controller must hand the next PID block"
    );
    let timed = admin
        .allocate_producer_ids_timeout(7, 42, Duration::from_secs(5))
        .await
        .unwrap();
    assert_eq!(timed.producer_id_start, 3000);
    assert_eq!(timed.producer_id_len, 1000);
    assert_eq!(
        mock.last_allocate_producer_ids_node(),
        Some(1),
        "allocate_producer_ids_timeout must stay on the controller"
    );
    assert_eq!(mock.last_allocate_producer_ids(), Some((7, 42, 3000, 1000)));
}

#[tokio::test]
async fn describe_transactions_follows_coordinator() {
    let mock = common::Mock::start_two_node().await;
    mock.set_txn_coordinator(2);
    mock.set_txn_fixture(TransactionState {
        error_code: 0,
        transactional_id: "tx-desc".into(),
        transaction_state: "Ongoing".into(),
        transaction_timeout_ms: 60_000,
        transaction_start_time_ms: 1_700_000_000_000,
        producer_id: 1001,
        producer_epoch: 3,
        topics: vec![TransactionTopic {
            name: "orders".into(),
            partitions: vec![0, 1],
        }],
    });
    let mut admin = Admin::connect(mock.addr.clone()).await.unwrap();

    let first = admin.describe_transactions(&["tx-desc"]).await.unwrap();
    assert_eq!(first.len(), 1);
    assert_eq!(first[0].error_code(), 0);
    assert_eq!(first[0].transactional_id(), "tx-desc");
    assert_eq!(first[0].state(), "Ongoing");
    assert_eq!(first[0].producer_id(), 1001);
    assert_eq!(first[0].producer_epoch(), 3);
    assert_eq!(first[0].transaction_timeout_ms(), 60_000);
    assert_eq!(
        first[0].transaction_start_time_ms(),
        Some(1_700_000_000_000)
    );
    assert_eq!(first[0].topics()[0].name(), "orders");
    assert_eq!(first[0].topics()[0].partitions(), &[0, 1]);
    assert_eq!(
        mock.last_describe_transactions_node(),
        Some(2),
        "DescribeTransactions must land on the transaction coordinator, not bootstrap"
    );
    assert!(
        mock.find_coordinator_key_types()
            .contains(&COORDINATOR_TRANSACTION),
        "DescribeTransactions must FindCoordinator key_type=1"
    );

    mock.move_txn_coordinator();
    let again = admin.describe_transactions(&["tx-desc"]).await.unwrap();
    assert_eq!(again.len(), 1);
    assert_eq!(again[0].error_code, 0);
    assert_eq!(again[0].producer_id, 1001);
    assert_eq!(
        again[0].topics[0].name.as_str(),
        "orders",
        "retry on the new coordinator must still return fixture state, not the 16 empty body"
    );
    assert_eq!(
        mock.describe_transactions_not_coordinator(),
        1,
        "stale coordinator must return NOT_COORDINATOR (16) once"
    );
    assert_eq!(
        mock.last_describe_transactions_node(),
        Some(1),
        "DescribeTransactions must FindCoordinator after NOT_COORDINATOR"
    );
    let timed = admin
        .describe_transactions_timeout(&["tx-desc"], Duration::from_secs(5))
        .await
        .unwrap();
    assert_eq!(timed.len(), 1);
    assert_eq!(timed[0].transactional_id, "tx-desc");
    admin.close().await.unwrap();
    mock.hide_api(DESCRIBE_TRANSACTIONS);
    let mut admin = Admin::connect(mock.addr.clone()).await.unwrap();
    let err = admin.describe_transactions(&["tx-desc"]).await.unwrap_err();
    assert!(
        err.to_string().contains("unsupported"),
        "DescribeTransactions is optional at connect: {err}"
    );
    let classic = admin
        .describe_classic_groups(&["g-classic"], false)
        .await
        .unwrap();
    assert_eq!(classic[0].group_id, "g-classic");
    admin.close().await.unwrap();
}

#[tokio::test]
async fn describe_transactions_batches_find_coordinator() {
    let mock = common::Mock::start().await;
    mock.set_txn_fixture(TransactionState {
        error_code: 0,
        transactional_id: "tx-a".into(),
        transaction_state: "Ongoing".into(),
        transaction_timeout_ms: 60_000,
        transaction_start_time_ms: 1,
        producer_id: 1,
        producer_epoch: 0,
        topics: Vec::new(),
    });
    mock.set_txn_fixture(TransactionState {
        error_code: 0,
        transactional_id: "tx-b".into(),
        transaction_state: "Ongoing".into(),
        transaction_timeout_ms: 60_000,
        transaction_start_time_ms: 1,
        producer_id: 2,
        producer_epoch: 0,
        topics: Vec::new(),
    });
    let mut admin = Admin::connect(mock.addr.clone()).await.unwrap();
    let before_find = mock.find_coordinator_calls();
    let before_desc = mock.describe_transactions_calls();
    let described = admin
        .describe_transactions(&["tx-a", "tx-b"])
        .await
        .unwrap();
    assert_eq!(described.len(), 2);
    assert_eq!(described[0].transactional_id, "tx-a");
    assert_eq!(described[1].transactional_id, "tx-b");
    assert_eq!(described[0].error_code, 0);
    assert_eq!(described[1].error_code, 0);
    assert_eq!(
        mock.last_find_coordinator_key_count(),
        2,
        "describeTransactions must send CoordinatorKeys of N on v4+"
    );
    assert_eq!(
        mock.find_coordinator_calls().saturating_sub(before_find),
        1,
        "transactional ids that share a coordinator must be one FindCoordinator"
    );
    assert_eq!(
        mock.last_describe_transactions_n(),
        2,
        "describeTransactions must send TransactionalIds of N on one coordinator"
    );
    assert_eq!(
        mock.describe_transactions_calls()
            .saturating_sub(before_desc),
        1,
        "transactional ids that share a coordinator must be one DescribeTransactions"
    );
    admin.close().await.unwrap();
}

#[tokio::test]
async fn list_transactions_follows_coordinator() {
    let mock = common::Mock::start_two_node().await;
    mock.set_txn_coordinator(2);
    mock.set_txn_fixture(TransactionState {
        error_code: 0,
        transactional_id: "tx-list".into(),
        transaction_state: "Ongoing".into(),
        transaction_timeout_ms: 60_000,
        transaction_start_time_ms: 1_700_000_000_000,
        producer_id: 1001,
        producer_epoch: 3,
        topics: vec![TransactionTopic {
            name: "orders".into(),
            partitions: vec![0, 1],
        }],
    });
    let mut admin = Admin::connect(mock.addr.clone()).await.unwrap();

    let first = admin.list_transactions(&[], &[]).await.unwrap();
    assert_eq!(first.len(), 1);
    assert_eq!(first[0].transactional_id(), "tx-list");
    assert_eq!(first[0].state(), "Ongoing");
    assert_eq!(first[0].producer_id(), 1001);
    assert_eq!(
        first[0].to_string(),
        "TransactionListing(transactionalId='tx-list', producerId=1001, transactionState=Ongoing)"
    );
    assert_eq!(
        mock.last_list_transactions_node(),
        Some(2),
        "ListTransactions must land on the transaction coordinator, not bootstrap"
    );
    assert_eq!(
        mock.last_list_transactions_version(),
        Some(1),
        "Admin must prefer ListTransactions v1 when the broker advertises it"
    );
    assert_eq!(
        mock.last_list_transactions_duration(),
        Some(-1),
        "list_transactions must send DurationFilter -1 (no filter)"
    );
    assert!(
        mock.find_coordinator_key_types()
            .contains(&COORDINATOR_TRANSACTION),
        "ListTransactions must FindCoordinator key_type=1"
    );

    mock.move_txn_coordinator();
    let again = admin.list_transactions(&[], &[]).await.unwrap();
    assert_eq!(again.len(), 1);
    assert_eq!(again[0].producer_id, 1001);
    assert_eq!(
        again[0].transactional_id.as_str(),
        "tx-list",
        "retry on the new coordinator must still return fixture txn ids, not the 16 empty body"
    );
    assert_eq!(
        mock.list_transactions_not_coordinator(),
        1,
        "stale coordinator must return NOT_COORDINATOR (16) once"
    );
    assert_eq!(
        mock.last_list_transactions_node(),
        Some(1),
        "ListTransactions must FindCoordinator after NOT_COORDINATOR"
    );
    let timed = admin
        .list_transactions_timeout(&[], &[], Duration::from_secs(5))
        .await
        .unwrap();
    assert_eq!(timed.len(), 1);
    assert_eq!(timed[0].transactional_id, "tx-list");
    let all = admin.list_transactions_all().await.unwrap();
    assert_eq!(all.len(), 1);
    assert_eq!(all[0].transactional_id, "tx-list");
    assert_eq!(
        mock.last_list_transactions_duration(),
        Some(-1),
        "listTransactions() sends DurationFilter -1"
    );
    let timed_all = admin
        .list_transactions_all_timeout(Duration::from_secs(5))
        .await
        .unwrap();
    assert_eq!(timed_all.len(), 1);
    admin.close().await.unwrap();
    mock.hide_api(LIST_TRANSACTIONS);
    let mut admin = Admin::connect(mock.addr.clone()).await.unwrap();
    let err = admin.list_transactions_all().await.unwrap_err();
    assert!(
        err.to_string().contains("unsupported"),
        "ListTransactions is optional at connect: {err}"
    );
    let classic = admin
        .describe_classic_groups(&["g-classic"], false)
        .await
        .unwrap();
    assert_eq!(classic[0].group_id, "g-classic");
    admin.close().await.unwrap();
}

#[tokio::test]
async fn list_transactions_negotiates_v1_duration_filter() {
    let mock = common::Mock::start().await;
    let mut admin = Admin::connect(mock.addr.clone()).await.unwrap();
    let listed = admin.list_transactions(&[], &[]).await.unwrap();
    assert!(listed.is_empty());
    assert_eq!(
        mock.last_list_transactions_version(),
        Some(1),
        "Admin must prefer ListTransactions v1 when the broker advertises it"
    );
    assert_eq!(
        mock.last_list_transactions_duration(),
        Some(-1),
        "list_transactions must send DurationFilter -1 (no filter)"
    );

    let filtered = admin
        .list_transactions_with_duration(&[], &[], 5000)
        .await
        .unwrap();
    assert!(filtered.is_empty());
    assert_eq!(
        mock.last_list_transactions_version(),
        Some(1),
        "list_transactions_with_duration must keep ListTransactions v1"
    );
    assert_eq!(
        mock.last_list_transactions_duration(),
        Some(5000),
        "list_transactions_with_duration must send DurationFilter on v1"
    );
    let timed = admin
        .list_transactions_timeout(&[], &[], Duration::from_secs(5))
        .await
        .unwrap();
    assert!(timed.is_empty());
    assert_eq!(
        mock.last_list_transactions_duration(),
        Some(-1),
        "list_transactions_timeout must keep DurationFilter -1"
    );
    let timed_filter = admin
        .list_transactions_with_duration_timeout(&[], &[], 5000, Duration::from_secs(5))
        .await
        .unwrap();
    assert!(timed_filter.is_empty());
    assert_eq!(
        mock.last_list_transactions_duration(),
        Some(5000),
        "list_transactions_with_duration_timeout must send DurationFilter on v1"
    );

    let mock = common::Mock::start().await;
    mock.set_api_max(LIST_TRANSACTIONS, 0);
    let mut admin = Admin::connect(mock.addr.clone()).await.unwrap();
    let capped = admin
        .list_transactions_with_duration(&[], &[], 5000)
        .await
        .unwrap();
    assert!(capped.is_empty());
    assert_eq!(
        mock.last_list_transactions_version(),
        Some(0),
        "client must speak ListTransactions v0 when the broker max is 0"
    );
    assert_eq!(
        mock.last_list_transactions_duration(),
        Some(-1),
        "v0 omits DurationFilter even when the caller passed a duration"
    );
}

#[tokio::test]
async fn consumer_group_describe_follows_group_coordinator() {
    let mock = common::Mock::start_two_node().await;
    mock.move_coordinator();
    let mut admin = Admin::connect(mock.addr.clone()).await.unwrap();

    let first = admin
        .consumer_group_describe(&["cg-desc"], false)
        .await
        .unwrap();
    assert_eq!(first.len(), 1);
    assert_eq!(first[0].error_code, 0);
    assert_eq!(first[0].group_id, "cg-desc");
    assert_eq!(first[0].group_state, "Stable");
    assert_eq!(first[0].group_epoch, 1);
    assert_eq!(first[0].assignor_name, "uniform");
    assert_eq!(
        mock.last_consumer_group_describe_node(),
        Some(2),
        "ConsumerGroupDescribe must land on the group coordinator, not bootstrap"
    );
    assert_eq!(
        mock.last_consumer_group_describe_version(),
        Some(1),
        "Admin must prefer ConsumerGroupDescribe v1 when the broker advertises it"
    );
    assert!(
        mock.find_coordinator_key_types()
            .contains(&COORDINATOR_GROUP),
        "ConsumerGroupDescribe must FindCoordinator key_type=0"
    );

    mock.move_coordinator();
    let again = admin
        .consumer_group_describe(&["cg-desc"], false)
        .await
        .unwrap();
    assert_eq!(again.len(), 1);
    assert_eq!(again[0].error_code, 0);
    assert_eq!(
        again[0].group_state.as_str(),
        "Stable",
        "retry on the new coordinator must still return fixture state, not the 16 empty body"
    );
    assert_eq!(
        mock.consumer_group_describe_not_coordinator(),
        1,
        "stale coordinator must return NOT_COORDINATOR (16) once"
    );
    assert_eq!(
        mock.last_consumer_group_describe_node(),
        Some(1),
        "ConsumerGroupDescribe must FindCoordinator after NOT_COORDINATOR"
    );
    let timed = admin
        .consumer_group_describe_timeout(&["cg-desc"], false, Duration::from_secs(5))
        .await
        .unwrap();
    assert_eq!(timed.len(), 1);
    assert_eq!(timed[0].group_id, "cg-desc");
}

#[tokio::test]
async fn consumer_group_describe_batches_find_coordinator() {
    let mock = common::Mock::start().await;
    let mut admin = Admin::connect(mock.addr.clone()).await.unwrap();
    let before_find = mock.find_coordinator_calls();
    let before_desc = mock.consumer_group_describe_calls();
    let described = admin
        .consumer_group_describe(&["g-a", "g-b"], false)
        .await
        .unwrap();
    assert_eq!(described.len(), 2);
    assert_eq!(described[0].group_id, "g-a");
    assert_eq!(described[1].group_id, "g-b");
    assert_eq!(described[0].error_code, 0);
    assert_eq!(described[1].error_code, 0);
    assert_eq!(
        mock.last_find_coordinator_key_count(),
        2,
        "consumerGroupDescribe must send CoordinatorKeys of N on v4+"
    );
    assert_eq!(
        mock.find_coordinator_calls().saturating_sub(before_find),
        1,
        "groups that share a coordinator must be one FindCoordinator"
    );
    assert_eq!(
        mock.last_consumer_group_describe_n(),
        2,
        "consumerGroupDescribe must send GroupIds of N on one coordinator"
    );
    assert_eq!(
        mock.consumer_group_describe_calls()
            .saturating_sub(before_desc),
        1,
        "groups that share a coordinator must be one ConsumerGroupDescribe"
    );
    admin.close().await.unwrap();
}

#[tokio::test]
async fn consumer_group_describe_negotiates_v0_when_broker_caps() {
    let mock = common::Mock::start().await;
    mock.set_api_max(CONSUMER_GROUP_DESCRIBE, 0);
    let mut admin = Admin::connect(mock.addr.clone()).await.unwrap();
    let described = admin
        .consumer_group_describe(&["cg-v0"], false)
        .await
        .unwrap();
    assert_eq!(described.len(), 1);
    assert_eq!(described[0].error_code, 0);
    assert_eq!(described[0].group_id, "cg-v0");
    assert_eq!(
        mock.last_consumer_group_describe_version(),
        Some(0),
        "client must speak ConsumerGroupDescribe v0 when the broker max is 0"
    );
}

#[tokio::test]
async fn describe_groups_follows_group_coordinator() {
    let mock = common::Mock::start_two_node().await;
    mock.move_coordinator();
    let mut admin = Admin::connect(mock.addr.clone()).await.unwrap();

    let first = admin.describe_groups(&["g-desc"], false).await.unwrap();
    assert_eq!(first.len(), 1);
    assert_eq!(first[0].error_code, 0);
    assert_eq!(first[0].group_id, "g-desc");
    assert_eq!(first[0].group_state, "Stable");
    assert_eq!(first[0].protocol_type, "consumer");
    assert_eq!(
        mock.last_describe_groups_node(),
        Some(2),
        "DescribeGroups must land on the group coordinator, not bootstrap"
    );
    assert_eq!(
        mock.last_describe_groups_version(),
        Some(6),
        "Admin must prefer DescribeGroups v6 when the broker advertises it"
    );
    assert!(
        mock.find_coordinator_key_types()
            .contains(&COORDINATOR_GROUP),
        "DescribeGroups must FindCoordinator key_type=0"
    );

    mock.move_coordinator();
    let again = admin.describe_groups(&["g-desc"], false).await.unwrap();
    assert_eq!(again.len(), 1);
    assert_eq!(again[0].error_code, 0);
    assert_eq!(
        again[0].group_state.as_str(),
        "Stable",
        "retry on the new coordinator must still return fixture state, not the 16 empty body"
    );
    assert_eq!(
        mock.describe_groups_not_coordinator(),
        1,
        "stale coordinator must return NOT_COORDINATOR (16) once"
    );
    assert_eq!(
        mock.last_describe_groups_node(),
        Some(1),
        "DescribeGroups must FindCoordinator after NOT_COORDINATOR"
    );
}

#[tokio::test]
async fn describe_groups_batches_find_coordinator() {
    let mock = common::Mock::start().await;
    let mut admin = Admin::connect(mock.addr.clone()).await.unwrap();
    let before_find = mock.find_coordinator_calls();
    let before_desc = mock.describe_groups_calls();
    let described = admin.describe_groups(&["g-a", "g-b"], false).await.unwrap();
    assert_eq!(described.len(), 2);
    assert_eq!(described[0].group_id, "g-a");
    assert_eq!(described[1].group_id, "g-b");
    assert_eq!(described[0].error_code, 0);
    assert_eq!(described[1].error_code, 0);
    assert_eq!(
        mock.last_find_coordinator_key_count(),
        2,
        "describeGroups must send CoordinatorKeys of N on v4+"
    );
    assert_eq!(
        mock.find_coordinator_calls().saturating_sub(before_find),
        1,
        "groups that share a coordinator must be one FindCoordinator"
    );
    assert_eq!(
        mock.last_describe_groups_n(),
        2,
        "describeGroups must send Groups of N on one coordinator"
    );
    assert_eq!(
        mock.describe_groups_calls().saturating_sub(before_desc),
        1,
        "groups that share a coordinator must be one DescribeGroups"
    );
    admin.close().await.unwrap();
}

#[tokio::test]
async fn describe_groups_negotiates_v5_when_broker_caps() {
    let mock = common::Mock::start().await;
    mock.set_api_max(DESCRIBE_GROUPS, 5);
    let mut admin = Admin::connect(mock.addr.clone()).await.unwrap();
    let described = admin.describe_groups(&["g-v5"], true).await.unwrap();
    assert_eq!(described.len(), 1);
    assert_eq!(described[0].error_code, 0);
    assert!(
        described[0].error_message.is_none(),
        "v5 has no ErrorMessage"
    );
    assert_eq!(
        mock.last_describe_groups_version(),
        Some(5),
        "client must speak DescribeGroups v5 when the broker max is 5"
    );
    assert_eq!(
        mock.last_describe_groups_include(),
        Some(true),
        "v5 must send IncludeAuthorizedOperations"
    );
}

#[tokio::test]
async fn describe_groups_negotiates_v4_when_broker_caps() {
    let mock = common::Mock::start().await;
    mock.set_api_max(DESCRIBE_GROUPS, 4);
    let mut admin = Admin::connect(mock.addr.clone()).await.unwrap();
    let described = admin.describe_groups(&["g-v4"], true).await.unwrap();
    assert_eq!(described.len(), 1);
    assert_eq!(described[0].error_code, 0);
    assert_eq!(
        mock.last_describe_groups_version(),
        Some(4),
        "client must speak DescribeGroups v4 when the broker max is 4"
    );
    assert_eq!(
        mock.last_describe_groups_include(),
        Some(true),
        "v4 must send IncludeAuthorizedOperations"
    );
}

#[tokio::test]
async fn describe_groups_negotiates_v0_when_broker_caps() {
    let mock = common::Mock::start().await;
    mock.set_api_max(DESCRIBE_GROUPS, 0);
    let mut admin = Admin::connect(mock.addr.clone()).await.unwrap();
    let described = admin.describe_groups(&["g-v0"], true).await.unwrap();
    assert_eq!(described.len(), 1);
    assert_eq!(described[0].error_code, 0);
    assert_eq!(
        described[0].authorized_operations,
        i32::MIN,
        "v0 has no AuthorizedOperations; decode fills omitted"
    );
    assert_eq!(
        mock.last_describe_groups_version(),
        Some(0),
        "client must speak DescribeGroups v0 when the broker max is 0"
    );
    assert_eq!(
        mock.last_describe_groups_include(),
        Some(false),
        "v0 has no IncludeAuthorizedOperations; decode fills false"
    );
}

#[tokio::test]
async fn describe_groups_negotiates_v2_when_broker_caps() {
    let mock = common::Mock::start().await;
    mock.set_api_max(DESCRIBE_GROUPS, 2);
    let mut admin = Admin::connect(mock.addr.clone()).await.unwrap();
    let described = admin.describe_groups(&["g-v2"], true).await.unwrap();
    assert_eq!(described[0].error_code, 0);
    assert_eq!(
        mock.last_describe_groups_version(),
        Some(2),
        "client must speak DescribeGroups v2 when the broker max is 2"
    );
    assert_eq!(
        mock.last_describe_groups_include(),
        Some(false),
        "v2 must not send IncludeAuthorizedOperations"
    );
}

#[tokio::test]
async fn admin_remove_all_members_from_consumer_group() {
    let mock = common::Mock::start_two_node().await;
    mock.move_coordinator();
    let group = ConsumerGroup::join(
        ConsumerConfig::bootstrap([mock.addr.clone()])
            .max_wait_ms(10)
            .group_instance_id("i-all"),
        "g-rm-all",
        "t",
    )
    .await
    .unwrap();
    let mut admin = Admin::connect(mock.addr.clone()).await.unwrap();
    let removed = admin
        .remove_all_members_from_consumer_group("g-rm-all")
        .await
        .unwrap();
    assert_eq!(removed.len(), 1);
    assert_eq!(removed[0].error_code, 0);
    assert_eq!(removed[0].group_instance_id.as_deref(), Some("i-all"));
    assert!(!removed[0].member_id.is_empty());
    assert_eq!(
        mock.last_describe_groups_node(),
        Some(2),
        "removeAll DescribeGroups must land on the group coordinator"
    );
    assert_eq!(
        mock.last_leave_group_node(),
        Some(2),
        "removeAll LeaveGroup must land on the group coordinator"
    );
    let members = mock.last_leave_group_members().expect("LeaveGroup members");
    assert_eq!(members.len(), 1);
    assert_eq!(members[0].group_instance_id.as_deref(), Some("i-all"));
    assert!(!members[0].member_id.is_empty());
    assert_eq!(
        mock.last_leave_group_version(),
        Some(5),
        "removeAll LeaveGroup must prefer v5 when the broker advertises it"
    );
    assert_eq!(
        members[0].reason.as_deref(),
        Some(DEFAULT_LEAVE_GROUP_REASON)
    );
    let timed = admin
        .remove_all_members_from_consumer_group_timeout("g-rm-all", Duration::from_secs(5))
        .await
        .unwrap();
    assert!(
        timed.is_empty(),
        "timeout overload after removeAll is a no-op when the group is empty"
    );
    admin.close().await.unwrap();
    group.close().await.unwrap();
}

#[tokio::test]
async fn list_groups_follows_broker() {
    let mock = common::Mock::start_two_node().await;
    mock.move_coordinator();
    mock.set_controller(2);
    let mut admin = Admin::connect(mock.addr.clone()).await.unwrap();

    let first = admin.list_groups(&["Stable"], &["classic"]).await.unwrap();
    assert_eq!(first.len(), 1);
    assert_eq!(first[0].group_id, "g");
    assert_eq!(first[0].protocol_type, "consumer");
    assert_eq!(first[0].group_state, "Stable");
    assert_eq!(first[0].group_type, "classic");
    assert_eq!(
        first[0].to_string(),
        "(groupId='g', type=Classic, protocol='consumer', groupState=Stable)"
    );
    assert_eq!(
        mock.last_list_groups_node(),
        Some(1),
        "ListGroups must land on the connected broker, not the coordinator or controller"
    );
    assert_eq!(
        mock.last_list_groups_version(),
        Some(5),
        "Admin must prefer ListGroups v5 when the broker advertises it"
    );
    assert_eq!(
        mock.last_list_groups(),
        Some((vec!["Stable".into()], vec!["classic".into()]))
    );
    assert_eq!(
        mock.last_describe_groups_node(),
        None,
        "ListGroups must not hop via DescribeGroups or FindCoordinator"
    );
    assert_eq!(
        mock.last_alter_client_quotas_node(),
        None,
        "ListGroups must not hop via Metadata controller_id"
    );
    let timed = admin
        .list_groups_timeout(&["Stable"], &["classic"], Duration::from_secs(5))
        .await
        .unwrap();
    assert_eq!(timed.len(), 1);
    assert_eq!(timed[0].group_id, "g");
    let all = admin.list_groups_all().await.unwrap();
    assert_eq!(all.len(), 1);
    assert_eq!(
        mock.last_list_groups(),
        Some((vec![], vec![])),
        "listGroups() sends empty StatesFilter and TypesFilter"
    );
    let timed_all = admin
        .list_groups_all_timeout(Duration::from_secs(5))
        .await
        .unwrap();
    assert_eq!(timed_all.len(), 1);
}

#[tokio::test]
async fn list_groups_negotiates_v4_when_broker_caps() {
    let mock = common::Mock::start().await;
    mock.set_api_max(LIST_GROUPS, 4);
    let mut admin = Admin::connect(mock.addr.clone()).await.unwrap();
    let listed = admin.list_groups(&["Stable"], &["classic"]).await.unwrap();
    assert_eq!(listed.len(), 1);
    assert_eq!(listed[0].group_id, "g");
    assert_eq!(listed[0].group_state, "Stable");
    assert!(
        listed[0].group_type.is_empty(),
        "v4 has no GroupType; decode fills empty"
    );
    assert_eq!(
        listed[0].to_string(),
        "(groupId='g', type=none, protocol='consumer', groupState=Stable)"
    );
    assert_eq!(
        mock.last_list_groups_version(),
        Some(4),
        "client must speak ListGroups v4 when the broker max is 4"
    );
    assert_eq!(
        mock.last_list_groups(),
        Some((vec!["Stable".into()], vec![])),
        "v4 must send StatesFilter and omit TypesFilter"
    );
}

#[tokio::test]
async fn list_groups_negotiates_v3_when_broker_caps() {
    let mock = common::Mock::start().await;
    mock.set_api_max(LIST_GROUPS, 3);
    let mut admin = Admin::connect(mock.addr.clone()).await.unwrap();
    let listed = admin.list_groups(&["Stable"], &["classic"]).await.unwrap();
    assert_eq!(listed[0].group_id, "g");
    assert!(listed[0].group_state.is_empty(), "v3 has no GroupState");
    assert!(listed[0].group_type.is_empty(), "v3 has no GroupType");
    assert_eq!(
        mock.last_list_groups_version(),
        Some(3),
        "client must speak ListGroups v3 when the broker max is 3"
    );
    assert_eq!(
        mock.last_list_groups(),
        Some((vec![], vec![])),
        "v3 must omit StatesFilter and TypesFilter"
    );
}

#[tokio::test]
async fn list_groups_negotiates_v0_when_broker_caps() {
    let mock = common::Mock::start().await;
    mock.set_api_max(LIST_GROUPS, 0);
    let mut admin = Admin::connect(mock.addr.clone()).await.unwrap();
    let listed = admin.list_groups(&["Stable"], &["classic"]).await.unwrap();
    assert_eq!(listed[0].group_id, "g");
    assert_eq!(listed[0].protocol_type, "consumer");
    assert!(listed[0].group_state.is_empty());
    assert!(listed[0].group_type.is_empty());
    assert_eq!(
        mock.last_list_groups_version(),
        Some(0),
        "client must speak ListGroups v0 when the broker max is 0"
    );
    assert_eq!(mock.last_list_groups(), Some((vec![], vec![])));
}

#[tokio::test]
async fn list_consumer_groups_follows_broker() {
    let mock = common::Mock::start_two_node().await;
    mock.move_coordinator();
    mock.set_controller(2);
    let mut admin = Admin::connect(mock.addr.clone()).await.unwrap();

    let first = admin
        .list_consumer_groups(&["Stable"], &["classic"])
        .await
        .unwrap();
    assert_eq!(first.len(), 1);
    assert_eq!(first[0].group_id, "g");
    assert_eq!(first[0].group_id(), "g");
    assert_eq!(
        mock.last_list_groups_node(),
        Some(1),
        "listConsumerGroups must land on the connected broker"
    );
    assert_eq!(
        mock.last_list_groups(),
        Some((vec!["Stable".into()], vec!["classic".into()]))
    );
    assert_eq!(
        mock.last_describe_groups_node(),
        None,
        "listConsumerGroups must not hop via DescribeGroups or FindCoordinator"
    );
    let timed = admin
        .list_consumer_groups_timeout(&["Stable"], &["classic"], Duration::from_secs(5))
        .await
        .unwrap();
    assert_eq!(timed.len(), 1);
    assert_eq!(timed[0].group_id, "g");
    let all = admin.list_consumer_groups_all().await.unwrap();
    assert_eq!(all.len(), 1);
    assert_eq!(
        mock.last_list_groups(),
        Some((vec![], vec![])),
        "listConsumerGroups() sends empty StatesFilter and TypesFilter"
    );
    let timed_all = admin
        .list_consumer_groups_all_timeout(Duration::from_secs(5))
        .await
        .unwrap();
    assert_eq!(timed_all.len(), 1);

    assert_eq!(first[0].group_state(), GroupState::Stable);
    assert_eq!(first[0].group_type(), GroupType::Classic);
    let typed = admin
        .list_groups_with([GroupState::Stable], [GroupType::Classic])
        .await
        .unwrap();
    assert_eq!(typed.len(), 1);
    assert_eq!(
        mock.last_list_groups(),
        Some((vec!["Stable".into()], vec!["Classic".into()])),
        "listGroups(inGroupStates, withTypes) must send Java toString values"
    );
    let timed_typed = admin
        .list_groups_with_timeout(
            [GroupState::Stable],
            [GroupType::Classic],
            Duration::from_secs(5),
        )
        .await
        .unwrap();
    assert_eq!(timed_typed.len(), 1);
    let cg_typed = admin
        .list_consumer_groups_with([GroupState::Empty], [GroupType::Consumer])
        .await
        .unwrap();
    assert_eq!(cg_typed.len(), 1);
    assert_eq!(
        mock.last_list_groups(),
        Some((vec!["Empty".into()], vec!["Consumer".into()]))
    );
    let cg_timed = admin
        .list_consumer_groups_with_timeout(
            [GroupState::Empty],
            [GroupType::Share],
            Duration::from_secs(5),
        )
        .await
        .unwrap();
    assert_eq!(cg_timed.len(), 1);
}

#[tokio::test]
async fn describe_topic_partitions_follows_broker() {
    let mock = common::Mock::start_two_node().await;
    mock.move_coordinator();
    mock.set_controller(2);
    let mut admin = Admin::connect(mock.addr.clone()).await.unwrap();

    let first = admin
        .describe_topic_partitions(&["t"], 2000, None)
        .await
        .unwrap();
    assert_eq!(first.topics.len(), 1);
    assert_eq!(first.topics[0].name.as_deref(), Some("t"));
    assert_eq!(first.topics[0].error_code, 0);
    assert_eq!(first.topics[0].partitions.len(), 1);
    assert_eq!(first.topics[0].partitions[0].error_code, 0);
    assert_eq!(first.topics[0].partitions[0].partition_index, 0);
    assert_eq!(first.next_cursor, None);
    assert_eq!(
        mock.last_describe_topic_partitions_node(),
        Some(1),
        "DescribeTopicPartitions must land on the connected broker, not the coordinator or controller"
    );
    assert_eq!(
        mock.last_describe_topic_partitions(),
        Some((vec!["t".into()], 2000, None))
    );
    assert_eq!(
        mock.last_describe_groups_node(),
        None,
        "DescribeTopicPartitions must not hop via DescribeGroups or FindCoordinator"
    );
    assert_eq!(
        mock.last_alter_client_quotas_node(),
        None,
        "DescribeTopicPartitions must not hop via Metadata controller_id"
    );
    assert_eq!(
        mock.last_delete_share_group_offsets_node(),
        None,
        "DescribeTopicPartitions must not hop via DeleteShareGroupOffsets"
    );
    let timed = admin
        .describe_topic_partitions_timeout(&["t"], 2000, None, Duration::from_secs(5))
        .await
        .unwrap();
    assert_eq!(timed.topics.len(), 1);
    assert_eq!(timed.topics[0].name.as_deref(), Some("t"));
    assert_eq!(
        mock.last_describe_topic_partitions_node(),
        Some(1),
        "describe_topic_partitions_timeout must stay on the connected broker"
    );
    assert_eq!(
        mock.last_describe_topic_partitions(),
        Some((vec!["t".into()], 2000, None))
    );
}

#[tokio::test]
async fn list_config_resources_follows_broker() {
    let mock = common::Mock::start_two_node().await;
    mock.move_coordinator();
    mock.set_controller(2);
    let mut admin = Admin::connect(mock.addr.clone()).await.unwrap();

    let first = admin
        .list_config_resources([ConfigResourceType::ClientMetrics])
        .await
        .unwrap();
    assert_eq!(first.len(), 1);
    assert_eq!(first[0].resource_name, "r");
    assert_eq!(first[0].name(), "r");
    assert_eq!(first[0].resource_type, CONFIG_RESOURCE_CLIENT_METRICS);
    assert_eq!(
        first[0].resource_type(),
        Some(ConfigResourceType::ClientMetrics)
    );
    let listed_as_resource = first[0].to_config_resource();
    assert_eq!(listed_as_resource.name(), "r");
    assert_eq!(
        listed_as_resource.resource_type(),
        Some(ConfigResourceType::ClientMetrics)
    );
    assert_eq!(
        mock.last_list_config_resources_node(),
        Some(1),
        "ListConfigResources must land on the connected broker, not the coordinator or controller"
    );
    assert_eq!(
        mock.last_list_config_resources_version(),
        Some(1),
        "Admin must prefer ListConfigResources v1 when the broker advertises it"
    );
    assert_eq!(
        mock.last_list_config_resources(),
        Some(vec![CONFIG_RESOURCE_CLIENT_METRICS])
    );
    assert_eq!(
        mock.last_describe_groups_node(),
        None,
        "ListConfigResources must not hop via DescribeGroups or FindCoordinator"
    );
    assert_eq!(
        mock.last_alter_client_quotas_node(),
        None,
        "ListConfigResources must not hop via Metadata controller_id"
    );
    assert_eq!(
        mock.last_describe_topic_partitions_node(),
        None,
        "ListConfigResources must not hop via DescribeTopicPartitions"
    );
    let timed = admin
        .list_config_resources_timeout([ConfigResourceType::ClientMetrics], Duration::from_secs(5))
        .await
        .unwrap();
    assert_eq!(timed.len(), 1);
    assert_eq!(timed[0].resource_name, "r");
    let all = admin.list_config_resources_all().await.unwrap();
    assert_eq!(all.len(), 1);
    assert_eq!(
        mock.last_list_config_resources(),
        Some(vec![]),
        "listConfigResources() sends empty ResourceTypes"
    );
    let timed_all = admin
        .list_config_resources_all_timeout(Duration::from_secs(5))
        .await
        .unwrap();
    assert_eq!(timed_all.len(), 1);
}

#[tokio::test]
async fn list_config_resources_negotiates_v0_when_broker_caps() {
    let mock = common::Mock::start().await;
    mock.set_api_max(LIST_CONFIG_RESOURCES, 0);
    let mut admin = Admin::connect(mock.addr.clone()).await.unwrap();
    let listed = admin
        .list_config_resources([ConfigResourceType::Topic])
        .await
        .unwrap();
    assert_eq!(listed.len(), 1);
    assert_eq!(listed[0].resource_name, "r");
    assert_eq!(
        listed[0].resource_type, CONFIG_RESOURCE_CLIENT_METRICS,
        "v0 has no ResourceType; decode fills CLIENT_METRICS"
    );
    assert_eq!(
        mock.last_list_config_resources_version(),
        Some(0),
        "client must speak ListConfigResources v0 when the broker max is 0"
    );
    assert_eq!(
        mock.last_list_config_resources(),
        Some(vec![]),
        "v0 omits ResourceTypes even when the caller passed types"
    );
}

#[tokio::test]
async fn list_client_metrics_resources_follows_broker() {
    let mock = common::Mock::start_two_node().await;
    mock.move_coordinator();
    mock.set_controller(2);
    let mut admin = Admin::connect(mock.addr.clone()).await.unwrap();

    let first = admin.list_client_metrics_resources().await.unwrap();
    assert_eq!(first.len(), 1);
    assert_eq!(first[0].resource_name, "r");
    assert_eq!(first[0].resource_type, CONFIG_RESOURCE_CLIENT_METRICS);
    assert_eq!(
        mock.last_list_config_resources_node(),
        Some(1),
        "listClientMetricsResources must land on the connected broker"
    );
    assert_eq!(
        mock.last_list_config_resources(),
        Some(vec![CONFIG_RESOURCE_CLIENT_METRICS])
    );
    assert_eq!(
        mock.last_describe_groups_node(),
        None,
        "listClientMetricsResources must not hop via DescribeGroups or FindCoordinator"
    );
    let timed = admin
        .list_client_metrics_resources_timeout(Duration::from_secs(5))
        .await
        .unwrap();
    assert_eq!(timed.len(), 1);
    assert_eq!(timed[0].resource_name, "r");
}

#[tokio::test]
async fn get_telemetry_subscriptions_follows_broker() {
    let mock = common::Mock::start_two_node().await;
    mock.move_coordinator();
    mock.set_controller(2);
    let mut admin = Admin::connect(mock.addr.clone()).await.unwrap();

    let first = admin.get_telemetry_subscriptions([0; 16]).await.unwrap();
    assert_eq!(first.error_code, 0);
    assert_eq!(first.error_code(), 0);
    assert_eq!(first.client_instance_id, [0x11; 16]);
    assert_eq!(first.client_instance_id(), Uuid::from_bytes([0x11; 16]));
    assert_eq!(first.subscription_id, 1);
    assert_eq!(first.subscription_id(), 1);
    assert_eq!(first.accepted_compression_types, vec![1]);
    assert_eq!(first.accepted_compression_types(), &[1]);
    assert_eq!(first.push_interval_ms, 1000);
    assert_eq!(first.push_interval_ms(), 1000);
    assert_eq!(first.telemetry_max_bytes, 100);
    assert_eq!(first.telemetry_max_bytes(), 100);
    assert!(first.delta_temporality);
    assert!(first.delta_temporality());
    assert_eq!(first.requested_metrics, vec!["m".to_string()]);
    assert_eq!(first.requested_metrics(), &["m".to_string()]);
    assert_eq!(
        mock.last_get_telemetry_subscriptions_node(),
        Some(1),
        "GetTelemetrySubscriptions must land on the connected broker, not the coordinator or controller"
    );
    assert_eq!(mock.last_get_telemetry_subscriptions(), Some([0; 16]));
    let via_uuid = admin.get_telemetry_subscriptions(Uuid::ZERO).await.unwrap();
    assert_eq!(via_uuid.client_instance_id(), Uuid::from_bytes([0x11; 16]));
    assert_eq!(mock.last_get_telemetry_subscriptions(), Some([0; 16]));
    assert_eq!(
        mock.last_describe_groups_node(),
        None,
        "GetTelemetrySubscriptions must not hop via DescribeGroups or FindCoordinator"
    );
    assert_eq!(
        mock.last_alter_client_quotas_node(),
        None,
        "GetTelemetrySubscriptions must not hop via Metadata controller_id"
    );
    assert_eq!(
        mock.last_list_config_resources_node(),
        None,
        "GetTelemetrySubscriptions must not hop via ListConfigResources"
    );
}

#[tokio::test]
async fn push_telemetry_follows_broker() {
    let mock = common::Mock::start_two_node().await;
    mock.move_coordinator();
    mock.set_controller(2);
    let mut admin = Admin::connect(mock.addr.clone()).await.unwrap();

    let first = admin
        .push_telemetry([0x11; 16], 1, false, 0, b"m")
        .await
        .unwrap();
    assert_eq!(first.error_code, 0);
    assert_eq!(first.error_code(), 0);
    assert_eq!(
        mock.last_push_telemetry_node(),
        Some(1),
        "PushTelemetry must land on the connected broker, not the coordinator or controller"
    );
    assert_eq!(
        mock.last_push_telemetry(),
        Some(common::LastPushTelemetry {
            client_instance_id: [0x11; 16],
            subscription_id: 1,
            terminating: false,
            compression_type: 0,
            metrics: b"m".to_vec(),
        })
    );
    let via_uuid = admin
        .push_telemetry(Uuid::from_bytes([0x11; 16]), 1, false, 0, b"m")
        .await
        .unwrap();
    assert_eq!(via_uuid.error_code(), 0);
    assert_eq!(
        mock.last_push_telemetry(),
        Some(common::LastPushTelemetry {
            client_instance_id: [0x11; 16],
            subscription_id: 1,
            terminating: false,
            compression_type: 0,
            metrics: b"m".to_vec(),
        })
    );
    assert_eq!(
        mock.last_get_telemetry_subscriptions_node(),
        None,
        "PushTelemetry must not hop via GetTelemetrySubscriptions"
    );
    assert_eq!(
        mock.last_describe_groups_node(),
        None,
        "PushTelemetry must not hop via DescribeGroups or FindCoordinator"
    );
    assert_eq!(
        mock.last_alter_client_quotas_node(),
        None,
        "PushTelemetry must not hop via Metadata controller_id"
    );
    assert_eq!(
        mock.last_list_config_resources_node(),
        None,
        "PushTelemetry must not hop via ListConfigResources"
    );
}

#[tokio::test]
async fn assign_replicas_to_dirs_follows_controller() {
    let mock = common::Mock::start_two_node().await;
    mock.set_controller(2);
    let mut admin = Admin::connect(mock.addr.clone()).await.unwrap();

    let dir = AssignReplicasToDirsDirectory::new(
        [0x11; 16],
        vec![AssignReplicasToDirsTopic::new(
            [0x22; 16],
            vec![AssignReplicasToDirsPartition::new(0)],
        )],
    );
    let first = admin
        .assign_replicas_to_dirs(7, -1, vec![dir.clone()])
        .await
        .unwrap();
    assert_eq!(first.error_code, 0);
    assert_eq!(first.error_code(), 0);
    assert_eq!(first.directories.len(), 1);
    assert_eq!(first.directories[0].id, [0x11; 16]);
    assert_eq!(first.directories()[0].id(), Uuid::from_bytes([0x11; 16]));
    assert_eq!(first.directories[0].topics[0].topic_id, [0x22; 16]);
    assert_eq!(
        first.directories()[0].topics()[0].topic_id(),
        Uuid::from_bytes([0x22; 16])
    );
    assert_eq!(
        first.directories[0].topics[0].partitions[0].partition_index,
        0
    );
    assert_eq!(
        first.directories()[0].topics()[0].partitions()[0].partition_index(),
        0
    );
    assert_eq!(first.directories[0].topics[0].partitions[0].error_code, 0);
    assert_eq!(
        first.directories()[0].topics()[0].partitions()[0].error_code(),
        0
    );
    assert_eq!(
        mock.last_assign_replicas_to_dirs_node(),
        Some(2),
        "AssignReplicasToDirs must land on the controller, not bootstrap"
    );
    assert_eq!(
        mock.last_assign_replicas_to_dirs(),
        Some(AssignReplicasToDirsRequest::new(7, -1, vec![dir.clone()]))
    );
    assert_eq!(
        mock.last_push_telemetry_node(),
        None,
        "AssignReplicasToDirs must not hop via PushTelemetry"
    );
    assert_eq!(
        mock.last_describe_groups_node(),
        None,
        "AssignReplicasToDirs must not hop via DescribeGroups or FindCoordinator"
    );

    mock.set_controller(1);
    let again = admin
        .assign_replicas_to_dirs(7, -1, vec![dir.clone()])
        .await
        .unwrap();
    assert_eq!(again.error_code, 0);
    assert_eq!(
        mock.assign_replicas_to_dirs_not_controller(),
        1,
        "stale controller must return NOT_CONTROLLER (41) once"
    );
    assert_eq!(
        mock.last_assign_replicas_to_dirs_node(),
        Some(1),
        "AssignReplicasToDirs must follow Metadata after NOT_CONTROLLER"
    );
    let timed = admin
        .assign_replicas_to_dirs_timeout(7, -1, vec![dir.clone()], Duration::from_secs(5))
        .await
        .unwrap();
    assert_eq!(timed.error_code, 0);
}

#[tokio::test]
async fn alter_replica_log_dirs_follows_broker() {
    let mock = common::Mock::start_two_node().await;
    mock.move_coordinator();
    mock.set_controller(2);
    let mut admin = Admin::connect(mock.addr.clone()).await.unwrap();

    let empty = admin
        .alter_replica_log_dirs_for(Vec::<(TopicPartitionReplica, String)>::new())
        .await
        .unwrap();
    assert!(empty.is_empty());
    assert_eq!(
        mock.last_alter_replica_log_dirs_node(),
        None,
        "empty alter_replica_log_dirs_for is a no-op"
    );

    let dir =
        AlterReplicaLogDirsDirectory::new("/d", vec![AlterReplicaLogDirsTopic::new("t", vec![0])]);
    let first = admin
        .alter_replica_log_dirs(vec![dir.clone()])
        .await
        .unwrap();
    assert_eq!(first.results.len(), 1);
    assert_eq!(first.results[0].topic_name, "t");
    assert_eq!(first.results()[0].topic_name(), "t");
    assert_eq!(first.results[0].partitions[0].partition_index, 0);
    assert_eq!(first.results()[0].partitions()[0].partition_index(), 0);
    assert_eq!(first.results[0].partitions[0].error_code, 0);
    assert_eq!(first.results()[0].partitions()[0].error_code(), 0);
    assert_eq!(
        mock.last_alter_replica_log_dirs_node(),
        Some(1),
        "AlterReplicaLogDirs must land on the connected broker, not the coordinator or controller"
    );
    assert_eq!(
        mock.last_alter_replica_log_dirs_version(),
        Some(2),
        "Admin must prefer AlterReplicaLogDirs v2 when the broker advertises it"
    );
    assert_eq!(
        mock.last_alter_replica_log_dirs(),
        Some(AlterReplicaLogDirsRequest::new(vec![dir]))
    );
    assert_eq!(
        mock.last_assign_replicas_to_dirs_node(),
        None,
        "AlterReplicaLogDirs must not hop via AssignReplicasToDirs"
    );
    assert_eq!(
        mock.last_push_telemetry_node(),
        None,
        "AlterReplicaLogDirs must not hop via PushTelemetry"
    );
    assert_eq!(
        mock.last_describe_groups_node(),
        None,
        "AlterReplicaLogDirs must not hop via DescribeGroups or FindCoordinator"
    );
    assert_eq!(
        mock.last_alter_client_quotas_node(),
        None,
        "AlterReplicaLogDirs must not hop via Metadata controller_id"
    );
    let timed_dir =
        AlterReplicaLogDirsDirectory::new("/d", vec![AlterReplicaLogDirsTopic::new("t", vec![0])]);
    let timed = admin
        .alter_replica_log_dirs_timeout(vec![timed_dir], Duration::from_secs(5))
        .await
        .unwrap();
    assert_eq!(timed.results.len(), 1);
    assert_eq!(timed.results[0].partitions[0].error_code, 0);

    let mapped = admin
        .alter_replica_log_dirs_for([(TopicPartitionReplica::new("t", 0, 2), "/d")])
        .await
        .unwrap();
    assert_eq!(mapped.len(), 1);
    assert_eq!(mapped[0].0, TopicPartitionReplica::new("t", 0, 2));
    assert_eq!(mapped[0].1, 0);
    assert_eq!(
        mock.last_alter_replica_log_dirs_node(),
        Some(2),
        "alter_replica_log_dirs_for must send AlterReplicaLogDirs to the replica broker"
    );
    assert_eq!(
        mock.last_alter_replica_log_dirs(),
        Some(AlterReplicaLogDirsRequest::new(vec![
            AlterReplicaLogDirsDirectory::new(
                "/d",
                vec![AlterReplicaLogDirsTopic::new("t", vec![0])]
            )
        ]))
    );
    let timed_map = admin
        .alter_replica_log_dirs_for_timeout(
            [(TopicPartitionReplica::new("t", 0, 2), "/d")],
            Duration::from_secs(5),
        )
        .await
        .unwrap();
    assert_eq!(timed_map.len(), 1);
    assert_eq!(timed_map[0].1, 0);
    admin.close().await.unwrap();
    mock.hide_api(ALTER_REPLICA_LOG_DIRS);
    let mut admin = Admin::connect(mock.addr.clone()).await.unwrap();
    let hidden =
        AlterReplicaLogDirsDirectory::new("/d", vec![AlterReplicaLogDirsTopic::new("t", vec![0])]);
    let err = admin
        .alter_replica_log_dirs(vec![hidden])
        .await
        .unwrap_err();
    assert!(
        err.to_string().contains("unsupported"),
        "AlterReplicaLogDirs is optional at connect: {err}"
    );
    let classic = admin
        .describe_classic_groups(&["g-classic"], false)
        .await
        .unwrap();
    assert_eq!(classic[0].group_id, "g-classic");
    admin.close().await.unwrap();
}

#[tokio::test]
async fn alter_replica_log_dirs_negotiates_v1_when_broker_caps() {
    let mock = common::Mock::start().await;
    mock.set_api_max(ALTER_REPLICA_LOG_DIRS, 1);
    let mut admin = Admin::connect(mock.addr.clone()).await.unwrap();
    let dir =
        AlterReplicaLogDirsDirectory::new("/d", vec![AlterReplicaLogDirsTopic::new("t", vec![0])]);
    let resp = admin
        .alter_replica_log_dirs(vec![dir.clone()])
        .await
        .unwrap();
    assert_eq!(resp.results.len(), 1);
    assert_eq!(resp.results[0].topic_name, "t");
    assert_eq!(resp.results[0].partitions[0].error_code, 0);
    assert_eq!(
        mock.last_alter_replica_log_dirs_version(),
        Some(1),
        "client must speak AlterReplicaLogDirs v1 when the broker max is 1"
    );
    assert_eq!(
        mock.last_alter_replica_log_dirs(),
        Some(AlterReplicaLogDirsRequest::new(vec![dir]))
    );
}

#[tokio::test]
async fn describe_log_dirs_follows_broker() {
    let mock = common::Mock::start_two_node().await;
    mock.move_coordinator();
    mock.set_controller(2);
    let mut admin = Admin::connect(mock.addr.clone()).await.unwrap();

    let topics = Some(vec![DescribableLogDirTopic::new("t", vec![0])]);
    let first = admin.describe_log_dirs(topics.clone()).await.unwrap();
    assert_eq!(first.error_code, 0);
    assert_eq!(first.results.len(), 1);
    assert_eq!(first.results[0].log_dir, "/d");
    assert_eq!(first.results[0].error_code, 0);
    assert_eq!(first.error_code(), 0);
    assert_eq!(first.results()[0].log_dir(), "/d");
    assert_eq!(first.results()[0].error_code(), 0);
    assert!(first.results()[0].total_bytes().is_none());
    assert!(first.results()[0].usable_bytes().is_none());
    assert_eq!(first.results()[0].topics()[0].name(), "t");
    assert_eq!(
        first.results()[0].topics()[0].partitions()[0].partition_index(),
        0
    );
    assert_eq!(first.results()[0].topics()[0].partitions()[0].size(), 0);
    assert_eq!(
        first.results()[0].topics()[0].partitions()[0].offset_lag(),
        0
    );
    assert!(!first.results()[0].topics()[0].partitions()[0].is_future());
    assert_eq!(first.results[0].topics[0].name, "t");
    assert_eq!(first.results[0].topics[0].partitions[0].partition_index, 0);
    assert_eq!(
        mock.last_describe_log_dirs_node(),
        Some(1),
        "DescribeLogDirs must land on the connected broker, not the coordinator or controller"
    );
    assert_eq!(
        mock.last_describe_log_dirs_version(),
        Some(4),
        "Admin must prefer DescribeLogDirs v4 when the broker advertises it"
    );
    assert_eq!(
        mock.last_describe_log_dirs(),
        Some(DescribeLogDirsRequest::new(topics))
    );
    assert_eq!(
        mock.last_alter_replica_log_dirs_node(),
        None,
        "DescribeLogDirs must not hop via AlterReplicaLogDirs"
    );
    assert_eq!(
        mock.last_assign_replicas_to_dirs_node(),
        None,
        "DescribeLogDirs must not hop via AssignReplicasToDirs"
    );
    assert_eq!(
        mock.last_push_telemetry_node(),
        None,
        "DescribeLogDirs must not hop via PushTelemetry"
    );
    assert_eq!(
        mock.last_describe_groups_node(),
        None,
        "DescribeLogDirs must not hop via DescribeGroups or FindCoordinator"
    );
    assert_eq!(
        mock.last_alter_client_quotas_node(),
        None,
        "DescribeLogDirs must not hop via Metadata controller_id"
    );
    let timed = admin
        .describe_log_dirs_timeout(
            Some(vec![DescribableLogDirTopic::new("t", vec![0])]),
            Duration::from_secs(5),
        )
        .await
        .unwrap();
    assert_eq!(timed.error_code, 0);
    assert_eq!(timed.results[0].log_dir, "/d");
    admin.close().await.unwrap();
    mock.hide_api(DESCRIBE_LOG_DIRS);
    let mut admin = Admin::connect(mock.addr.clone()).await.unwrap();
    let err = admin
        .describe_log_dirs(Some(vec![DescribableLogDirTopic::new("t", vec![0])]))
        .await
        .unwrap_err();
    assert!(
        err.to_string().contains("unsupported"),
        "DescribeLogDirs is optional at connect: {err}"
    );
    let classic = admin
        .describe_classic_groups(&["g-classic"], false)
        .await
        .unwrap();
    assert_eq!(classic[0].group_id, "g-classic");
    admin.close().await.unwrap();
}

#[tokio::test]
async fn describe_features_prefers_api_versions_v4() {
    let mock = common::Mock::start().await;
    let mut admin = Admin::new(AdminConfig::bootstrap([mock.addr.clone()]))
        .await
        .unwrap();
    assert_eq!(
        mock.last_api_versions_version(),
        Some(4),
        "Admin must send ApiVersions v4 on connect"
    );
    let features = admin.describe_features().await.unwrap();
    assert_eq!(
        mock.last_api_versions_version(),
        Some(4),
        "Admin must prefer ApiVersions v4 when the broker advertises it"
    );
    let kraft = features
        .supported_features
        .iter()
        .find(|f| f.name == "kraft.version")
        .expect("kraft.version supported on v4");
    assert_eq!(kraft.min_version, 0);
    assert_eq!(kraft.max_version, 1);
    assert_eq!(
        kraft.to_string(),
        "SupportedVersionRange[min_version:0, max_version:1]"
    );
    let meta = features
        .supported_features
        .iter()
        .find(|f| f.name == "metadata.version")
        .expect("metadata.version supported");
    assert_eq!(meta.min_version, 1);
    assert_eq!(meta.max_version, 20);
    assert_eq!(
        meta.to_string(),
        "SupportedVersionRange[min_version:1, max_version:20]"
    );
    let timed = admin
        .describe_features_timeout(Duration::from_secs(5))
        .await
        .unwrap();
    assert!(timed
        .supported_features
        .iter()
        .any(|f| f.name == "kraft.version"));
}

#[tokio::test]
async fn describe_features_negotiates_v3_when_broker_caps() {
    let mock = common::Mock::start().await;
    mock.set_api_max(API_VERSIONS, 3);
    let mut admin = Admin::new(AdminConfig::bootstrap([mock.addr.clone()]))
        .await
        .unwrap();
    assert_eq!(
        mock.api_versions_versions(),
        vec![4, 3],
        "KIP-511 retries ApiVersions at the advertised max"
    );
    assert_eq!(
        mock.last_api_versions_version(),
        Some(3),
        "connect must land on v3 after UNSUPPORTED_VERSION"
    );
    let features = admin.describe_features().await.unwrap();
    assert_eq!(
        mock.last_api_versions_version(),
        Some(3),
        "client must speak ApiVersions v3 when the broker max is 3"
    );
    assert!(
        features
            .supported_features
            .iter()
            .all(|f| f.name != "kraft.version"),
        "v3 omits SupportedFeatures with MinVersion 0"
    );
    let meta = features
        .supported_features
        .iter()
        .find(|f| f.name == "metadata.version")
        .expect("metadata.version supported on v3");
    assert_eq!(meta.min_version, 1);
    assert_eq!(meta.max_version, 20);
}

#[tokio::test]
async fn describe_log_dirs_negotiates_v1_when_broker_caps() {
    let mock = common::Mock::start().await;
    mock.set_api_max(DESCRIBE_LOG_DIRS, 1);
    let mut admin = Admin::connect(mock.addr.clone()).await.unwrap();
    let topics = Some(vec![DescribableLogDirTopic::new("t", vec![0])]);
    let resp = admin.describe_log_dirs(topics.clone()).await.unwrap();
    assert_eq!(
        resp.error_code, 0,
        "v1 omits top-level ErrorCode; decode fills 0"
    );
    assert_eq!(resp.results.len(), 1);
    assert_eq!(resp.results[0].log_dir, "/d");
    assert_eq!(
        resp.results[0].total_bytes, -1,
        "v1 omits TotalBytes; decode fills -1"
    );
    assert_eq!(
        mock.last_describe_log_dirs_version(),
        Some(1),
        "client must speak DescribeLogDirs v1 when the broker max is 1"
    );
    assert_eq!(
        mock.last_describe_log_dirs(),
        Some(DescribeLogDirsRequest::new(topics))
    );
}

#[tokio::test]
async fn describe_log_dirs_negotiates_v3_when_broker_caps() {
    let mock = common::Mock::start().await;
    mock.set_api_max(DESCRIBE_LOG_DIRS, 3);
    let mut admin = Admin::connect(mock.addr.clone()).await.unwrap();
    let topics = Some(vec![DescribableLogDirTopic::new("t", vec![0])]);
    let resp = admin.describe_log_dirs(topics).await.unwrap();
    assert_eq!(resp.error_code, 0);
    assert_eq!(resp.results[0].log_dir, "/d");
    assert_eq!(
        resp.results[0].total_bytes, -1,
        "v3 omits TotalBytes; decode fills -1"
    );
    assert_eq!(
        mock.last_describe_log_dirs_version(),
        Some(3),
        "client must speak DescribeLogDirs v3 when the broker max is 3"
    );
}

#[tokio::test]
async fn describe_replica_log_dirs_follows_replica_broker() {
    let mock = common::Mock::start_two_node().await;
    mock.move_coordinator();
    mock.set_controller(2);
    let mut admin = Admin::connect(mock.addr.clone()).await.unwrap();

    let empty = admin
        .describe_replica_log_dirs(Vec::<TopicPartitionReplica>::new())
        .await
        .unwrap();
    assert!(empty.is_empty());
    assert_eq!(
        mock.last_describe_log_dirs_node(),
        None,
        "empty describe_replica_log_dirs is a no-op"
    );
    assert_eq!(
        mock.metadata_calls(),
        0,
        "empty input must not send Metadata"
    );

    let described = admin
        .describe_replica_log_dirs([TopicPartitionReplica::new("t", 0, 2)])
        .await
        .unwrap();
    assert_eq!(described.len(), 1);
    assert_eq!(described[0].0.broker_id, 2);
    assert_eq!(format!("{}", described[0].0), "t-0-2");
    assert_eq!(
        described[0].1,
        ReplicaLogDirInfo::new(Some("/d".into()), 0, None, -1)
    );
    assert_eq!(
        format!("{}", described[0].1),
        "ReplicaLogDirInfo(currentReplicaLogDir=/d)"
    );
    assert_eq!(
        mock.last_describe_log_dirs_node(),
        Some(2),
        "describe_replica_log_dirs must send DescribeLogDirs to the replica broker"
    );
    assert_eq!(
        mock.last_describe_log_dirs(),
        Some(DescribeLogDirsRequest::new(Some(vec![
            DescribableLogDirTopic::new("t", vec![0])
        ])))
    );
    assert_eq!(
        mock.last_alter_replica_log_dirs_node(),
        None,
        "describe_replica_log_dirs must not hop via AlterReplicaLogDirs"
    );
    assert_eq!(
        mock.last_assign_replicas_to_dirs_node(),
        None,
        "describe_replica_log_dirs must not hop via AssignReplicasToDirs"
    );
    assert_eq!(
        mock.last_describe_groups_node(),
        None,
        "describe_replica_log_dirs must not hop via DescribeGroups or FindCoordinator"
    );
    let timed = admin
        .describe_replica_log_dirs_timeout(
            [TopicPartitionReplica::new("t", 0, 2)],
            Duration::from_secs(5),
        )
        .await
        .unwrap();
    assert_eq!(timed.len(), 1);
    assert_eq!(timed[0].0.broker_id, 2);
}

#[tokio::test]
async fn describe_broker_log_dirs_follows_each_broker() {
    let mock = common::Mock::start_two_node().await;
    mock.move_coordinator();
    mock.set_controller(2);
    let mut admin = Admin::connect(mock.addr.clone()).await.unwrap();

    let empty = admin
        .describe_broker_log_dirs(Vec::<i32>::new())
        .await
        .unwrap();
    assert!(empty.is_empty());
    assert_eq!(
        mock.last_describe_log_dirs_node(),
        None,
        "empty describe_broker_log_dirs is a no-op"
    );
    assert_eq!(
        mock.metadata_calls(),
        0,
        "empty input must not send Metadata"
    );

    let dirs = admin.describe_broker_log_dirs([1, 2, 1]).await.unwrap();
    assert_eq!(dirs.len(), 2, "duplicate broker ids are sent once");
    assert_eq!(dirs[0].0, 1);
    assert_eq!(dirs[1].0, 2);
    assert_eq!(dirs[0].1.error_code, 0);
    assert_eq!(dirs[1].1.error_code, 0);
    assert_eq!(dirs[0].1.results[0].log_dir, "/d");
    assert_eq!(
        mock.describe_log_dirs_nodes(),
        vec![1, 2],
        "describeLogDirs must send one DescribeLogDirs to each broker"
    );
    assert_eq!(
        mock.last_describe_log_dirs(),
        Some(DescribeLogDirsRequest::new(None)),
        "Java describeLogDirs uses a null topic array (all dirs)"
    );
    assert_eq!(
        mock.last_alter_replica_log_dirs_node(),
        None,
        "describe_broker_log_dirs must not hop via AlterReplicaLogDirs"
    );
    assert_eq!(
        mock.last_describe_groups_node(),
        None,
        "describe_broker_log_dirs must not hop via DescribeGroups or FindCoordinator"
    );
    let m = admin.metrics();
    assert!(
        m.connections >= 2,
        "describe_broker_log_dirs([1, 2]) opens per-node sockets: {m:?}"
    );
    assert!(m.requests >= 2, "ApiVersions plus DescribeLogDirs: {m:?}");
    assert_eq!(m.errors, 0);
    let timed = admin
        .describe_broker_log_dirs_timeout([1], Duration::from_secs(5))
        .await
        .unwrap();
    assert_eq!(timed.len(), 1);
    assert_eq!(timed[0].0, 1);
    admin.close().await.unwrap();
}

#[tokio::test]
async fn create_delegation_token_follows_broker() {
    let mock = common::Mock::start_two_node().await;
    mock.move_coordinator();
    mock.set_controller(2);
    let mut admin = Admin::connect(mock.addr.clone()).await.unwrap();

    let req =
        CreateDelegationTokenRequest::new(None, None, vec![CreatableRenewer::new("User", "r")], -1);
    let first = admin.create_delegation_token(req.clone()).await.unwrap();
    assert_eq!(first.error_code, 0);
    assert_eq!(first.error_code(), 0);
    assert_eq!(first.principal_type, "");
    assert_eq!(first.principal_type(), "");
    assert_eq!(first.principal_name, "");
    assert_eq!(first.token_id, "");
    assert_eq!(first.token_id(), "");
    assert!(first.hmac.is_empty());
    assert!(first.hmac().is_empty());
    assert_eq!(first.hmac_as_base64_string(), "");
    assert_eq!(first.owner_as_string(), ":");
    assert_eq!(
        mock.last_create_delegation_token_node(),
        Some(1),
        "CreateDelegationToken must land on the connected broker, not the coordinator or controller"
    );
    assert_eq!(
        mock.last_create_delegation_token_version(),
        Some(3),
        "Admin must prefer CreateDelegationToken v3 when the broker advertises it"
    );
    assert_eq!(mock.last_create_delegation_token(), Some(req));
    assert_eq!(
        mock.last_describe_log_dirs_node(),
        None,
        "CreateDelegationToken must not hop via DescribeLogDirs"
    );
    assert_eq!(
        mock.last_assign_replicas_to_dirs_node(),
        None,
        "CreateDelegationToken must not hop via AssignReplicasToDirs"
    );
    assert_eq!(
        mock.last_alter_replica_log_dirs_node(),
        None,
        "CreateDelegationToken must not hop via AlterReplicaLogDirs"
    );
    assert_eq!(
        mock.last_describe_groups_node(),
        None,
        "CreateDelegationToken must not hop via DescribeGroups or FindCoordinator"
    );
    assert_eq!(
        mock.last_alter_client_quotas_node(),
        None,
        "CreateDelegationToken must not hop via Metadata controller_id"
    );
    let timed = admin
        .create_delegation_token_timeout(
            CreateDelegationTokenRequest::new(
                None,
                None,
                vec![CreatableRenewer::new("User", "r")],
                -1,
            ),
            Duration::from_secs(5),
        )
        .await
        .unwrap();
    assert_eq!(timed.error_code, 0);
    let defaulted = admin.create_delegation_token_default().await.unwrap();
    assert_eq!(defaulted.error_code, 0);
    assert_eq!(
        mock.last_create_delegation_token(),
        Some(CreateDelegationTokenRequest::default()),
        "createDelegationToken() sends default owner / renewers / maxLifetimeMs -1"
    );
    let timed_default = admin
        .create_delegation_token_default_timeout(Duration::from_secs(5))
        .await
        .unwrap();
    assert_eq!(timed_default.error_code, 0);
    admin.close().await.unwrap();
    mock.hide_api(CREATE_DELEGATION_TOKEN);
    let mut admin = Admin::connect(mock.addr.clone()).await.unwrap();
    let err = admin.create_delegation_token_default().await.unwrap_err();
    assert!(
        err.to_string().contains("unsupported"),
        "CreateDelegationToken is optional at connect: {err}"
    );
    let classic = admin
        .describe_classic_groups(&["g-classic"], false)
        .await
        .unwrap();
    assert_eq!(classic[0].group_id, "g-classic");
    admin.close().await.unwrap();
}

#[tokio::test]
async fn create_delegation_token_negotiates_v1_when_broker_caps() {
    let mock = common::Mock::start().await;
    mock.set_api_max(CREATE_DELEGATION_TOKEN, 1);
    let mut admin = Admin::connect(mock.addr.clone()).await.unwrap();
    let req =
        CreateDelegationTokenRequest::new(Some("User".into()), Some("alice".into()), vec![], -1);
    let resp = admin.create_delegation_token(req).await.unwrap();
    assert_eq!(resp.error_code, 0);
    assert_eq!(
        resp.token_requester_principal_type, "",
        "v1 omits TokenRequesterPrincipalType; decode fills empty"
    );
    assert_eq!(
        resp.token_requester_principal_name, "",
        "v1 omits TokenRequesterPrincipalName; decode fills empty"
    );
    assert_eq!(
        mock.last_create_delegation_token_version(),
        Some(1),
        "client must speak CreateDelegationToken v1 when the broker max is 1"
    );
    assert_eq!(
        mock.last_create_delegation_token(),
        Some(CreateDelegationTokenRequest::new(None, None, vec![], -1)),
        "v1 omits owner on the wire; decode fills None"
    );
}

#[tokio::test]
async fn renew_delegation_token_follows_broker() {
    let mock = common::Mock::start_two_node().await;
    mock.move_coordinator();
    mock.set_controller(2);
    let mut admin = Admin::connect(mock.addr.clone()).await.unwrap();

    let req = RenewDelegationTokenRequest::new(vec![0xaa], -1);
    let first = admin.renew_delegation_token(req.clone()).await.unwrap();
    assert_eq!(first.error_code, 0);
    assert_eq!(first.error_code(), 0);
    assert_eq!(first.expiry_timestamp_ms, 0);
    assert_eq!(first.expiry_timestamp(), 0);
    assert_eq!(
        mock.last_renew_delegation_token_node(),
        Some(1),
        "RenewDelegationToken must land on the connected broker, not the coordinator or controller"
    );
    assert_eq!(
        mock.last_renew_delegation_token_version(),
        Some(2),
        "Admin must prefer RenewDelegationToken v2 when the broker advertises it"
    );
    assert_eq!(mock.last_renew_delegation_token(), Some(req));
    assert_eq!(
        mock.last_create_delegation_token_node(),
        None,
        "RenewDelegationToken must not hop via CreateDelegationToken"
    );
    assert_eq!(
        mock.last_describe_log_dirs_node(),
        None,
        "RenewDelegationToken must not hop via DescribeLogDirs"
    );
    assert_eq!(
        mock.last_assign_replicas_to_dirs_node(),
        None,
        "RenewDelegationToken must not hop via AssignReplicasToDirs"
    );
    assert_eq!(
        mock.last_alter_replica_log_dirs_node(),
        None,
        "RenewDelegationToken must not hop via AlterReplicaLogDirs"
    );
    assert_eq!(
        mock.last_describe_groups_node(),
        None,
        "RenewDelegationToken must not hop via DescribeGroups or FindCoordinator"
    );
    assert_eq!(
        mock.last_alter_client_quotas_node(),
        None,
        "RenewDelegationToken must not hop via Metadata controller_id"
    );
    let timed = admin
        .renew_delegation_token_timeout(
            RenewDelegationTokenRequest::new(vec![0xaa], -1),
            Duration::from_secs(5),
        )
        .await
        .unwrap();
    assert_eq!(timed.error_code, 0);
    let hmac = admin.renew_delegation_token_hmac([0xbb]).await.unwrap();
    assert_eq!(hmac.error_code, 0);
    assert_eq!(
        mock.last_renew_delegation_token(),
        Some(RenewDelegationTokenRequest::new(vec![0xbb], -1)),
        "renewDelegationToken(byte[]) sends hmac and renew_period_ms -1"
    );
    let timed_hmac = admin
        .renew_delegation_token_hmac_timeout([0xcc], Duration::from_secs(5))
        .await
        .unwrap();
    assert_eq!(timed_hmac.error_code, 0);
    admin.close().await.unwrap();
    mock.hide_api(RENEW_DELEGATION_TOKEN);
    let mut admin = Admin::connect(mock.addr.clone()).await.unwrap();
    let err = admin.renew_delegation_token_hmac([0xdd]).await.unwrap_err();
    assert!(
        err.to_string().contains("unsupported"),
        "RenewDelegationToken is optional at connect: {err}"
    );
    let classic = admin
        .describe_classic_groups(&["g-classic"], false)
        .await
        .unwrap();
    assert_eq!(classic[0].group_id, "g-classic");
    admin.close().await.unwrap();
}

#[tokio::test]
async fn renew_delegation_token_negotiates_v1_when_broker_caps() {
    let mock = common::Mock::start().await;
    mock.set_api_max(RENEW_DELEGATION_TOKEN, 1);
    let mut admin = Admin::connect(mock.addr.clone()).await.unwrap();
    let req = RenewDelegationTokenRequest::new(vec![0xaa], -1);
    let resp = admin.renew_delegation_token(req.clone()).await.unwrap();
    assert_eq!(resp.error_code, 0);
    assert_eq!(resp.expiry_timestamp_ms, 0);
    assert_eq!(
        mock.last_renew_delegation_token_version(),
        Some(1),
        "client must speak RenewDelegationToken v1 when the broker max is 1"
    );
    assert_eq!(
        mock.last_renew_delegation_token(),
        Some(req),
        "v1 request fields match v2"
    );
}

#[tokio::test]
async fn expire_delegation_token_follows_broker() {
    let mock = common::Mock::start_two_node().await;
    mock.move_coordinator();
    mock.set_controller(2);
    let mut admin = Admin::connect(mock.addr.clone()).await.unwrap();

    let req = ExpireDelegationTokenRequest::new(vec![0xaa], -1);
    let first = admin.expire_delegation_token(req.clone()).await.unwrap();
    assert_eq!(first.error_code, 0);
    assert_eq!(first.error_code(), 0);
    assert_eq!(first.expiry_timestamp_ms, 0);
    assert_eq!(first.expiry_timestamp(), 0);
    assert_eq!(
        mock.last_expire_delegation_token_node(),
        Some(1),
        "ExpireDelegationToken must land on the connected broker, not the coordinator or controller"
    );
    assert_eq!(
        mock.last_expire_delegation_token_version(),
        Some(2),
        "Admin must prefer ExpireDelegationToken v2 when the broker advertises it"
    );
    assert_eq!(mock.last_expire_delegation_token(), Some(req));
    assert_eq!(
        mock.last_renew_delegation_token_node(),
        None,
        "ExpireDelegationToken must not hop via RenewDelegationToken"
    );
    assert_eq!(
        mock.last_create_delegation_token_node(),
        None,
        "ExpireDelegationToken must not hop via CreateDelegationToken"
    );
    assert_eq!(
        mock.last_describe_log_dirs_node(),
        None,
        "ExpireDelegationToken must not hop via DescribeLogDirs"
    );
    assert_eq!(
        mock.last_assign_replicas_to_dirs_node(),
        None,
        "ExpireDelegationToken must not hop via AssignReplicasToDirs"
    );
    assert_eq!(
        mock.last_alter_replica_log_dirs_node(),
        None,
        "ExpireDelegationToken must not hop via AlterReplicaLogDirs"
    );
    assert_eq!(
        mock.last_describe_groups_node(),
        None,
        "ExpireDelegationToken must not hop via DescribeGroups or FindCoordinator"
    );
    assert_eq!(
        mock.last_alter_client_quotas_node(),
        None,
        "ExpireDelegationToken must not hop via Metadata controller_id"
    );
    let timed = admin
        .expire_delegation_token_timeout(
            ExpireDelegationTokenRequest::new(vec![0xaa], -1),
            Duration::from_secs(5),
        )
        .await
        .unwrap();
    assert_eq!(timed.error_code, 0);
    let hmac = admin.expire_delegation_token_hmac([0xbb]).await.unwrap();
    assert_eq!(hmac.error_code, 0);
    assert_eq!(
        mock.last_expire_delegation_token(),
        Some(ExpireDelegationTokenRequest::new(vec![0xbb], -1)),
        "expireDelegationToken(byte[]) sends hmac and expiry_time_period_ms -1"
    );
    let timed_hmac = admin
        .expire_delegation_token_hmac_timeout([0xcc], Duration::from_secs(5))
        .await
        .unwrap();
    assert_eq!(timed_hmac.error_code, 0);
    admin.close().await.unwrap();
    mock.hide_api(EXPIRE_DELEGATION_TOKEN);
    let mut admin = Admin::connect(mock.addr.clone()).await.unwrap();
    let err = admin
        .expire_delegation_token_hmac([0xdd])
        .await
        .unwrap_err();
    assert!(
        err.to_string().contains("unsupported"),
        "ExpireDelegationToken is optional at connect: {err}"
    );
    let classic = admin
        .describe_classic_groups(&["g-classic"], false)
        .await
        .unwrap();
    assert_eq!(classic[0].group_id, "g-classic");
    admin.close().await.unwrap();
}

#[tokio::test]
async fn expire_delegation_token_negotiates_v1_when_broker_caps() {
    let mock = common::Mock::start().await;
    mock.set_api_max(EXPIRE_DELEGATION_TOKEN, 1);
    let mut admin = Admin::connect(mock.addr.clone()).await.unwrap();
    let req = ExpireDelegationTokenRequest::new(vec![0xaa], -1);
    let resp = admin.expire_delegation_token(req.clone()).await.unwrap();
    assert_eq!(resp.error_code, 0);
    assert_eq!(resp.expiry_timestamp_ms, 0);
    assert_eq!(
        mock.last_expire_delegation_token_version(),
        Some(1),
        "client must speak ExpireDelegationToken v1 when the broker max is 1"
    );
    assert_eq!(
        mock.last_expire_delegation_token(),
        Some(req),
        "v1 request fields match v2"
    );
}

#[tokio::test]
async fn describe_delegation_token_follows_broker() {
    let mock = common::Mock::start_two_node().await;
    mock.move_coordinator();
    mock.set_controller(2);
    let mut admin = Admin::connect(mock.addr.clone()).await.unwrap();

    let req = DescribeDelegationTokenRequest::new(Some(vec![DescribeDelegationTokenOwner::new(
        "User", "r",
    )]));
    let first = admin.describe_delegation_token(req.clone()).await.unwrap();
    assert_eq!(first.error_code, 0);
    assert_eq!(first.error_code(), 0);
    assert!(first.tokens.is_empty());
    assert!(first.tokens().is_empty());
    assert_eq!(
        mock.last_describe_delegation_token_node(),
        Some(1),
        "DescribeDelegationToken must land on the connected broker, not the coordinator or controller"
    );
    assert_eq!(
        mock.last_describe_delegation_token_version(),
        Some(3),
        "Admin must prefer DescribeDelegationToken v3 when the broker advertises it"
    );
    assert_eq!(mock.last_describe_delegation_token(), Some(req));
    assert_eq!(
        mock.last_expire_delegation_token_node(),
        None,
        "DescribeDelegationToken must not hop via ExpireDelegationToken"
    );
    assert_eq!(
        mock.last_renew_delegation_token_node(),
        None,
        "DescribeDelegationToken must not hop via RenewDelegationToken"
    );
    assert_eq!(
        mock.last_create_delegation_token_node(),
        None,
        "DescribeDelegationToken must not hop via CreateDelegationToken"
    );
    assert_eq!(
        mock.last_describe_log_dirs_node(),
        None,
        "DescribeDelegationToken must not hop via DescribeLogDirs"
    );
    assert_eq!(
        mock.last_assign_replicas_to_dirs_node(),
        None,
        "DescribeDelegationToken must not hop via AssignReplicasToDirs"
    );
    assert_eq!(
        mock.last_alter_replica_log_dirs_node(),
        None,
        "DescribeDelegationToken must not hop via AlterReplicaLogDirs"
    );
    assert_eq!(
        mock.last_describe_groups_node(),
        None,
        "DescribeDelegationToken must not hop via DescribeGroups or FindCoordinator"
    );
    assert_eq!(
        mock.last_alter_client_quotas_node(),
        None,
        "DescribeDelegationToken must not hop via Metadata controller_id"
    );
    let timed = admin
        .describe_delegation_token_timeout(
            DescribeDelegationTokenRequest::new(Some(vec![DescribeDelegationTokenOwner::new(
                "User", "r",
            )])),
            Duration::from_secs(5),
        )
        .await
        .unwrap();
    assert_eq!(timed.error_code, 0);
    let all = admin.describe_delegation_tokens().await.unwrap();
    assert_eq!(all.error_code, 0);
    assert_eq!(
        mock.last_describe_delegation_token(),
        Some(DescribeDelegationTokenRequest::default()),
        "describeDelegationToken() sends owners None"
    );
    let timed_all = admin
        .describe_delegation_tokens_timeout(Duration::from_secs(5))
        .await
        .unwrap();
    assert_eq!(timed_all.error_code, 0);
    admin.close().await.unwrap();
    mock.hide_api(DESCRIBE_DELEGATION_TOKEN);
    let mut admin = Admin::connect(mock.addr.clone()).await.unwrap();
    let err = admin.describe_delegation_tokens().await.unwrap_err();
    assert!(
        err.to_string().contains("unsupported"),
        "DescribeDelegationToken is optional at connect: {err}"
    );
    let classic = admin
        .describe_classic_groups(&["g-classic"], false)
        .await
        .unwrap();
    assert_eq!(classic[0].group_id, "g-classic");
    admin.close().await.unwrap();
}

#[tokio::test]
async fn describe_delegation_token_negotiates_v1_when_broker_caps() {
    let mock = common::Mock::start().await;
    mock.set_api_max(DESCRIBE_DELEGATION_TOKEN, 1);
    let mut admin = Admin::connect(mock.addr.clone()).await.unwrap();
    let req = DescribeDelegationTokenRequest::new(Some(vec![DescribeDelegationTokenOwner::new(
        "User", "r",
    )]));
    let resp = admin.describe_delegation_token(req.clone()).await.unwrap();
    assert_eq!(resp.error_code, 0);
    assert!(resp.tokens.is_empty());
    assert_eq!(
        mock.last_describe_delegation_token_version(),
        Some(1),
        "client must speak DescribeDelegationToken v1 when the broker max is 1"
    );
    assert_eq!(
        mock.last_describe_delegation_token(),
        Some(req),
        "v1 request owners match v3"
    );
}

#[tokio::test]
async fn delete_groups_follows_group_coordinator() {
    let mock = common::Mock::start_two_node().await;
    mock.move_coordinator();
    let mut admin = Admin::connect(mock.addr.clone()).await.unwrap();

    let first = admin.delete_groups(&["g-del"]).await.unwrap();
    assert_eq!(first.len(), 1);
    assert_eq!(first[0].error_code, 0);
    assert_eq!(first[0].group_id, "g-del");
    assert_eq!(
        mock.last_delete_groups_node(),
        Some(2),
        "DeleteGroups must land on the group coordinator, not bootstrap"
    );
    assert_eq!(
        mock.last_delete_groups_version(),
        Some(2),
        "Admin must prefer DeleteGroups v2 when the broker advertises it"
    );
    assert!(
        mock.find_coordinator_key_types()
            .contains(&COORDINATOR_GROUP),
        "DeleteGroups must FindCoordinator key_type=0"
    );

    mock.move_coordinator();
    let again = admin.delete_groups(&["g-del"]).await.unwrap();
    assert_eq!(again.len(), 1);
    assert_eq!(again[0].error_code, 0);
    assert_eq!(
        again[0].group_id.as_str(),
        "g-del",
        "retry on the new coordinator must still return fixture group, not the 16 empty body"
    );
    assert_eq!(
        mock.delete_groups_not_coordinator(),
        1,
        "stale coordinator must return NOT_COORDINATOR (16) once"
    );
    assert_eq!(
        mock.last_delete_groups_node(),
        Some(1),
        "DeleteGroups must FindCoordinator after NOT_COORDINATOR"
    );
    let timed = admin
        .delete_groups_timeout(&["g-del"], Duration::from_secs(5))
        .await
        .unwrap();
    assert_eq!(timed.len(), 1);
    assert_eq!(timed[0].group_id, "g-del");
}

#[tokio::test]
async fn delete_groups_batches_find_coordinator() {
    let mock = common::Mock::start().await;
    let mut admin = Admin::connect(mock.addr.clone()).await.unwrap();
    let before_find = mock.find_coordinator_calls();
    let before_del = mock.delete_groups_calls();
    let deleted = admin.delete_groups(&["g-a", "g-b"]).await.unwrap();
    assert_eq!(deleted.len(), 2);
    assert_eq!(deleted[0].group_id, "g-a");
    assert_eq!(deleted[1].group_id, "g-b");
    assert_eq!(deleted[0].error_code, 0);
    assert_eq!(deleted[1].error_code, 0);
    assert_eq!(
        mock.last_find_coordinator_key_count(),
        2,
        "deleteGroups must send CoordinatorKeys of N on v4+"
    );
    assert_eq!(
        mock.find_coordinator_calls().saturating_sub(before_find),
        1,
        "groups that share a coordinator must be one FindCoordinator"
    );
    assert_eq!(
        mock.last_delete_groups_n(),
        2,
        "deleteGroups must send GroupId array of N on one coordinator"
    );
    assert_eq!(
        mock.delete_groups_calls().saturating_sub(before_del),
        1,
        "groups that share a coordinator must be one DeleteGroups"
    );
    admin.close().await.unwrap();
}

#[tokio::test]
async fn delete_groups_negotiates_v1_when_broker_caps() {
    let mock = common::Mock::start().await;
    mock.set_api_max(DELETE_GROUPS, 1);
    let mut admin = Admin::connect(mock.addr.clone()).await.unwrap();
    let deleted = admin.delete_groups(&["g-v1"]).await.unwrap();
    assert_eq!(deleted.len(), 1);
    assert_eq!(deleted[0].error_code, 0);
    assert_eq!(deleted[0].group_id, "g-v1");
    assert_eq!(
        mock.last_delete_groups_version(),
        Some(1),
        "client must speak DeleteGroups v1 when the broker max is 1"
    );
}

#[tokio::test]
async fn delete_groups_negotiates_v0_when_broker_caps() {
    let mock = common::Mock::start().await;
    mock.set_api_max(DELETE_GROUPS, 0);
    let mut admin = Admin::connect(mock.addr.clone()).await.unwrap();
    let deleted = admin.delete_groups(&["g-v0"]).await.unwrap();
    assert_eq!(deleted.len(), 1);
    assert_eq!(deleted[0].error_code, 0);
    assert_eq!(deleted[0].group_id, "g-v0");
    assert_eq!(
        mock.last_delete_groups_version(),
        Some(0),
        "client must speak DeleteGroups v0 when the broker max is 0"
    );
}

#[tokio::test]
async fn delete_share_groups_follows_group_coordinator() {
    let mock = common::Mock::start_two_node().await;
    mock.move_coordinator();
    let mut admin = Admin::connect(mock.addr.clone()).await.unwrap();

    let empty = admin
        .delete_share_groups(Vec::<String>::new())
        .await
        .unwrap();
    assert!(empty.is_empty());
    assert_eq!(
        mock.last_delete_groups_node(),
        None,
        "empty delete_share_groups is a no-op"
    );

    let first = admin.delete_share_groups(["g-share"]).await.unwrap();
    assert_eq!(first.len(), 1);
    assert_eq!(first[0].error_code, 0);
    assert_eq!(first[0].group_id, "g-share");
    assert_eq!(
        mock.last_delete_groups_node(),
        Some(2),
        "deleteShareGroups must land on the group coordinator"
    );
    assert!(
        mock.find_coordinator_key_types()
            .contains(&COORDINATOR_GROUP),
        "deleteShareGroups must FindCoordinator key_type=0"
    );
    let timed = admin
        .delete_share_groups_timeout(["g-share"], Duration::from_secs(5))
        .await
        .unwrap();
    assert_eq!(timed[0].group_id, "g-share");
    admin.close().await.unwrap();
}

#[tokio::test]
async fn delete_consumer_groups_follows_group_coordinator() {
    let mock = common::Mock::start_two_node().await;
    mock.move_coordinator();
    let mut admin = Admin::connect(mock.addr.clone()).await.unwrap();

    let empty = admin
        .delete_consumer_groups(Vec::<String>::new())
        .await
        .unwrap();
    assert!(empty.is_empty());
    assert_eq!(
        mock.last_delete_groups_node(),
        None,
        "empty delete_consumer_groups is a no-op"
    );

    let first = admin.delete_consumer_groups(["g-cons"]).await.unwrap();
    assert_eq!(first.len(), 1);
    assert_eq!(first[0].error_code, 0);
    assert_eq!(first[0].group_id, "g-cons");
    assert_eq!(
        mock.last_delete_groups_node(),
        Some(2),
        "deleteConsumerGroups must land on the group coordinator"
    );
    assert!(
        mock.find_coordinator_key_types()
            .contains(&COORDINATOR_GROUP),
        "deleteConsumerGroups must FindCoordinator key_type=0"
    );
    let timed = admin
        .delete_consumer_groups_timeout(["g-cons"], Duration::from_secs(5))
        .await
        .unwrap();
    assert_eq!(timed[0].group_id, "g-cons");
    admin.close().await.unwrap();
}

#[tokio::test]
async fn describe_classic_groups_follows_group_coordinator() {
    let mock = common::Mock::start_two_node().await;
    mock.move_coordinator();
    let mut admin = Admin::connect(mock.addr.clone()).await.unwrap();
    let described = admin
        .describe_classic_groups(&["g-classic"], false)
        .await
        .unwrap();
    assert_eq!(described.len(), 1);
    assert_eq!(described[0].group_id, "g-classic");
    assert_eq!(described[0].error_code, 0);
    assert_eq!(
        mock.last_describe_groups_node(),
        Some(2),
        "describeClassicGroups must land on the group coordinator"
    );
    let timed = admin
        .describe_classic_groups_timeout(&["g-classic"], false, Duration::from_secs(5))
        .await
        .unwrap();
    assert_eq!(timed.len(), 1);
    assert_eq!(timed[0].group_id, "g-classic");
    admin.close().await.unwrap();
}

#[tokio::test]
async fn describe_consumer_groups_follows_group_coordinator() {
    let mock = common::Mock::start_two_node().await;
    mock.move_coordinator();
    let mut admin = Admin::connect(mock.addr.clone()).await.unwrap();
    let described = admin
        .describe_consumer_groups(&["g-cons"], false)
        .await
        .unwrap();
    assert_eq!(described.len(), 1);
    assert_eq!(described[0].group_id(), "g-cons");
    assert_eq!(described[0].error_code(), 0);
    assert!(described[0].is_consumer_protocol());
    assert_eq!(described[0].group_state(), "Stable");
    assert_eq!(described[0].partition_assignor(), "uniform");
    assert_eq!(described[0].group_type(), GroupType::Consumer);
    assert_eq!(described[0].group_epoch(), Some(1));
    assert_eq!(described[0].target_assignment_epoch(), Some(1));
    assert!(!described[0].is_simple_consumer_group());
    assert_eq!(
        mock.last_consumer_group_describe_node(),
        Some(2),
        "describeConsumerGroups must land ConsumerGroupDescribe on the group coordinator"
    );
    assert_eq!(
        mock.last_describe_groups_node(),
        None,
        "successful api 69 must not fall back to DescribeGroups"
    );
    let timed = admin
        .describe_consumer_groups_timeout(&["g-cons"], false, Duration::from_secs(5))
        .await
        .unwrap();
    assert_eq!(timed.len(), 1);
    assert_eq!(timed[0].group_id(), "g-cons");
    assert!(timed[0].is_consumer_protocol());

    mock.set_consumer_group_describe_error("g-classic-fb", error::GROUP_ID_NOT_FOUND);
    let classic = admin
        .describe_consumer_groups(&["g-classic-fb"], false)
        .await
        .unwrap();
    assert_eq!(classic.len(), 1);
    assert!(!classic[0].is_consumer_protocol());
    assert_eq!(classic[0].group_id(), "g-classic-fb");
    assert_eq!(classic[0].group_type(), GroupType::Classic);
    assert!(classic[0].group_epoch().is_none());
    assert!(classic[0].target_assignment_epoch().is_none());
    assert!(!classic[0].is_simple_consumer_group());
    assert_eq!(classic[0].partition_assignor(), "");
    assert_eq!(
        mock.last_describe_groups_node(),
        Some(2),
        "classic fallback must still land DescribeGroups on the group coordinator"
    );

    admin.close().await.unwrap();
    let cg_calls = mock.consumer_group_describe_calls();
    mock.hide_api(CONSUMER_GROUP_DESCRIBE);
    let mut admin = Admin::connect(mock.addr.clone()).await.unwrap();
    let hidden = admin
        .describe_consumer_groups(&["g-cons"], false)
        .await
        .unwrap();
    assert_eq!(hidden.len(), 1);
    assert!(!hidden[0].is_consumer_protocol());
    assert_eq!(
        mock.consumer_group_describe_calls(),
        cg_calls,
        "hidden ConsumerGroupDescribe must not be sent"
    );
    assert_eq!(
        mock.last_describe_groups_node(),
        Some(2),
        "when api 69 is hidden, describeConsumerGroups must DescribeGroups on the coordinator"
    );
    admin.close().await.unwrap();
}

#[tokio::test]
async fn force_terminate_transaction_inits_on_txn_coordinator() {
    let mock = common::Mock::start_two_node().await;
    mock.move_txn_coordinator();
    let mut admin = Admin::connect(mock.addr.clone()).await.unwrap();
    let terminated = admin.force_terminate_transaction("tid-term").await.unwrap();
    assert_eq!(terminated.transactional_id, "tid-term");
    assert_eq!(terminated.producer_id, 1000);
    assert_eq!(
        mock.last_init_producer_id_node(),
        Some(2),
        "forceTerminateTransaction must InitProducerId on the txn coordinator"
    );
    admin.close().await.unwrap();
}

#[tokio::test]
async fn share_group_describe_follows_group_coordinator() {
    let mock = common::Mock::start_two_node().await;
    mock.move_coordinator();
    let mut admin = Admin::connect(mock.addr.clone()).await.unwrap();

    let first = admin
        .share_group_describe(&["sg-desc"], false)
        .await
        .unwrap();
    assert_eq!(first.len(), 1);
    assert_eq!(first[0].error_code, 0);
    assert_eq!(first[0].group_id, "sg-desc");
    assert_eq!(first[0].group_state, "Stable");
    assert_eq!(first[0].group_epoch, 1);
    assert_eq!(first[0].assignor_name, "uniform");
    assert_eq!(
        mock.last_share_group_describe_node(),
        Some(2),
        "ShareGroupDescribe must land on the group coordinator, not bootstrap"
    );
    assert_eq!(
        mock.last_share_group_describe_version(),
        Some(1),
        "Admin must prefer ShareGroupDescribe v1 when the broker advertises it"
    );
    assert!(
        mock.find_coordinator_key_types()
            .contains(&COORDINATOR_GROUP),
        "ShareGroupDescribe must FindCoordinator key_type=0"
    );

    mock.move_coordinator();
    let again = admin
        .share_group_describe(&["sg-desc"], false)
        .await
        .unwrap();
    assert_eq!(again.len(), 1);
    assert_eq!(again[0].error_code, 0);
    assert_eq!(
        again[0].group_state.as_str(),
        "Stable",
        "retry on the new coordinator must still return fixture state, not the 16 empty body"
    );
    assert_eq!(
        mock.share_group_describe_not_coordinator(),
        1,
        "stale coordinator must return NOT_COORDINATOR (16) once"
    );
    assert_eq!(
        mock.last_share_group_describe_node(),
        Some(1),
        "ShareGroupDescribe must FindCoordinator after NOT_COORDINATOR"
    );
    let timed = admin
        .share_group_describe_timeout(&["sg-desc"], false, Duration::from_secs(5))
        .await
        .unwrap();
    assert_eq!(timed.len(), 1);
    assert_eq!(timed[0].group_id, "sg-desc");
}

#[tokio::test]
async fn share_group_describe_batches_find_coordinator() {
    let mock = common::Mock::start().await;
    let mut admin = Admin::connect(mock.addr.clone()).await.unwrap();
    let before_find = mock.find_coordinator_calls();
    let before_desc = mock.share_group_describe_calls();
    let described = admin
        .share_group_describe(&["g-a", "g-b"], false)
        .await
        .unwrap();
    assert_eq!(described.len(), 2);
    assert_eq!(described[0].group_id, "g-a");
    assert_eq!(described[1].group_id, "g-b");
    assert_eq!(described[0].error_code, 0);
    assert_eq!(described[1].error_code, 0);
    assert_eq!(
        mock.last_find_coordinator_key_count(),
        2,
        "shareGroupDescribe must send CoordinatorKeys of N on v4+"
    );
    assert_eq!(
        mock.find_coordinator_calls().saturating_sub(before_find),
        1,
        "groups that share a coordinator must be one FindCoordinator"
    );
    assert_eq!(
        mock.last_share_group_describe_n(),
        2,
        "shareGroupDescribe must send GroupIds of N on one coordinator"
    );
    assert_eq!(
        mock.share_group_describe_calls()
            .saturating_sub(before_desc),
        1,
        "groups that share a coordinator must be one ShareGroupDescribe"
    );
    admin.close().await.unwrap();
}

#[tokio::test]
async fn share_group_describe_negotiates_v0_when_broker_caps() {
    let mock = common::Mock::start().await;
    mock.set_api_max(SHARE_GROUP_DESCRIBE, 0);
    let mut admin = Admin::connect(mock.addr.clone()).await.unwrap();
    let described = admin.share_group_describe(&["sg-v0"], false).await.unwrap();
    assert_eq!(described.len(), 1);
    assert_eq!(described[0].error_code, 0);
    assert_eq!(described[0].group_id, "sg-v0");
    assert_eq!(
        mock.last_share_group_describe_version(),
        Some(0),
        "client must speak ShareGroupDescribe v0 when the broker max is 0"
    );
    admin.close().await.unwrap();
}

#[tokio::test]
async fn describe_share_groups_follows_group_coordinator() {
    let mock = common::Mock::start_two_node().await;
    mock.move_coordinator();
    let mut admin = Admin::connect(mock.addr.clone()).await.unwrap();
    let described = admin
        .describe_share_groups(&["sg-java"], false)
        .await
        .unwrap();
    assert_eq!(described.len(), 1);
    assert_eq!(described[0].group_id(), "sg-java");
    assert_eq!(described[0].error_code(), 0);
    assert_eq!(described[0].group_state(), "Stable");
    assert_eq!(described[0].group_epoch(), 1);
    assert_eq!(described[0].assignment_epoch(), 1);
    assert_eq!(described[0].assignor_name(), "uniform");
    assert!(described[0].members().is_empty());
    assert_eq!(
        mock.last_share_group_describe_node(),
        Some(2),
        "describeShareGroups must land on the group coordinator"
    );
    let timed = admin
        .describe_share_groups_timeout(&["sg-java"], false, Duration::from_secs(5))
        .await
        .unwrap();
    assert_eq!(timed.len(), 1);
    assert_eq!(timed[0].group_id, "sg-java");
    admin.close().await.unwrap();
}

#[tokio::test]
async fn describe_share_group_offsets_follows_group_coordinator() {
    let mock = common::Mock::start_two_node().await;
    mock.move_coordinator();
    let mut admin = Admin::connect(mock.addr.clone()).await.unwrap();

    let first = admin
        .describe_share_group_offsets(&[DescribeShareGroupOffsetsGroup::new("sg-off")])
        .await
        .unwrap();
    assert_eq!(first.len(), 1);
    assert_eq!(first[0].error_code(), 0);
    assert_eq!(first[0].group_id(), "sg-off");
    assert!(first[0].topics().is_empty());
    assert_eq!(
        mock.last_describe_share_group_offsets_node(),
        Some(2),
        "DescribeShareGroupOffsets must land on the group coordinator, not bootstrap"
    );
    assert!(
        mock.find_coordinator_key_types()
            .contains(&COORDINATOR_GROUP),
        "DescribeShareGroupOffsets must FindCoordinator key_type=0"
    );

    mock.move_coordinator();
    let again = admin
        .describe_share_group_offsets(&[DescribeShareGroupOffsetsGroup::new("sg-off")])
        .await
        .unwrap();
    assert_eq!(again.len(), 1);
    assert_eq!(again[0].error_code, 0);
    assert_eq!(
        again[0].group_id.as_str(),
        "sg-off",
        "retry on the new coordinator must still return fixture group, not the 16 empty body"
    );
    assert_eq!(
        mock.describe_share_group_offsets_not_coordinator(),
        1,
        "stale coordinator must return NOT_COORDINATOR (16) once"
    );
    assert_eq!(
        mock.last_describe_share_group_offsets_node(),
        Some(1),
        "DescribeShareGroupOffsets must FindCoordinator after NOT_COORDINATOR"
    );
    let timed_groups = [DescribeShareGroupOffsetsGroup::new("sg-off")];
    let timed = admin
        .describe_share_group_offsets_timeout(&timed_groups, Duration::from_secs(5))
        .await
        .unwrap();
    assert_eq!(timed.len(), 1);
    assert_eq!(timed[0].group_id, "sg-off");
}

#[tokio::test]
async fn describe_share_group_offsets_batches_find_coordinator() {
    let mock = common::Mock::start().await;
    let mut admin = Admin::connect(mock.addr.clone()).await.unwrap();
    let before_find = mock.find_coordinator_calls();
    let before_desc = mock.describe_share_group_offsets_calls();
    let described = admin
        .describe_share_group_offsets(&[
            DescribeShareGroupOffsetsGroup::new("g-a"),
            DescribeShareGroupOffsetsGroup::new("g-b"),
        ])
        .await
        .unwrap();
    assert_eq!(described.len(), 2);
    assert_eq!(described[0].group_id, "g-a");
    assert_eq!(described[1].group_id, "g-b");
    assert_eq!(described[0].error_code, 0);
    assert_eq!(described[1].error_code, 0);
    assert_eq!(
        mock.last_find_coordinator_key_count(),
        2,
        "describeShareGroupOffsets must send CoordinatorKeys of N on v4+"
    );
    assert_eq!(
        mock.find_coordinator_calls().saturating_sub(before_find),
        1,
        "groups that share a coordinator must be one FindCoordinator"
    );
    assert_eq!(
        mock.last_describe_share_group_offsets_n(),
        2,
        "describeShareGroupOffsets must send Groups of N on one coordinator"
    );
    assert_eq!(
        mock.describe_share_group_offsets_calls()
            .saturating_sub(before_desc),
        1,
        "groups that share a coordinator must be one DescribeShareGroupOffsets"
    );
    admin.close().await.unwrap();
}

#[tokio::test]
async fn list_share_group_offsets_follows_group_coordinator() {
    let mock = common::Mock::start_two_node().await;
    mock.move_coordinator();
    let mut admin = Admin::connect(mock.addr.clone()).await.unwrap();
    let listed = admin
        .list_share_group_offsets(&[DescribeShareGroupOffsetsGroup::new("sg-list")])
        .await
        .unwrap();
    assert_eq!(listed.len(), 1);
    assert_eq!(listed[0].group_id, "sg-list");
    assert_eq!(listed[0].error_code, 0);
    assert_eq!(
        mock.last_describe_share_group_offsets_node(),
        Some(2),
        "listShareGroupOffsets must land on the group coordinator"
    );
    let timed_groups = [DescribeShareGroupOffsetsGroup::new("sg-list")];
    let timed = admin
        .list_share_group_offsets_timeout(&timed_groups, Duration::from_secs(5))
        .await
        .unwrap();
    assert_eq!(timed.len(), 1);
    assert_eq!(timed[0].group_id, "sg-list");
    admin.close().await.unwrap();
}

#[tokio::test]
async fn alter_share_group_offsets_follows_group_coordinator() {
    let mock = common::Mock::start_two_node().await;
    mock.move_coordinator();
    let mut admin = Admin::connect(mock.addr.clone()).await.unwrap();

    let first = admin
        .alter_share_group_offsets("sg-alt", &[])
        .await
        .unwrap();
    assert_eq!(first.error_code(), 0);
    assert!(first.topics().is_empty());
    assert_eq!(
        mock.last_alter_share_group_offsets_node(),
        Some(2),
        "AlterShareGroupOffsets must land on the group coordinator, not bootstrap"
    );
    assert!(
        mock.find_coordinator_key_types()
            .contains(&COORDINATOR_GROUP),
        "AlterShareGroupOffsets must FindCoordinator key_type=0"
    );

    mock.move_coordinator();
    let again = admin
        .alter_share_group_offsets("sg-alt", &[AlterShareGroupOffsetsTopic::new("t", vec![])])
        .await
        .unwrap();
    assert_eq!(again.error_code, 0);
    assert!(
        again.topics.is_empty(),
        "retry on the new coordinator must still return fixture empty topics, not the 16 empty body"
    );
    assert_eq!(
        mock.alter_share_group_offsets_not_coordinator(),
        1,
        "stale coordinator must return NOT_COORDINATOR (16) once"
    );
    assert_eq!(
        mock.last_alter_share_group_offsets_node(),
        Some(1),
        "AlterShareGroupOffsets must FindCoordinator after NOT_COORDINATOR"
    );
    let timed_topics = [AlterShareGroupOffsetsTopic::new("t", vec![])];
    let timed = admin
        .alter_share_group_offsets_timeout("sg-alt", &timed_topics, Duration::from_secs(5))
        .await
        .unwrap();
    assert_eq!(timed.error_code, 0);
}

#[tokio::test]
async fn delete_share_group_offsets_follows_group_coordinator() {
    let mock = common::Mock::start_two_node().await;
    mock.move_coordinator();
    let mut admin = Admin::connect(mock.addr.clone()).await.unwrap();

    let first = admin
        .delete_share_group_offsets("sg-del", &[])
        .await
        .unwrap();
    assert_eq!(first.error_code(), 0);
    assert!(first.topics().is_empty());
    assert_eq!(
        mock.last_delete_share_group_offsets_node(),
        Some(2),
        "DeleteShareGroupOffsets must land on the group coordinator, not bootstrap"
    );
    assert!(
        mock.find_coordinator_key_types()
            .contains(&COORDINATOR_GROUP),
        "DeleteShareGroupOffsets must FindCoordinator key_type=0"
    );

    mock.move_coordinator();
    let again = admin
        .delete_share_group_offsets("sg-del", &[DeleteShareGroupOffsetsTopic::new("t")])
        .await
        .unwrap();
    assert_eq!(again.error_code, 0);
    assert!(
        again.topics.is_empty(),
        "retry on the new coordinator must still return fixture empty topics, not the 16 empty body"
    );
    assert_eq!(
        mock.delete_share_group_offsets_not_coordinator(),
        1,
        "stale coordinator must return NOT_COORDINATOR (16) once"
    );
    assert_eq!(
        mock.last_delete_share_group_offsets_node(),
        Some(1),
        "DeleteShareGroupOffsets must FindCoordinator after NOT_COORDINATOR"
    );
    let timed_topics = [DeleteShareGroupOffsetsTopic::new("t")];
    let timed = admin
        .delete_share_group_offsets_timeout("sg-del", &timed_topics, Duration::from_secs(5))
        .await
        .unwrap();
    assert_eq!(timed.error_code, 0);
}

#[tokio::test]
async fn offset_delete_removes_committed_offset() {
    let mock = common::Mock::start().await;
    let mut pcfg = ProducerConfig::bootstrap([mock.addr.clone()]);
    pcfg.linger = Duration::ZERO;
    let producer = Producer::new(pcfg).await.unwrap();
    producer
        .send(ProduceRecord::to("t").value(&b"od-keep"[..]))
        .await
        .unwrap();
    producer.close().await.unwrap();

    let mut ccfg = ConsumerConfig::bootstrap([mock.addr.clone()]);
    ccfg.max_wait_ms = 10;
    let mut g = ConsumerGroup::join(ccfg.clone(), "od-del", "t")
        .await
        .unwrap();
    let recs = g.poll().await.unwrap();
    assert_eq!(recs[0].value.as_deref(), Some(&b"od-keep"[..]));
    g.commit().await.unwrap();
    g.leave().await.unwrap();

    let mut admin = Admin::connect(mock.addr.clone()).await.unwrap();
    let deleted = admin
        .delete_offsets("od-del", [TopicPartition::new("t", 0)])
        .await
        .unwrap();
    assert_eq!(deleted.len(), 1);
    assert_eq!(deleted[0].error_code, 0);
    assert_eq!(
        mock.last_offset_delete_node(),
        Some(1),
        "OffsetDelete must land on the group coordinator"
    );

    let mut g2 = ConsumerGroup::join(ccfg, "od-del", "t").await.unwrap();
    let recs = g2.poll().await.unwrap();
    assert_eq!(
        recs[0].value.as_deref(),
        Some(&b"od-keep"[..]),
        "OffsetDelete must drop the committed offset so rejoin replays"
    );
    g2.leave().await.unwrap();
}

#[tokio::test]
async fn offset_delete_follows_group_coordinator() {
    let mock = common::Mock::start_two_node().await;
    mock.move_coordinator();
    let mut pcfg = ProducerConfig::bootstrap([mock.addr.clone()]);
    pcfg.linger = Duration::ZERO;
    let producer = Producer::new(pcfg).await.unwrap();
    producer
        .send(ProduceRecord::to("t").value(&b"od-coord"[..]))
        .await
        .unwrap();
    producer.close().await.unwrap();

    let mut ccfg = ConsumerConfig::bootstrap([mock.addr.clone()]);
    ccfg.max_wait_ms = 10;
    let mut g = ConsumerGroup::join(ccfg, "od-coord", "t").await.unwrap();
    let recs = g.poll().await.unwrap();
    assert_eq!(recs[0].value.as_deref(), Some(&b"od-coord"[..]));
    g.commit().await.unwrap();
    g.leave().await.unwrap();

    let mut admin = Admin::connect(mock.addr.clone()).await.unwrap();
    let deleted = admin
        .delete_offsets("od-coord", [TopicPartition::new("t", 0)])
        .await
        .unwrap();
    assert_eq!(deleted[0].error_code, 0);
    assert_eq!(
        mock.last_offset_delete_node(),
        Some(2),
        "OffsetDelete must land on the group coordinator, not bootstrap"
    );

    mock.move_coordinator();
    let again = admin
        .delete_offsets("od-coord", [TopicPartition::new("t", 0)])
        .await
        .unwrap();
    assert_eq!(again[0].error_code, 0);
    assert_eq!(
        mock.offset_delete_not_coordinator(),
        1,
        "stale coordinator must return NOT_COORDINATOR (16) once"
    );
    assert_eq!(
        mock.last_offset_delete_node(),
        Some(1),
        "OffsetDelete must FindCoordinator after NOT_COORDINATOR"
    );
    let timed_tp = TopicPartition::new("t", 0);
    let timed = admin
        .delete_offsets_timeout("od-coord", [timed_tp], Duration::from_secs(5))
        .await
        .unwrap();
    assert_eq!(timed[0].error_code, 0);
    admin.close().await.unwrap();
    mock.hide_api(OFFSET_DELETE);
    let mut admin = Admin::connect(mock.addr.clone()).await.unwrap();
    let err = admin
        .delete_offsets("od-coord", [TopicPartition::new("t", 0)])
        .await
        .unwrap_err();
    assert!(
        err.to_string().contains("unsupported"),
        "OffsetDelete is optional at connect: {err}"
    );
    let classic = admin
        .describe_classic_groups(&["g-classic"], false)
        .await
        .unwrap();
    assert_eq!(classic[0].group_id, "g-classic");
    admin.close().await.unwrap();
}

#[tokio::test]
async fn delete_consumer_group_offsets_follows_group_coordinator() {
    let mock = common::Mock::start_two_node().await;
    mock.move_coordinator();
    let mut admin = Admin::connect(mock.addr.clone()).await.unwrap();
    let deleted = admin
        .delete_consumer_group_offsets("g-off", [TopicPartition::new("t", 0)])
        .await
        .unwrap();
    assert_eq!(deleted.len(), 1);
    assert_eq!(deleted[0].error_code, 0);
    assert_eq!(
        mock.last_offset_delete_node(),
        Some(2),
        "deleteConsumerGroupOffsets must land on the group coordinator"
    );
    let timed_tp = TopicPartition::new("t", 0);
    let timed = admin
        .delete_consumer_group_offsets_timeout("g-off", [timed_tp], Duration::from_secs(5))
        .await
        .unwrap();
    assert_eq!(timed[0].error_code, 0);
    admin.close().await.unwrap();
}

#[tokio::test]
async fn admin_against_kafka_if_present() {
    if tokio::net::TcpStream::connect("127.0.0.1:9092")
        .await
        .is_err()
    {
        return;
    }
    let name = format!("pl-admin-{}", std::process::id());
    let mut admin = Admin::connect("127.0.0.1:9092").await.unwrap();
    let broker = admin
        .describe_configs(&[ConfigResource::broker(1)], false)
        .await
        .unwrap();
    assert_eq!(broker[0].error_code, 0, "broker describe: {broker:?}");
    assert!(
        !broker[0].entries.is_empty(),
        "broker describe returned no entries: {broker:?}"
    );
    let _ = admin.delete_topics(&[&name], 10_000).await;
    let created = admin
        .create_topics(
            &[NewTopic::new(&name, 3, 1).config("cleanup.policy", "delete")],
            10_000,
            false,
        )
        .await
        .unwrap();
    assert_eq!(created[0].error_code, 0, "{created:?}");
    let mut described = None;
    for _ in 0..20 {
        let got = admin
            .describe_configs(&[ConfigResource::topic(&name)], false)
            .await
            .unwrap();
        if got[0].error_code == 0 {
            described = Some(got);
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    let described = described.expect("DescribeConfigs did not see created topic");
    assert!(
        described[0]
            .entries
            .iter()
            .any(|e| e.name == "cleanup.policy"),
        "{described:?}"
    );
    let deleted = admin.delete_topics(&[&name], 10_000).await.unwrap();
    assert_eq!(deleted[0].error_code, 0, "{deleted:?}");
}

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

use partitionline::protocol::group::{COORDINATOR_GROUP, COORDINATOR_TRANSACTION};
use partitionline::{
    error, AbortTransactionSpec, AclBinding, AclResourceType, Admin, AdminConfig, AlterConfig,
    AlterReplicaLogDirsDirectory, AlterReplicaLogDirsRequest, AlterReplicaLogDirsTopic,
    AlterShareGroupOffsetsTopic, AssignReplicasToDirsDirectory, AssignReplicasToDirsPartition,
    AssignReplicasToDirsRequest, AssignReplicasToDirsTopic, ClientQuotaAlteration,
    ClientQuotaEntity, ClientQuotaFilterComponent, ClientQuotaOp, Compression, ConfigResource,
    ConfigResourceType, Consumer, ConsumerConfig, ConsumerGroup, CreatableRenewer,
    CreateDelegationTokenRequest, DeleteShareGroupOffsetsTopic, DescribableLogDirTopic,
    DescribeDelegationTokenOwner, DescribeDelegationTokenRequest, DescribeLogDirsRequest,
    DescribeShareGroupOffsetsGroup, Error, ExpireDelegationTokenRequest, FeatureUpdate,
    IsolationLevel, NewPartitions, NewTopic, OidcConfig, OngoingReassignment,
    PartitionReassignment, ProduceRecord, Producer, ProducerConfig, RenewDelegationTokenRequest,
    ReplicaLogDirInfo, ScramMechanism, ShareGroup, TopicPartition, TopicPartitionReplica,
    TransactionState, TransactionTopic, UserScramCredentialDeletion, UserScramCredentialUpsertion,
    CONFIG_RESOURCE_CLIENT_METRICS, DEFAULT_LEAVE_GROUP_REASON, EARLIEST_TIMESTAMP,
    LATEST_TIMESTAMP, QUOTA_MATCH_EXACT, SCRAM_SHA_256, SCRAM_SHA_512,
};
use std::time::{Duration, Instant};

#[tokio::test]
async fn try_send_flush_writes_record() {
    let mock = common::Mock::start().await;
    let mut pcfg = ProducerConfig::bootstrap([mock.addr.clone()]);
    pcfg.linger = Duration::ZERO;
    let producer = Producer::new(pcfg).await.unwrap();
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
        Some(11),
        "Producer must prefer Produce v11 when the broker advertises it"
    );
    producer.close().await.unwrap();
    let mut ccfg = ConsumerConfig::bootstrap([mock.addr.clone()]);
    ccfg.max_wait_ms = 10;
    let mut consumer = Consumer::new(ccfg).await.unwrap();
    consumer.assign("t", 0, 0).await.unwrap();
    let recs = consumer.fetch().await.unwrap();
    assert_eq!(recs[0].value.as_deref(), Some(&b"try-send"[..]));
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
        1,
        "AddPartitionsToTxn must be one RPC, got {}",
        mock.add_partitions_to_txn_calls()
    );
    assert_eq!(mock.last_add_partitions_to_txn(), 3);
    assert_eq!(
        mock.last_add_partitions_to_txn_version(),
        Some(3),
        "Producer must prefer AddPartitionsToTxn v3 when the broker advertises it"
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
        mock.last_add_partitions_producer_epoch(),
        Some(1),
        "EndTxn v5 must apply the bumped producer epoch on the next AddPartitionsToTxn"
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
        Some(3),
        "Producer must prefer FindCoordinator v3 when the broker advertises it"
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
    assert_eq!(mock.last_add_partitions_node(), Some(2));
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
        Some(12),
        "Consumer must prefer Fetch v12 when the broker advertises it"
    );
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
        Some(9),
        "Consumer must prefer ListOffsets v9 when the broker advertises it"
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
    let md = producer
        .send(ProduceRecord::to("t").value(&b"sasl-ok"[..]))
        .await
        .unwrap();
    assert_eq!(md.offset, 0);
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
        Some(3),
        "ConsumerGroup must prefer FindCoordinator v3 when the broker advertises it"
    );
    let recs = group.poll().await.unwrap();
    assert_eq!(recs.len(), 1);
    assert_eq!(recs[0].value.as_deref(), Some(&b"grouped"[..]));
    group.commit().await.unwrap();
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
    group.leave().await.unwrap();
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
    assert_eq!(recs[0].value.as_deref(), Some(&b"share-a"[..]));
    let off = recs[0].offset;
    g.accept(&recs).await.unwrap();
    assert_eq!(
        mock.share_ack_calls(),
        1,
        "accept must be one ShareAcknowledge, not one RPC per record"
    );
    assert_eq!(mock.last_share_ack_epoch(), Some(1));
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
    let created = admin
        .create_acls(&[AclBinding::allow_topic("acl-t", "User:alice")])
        .await
        .unwrap();
    assert_eq!(created, vec![0]);
    let listed = admin.describe_acls(AclResourceType::Topic).await.unwrap();
    assert_eq!(listed.len(), 1);
    assert_eq!(listed[0].principal, "User:alice");
    assert_eq!(admin.delete_acls(AclResourceType::Topic).await.unwrap(), 0);
    assert!(admin
        .describe_acls(AclResourceType::Topic)
        .await
        .unwrap()
        .is_empty());
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
    let (low, err) = admin
        .delete_records(("rest", md0.partition), md0.offset + 1, 10_000)
        .await
        .unwrap();
    assert_eq!(err, 0);
    assert_eq!(low, md0.offset + 1);

    let cluster = admin.describe_cluster().await.unwrap();
    assert_eq!(cluster.error_code, 0);
    assert!(!cluster.brokers.is_empty());
    assert_eq!(cluster.cluster_id.as_deref(), Some("mock"));
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
    let (low, err) = admin
        .delete_records(("t", md.partition), md.offset + 1, 10_000)
        .await
        .unwrap();
    assert_eq!(err, 0);
    assert_eq!(low, md.offset + 1);
    assert_eq!(
        mock.last_delete_records_node(),
        Some(2),
        "DeleteRecords must land on the partition leader, not a follower"
    );
    assert_eq!(mock.log_len("t", md.partition), 0);

    mock.set_partition_leader("t", md.partition, 1);
    let (again, err) = admin
        .delete_records(("t", md.partition), md.offset + 1, 10_000)
        .await
        .unwrap();
    assert_eq!(err, 0);
    assert_eq!(again, md.offset + 1);
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
async fn describe_producers_follows_partition_leader() {
    let mock = common::Mock::start_two_node().await;
    let mut admin = Admin::connect(mock.addr.clone()).await.unwrap();
    let first = admin.describe_producers(("t", 0)).await.unwrap();
    assert_eq!(first.error_code, 0);
    assert_eq!(first.partition_index, 0);
    assert_eq!(first.active_producers.len(), 1);
    assert_eq!(first.active_producers[0].producer_id, 1000);
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
}

#[tokio::test]
async fn abort_transaction_follows_partition_leader() {
    let mock = common::Mock::start_two_node().await;
    let mut admin = Admin::connect(mock.addr.clone()).await.unwrap();
    admin
        .abort_transaction(AbortTransactionSpec::new(("t", 0), 1000, 0, 1))
        .await
        .unwrap();
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
}

#[tokio::test]
async fn admin_list_and_describe_topics_on_bootstrap() {
    let mock = common::Mock::start().await;
    let mut admin = Admin::connect(mock.addr.clone()).await.unwrap();
    let listed = admin.list_topics().await.unwrap();
    assert!(listed.iter().any(|t| t.name == "t" && !t.is_internal));
    assert_eq!(
        mock.last_metadata_topics(),
        Some(None),
        "list_topics must send Metadata with a null topic array"
    );
    let described = admin.describe_topics(["t"]).await.unwrap();
    assert_eq!(described.len(), 1);
    assert_eq!(described[0].partitions.len(), 1);
    assert_eq!(described[0].partitions[0].leader, 1);
    assert_eq!(
        mock.last_metadata_topics(),
        Some(Some(vec!["t".into()])),
        "describe_topics must send Metadata for the named topics"
    );
    let calls = mock.metadata_calls();
    let empty = admin.describe_topics(Vec::<&str>::new()).await.unwrap();
    assert!(empty.is_empty());
    assert_eq!(
        mock.metadata_calls(),
        calls,
        "empty describe_topics is a no-op"
    );
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
        mock.last_create_partitions_node(),
        Some(2),
        "CreatePartitions must land on the controller, not bootstrap"
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
}

#[tokio::test]
async fn update_features_follows_controller() {
    let mock = common::Mock::start_two_node().await;
    mock.set_controller(2);
    let mut admin = Admin::connect(mock.addr.clone()).await.unwrap();

    let results = admin
        .update_features(&[FeatureUpdate::new("metadata.version", 17)], 10_000)
        .await
        .unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].error_code, 0);
    assert_eq!(
        mock.last_update_features_node(),
        Some(2),
        "UpdateFeatures must land on the controller, not bootstrap"
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
        mock.last_describe_user_scram_node(),
        Some(2),
        "DescribeUserScramCredentials must land on the controller, not bootstrap"
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
}

#[tokio::test]
async fn describe_client_quotas_follows_broker() {
    let mock = common::Mock::start_two_node().await;
    mock.set_controller(2);
    let mut admin = Admin::connect(mock.addr.clone()).await.unwrap();

    let filter = ClientQuotaFilterComponent::new("user", QUOTA_MATCH_EXACT, Some("alice".into()));
    let entries = admin
        .describe_client_quotas(std::slice::from_ref(&filter), false)
        .await
        .unwrap();
    assert_eq!(entries.len(), 1);
    assert_eq!(
        entries[0].entity,
        vec![ClientQuotaEntity::new("user", Some("alice".into()))]
    );
    assert_eq!(entries[0].values.len(), 1);
    assert_eq!(entries[0].values[0].key, "producer_byte_rate");
    assert_eq!(entries[0].values[0].value, 1024.0);
    assert_eq!(
        mock.last_describe_client_quotas_node(),
        Some(1),
        "DescribeClientQuotas must land on the connected broker, not the controller"
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
}

#[tokio::test]
async fn alter_client_quotas_follows_controller() {
    let mock = common::Mock::start_two_node().await;
    mock.set_controller(2);
    let mut admin = Admin::connect(mock.addr.clone()).await.unwrap();

    let alice = ClientQuotaAlteration::new(
        vec![ClientQuotaEntity::new("user", Some("alice".into()))],
        vec![ClientQuotaOp::set("producer_byte_rate", 1024.0)],
    );
    let results = admin.alter_client_quotas(&[alice], false).await.unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].error_code, 0);
    assert_eq!(
        mock.last_alter_client_quotas_node(),
        Some(2),
        "AlterClientQuotas must land on the controller, not bootstrap"
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
    let again = admin
        .alter_client_quotas(&[bob, delete_carol], false)
        .await
        .unwrap();
    assert_eq!(again.len(), 2);
    assert_eq!(again[0].error_code, 0);
    assert_eq!(again[1].error_code, 0);
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
}

#[tokio::test]
async fn allocate_producer_ids_follows_controller() {
    let mock = common::Mock::start_two_node().await;
    mock.set_controller(2);
    let mut admin = Admin::connect(mock.addr.clone()).await.unwrap();

    let first = admin.allocate_producer_ids(7, 42).await.unwrap();
    assert_eq!(first.producer_id_start, 1000);
    assert_eq!(first.producer_id_len, 1000);
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
    assert_eq!(first[0].error_code, 0);
    assert_eq!(first[0].transactional_id, "tx-desc");
    assert_eq!(first[0].transaction_state, "Ongoing");
    assert_eq!(first[0].producer_id, 1001);
    assert_eq!(first[0].producer_epoch, 3);
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
    assert_eq!(first[0].transactional_id, "tx-list");
    assert_eq!(first[0].transaction_state, "Ongoing");
    assert_eq!(first[0].producer_id, 1001);
    assert_eq!(
        mock.last_list_transactions_node(),
        Some(2),
        "ListTransactions must land on the transaction coordinator, not bootstrap"
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
        mock.last_list_groups_node(),
        Some(1),
        "ListGroups must land on the connected broker, not the coordinator or controller"
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
    assert_eq!(first[0].resource_type, CONFIG_RESOURCE_CLIENT_METRICS);
    assert_eq!(
        mock.last_list_config_resources_node(),
        Some(1),
        "ListConfigResources must land on the connected broker, not the coordinator or controller"
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
}

#[tokio::test]
async fn get_telemetry_subscriptions_follows_broker() {
    let mock = common::Mock::start_two_node().await;
    mock.move_coordinator();
    mock.set_controller(2);
    let mut admin = Admin::connect(mock.addr.clone()).await.unwrap();

    let first = admin.get_telemetry_subscriptions([0; 16]).await.unwrap();
    assert_eq!(first.error_code, 0);
    assert_eq!(first.client_instance_id, [0x11; 16]);
    assert_eq!(first.subscription_id, 1);
    assert_eq!(first.accepted_compression_types, vec![1]);
    assert_eq!(first.push_interval_ms, 1000);
    assert_eq!(first.telemetry_max_bytes, 100);
    assert!(first.delta_temporality);
    assert_eq!(first.requested_metrics, vec!["m".to_string()]);
    assert_eq!(
        mock.last_get_telemetry_subscriptions_node(),
        Some(1),
        "GetTelemetrySubscriptions must land on the connected broker, not the coordinator or controller"
    );
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
    assert_eq!(first.directories.len(), 1);
    assert_eq!(first.directories[0].id, [0x11; 16]);
    assert_eq!(first.directories[0].topics[0].topic_id, [0x22; 16]);
    assert_eq!(
        first.directories[0].topics[0].partitions[0].partition_index,
        0
    );
    assert_eq!(first.directories[0].topics[0].partitions[0].error_code, 0);
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
}

#[tokio::test]
async fn alter_replica_log_dirs_follows_broker() {
    let mock = common::Mock::start_two_node().await;
    mock.move_coordinator();
    mock.set_controller(2);
    let mut admin = Admin::connect(mock.addr.clone()).await.unwrap();

    let dir =
        AlterReplicaLogDirsDirectory::new("/d", vec![AlterReplicaLogDirsTopic::new("t", vec![0])]);
    let first = admin
        .alter_replica_log_dirs(vec![dir.clone()])
        .await
        .unwrap();
    assert_eq!(first.results.len(), 1);
    assert_eq!(first.results[0].topic_name, "t");
    assert_eq!(first.results[0].partitions[0].partition_index, 0);
    assert_eq!(first.results[0].partitions[0].error_code, 0);
    assert_eq!(
        mock.last_alter_replica_log_dirs_node(),
        Some(1),
        "AlterReplicaLogDirs must land on the connected broker, not the coordinator or controller"
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
    assert_eq!(first.results[0].topics[0].name, "t");
    assert_eq!(first.results[0].topics[0].partitions[0].partition_index, 0);
    assert_eq!(
        mock.last_describe_log_dirs_node(),
        Some(1),
        "DescribeLogDirs must land on the connected broker, not the coordinator or controller"
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
    assert_eq!(
        described[0].1,
        ReplicaLogDirInfo::new(Some("/d".into()), 0, None, -1)
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
    assert_eq!(first.principal_type, "");
    assert_eq!(first.principal_name, "");
    assert_eq!(first.token_id, "");
    assert!(first.hmac.is_empty());
    assert_eq!(
        mock.last_create_delegation_token_node(),
        Some(1),
        "CreateDelegationToken must land on the connected broker, not the coordinator or controller"
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
    assert_eq!(first.expiry_timestamp_ms, 0);
    assert_eq!(
        mock.last_renew_delegation_token_node(),
        Some(1),
        "RenewDelegationToken must land on the connected broker, not the coordinator or controller"
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
    assert_eq!(first.expiry_timestamp_ms, 0);
    assert_eq!(
        mock.last_expire_delegation_token_node(),
        Some(1),
        "ExpireDelegationToken must land on the connected broker, not the coordinator or controller"
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
    assert!(first.tokens.is_empty());
    assert_eq!(
        mock.last_describe_delegation_token_node(),
        Some(1),
        "DescribeDelegationToken must land on the connected broker, not the coordinator or controller"
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
    assert_eq!(described[0].group_id, "g-cons");
    assert_eq!(described[0].error_code, 0);
    assert_eq!(
        mock.last_describe_groups_node(),
        Some(2),
        "describeConsumerGroups must land on the group coordinator"
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
    assert_eq!(described[0].group_id, "sg-java");
    assert_eq!(described[0].error_code, 0);
    assert_eq!(
        mock.last_share_group_describe_node(),
        Some(2),
        "describeShareGroups must land on the group coordinator"
    );
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
    assert_eq!(first[0].error_code, 0);
    assert_eq!(first[0].group_id, "sg-off");
    assert!(first[0].topics.is_empty());
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
    assert_eq!(first.error_code, 0);
    assert!(first.topics.is_empty());
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
    assert_eq!(first.error_code, 0);
    assert!(first.topics.is_empty());
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

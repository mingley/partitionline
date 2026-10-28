//! A bootstrap that accepts TCP and then stalls must not hold startup.
mod common;

use partitionline::protocol::api_keys::{
    GET_TELEMETRY_SUBSCRIPTIONS, LIST_OFFSETS, METADATA, OFFSET_FETCH,
};
use partitionline::protocol::group::{OffsetFetchGroupData, OffsetFetchRequestData};
use partitionline::{
    error, AbortTransactionSpec, AclBinding, Admin, AdminConfig, AlterConfig,
    AssignReplicasToDirsDirectory, AssignReplicasToDirsPartition, AssignReplicasToDirsTopic,
    ClientQuotaAlteration, ClientQuotaEntity, ClientQuotaOp, ConfigResource, Consumer,
    ConsumerConfig, ConsumerGroup, DescribeShareGroupOffsetsGroup, ElectLeadersOptions, Error,
    FeatureUpdate, ListConsumerGroupOffsetsSpec, MemberToRemove, NewTopic, OffsetAndMetadata,
    OffsetClient, OffsetOptions, OffsetSpec, PartitionReassignment, ProduceRecord, Producer,
    ProducerConfig, ScramMechanism, TopicPartition, TopicPartitionReplica,
    UserScramCredentialUpsertion,
};
use std::time::{Duration, Instant};

#[tokio::test]
async fn producer_metadata_reconnect_uses_new_broker_version() {
    let mock = common::Mock::start().await;
    let producer = Producer::new(
        ProducerConfig::bootstrap([mock.addr.clone()])
            .connections_max_idle(Duration::from_millis(20)),
    )
    .await
    .unwrap();
    let _partitions = producer.partitions_for("t").await.unwrap();
    assert_eq!(mock.last_metadata_version(), Some(13));
    mock.set_node_api_max(1, METADATA, 7);
    tokio::time::sleep(Duration::from_millis(40)).await;
    let _partitions = producer.partitions_for("t").await.unwrap();
    assert_eq!(mock.last_metadata_version(), Some(7));
    producer.close().await.unwrap();
}

#[tokio::test]
async fn producer_telemetry_reconnect_rejects_a_removed_api() {
    let mock = common::Mock::start().await;
    let producer = Producer::new(
        ProducerConfig::bootstrap([mock.addr.clone()])
            .connections_max_idle(Duration::from_millis(20)),
    )
    .await
    .unwrap();
    mock.hide_api(GET_TELEMETRY_SUBSCRIPTIONS);
    tokio::time::sleep(Duration::from_millis(40)).await;
    assert!(matches!(
        producer
            .client_instance_id_timeout(Duration::from_secs(2))
            .await,
        Err(Error::Unsupported(_))
    ));
    assert_eq!(mock.last_get_telemetry_subscriptions(), None);
    producer.close().await.unwrap();
}

#[expect(clippy::unwrap_used, reason = "the loopback test fixture must start")]
async fn stall_listener() -> String {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let stall = listener.local_addr().unwrap();
    drop(tokio::spawn(async move {
        loop {
            let Ok((socket, _)) = listener.accept().await else {
                break;
            };
            tokio::time::sleep(Duration::from_secs(30)).await;
            drop(socket);
        }
    }));
    stall.to_string()
}

/// Startup drops a slower bootstrap handshake before coordinator discovery returns.
#[tokio::test(flavor = "current_thread")]
async fn coordinator_startup_finishes_a_blocked_bootstrap_handshake() {
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::Arc;
    use tokio::io::AsyncReadExt;

    let mock = common::Mock::start().await;
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let slow = listener.local_addr().unwrap().to_string();
    let dropped = Arc::new(AtomicBool::new(false));
    let flag = Arc::clone(&dropped);
    let peer = tokio::spawn(async move {
        let (mut sock, _) = listener.accept().await.unwrap();
        let mut buf = [0u8; 64];
        loop {
            match sock.read(&mut buf).await {
                Ok(0) | Err(_) => break,
                Ok(_) => continue,
            }
        }
        flag.store(true, Ordering::SeqCst);
    });
    let mut cfg = ConsumerConfig::bootstrap([mock.addr.clone(), slow.clone()]);
    cfg.request_timeout = Duration::from_secs(2);
    partitionline::group::test_race_coordinator(&cfg, "coord-race")
        .await
        .unwrap();
    assert!(
        dropped.load(Ordering::SeqCst),
        "startup left the slower bootstrap handshake blocked in a broker read"
    );
    peer.abort();
}

/// Startup drops a slower bootstrap handshake before the offset race returns.
#[tokio::test(flavor = "current_thread")]
async fn offset_startup_finishes_a_blocked_bootstrap_handshake() {
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::Arc;
    use tokio::io::AsyncReadExt;

    let mock = common::Mock::start().await;
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let slow = listener.local_addr().unwrap().to_string();
    let dropped = Arc::new(AtomicBool::new(false));
    let flag = Arc::clone(&dropped);
    let peer = tokio::spawn(async move {
        let (mut sock, _) = listener.accept().await.unwrap();
        let mut buf = [0u8; 64];
        loop {
            match sock.read(&mut buf).await {
                Ok(0) | Err(_) => break,
                Ok(_) => continue,
            }
        }
        flag.store(true, Ordering::SeqCst);
    });
    let mut client =
        OffsetClient::new(AdminConfig::bootstrap([mock.addr.clone(), slow.clone()])).unwrap();
    client.test_race_bootstrap().await.unwrap();
    assert!(
        dropped.load(Ordering::SeqCst),
        "startup left the slower bootstrap handshake blocked in a broker read"
    );
    peer.abort();
}

/// A leader dial aborted at the deadline is finished before the call returns.
#[tokio::test(flavor = "current_thread")]
async fn leader_exchange_finishes_a_blocked_dial() {
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::Arc;
    use tokio::io::AsyncReadExt;

    let mock = common::Mock::start().await;
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let slow = listener.local_addr().unwrap().to_string();
    let dropped = Arc::new(AtomicBool::new(false));
    let flag = Arc::clone(&dropped);
    let peer = tokio::spawn(async move {
        let (mut sock, _) = listener.accept().await.unwrap();
        let mut buf = [0u8; 64];
        loop {
            match sock.read(&mut buf).await {
                Ok(0) | Err(_) => break,
                Ok(_) => continue,
            }
        }
        flag.store(true, Ordering::SeqCst);
    });
    let mut admin = Admin::new(AdminConfig::bootstrap([mock.addr.clone()]))
        .await
        .unwrap();
    admin.test_exchange_stall(slow).await;
    assert!(
        dropped.load(Ordering::SeqCst),
        "the deadline left the leader dial blocked in a broker read"
    );
    peer.abort();
}

/// Startup drops a slower bootstrap handshake before the admin race returns.
#[tokio::test(flavor = "current_thread")]
async fn startup_finishes_a_blocked_bootstrap_handshake() {
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::Arc;
    use tokio::io::AsyncReadExt;

    let mock = common::Mock::start().await;
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let slow = listener.local_addr().unwrap().to_string();
    let dropped = Arc::new(AtomicBool::new(false));
    let flag = Arc::clone(&dropped);
    let peer = tokio::spawn(async move {
        let (mut sock, _) = listener.accept().await.unwrap();
        let mut buf = [0u8; 64];
        loop {
            match sock.read(&mut buf).await {
                Ok(0) | Err(_) => break,
                Ok(_) => continue,
            }
        }
        flag.store(true, Ordering::SeqCst);
    });
    Admin::test_race_bootstrap(AdminConfig::bootstrap([mock.addr.clone(), slow.clone()]))
        .await
        .unwrap();
    assert!(
        dropped.load(Ordering::SeqCst),
        "startup left the slower bootstrap handshake blocked in a broker read"
    );
    peer.abort();
}

/// An idle admin socket reconnects without a second ApiVersions handshake.
#[tokio::test]
async fn admin_reuses_bootstrap_api_versions_after_idle() {
    let mock = common::Mock::start().await;
    let mut admin = Admin::new(
        AdminConfig::bootstrap([mock.addr.clone()]).connections_max_idle(Duration::from_millis(40)),
    )
    .await
    .unwrap();
    let _first = admin.describe_cluster().await.unwrap();
    tokio::time::sleep(Duration::from_millis(60)).await;
    let versions_before = mock.api_versions_versions().len();
    let accepts_before = mock.accepts();
    mock.set_api_versions_delay(Duration::from_millis(250));
    let started = Instant::now();
    let _again = admin.describe_cluster().await.unwrap();
    let elapsed = started.elapsed();
    assert_eq!(
        mock.api_versions_versions().len(),
        versions_before,
        "an idle admin socket should reuse the versions already learned"
    );
    assert_eq!(
        mock.accepts(),
        accepts_before + 1,
        "idle expiry replaces the TCP socket"
    );
    assert!(
        elapsed < Duration::from_millis(200),
        "idle admin reconnect repeated ApiVersions: {elapsed:?}"
    );
}

#[tokio::test]
async fn a_slow_bootstrap_does_not_hold_admin_startup() {
    let mock = common::Mock::start().await;
    let stall = stall_listener().await;
    let mut cfg = AdminConfig::bootstrap([mock.addr.clone()]);
    cfg.bootstrap.insert(0, stall);
    cfg.connect_timeout = Duration::from_millis(200);
    cfg.request_timeout = Duration::from_secs(2);
    let started = Instant::now();
    let mut admin = Admin::new(cfg).await.unwrap();
    let elapsed = started.elapsed();
    assert!(
        elapsed < Duration::from_millis(500),
        "admin waited for the stalled bootstrap: {elapsed:?}"
    );
    let cluster = tokio::time::timeout(Duration::from_millis(500), admin.describe_cluster())
        .await
        .unwrap()
        .unwrap();
    assert!(
        !cluster.brokers.is_empty(),
        "admin RPC after the raced handshake must return brokers, got {cluster:?}"
    );
}

#[tokio::test]
async fn a_slow_bootstrap_does_not_hold_producer_startup() {
    let mock = common::Mock::start().await;
    let stall = stall_listener().await;
    let mut cfg = ProducerConfig::bootstrap([mock.addr.clone()]);
    cfg.bootstrap.insert(0, stall);
    cfg.connect_timeout = Duration::from_millis(200);
    cfg.request_timeout = Duration::from_secs(2);
    cfg.linger = Duration::ZERO;
    let started = Instant::now();
    let producer = Producer::new(cfg).await.unwrap();
    let elapsed = started.elapsed();
    assert!(
        elapsed < Duration::from_millis(500),
        "producer waited for the stalled bootstrap: {elapsed:?}"
    );
    producer.close().await.unwrap();
}

#[tokio::test]
async fn a_slow_bootstrap_does_not_hold_transactional_producer_startup() {
    let mock = common::Mock::start().await;
    let stall = stall_listener().await;
    let mut cfg = ProducerConfig::bootstrap([mock.addr.clone()]).transactional_id("stall-txn");
    cfg.bootstrap.insert(0, stall);
    cfg.connect_timeout = Duration::from_millis(200);
    cfg.request_timeout = Duration::from_secs(2);
    cfg.linger = Duration::ZERO;
    let started = Instant::now();
    let producer = Producer::new(cfg).await.unwrap();
    let elapsed = started.elapsed();
    assert!(
        elapsed < Duration::from_millis(500),
        "transactional producer waited for the stalled bootstrap: {elapsed:?}"
    );
    producer.close().await.unwrap();
}

#[tokio::test]
async fn terminal_coordinator_errors_do_not_ask_the_next_bootstrap() {
    let mock = common::Mock::start().await;
    let other = common::Mock::start().await;
    mock.fail_find_coordinator_once(error::GROUP_AUTHORIZATION_FAILED);
    let mut cfg = ProducerConfig::bootstrap([mock.addr.clone()]).transactional_id("terminal");
    cfg.bootstrap.push(other.addr.clone());
    cfg.request_timeout = Duration::from_secs(2);
    let result = Producer::new(cfg).await;
    assert!(
        matches!(result, Err(Error::Broker { code, .. }) if code == error::GROUP_AUTHORIZATION_FAILED)
    );
    assert_eq!(mock.find_coordinator_calls(), 1);
    assert_eq!(other.find_coordinator_calls(), 0);
}

async fn past_eighth(live: &str) -> Vec<String> {
    let mut bootstrap = Vec::new();
    for _ in 0..9 {
        bootstrap.push(common::closed_tcp_addr().await);
    }
    bootstrap.push(live.to_string());
    bootstrap
}

#[tokio::test]
async fn startup_reaches_a_bootstrap_past_the_eighth() {
    let mock = common::Mock::start().await;
    let mut consumer_cfg = ConsumerConfig::bootstrap(past_eighth(&mock.addr).await);
    consumer_cfg.connect_timeout = Duration::from_millis(200);
    consumer_cfg.request_timeout = Duration::from_secs(2);
    consumer_cfg.max_wait_ms = 10;
    let consumer = Consumer::new(consumer_cfg).await.unwrap();
    drop(consumer);

    let mut admin_cfg = AdminConfig::bootstrap(past_eighth(&mock.addr).await);
    admin_cfg.connect_timeout = Duration::from_millis(200);
    admin_cfg.request_timeout = Duration::from_secs(2);
    let admin = Admin::new(admin_cfg).await.unwrap();
    drop(admin);

    let mut producer_cfg = ProducerConfig::bootstrap(past_eighth(&mock.addr).await);
    producer_cfg.connect_timeout = Duration::from_millis(200);
    producer_cfg.request_timeout = Duration::from_secs(2);
    producer_cfg.linger = Duration::ZERO;
    let producer = Producer::new(producer_cfg).await.unwrap();
    producer.close().await.unwrap();

    let mut group_cfg = ConsumerConfig::bootstrap(past_eighth(&mock.addr).await);
    group_cfg.connect_timeout = Duration::from_millis(200);
    group_cfg.request_timeout = Duration::from_secs(2);
    group_cfg.max_wait_ms = 10;
    let group = ConsumerGroup::join(group_cfg, "past-eighth", "t")
        .await
        .unwrap();
    group.close().await.unwrap();
}

#[tokio::test]
async fn list_offsets_leader_dials_overlap() {
    let mock = common::Mock::start_two_node().await;
    mock.set_topic_partitions("t", 2);
    let mut cfg = AdminConfig::bootstrap([mock.addr.clone()]);
    cfg.request_timeout = Duration::from_secs(3);
    cfg.connect_timeout = Duration::from_millis(200);
    let mut admin = Admin::new(cfg).await.unwrap();
    let before = mock.api_versions_versions().len();
    mock.set_api_versions_delay(Duration::from_millis(250));
    let started = Instant::now();
    let listed = admin
        .list_offsets_timeout(
            [
                (("t", 0), OffsetSpec::latest()),
                (("t", 1), OffsetSpec::latest()),
            ],
            Duration::from_secs(3),
        )
        .await
        .unwrap();
    let elapsed = started.elapsed();
    assert!(
        elapsed < Duration::from_millis(400),
        "leader dials ran one after another: {elapsed:?}"
    );
    assert!(
        mock.api_versions_versions().len() >= before + 2,
        "both leaders should negotiate, saw {}",
        mock.api_versions_versions().len() - before
    );
    assert_eq!(listed.len(), 2, "{listed:?}");
}

/// Admin startup keeps ApiVersions from a bootstrap that finished beside the first.
#[tokio::test]
async fn admin_startup_reuses_a_finished_bootstrap_handshake() {
    let mock = common::Mock::start_two_node().await;
    mock.set_topic_partitions("t", 1);
    mock.set_partition_leader("t", 0, 2);
    mock.set_api_versions_delay(Duration::from_millis(250));
    let addr2 = mock.broker_addr(2).unwrap();
    let mut cfg = AdminConfig::bootstrap([mock.addr.clone(), addr2]);
    cfg.request_timeout = Duration::from_secs(3);
    let started = Instant::now();
    let mut admin = Admin::new(cfg).await.unwrap();
    let startup = started.elapsed();
    let versions = mock.api_versions_versions().len();
    assert!(
        versions <= 4,
        "startup negotiated a bootstrap more than once: {versions}"
    );
    assert!(
        startup < Duration::from_millis(700),
        "startup repeated a bootstrap handshake: {startup:?}"
    );
    let started = Instant::now();
    let listed = admin
        .list_offsets([(("t", 0), OffsetSpec::latest())])
        .await
        .unwrap();
    let elapsed = started.elapsed();
    assert_eq!(listed.len(), 1, "{listed:?}");
    assert_eq!(
        mock.api_versions_versions().len(),
        versions,
        "listing offsets renegotiated a bootstrap that finished at startup"
    );
    assert!(
        elapsed < Duration::from_millis(200),
        "listing offsets repeated ApiVersions: {elapsed:?}"
    );
}

/// A replacement leader socket negotiates the broker's current API ranges.
#[tokio::test]
async fn list_offsets_renegotiates_a_remote_leader_after_idle() {
    let mock = common::Mock::start_two_node().await;
    let mut cfg =
        AdminConfig::bootstrap([mock.addr.clone()]).connections_max_idle(Duration::from_millis(40));
    cfg.request_timeout = Duration::from_secs(3);
    let mut admin = Admin::new(cfg).await.unwrap();
    let listed = admin
        .list_offsets([(("t", 0), OffsetSpec::latest())])
        .await
        .unwrap();
    assert_eq!(listed.len(), 1, "{listed:?}");
    assert_eq!(mock.api_versions_versions().len(), 2);
    tokio::time::sleep(Duration::from_millis(60)).await;
    mock.set_node_api_max(2, LIST_OFFSETS, 7);
    mock.set_api_versions_delay(Duration::from_millis(250));
    let started = Instant::now();
    let listed = admin
        .list_offsets([(("t", 0), OffsetSpec::latest())])
        .await
        .unwrap();
    let elapsed = started.elapsed();
    assert_eq!(listed.len(), 1, "{listed:?}");
    assert!(
        elapsed < Duration::from_millis(400),
        "remote leader renegotiation exceeded its budget: {elapsed:?}"
    );
    assert_eq!(mock.api_versions_versions().len(), 3);
    assert_eq!(mock.last_list_offsets_version(), Some(7));
}

/// Parallel replacement leader sockets negotiate without serializing dials.
#[tokio::test]
async fn list_offsets_renegotiates_remote_leaders_in_parallel() {
    let mock = common::Mock::start_three_node().await;
    mock.set_topic_partitions("t", 2);
    mock.set_partition_leader("t", 0, 2);
    mock.set_partition_leader("t", 1, 3);
    let mut cfg =
        AdminConfig::bootstrap([mock.addr.clone()]).connections_max_idle(Duration::from_millis(40));
    cfg.request_timeout = Duration::from_secs(3);
    let mut admin = Admin::new(cfg).await.unwrap();
    let listed = admin
        .list_offsets([
            (("t", 0), OffsetSpec::latest()),
            (("t", 1), OffsetSpec::latest()),
        ])
        .await
        .unwrap();
    assert_eq!(listed.len(), 2, "{listed:?}");
    assert_eq!(mock.api_versions_versions().len(), 3);
    tokio::time::sleep(Duration::from_millis(60)).await;
    mock.set_node_api_max(2, LIST_OFFSETS, 7);
    mock.set_node_api_max(3, LIST_OFFSETS, 7);
    mock.set_api_versions_delay(Duration::from_millis(250));
    let started = Instant::now();
    let listed = admin
        .list_offsets([
            (("t", 0), OffsetSpec::latest()),
            (("t", 1), OffsetSpec::latest()),
        ])
        .await
        .unwrap();
    let elapsed = started.elapsed();
    assert_eq!(listed.len(), 2, "{listed:?}");
    assert!(
        elapsed < Duration::from_millis(400),
        "remote leader renegotiations did not overlap: {elapsed:?}"
    );
    assert_eq!(mock.api_versions_versions().len(), 5);
    assert_eq!(mock.last_list_offsets_version(), Some(7));
}

/// A leader that has never been contacted still negotiates.
#[tokio::test]
async fn list_offsets_negotiates_a_new_leader() {
    let mock = common::Mock::start_three_node().await;
    mock.set_topic_partitions("t", 2);
    mock.set_partition_leader("t", 0, 2);
    mock.set_partition_leader("t", 1, 3);
    let mut cfg = AdminConfig::bootstrap([mock.addr.clone()]);
    cfg.request_timeout = Duration::from_secs(3);
    let mut admin = Admin::new(cfg).await.unwrap();
    let listed = admin
        .list_offsets([(("t", 0), OffsetSpec::latest())])
        .await
        .unwrap();
    assert_eq!(listed.len(), 1, "{listed:?}");
    let before = mock.api_versions_versions().len();
    assert_eq!(before, 2);
    mock.set_api_versions_delay(Duration::from_millis(250));
    let started = Instant::now();
    let listed = admin
        .list_offsets([(("t", 1), OffsetSpec::latest())])
        .await
        .unwrap();
    let elapsed = started.elapsed();
    assert_eq!(listed.len(), 1, "{listed:?}");
    assert_eq!(mock.api_versions_versions().len(), before + 1);
    assert!(
        elapsed < Duration::from_millis(450),
        "new leader redid a handshake that was already known: {elapsed:?}"
    );
}

#[expect(clippy::unwrap_used, reason = "the loopback test fixture must connect")]
async fn two_leader_admin() -> (common::Mock, Admin) {
    let mock = common::Mock::start_two_node().await;
    mock.set_topic_partitions("t", 2);
    let mut cfg = AdminConfig::bootstrap([mock.addr.clone()]);
    cfg.request_timeout = Duration::from_secs(3);
    cfg.connect_timeout = Duration::from_millis(200);
    let admin = Admin::new(cfg).await.unwrap();
    (mock, admin)
}

#[tokio::test]
async fn describe_producers_leader_dials_overlap() {
    let (mock, mut admin) = two_leader_admin().await;
    let before = mock.api_versions_versions().len();
    mock.set_api_versions_delay(Duration::from_millis(250));
    let started = Instant::now();
    let described = admin
        .describe_producers_for([("t", 0), ("t", 1)])
        .await
        .unwrap();
    let elapsed = started.elapsed();
    assert!(
        elapsed < Duration::from_millis(400),
        "leader dials ran one after another: {elapsed:?}"
    );
    assert!(mock.api_versions_versions().len() >= before + 2);
    assert!(!described.is_empty(), "{described:?}");
}

/// DescribeProducers on the bootstrap leader does not open another socket.
#[tokio::test]
async fn describe_producers_stays_on_the_bootstrap_socket() {
    let mock = common::Mock::start().await;
    let mut admin = Admin::new(AdminConfig::bootstrap([mock.addr.clone()]))
        .await
        .unwrap();
    let accepts_before = mock.accepts();
    mock.set_api_versions_delay(Duration::from_millis(250));
    let started = Instant::now();
    let described = admin.describe_producers(("t", 0)).await.unwrap();
    let elapsed = started.elapsed();
    assert_eq!(described.error_code, 0);
    assert_eq!(
        mock.accepts(),
        accepts_before,
        "DescribeProducers should stay on the admin bootstrap socket"
    );
    assert!(
        elapsed < Duration::from_millis(200),
        "describe producers opened another socket: {elapsed:?}"
    );
}

#[tokio::test]
async fn delete_records_leader_dials_overlap() {
    let (mock, mut admin) = two_leader_admin().await;
    let before = mock.api_versions_versions().len();
    mock.set_api_versions_delay(Duration::from_millis(250));
    let started = Instant::now();
    let deleted = admin
        .delete_records_for([(("t", 0), 1i64), (("t", 1), 1i64)])
        .await
        .unwrap();
    let elapsed = started.elapsed();
    assert!(
        elapsed < Duration::from_millis(400),
        "leader dials ran one after another: {elapsed:?}"
    );
    assert!(mock.api_versions_versions().len() >= before + 2);
    assert_eq!(deleted.len(), 2, "{deleted:?}");
}

/// DeleteRecords on the bootstrap leader does not open another socket.
#[tokio::test]
async fn delete_records_stays_on_the_bootstrap_socket() {
    let mock = common::Mock::start().await;
    let mut admin = Admin::new(AdminConfig::bootstrap([mock.addr.clone()]))
        .await
        .unwrap();
    let accepts_before = mock.accepts();
    mock.set_api_versions_delay(Duration::from_millis(250));
    let started = Instant::now();
    let deleted = admin.delete_records(("t", 0), 1i64, 10_000).await.unwrap();
    let elapsed = started.elapsed();
    assert_eq!(deleted.error_code(), 0);
    assert_eq!(
        mock.accepts(),
        accepts_before,
        "DeleteRecords should stay on the admin bootstrap socket"
    );
    assert!(
        elapsed < Duration::from_millis(200),
        "delete records opened another socket: {elapsed:?}"
    );
}

#[tokio::test]
async fn describe_groups_coordinator_dials_overlap() {
    let (mock, mut admin) = two_leader_admin().await;
    mock.set_group_coordinator("ga", 1);
    mock.set_group_coordinator("gb", 2);
    let before = mock.api_versions_versions().len();
    mock.set_api_versions_delay(Duration::from_millis(250));
    let started = Instant::now();
    let described = admin.describe_groups(&["ga", "gb"], false).await.unwrap();
    let elapsed = started.elapsed();
    assert!(
        elapsed < Duration::from_millis(400),
        "coordinator dials ran one after another: {elapsed:?}"
    );
    assert!(mock.api_versions_versions().len() >= before + 2);
    assert_eq!(described.len(), 2, "{described:?}");
    assert!(described.iter().all(|group| group.error_code == 0));
}

/// IncrementalAlterConfigs on the bootstrap controller does not open another socket.
#[tokio::test]
async fn incremental_alter_configs_stays_on_the_bootstrap_socket() {
    let mock = common::Mock::start().await;
    let mut admin = Admin::new(AdminConfig::bootstrap([mock.addr.clone()]))
        .await
        .unwrap();
    let created = admin
        .create_topics(&[NewTopic::new("cfg-t", 1, 1)], 10_000, false)
        .await
        .unwrap();
    assert_eq!(created[0].error_code, 0);
    let accepts_before = mock.accepts();
    mock.set_api_versions_delay(Duration::from_millis(250));
    let started = Instant::now();
    let err = admin
        .incremental_alter_configs(
            &ConfigResource::topic("cfg-t"),
            &[AlterConfig::set("retention.ms", "1000")],
            false,
        )
        .await
        .unwrap();
    let elapsed = started.elapsed();
    assert_eq!(err, 0);
    assert_eq!(
        mock.accepts(),
        accepts_before,
        "config changes should stay on the admin bootstrap socket"
    );
    assert!(
        elapsed < Duration::from_millis(200),
        "incremental alter opened another socket: {elapsed:?}"
    );
}

/// CreateAcls on the bootstrap controller does not open another socket.
#[tokio::test]
async fn create_acls_stays_on_the_bootstrap_socket() {
    let mock = common::Mock::start().await;
    let mut admin = Admin::new(AdminConfig::bootstrap([mock.addr.clone()]))
        .await
        .unwrap();
    let accepts_before = mock.accepts();
    mock.set_api_versions_delay(Duration::from_millis(250));
    let started = Instant::now();
    let created = admin
        .create_acls(&[AclBinding::allow_topic("boot-acl", "User:alice")])
        .await
        .unwrap();
    let elapsed = started.elapsed();
    assert_eq!(created.len(), 1);
    assert_eq!(created[0].error_code, 0);
    assert_eq!(
        mock.accepts(),
        accepts_before,
        "CreateAcls should stay on the admin bootstrap socket"
    );
    assert!(
        elapsed < Duration::from_millis(200),
        "create acls opened another socket: {elapsed:?}"
    );
}

/// ListPartitionReassignments on the bootstrap controller does not open another socket.
#[tokio::test]
async fn list_partition_reassignments_stays_on_the_bootstrap_socket() {
    let mock = common::Mock::start().await;
    let mut admin = Admin::new(AdminConfig::bootstrap([mock.addr.clone()]))
        .await
        .unwrap();
    let accepts_before = mock.accepts();
    mock.set_api_versions_delay(Duration::from_millis(250));
    let started = Instant::now();
    let listed = admin
        .list_partition_reassignments(None, 10_000)
        .await
        .unwrap();
    let elapsed = started.elapsed();
    assert!(listed.is_empty(), "{listed:?}");
    assert_eq!(
        mock.accepts(),
        accepts_before,
        "ListPartitionReassignments should stay on the admin bootstrap socket"
    );
    assert!(
        elapsed < Duration::from_millis(200),
        "list partition reassignments opened another socket: {elapsed:?}"
    );
}

/// UpdateFeatures on the bootstrap controller does not open another socket.
#[tokio::test]
async fn update_features_stays_on_the_bootstrap_socket() {
    let mock = common::Mock::start().await;
    let mut admin = Admin::new(AdminConfig::bootstrap([mock.addr.clone()]))
        .await
        .unwrap();
    let accepts_before = mock.accepts();
    mock.set_api_versions_delay(Duration::from_millis(250));
    let started = Instant::now();
    let updated = admin
        .update_features(&[FeatureUpdate::new("metadata.version", 17)], 10_000)
        .await
        .unwrap();
    let elapsed = started.elapsed();
    assert_eq!(updated.len(), 1);
    assert_eq!(updated[0].error_code, 0);
    assert_eq!(
        mock.accepts(),
        accepts_before,
        "UpdateFeatures should stay on the admin bootstrap socket"
    );
    assert!(
        elapsed < Duration::from_millis(200),
        "update features opened another socket: {elapsed:?}"
    );
}

/// AlterUserScramCredentials on the bootstrap controller does not open another socket.
#[tokio::test]
async fn alter_user_scram_stays_on_the_bootstrap_socket() {
    let mock = common::Mock::start().await;
    let mut admin = Admin::new(AdminConfig::bootstrap([mock.addr.clone()]))
        .await
        .unwrap();
    let accepts_before = mock.accepts();
    mock.set_api_versions_delay(Duration::from_millis(250));
    let started = Instant::now();
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
    let elapsed = started.elapsed();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].error_code, 0);
    assert_eq!(
        mock.accepts(),
        accepts_before,
        "AlterUserScramCredentials should stay on the admin bootstrap socket"
    );
    assert!(
        elapsed < Duration::from_millis(200),
        "alter user scram opened another socket: {elapsed:?}"
    );
}

/// DescribeUserScramCredentials on the bootstrap controller does not open another socket.
#[tokio::test]
async fn describe_user_scram_stays_on_the_bootstrap_socket() {
    let mock = common::Mock::start().await;
    let mut admin = Admin::new(AdminConfig::bootstrap([mock.addr.clone()]))
        .await
        .unwrap();
    let accepts_before = mock.accepts();
    mock.set_api_versions_delay(Duration::from_millis(250));
    let started = Instant::now();
    let described = admin.describe_user_scram_credentials(&[]).await.unwrap();
    let elapsed = started.elapsed();
    assert!(described.is_empty(), "{described:?}");
    assert_eq!(
        mock.accepts(),
        accepts_before,
        "DescribeUserScramCredentials should stay on the admin bootstrap socket"
    );
    assert!(
        elapsed < Duration::from_millis(200),
        "describe user scram opened another socket: {elapsed:?}"
    );
}

/// UnregisterBroker on the bootstrap controller does not open another socket.
#[tokio::test]
async fn unregister_broker_stays_on_the_bootstrap_socket() {
    let mock = common::Mock::start().await;
    let mut admin = Admin::new(AdminConfig::bootstrap([mock.addr.clone()]))
        .await
        .unwrap();
    let accepts_before = mock.accepts();
    mock.set_api_versions_delay(Duration::from_millis(250));
    let started = Instant::now();
    admin.unregister_broker(3).await.unwrap();
    let elapsed = started.elapsed();
    assert_eq!(
        mock.accepts(),
        accepts_before,
        "UnregisterBroker should stay on the admin bootstrap socket"
    );
    assert!(
        elapsed < Duration::from_millis(200),
        "unregister broker opened another socket: {elapsed:?}"
    );
}

/// AlterClientQuotas on the bootstrap controller does not open another socket.
#[tokio::test]
async fn alter_client_quotas_stays_on_the_bootstrap_socket() {
    let mock = common::Mock::start().await;
    let mut admin = Admin::new(AdminConfig::bootstrap([mock.addr.clone()]))
        .await
        .unwrap();
    let accepts_before = mock.accepts();
    mock.set_api_versions_delay(Duration::from_millis(250));
    let started = Instant::now();
    let alice = ClientQuotaAlteration::new(
        vec![ClientQuotaEntity::new(
            ClientQuotaEntity::USER,
            Some("alice".into()),
        )],
        vec![ClientQuotaOp::set("producer_byte_rate", 1024.0)],
    );
    let results = admin.alter_client_quotas(&[alice], false).await.unwrap();
    let elapsed = started.elapsed();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].error_code(), 0);
    assert_eq!(
        mock.accepts(),
        accepts_before,
        "AlterClientQuotas should stay on the admin bootstrap socket"
    );
    assert!(
        elapsed < Duration::from_millis(200),
        "alter client quotas opened another socket: {elapsed:?}"
    );
}

/// AllocateProducerIds on the bootstrap controller does not open another socket.
#[tokio::test]
async fn allocate_producer_ids_stays_on_the_bootstrap_socket() {
    let mock = common::Mock::start().await;
    let mut admin = Admin::new(AdminConfig::bootstrap([mock.addr.clone()]))
        .await
        .unwrap();
    let accepts_before = mock.accepts();
    mock.set_api_versions_delay(Duration::from_millis(250));
    let started = Instant::now();
    let block = admin.allocate_producer_ids(7, 42).await.unwrap();
    let elapsed = started.elapsed();
    assert_eq!(block.producer_id_start, 1000);
    assert_eq!(block.producer_id_len, 1000);
    assert_eq!(
        mock.accepts(),
        accepts_before,
        "AllocateProducerIds should stay on the admin bootstrap socket"
    );
    assert!(
        elapsed < Duration::from_millis(200),
        "allocate producer ids opened another socket: {elapsed:?}"
    );
}

/// InitProducerId on the bootstrap transaction coordinator does not open another socket.
#[tokio::test]
async fn fence_producers_stays_on_the_bootstrap_socket() {
    let mock = common::Mock::start().await;
    let mut admin = Admin::new(AdminConfig::bootstrap([mock.addr.clone()]))
        .await
        .unwrap();
    let accepts_before = mock.accepts();
    mock.set_api_versions_delay(Duration::from_millis(250));
    let started = Instant::now();
    let fenced = admin.fence_producers(["tid-boot"]).await.unwrap();
    let elapsed = started.elapsed();
    assert_eq!(fenced.len(), 1);
    assert_eq!(fenced[0].producer_id, 1000);
    assert_eq!(
        mock.accepts(),
        accepts_before,
        "InitProducerId should stay on the admin bootstrap socket"
    );
    assert!(
        elapsed < Duration::from_millis(200),
        "fence producers opened another socket: {elapsed:?}"
    );
}

/// WriteTxnMarkers on the bootstrap leader does not open another socket.
#[tokio::test]
async fn abort_transaction_stays_on_the_bootstrap_socket() {
    let mock = common::Mock::start().await;
    let mut admin = Admin::new(AdminConfig::bootstrap([mock.addr.clone()]))
        .await
        .unwrap();
    let accepts_before = mock.accepts();
    mock.set_api_versions_delay(Duration::from_millis(250));
    let started = Instant::now();
    admin
        .abort_transaction(AbortTransactionSpec::new(("t", 0), 1000, 0, 1))
        .await
        .unwrap();
    let elapsed = started.elapsed();
    assert_eq!(
        mock.accepts(),
        accepts_before,
        "WriteTxnMarkers should stay on the admin bootstrap socket"
    );
    assert!(
        elapsed < Duration::from_millis(200),
        "abort transaction opened another socket: {elapsed:?}"
    );
}

/// LeaveGroup on the bootstrap group coordinator does not open another socket.
#[tokio::test]
async fn remove_members_stays_on_the_bootstrap_socket() {
    let mock = common::Mock::start().await;
    let group = ConsumerGroup::join(
        ConsumerConfig::bootstrap([mock.addr.clone()])
            .max_wait_ms(10)
            .group_instance_id("worker-boot"),
        "g-boot",
        "t",
    )
    .await
    .unwrap();
    tokio::time::timeout(Duration::from_secs(2), async {
        while mock.membership_heartbeats_on(1) == 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("group heartbeat must connect before counting Admin sockets");
    let mut admin = Admin::new(AdminConfig::bootstrap([mock.addr.clone()]))
        .await
        .unwrap();
    let accepts_before = mock.accepts();
    mock.set_api_versions_delay(Duration::from_millis(250));
    let started = Instant::now();
    let removed = admin
        .remove_members_from_consumer_group("g-boot", [MemberToRemove::new("worker-boot")])
        .await
        .unwrap();
    let elapsed = started.elapsed();
    assert_eq!(removed.len(), 1);
    assert_eq!(removed[0].error_code, 0);
    assert_eq!(
        mock.accepts(),
        accepts_before,
        "LeaveGroup should stay on the admin bootstrap socket"
    );
    assert!(
        elapsed < Duration::from_millis(200),
        "remove members opened another socket: {elapsed:?}"
    );
    group.close().await.unwrap();
}

/// AlterShareGroupOffsets on the bootstrap coordinator does not open another socket.
#[tokio::test]
async fn alter_share_group_offsets_stays_on_the_bootstrap_socket() {
    let mock = common::Mock::start().await;
    let mut admin = Admin::new(AdminConfig::bootstrap([mock.addr.clone()]))
        .await
        .unwrap();
    let accepts_before = mock.accepts();
    mock.set_api_versions_delay(Duration::from_millis(250));
    let started = Instant::now();
    let altered = admin
        .alter_share_group_offsets("sg-boot", &[])
        .await
        .unwrap();
    let elapsed = started.elapsed();
    assert_eq!(altered.error_code(), 0);
    assert_eq!(
        mock.accepts(),
        accepts_before,
        "AlterShareGroupOffsets should stay on the admin bootstrap socket"
    );
    assert!(
        elapsed < Duration::from_millis(200),
        "alter share group offsets opened another socket: {elapsed:?}"
    );
}

/// DeleteShareGroupOffsets on the bootstrap coordinator does not open another socket.
#[tokio::test]
async fn delete_share_group_offsets_stays_on_the_bootstrap_socket() {
    let mock = common::Mock::start().await;
    let mut admin = Admin::new(AdminConfig::bootstrap([mock.addr.clone()]))
        .await
        .unwrap();
    let accepts_before = mock.accepts();
    mock.set_api_versions_delay(Duration::from_millis(250));
    let started = Instant::now();
    let deleted = admin
        .delete_share_group_offsets("sg-boot", &[])
        .await
        .unwrap();
    let elapsed = started.elapsed();
    assert_eq!(deleted.error_code(), 0);
    assert_eq!(
        mock.accepts(),
        accepts_before,
        "DeleteShareGroupOffsets should stay on the admin bootstrap socket"
    );
    assert!(
        elapsed < Duration::from_millis(200),
        "delete share group offsets opened another socket: {elapsed:?}"
    );
}

/// AssignReplicasToDirs on the bootstrap controller does not open another socket.
#[tokio::test]
async fn assign_replicas_to_dirs_stays_on_the_bootstrap_socket() {
    let mock = common::Mock::start().await;
    let mut admin = Admin::new(AdminConfig::bootstrap([mock.addr.clone()]))
        .await
        .unwrap();
    let accepts_before = mock.accepts();
    mock.set_api_versions_delay(Duration::from_millis(250));
    let started = Instant::now();
    let dir = AssignReplicasToDirsDirectory::new(
        [0x11; 16],
        vec![AssignReplicasToDirsTopic::new(
            [0x22; 16],
            vec![AssignReplicasToDirsPartition::new(0)],
        )],
    );
    let assigned = admin
        .assign_replicas_to_dirs(7, -1, vec![dir])
        .await
        .unwrap();
    let elapsed = started.elapsed();
    assert_eq!(assigned.error_code(), 0);
    assert_eq!(
        mock.accepts(),
        accepts_before,
        "AssignReplicasToDirs should stay on the admin bootstrap socket"
    );
    assert!(
        elapsed < Duration::from_millis(200),
        "assign replicas to dirs opened another socket: {elapsed:?}"
    );
}

/// AlterReplicaLogDirs for the bootstrap broker does not open another socket.
#[tokio::test]
async fn alter_replica_log_dirs_for_stays_on_the_bootstrap_socket() {
    let mock = common::Mock::start().await;
    let mut admin = Admin::new(AdminConfig::bootstrap([mock.addr.clone()]))
        .await
        .unwrap();
    let accepts_before = mock.accepts();
    mock.set_api_versions_delay(Duration::from_millis(250));
    let started = Instant::now();
    let altered = admin
        .alter_replica_log_dirs_for([(TopicPartitionReplica::new("t", 0, 1), "/d")])
        .await
        .unwrap();
    let elapsed = started.elapsed();
    assert_eq!(altered.len(), 1);
    assert_eq!(altered[0].1, 0);
    assert_eq!(
        mock.accepts(),
        accepts_before,
        "AlterReplicaLogDirs should stay on the admin bootstrap socket"
    );
    assert!(
        elapsed < Duration::from_millis(200),
        "alter replica log dirs opened another socket: {elapsed:?}"
    );
}

/// ElectLeaders on the bootstrap controller does not open another socket.
#[tokio::test]
async fn elect_leaders_stays_on_the_bootstrap_socket() {
    let mock = common::Mock::start().await;
    let mut admin = Admin::new(AdminConfig::bootstrap([mock.addr.clone()]))
        .await
        .unwrap();
    let accepts_before = mock.accepts();
    mock.set_api_versions_delay(Duration::from_millis(250));
    let started = Instant::now();
    let elected = admin
        .elect_leaders(ElectLeadersOptions::default())
        .await
        .unwrap();
    let elapsed = started.elapsed();
    assert!(!elected.results.is_empty(), "{elected:?}");
    assert_eq!(
        mock.accepts(),
        accepts_before,
        "ElectLeaders should stay on the admin bootstrap socket"
    );
    assert!(
        elapsed < Duration::from_millis(200),
        "elect leaders opened another socket: {elapsed:?}"
    );
}

/// AlterPartitionReassignments on the bootstrap controller does not open another socket.
#[tokio::test]
async fn alter_partition_reassignments_stays_on_the_bootstrap_socket() {
    let mock = common::Mock::start().await;
    let mut admin = Admin::new(AdminConfig::bootstrap([mock.addr.clone()]))
        .await
        .unwrap();
    let accepts_before = mock.accepts();
    mock.set_api_versions_delay(Duration::from_millis(250));
    let started = Instant::now();
    let assigned = admin
        .alter_partition_reassignments(
            &[PartitionReassignment::assign(
                TopicPartition::new("t", 0),
                [1],
            )],
            10_000,
        )
        .await
        .unwrap();
    let elapsed = started.elapsed();
    assert_eq!(assigned.len(), 1);
    assert_eq!(assigned[0].error_code, 0);
    assert_eq!(
        mock.accepts(),
        accepts_before,
        "AlterPartitionReassignments should stay on the admin bootstrap socket"
    );
    assert!(
        elapsed < Duration::from_millis(200),
        "alter partition reassignments opened another socket: {elapsed:?}"
    );
}

/// ListOffsets on the bootstrap leader does not open another socket.
#[tokio::test]
async fn list_offsets_stays_on_the_bootstrap_socket() {
    let mock = common::Mock::start().await;
    let mut admin = Admin::new(AdminConfig::bootstrap([mock.addr.clone()]))
        .await
        .unwrap();
    let accepts_before = mock.accepts();
    mock.set_api_versions_delay(Duration::from_millis(250));
    let started = Instant::now();
    let listed = admin
        .list_offsets([(TopicPartition::new("t", 0), -1i64)])
        .await
        .unwrap();
    let elapsed = started.elapsed();
    assert_eq!(listed.len(), 1);
    assert_eq!(
        mock.accepts(),
        accepts_before,
        "ListOffsets should stay on the admin bootstrap socket"
    );
    assert!(
        elapsed < Duration::from_millis(200),
        "list offsets opened another socket: {elapsed:?}"
    );
}

/// OffsetCommit and OffsetFetch on the bootstrap coordinator do not open another socket.
#[tokio::test]
async fn consumer_group_offsets_stay_on_the_bootstrap_socket() {
    let mock = common::Mock::start().await;
    let mut admin = Admin::new(AdminConfig::bootstrap([mock.addr.clone()]))
        .await
        .unwrap();
    let accepts_before = mock.accepts();
    mock.set_api_versions_delay(Duration::from_millis(250));
    let started = Instant::now();
    admin
        .alter_consumer_group_offsets(
            "g-off",
            [(
                TopicPartition::new("t", 0),
                OffsetAndMetadata::with_metadata(3, "admin").with_leader_epoch(0),
            )],
        )
        .await
        .unwrap();
    let listed = admin
        .list_consumer_group_offsets("g-off", [TopicPartition::new("t", 0)])
        .await
        .unwrap();
    let elapsed = started.elapsed();
    assert_eq!(listed.len(), 1);
    assert_eq!(listed[0].1.offset, 3);
    assert_eq!(
        mock.accepts(),
        accepts_before,
        "offset commit and fetch should stay on the admin bootstrap socket"
    );
    assert!(
        elapsed < Duration::from_millis(200),
        "consumer group offsets opened another socket: {elapsed:?}"
    );
}

/// OffsetDelete on the bootstrap coordinator does not open another socket.
#[tokio::test]
async fn delete_consumer_group_offsets_stays_on_the_bootstrap_socket() {
    let mock = common::Mock::start().await;
    let mut admin = Admin::new(AdminConfig::bootstrap([mock.addr.clone()]))
        .await
        .unwrap();
    let accepts_before = mock.accepts();
    mock.set_api_versions_delay(Duration::from_millis(250));
    let started = Instant::now();
    let deleted = admin
        .delete_consumer_group_offsets("g-off", [TopicPartition::new("t", 0)])
        .await
        .unwrap();
    let elapsed = started.elapsed();
    assert_eq!(deleted.len(), 1);
    assert_eq!(deleted[0].error_code, 0);
    assert_eq!(
        mock.accepts(),
        accepts_before,
        "OffsetDelete should stay on the admin bootstrap socket"
    );
    assert!(
        elapsed < Duration::from_millis(200),
        "delete offsets opened another socket: {elapsed:?}"
    );
}

/// DescribeLogDirs for the bootstrap broker does not open another socket.
#[tokio::test]
async fn describe_replica_log_dirs_stays_on_the_bootstrap_socket() {
    let mock = common::Mock::start().await;
    let mut admin = Admin::new(AdminConfig::bootstrap([mock.addr.clone()]))
        .await
        .unwrap();
    let accepts_before = mock.accepts();
    mock.set_api_versions_delay(Duration::from_millis(250));
    let started = Instant::now();
    let described = admin
        .describe_replica_log_dirs([TopicPartitionReplica::new("t", 0, 1)])
        .await
        .unwrap();
    let elapsed = started.elapsed();
    assert_eq!(described.len(), 1);
    assert_eq!(
        mock.accepts(),
        accepts_before,
        "DescribeLogDirs should stay on the admin bootstrap socket"
    );
    assert!(
        elapsed < Duration::from_millis(200),
        "describe replica log dirs opened another socket: {elapsed:?}"
    );
}

/// A later controller call to the bootstrap broker does not repeat ApiVersions.
#[tokio::test]
async fn delete_topics_skips_a_second_handshake_on_the_bootstrap_broker() {
    let mock = common::Mock::start().await;
    let mut admin = Admin::new(AdminConfig::bootstrap([mock.addr.clone()]))
        .await
        .unwrap();
    let created = admin
        .create_topics(&[NewTopic::new("gone", 1, 1)], 10_000, false)
        .await
        .unwrap();
    assert_eq!(created[0].error_code, 0);
    mock.set_api_versions_delay(Duration::from_millis(250));
    let versions_before = mock.api_versions_versions().len();
    let accepts_before = mock.accepts();
    let started = Instant::now();
    let deleted = admin.delete_topics(&["gone"], 10_000).await.unwrap();
    let elapsed = started.elapsed();
    assert_eq!(deleted.len(), 1);
    assert_eq!(deleted[0].error_code, 0);
    assert_eq!(
        mock.api_versions_versions().len(),
        versions_before,
        "deleting on the bootstrap broker should reuse the versions already learned"
    );
    assert_eq!(
        mock.accepts(),
        accepts_before,
        "DeleteTopics should stay on the admin bootstrap socket"
    );
    assert!(
        elapsed < Duration::from_millis(200),
        "delete topics repeated ApiVersions: {elapsed:?}"
    );
}

/// DescribeGroups on the bootstrap coordinator does not open another socket.
#[tokio::test]
async fn describe_groups_stays_on_the_bootstrap_socket() {
    let mock = common::Mock::start().await;
    let mut admin = Admin::new(AdminConfig::bootstrap([mock.addr.clone()]))
        .await
        .unwrap();
    let accepts_before = mock.accepts();
    mock.set_api_versions_delay(Duration::from_millis(250));
    let started = Instant::now();
    let described = admin.describe_groups(&["g"], false).await.unwrap();
    let elapsed = started.elapsed();
    assert_eq!(described.len(), 1);
    assert_eq!(described[0].error_code, 0);
    assert_eq!(
        mock.accepts(),
        accepts_before,
        "DescribeGroups should stay on the admin bootstrap socket"
    );
    assert!(
        elapsed < Duration::from_millis(200),
        "describe groups opened another socket: {elapsed:?}"
    );
}

/// CreateTopics on the bootstrap broker reuses that socket.
#[tokio::test]
async fn create_topics_reuses_the_bootstrap_socket() {
    let mock = common::Mock::start().await;
    let mut admin = Admin::new(AdminConfig::bootstrap([mock.addr.clone()]))
        .await
        .unwrap();
    let delay = Duration::from_millis(250);
    mock.set_metadata_delay(delay);
    mock.set_api_versions_delay(delay);
    let versions_before = mock.api_versions_versions().len();
    let started = Instant::now();
    let created = admin
        .create_topics(
            &[NewTopic::new("created-on-bootstrap", 1, 1)],
            10_000,
            false,
        )
        .await
        .unwrap();
    let elapsed = started.elapsed();
    assert_eq!(created.len(), 1);
    assert_eq!(created[0].error_code, 0);
    assert_eq!(
        mock.api_versions_versions().len(),
        versions_before,
        "the controller RPC should stay on the negotiated bootstrap socket"
    );
    assert!(
        elapsed < Duration::from_millis(450),
        "create topics opened a second socket: {elapsed:?}"
    );
}

/// The first coordinator lookup uses the host from FindCoordinator.
#[tokio::test]
async fn describe_groups_uses_find_coordinator_address() {
    let mock = common::Mock::start_two_node().await;
    mock.set_group_coordinator("g-addr", 2);
    let mut admin = Admin::new(AdminConfig::bootstrap([mock.addr.clone()]))
        .await
        .unwrap();
    let delay = Duration::from_millis(250);
    mock.set_metadata_delay(delay);
    mock.set_find_coordinator_delay(delay);
    mock.set_api_versions_delay(delay);
    let metadata_before = mock.metadata_calls();
    let versions_before = mock.api_versions_versions().len();
    let started = Instant::now();
    let described = admin.describe_groups(&["g-addr"], false).await.unwrap();
    let elapsed = started.elapsed();
    assert_eq!(described.len(), 1);
    assert_eq!(described[0].error_code, 0);
    assert_eq!(
        mock.metadata_calls(),
        metadata_before,
        "FindCoordinator already returned the coordinator address"
    );
    assert!(
        mock.api_versions_versions().len() > versions_before,
        "the coordinator socket should still be negotiated"
    );
    assert!(
        elapsed < Duration::from_millis(650),
        "coordinator lookup loaded the broker list first: {elapsed:?}"
    );
}

#[tokio::test]
async fn consumer_group_describe_coordinator_dials_overlap() {
    let (mock, mut admin) = two_leader_admin().await;
    mock.set_group_coordinator("ga", 1);
    mock.set_group_coordinator("gb", 2);
    let before = mock.api_versions_versions().len();
    mock.set_api_versions_delay(Duration::from_millis(250));
    let started = Instant::now();
    let described = admin
        .consumer_group_describe(&["ga", "gb"], false)
        .await
        .unwrap();
    let elapsed = started.elapsed();
    assert!(
        elapsed < Duration::from_millis(400),
        "coordinator dials ran one after another: {elapsed:?}"
    );
    assert!(mock.api_versions_versions().len() >= before + 2);
    assert_eq!(described.len(), 2, "{described:?}");
    assert!(described.iter().all(|group| group.error_code == 0));
}

#[tokio::test]
async fn delete_groups_coordinator_dials_overlap() {
    let (mock, mut admin) = two_leader_admin().await;
    mock.set_group_coordinator("ga", 1);
    mock.set_group_coordinator("gb", 2);
    let before = mock.api_versions_versions().len();
    mock.set_api_versions_delay(Duration::from_millis(250));
    let started = Instant::now();
    let deleted = admin.delete_groups(&["ga", "gb"]).await.unwrap();
    let elapsed = started.elapsed();
    assert!(
        elapsed < Duration::from_millis(400),
        "coordinator dials ran one after another: {elapsed:?}"
    );
    assert!(mock.api_versions_versions().len() >= before + 2);
    assert_eq!(deleted.len(), 2, "{deleted:?}");
    assert!(deleted.iter().all(|group| group.error_code == 0));
}

#[tokio::test]
async fn share_group_describe_coordinator_dials_overlap() {
    let (mock, mut admin) = two_leader_admin().await;
    mock.set_group_coordinator("ga", 1);
    mock.set_group_coordinator("gb", 2);
    let before = mock.api_versions_versions().len();
    mock.set_api_versions_delay(Duration::from_millis(250));
    let started = Instant::now();
    let described = admin
        .share_group_describe(&["ga", "gb"], false)
        .await
        .unwrap();
    let elapsed = started.elapsed();
    assert!(
        elapsed < Duration::from_millis(400),
        "coordinator dials ran one after another: {elapsed:?}"
    );
    assert!(mock.api_versions_versions().len() >= before + 2);
    assert_eq!(described.len(), 2, "{described:?}");
    assert!(described.iter().all(|group| group.error_code == 0));
}

#[tokio::test]
async fn describe_share_group_offsets_coordinator_dials_overlap() {
    let (mock, mut admin) = two_leader_admin().await;
    mock.set_group_coordinator("ga", 1);
    mock.set_group_coordinator("gb", 2);
    let before = mock.api_versions_versions().len();
    mock.set_api_versions_delay(Duration::from_millis(250));
    let started = Instant::now();
    let described = admin
        .describe_share_group_offsets(&[
            DescribeShareGroupOffsetsGroup::new("ga"),
            DescribeShareGroupOffsetsGroup::new("gb"),
        ])
        .await
        .unwrap();
    let elapsed = started.elapsed();
    assert!(
        elapsed < Duration::from_millis(400),
        "coordinator dials ran one after another: {elapsed:?}"
    );
    assert!(mock.api_versions_versions().len() >= before + 2);
    assert_eq!(described.len(), 2, "{described:?}");
    assert!(described.iter().all(|group| group.error_code == 0));
}

#[tokio::test]
async fn describe_transactions_coordinator_dials_overlap() {
    let (mock, mut admin) = two_leader_admin().await;
    mock.set_txn_id_coordinator("tx-a", 1);
    mock.set_txn_id_coordinator("tx-b", 2);
    let before = mock.api_versions_versions().len();
    mock.set_api_versions_delay(Duration::from_millis(250));
    let started = Instant::now();
    let described = admin
        .describe_transactions(&["tx-a", "tx-b"])
        .await
        .unwrap();
    let elapsed = started.elapsed();
    assert!(
        elapsed < Duration::from_millis(400),
        "coordinator dials ran one after another: {elapsed:?}"
    );
    assert!(mock.api_versions_versions().len() >= before + 2);
    assert_eq!(described.len(), 2, "{described:?}");
}

#[tokio::test]
async fn list_group_offsets_coordinator_dials_overlap() {
    let mock = common::Mock::start_two_node().await;
    mock.set_api_max(OFFSET_FETCH, 9);
    mock.set_group_coordinator("ga", 1);
    mock.set_group_coordinator("gb", 2);
    let mut cfg = AdminConfig::bootstrap([mock.addr.clone()]);
    cfg.request_timeout = Duration::from_secs(3);
    cfg.connect_timeout = Duration::from_millis(200);
    let mut admin = Admin::new(cfg).await.unwrap();
    let before = mock.api_versions_versions().len();
    mock.set_api_versions_delay(Duration::from_millis(250));
    let started = Instant::now();
    let listed = admin
        .list_consumer_group_offsets_for_groups([
            ("ga", ListConsumerGroupOffsetsSpec::all()),
            ("gb", ListConsumerGroupOffsetsSpec::all()),
        ])
        .await
        .unwrap();
    let elapsed = started.elapsed();
    assert!(
        elapsed < Duration::from_millis(400),
        "coordinator dials ran one after another: {elapsed:?}"
    );
    assert!(mock.api_versions_versions().len() >= before + 2);
    assert_eq!(listed.len(), 2, "{listed:?}");
}

/// OffsetFetch on the bootstrap coordinator keeps that socket.
#[tokio::test]
async fn offset_fetch_reuses_the_bootstrap_coordinator_socket() {
    let mock = common::Mock::start().await;
    mock.set_group_coordinator("g", 1);
    let mut cfg = AdminConfig::bootstrap([mock.addr.clone()]);
    cfg.request_timeout = Duration::from_secs(3);
    let mut client = OffsetClient::new(cfg).unwrap();
    let delay = Duration::from_millis(250);
    mock.set_api_versions_delay(delay);
    mock.set_find_coordinator_delay(delay);
    let started = Instant::now();
    let fetched = client
        .fetch(
            &OffsetFetchRequestData {
                groups: vec![OffsetFetchGroupData {
                    group_id: "g".into(),
                    member_id: None,
                    member_epoch: -1,
                    topics: None,
                }],
                require_stable: false,
            },
            &OffsetOptions {
                topic_ids: false,
                timeout: Some(Duration::from_secs(3)),
                ..OffsetOptions::default()
            },
        )
        .await
        .unwrap();
    let elapsed = started.elapsed();
    assert_eq!(fetched.groups.len(), 1);
    assert_eq!(
        mock.api_versions_versions().len(),
        1,
        "offset fetch should keep the bootstrap coordinator socket"
    );
    assert!(
        elapsed < Duration::from_millis(650),
        "offset fetch opened a second socket to the bootstrap broker: {elapsed:?}"
    );
}

/// An offset fetch that times out finishes a coordinator dial still in flight.
#[tokio::test(flavor = "current_thread")]
async fn offset_fetch_finishes_a_blocked_coordinator_dial() {
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::Arc;
    use tokio::io::AsyncReadExt;

    let mock = common::Mock::start_two_node().await;
    let mut slow_addr = String::new();
    let mut listener = None;
    for _ in 0..32 {
        let candidate = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = candidate.local_addr().unwrap().to_string();
        if addr > mock.addr {
            slow_addr = addr;
            listener = Some(candidate);
            break;
        }
    }
    let listener = listener.expect("blackhole port sorts after the mock");
    let slow = listener.local_addr().unwrap();
    mock.set_broker_address(2, "127.0.0.1", i32::from(slow.port()));
    mock.set_group_coordinator("fast", 1);
    mock.set_group_coordinator("slow", 2);
    mock.set_offset_fetch_delay(Duration::from_secs(2));
    let dropped = Arc::new(AtomicBool::new(false));
    let flag = Arc::clone(&dropped);
    let peer = tokio::spawn(async move {
        let (mut sock, _) = listener.accept().await.unwrap();
        let mut buf = [0u8; 64];
        loop {
            match sock.read(&mut buf).await {
                Ok(0) | Err(_) => break,
                Ok(_) => continue,
            }
        }
        flag.store(true, Ordering::SeqCst);
    });
    let mut cfg = AdminConfig::bootstrap([mock.addr.clone()]);
    cfg.request_timeout = Duration::from_millis(300);
    let mut client = OffsetClient::new(cfg).unwrap();
    let request = OffsetFetchRequestData {
        groups: vec![
            OffsetFetchGroupData {
                group_id: "fast".into(),
                member_id: None,
                member_epoch: -1,
                topics: None,
            },
            OffsetFetchGroupData {
                group_id: "slow".into(),
                member_id: None,
                member_epoch: -1,
                topics: None,
            },
        ],
        require_stable: false,
    };
    let mut options = offset_options();
    options.timeout = Some(Duration::from_millis(300));
    let result = client.test_fetch_once(&request, &options).await;
    assert!(result.is_err(), "the delayed offset fetch should time out");
    assert!(
        dropped.load(Ordering::SeqCst),
        "offset fetch left the coordinator dial blocked in a broker read"
    );
    peer.abort();
    let _ = slow_addr;
}

/// A consumer fetch timeout closes a leader handshake before the call returns.
#[tokio::test(flavor = "current_thread")]
async fn fetch_timeout_closes_the_leader_socket() {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;
    use tokio::io::AsyncReadExt;

    let mock = common::Mock::start_two_node().await;
    let mut cfg = ConsumerConfig::bootstrap([mock.addr.clone()]);
    cfg.request_timeout = Duration::from_millis(200);
    cfg.connect_timeout = Duration::from_millis(200);
    cfg.reconnect_backoff = Duration::ZERO;
    cfg.reconnect_backoff_max = Duration::ZERO;
    cfg.retry_backoff = Duration::ZERO;
    cfg.retry_backoff_max = Duration::ZERO;
    cfg.max_wait_ms = 10;
    let mut consumer = Consumer::new(cfg).await.unwrap();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let slow = listener.local_addr().unwrap();
    let open = Arc::new(AtomicUsize::new(0));
    let open_flag = Arc::clone(&open);
    let peer = tokio::spawn(async move {
        loop {
            let (mut sock, _) = listener.accept().await.unwrap();
            let _started = open_flag.fetch_add(1, Ordering::SeqCst);
            let open_flag = Arc::clone(&open_flag);
            let _reader = tokio::spawn(async move {
                let mut buf = [0u8; 64];
                loop {
                    match sock.read(&mut buf).await {
                        Ok(0) | Err(_) => break,
                        Ok(_) => continue,
                    }
                }
                let _left = open_flag.fetch_sub(1, Ordering::SeqCst);
            });
        }
    });
    mock.set_broker_address(2, "127.0.0.1", i32::from(slow.port()));
    mock.set_topic_partitions("u", 1);
    mock.set_partition_leader("u", 0, 2);
    consumer.assign("u", 0, 0).await.unwrap();
    let result = consumer.fetch().await;
    assert!(result.is_err(), "fetch should time out");
    assert_eq!(
        open.load(Ordering::SeqCst),
        0,
        "fetch left the leader socket blocked in a broker read"
    );
    peer.abort();
}

/// A group-coordinator lookup closes the coordinator handshake before the error returns.
#[tokio::test(flavor = "current_thread")]
async fn group_coordinator_timeout_closes_the_socket() {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;
    use tokio::io::AsyncReadExt;

    let mock = common::Mock::start_two_node().await;
    let mut cfg = ProducerConfig::bootstrap([mock.addr.clone()]);
    cfg.request_timeout = Duration::from_millis(400);
    cfg.connect_timeout = Duration::from_millis(200);
    cfg.reconnect_backoff = Duration::ZERO;
    cfg.reconnect_backoff_max = Duration::ZERO;
    cfg.retry_backoff = Duration::ZERO;
    cfg.retry_backoff_max = Duration::ZERO;
    let producer = Producer::new(cfg).await.unwrap();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let slow = listener.local_addr().unwrap();
    let open = Arc::new(AtomicUsize::new(0));
    let open_flag = Arc::clone(&open);
    let peer = tokio::spawn(async move {
        loop {
            let (mut sock, _) = listener.accept().await.unwrap();
            let _started = open_flag.fetch_add(1, Ordering::SeqCst);
            let open_flag = Arc::clone(&open_flag);
            let _reader = tokio::spawn(async move {
                let mut buf = [0u8; 64];
                loop {
                    match sock.read(&mut buf).await {
                        Ok(0) | Err(_) => break,
                        Ok(_) => continue,
                    }
                }
                let _left = open_flag.fetch_sub(1, Ordering::SeqCst);
            });
        }
    });
    mock.set_broker_address(2, "127.0.0.1", i32::from(slow.port()));
    mock.set_group_coordinator("g-stop", 2);
    let result = producer.test_discover_group_coordinator("g-stop").await;
    assert!(result.is_err(), "group coordinator lookup should time out");
    assert_eq!(
        open.load(Ordering::SeqCst),
        0,
        "group coordinator lookup left the socket blocked in a broker read"
    );
    peer.abort();
}

/// Closing an admin client closes the bootstrap socket before close returns.
#[tokio::test(flavor = "current_thread")]
async fn admin_close_closes_the_bootstrap_socket() {
    let mock = common::Mock::start().await;
    let mut cfg = AdminConfig::bootstrap([mock.addr.clone()]);
    cfg.request_timeout = Duration::from_secs(2);
    let admin = Admin::new(cfg).await.unwrap();
    assert_eq!(
        mock.open_conns(),
        1,
        "admin connect should hold the bootstrap socket"
    );
    admin.close().await.unwrap();
    assert_eq!(
        mock.open_conns(),
        0,
        "admin close left the socket blocked in a broker read"
    );
}

/// A consumer-protocol coordinator that closes during the interval is dialed again.
#[tokio::test(flavor = "current_thread")]
async fn consumer_protocol_heartbeat_reconnects_when_the_coordinator_closes() {
    let mock = common::Mock::start().await;
    mock.set_cg_heartbeat_interval_ms(40);
    let mut cfg = ConsumerConfig::bootstrap([mock.addr.clone()]);
    cfg.request_timeout = Duration::from_secs(2);
    cfg.max_wait_ms = 10;
    cfg.heartbeat_interval = Duration::from_millis(40);
    cfg.session_timeout_ms = 60_000;
    let group = ConsumerGroup::join_consumer(cfg, "g848-reconn", "t")
        .await
        .unwrap();
    let started = Instant::now();
    while mock.cg_heartbeat_calls() < 1 {
        if started.elapsed() > Duration::from_secs(2) {
            panic!("consumer-protocol heartbeat did not start");
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    mock.set_cg_heartbeat_interval_ms(30_000);
    let learned = mock.cg_heartbeat_calls();
    while mock.cg_heartbeat_calls() == learned {
        if started.elapsed() > Duration::from_secs(2) {
            panic!("consumer-protocol heartbeat did not learn the long interval");
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    tokio::time::sleep(Duration::from_millis(50)).await;
    let accepts = mock.accepts();
    mock.drop_connections();
    let started = Instant::now();
    while mock.accepts() == accepts {
        if started.elapsed() > Duration::from_millis(400) {
            panic!("consumer-protocol heartbeat did not reconnect after the coordinator closed");
        }
        tokio::task::yield_now().await;
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    group.close().await.unwrap();
}

/// A coordinator that closes during the heartbeat interval is dialed again.
#[tokio::test(flavor = "current_thread")]
async fn group_heartbeat_reconnects_when_the_coordinator_closes() {
    let mock = common::Mock::start().await;
    mock.set_cg_heartbeat_interval_ms(30_000);
    let mut cfg = ConsumerConfig::bootstrap([mock.addr.clone()]);
    cfg.request_timeout = Duration::from_secs(2);
    cfg.max_wait_ms = 10;
    cfg.heartbeat_interval = Duration::from_secs(30);
    cfg.session_timeout_ms = 60_000;
    let group = ConsumerGroup::join(cfg, "g-reconn", "t").await.unwrap();
    tokio::time::sleep(Duration::from_millis(50)).await;
    let accepts = mock.accepts();
    mock.drop_connections();
    let started = Instant::now();
    while mock.accepts() == accepts {
        if started.elapsed() > Duration::from_millis(400) {
            panic!("heartbeat did not reconnect after the coordinator closed");
        }
        tokio::task::yield_now().await;
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    group.close().await.unwrap();
}

/// Closing a consumer group closes its sockets before close returns.
#[tokio::test(flavor = "current_thread")]
async fn group_close_closes_the_member_socket() {
    let mock = common::Mock::start().await;
    let mut cfg = ConsumerConfig::bootstrap([mock.addr.clone()]);
    cfg.request_timeout = Duration::from_secs(2);
    cfg.max_wait_ms = 10;
    let group = ConsumerGroup::join(cfg, "g-close", "t").await.unwrap();
    assert!(
        mock.open_conns() >= 1,
        "group join should hold a broker socket"
    );
    group.close().await.unwrap();
    assert_eq!(
        mock.open_conns(),
        0,
        "group close left a socket blocked in a broker read"
    );
}

/// An idle group must drop every socket when the broker stops writing.
#[tokio::test(flavor = "current_thread")]
async fn idle_group_drops_sockets_when_the_broker_stops_writing() {
    let mock = common::Mock::start().await;
    mock.set_topic_partitions("t", 1);
    mock.set_cg_heartbeat_interval_ms(30_000);
    let mut cfg = ConsumerConfig::bootstrap([mock.addr.clone()]);
    cfg.request_timeout = Duration::from_secs(2);
    cfg.max_wait_ms = 10;
    cfg.session_timeout_ms = 60_000;
    cfg.heartbeat_interval = Duration::from_secs(30);
    let group = ConsumerGroup::join(cfg, "g-idle", "t").await.unwrap();
    tokio::time::sleep(Duration::from_millis(50)).await;
    let open = mock.open_conns();
    assert!(open > 0, "joined group should hold a broker socket");
    mock.shutdown_writes();
    let window = Instant::now();
    while mock.open_conns() >= open {
        if window.elapsed() > Duration::from_millis(400) {
            panic!("idle group kept every socket after the broker stopped writing; open {open}");
        }
        tokio::task::yield_now().await;
    }
    group.close().await.unwrap();
}

/// A broker that closes during linger is dialed again before that linger ends.
#[tokio::test(flavor = "current_thread")]
async fn produce_reconnects_when_the_broker_closes_during_linger() {
    let mock = common::Mock::start().await;
    mock.set_topic_partitions("t", 1);
    let mut cfg = ProducerConfig::bootstrap([mock.addr.clone()]);
    cfg.linger = Duration::from_secs(30);
    cfg.request_timeout = Duration::from_secs(2);
    cfg.delivery_timeout = Duration::from_secs(60);
    let producer = Producer::new(cfg).await.unwrap();
    let sending = producer.clone();
    let mut send = Box::pin(sending.send(ProduceRecord::to("t").value(b"x".to_vec())));
    let started = Instant::now();
    while mock.open_conns() < 2 {
        if started.elapsed() > Duration::from_secs(2) {
            panic!("produce socket did not open");
        }
        tokio::select! {
            result = &mut send => panic!("send finished before the broker closed: {result:?}"),
            _ = tokio::time::sleep(Duration::from_millis(5)) => {}
        }
    }
    let accepts = mock.accepts();
    mock.drop_connections();
    let started = Instant::now();
    while mock.accepts() == accepts {
        if started.elapsed() > Duration::from_millis(400) {
            panic!("produce did not reconnect during linger");
        }
        tokio::select! {
            result = &mut send => panic!("send finished during reconnect wait: {result:?}"),
            _ = tokio::time::sleep(Duration::from_millis(5)) => {}
        }
    }
    drop(send);
    producer.close().await.unwrap();
}

/// A flush waiting on a broker quota replaces a socket the broker has already closed.
#[tokio::test(flavor = "current_thread")]
async fn produce_reconnects_when_the_broker_closes_during_throttle() {
    let mock = common::Mock::start().await;
    mock.set_topic_partitions("t", 1);
    mock.set_produce_throttles(1, [5_000]);
    let mut cfg = ProducerConfig::bootstrap([mock.addr.clone()])
        .batch_records(1)
        .connections(1);
    cfg.linger = Duration::ZERO;
    cfg.request_timeout = Duration::from_secs(2);
    cfg.delivery_timeout = Duration::from_secs(30);
    let producer = Producer::new(cfg).await.unwrap();
    let _sent = producer
        .send(ProduceRecord::to("t").value(b"first".to_vec()))
        .await
        .unwrap();
    let queued = producer.metrics().records_queued;
    let sending = producer.clone();
    let mut send = Box::pin(sending.send(ProduceRecord::to("t").value(b"second".to_vec())));
    let started = Instant::now();
    while producer.metrics().records_queued <= queued {
        if started.elapsed() > Duration::from_secs(2) {
            panic!("second record was not queued");
        }
        tokio::select! {
            result = &mut send => panic!("send finished before the broker closed: {result:?}"),
            _ = tokio::time::sleep(Duration::from_millis(5)) => {}
        }
    }
    let flushing = producer.clone();
    let flush = tokio::spawn(async move { flushing.flush().await });
    tokio::time::sleep(Duration::from_millis(50)).await;
    let accepts = mock.accepts();
    mock.drop_connections();
    let started = Instant::now();
    while mock.accepts() == accepts {
        if started.elapsed() > Duration::from_millis(400) {
            panic!("produce did not reconnect during throttle");
        }
        tokio::select! {
            result = &mut send => panic!("send finished during reconnect wait: {result:?}"),
            _ = tokio::time::sleep(Duration::from_millis(5)) => {}
        }
    }
    flush.abort();
    drop(send);
    drop(producer);
}

/// CreateTopics retries a NOT_CONTROLLER response without leaving the broker in its read.
#[tokio::test(flavor = "current_thread")]
async fn create_topics_retry_drops_the_retired_bootstrap_socket() {
    let mock = common::Mock::start().await;
    mock.fail_create_topics_once(error::NOT_CONTROLLER);
    let mut cfg = AdminConfig::bootstrap([mock.addr.clone()]);
    cfg.request_timeout = Duration::from_secs(30);
    cfg.retry_backoff = Duration::from_secs(5);
    cfg.retry_backoff_max = Duration::from_secs(5);
    let mut admin = Admin::new(cfg).await.unwrap();
    let topics = [NewTopic::new("retry-t", 1, 1)];
    let creating = admin.create_topics(&topics, 10_000, false);
    tokio::pin!(creating);
    let started = Instant::now();
    loop {
        if mock.last_create_topics_names().is_some() {
            break;
        }
        if started.elapsed() > Duration::from_secs(2) {
            panic!("create topics was not rejected as not controller");
        }
        tokio::select! {
            result = &mut creating => panic!("create topics finished before the retry wait: {result:?}"),
            _ = tokio::time::sleep(Duration::from_millis(5)) => {}
        }
    }
    let started = Instant::now();
    loop {
        if mock.open_conns() == 0 {
            break;
        }
        if started.elapsed() > Duration::from_millis(400) {
            panic!("create topics left the retired bootstrap socket in a broker read");
        }
        tokio::select! {
            result = &mut creating => panic!("create topics finished during retry backoff: {result:?}"),
            _ = tokio::time::sleep(Duration::from_millis(5)) => {}
        }
    }
}

/// A fetch session retry drops the leader socket when that broker closes during backoff.
#[tokio::test(flavor = "current_thread")]
async fn fetch_drops_the_socket_when_the_broker_closes_during_session_retry() {
    let mock = common::Mock::start().await;
    mock.enable_fetch_sessions([1]);
    let mut producer_cfg = ProducerConfig::bootstrap([mock.addr.clone()]);
    producer_cfg.linger = Duration::ZERO;
    let producer = Producer::new(producer_cfg).await.unwrap();
    let _sent = producer
        .send(ProduceRecord::to("t").value(b"session".to_vec()))
        .await
        .unwrap();
    producer.close().await.unwrap();
    let mut cfg = ConsumerConfig::bootstrap([mock.addr.clone()]);
    cfg.request_timeout = Duration::from_secs(30);
    cfg.max_wait_ms = 10;
    cfg.retry_backoff = Duration::from_secs(5);
    cfg.retry_backoff_max = Duration::from_secs(5);
    let mut consumer = Consumer::new(cfg).await.unwrap();
    consumer.assign("t", 0, 0).await.unwrap();
    let first = consumer.fetch().await.unwrap();
    assert_eq!(first.len(), 1);
    assert!(mock.fetch_session_id(1).is_some());
    mock.reset_fetch_sessions(1);
    let before = mock.fetch_session_requests().len();
    let fetch = consumer.fetch();
    tokio::pin!(fetch);
    let started = Instant::now();
    loop {
        if mock.fetch_session_requests().len() > before {
            break;
        }
        if started.elapsed() > Duration::from_secs(2) {
            panic!("incremental fetch was not sent");
        }
        tokio::select! {
            result = &mut fetch => panic!("fetch finished before the session retry: {result:?}"),
            _ = tokio::time::sleep(Duration::from_millis(5)) => {}
        }
    }
    let seen = mock.fetch_session_requests().len();
    let started = Instant::now();
    while started.elapsed() < Duration::from_millis(80) {
        if mock.fetch_session_requests().len() != seen {
            panic!("session retry did not stay in backoff");
        }
        tokio::select! {
            result = &mut fetch => panic!("fetch finished before the retry backoff: {result:?}"),
            _ = tokio::time::sleep(Duration::from_millis(5)) => {}
        }
    }
    let accepts = mock.accepts();
    mock.shutdown_writes();
    let started = Instant::now();
    loop {
        if mock.accepts() > accepts {
            break;
        }
        if started.elapsed() > Duration::from_millis(400) {
            panic!("fetch kept the socket during session retry backoff");
        }
        tokio::select! {
            result = &mut fetch => panic!("fetch finished during session retry backoff: {result:?}"),
            _ = tokio::time::sleep(Duration::from_millis(5)) => {}
        }
    }
}

/// A fetch waiting on a broker quota drops the leader socket when that broker closes.
#[tokio::test(flavor = "current_thread")]
async fn fetch_drops_the_socket_when_the_broker_closes_during_throttle() {
    let mock = common::Mock::start().await;
    let mut producer_cfg = ProducerConfig::bootstrap([mock.addr.clone()]);
    producer_cfg.linger = Duration::ZERO;
    let producer = Producer::new(producer_cfg).await.unwrap();
    let _sent = producer
        .send(ProduceRecord::to("t").value(b"first".to_vec()))
        .await
        .unwrap();
    mock.set_fetch_throttles(1, [5_000]);
    let mut cfg = ConsumerConfig::bootstrap([mock.addr.clone()]);
    cfg.request_timeout = Duration::from_secs(30);
    cfg.max_wait_ms = 10_000;
    let mut consumer = Consumer::new(cfg).await.unwrap();
    consumer.assign("t", 0, 0).await.unwrap();
    let first = consumer.fetch().await.unwrap();
    assert_eq!(first.len(), 1);
    producer.close().await.unwrap();
    let fetch = consumer.fetch();
    tokio::pin!(fetch);
    let started = Instant::now();
    while started.elapsed() < Duration::from_millis(50) {
        tokio::select! {
            result = &mut fetch => panic!("fetch finished before the broker closed: {result:?}"),
            _ = tokio::time::sleep(Duration::from_millis(5)) => {}
        }
    }
    let accepts = mock.accepts();
    mock.shutdown_writes();
    let started = Instant::now();
    loop {
        if mock.accepts() > accepts {
            break;
        }
        if started.elapsed() > Duration::from_millis(400) {
            panic!("fetch kept the socket while the broker quota wait ran");
        }
        tokio::select! {
            result = &mut fetch => panic!("fetch finished during the quota wait: {result:?}"),
            _ = tokio::time::sleep(Duration::from_millis(5)) => {}
        }
    }
}

/// A later muted leader is dropped when it closes, even if an earlier quota is still running.
#[tokio::test(flavor = "current_thread")]
async fn fetch_drops_a_later_leader_when_it_closes_during_throttle() {
    let mock = common::Mock::start_two_node().await;
    mock.set_topic_partitions("t", 2);
    mock.set_partition_leader("t", 0, 1);
    mock.set_partition_leader("t", 1, 2);
    let mut producer_cfg = ProducerConfig::bootstrap([mock.addr.clone()]);
    producer_cfg.linger = Duration::ZERO;
    let producer = Producer::new(producer_cfg).await.unwrap();
    for partition in 0..2 {
        let _sent = producer
            .send(
                ProduceRecord::to("t")
                    .partition(partition)
                    .value(b"v".to_vec()),
            )
            .await
            .unwrap();
    }
    mock.set_fetch_throttles(1, [5_000]);
    mock.set_fetch_throttles(2, [8_000]);
    let mut cfg = ConsumerConfig::bootstrap([mock.addr.clone()]);
    cfg.request_timeout = Duration::from_secs(30);
    cfg.max_wait_ms = 10_000;
    let mut consumer = Consumer::new(cfg).await.unwrap();
    consumer
        .assign_many([(("t", 0), 0), (("t", 1), 0)])
        .await
        .unwrap();
    let first = consumer.fetch().await.unwrap();
    assert_eq!(first.len(), 2);
    producer.close().await.unwrap();
    let fetch = consumer.fetch();
    tokio::pin!(fetch);
    let started = Instant::now();
    while started.elapsed() < Duration::from_millis(50) {
        tokio::select! {
            result = &mut fetch => panic!("fetch finished before the later leader closed: {result:?}"),
            _ = tokio::time::sleep(Duration::from_millis(5)) => {}
        }
    }
    let open = mock.open_conns();
    if open == 0 {
        panic!("throttled fetch did not keep a broker socket");
    }
    mock.shutdown_node_writes(2);
    let started = Instant::now();
    loop {
        if mock.open_conns() < open {
            break;
        }
        if started.elapsed() > Duration::from_millis(400) {
            panic!("fetch kept the later leader socket while an earlier quota wait ran");
        }
        tokio::select! {
            result = &mut fetch => panic!("fetch finished during the quota wait: {result:?}"),
            _ = tokio::time::sleep(Duration::from_millis(5)) => {}
        }
    }
}

/// Closing a throttled fetch drops the leader socket when that broker is already gone.
#[tokio::test(flavor = "current_thread")]
async fn fetch_close_drops_the_socket_when_the_broker_closes_during_throttle() {
    let mock = common::Mock::start().await;
    mock.enable_fetch_sessions([1]);
    mock.set_fetch_throttles(1, [5_000]);
    let mut cfg = ConsumerConfig::bootstrap([mock.addr.clone()]);
    cfg.request_timeout = Duration::from_secs(30);
    cfg.max_wait_ms = 10;
    let mut consumer = Consumer::new(cfg).await.unwrap();
    consumer.assign("t", 0, 0).await.unwrap();
    let _first = consumer.fetch().await.unwrap();
    assert!(mock.fetch_session_id(1).is_some());
    let close = consumer.close_timeout(Duration::from_secs(30));
    tokio::pin!(close);
    let started = Instant::now();
    while started.elapsed() < Duration::from_millis(50) {
        tokio::select! {
            result = &mut close => panic!("close finished before the quota wait: {result:?}"),
            _ = tokio::time::sleep(Duration::from_millis(5)) => {}
        }
    }
    let open = mock.open_conns();
    if open == 0 {
        panic!("fetch close did not keep a broker socket through the quota wait");
    }
    mock.shutdown_writes();
    let started = Instant::now();
    loop {
        if mock.open_conns() < open {
            break;
        }
        if started.elapsed() > Duration::from_millis(400) {
            panic!("fetch close kept the socket while the broker quota wait ran");
        }
        tokio::select! {
            result = &mut close => {
                if mock.open_conns() < open {
                    result.expect("close");
                    break;
                }
                panic!("close finished while the broker was still reading: {result:?}");
            }
            _ = tokio::time::sleep(Duration::from_millis(5)) => {}
        }
    }
}

/// Closing a producer closes the metadata socket before close returns.
#[tokio::test(flavor = "current_thread")]
async fn producer_close_closes_the_metadata_socket() {
    let mock = common::Mock::start().await;
    let mut cfg = ProducerConfig::bootstrap([mock.addr.clone()]);
    cfg.request_timeout = Duration::from_secs(2);
    let producer = Producer::new(cfg).await.unwrap();
    assert_eq!(
        mock.open_conns(),
        1,
        "producer connect should hold the metadata socket"
    );
    producer.close().await.unwrap();
    assert_eq!(
        mock.open_conns(),
        0,
        "producer close left the socket blocked in a broker read"
    );
}

/// Closing a manual consumer closes the bootstrap socket before close returns.
#[tokio::test(flavor = "current_thread")]
async fn consumer_close_closes_the_bootstrap_socket() {
    let mock = common::Mock::start().await;
    let mut cfg = ConsumerConfig::bootstrap([mock.addr.clone()]);
    cfg.request_timeout = Duration::from_secs(2);
    cfg.max_wait_ms = 10;
    let consumer = Consumer::new(cfg).await.unwrap();
    assert_eq!(
        mock.open_conns(),
        1,
        "consumer connect should hold the bootstrap socket"
    );
    consumer.close().await.unwrap();
    assert_eq!(
        mock.open_conns(),
        0,
        "consumer close left the socket blocked in a broker read"
    );
}

/// A second socket to a known admin broker closes when SASL times out.
#[tokio::test(flavor = "current_thread")]
async fn known_admin_peer_timeout_closes_the_socket() {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;
    use tokio::io::AsyncReadExt;

    let mock = common::Mock::start().await;
    let mut cfg = AdminConfig::bootstrap([mock.addr.clone()]);
    cfg.request_timeout = Duration::from_millis(400);
    cfg.connect_timeout = Duration::from_millis(200);
    let mut admin = Admin::new(cfg).await.unwrap();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let slow = listener.local_addr().unwrap();
    let open = Arc::new(AtomicUsize::new(0));
    let open_flag = Arc::clone(&open);
    let peer = tokio::spawn(async move {
        loop {
            let (mut sock, _) = listener.accept().await.unwrap();
            let _started = open_flag.fetch_add(1, Ordering::SeqCst);
            let open_flag = Arc::clone(&open_flag);
            let _reader = tokio::spawn(async move {
                let mut buf = [0u8; 64];
                loop {
                    match sock.read(&mut buf).await {
                        Ok(0) | Err(_) => break,
                        Ok(_) => continue,
                    }
                }
                let _left = open_flag.fetch_sub(1, Ordering::SeqCst);
            });
        }
    });
    let result = admin.test_open_known_sasl_peer(&slow.to_string()).await;
    assert!(result.is_err(), "known admin peer should time out");
    assert_eq!(
        open.load(Ordering::SeqCst),
        0,
        "known admin peer left the socket blocked in a broker read"
    );
    peer.abort();
}

/// A fetch that reloads metadata closes the leader dial started beside that reload.
#[tokio::test(flavor = "current_thread")]
async fn fetch_refresh_timeout_closes_the_leader_socket() {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;
    use tokio::io::AsyncReadExt;

    let mock = common::Mock::start_two_node().await;
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let slow = listener.local_addr().unwrap();
    let open = Arc::new(AtomicUsize::new(0));
    let open_flag = Arc::clone(&open);
    let peer = tokio::spawn(async move {
        loop {
            let (mut sock, _) = listener.accept().await.unwrap();
            let _started = open_flag.fetch_add(1, Ordering::SeqCst);
            let open_flag = Arc::clone(&open_flag);
            let _reader = tokio::spawn(async move {
                let mut buf = [0u8; 64];
                loop {
                    match sock.read(&mut buf).await {
                        Ok(0) | Err(_) => break,
                        Ok(_) => continue,
                    }
                }
                let _left = open_flag.fetch_sub(1, Ordering::SeqCst);
            });
        }
    });
    mock.set_broker_address(2, "127.0.0.1", i32::from(slow.port()));
    mock.set_topic_partitions("u", 1);
    mock.set_partition_leader("u", 0, 2);
    let mut cfg = ConsumerConfig::bootstrap([mock.addr.clone()]);
    cfg.request_timeout = Duration::from_millis(400);
    cfg.connect_timeout = Duration::from_millis(200);
    cfg.reconnect_backoff = Duration::ZERO;
    cfg.reconnect_backoff_max = Duration::ZERO;
    cfg.retry_backoff = Duration::ZERO;
    cfg.retry_backoff_max = Duration::ZERO;
    cfg.max_wait_ms = 10;
    cfg.metadata_max_age = Duration::ZERO;
    let mut consumer = Consumer::new(cfg).await.unwrap();
    consumer.assign("u", 0, 0).await.unwrap();
    let result = consumer.fetch().await;
    assert!(result.is_err(), "fetch should time out");
    assert_eq!(
        open.load(Ordering::SeqCst),
        0,
        "fetch refresh left the leader socket blocked in a broker read"
    );
    peer.abort();
}

/// List offsets reloads an unknown topic while a known leader is dialed.
#[tokio::test(flavor = "current_thread")]
async fn list_offsets_refresh_timeout_closes_the_leader_socket() {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;
    use tokio::io::AsyncReadExt;

    let mock = common::Mock::start_two_node().await;
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let slow = listener.local_addr().unwrap();
    let open = Arc::new(AtomicUsize::new(0));
    let open_flag = Arc::clone(&open);
    let peer = tokio::spawn(async move {
        loop {
            let (mut sock, _) = listener.accept().await.unwrap();
            let _started = open_flag.fetch_add(1, Ordering::SeqCst);
            let open_flag = Arc::clone(&open_flag);
            let _reader = tokio::spawn(async move {
                let mut buf = [0u8; 64];
                loop {
                    match sock.read(&mut buf).await {
                        Ok(0) | Err(_) => break,
                        Ok(_) => continue,
                    }
                }
                let _left = open_flag.fetch_sub(1, Ordering::SeqCst);
            });
        }
    });
    mock.set_broker_address(2, "127.0.0.1", i32::from(slow.port()));
    mock.set_topic_partitions("u", 1);
    mock.set_partition_leader("u", 0, 2);
    let mut cfg = AdminConfig::bootstrap([mock.addr.clone()]);
    cfg.request_timeout = Duration::from_secs(2);
    cfg.connect_timeout = Duration::from_millis(200);
    cfg.reconnect_backoff = Duration::ZERO;
    cfg.reconnect_backoff_max = Duration::ZERO;
    cfg.retry_backoff = Duration::ZERO;
    cfg.retry_backoff_max = Duration::ZERO;
    let mut admin = Admin::new(cfg).await.unwrap();
    let warmed = admin
        .list_offsets_timeout(
            [(TopicPartition::new("u", 0), -1i64)],
            Duration::from_millis(400),
        )
        .await;
    assert!(warmed.is_err(), "the first list offsets should time out");
    assert_eq!(
        open.load(Ordering::SeqCst),
        0,
        "the first list offsets left the leader socket open"
    );
    mock.set_topic_partitions("v", 1);
    mock.set_partition_leader("v", 0, 1);
    let result = admin
        .list_offsets_timeout(
            [
                (TopicPartition::new("u", 0), -1i64),
                (TopicPartition::new("v", 0), -1i64),
            ],
            Duration::from_millis(400),
        )
        .await;
    assert!(result.is_err(), "list offsets should time out");
    assert_eq!(
        open.load(Ordering::SeqCst),
        0,
        "list offsets refresh left the leader socket blocked in a broker read"
    );
    peer.abort();
}

#[tokio::test(flavor = "current_thread")]
async fn list_offsets_timeout_closes_the_leader_socket() {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;
    use tokio::io::AsyncReadExt;

    let mock = common::Mock::start_two_node().await;
    let mut cfg = AdminConfig::bootstrap([mock.addr.clone()]);
    cfg.request_timeout = Duration::from_secs(2);
    cfg.connect_timeout = Duration::from_millis(200);
    cfg.reconnect_backoff = Duration::ZERO;
    cfg.reconnect_backoff_max = Duration::ZERO;
    cfg.retry_backoff = Duration::ZERO;
    cfg.retry_backoff_max = Duration::ZERO;
    let mut admin = Admin::new(cfg).await.unwrap();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let slow = listener.local_addr().unwrap();
    let open = Arc::new(AtomicUsize::new(0));
    let open_flag = Arc::clone(&open);
    let peer = tokio::spawn(async move {
        loop {
            let (mut sock, _) = listener.accept().await.unwrap();
            let _started = open_flag.fetch_add(1, Ordering::SeqCst);
            let open_flag = Arc::clone(&open_flag);
            let _reader = tokio::spawn(async move {
                let mut buf = [0u8; 64];
                loop {
                    match sock.read(&mut buf).await {
                        Ok(0) | Err(_) => break,
                        Ok(_) => continue,
                    }
                }
                let _left = open_flag.fetch_sub(1, Ordering::SeqCst);
            });
        }
    });
    mock.set_broker_address(2, "127.0.0.1", i32::from(slow.port()));
    mock.set_topic_partitions("u", 1);
    mock.set_partition_leader("u", 0, 2);
    let result = admin
        .list_offsets_timeout(
            [(TopicPartition::new("u", 0), -1i64)],
            Duration::from_millis(200),
        )
        .await;
    assert!(result.is_err(), "list offsets should time out");
    assert_eq!(
        open.load(Ordering::SeqCst),
        0,
        "list offsets left the leader socket blocked in a broker read"
    );
    peer.abort();
}

/// An offset fetch timeout closes the bootstrap socket before the call returns.
#[tokio::test(flavor = "current_thread")]
async fn offset_fetch_timeout_closes_the_bootstrap_socket() {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;
    use tokio::io::AsyncReadExt;

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap().to_string();
    let open = Arc::new(AtomicUsize::new(0));
    let open_flag = Arc::clone(&open);
    let peer = tokio::spawn(async move {
        loop {
            let (mut sock, _) = listener.accept().await.unwrap();
            let _started = open_flag.fetch_add(1, Ordering::SeqCst);
            let open_flag = Arc::clone(&open_flag);
            let _reader = tokio::spawn(async move {
                let mut buf = [0u8; 64];
                loop {
                    match sock.read(&mut buf).await {
                        Ok(0) | Err(_) => break,
                        Ok(_) => continue,
                    }
                }
                let _left = open_flag.fetch_sub(1, Ordering::SeqCst);
            });
        }
    });
    let mut cfg = AdminConfig::bootstrap([addr]);
    cfg.request_timeout = Duration::from_millis(200);
    cfg.connect_timeout = Duration::from_millis(200);
    cfg.retry_backoff = Duration::ZERO;
    cfg.retry_backoff_max = Duration::ZERO;
    let mut client = OffsetClient::new(cfg).unwrap();
    let mut options = offset_options();
    options.timeout = Some(Duration::from_millis(200));
    let result = client.fetch(&offset_fetch("g"), &options).await;
    assert!(result.is_err(), "the offset fetch should time out");
    assert_eq!(
        open.load(Ordering::SeqCst),
        0,
        "offset fetch left the bootstrap socket blocked in a broker read"
    );
    peer.abort();
}

/// A failed coordinator handshake still finishes the next dial already in flight.
#[tokio::test(flavor = "current_thread")]
async fn offset_fetch_finishes_a_dial_when_an_earlier_handshake_fails() {
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::Arc;
    use tokio::io::AsyncReadExt;

    let mock = common::Mock::start_two_node().await;
    let mut closed = None;
    let mut listener = None;
    for _ in 0..32 {
        let refused = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let blackhole = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let refused_addr = refused.local_addr().unwrap().to_string();
        let blackhole_addr = blackhole.local_addr().unwrap().to_string();
        if refused_addr < blackhole_addr {
            closed = Some(refused.local_addr().unwrap());
            listener = Some(blackhole);
            drop(refused);
            break;
        }
    }
    let closed = closed.expect("refused port sorts before the blackhole");
    let listener = listener.expect("blackhole");
    let slow = listener.local_addr().unwrap();
    let dropped = Arc::new(AtomicBool::new(false));
    let flag = Arc::clone(&dropped);
    let peer = tokio::spawn(async move {
        let (mut sock, _) = listener.accept().await.unwrap();
        let mut buf = [0u8; 64];
        loop {
            match sock.read(&mut buf).await {
                Ok(0) | Err(_) => break,
                Ok(_) => continue,
            }
        }
        flag.store(true, Ordering::SeqCst);
    });
    mock.set_broker_address(1, "127.0.0.1", i32::from(closed.port()));
    mock.set_broker_address(2, "127.0.0.1", i32::from(slow.port()));
    mock.set_group_coordinator("fail", 1);
    mock.set_group_coordinator("slow", 2);
    let mut cfg = AdminConfig::bootstrap([mock.addr.clone()]);
    cfg.request_timeout = Duration::from_secs(2);
    cfg.connect_timeout = Duration::from_millis(500);
    let mut client = OffsetClient::new(cfg).unwrap();
    let request = OffsetFetchRequestData {
        groups: vec![
            OffsetFetchGroupData {
                group_id: "fail".into(),
                member_id: None,
                member_epoch: -1,
                topics: None,
            },
            OffsetFetchGroupData {
                group_id: "slow".into(),
                member_id: None,
                member_epoch: -1,
                topics: None,
            },
        ],
        require_stable: false,
    };
    let mut options = offset_options();
    options.timeout = Some(Duration::from_secs(2));
    let result = client.test_fetch_once(&request, &options).await;
    assert!(
        result.is_err(),
        "the refused coordinator should fail the fetch"
    );
    assert!(
        dropped.load(Ordering::SeqCst),
        "offset fetch left the next coordinator dial blocked in a broker read"
    );
    peer.abort();
}

fn offset_fetch(group: &str) -> OffsetFetchRequestData {
    OffsetFetchRequestData {
        groups: vec![OffsetFetchGroupData {
            group_id: group.into(),
            member_id: None,
            member_epoch: -1,
            topics: None,
        }],
        require_stable: false,
    }
}

fn offset_options() -> OffsetOptions {
    OffsetOptions {
        topic_ids: false,
        timeout: Some(Duration::from_secs(3)),
        ..OffsetOptions::default()
    }
}

/// A coordinator that is another bootstrap reuses that finished handshake.
#[tokio::test]
async fn offset_fetch_reuses_a_finished_bootstrap_handshake() {
    let mock = common::Mock::start_two_node().await;
    mock.set_group_coordinator("g", 2);
    mock.set_api_versions_delay(Duration::from_millis(250));
    let addr2 = mock.broker_addr(2).unwrap();
    let mut cfg = AdminConfig::bootstrap([mock.addr.clone(), addr2]);
    cfg.request_timeout = Duration::from_secs(3);
    let mut client = OffsetClient::new(cfg).unwrap();
    let started = Instant::now();
    let fetched = client
        .fetch(&offset_fetch("g"), &offset_options())
        .await
        .unwrap();
    let elapsed = started.elapsed();
    assert_eq!(fetched.groups.len(), 1);
    assert!(
        mock.api_versions_versions().len() <= 4,
        "offset fetch negotiated a bootstrap that was already open: {}",
        mock.api_versions_versions().len()
    );
    assert!(
        elapsed < Duration::from_millis(700),
        "offset fetch repeated a bootstrap handshake: {elapsed:?}"
    );
}

/// A second offset fetch reuses ApiVersions learned on the first contact.
#[tokio::test]
async fn offset_fetch_repeats_without_another_handshake() {
    let mock = common::Mock::start().await;
    mock.set_group_coordinator("g", 1);
    let mut cfg = AdminConfig::bootstrap([mock.addr.clone()]);
    cfg.request_timeout = Duration::from_secs(3);
    let mut client = OffsetClient::new(cfg).unwrap();
    let request = offset_fetch("g");
    let options = offset_options();
    let fetched = client.fetch(&request, &options).await.unwrap();
    assert_eq!(fetched.groups.len(), 1);
    assert_eq!(mock.api_versions_versions().len(), 1);
    mock.set_api_versions_delay(Duration::from_millis(250));
    let started = Instant::now();
    let fetched = client.fetch(&request, &options).await.unwrap();
    assert_eq!(fetched.groups.len(), 1);
    let elapsed = started.elapsed();
    assert!(
        elapsed < Duration::from_millis(200),
        "repeated offset fetch negotiated again: {elapsed:?}"
    );
    assert_eq!(mock.api_versions_versions().len(), 1);
}

/// A coordinator that is not the bootstrap broker is remembered too.
#[tokio::test]
async fn offset_fetch_repeats_a_remote_coordinator_without_another_handshake() {
    let mock = common::Mock::start_two_node().await;
    mock.set_group_coordinator("g", 2);
    let mut cfg = AdminConfig::bootstrap([mock.addr.clone()]);
    cfg.request_timeout = Duration::from_secs(3);
    let mut client = OffsetClient::new(cfg).unwrap();
    let request = offset_fetch("g");
    let options = offset_options();
    let fetched = client.fetch(&request, &options).await.unwrap();
    assert_eq!(fetched.groups.len(), 1);
    assert_eq!(mock.api_versions_versions().len(), 2);
    mock.set_api_versions_delay(Duration::from_millis(250));
    let started = Instant::now();
    let fetched = client.fetch(&request, &options).await.unwrap();
    assert_eq!(fetched.groups.len(), 1);
    let elapsed = started.elapsed();
    assert!(
        elapsed < Duration::from_millis(200),
        "repeated remote offset fetch negotiated again: {elapsed:?}"
    );
    assert_eq!(mock.api_versions_versions().len(), 2);
}

/// A coordinator that has never been contacted still negotiates.
#[tokio::test]
async fn offset_fetch_negotiates_a_new_coordinator() {
    let mock = common::Mock::start_three_node().await;
    mock.set_group_coordinator("known", 2);
    mock.set_group_coordinator("fresh", 3);
    let mut cfg = AdminConfig::bootstrap([mock.addr.clone()]);
    cfg.request_timeout = Duration::from_secs(3);
    let mut client = OffsetClient::new(cfg).unwrap();
    let options = offset_options();
    let fetched = client
        .fetch(&offset_fetch("known"), &options)
        .await
        .unwrap();
    assert_eq!(fetched.groups.len(), 1);
    let before = mock.api_versions_versions().len();
    assert_eq!(before, 2);
    mock.set_api_versions_delay(Duration::from_millis(250));
    let started = Instant::now();
    let fetched = client
        .fetch(&offset_fetch("fresh"), &options)
        .await
        .unwrap();
    assert_eq!(fetched.groups.len(), 1);
    let elapsed = started.elapsed();
    assert_eq!(mock.api_versions_versions().len(), before + 1);
    assert!(
        elapsed < Duration::from_millis(450),
        "new coordinator redid the bootstrap handshake: {elapsed:?}"
    );
}

#[tokio::test]
async fn offset_client_uses_the_first_bootstrap_that_finishes() {
    let mock = common::Mock::start().await;
    let stall = stall_listener().await;
    let mut cfg = AdminConfig::bootstrap([stall, mock.addr.clone()]);
    cfg.connect_timeout = Duration::from_millis(200);
    cfg.request_timeout = Duration::from_secs(2);
    let mut client = OffsetClient::new(cfg).unwrap();
    let started = Instant::now();
    let fetched = client
        .fetch(
            &OffsetFetchRequestData {
                groups: vec![OffsetFetchGroupData {
                    group_id: "g".into(),
                    member_id: None,
                    member_epoch: -1,
                    topics: None,
                }],
                require_stable: false,
            },
            &OffsetOptions {
                topic_ids: false,
                timeout: Some(Duration::from_secs(2)),
                ..OffsetOptions::default()
            },
        )
        .await
        .unwrap();
    let elapsed = started.elapsed();
    assert!(
        elapsed < Duration::from_millis(500),
        "offset client waited for the stalled bootstrap: {elapsed:?}"
    );
    assert_eq!(fetched.groups.len(), 1);
}

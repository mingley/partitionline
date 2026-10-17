//! A bootstrap that accepts TCP and then stalls must not hold startup.
mod common;

use partitionline::protocol::api_keys::OFFSET_FETCH;
use partitionline::protocol::group::{OffsetFetchGroupData, OffsetFetchRequestData};
use partitionline::{
    error, AbortTransactionSpec, AclBinding, Admin, AdminConfig, AlterConfig,
    AssignReplicasToDirsDirectory, AssignReplicasToDirsPartition, AssignReplicasToDirsTopic,
    ClientQuotaAlteration, ClientQuotaEntity, ClientQuotaOp, ConfigResource, Consumer,
    ConsumerConfig, ConsumerGroup, DescribeShareGroupOffsetsGroup, ElectLeadersOptions, Error,
    FeatureUpdate, ListConsumerGroupOffsetsSpec, MemberToRemove, NewTopic, OffsetAndMetadata,
    OffsetClient, OffsetOptions, OffsetSpec, PartitionReassignment, Producer, ProducerConfig,
    ScramMechanism, TopicPartition, TopicPartitionReplica, UserScramCredentialUpsertion,
};
use std::time::{Duration, Instant};

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

/// An idle socket to a leader that is not the bootstrap broker keeps the versions
/// learned on the first dial.
#[tokio::test]
async fn list_offsets_redials_a_remote_leader_without_another_handshake() {
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
    mock.set_api_versions_delay(Duration::from_millis(250));
    let started = Instant::now();
    let listed = admin
        .list_offsets([(("t", 0), OffsetSpec::latest())])
        .await
        .unwrap();
    let elapsed = started.elapsed();
    assert_eq!(listed.len(), 1, "{listed:?}");
    assert!(
        elapsed < Duration::from_millis(200),
        "remote leader redial repeated ApiVersions: {elapsed:?}"
    );
    assert_eq!(mock.api_versions_versions().len(), 2);
}

/// Parallel leader dials reuse versions after the sockets expire.
#[tokio::test]
async fn list_offsets_redials_remote_leaders_together_without_another_handshake() {
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
        "remote leader redials repeated ApiVersions: {elapsed:?}"
    );
    assert_eq!(mock.api_versions_versions().len(), 3);
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

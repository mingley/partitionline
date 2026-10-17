//! A bootstrap that accepts TCP and then stalls must not hold startup.
mod common;

use partitionline::protocol::api_keys::OFFSET_FETCH;
use partitionline::protocol::group::{OffsetFetchGroupData, OffsetFetchRequestData};
use partitionline::{
    error, Admin, AdminConfig, Consumer, ConsumerConfig, ConsumerGroup,
    DescribeShareGroupOffsetsGroup, Error, ListConsumerGroupOffsetsSpec, OffsetClient,
    OffsetOptions, OffsetSpec, Producer, ProducerConfig,
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

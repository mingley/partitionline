//! Discovery can use a live authenticated bootstrap socket while replacements negotiate.
mod common;

use partitionline::protocol::api_keys::LEAVE_GROUP;
use partitionline::{ConsumerConfig, ConsumerGroup};
use std::time::{Duration, Instant};

#[tokio::test]
async fn moved_group_negotiates_destination_without_redialing_live_bootstrap() {
    let mock = common::Mock::start_two_node().await;
    mock.set_topic_partitions("t", 1);
    mock.set_partition_leader("t", 0, 1);
    let mut cfg = ConsumerConfig::bootstrap([mock.addr.clone()]).max_wait_ms(10);
    cfg.heartbeat_interval = Duration::from_secs(5);
    let mut group = ConsumerGroup::join(cfg, "live-bootstrap", "t")
        .await
        .unwrap();
    let _records = group.poll().await.unwrap();
    common::wait_pred("initial heartbeat", || {
        mock.heartbeat_total("live-bootstrap") >= 1
    })
    .await;
    let before = mock.api_versions_nodes();
    mock.set_node_api_max(2, LEAVE_GROUP, 0);
    mock.set_coordinator(2);
    mock.set_api_versions_delay(Duration::from_millis(250));
    let started = Instant::now();
    group.leave().await.unwrap();
    let elapsed = started.elapsed();
    let after = mock.api_versions_nodes();
    let new_handshakes = &after[before.len()..];
    assert_eq!(mock.last_leave_group_node(), Some(2));
    assert_eq!(mock.last_leave_group_version(), Some(0));
    assert_eq!(
        new_handshakes,
        &[(2, 4)],
        "only the replacement coordinator should open and negotiate a new connection"
    );
    assert!(elapsed < Duration::from_millis(450), "bootstrap discovery and replacement dial exceeded the established overlap bound: {elapsed:?}");
}

#[tokio::test]
async fn broken_live_bootstrap_lookup_falls_back_to_another_bootstrap() {
    let mock = common::Mock::start_two_node().await;
    mock.set_topic_partitions("t", 1);
    mock.set_partition_leader("t", 0, 1);
    let addr2 = mock.broker_addr(2).unwrap();
    let mut cfg = ConsumerConfig::bootstrap([mock.addr.clone(), addr2]).max_wait_ms(10);
    cfg.heartbeat_interval = Duration::from_secs(5);
    let mut group = ConsumerGroup::join(cfg, "broken-bootstrap", "t")
        .await
        .unwrap();
    let _records = group.poll().await.unwrap();
    common::wait_pred("initial heartbeat", || {
        mock.heartbeat_total("broken-bootstrap") >= 1
    })
    .await;
    let previous = mock.find_coordinator_nodes().len();
    mock.disconnect_find_coordinator_on(1);
    mock.set_coordinator(2);
    group.leave().await.unwrap();
    let nodes = mock.find_coordinator_nodes();
    let attempts = &nodes[previous..];
    assert!(
        attempts.contains(&1),
        "the live bootstrap must be attempted: {attempts:?}"
    );
    assert!(
        attempts.contains(&2),
        "discovery must try the other bootstrap after the live socket closes: {attempts:?}"
    );
    assert_eq!(mock.last_leave_group_node(), Some(2));
}

#[tokio::test]
async fn fatal_live_bootstrap_lookup_does_not_redial() {
    let mock = common::Mock::start_two_node().await;
    mock.set_topic_partitions("t", 1);
    mock.set_partition_leader("t", 0, 1);
    let addr2 = mock.broker_addr(2).unwrap();
    let mut cfg = ConsumerConfig::bootstrap([mock.addr.clone(), addr2]).max_wait_ms(10);
    cfg.heartbeat_interval = Duration::from_secs(5);
    let mut group = ConsumerGroup::join(cfg, "fatal-bootstrap", "t")
        .await
        .unwrap();
    let _records = group.poll().await.unwrap();
    common::wait_pred("initial heartbeat", || {
        mock.heartbeat_total("fatal-bootstrap") >= 1
    })
    .await;
    let before = mock.api_versions_nodes().len();
    mock.set_coordinator(2);
    mock.fail_find_coordinator_once(30);
    let result = group.leave().await;
    assert!(
        matches!(result, Err(partitionline::Error::Broker { code: 30, .. })),
        "unexpected result: {result:?}"
    );
    assert_eq!(
        mock.api_versions_nodes().len(),
        before,
        "fatal lookup must not start another broker dial"
    );
}

#[tokio::test]
async fn returned_bootstrap_reuses_its_negotiated_lookup_socket() {
    let mock = common::Mock::start_two_node().await;
    mock.set_topic_partitions("t", 1);
    mock.set_partition_leader("t", 0, 1);
    mock.set_coordinator(2);
    let mut cfg = ConsumerConfig::bootstrap([mock.addr.clone()]).max_wait_ms(10);
    cfg.heartbeat_interval = Duration::from_secs(5);
    let mut group = ConsumerGroup::join(cfg, "returned-bootstrap", "t")
        .await
        .unwrap();
    let _records = group.poll().await.unwrap();
    common::wait_pred("initial heartbeat", || {
        mock.heartbeat_total("returned-bootstrap") >= 1
    })
    .await;
    group.commit().await.unwrap();
    assert_eq!(mock.last_offset_commit_node(), Some(2));
    let before = mock.api_versions_nodes().len();
    mock.set_coordinator(1);
    mock.set_api_versions_delay(Duration::from_millis(250));
    let started = Instant::now();
    group.commit().await.unwrap();
    let elapsed = started.elapsed();
    let nodes = mock.api_versions_nodes();
    let recent = &nodes[before..];
    assert_eq!(mock.last_offset_commit_node(), Some(1));
    assert_eq!(
        recent.iter().filter(|(node, _)| *node == 1).count(),
        1,
        "the authenticated lookup socket should become the returned coordinator socket: {recent:?}"
    );
    assert!(
        elapsed < Duration::from_millis(450),
        "reopening the discovered bootstrap serialized two handshakes: {elapsed:?}"
    );
    group.leave().await.unwrap();
}

#[tokio::test]
async fn returned_bootstrap_reuse_checks_its_new_offset_version_range() {
    use partitionline::protocol::api_keys::OFFSET_COMMIT;

    let mock = common::Mock::start_two_node().await;
    mock.set_topic_partitions("t", 1);
    mock.set_partition_leader("t", 0, 1);
    mock.set_coordinator(2);
    let mut cfg = ConsumerConfig::bootstrap([mock.addr.clone()]).max_wait_ms(10);
    cfg.heartbeat_interval = Duration::from_secs(5);
    let mut group = ConsumerGroup::join(cfg, "returned-bootstrap-range", "t")
        .await
        .unwrap();
    let _records = group.poll().await.unwrap();
    common::wait_pred("initial heartbeat", || {
        mock.heartbeat_total("returned-bootstrap-range") >= 1
    })
    .await;
    group.commit().await.unwrap();
    assert_eq!(mock.last_offset_commit_node(), Some(2));
    assert_eq!(mock.last_offset_commit_version(), Some(9));
    let before = mock.offset_commit_calls();
    let versions_before = mock.api_versions_nodes().len();
    mock.set_node_api_max(1, OFFSET_COMMIT, 8);
    mock.set_coordinator(1);
    group.commit().await.unwrap();
    let nodes = mock.api_versions_nodes();
    assert_eq!(
        nodes[versions_before..]
            .iter()
            .filter(|(node, _)| *node == 1)
            .count(),
        1,
        "the returned bootstrap must use its one fresh lookup handshake"
    );
    assert!(mock.offset_commit_calls() > before);
    assert_eq!(mock.last_offset_commit_node(), Some(1));
    assert_eq!(mock.last_offset_commit_version(), Some(8));
    group.leave().await.unwrap();
}

//! Retriable coordinator loading preserves a healthy heartbeat connection.
mod common;

use partitionline::{ConsumerConfig, ConsumerGroup};
use std::time::{Duration, Instant};

#[tokio::test]
async fn classic_heartbeat_retries_coordinator_load_on_the_same_socket() {
    let mock = common::Mock::start_two_node().await;
    mock.set_topic_partitions("t", 1);
    mock.set_partition_leader("t", 0, 1);
    mock.set_coordinator(2);
    let mut cfg = ConsumerConfig::bootstrap([mock.addr.clone()]).max_wait_ms(10);
    cfg.heartbeat_interval = Duration::from_millis(40);
    let group = ConsumerGroup::join(cfg, "classic-load-reuse", "t")
        .await
        .unwrap();
    common::wait_pred("classic heartbeat settled", || {
        mock.heartbeat_total("classic-load-reuse") >= 3
    })
    .await;
    let beats = mock.heartbeat_total("classic-load-reuse");
    let accepts = mock.accepts();
    let versions = mock.api_versions_versions().len();
    mock.fail_heartbeat_once(partitionline::error::COORDINATOR_LOAD_IN_PROGRESS);
    mock.set_api_versions_delay(Duration::from_millis(250));
    let started = Instant::now();
    common::wait_pred("classic heartbeat retried", || {
        mock.heartbeat_total("classic-load-reuse") >= beats + 2
    })
    .await;
    let elapsed = started.elapsed();
    assert_eq!(mock.api_versions_versions().len(), versions);
    assert_eq!(mock.accepts(), accepts, "retry opened another socket");
    assert!(
        elapsed < Duration::from_millis(200),
        "retry took {elapsed:?}"
    );
    group.leave().await.unwrap();
}

#[tokio::test]
async fn consumer_heartbeat_retries_coordinator_load_on_the_same_socket() {
    let mock = common::Mock::start_two_node().await;
    mock.set_topic_partitions("t", 1);
    mock.set_partition_leader("t", 0, 1);
    mock.set_coordinator(2);
    mock.set_cg_heartbeat_interval_ms(40);
    let mut cfg = ConsumerConfig::bootstrap([mock.addr.clone()]).max_wait_ms(10);
    cfg.heartbeat_interval = Duration::from_millis(40);
    cfg.retry_backoff = Duration::from_millis(20);
    cfg.retry_backoff_max = Duration::from_millis(20);
    let group = ConsumerGroup::join_consumer(cfg, "consumer-load-reuse", "t")
        .await
        .unwrap();
    common::wait_pred("consumer heartbeat settled", || {
        mock.cg_heartbeat_calls() >= 3
    })
    .await;
    let beats = mock.cg_heartbeat_calls();
    let accepts = mock.accepts();
    let versions = mock.api_versions_versions().len();
    mock.set_cg_heartbeat_error_times(partitionline::error::COORDINATOR_LOAD_IN_PROGRESS, 1);
    mock.set_api_versions_delay(Duration::from_millis(250));
    let started = Instant::now();
    common::wait_pred("consumer heartbeat retried", || {
        mock.cg_heartbeat_calls() >= beats + 2
    })
    .await;
    let elapsed = started.elapsed();
    assert_eq!(mock.api_versions_versions().len(), versions);
    assert_eq!(mock.accepts(), accepts, "retry opened another socket");
    assert!(
        elapsed < Duration::from_millis(200),
        "retry took {elapsed:?}"
    );
    group.leave().await.unwrap();
}

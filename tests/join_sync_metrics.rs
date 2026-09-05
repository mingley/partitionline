//! KL-07 Partial: classic JoinGroup/SyncGroup ok/fail metrics.
//!
//! Join and Sync outcomes must move distinct counters so operators can diagnose
//! join storms and sync fencing from telemetry rather than payload logs. Mock
//! honesty only — not two independent human diagnosis runs and not a Suite HOLD
//! lift.
#![expect(
    dead_code,
    reason = "tests/common mock helpers are shared; this file uses a subset"
)]
#![expect(
    unused_results,
    reason = "tests often discard RecordMetadata"
)]
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    reason = "integration-test helpers; clippy.toml allow-*-in-tests covers #[test] only"
)]

mod common;

use partitionline::{ConsumerConfig, ConsumerGroup, ProduceRecord, Producer, ProducerConfig};
use std::time::Duration;

#[tokio::test]
async fn join_sync_ok_metrics_increment_on_successful_join() {
    let mock = common::Mock::start().await;
    let producer =
        Producer::new(ProducerConfig::bootstrap([mock.addr.clone()]).linger(Duration::ZERO))
            .await
            .unwrap();
    producer
        .send(ProduceRecord::to("t").value(&b"js-ok"[..]))
        .await
        .unwrap();
    producer.close().await.unwrap();

    let mut ccfg = ConsumerConfig::bootstrap([mock.addr.clone()]);
    ccfg.max_wait_ms = 10;
    let group = ConsumerGroup::join(ccfg, "js-ok", "t").await.unwrap();
    let m = group.metrics();
    assert!(
        m.join_ok >= 1,
        "successful JoinGroup must increment join_ok; got {}",
        m.join_ok
    );
    assert!(
        m.sync_ok >= 1,
        "successful SyncGroup must increment sync_ok; got {}",
        m.sync_ok
    );
    assert_eq!(m.join_fail, 0);
    assert_eq!(m.sync_fail, 0);
    group.leave().await.unwrap();
}

#[tokio::test]
async fn join_fail_metric_increments_on_rejoin_broker_error() {
    let mock = common::Mock::start().await;
    let mut ccfg = ConsumerConfig::bootstrap([mock.addr.clone()]);
    ccfg.max_wait_ms = 10;
    let mut group = ConsumerGroup::join(ccfg, "js-jfail", "t").await.unwrap();
    assert!(group.metrics().join_ok >= 1);
    let fail_before = group.metrics().join_fail;

    mock.join_group_fail_once();
    group.enforce_rebalance();
    let err = group.poll().await.unwrap_err();
    assert!(
        err.to_string().contains("GROUP_AUTHORIZATION_FAILED")
            || err.to_string().contains("JoinGroup"),
        "expected JoinGroup failure on rejoin, got {err}"
    );
    let m = group.metrics();
    assert!(
        m.join_fail > fail_before,
        "failed JoinGroup rejoin must increment join_fail; before={fail_before} after={}",
        m.join_fail
    );
    group.leave().await.unwrap();
}

#[tokio::test]
async fn sync_fail_metric_increments_on_rejoin_broker_error() {
    let mock = common::Mock::start().await;
    let mut ccfg = ConsumerConfig::bootstrap([mock.addr.clone()]);
    ccfg.max_wait_ms = 10;
    let mut group = ConsumerGroup::join(ccfg, "js-sfail", "t").await.unwrap();
    assert!(group.metrics().sync_ok >= 1);
    let fail_before = group.metrics().sync_fail;

    mock.sync_group_fail_once();
    group.enforce_rebalance();
    let err = group.poll().await.unwrap_err();
    assert!(
        err.to_string().contains("ILLEGAL_GENERATION")
            || err.to_string().contains("SyncGroup"),
        "expected SyncGroup failure on rejoin, got {err}"
    );
    let m = group.metrics();
    assert!(
        m.sync_fail > fail_before,
        "failed SyncGroup rejoin must increment sync_fail; before={fail_before} after={}",
        m.sync_fail
    );
    // Join succeeded before Sync failed.
    assert!(m.join_ok >= 2, "rejoin JoinGroup success should count; got {}", m.join_ok);
    group.leave().await.unwrap();
}

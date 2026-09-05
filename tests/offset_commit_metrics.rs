//! KL-07 Partial: `offset_commit_ok` / `offset_commit_fail` on ConsumerMetrics.
//!
//! OffsetCommit success and terminal failure must move distinct counters so
//! operators can diagnose commit health from telemetry rather than payload logs.
//! Mock honesty only — not two independent human diagnosis runs and not a
//! Suite HOLD lift.
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
async fn offset_commit_ok_metric_increments_on_successful_commit() {
    let mock = common::Mock::start().await;
    let mut pcfg = ProducerConfig::bootstrap([mock.addr.clone()]);
    pcfg.linger = Duration::ZERO;
    let producer = Producer::new(pcfg).await.unwrap();
    producer
        .send(ProduceRecord::to("t").value(&b"commit-ok"[..]))
        .await
        .unwrap();
    producer.close().await.unwrap();

    let mut ccfg = ConsumerConfig::bootstrap([mock.addr.clone()]);
    ccfg.max_wait_ms = 10;
    let mut group = ConsumerGroup::join(ccfg, "oc-ok", "t").await.unwrap();
    let _recs = group.poll().await.unwrap();

    assert_eq!(group.metrics().offset_commit_ok, 0);
    assert_eq!(group.metrics().offset_commit_fail, 0);

    group.commit().await.unwrap();

    let m = group.metrics();
    assert!(
        m.offset_commit_ok >= 1,
        "successful OffsetCommit must increment offset_commit_ok; got {}",
        m.offset_commit_ok
    );
    assert_eq!(
        m.offset_commit_fail, 0,
        "successful commit must not invent offset_commit_fail"
    );
    group.leave().await.unwrap();
}

#[tokio::test]
async fn offset_commit_fail_metric_increments_on_broker_error() {
    let mock = common::Mock::start().await;
    let mut pcfg = ProducerConfig::bootstrap([mock.addr.clone()]);
    pcfg.linger = Duration::ZERO;
    let producer = Producer::new(pcfg).await.unwrap();
    producer
        .send(ProduceRecord::to("t").value(&b"commit-fail"[..]))
        .await
        .unwrap();
    producer.close().await.unwrap();

    let mut ccfg = ConsumerConfig::bootstrap([mock.addr.clone()]);
    ccfg.max_wait_ms = 10;
    let mut group = ConsumerGroup::join(ccfg, "oc-fail", "t").await.unwrap();
    let _recs = group.poll().await.unwrap();

    mock.offset_commit_fail_once();
    let err = group.commit().await.unwrap_err();
    assert!(
        err.to_string().contains("GROUP_AUTHORIZATION_FAILED")
            || err.to_string().contains("OffsetCommit"),
        "expected OffsetCommit broker failure, got {err}"
    );

    let m = group.metrics();
    assert!(
        m.offset_commit_fail >= 1,
        "failed OffsetCommit must increment offset_commit_fail; got {}",
        m.offset_commit_fail
    );
    assert_eq!(
        m.offset_commit_ok, 0,
        "failed commit must not increment offset_commit_ok"
    );
    group.leave().await.unwrap();
}

#[tokio::test]
async fn offset_commit_metrics_stay_zero_without_commit() {
    let mock = common::Mock::start().await;
    let mut ccfg = ConsumerConfig::bootstrap([mock.addr.clone()]);
    ccfg.max_wait_ms = 10;
    let mut group = ConsumerGroup::join(ccfg, "oc-none", "t").await.unwrap();
    let _ = group.poll().await.unwrap();
    let m = group.metrics();
    assert_eq!(m.offset_commit_ok, 0);
    assert_eq!(m.offset_commit_fail, 0);
    group.leave().await.unwrap();
}

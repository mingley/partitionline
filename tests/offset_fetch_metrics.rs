//! KL-07 Partial: `offset_fetch_ok` / `offset_fetch_fail` on ConsumerMetrics.
//!
//! OffsetFetch success and terminal failure must move distinct counters so
//! operators can diagnose committed-offset lookups from telemetry rather than
//! payload logs. Mock honesty only — not two independent human diagnosis runs
//! and not a Suite HOLD lift.
#![expect(
    dead_code,
    reason = "tests/common mock helpers are shared; this file uses a subset"
)]
#![expect(unused_results, reason = "tests often discard RecordMetadata")]
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    reason = "integration-test helpers; clippy.toml allow-*-in-tests covers #[test] only"
)]

mod common;

use partitionline::{
    ConsumerConfig, ConsumerGroup, ProduceRecord, Producer, ProducerConfig,
};
use std::time::Duration;

#[tokio::test]
async fn offset_fetch_ok_metric_increments_on_successful_committed() {
    let mock = common::Mock::start().await;
    let producer =
        Producer::new(ProducerConfig::bootstrap([mock.addr.clone()]).linger(Duration::ZERO))
            .await
            .unwrap();
    producer
        .send(ProduceRecord::to("t").value(&b"of-ok"[..]))
        .await
        .unwrap();
    producer.close().await.unwrap();

    let mut group = ConsumerGroup::join(
        ConsumerConfig::bootstrap([mock.addr.clone()]).max_wait_ms(10),
        "of-ok",
        "t",
    )
    .await
    .unwrap();
    // Join/assignment may already have counted OffsetFetch successes.
    let before_ok = group.metrics().offset_fetch_ok;
    let before_fail = group.metrics().offset_fetch_fail;

    let committed = group.committed().await.unwrap();
    assert!(!committed.is_empty());

    let m = group.metrics();
    assert!(
        m.offset_fetch_ok > before_ok,
        "successful OffsetFetch must increment offset_fetch_ok; before={before_ok} after={}",
        m.offset_fetch_ok
    );
    assert_eq!(
        m.offset_fetch_fail, before_fail,
        "successful OffsetFetch must not invent offset_fetch_fail"
    );
    group.leave().await.unwrap();
}

#[tokio::test]
async fn offset_fetch_fail_metric_increments_on_broker_error() {
    let mock = common::Mock::start().await;
    let producer =
        Producer::new(ProducerConfig::bootstrap([mock.addr.clone()]).linger(Duration::ZERO))
            .await
            .unwrap();
    producer
        .send(ProduceRecord::to("t").value(&b"of-fail"[..]))
        .await
        .unwrap();
    producer.close().await.unwrap();

    let mut group = ConsumerGroup::join(
        ConsumerConfig::bootstrap([mock.addr.clone()]).max_wait_ms(10),
        "of-fail",
        "t",
    )
    .await
    .unwrap();
    let before_ok = group.metrics().offset_fetch_ok;
    let before_fail = group.metrics().offset_fetch_fail;

    mock.offset_fetch_fail_once();
    let err = group.committed().await.unwrap_err();
    assert!(
        err.to_string().contains("GROUP_AUTHORIZATION_FAILED")
            || err.to_string().contains("OffsetFetch"),
        "expected OffsetFetch broker failure, got {err}"
    );

    let m = group.metrics();
    assert!(
        m.offset_fetch_fail > before_fail,
        "failed OffsetFetch must increment offset_fetch_fail; before={before_fail} after={}",
        m.offset_fetch_fail
    );
    assert!(
        m.offset_fetch_ok >= before_ok,
        "prior offset_fetch_ok must not be wiped"
    );
    group.leave().await.unwrap();
}

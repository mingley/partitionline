//! KL-07 Partial: `ConsumerMetrics::metadata_refresh_ok` / `metadata_refresh_fail`.
//!
//! Metadata refresh successes and terminal failures must move distinct counters
//! so operators can diagnose stale clusters / unknown-topic storms from
//! telemetry rather than payload logs. Mock honesty only — not two independent
//! human diagnosis runs and not a Suite HOLD lift.
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

use partitionline::{Consumer, ConsumerConfig, ProduceRecord, Producer, ProducerConfig};
use std::time::Duration;

#[tokio::test]
async fn metadata_refresh_ok_increments_on_assign() {
    let mock = common::Mock::start().await;
    let producer =
        Producer::new(ProducerConfig::bootstrap([mock.addr.clone()]).linger(Duration::ZERO))
            .await
            .unwrap();
    producer
        .send(ProduceRecord::to("t").value(&b"md-ok"[..]))
        .await
        .unwrap();
    producer.close().await.unwrap();

    let mut consumer = Consumer::new(ConsumerConfig::bootstrap([mock.addr.clone()]))
        .await
        .unwrap();
    let before = consumer.metrics().metadata_refresh_ok;
    consumer.assign("t", 0, 0).await.unwrap();
    let m = consumer.metrics();
    assert!(
        m.metadata_refresh_ok > before,
        "successful Metadata refresh must increment metadata_refresh_ok; before={before} after={}",
        m.metadata_refresh_ok
    );
    assert_eq!(m.metadata_refresh_fail, 0);
    consumer.close().await.unwrap();
}

#[tokio::test]
async fn metadata_refresh_fail_increments_on_broker_error() {
    let mock = common::Mock::start().await;
    let producer =
        Producer::new(ProducerConfig::bootstrap([mock.addr.clone()]).linger(Duration::ZERO))
            .await
            .unwrap();
    producer
        .send(ProduceRecord::to("t").value(&b"md-fail"[..]))
        .await
        .unwrap();
    producer.close().await.unwrap();

    let mut consumer = Consumer::new(ConsumerConfig::bootstrap([mock.addr.clone()]))
        .await
        .unwrap();
    consumer.assign("t", 0, 0).await.unwrap();
    let ok_before = consumer.metrics().metadata_refresh_ok;
    let fail_before = consumer.metrics().metadata_refresh_fail;

    mock.metadata_fail_once();
    let err = consumer.assign("t2", 0, 0).await.unwrap_err();
    assert!(
        err.to_string().contains("Metadata") || err.to_string().contains("UNKNOWN_SERVER_ERROR"),
        "expected Metadata top-level error, got {err}"
    );
    let m = consumer.metrics();
    assert!(
        m.metadata_refresh_fail > fail_before,
        "failed Metadata refresh must increment metadata_refresh_fail; before={fail_before} after={}",
        m.metadata_refresh_fail
    );
    assert!(
        m.metadata_refresh_ok >= ok_before,
        "prior metadata_refresh_ok must not be wiped"
    );
    consumer.close().await.unwrap();
}

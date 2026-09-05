//! KL-07 slice: surface broker `ThrottleTimeMs` on produce/fetch metrics.
//!
//! Mock brokers inject `throttle_time_ms`; clients must accumulate
//! `broker_throttle_ms_total` so operators can diagnose quota pressure
//! without payload logging. Does not close full KL-07 (no two-user
//! exercise; no instrumentation cost measurement).
#![expect(
    dead_code,
    reason = "tests/common mock helpers are shared; this file uses a subset"
)]
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    reason = "integration-test helpers; clippy.toml allow-*-in-tests covers #[test] only"
)]

mod common;

use partitionline::{Consumer, ProduceRecord, Producer, ProducerConfig};
use std::time::Duration;

#[tokio::test]
async fn produce_metrics_accumulate_broker_throttle_ms() {
    let mock = common::Mock::start().await;
    mock.set_produce_throttle_ms(250);
    let producer = Producer::new(
        ProducerConfig::bootstrap([mock.addr.clone()]).linger(Duration::ZERO),
    )
    .await
    .unwrap();
    let _md = producer
        .send(ProduceRecord::to("t").value(&b"throttle"[..]))
        .await
        .unwrap();
    let m = producer.metrics();
    assert!(
        m.broker_throttle_ms_total >= 250,
        "expected produce throttle ≥ 250, got {}",
        m.broker_throttle_ms_total
    );
    producer.close().await.unwrap();
}

#[tokio::test]
async fn fetch_metrics_accumulate_broker_throttle_ms() {
    let mock = common::Mock::start().await;
    let producer = Producer::new(
        ProducerConfig::bootstrap([mock.addr.clone()]).linger(Duration::ZERO),
    )
    .await
    .unwrap();
    let _md = producer
        .send(ProduceRecord::to("t").value(&b"seed"[..]))
        .await
        .unwrap();
    producer.flush().await.unwrap();
    producer.close().await.unwrap();

    mock.set_fetch_throttle_ms(175);
    let mut consumer = Consumer::connect(mock.addr.clone()).await.unwrap();
    consumer.assign("t", 0, 0).await.unwrap();
    let _recs = consumer.fetch().await.unwrap();
    let m = consumer.metrics();
    assert!(
        m.broker_throttle_ms_total >= 175,
        "expected fetch throttle ≥ 175, got {}",
        m.broker_throttle_ms_total
    );
    consumer.close().await.unwrap();
}

#[tokio::test]
async fn zero_throttle_leaves_metric_at_zero() {
    let mock = common::Mock::start().await;
    let producer = Producer::new(
        ProducerConfig::bootstrap([mock.addr.clone()]).linger(Duration::ZERO),
    )
    .await
    .unwrap();
    let _md = producer
        .send(ProduceRecord::to("t").value(&b"ok"[..]))
        .await
        .unwrap();
    assert_eq!(producer.metrics().broker_throttle_ms_total, 0);
    producer.close().await.unwrap();
}

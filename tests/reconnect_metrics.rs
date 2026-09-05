//! KL-07 slice: surface broker connect/reconnect failures on metrics.
//!
//! Mock brokers that refuse TCP handshakes must bump
//! `broker_reconnect_failures` so operators can diagnose flaps without
//! payload logging. Does not close full KL-07 (no two-user exercise;
//! throttle/lag recipes may live on sibling branches).
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

use partitionline::{Consumer, ConsumerConfig, ProduceRecord, Producer, ProducerConfig};
use std::time::Duration;

#[tokio::test]
async fn produce_metrics_count_refused_connects() {
    let mock = common::Mock::start().await;
    let mut pcfg = ProducerConfig::bootstrap([mock.addr.clone()]);
    pcfg.linger = Duration::ZERO;
    pcfg.connections = 1;
    pcfg.reconnect_backoff = Duration::from_millis(20);
    pcfg.reconnect_backoff_max = Duration::from_millis(20);
    let producer = Producer::new(pcfg).await.unwrap();
    mock.refuse_connections(1);
    let _md = producer
        .send(ProduceRecord::to("t").value(&b"reconnect"[..]))
        .await
        .unwrap();
    let m = producer.metrics();
    assert!(
        m.broker_reconnect_failures >= 1,
        "expected ≥1 produce reconnect failure after refuse_connections, got {}",
        m.broker_reconnect_failures
    );
    producer.close().await.unwrap();
}

#[tokio::test]
async fn fetch_metrics_count_refused_connects() {
    let mock = common::Mock::start().await;
    let mut pcfg = ProducerConfig::bootstrap([mock.addr.clone()]);
    pcfg.linger = Duration::ZERO;
    let producer = Producer::new(pcfg).await.unwrap();
    let _md = producer
        .send(ProduceRecord::to("t").value(&b"seed"[..]))
        .await
        .unwrap();
    producer.close().await.unwrap();

    let mut ccfg = ConsumerConfig::bootstrap([mock.addr.clone()]);
    ccfg.max_wait_ms = 10;
    ccfg.retry_backoff = Duration::ZERO;
    ccfg.reconnect_backoff = Duration::from_millis(20);
    ccfg.reconnect_backoff_max = Duration::from_millis(20);
    let mut consumer = Consumer::new(ccfg).await.unwrap();
    consumer.assign("t", 0, 0).await.unwrap();
    mock.refuse_connections(1);
    let _recs = consumer.fetch().await.unwrap();
    let m = consumer.metrics();
    assert!(
        m.broker_reconnect_failures >= 1,
        "expected ≥1 fetch reconnect failure after refuse_connections, got {}",
        m.broker_reconnect_failures
    );
    consumer.close().await.unwrap();
}

#[tokio::test]
async fn healthy_path_leaves_reconnect_failures_at_zero() {
    let mock = common::Mock::start().await;
    let mut pcfg = ProducerConfig::bootstrap([mock.addr.clone()]);
    pcfg.linger = Duration::ZERO;
    let producer = Producer::new(pcfg).await.unwrap();
    let _md = producer
        .send(ProduceRecord::to("t").value(&b"ok"[..]))
        .await
        .unwrap();
    assert_eq!(producer.metrics().broker_reconnect_failures, 0);
    producer.close().await.unwrap();
}

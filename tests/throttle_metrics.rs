//! KL-07 Partial: broker throttle counters for quota diagnosis.
//!
//! `ProducerMetrics` / `ConsumerMetrics` `{broker_throttles, broker_throttle_ms}`
//! must move when Produce/Fetch responses carry `throttle_time_ms > 0`, so
//! operators can spot broker quotas from telemetry — not payload dumps.
#![expect(
    dead_code,
    reason = "tests/common mock helpers are shared; this file uses a subset"
)]

mod common;

use partitionline::{
    Consumer, ConsumerConfig, ProduceRecord, Producer, ProducerConfig,
};
use std::time::Duration;

#[tokio::test]
async fn throttle_metrics_increment_on_throttled_produce() {
    let mock = common::Mock::start().await;
    mock.set_produce_throttle_ms(250);

    let mut cfg = ProducerConfig::bootstrap([mock.addr.clone()]);
    cfg.linger = Duration::ZERO;
    let producer = Producer::new(cfg).await.unwrap();
    let before = producer.metrics();
    assert_eq!(before.broker_throttles, 0);
    assert_eq!(before.broker_throttle_ms, 0);

    let _md = producer
        .send(ProduceRecord::to("t").value(&b"throttle-1"[..]))
        .await
        .unwrap();

    let after = producer.metrics();
    assert!(
        after.broker_throttles >= 1,
        "expected broker_throttles>=1 after throttled Produce, got {}",
        after.broker_throttles
    );
    assert!(
        after.broker_throttle_ms >= 250,
        "expected broker_throttle_ms>=250, got {}",
        after.broker_throttle_ms
    );
    producer.close().await.unwrap();
}

#[tokio::test]
async fn throttle_metrics_increment_on_throttled_fetch() {
    let mock = common::Mock::start().await;
    let mut pcfg = ProducerConfig::bootstrap([mock.addr.clone()]);
    pcfg.linger = Duration::ZERO;
    let producer = Producer::new(pcfg).await.unwrap();
    let _md = producer
        .send(ProduceRecord::to("t").value(&b"fetch-throttle"[..]))
        .await
        .unwrap();
    producer.close().await.unwrap();

    mock.set_fetch_throttle_ms(180);
    let mut consumer = Consumer::new(
        ConsumerConfig::bootstrap([mock.addr.clone()]).max_wait_ms(10),
    )
    .await
    .unwrap();
    consumer.assign("t", 0, 0).await.unwrap();
    let before = consumer.metrics();
    assert_eq!(before.broker_throttles, 0);

    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    loop {
        let _ = consumer.fetch().await.unwrap();
        let m = consumer.metrics();
        if m.broker_throttles >= 1 {
            assert!(
                m.broker_throttle_ms >= 180,
                "expected broker_throttle_ms>=180, got {}",
                m.broker_throttle_ms
            );
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "timed out waiting for throttled Fetch metrics"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    consumer.close().await.unwrap();
}

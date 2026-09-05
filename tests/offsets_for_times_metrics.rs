//! KL-07 Partial: `offsets_for_times_ok` / `offsets_for_times_fail` on ConsumerMetrics.
//!
//! Timestamp→offset batch lookups must move distinct counters so operators can
//! diagnose seek-by-time storms from telemetry without dumping payloads. Mock
//! honesty only — not two independent human diagnosis runs and not a Suite
//! HOLD lift.
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
    Consumer, ConsumerConfig, ProduceRecord, Producer, ProducerConfig, TopicPartition,
};
use std::time::Duration;

#[tokio::test]
async fn offsets_for_times_ok_metric_increments_on_success() {
    let mock = common::Mock::start().await;
    let producer =
        Producer::new(ProducerConfig::bootstrap([mock.addr.clone()]).linger(Duration::ZERO))
            .await
            .unwrap();
    let _md = producer
        .send(ProduceRecord::to("t").value(&b"oft-ok"[..]).timestamp(1_000))
        .await
        .unwrap();
    producer.close().await.unwrap();

    let mut consumer =
        Consumer::new(ConsumerConfig::bootstrap([mock.addr.clone()]).max_wait_ms(10))
            .await
            .unwrap();
    let tp = TopicPartition::new("t", 0);
    let before = consumer.metrics().offsets_for_times_ok;
    let hit = consumer
        .offsets_for_times_timeout([(tp, 500)], Duration::from_secs(5))
        .await
        .unwrap();
    assert_eq!(hit.len(), 1);
    assert!(hit[0].1.is_some());
    let m = consumer.metrics();
    assert!(
        m.offsets_for_times_ok > before,
        "successful offsets_for_times must increment offsets_for_times_ok; before={before} after={}",
        m.offsets_for_times_ok
    );
    assert_eq!(m.offsets_for_times_fail, 0);
    consumer.close().await.unwrap();
}

#[tokio::test]
async fn offsets_for_times_fail_metric_increments_on_broker_error() {
    let mock = common::Mock::start().await;
    let producer =
        Producer::new(ProducerConfig::bootstrap([mock.addr.clone()]).linger(Duration::ZERO))
            .await
            .unwrap();
    let _md = producer
        .send(ProduceRecord::to("t").value(&b"oft-fail"[..]).timestamp(1_000))
        .await
        .unwrap();
    producer.close().await.unwrap();

    let mut consumer =
        Consumer::new(ConsumerConfig::bootstrap([mock.addr.clone()]).max_wait_ms(10))
            .await
            .unwrap();
    let tp = TopicPartition::new("t", 0);
    let ok_before = consumer.metrics().offsets_for_times_ok;
    let fail_before = consumer.metrics().offsets_for_times_fail;

    mock.list_offsets_fail_once();
    let err = consumer
        .offsets_for_times_timeout([(tp, 500)], Duration::from_secs(5))
        .await
        .unwrap_err();
    assert!(
        err.to_string().contains("OFFSET_OUT_OF_RANGE")
            || err.to_string().contains("ListOffsets")
            || err.to_string().contains("list_offsets"),
        "expected ListOffsets partition error via offsets_for_times, got {err}"
    );
    let m = consumer.metrics();
    assert!(
        m.offsets_for_times_fail > fail_before,
        "failed offsets_for_times must increment offsets_for_times_fail; before={fail_before} after={}",
        m.offsets_for_times_fail
    );
    assert!(
        m.offsets_for_times_ok >= ok_before,
        "prior offsets_for_times_ok must not be wiped"
    );
    consumer.close().await.unwrap();
}

#[tokio::test]
async fn offsets_for_times_fail_metric_increments_on_negative_timestamp() {
    let mock = common::Mock::start().await;
    let mut consumer =
        Consumer::new(ConsumerConfig::bootstrap([mock.addr.clone()]).max_wait_ms(10))
            .await
            .unwrap();
    let tp = TopicPartition::new("t", 0);
    let fail_before = consumer.metrics().offsets_for_times_fail;
    let err = consumer
        .offsets_for_times([(tp, -1)])
        .await
        .unwrap_err();
    assert!(
        err.to_string().contains("cannot be negative"),
        "expected negative timestamp rejection, got {err}"
    );
    let m = consumer.metrics();
    assert!(
        m.offsets_for_times_fail > fail_before,
        "validation failure must increment offsets_for_times_fail; before={fail_before} after={}",
        m.offsets_for_times_fail
    );
    consumer.close().await.unwrap();
}

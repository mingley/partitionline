//! KL-07 Partial: `ConsumerMetrics::list_offsets_ok` / `list_offsets_fail`.
//!
//! Terminal ListOffsets successes and failures must move distinct counters so
//! operators can diagnose seek/lag/reset storms from telemetry rather than
//! payload logs. Retries inside the ListOffsets loop must not count as fail.
//! Mock honesty only — not two independent human diagnosis runs and not a
//! Suite HOLD lift.
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
    Consumer, ConsumerConfig, ProduceRecord, Producer, ProducerConfig, EARLIEST_TIMESTAMP,
};
use std::time::Duration;

#[tokio::test]
async fn list_offsets_ok_increments_on_list_offsets() {
    let mock = common::Mock::start().await;
    let producer =
        Producer::new(ProducerConfig::bootstrap([mock.addr.clone()]).linger(Duration::ZERO))
            .await
            .unwrap();
    producer
        .send(ProduceRecord::to("t").value(&b"lo-ok"[..]))
        .await
        .unwrap();
    producer.close().await.unwrap();

    let mut consumer = Consumer::new(ConsumerConfig::bootstrap([mock.addr.clone()]))
        .await
        .unwrap();
    consumer.assign("t", 0, 0).await.unwrap();
    let before = consumer.metrics().list_offsets_ok;
    let offset = consumer
        .list_offsets("t", 0, EARLIEST_TIMESTAMP)
        .await
        .unwrap();
    assert_eq!(offset, 0);
    let m = consumer.metrics();
    assert!(
        m.list_offsets_ok > before,
        "successful ListOffsets must increment list_offsets_ok; before={before} after={}",
        m.list_offsets_ok
    );
    assert_eq!(m.list_offsets_fail, 0);
    consumer.close().await.unwrap();
}

#[tokio::test]
async fn list_offsets_fail_increments_on_broker_error() {
    let mock = common::Mock::start().await;
    let producer =
        Producer::new(ProducerConfig::bootstrap([mock.addr.clone()]).linger(Duration::ZERO))
            .await
            .unwrap();
    producer
        .send(ProduceRecord::to("t").value(&b"lo-fail"[..]))
        .await
        .unwrap();
    producer.close().await.unwrap();

    let mut consumer = Consumer::new(ConsumerConfig::bootstrap([mock.addr.clone()]))
        .await
        .unwrap();
    consumer.assign("t", 0, 0).await.unwrap();
    let ok_before = consumer.metrics().list_offsets_ok;
    let fail_before = consumer.metrics().list_offsets_fail;

    mock.list_offsets_fail_once();
    let err = consumer
        .list_offsets("t", 0, EARLIEST_TIMESTAMP)
        .await
        .unwrap_err();
    assert!(
        err.to_string().contains("OFFSET_OUT_OF_RANGE") || err.to_string().contains("ListOffsets"),
        "expected ListOffsets partition error, got {err}"
    );
    let m = consumer.metrics();
    assert!(
        m.list_offsets_fail > fail_before,
        "failed ListOffsets must increment list_offsets_fail; before={fail_before} after={}",
        m.list_offsets_fail
    );
    assert!(
        m.list_offsets_ok >= ok_before,
        "prior list_offsets_ok must not be wiped"
    );
    consumer.close().await.unwrap();
}

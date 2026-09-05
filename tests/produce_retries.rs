//! KL-07 Partial: `produce_retries` counts retriable Produce re-queues.
//!
//! After a retriable broker error, the client enqueues another Produce attempt.
//! `metrics().produce_retries` must climb so operators can diagnose flaky
//! leadership / transient errors from telemetry rather than payload logs.
//! Mock honesty only — not two independent human diagnosis runs and not a
//! Suite HOLD lift.
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

use partitionline::error;
use partitionline::{ProduceRecord, Producer, ProducerConfig};
use std::time::Duration;

#[tokio::test]
async fn produce_retries_metric_increments_on_retriable_broker_error() {
    let mock = common::Mock::start().await;
    mock.set_produce_error_times(error::LEADER_NOT_AVAILABLE, 1);
    let mut pcfg = ProducerConfig::bootstrap([mock.addr.clone()]);
    pcfg.linger = Duration::ZERO;
    pcfg.retry_backoff = Duration::ZERO;
    let producer = Producer::new(pcfg).await.unwrap();

    assert_eq!(producer.metrics().produce_retries, 0);

    let md = producer
        .send(ProduceRecord::to("t").value(&b"retry-metric"[..]))
        .await
        .unwrap();
    assert_eq!(md.offset, 0);

    let m = producer.metrics();
    assert!(
        m.produce_retries >= 1,
        "retriable LEADER_NOT_AVAILABLE must increment produce_retries; got {}",
        m.produce_retries
    );
    assert_eq!(m.records_acked, 1);
    assert_eq!(
        m.produce_errors, 0,
        "successful retry must not count as a terminal produce_error"
    );
    producer.close().await.unwrap();
}

#[tokio::test]
async fn produce_retries_stay_zero_on_clean_path() {
    let mock = common::Mock::start().await;
    let mut pcfg = ProducerConfig::bootstrap([mock.addr.clone()]);
    pcfg.linger = Duration::ZERO;
    let producer = Producer::new(pcfg).await.unwrap();
    let _md = producer
        .send(ProduceRecord::to("t").value(&b"clean"[..]))
        .await
        .unwrap();
    assert_eq!(
        producer.metrics().produce_retries,
        0,
        "clean produce path must not invent retries"
    );
    producer.close().await.unwrap();
}

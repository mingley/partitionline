//! KL-07 Partial: stale-leader diagnosis via tip-shipped produce metrics.
//!
//! When Produce keeps returning `NOT_LEADER_OR_FOLLOWER` until delivery
//! timeout, `metrics().produce_errors` must rise so operators can diagnose a
//! stale leader / fence without logging record payloads. Distinguishes from a
//! healthy path where `ack_latency` samples and `produce_errors` stay at 0.
//! Does **not** close full KL-07 (no two independent human diagnosis runs;
//! throttle/queue-age/reconnect recipes may land separately). Does **not**
//! lift Suite HOLD.
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
async fn healthy_produce_records_ack_latency_without_errors() {
    let mock = common::Mock::start().await;
    let mut pcfg = ProducerConfig::bootstrap([mock.addr.clone()]);
    pcfg.linger = Duration::ZERO;
    let producer = Producer::new(pcfg).await.unwrap();
    let _md = producer
        .send(ProduceRecord::to("t").value(&b"healthy"[..]))
        .await
        .unwrap();
    let m = producer.metrics();
    assert_eq!(m.produce_errors, 0, "healthy produce must not count errors");
    assert!(
        m.ack_latency.count >= 1,
        "healthy produce must sample ack_latency (got count={})",
        m.ack_latency.count
    );
    producer.close().await.unwrap();
}

#[tokio::test]
async fn not_leader_exhaustion_surfaces_produce_errors_for_diagnosis() {
    let mock = common::Mock::start().await;
    // Permanent stale-leader signal until delivery timeout fails the send.
    mock.set_produce_error(error::NOT_LEADER_OR_FOLLOWER);
    let mut pcfg = ProducerConfig::bootstrap([mock.addr.clone()]);
    pcfg.linger = Duration::ZERO;
    pcfg.retry_backoff = Duration::from_millis(5);
    pcfg.retry_backoff_max = Duration::from_millis(5);
    pcfg.delivery_timeout = Duration::from_millis(80);
    let producer = Producer::new(pcfg).await.unwrap();
    let err = producer
        .send(ProduceRecord::to("t").value(&b"stale-leader"[..]))
        .await
        .expect_err("permanent NOT_LEADER must fail within delivery_timeout");
    assert!(
        err.to_string().to_ascii_lowercase().contains("timeout")
            || err.to_string().contains("NOT_LEADER")
            || err.to_string().contains("not leader"),
        "expected timeout or not-leader failure, got {err}"
    );
    let m = producer.metrics();
    assert!(
        m.produce_errors >= 1,
        "stale-leader exhaustion must bump produce_errors for telemetry diagnosis, got {}",
        m.produce_errors
    );
    // Prefer metrics over payload dumps — value body never needed for this assert.
    producer.close().await.unwrap();
}

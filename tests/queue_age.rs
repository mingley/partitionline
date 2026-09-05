//! KL-07 Partial: producer `queue_latency` surfaces linger/queue age.
//!
//! Accept→batch-drain latency is distinct from accept→ack. With a non-zero
//! linger, `metrics().queue_latency` must reflect time spent in the local
//! queue before the produce worker drains onto the wire — telemetry for
//! backlog diagnosis without payload logging. Mock honesty only; not two
//! independent human diagnosis runs and not a Suite HOLD lift.
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

use partitionline::{ProduceRecord, Producer, ProducerConfig};
use std::time::Duration;

#[tokio::test]
async fn queue_latency_reflects_linger_before_batch_drain() {
    let mock = common::Mock::start().await;
    let producer = Producer::new(
        ProducerConfig::bootstrap([mock.addr.clone()]).linger(Duration::from_millis(40)),
    )
    .await
    .unwrap();

    let md = producer
        .send(ProduceRecord::to("t").value(&b"queued"[..]))
        .await
        .unwrap();
    assert_eq!(md.offset, 0);

    let m = producer.metrics();
    assert!(
        m.queue_latency.count >= 1,
        "queue_latency must record accept→batch drain samples"
    );
    assert!(
        m.queue_latency.max_nanos >= 20_000_000,
        "with linger=40ms, max queue_latency should be tens of ms; got max_nanos={}",
        m.queue_latency.max_nanos
    );
    assert!(
        m.ack_latency.count >= 1,
        "ack_latency remains a separate accept→ack series"
    );
    producer.close().await.unwrap();
}

#[tokio::test]
async fn queue_latency_stays_near_zero_with_linger_zero() {
    let mock = common::Mock::start().await;
    let producer = Producer::new(
        ProducerConfig::bootstrap([mock.addr.clone()]).linger(Duration::ZERO),
    )
    .await
    .unwrap();
    let _md = producer
        .send(ProduceRecord::to("t").value(&b"fast"[..]))
        .await
        .unwrap();
    let m = producer.metrics();
    assert!(m.queue_latency.count >= 1);
    // Local drain without linger should be well under 20ms on the mock.
    assert!(
        m.queue_latency.p99_nanos < 20_000_000,
        "linger=0 queue_latency p99 should stay small; got {}",
        m.queue_latency.p99_nanos
    );
    producer.close().await.unwrap();
}

//! KL-07 Partial: diagnose an idle consumer via empty `assignment()`.
//!
//! When produce continues but fetch fails or looks idle, check
//! `Consumer::assignment().is_empty()` (and the Java-shaped protocol error)
//! before logging record bodies. Mock-broker honesty only — **not** two
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

use partitionline::{
    Consumer, ConsumerConfig, ProduceRecord, Producer, ProducerConfig,
};
use std::time::Duration;

#[tokio::test]
async fn empty_assignment_diagnoses_idle_consumer_without_payload_logging() {
    let mock = common::Mock::start().await;
    let java = "Consumer is not subscribed to any topics or assigned any partitions";

    // Produce continues while the consumer has no assignment (idle / misconfigured).
    let producer =
        Producer::new(ProducerConfig::bootstrap([mock.addr.clone()]).linger(Duration::ZERO))
            .await
            .unwrap();
    let _mds = producer
        .send_all([
            ProduceRecord::to("t").value(&b"idle-1"[..]),
            ProduceRecord::to("t").value(&b"idle-2"[..]),
        ])
        .await
        .unwrap();
    producer.close().await.unwrap();

    let mut consumer =
        Consumer::new(ConsumerConfig::bootstrap([mock.addr.clone()]).max_wait_ms(10))
            .await
            .unwrap();

    // Operator signal: empty assignment — diagnose before inspecting payloads.
    assert!(
        consumer.assignment().is_empty(),
        "idle consumer must report empty assignment()"
    );
    let err = consumer.fetch().await.unwrap_err().to_string();
    assert!(
        err.contains(java),
        "fetch without assignment must fail closed with Java-shaped error, got {err}"
    );

    // After assign, telemetry shows non-empty assignment and fetch returns records
    // without needing payload dumps for the diagnosis path.
    consumer.assign("t", 0, 0).await.unwrap();
    assert_eq!(consumer.assignment().len(), 1);
    let recs = consumer.fetch().await.unwrap();
    assert_eq!(recs.len(), 2, "assigned fetch must observe produced records");
    assert!(
        !consumer.assignment().is_empty(),
        "assignment() stays non-empty after successful fetch"
    );
    consumer.close().await.unwrap();
}

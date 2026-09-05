//! KL-07 Partial: consumer lag diagnosis via `current_lag` (not payload dumps).
//!
//! Seeds known produce counts, asserts `current_lag == hw − position` before
//! and after fetch, so operators can diagnose a blocked/behind consumer from
//! telemetry rather than logging record bodies. Mock-broker honesty only —
//! **not** two independent human diagnosis runs and not a Suite HOLD lift.
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
    Consumer, ConsumerConfig, ProduceRecord, Producer, ProducerConfig, TopicPartition,
};
use std::time::Duration;

#[tokio::test]
async fn current_lag_diagnoses_behind_then_caught_up_consumer() {
    let mock = common::Mock::start().await;
    let producer = Producer::new(
        ProducerConfig::bootstrap([mock.addr.clone()]).linger(Duration::ZERO),
    )
    .await
    .unwrap();
    let _mds = producer
        .send_all([
            ProduceRecord::to("t").value(&b"lag-1"[..]),
            ProduceRecord::to("t").value(&b"lag-2"[..]),
            ProduceRecord::to("t").value(&b"lag-3"[..]),
        ])
        .await
        .unwrap();
    producer.close().await.unwrap();

    let mut consumer = Consumer::new(
        ConsumerConfig::bootstrap([mock.addr.clone()]).max_wait_ms(10),
    )
    .await
    .unwrap();
    consumer.assign("t", 0, 0).await.unwrap();

    // Behind: position 0, hw 3 → lag 3 (diagnose before reading payloads).
    assert_eq!(
        consumer
            .current_lag_timeout(("t", 0), Duration::from_secs(5))
            .await
            .unwrap(),
        Some(3),
        "lag must equal hw − position before fetch"
    );

    let recs = consumer.fetch().await.unwrap();
    assert_eq!(recs.len(), 3);

    // Caught up: lag 0 without inspecting record bodies.
    assert_eq!(
        consumer
            .current_lag(TopicPartition::new("t", 0))
            .await
            .unwrap(),
        Some(0),
        "lag must return to 0 after catching up"
    );
    consumer.close().await.unwrap();
}

//! KL-07 Partial: diagnose a blocked consumer via `paused()` + `current_lag`.
//!
//! When fetch looks idle while produce continues, check `Consumer::paused`
//! and partition lag from telemetry — not record bodies. Mock-broker honesty
//! only. Does **not** close two independent human diagnosis runs. Does **not**
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

use partitionline::{
    Admin, AdminConfig, Consumer, ConsumerConfig, NewTopic, ProduceRecord, Producer,
    ProducerConfig, TopicPartition,
};
use std::time::Duration;

#[tokio::test]
async fn paused_partition_diagnoses_blocked_consumer_without_payload_logging() {
    let mock = common::Mock::start().await;
    let mut admin = Admin::new(AdminConfig::bootstrap([mock.addr.clone()]))
        .await
        .unwrap();
    assert_eq!(
        admin
            .create_topics(&[NewTopic::new("pause-diag", 2, 1)], 10_000, false)
            .await
            .unwrap()[0]
            .error_code,
        0
    );

    let producer =
        Producer::new(ProducerConfig::bootstrap([mock.addr.clone()]).linger(Duration::ZERO))
            .await
            .unwrap();
    let _mds = producer
        .send_all([
            ProduceRecord::to("pause-diag").partition(0).value(&b"a0"[..]),
            ProduceRecord::to("pause-diag").partition(1).value(&b"a1"[..]),
        ])
        .await
        .unwrap();
    producer.close().await.unwrap();

    let mut consumer =
        Consumer::new(ConsumerConfig::bootstrap([mock.addr.clone()]).max_wait_ms(10))
            .await
            .unwrap();
    consumer.assign("pause-diag", 0, 0).await.unwrap();
    consumer.assign("pause-diag", 1, 0).await.unwrap();

    // Both partitions behind before any fetch.
    assert_eq!(
        consumer
            .current_lag_timeout(("pause-diag", 0), Duration::from_secs(5))
            .await
            .unwrap(),
        Some(1)
    );
    assert_eq!(
        consumer
            .current_lag_timeout(("pause-diag", 1), Duration::from_secs(5))
            .await
            .unwrap(),
        Some(1)
    );

    // Operator signal: pause one assigned partition (telemetry, not payloads).
    consumer.pause([TopicPartition::new("pause-diag", 1)]);
    assert_eq!(
        consumer.paused(),
        vec![TopicPartition::new("pause-diag", 1)],
        "paused() must surface the blocked partition"
    );

    let recs = consumer.fetch().await.unwrap();
    assert_eq!(recs.len(), 1, "paused partition must be skipped");
    assert_eq!(recs[0].partition, 0);

    // Unpaused partition caught up; paused partition still lags — diagnose via
    // paused() + current_lag, without inspecting record bodies.
    assert_eq!(
        consumer
            .current_lag(TopicPartition::new("pause-diag", 0))
            .await
            .unwrap(),
        Some(0)
    );
    assert_eq!(
        consumer
            .current_lag(TopicPartition::new("pause-diag", 1))
            .await
            .unwrap(),
        Some(1),
        "paused partition lag stays high until resume"
    );
    assert_eq!(
        consumer.paused(),
        vec![TopicPartition::new("pause-diag", 1)]
    );

    consumer.resume([("pause-diag", 1)]);
    assert!(consumer.paused().is_empty());
    let recs = consumer.fetch().await.unwrap();
    assert_eq!(recs.len(), 1);
    assert_eq!(recs[0].partition, 1);
    assert_eq!(
        consumer
            .current_lag(TopicPartition::new("pause-diag", 1))
            .await
            .unwrap(),
        Some(0),
        "after resume, lag returns to 0"
    );
    consumer.close().await.unwrap();
}

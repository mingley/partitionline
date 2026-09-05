//! KL-07 Partial: `ShareMetrics::acknowledge_errors` for ShareAcknowledge diagnosis.
//!
//! Terminal ShareAcknowledge failures must increment a counter distinct from
//! `records_acknowledged` so operators can diagnose share ack health from
//! telemetry rather than payload logs. Mock honesty only — not two independent
//! human diagnosis runs and not a Suite HOLD lift.
#![expect(
    dead_code,
    reason = "tests/common mock helpers are shared; this file uses a subset"
)]
#![expect(
    unused_results,
    reason = "tests often discard RecordMetadata"
)]
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    reason = "integration-test helpers; clippy.toml allow-*-in-tests covers #[test] only"
)]

mod common;

use partitionline::{
    AcknowledgeType, ConsumerConfig, ProduceRecord, Producer, ProducerConfig, ShareGroup,
    ShareRecord, TimestampType,
};
use std::time::Duration;

#[tokio::test]
async fn acknowledge_errors_metric_increments_on_ack_before_poll() {
    let mock = common::Mock::start().await;
    let mut group = ShareGroup::join(
        ConsumerConfig::bootstrap([mock.addr.clone()]).max_wait_ms(10),
        "ack-err",
        "t",
    )
    .await
    .unwrap();

    assert_eq!(group.metrics().acknowledge_errors, 0);
    assert_eq!(group.metrics().records_acknowledged, 0);

    let rec = ShareRecord {
        topic: "t".into(),
        partition: 0,
        offset: 0,
        timestamp: 0,
        timestamp_type: TimestampType::CreateTime,
        key: None,
        value: None,
        headers: Vec::new(),
        delivery_count: 1,
        leader_epoch: None,
    };
    let err = group
        .acknowledge(std::slice::from_ref(&rec), AcknowledgeType::Accept)
        .await
        .unwrap_err();
    assert!(
        err.to_string().contains("Acknowledge called before poll")
            || err.to_string().contains("ShareAcknowledge")
            || err.to_string().contains("share session"),
        "expected pre-poll ShareAcknowledge failure, got {err}"
    );

    let m = group.metrics();
    assert!(
        m.acknowledge_errors >= 1,
        "terminal ShareAcknowledge failure must increment acknowledge_errors; got {}",
        m.acknowledge_errors
    );
    assert_eq!(
        m.records_acknowledged, 0,
        "failed ack must not invent records_acknowledged"
    );
    group.leave().await.unwrap();
}

#[tokio::test]
async fn acknowledge_errors_stay_zero_on_successful_accept() {
    let mock = common::Mock::start().await;
    let producer =
        Producer::new(ProducerConfig::bootstrap([mock.addr.clone()]).linger(Duration::ZERO))
            .await
            .unwrap();
    producer
        .send(ProduceRecord::to("t").value(&b"share-ack-ok"[..]))
        .await
        .unwrap();
    producer.close().await.unwrap();

    let mut group = ShareGroup::join(
        ConsumerConfig::bootstrap([mock.addr.clone()]).max_wait_ms(10),
        "ack-ok",
        "t",
    )
    .await
    .unwrap();
    let recs = group.poll().await.unwrap();
    assert!(!recs.is_empty());
    group.accept(&recs).await.unwrap();

    let m = group.metrics();
    assert!(m.records_acknowledged >= 1);
    assert_eq!(
        m.acknowledge_errors, 0,
        "successful accept must not invent acknowledge_errors"
    );
    group.leave().await.unwrap();
}

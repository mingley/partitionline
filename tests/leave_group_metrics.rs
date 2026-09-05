//! KL-07 Partial: `ConsumerMetrics::leave_ok` / `leave_fail`.
//!
//! Classic LeaveGroup successes and terminal failures must move distinct
//! counters so operators can diagnose unclean leave / fenced shutdown from
//! telemetry rather than payload logs. Mock honesty only — not two independent
//! human diagnosis runs and not a Suite HOLD lift.
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

use partitionline::{ConsumerConfig, ConsumerGroup, ProduceRecord, Producer, ProducerConfig};
use std::time::Duration;

#[tokio::test]
async fn leave_ok_increments_on_successful_unsubscribe() {
    let mock = common::Mock::start().await;
    let producer =
        Producer::new(ProducerConfig::bootstrap([mock.addr.clone()]).linger(Duration::ZERO))
            .await
            .unwrap();
    producer
        .send(ProduceRecord::to("t").value(&b"leave-ok"[..]))
        .await
        .unwrap();
    producer.close().await.unwrap();

    let mut ccfg = ConsumerConfig::bootstrap([mock.addr.clone()]);
    ccfg.max_wait_ms = 10;
    let mut group = ConsumerGroup::join(ccfg, "leave-ok", "t").await.unwrap();
    assert_eq!(group.metrics().leave_ok, 0);
    assert_eq!(group.metrics().leave_fail, 0);

    group.unsubscribe().await.unwrap();
    let m = group.metrics();
    assert!(
        m.leave_ok >= 1,
        "successful LeaveGroup must increment leave_ok; got {}",
        m.leave_ok
    );
    assert_eq!(m.leave_fail, 0);
    group.leave().await.unwrap();
}

#[tokio::test]
async fn leave_fail_increments_on_broker_error() {
    let mock = common::Mock::start().await;
    let mut ccfg = ConsumerConfig::bootstrap([mock.addr.clone()]);
    ccfg.max_wait_ms = 10;
    let mut group = ConsumerGroup::join(ccfg, "leave-fail", "t").await.unwrap();
    let fail_before = group.metrics().leave_fail;

    mock.leave_group_fail_once();
    let err = group.unsubscribe().await.unwrap_err();
    assert!(
        err.to_string().contains("UNKNOWN_MEMBER_ID") || err.to_string().contains("LeaveGroup"),
        "expected LeaveGroup broker error, got {err}"
    );
    let m = group.metrics();
    assert!(
        m.leave_fail > fail_before,
        "failed LeaveGroup must increment leave_fail; before={fail_before} after={}",
        m.leave_fail
    );
    // Allow a clean leave after the injected fail is consumed.
    let _ = group.leave().await;
}

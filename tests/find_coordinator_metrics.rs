//! KL-07 Partial: `ConsumerMetrics::find_coordinator_ok` / `find_coordinator_fail`.
//!
//! FindCoordinator successes and terminal failures must move distinct counters
//! so operators can diagnose coordinator moves / discovery storms from
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
async fn find_coordinator_ok_increments_on_successful_join() {
    let mock = common::Mock::start().await;
    let producer =
        Producer::new(ProducerConfig::bootstrap([mock.addr.clone()]).linger(Duration::ZERO))
            .await
            .unwrap();
    producer
        .send(ProduceRecord::to("t").value(&b"fc-ok"[..]))
        .await
        .unwrap();
    producer.close().await.unwrap();

    let mut ccfg = ConsumerConfig::bootstrap([mock.addr.clone()]);
    ccfg.max_wait_ms = 10;
    let group = ConsumerGroup::join(ccfg, "fc-ok", "t").await.unwrap();
    let m = group.metrics();
    assert!(
        m.find_coordinator_ok >= 1,
        "successful FindCoordinator must increment find_coordinator_ok; got {}",
        m.find_coordinator_ok
    );
    assert_eq!(
        m.find_coordinator_fail, 0,
        "healthy join must not invent find_coordinator_fail"
    );
    group.leave().await.unwrap();
}

#[tokio::test]
async fn find_coordinator_fail_increments_on_rediscovery_error() {
    let mock = common::Mock::start_two_node().await;
    let producer =
        Producer::new(ProducerConfig::bootstrap([mock.addr.clone()]).linger(Duration::ZERO))
            .await
            .unwrap();
    producer
        .send(ProduceRecord::to("t").value(&b"fc-fail"[..]))
        .await
        .unwrap();
    producer.close().await.unwrap();

    let mut ccfg = ConsumerConfig::bootstrap([mock.addr.clone()]);
    ccfg.max_wait_ms = 10;
    ccfg.heartbeat_interval = Duration::from_millis(20);
    let group = ConsumerGroup::join(ccfg, "fc-fail", "t").await.unwrap();
    assert!(group.metrics().find_coordinator_ok >= 1);
    let fail_before = group.metrics().find_coordinator_fail;

    // Exhaust FindCoordinator while forcing the member off its coordinator so
    // the heartbeat task rediscovers and records a terminal fail.
    mock.find_coordinator_fail_n(64);
    mock.move_coordinator();

    common::wait_pred("FindCoordinator fail metric after coordinator move", || {
        group.metrics().find_coordinator_fail > fail_before
    })
    .await;

    let m = group.metrics();
    assert!(
        m.find_coordinator_fail > fail_before,
        "failed rediscovery must increment find_coordinator_fail; before={fail_before} after={}",
        m.find_coordinator_fail
    );
    let _ = group.leave().await;
}

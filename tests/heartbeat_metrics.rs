//! KL-07 Partial: `ConsumerMetrics::heartbeat_ok` / `heartbeat_fail`.
//!
//! Classic Heartbeat successes and terminal failures must move distinct
//! counters so operators can diagnose blocked / fenced group members from
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

use partitionline::{ConsumerConfig, ConsumerGroup, ProduceRecord, Producer, ProducerConfig};
use std::time::Duration;

#[tokio::test]
async fn heartbeat_ok_metric_increments_after_join() {
    let mock = common::Mock::start().await;
    let producer =
        Producer::new(ProducerConfig::bootstrap([mock.addr.clone()]).linger(Duration::ZERO))
            .await
            .unwrap();
    producer
        .send(ProduceRecord::to("t").value(&b"hb-ok"[..]))
        .await
        .unwrap();
    producer.close().await.unwrap();

    let mut ccfg = ConsumerConfig::bootstrap([mock.addr.clone()]);
    ccfg.max_wait_ms = 10;
    ccfg.heartbeat_interval = Duration::from_millis(20);
    let group = ConsumerGroup::join(ccfg, "hb-ok", "t").await.unwrap();
    assert_eq!(group.metrics().heartbeat_ok, 0);
    assert_eq!(group.metrics().heartbeat_fail, 0);

    common::wait_pred("classic Heartbeat after join", || {
        mock.heartbeat_total("hb-ok") >= 1
    })
    .await;

    // Give the client task a beat to record the metric after the mock counts the RPC.
    tokio::time::sleep(Duration::from_millis(50)).await;
    let m = group.metrics();
    assert!(
        m.heartbeat_ok >= 1,
        "successful Heartbeat must increment heartbeat_ok; got {}",
        m.heartbeat_ok
    );
    assert_eq!(
        m.heartbeat_fail, 0,
        "healthy Heartbeat path must not invent heartbeat_fail"
    );
    group.leave().await.unwrap();
}

#[tokio::test]
async fn heartbeat_fail_metric_increments_on_broker_error() {
    let mock = common::Mock::start().await;
    let mut ccfg = ConsumerConfig::bootstrap([mock.addr.clone()]);
    ccfg.max_wait_ms = 10;
    ccfg.heartbeat_interval = Duration::from_millis(20);
    let group = ConsumerGroup::join(ccfg, "hb-fail", "t").await.unwrap();

    common::wait_pred("first Heartbeat before fail inject", || {
        mock.heartbeat_total("hb-fail") >= 1
    })
    .await;
    tokio::time::sleep(Duration::from_millis(30)).await;
    let ok_before = group.metrics().heartbeat_ok;

    mock.heartbeat_fail_once();
    common::wait_pred("Heartbeat after fail inject", || {
        mock.heartbeat_total("hb-fail") >= 2
    })
    .await;
    tokio::time::sleep(Duration::from_millis(50)).await;

    let m = group.metrics();
    assert!(
        m.heartbeat_fail >= 1,
        "ILLEGAL_GENERATION Heartbeat must increment heartbeat_fail; got {}",
        m.heartbeat_fail
    );
    assert!(
        m.heartbeat_ok >= ok_before,
        "prior heartbeat_ok must not be wiped; before={ok_before} after={}",
        m.heartbeat_ok
    );
    group.leave().await.unwrap();
}

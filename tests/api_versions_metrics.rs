//! KL-07 Partial: `api_versions_ok` / `api_versions_fail` on ConsumerMetrics.
//!
//! ApiVersions success and terminal failure must move distinct counters so
//! operators can diagnose broker capability / version-skew from telemetry
//! without dumping payloads. Mock honesty only — not two independent human
//! diagnosis runs and not a Suite HOLD lift.
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

use partitionline::{Consumer, ConsumerConfig};
use std::time::Duration;

#[tokio::test]
async fn api_versions_ok_metric_increments_on_connect() {
    let mock = common::Mock::start().await;
    let mut cfg = ConsumerConfig::bootstrap([mock.addr.clone()]);
    cfg.request_timeout = Duration::from_secs(5);
    let consumer = Consumer::new(cfg).await.unwrap();
    let m = consumer.metrics();
    assert!(
        m.api_versions_ok >= 1,
        "successful ApiVersions must increment api_versions_ok; got {}",
        m.api_versions_ok
    );
    assert_eq!(
        m.api_versions_fail, 0,
        "successful negotiate must not invent api_versions_fail"
    );
    consumer.close().await.unwrap();
}

#[tokio::test]
async fn api_versions_fail_metric_increments_on_reconnect_reject() {
    let mock = common::Mock::start().await;
    let mut cfg = ConsumerConfig::bootstrap([mock.addr.clone()]);
    cfg.request_timeout = Duration::from_secs(5);
    cfg.max_wait_ms = 10;
    let mut consumer = Consumer::new(cfg).await.unwrap();
    consumer.assign("t", 0, 0).await.unwrap();
    let ok_before = consumer.metrics().api_versions_ok;
    let fail_before = consumer.metrics().api_versions_fail;
    assert!(ok_before >= 1);

    mock.api_versions_fail_once();
    mock.drop_connections();
    // Fetch forces node reconnect + ApiVersions against the injected failure.
    let err = consumer.fetch().await.unwrap_err();
    assert!(
        err.to_string().contains("ApiVersions")
            || err.to_string().contains("INVALID_REQUEST")
            || err.to_string().contains("42"),
        "expected ApiVersions failure after reconnect, got {err}"
    );

    let m = consumer.metrics();
    assert!(
        m.api_versions_fail > fail_before,
        "failed ApiVersions must increment api_versions_fail; before={fail_before} after={}",
        m.api_versions_fail
    );
    assert!(
        m.api_versions_ok >= ok_before,
        "prior api_versions_ok must not be wiped"
    );
    // Consumer may be unusable after failed reconnect; drop without close.
    drop(consumer);
}

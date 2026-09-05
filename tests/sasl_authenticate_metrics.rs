//! KL-07 Partial: `sasl_authenticate_ok` / `sasl_authenticate_fail` on ConsumerMetrics.
//!
//! SASL authenticate success and terminal failure must move distinct counters so
//! operators can diagnose auth health from telemetry without dumping credentials.
//! Mock honesty only — not two independent human diagnosis runs and not a Suite
//! HOLD lift.
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
async fn sasl_authenticate_ok_metric_increments_on_connect() {
    let mock = common::Mock::start_with_sasl(Some(("alice".into(), "secret".into()))).await;
    let mut cfg = ConsumerConfig::bootstrap([mock.addr.clone()]);
    cfg.request_timeout = Duration::from_secs(5);
    cfg.sasl_plain = Some(("alice".into(), "secret".into()));
    let consumer = Consumer::new(cfg).await.unwrap();
    let m = consumer.metrics();
    assert!(
        m.sasl_authenticate_ok >= 1,
        "successful SASL authenticate must increment sasl_authenticate_ok; got {}",
        m.sasl_authenticate_ok
    );
    assert_eq!(
        m.sasl_authenticate_fail, 0,
        "successful auth must not invent sasl_authenticate_fail"
    );
    consumer.close().await.unwrap();
}

#[tokio::test]
async fn sasl_authenticate_fail_metric_increments_on_reconnect_reject() {
    let mock = common::Mock::start_with_sasl(Some(("alice".into(), "secret".into()))).await;
    let mut cfg = ConsumerConfig::bootstrap([mock.addr.clone()]);
    cfg.request_timeout = Duration::from_secs(5);
    cfg.max_wait_ms = 10;
    cfg.sasl_plain = Some(("alice".into(), "secret".into()));
    let mut consumer = Consumer::new(cfg).await.unwrap();
    consumer.assign("t", 0, 0).await.unwrap();
    let ok_before = consumer.metrics().sasl_authenticate_ok;
    let fail_before = consumer.metrics().sasl_authenticate_fail;
    assert!(ok_before >= 1);

    mock.sasl_authenticate_fail_once();
    mock.drop_connections();
    // Fetch forces node reconnect + SASL re-auth against the injected failure.
    let err = consumer.fetch().await.unwrap_err();
    assert!(
        err.to_string().contains("SASL")
            || err.to_string().contains("AUTHENTICATION")
            || err.to_string().contains("58")
            || err.to_string().contains("injected"),
        "expected SASL authenticate failure after reconnect, got {err}"
    );

    let m = consumer.metrics();
    assert!(
        m.sasl_authenticate_fail > fail_before,
        "failed SASL authenticate must increment sasl_authenticate_fail; before={fail_before} after={}",
        m.sasl_authenticate_fail
    );
    assert!(
        m.sasl_authenticate_ok >= ok_before,
        "prior sasl_authenticate_ok must not be wiped"
    );
    // Consumer may be unusable after failed reauth; drop without close.
    drop(consumer);
}

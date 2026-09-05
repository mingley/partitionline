//! KL-07 Partial: `wakeups_signaled` / `wakeups_consumed` on ConsumerMetrics.
//!
//! Wakeup signal and consume must move distinct counters so operators can
//! diagnose blocked poll loops from telemetry without dumping stacks. Mock
//! honesty only — not two independent human diagnosis runs and not a Suite
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

use partitionline::{Consumer, ConsumerConfig, Error};
use std::time::Duration;

#[tokio::test]
async fn wakeups_signaled_metric_increments_on_wakeup() {
    let mock = common::Mock::start().await;
    let mut cfg = ConsumerConfig::bootstrap([mock.addr.clone()]);
    cfg.request_timeout = Duration::from_secs(5);
    cfg.max_wait_ms = 10;
    let consumer = Consumer::new(cfg).await.unwrap();
    assert_eq!(consumer.metrics().wakeups_signaled, 0);
    consumer.wakeup();
    assert_eq!(
        consumer.metrics().wakeups_signaled, 1,
        "Consumer::wakeup must increment wakeups_signaled"
    );
    assert_eq!(
        consumer.metrics().wakeups_consumed, 0,
        "signal alone must not invent wakeups_consumed"
    );
    let handle = consumer.wakeup_handle();
    handle.wakeup();
    assert_eq!(
        consumer.metrics().wakeups_signaled, 2,
        "WakeupHandle::wakeup must share wakeups_signaled"
    );
    consumer.close().await.unwrap();
}

#[tokio::test]
async fn wakeups_consumed_metric_increments_when_fetch_drains_wakeup() {
    let mock = common::Mock::start().await;
    let mut cfg = ConsumerConfig::bootstrap([mock.addr.clone()]);
    cfg.request_timeout = Duration::from_secs(5);
    cfg.max_wait_ms = 10;
    let mut consumer = Consumer::new(cfg).await.unwrap();
    consumer.assign("t", 0, 0).await.unwrap();
    consumer.wakeup();
    let err = consumer.fetch().await.unwrap_err();
    assert!(
        matches!(err, Error::Wakeup),
        "expected Wakeup after signal, got {err}"
    );
    let m = consumer.metrics();
    assert!(
        m.wakeups_signaled >= 1,
        "wakeup signal must be counted; got {}",
        m.wakeups_signaled
    );
    assert!(
        m.wakeups_consumed >= 1,
        "fetch draining wakeup must increment wakeups_consumed; got {}",
        m.wakeups_consumed
    );
    // Subsequent fetch succeeds and must not invent extra consumed without signal.
    let consumed_after = consumer.metrics().wakeups_consumed;
    let _ = consumer.fetch().await.unwrap();
    assert_eq!(
        consumer.metrics().wakeups_consumed,
        consumed_after,
        "idle fetch must not invent wakeups_consumed"
    );
    consumer.close().await.unwrap();
}

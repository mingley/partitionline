//! Coordinator creation and discovery must fit one bounded startup budget.
mod common;
use partitionline::{error, ConsumerConfig, ConsumerGroup, Error};
use std::time::{Duration, Instant};

fn config(mock: &common::Mock) -> ConsumerConfig {
    let mut config = ConsumerConfig::bootstrap([mock.addr.clone()]);
    config.request_timeout = Duration::from_secs(2);
    config.retry_backoff = Duration::from_millis(5);
    config.retry_backoff_max = Duration::from_millis(10);
    config
}

#[tokio::test]
async fn startup_recovers_after_more_than_three_typed_coordinator_failures() {
    for kip848 in [false, true] {
        let mock = common::Mock::start().await;
        for code in [
            error::COORDINATOR_NOT_AVAILABLE,
            error::COORDINATOR_LOAD_IN_PROGRESS,
            error::NOT_COORDINATOR,
            error::COORDINATOR_NOT_AVAILABLE,
        ] {
            mock.fail_find_coordinator_once(code);
        }
        let started = Instant::now();
        let group = if kip848 {
            ConsumerGroup::join_consumer(config(&mock), "startup", "t").await
        } else {
            ConsumerGroup::join(config(&mock), "startup", "t").await
        }
        .unwrap();
        assert_eq!(mock.find_coordinator_calls(), 5);
        assert!(started.elapsed() >= Duration::from_millis(20));
        group.close().await.unwrap();
    }
}

#[tokio::test]
async fn terminal_discovery_errors_are_not_retried() {
    for code in [error::GROUP_AUTHORIZATION_FAILED, error::REQUEST_TIMED_OUT] {
        let mock = common::Mock::start().await;
        let other = common::Mock::start().await;
        mock.fail_find_coordinator_once(code);
        let mut config = config(&mock);
        config.bootstrap.push(other.addr.clone());
        let result = ConsumerGroup::join(config, "terminal", "t").await;
        assert!(matches!(result, Err(Error::Broker { code: actual, .. }) if actual == code));
        assert_eq!(mock.find_coordinator_calls(), 1);
        assert_eq!(other.find_coordinator_calls(), 0);
    }
}

#[tokio::test]
async fn slow_bootstrap_negotiation_cannot_reset_the_discovery_deadline() {
    let first = common::Mock::start().await;
    let second = common::Mock::start().await;
    let third = common::Mock::start().await;
    first.fail_find_coordinator_once(error::COORDINATOR_NOT_AVAILABLE);
    second.set_api_versions_delay(Duration::from_secs(1));
    third.set_api_versions_delay(Duration::from_secs(1));
    let mut config = config(&first);
    config
        .bootstrap
        .extend([second.addr.clone(), third.addr.clone()]);
    config.request_timeout = Duration::from_millis(80);
    let result = tokio::time::timeout(
        Duration::from_millis(150),
        ConsumerGroup::join(config, "slow-bootstrap", "t"),
    )
    .await;
    assert!(matches!(result, Ok(Err(Error::Timeout))));
    assert_eq!(third.find_coordinator_calls(), 0);
}

#[tokio::test]
async fn persistent_startup_errors_stop_at_the_original_deadline_with_backoff() {
    let mock = common::Mock::start().await;
    for _ in 0..64 {
        mock.fail_find_coordinator_once(error::COORDINATOR_NOT_AVAILABLE);
    }
    let mut config = config(&mock);
    config.request_timeout = Duration::from_millis(80);
    config.retry_backoff = Duration::from_millis(25);
    config.retry_backoff_max = config.retry_backoff;
    let started = Instant::now();
    let result = tokio::time::timeout(
        Duration::from_secs(2),
        ConsumerGroup::join(config, "bounded", "t"),
    )
    .await
    .unwrap();
    assert!(matches!(result, Err(Error::Timeout)));
    assert!(started.elapsed() >= Duration::from_millis(70));
    assert!((1..=4).contains(&mock.find_coordinator_calls()));
}

//! Producer-ID startup retries are paced and share one request deadline.
mod common;

use partitionline::{error, Error, Producer, ProducerConfig};
use std::time::{Duration, Instant};

fn config(mock: &common::Mock, transactional: bool) -> ProducerConfig {
    let mut cfg = ProducerConfig::bootstrap([mock.addr.clone()]).idempotent(true);
    if transactional {
        cfg = cfg.transactional_id("cold-start");
    }
    cfg.request_timeout = Duration::from_secs(2);
    cfg.retry_backoff = Duration::from_millis(5);
    cfg.retry_backoff_max = Duration::from_millis(10);
    cfg
}

#[tokio::test]
async fn producer_id_startup_recovers_after_multiple_transient_responses() {
    for transactional in [false, true] {
        let mock = common::Mock::start().await;
        for code in [14, 15, 16, 14] {
            mock.fail_init_producer_id_once(code);
        }
        let started = Instant::now();
        let producer = Producer::new(config(&mock, transactional)).await.unwrap();
        assert_eq!(mock.init_producer_id_nodes().len(), 5);
        assert!(started.elapsed() >= Duration::from_millis(20));
        producer.close().await.unwrap();
    }
}

#[tokio::test]
async fn producer_id_terminal_errors_do_not_retry() {
    for code in [31, 53, error::INVALID_TXN_STATE, error::PRODUCER_FENCED, 7] {
        for transactional in [false, true] {
            let mock = common::Mock::start().await;
            mock.fail_init_producer_id_once(code);
            let result = Producer::new(config(&mock, transactional)).await;
            assert!(matches!(result, Err(Error::Broker { code: actual, .. }) if actual == code));
            assert_eq!(mock.init_producer_id_nodes().len(), 1);
        }
    }
}

#[tokio::test]
async fn persistent_producer_id_errors_stop_at_one_deadline() {
    for transactional in [false, true] {
        let mock = common::Mock::start().await;
        for _ in 0..64 {
            mock.fail_init_producer_id_once(14);
        }
        let mut cfg = config(&mock, transactional);
        cfg.request_timeout = Duration::from_millis(80);
        cfg.retry_backoff = Duration::from_millis(25);
        cfg.retry_backoff_max = cfg.retry_backoff;
        let started = Instant::now();
        let result = tokio::time::timeout(Duration::from_secs(1), Producer::new(cfg))
            .await
            .unwrap();
        assert!(matches!(result, Err(Error::Timeout)));
        assert!(started.elapsed() >= Duration::from_millis(70));
        assert!((1..=4).contains(&mock.init_producer_id_nodes().len()));
    }
}

#[tokio::test]
async fn slow_responses_cannot_renew_the_producer_id_deadline() {
    let mock = common::Mock::start().await;
    mock.set_init_producer_id_delay(Duration::from_millis(55));
    for _ in 0..64 {
        mock.fail_init_producer_id_once(14);
    }
    let mut cfg = config(&mock, false);
    cfg.request_timeout = Duration::from_millis(100);
    cfg.retry_backoff = Duration::from_millis(5);
    cfg.retry_backoff_max = cfg.retry_backoff;
    let result = tokio::time::timeout(Duration::from_millis(180), Producer::new(cfg)).await;
    assert!(matches!(result, Ok(Err(Error::Timeout))));
}

#[tokio::test]
async fn transaction_coordinator_startup_retries_within_one_deadline() {
    let mock = common::Mock::start().await;
    for code in [15, 14, 16, 15] {
        mock.fail_find_coordinator_once(code);
    }
    let producer = Producer::new(config(&mock, true)).await.unwrap();
    assert_eq!(mock.find_coordinator_calls(), 5);
    assert_eq!(mock.init_producer_id_nodes().len(), 1);
    producer.close().await.unwrap();
}

#[tokio::test]
async fn discovery_and_allocation_share_the_original_initialization_deadline() {
    let mock = common::Mock::start().await;
    for _ in 0..3 {
        mock.fail_find_coordinator_once(14);
    }
    mock.set_init_producer_id_delay(Duration::from_millis(55));
    let mut cfg = config(&mock, true);
    cfg.request_timeout = Duration::from_millis(100);
    cfg.retry_backoff = Duration::from_millis(20);
    cfg.retry_backoff_max = cfg.retry_backoff;
    let result = tokio::time::timeout(Duration::from_millis(160), Producer::new(cfg)).await;
    assert!(matches!(result, Ok(Err(Error::Timeout))));
}

/// A coordinator-load backoff must drop the socket when that broker stops writing.
#[tokio::test(flavor = "current_thread")]
async fn init_producer_id_drops_the_socket_when_the_broker_closes_during_load_backoff() {
    let mock = common::Mock::start().await;
    mock.fail_init_producer_id_once(error::COORDINATOR_LOAD_IN_PROGRESS);
    let mut cfg = ProducerConfig::bootstrap([mock.addr.clone()]);
    cfg.transactional_id = Some("tx-load-backoff".into());
    cfg.linger = Duration::ZERO;
    cfg.retry_backoff = Duration::from_secs(5);
    cfg.retry_backoff_max = Duration::from_secs(5);
    cfg.request_timeout = Duration::from_secs(30);
    let mut opening = std::pin::pin!(Producer::new(cfg));
    let started = Instant::now();
    loop {
        if !mock.init_producer_id_nodes().is_empty() {
            break;
        }
        tokio::select! {
            _ = &mut opening => panic!("init finished before the load response"),
            _ = tokio::time::sleep(Duration::from_millis(10)) => {}
        }
        if started.elapsed() > Duration::from_secs(2) {
            panic!("did not see InitProducerId");
        }
    }
    let settle = Instant::now();
    while settle.elapsed() < Duration::from_millis(80) {
        tokio::select! {
            _ = &mut opening => panic!("init left the load backoff early"),
            _ = tokio::time::sleep(Duration::from_millis(10)) => {}
        }
    }
    assert_eq!(
        mock.init_producer_id_nodes().len(),
        1,
        "a second InitProducerId means the client is not waiting out the backoff"
    );
    let before = mock.accepts();
    mock.shutdown_writes();
    let window = Instant::now();
    loop {
        if mock.accepts() > before {
            break;
        }
        if window.elapsed() > Duration::from_millis(400) {
            panic!("init kept the socket during coordinator load backoff; accepts {before}");
        }
        tokio::select! {
            result = &mut opening => {
                let producer = result.expect("init after the broker closed");
                assert!(
                    mock.accepts() > before,
                    "init finished without replacing the closed coordinator socket"
                );
                producer.close().await.unwrap();
                return;
            }
            _ = tokio::time::sleep(Duration::from_millis(10)) => {}
        }
    }
}

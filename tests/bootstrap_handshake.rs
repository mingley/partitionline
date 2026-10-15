//! A bootstrap that accepts TCP and then stalls must not hold startup.
mod common;

use partitionline::{error, Admin, AdminConfig, Error, Producer, ProducerConfig};
use std::time::{Duration, Instant};

async fn stall_listener() -> String {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let stall = listener.local_addr().unwrap();
    drop(tokio::spawn(async move {
        loop {
            let Ok((socket, _)) = listener.accept().await else {
                break;
            };
            tokio::time::sleep(Duration::from_secs(30)).await;
            drop(socket);
        }
    }));
    stall.to_string()
}

#[tokio::test]
async fn a_slow_bootstrap_does_not_hold_admin_startup() {
    let mock = common::Mock::start().await;
    let stall = stall_listener().await;
    let mut cfg = AdminConfig::bootstrap([mock.addr.clone()]);
    cfg.bootstrap.insert(0, stall);
    cfg.connect_timeout = Duration::from_millis(200);
    cfg.request_timeout = Duration::from_secs(2);
    let started = Instant::now();
    let mut admin = Admin::new(cfg).await.unwrap();
    let elapsed = started.elapsed();
    assert!(
        elapsed < Duration::from_millis(500),
        "admin waited for the stalled bootstrap: {elapsed:?}"
    );
    let cluster = tokio::time::timeout(Duration::from_millis(500), admin.describe_cluster())
        .await
        .unwrap()
        .unwrap();
    assert!(
        !cluster.brokers.is_empty(),
        "admin RPC after the raced handshake must return brokers, got {cluster:?}"
    );
}

#[tokio::test]
async fn a_slow_bootstrap_does_not_hold_producer_startup() {
    let mock = common::Mock::start().await;
    let stall = stall_listener().await;
    let mut cfg = ProducerConfig::bootstrap([mock.addr.clone()]);
    cfg.bootstrap.insert(0, stall);
    cfg.connect_timeout = Duration::from_millis(200);
    cfg.request_timeout = Duration::from_secs(2);
    cfg.linger = Duration::ZERO;
    let started = Instant::now();
    let producer = Producer::new(cfg).await.unwrap();
    let elapsed = started.elapsed();
    assert!(
        elapsed < Duration::from_millis(500),
        "producer waited for the stalled bootstrap: {elapsed:?}"
    );
    producer.close().await.unwrap();
}

#[tokio::test]
async fn a_slow_bootstrap_does_not_hold_transactional_producer_startup() {
    let mock = common::Mock::start().await;
    let stall = stall_listener().await;
    let mut cfg = ProducerConfig::bootstrap([mock.addr.clone()]).transactional_id("stall-txn");
    cfg.bootstrap.insert(0, stall);
    cfg.connect_timeout = Duration::from_millis(200);
    cfg.request_timeout = Duration::from_secs(2);
    cfg.linger = Duration::ZERO;
    let started = Instant::now();
    let producer = Producer::new(cfg).await.unwrap();
    let elapsed = started.elapsed();
    assert!(
        elapsed < Duration::from_millis(500),
        "transactional producer waited for the stalled bootstrap: {elapsed:?}"
    );
    producer.close().await.unwrap();
}

#[tokio::test]
async fn terminal_coordinator_errors_do_not_ask_the_next_bootstrap() {
    let mock = common::Mock::start().await;
    let other = common::Mock::start().await;
    mock.fail_find_coordinator_once(error::GROUP_AUTHORIZATION_FAILED);
    let mut cfg = ProducerConfig::bootstrap([mock.addr.clone()]).transactional_id("terminal");
    cfg.bootstrap.push(other.addr.clone());
    cfg.request_timeout = Duration::from_secs(2);
    let result = Producer::new(cfg).await;
    assert!(
        matches!(result, Err(Error::Broker { code, .. }) if code == error::GROUP_AUTHORIZATION_FAILED)
    );
    assert_eq!(mock.find_coordinator_calls(), 1);
    assert_eq!(other.find_coordinator_calls(), 0);
}

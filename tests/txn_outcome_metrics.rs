//! KL-07 Partial: transaction outcome counters for diagnosis.
//!
//! `ProducerMetrics::{transactions_committed, transactions_aborted}` must move
//! on successful EndTxn commit/abort so operators can tell commit vs abort
//! storms from telemetry — without payload logging.
#![expect(
    dead_code,
    reason = "tests/common mock helpers are shared; this file uses a subset"
)]

mod common;

use partitionline::{Producer, ProducerConfig};
use std::time::Duration;

#[tokio::test]
async fn txn_outcome_metrics_increment_on_commit_and_abort() {
    let mock = common::Mock::start().await;
    let mut cfg = ProducerConfig::bootstrap([mock.addr.clone()]);
    cfg.linger = Duration::ZERO;
    cfg.transactional_id = Some("txn-outcome-metrics".into());
    let producer = Producer::new(cfg).await.unwrap();
    producer.init_transactions().await.unwrap();

    let before = producer.metrics();
    assert_eq!(before.transactions_committed, 0);
    assert_eq!(before.transactions_aborted, 0);

    producer.begin_transaction().await.unwrap();
    producer.commit_transaction().await.unwrap();
    let after_commit = producer.metrics();
    assert_eq!(
        after_commit.transactions_committed,
        before.transactions_committed + 1,
        "successful commit_transaction must increment transactions_committed"
    );
    assert_eq!(
        after_commit.transactions_aborted, before.transactions_aborted,
        "commit must not bump transactions_aborted"
    );

    producer.begin_transaction().await.unwrap();
    producer.abort_transaction().await.unwrap();
    let after_abort = producer.metrics();
    assert_eq!(
        after_abort.transactions_committed, after_commit.transactions_committed,
        "abort must not bump transactions_committed"
    );
    assert_eq!(
        after_abort.transactions_aborted,
        after_commit.transactions_aborted + 1,
        "successful abort_transaction must increment transactions_aborted"
    );

    producer.close_timeout(Duration::from_secs(5)).await.unwrap();
}

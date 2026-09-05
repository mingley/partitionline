//! KL-07 slice: rebalance counters for thrash diagnosis.
//!
//! `ConsumerMetrics::{rebalances, partitions_revoked, partitions_assigned}`
//! must move on first assign and on unsubscribe revoke so operators can
//! correlate lag spikes with membership churn — without payload logging.
#![expect(
    dead_code,
    reason = "tests/common mock helpers are shared; this file uses a subset"
)]

mod common;

use partitionline::{ConsumerConfig, ConsumerGroup};
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};
use std::time::Duration;

#[tokio::test]
async fn rebalance_metrics_increment_on_first_assignment() {
    let mock = common::Mock::start().await;
    let assigned = Arc::new(AtomicUsize::new(0));
    let hook = {
        let assigned = Arc::clone(&assigned);
        move |_revoked: &[partitionline::TopicPartition],
              added: &[partitionline::TopicPartition]| {
            let _ = assigned.fetch_add(added.len(), Ordering::SeqCst);
        }
    };

    let mut group = ConsumerGroup::join(
        ConsumerConfig::bootstrap([mock.addr.clone()])
            .max_wait_ms(10)
            .on_rebalance(hook),
        "rebalance-metrics-assign",
        "t",
    )
    .await
    .unwrap();

    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    while assigned.load(Ordering::SeqCst) == 0 {
        let _ = group.poll().await.unwrap();
        assert!(
            tokio::time::Instant::now() < deadline,
            "timed out waiting for first assignment"
        );
        tokio::time::sleep(Duration::from_millis(25)).await;
    }

    let m = group.metrics();
    assert!(
        m.rebalances >= 1,
        "expected rebalances>=1 after first assign, got {}",
        m.rebalances
    );
    assert!(
        m.partitions_assigned >= 1,
        "expected partitions_assigned>=1, got {}",
        m.partitions_assigned
    );
    group.leave().await.unwrap();
}

#[tokio::test]
async fn rebalance_metrics_increment_on_unsubscribe_revoke() {
    let mock = common::Mock::start().await;
    let assigned = Arc::new(AtomicUsize::new(0));
    let hook = {
        let assigned = Arc::clone(&assigned);
        move |_revoked: &[partitionline::TopicPartition],
              added: &[partitionline::TopicPartition]| {
            if !added.is_empty() {
                let _ = assigned.fetch_add(added.len(), Ordering::SeqCst);
            }
        }
    };

    let mut group = ConsumerGroup::join(
        ConsumerConfig::bootstrap([mock.addr.clone()])
            .max_wait_ms(10)
            .on_rebalance(hook),
        "rebalance-metrics-unsub",
        "t",
    )
    .await
    .unwrap();

    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    while assigned.load(Ordering::SeqCst) == 0 {
        let _ = group.poll().await.unwrap();
        assert!(
            tokio::time::Instant::now() < deadline,
            "timed out waiting for assignment before unsubscribe"
        );
        tokio::time::sleep(Duration::from_millis(25)).await;
    }

    let before = group.metrics();
    assert!(before.rebalances >= 1);
    assert!(before.partitions_assigned >= 1);

    group.unsubscribe().await.unwrap();

    let after = group.metrics();
    assert!(
        after.rebalances > before.rebalances,
        "unsubscribe revoke should increment rebalances (before={}, after={})",
        before.rebalances,
        after.rebalances
    );
    assert!(
        after.partitions_revoked > before.partitions_revoked,
        "unsubscribe revoke should increment partitions_revoked (before={}, after={})",
        before.partitions_revoked,
        after.partitions_revoked
    );
    group.leave().await.unwrap();
}

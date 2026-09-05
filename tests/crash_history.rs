//! KL-03 Partial: unique-ID crash/txn history on a mock broker.
//!
//! Seeds stable record IDs before send, commits one txn and aborts another,
//! then checks an independent `read_committed` consumer sees only committed
//! IDs in seed order. This is history-classification honesty — **not** a
//! three-broker KRaft HA close and not a Suite HOLD lift.
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

use partitionline::{
    Consumer, ConsumerConfig, IsolationLevel, ProduceRecord, Producer, ProducerConfig,
};
use std::collections::BTreeSet;
use std::time::Duration;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Outcome {
    CommittedVisible,
    AbortedHidden,
}

#[tokio::test]
async fn unique_id_commit_abort_history_classifies_read_committed() {
    let mock = common::Mock::start().await;
    let mut pcfg = ProducerConfig::bootstrap([mock.addr.clone()]);
    pcfg.linger = Duration::ZERO;
    pcfg.transactional_id = Some("tx-crash-history".into());
    let producer = Producer::new(pcfg).await.unwrap();

    // Pre-send unique IDs (history discipline: record before submission).
    let committed_ids: &[&[u8]] = &[b"id-committed-0001", b"id-committed-0002"];
    let aborted_id: &[u8] = b"id-aborted-0003";
    let mut expected = Vec::new();

    producer.begin_transaction().await.unwrap();
    for id in committed_ids {
        let _meta = producer
            .send(ProduceRecord::to("t").value(*id))
            .await
            .unwrap();
        expected.push((*id, Outcome::CommittedVisible));
    }
    producer.commit_transaction().await.unwrap();

    producer.begin_transaction().await.unwrap();
    let _meta = producer
        .send(ProduceRecord::to("t").value(aborted_id))
        .await
        .unwrap();
    producer.abort_transaction().await.unwrap();
    expected.push((aborted_id, Outcome::AbortedHidden));
    producer.close().await.unwrap();

    // Independent uncommitted consumer: mock must still return aborted bodies
    // so filtering is client-side under read_committed.
    let mut ccfg_u = ConsumerConfig::bootstrap([mock.addr.clone()]);
    ccfg_u.max_wait_ms = 10;
    ccfg_u.isolation_level = IsolationLevel::ReadUncommitted;
    let mut uncommitted = Consumer::new(ccfg_u).await.unwrap();
    uncommitted.assign("t", 0, 0).await.unwrap();
    let all = uncommitted.fetch().await.unwrap();
    let all_ids: BTreeSet<&[u8]> = all.iter().filter_map(|r| r.value.as_deref()).collect();
    assert!(
        committed_ids.iter().all(|id| all_ids.contains(id)) && all_ids.contains(&aborted_id),
        "mock must retain all seeded IDs for history classification; got {all_ids:?}"
    );
    uncommitted.close().await.unwrap();

    // Independent read_committed consumer: only committed IDs visible, in order.
    let mut ccfg = ConsumerConfig::bootstrap([mock.addr.clone()]);
    ccfg.max_wait_ms = 10;
    ccfg.isolation_level = IsolationLevel::ReadCommitted;
    let mut consumer = Consumer::new(ccfg).await.unwrap();
    consumer.assign("t", 0, 0).await.unwrap();
    let recs = consumer.fetch().await.unwrap();
    let visible: Vec<&[u8]> = recs.iter().filter_map(|r| r.value.as_deref()).collect();
    let visible_set: BTreeSet<&[u8]> = visible.iter().copied().collect();

    for (id, outcome) in &expected {
        match outcome {
            Outcome::CommittedVisible => assert!(
                visible_set.contains(id),
                "committed id {:?} missing from read_committed history: {visible:?}",
                std::str::from_utf8(id).ok()
            ),
            Outcome::AbortedHidden => assert!(
                !visible_set.contains(id),
                "aborted id {:?} leaked into read_committed history: {visible:?}",
                std::str::from_utf8(id).ok()
            ),
        }
    }
    assert_eq!(
        visible, committed_ids,
        "read_committed history must match committed seed order"
    );
    consumer.close().await.unwrap();
}

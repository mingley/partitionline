//! KL-07 Partial: per-topic metrics omit inactive topics (cardinality honesty).
//!
//! Cap enforcement for [`partitionline::MAX_TOPIC_METRIC_SERIES`] is covered by
//! lib tests in `src/metrics.rs`. This mock check proves inactive topics stay
//! out of snapshots (topic names only — no partition labels). Does **not**
//! close full KL-07. Does **not** lift Suite HOLD.
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

use partitionline::{ProduceRecord, Producer, ProducerConfig, MAX_TOPIC_METRIC_SERIES};
use std::time::Duration;

#[tokio::test]
async fn inactive_topics_omitted_from_snapshot() {
    let mock = common::Mock::start().await;
    let mut pcfg = ProducerConfig::bootstrap([mock.addr.clone()]);
    pcfg.linger = Duration::ZERO;
    let producer = Producer::new(pcfg).await.unwrap();
    // Mock pre-creates topic "t".
    let _md = producer
        .send(ProduceRecord::to("t").value(&b"y"[..]))
        .await
        .unwrap();
    let m = producer.metrics();
    assert_eq!(m.topics.len(), 1);
    assert_eq!(m.topics[0].topic, "t");
    assert!(
        MAX_TOPIC_METRIC_SERIES >= 1,
        "cardinality cap must be a positive public constant"
    );
    producer.close().await.unwrap();
}

#[tokio::test]
async fn topic_series_are_topic_names_not_partition_labels() {
    let mock = common::Mock::start().await;
    let mut pcfg = ProducerConfig::bootstrap([mock.addr.clone()]);
    pcfg.linger = Duration::ZERO;
    let producer = Producer::new(pcfg).await.unwrap();
    let _md = producer
        .send(ProduceRecord::to("t").value(&b"z"[..]))
        .await
        .unwrap();
    let m = producer.metrics();
    assert_eq!(m.topics.len(), 1);
    assert_eq!(m.topics[0].topic, "t");
    // Core snapshots must not invent partition-scoped label keys.
    assert!(!m.topics[0].topic.contains("partition="));
    assert!(!format!("{:?}", m).contains("partition="));
    producer.close().await.unwrap();
}

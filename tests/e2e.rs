//! Suite protocol-client e2e against the in-tree mock broker.
//!
//! Produce one record, fetch it back, and complete one classic group hop
//! (JoinGroup). Not a live cluster. Not a bench. Not a win.
#![expect(
    dead_code,
    reason = "tests/common mock helpers are shared; this file uses a subset"
)]

mod common;

use partitionline::{
    ConsumerConfig, ConsumerGroup, ProduceRecord, Producer, ProducerConfig, TopicPartition,
};
use std::time::Duration;

#[tokio::test]
async fn produce_fetch_classic_join_same_payload() {
    let mock = common::Mock::start().await;
    let payload = &b"e2e-produce-fetch-join"[..];

    let mut pcfg = ProducerConfig::bootstrap([mock.addr.clone()]);
    pcfg.linger = Duration::ZERO;
    let producer = Producer::new(pcfg).await.unwrap();
    let md = producer
        .send(ProduceRecord::to("t").value(payload))
        .await
        .unwrap();
    assert_eq!(md.topic, "t");
    assert_eq!(md.partition, 0);
    assert_eq!(md.offset, 0);
    producer.close().await.unwrap();

    let mut ccfg = ConsumerConfig::bootstrap([mock.addr.clone()]);
    ccfg.max_wait_ms = 10;
    let mut group = ConsumerGroup::join(ccfg, "e2e", "t").await.unwrap();
    assert!(
        mock.join_group_calls() >= 1,
        "classic JoinGroup must land on the mock, got {}",
        mock.join_group_calls()
    );
    assert!(
        !group.member_id().is_empty(),
        "classic join must assign a member id"
    );
    assert_eq!(group.assignment(), vec![TopicPartition::new("t", 0)]);

    let recs = group.poll().await.unwrap();
    assert_eq!(recs.len(), 1, "fetched record count");
    assert_eq!(recs[0].topic, "t");
    assert_eq!(recs[0].partition, 0);
    assert_eq!(recs[0].offset, 0);
    assert_eq!(
        recs[0].value.as_deref(),
        Some(payload),
        "fetched payload must match produced payload"
    );
}

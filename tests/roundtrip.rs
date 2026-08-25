//! Produce then fetch against the mock broker.
#![expect(
    dead_code,
    reason = "tests/common mock helpers are shared; this file uses a subset"
)]

mod common;

use partitionline::{Consumer, ConsumerConfig, ProduceRecord, Producer, ProducerConfig};
use std::time::Duration;

#[tokio::test]
async fn produce_then_fetch_same_payload() {
    let mock = common::Mock::start().await;
    let mut pcfg = ProducerConfig::bootstrap([mock.addr.clone()]);
    pcfg.linger = Duration::ZERO;
    let producer = Producer::new(pcfg).await.unwrap();
    let md = producer
        .send(ProduceRecord::to("t").value(&b"hello-roundtrip"[..]))
        .await
        .unwrap();
    assert_eq!(md.topic, "t");
    assert_eq!(md.partition, 0);
    assert_eq!(md.offset, 0);
    producer.close().await.unwrap();

    let mut ccfg = ConsumerConfig::bootstrap([mock.addr.clone()]);
    ccfg.max_wait_ms = 10;
    let mut consumer = Consumer::new(ccfg).await.unwrap();
    consumer.assign("t", 0, 0).await.unwrap();
    let recs = consumer.fetch().await.unwrap();
    assert_eq!(recs.len(), 1);
    assert_eq!(recs[0].topic, "t");
    assert_eq!(recs[0].partition, 0);
    assert_eq!(recs[0].offset, 0);
    assert_eq!(recs[0].value.as_deref(), Some(&b"hello-roundtrip"[..]));
}

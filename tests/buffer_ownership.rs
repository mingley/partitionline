//! KL-02 slice: buffer ownership under mock overload (not a 2×/24h RSS close).
//!
//! `buffer_memory` counts key+value bytes reserved from accept until ack/fail.
//! Saturating `try_send` must never push `metrics().bytes_buffered` over the cap,
//! and flush/close must drain reserved bytes to zero.
#![expect(
    dead_code,
    reason = "tests/common mock helpers are shared; this file uses a subset"
)]

mod common;

use partitionline::protocol::records::Records;
use partitionline::{Error, ProduceRecord, Producer, ProducerConfig};
use std::sync::Arc;
use std::time::Duration;
use tokio::task::JoinSet;

fn value_upper_bound(payload: &[u8]) -> usize {
    usize::try_from(
        Records::estimate_size_in_bytes_upper_bound(None, Some(payload), &[]).unwrap(),
    )
    .unwrap()
}

async fn warm_metadata(producer: &Producer) {
    // `try_send` returns QueueFull until metadata/leader routing is ready.
    let _md = producer
        .send(ProduceRecord::to("t").value(&b""[..]))
        .await
        .unwrap();
    producer.flush().await.unwrap();
    assert_eq!(producer.metrics().bytes_buffered, 0);
}

#[tokio::test]
async fn saturating_try_send_never_exceeds_buffer_memory() {
    let mock = common::Mock::start().await;
    let payload = vec![b'x'; 100];
    let upper = value_upper_bound(&payload);
    // Cap fits a few key+value reservations but stays near the Java upper-bound floor.
    let cap = upper.saturating_mul(2).max(upper + payload.len());
    let producer = Producer::new(
        ProducerConfig::bootstrap([mock.addr.clone()])
            .linger(Duration::ZERO)
            .buffer_memory(cap),
    )
    .await
    .unwrap();
    warm_metadata(&producer).await;

    let mut accepted = 0usize;
    let mut rejected = 0usize;
    for _ in 0..64 {
        match producer.try_send(ProduceRecord::to("t").value(payload.clone())) {
            Ok(()) => {
                accepted += 1;
                let buffered = producer.metrics().bytes_buffered as usize;
                assert!(
                    buffered <= cap,
                    "bytes_buffered {buffered} exceeded buffer_memory {cap}"
                );
            }
            Err(Error::QueueFull) => {
                rejected += 1;
                let buffered = producer.metrics().bytes_buffered as usize;
                assert!(
                    buffered <= cap,
                    "QueueFull path left bytes_buffered {buffered} > {cap}"
                );
            }
            Err(other) => panic!("unexpected try_send error: {other:?}"),
        }
    }
    assert!(accepted >= 1, "expected at least one accepted record");
    assert!(rejected >= 1, "expected QueueFull once the cap is saturated");
    assert!(
        producer.metrics().bytes_buffered as usize <= cap,
        "post-loop bytes_buffered exceeded cap"
    );

    producer.flush().await.unwrap();
    assert_eq!(
        producer.metrics().bytes_buffered, 0,
        "flush must release all buffer_memory reservations"
    );
    producer.clone().close().await.unwrap();
}

#[tokio::test]
async fn queue_full_under_overload_releases_no_orphan_bytes() {
    let mock = common::Mock::start().await;
    let payload = vec![b'y'; 100];
    let upper = value_upper_bound(&payload);
    let cap = upper.saturating_mul(2).max(upper + payload.len());
    let producer = Arc::new(
        Producer::new(
            ProducerConfig::bootstrap([mock.addr.clone()])
                .linger(Duration::ZERO)
                .buffer_memory(cap),
        )
        .await
        .unwrap(),
    );
    warm_metadata(producer.as_ref()).await;

    let mut joins = JoinSet::new();
    for _ in 0..8 {
        let producer = Arc::clone(&producer);
        let payload = payload.clone();
        let _ = joins.spawn(async move {
            let mut full = 0usize;
            for _ in 0..32 {
                match producer.try_send(ProduceRecord::to("t").value(payload.clone())) {
                    Ok(()) => {}
                    Err(Error::QueueFull) => full += 1,
                    Err(other) => panic!("unexpected try_send error: {other:?}"),
                }
                let buffered = producer.metrics().bytes_buffered as usize;
                assert!(buffered <= cap, "concurrent overload exceeded cap: {buffered}");
            }
            full
        });
    }

    let mut queue_fulls = 0usize;
    while let Some(res) = joins.join_next().await {
        queue_fulls += res.unwrap();
    }
    assert!(
        queue_fulls >= 1,
        "concurrent hammering should observe QueueFull"
    );
    assert!(
        producer.metrics().bytes_buffered as usize <= cap,
        "bytes_buffered drifted over cap after concurrent overload"
    );

    producer.flush().await.unwrap();
    assert_eq!(
        producer.metrics().bytes_buffered, 0,
        "flush after overload must leave no orphan buffer reservations"
    );
    producer.as_ref().clone().close().await.unwrap();
}

#[tokio::test]
async fn send_timeout_when_buffer_full_and_max_block_expires() {
    let mock = common::Mock::start().await;
    let payload = vec![b'z'; 100];
    let upper = value_upper_bound(&payload);
    // One record's Java upper bound fits; a second key+value reservation must not.
    let cap = upper.max(payload.len());
    let producer = Producer::new(
        ProducerConfig::bootstrap([mock.addr.clone()])
            .linger(Duration::ZERO)
            .buffer_memory(cap)
            .max_block(Duration::from_millis(50)),
    )
    .await
    .unwrap();
    warm_metadata(&producer).await;

    producer
        .try_send(ProduceRecord::to("t").value(payload.clone()))
        .unwrap();
    let buffered = producer.metrics().bytes_buffered;
    assert!(buffered > 0, "accepted record must reserve buffer_memory");
    assert!(buffered as usize <= cap);

    let err = producer
        .send(ProduceRecord::to("t").value(payload))
        .await
        .expect_err("full buffer + short max_block must time out");
    assert!(
        matches!(err, Error::Timeout),
        "expected Timeout while blocked on buffer_memory, got {err:?}"
    );
    assert_eq!(
        producer.metrics().bytes_buffered, buffered,
        "timed-out send must not leave an extra reservation"
    );

    producer.flush().await.unwrap();
    assert_eq!(producer.metrics().bytes_buffered, 0);
    producer.clone().close().await.unwrap();
}

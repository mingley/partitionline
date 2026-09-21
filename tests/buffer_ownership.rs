//! KL-02 slice: buffer ownership under mock overload (not a 2×/24h RSS close).
//!
//! `buffer_memory` counts key+value bytes reserved from accept until ack/fail.
//! Saturating `try_send` must never push `metrics().bytes_buffered` over the cap,
//! and flush/close must drain reserved bytes to zero.
#![expect(
    dead_code,
    reason = "tests/common mock helpers are shared; this file uses a subset"
)]
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing,
    reason = "integration-test helpers; clippy.toml allow-*-in-tests covers #[test] only"
)]

mod common;

use bytes::Bytes;
use partitionline::protocol::records::{Header, Records};
use partitionline::{Compression, Error, ProduceRecord, Producer, ProducerConfig};
use std::sync::Arc;
use std::time::Duration;
use tokio::task::JoinSet;

fn value_upper_bound(payload: &[u8]) -> usize {
    usize::try_from(Records::estimate_size_in_bytes_upper_bound(None, Some(payload), &[]).unwrap())
        .unwrap()
}

fn record_upper_bound(payload: Option<&[u8]>, headers: &[Header]) -> usize {
    usize::try_from(Records::estimate_size_in_bytes_upper_bound(None, payload, headers).unwrap())
        .unwrap()
}

fn buffered_usize(producer: &Producer) -> usize {
    usize::try_from(producer.metrics().bytes_buffered).expect("bytes_buffered fits usize")
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
                let buffered = buffered_usize(&producer);
                assert!(
                    buffered <= cap,
                    "bytes_buffered {buffered} exceeded buffer_memory {cap}"
                );
            }
            Err(Error::QueueFull) => {
                rejected += 1;
                let buffered = buffered_usize(&producer);
                assert!(
                    buffered <= cap,
                    "QueueFull path left bytes_buffered {buffered} > {cap}"
                );
            }
            Err(other) => panic!("unexpected try_send error: {other:?}"),
        }
    }
    assert!(accepted >= 1, "expected at least one accepted record");
    assert!(
        rejected >= 1,
        "expected QueueFull once the cap is saturated"
    );
    assert!(
        buffered_usize(&producer) <= cap,
        "post-loop bytes_buffered exceeded cap"
    );

    producer.flush().await.unwrap();
    assert_eq!(
        producer.metrics().bytes_buffered,
        0,
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
                let buffered = buffered_usize(&producer);
                assert!(
                    buffered <= cap,
                    "concurrent overload exceeded cap: {buffered}"
                );
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
        buffered_usize(producer.as_ref()) <= cap,
        "bytes_buffered drifted over cap after concurrent overload"
    );

    producer.flush().await.unwrap();
    assert_eq!(
        producer.metrics().bytes_buffered,
        0,
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
            // Hold the first reservation in the linger buffer so send() must wait.
            .linger(Duration::from_millis(1500))
            .delivery_timeout(Duration::from_secs(5))
            .buffer_memory(cap)
            .max_block(Duration::from_millis(80)),
    )
    .await
    .unwrap();
    warm_metadata(&producer).await;

    producer
        .try_send(ProduceRecord::to("t").value(payload.clone()))
        .unwrap();
    let buffered = producer.metrics().bytes_buffered;
    let buffered_n = usize::try_from(buffered).expect("bytes_buffered fits usize");
    assert!(
        buffered_n >= payload.len(),
        "accepted record must still hold buffer_memory (linger); got {buffered}"
    );
    assert!(buffered_n <= cap);

    let err = producer
        .send(ProduceRecord::to("t").value(payload))
        .await
        .expect_err("full buffer + short max_block must time out");
    assert!(
        matches!(err, Error::Timeout),
        "expected Timeout while blocked on buffer_memory, got {err:?}"
    );
    assert_eq!(
        producer.metrics().bytes_buffered,
        buffered,
        "timed-out send must not leave an extra reservation"
    );

    producer.flush().await.unwrap();
    assert_eq!(producer.metrics().bytes_buffered, 0);
    producer.clone().close().await.unwrap();
}

#[tokio::test]
async fn header_heavy_records_count_against_buffer_budget() {
    let mock = common::Mock::start().await;
    let header_val = vec![b'h'; 80];
    let headers = vec![Header::new("my-custom-header", header_val.clone())];
    let upper = record_upper_bound(None, &headers);
    let header_bytes = "my-custom-header".len() + header_val.len(); // 16 + 80 = 96
    let cap = upper.saturating_mul(2).max(upper + header_bytes);
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
        let rec = ProduceRecord::to("t").header("my-custom-header", header_val.clone());
        match producer.try_send(rec) {
            Ok(()) => {
                accepted += 1;
                let buffered = buffered_usize(&producer);
                assert!(
                    buffered <= cap,
                    "bytes_buffered {buffered} exceeded buffer_memory {cap}"
                );
            }
            Err(Error::QueueFull) => {
                rejected += 1;
                let buffered = buffered_usize(&producer);
                assert!(
                    buffered <= cap,
                    "QueueFull path left bytes_buffered {buffered} > {cap}"
                );
            }
            Err(other) => panic!("unexpected try_send error: {other:?}"),
        }
    }
    assert!(accepted >= 1, "expected at least one accepted record");
    assert!(
        rejected >= 1,
        "expected QueueFull once header bytes saturate the cap"
    );
    assert!(
        buffered_usize(&producer) <= cap,
        "post-loop bytes_buffered exceeded cap"
    );

    producer.flush().await.unwrap();
    assert_eq!(
        producer.metrics().bytes_buffered,
        0,
        "flush must release all header buffer_memory reservations"
    );
    producer.clone().close().await.unwrap();
}

#[tokio::test]
async fn zero_value_records_do_not_bypass_saturated_buffer() {
    let mock = common::Mock::start().await;
    let payload = vec![b'z'; 100];
    let upper = value_upper_bound(&payload);
    // cap allows 100-byte payloads to pass reject_oversized (upper <= cap).
    let cap = upper.max(200);
    let producer = Producer::new(
        ProducerConfig::bootstrap([mock.addr.clone()])
            .linger(Duration::from_millis(1500))
            .delivery_timeout(Duration::from_secs(5))
            .buffer_memory(cap)
            .max_block(Duration::from_millis(80)),
    )
    .await
    .unwrap();
    warm_metadata(&producer).await;

    let mut filled = 0usize;
    while filled + payload.len() <= cap {
        producer
            .try_send(ProduceRecord::to("t").value(payload.clone()))
            .unwrap();
        filled += payload.len();
    }
    let rem = cap.saturating_sub(filled);
    if rem > 0 {
        producer
            .try_send(ProduceRecord::to("t").value(vec![b'z'; rem]))
            .unwrap();
    }
    assert_eq!(buffered_usize(&producer), cap, "buffer must be 100% full");

    // Buffer is full (buffered >= cap). A zero-value record must NOT bypass the budget.
    let err = producer
        .try_send(ProduceRecord::to("t").value(&b""[..]))
        .expect_err("zero-value record must not bypass full buffer");
    assert!(
        matches!(err, Error::QueueFull),
        "expected QueueFull for zero-value record under saturation, got {err:?}"
    );

    producer.flush().await.unwrap();
    assert_eq!(producer.metrics().bytes_buffered, 0);

    // After flush, buffer has capacity; zero-value record succeeds.
    producer
        .try_send(ProduceRecord::to("t").value(&b""[..]))
        .unwrap();
    producer.flush().await.unwrap();
    assert_eq!(producer.metrics().bytes_buffered, 0);
    producer.clone().close().await.unwrap();
}

#[tokio::test]
async fn shared_backing_and_compressed_records_respect_buffer_budget() {
    let mock = common::Mock::start().await;
    let big_buffer = Bytes::from(vec![b'c'; 10_000]);
    let slice = big_buffer.slice(0..100);
    let header_val = big_buffer.slice(100..150);
    let headers = vec![Header::new("compressed-hdr", header_val.clone())];
    let upper = record_upper_bound(Some(&slice), &headers);
    let rec_len = slice.len() + "compressed-hdr".len() + header_val.len(); // 100 + 14 + 50 = 164
    let cap = upper.saturating_mul(2).max(upper + rec_len);
    let producer = Producer::new(
        ProducerConfig::bootstrap([mock.addr.clone()])
            .linger(Duration::ZERO)
            .compression(Compression::Gzip)
            .buffer_memory(cap),
    )
    .await
    .unwrap();
    warm_metadata(&producer).await;

    let mut accepted = 0usize;
    let mut rejected = 0usize;
    for _ in 0..64 {
        let rec = ProduceRecord::to("t")
            .value(slice.clone())
            .header("compressed-hdr", header_val.clone());
        match producer.try_send(rec) {
            Ok(()) => {
                accepted += 1;
                let buffered = buffered_usize(&producer);
                assert!(
                    buffered <= cap,
                    "bytes_buffered {buffered} exceeded buffer_memory {cap}"
                );
            }
            Err(Error::QueueFull) => {
                rejected += 1;
                let buffered = buffered_usize(&producer);
                assert!(
                    buffered <= cap,
                    "QueueFull path left bytes_buffered {buffered} > {cap}"
                );
            }
            Err(other) => panic!("unexpected try_send error: {other:?}"),
        }
    }
    assert!(accepted >= 1, "expected at least one accepted record");
    assert!(
        rejected >= 1,
        "expected QueueFull once compressed records saturate cap"
    );
    assert!(
        buffered_usize(&producer) <= cap,
        "post-loop bytes_buffered exceeded cap"
    );

    producer.flush().await.unwrap();
    assert_eq!(
        producer.metrics().bytes_buffered,
        0,
        "flush must release all reservations for compressed/shared-backing records"
    );
    producer.clone().close().await.unwrap();
}

#[tokio::test]
async fn permit_stays_owned_across_retry_and_releases_exactly_once() {
    let mock = common::Mock::start().await;
    mock.set_produce_error_times(partitionline::error::NOT_LEADER_OR_FOLLOWER, 1);
    let producer = Producer::new(
        ProducerConfig::bootstrap([mock.addr.clone()])
            .linger(Duration::ZERO)
            .retry_backoff(Duration::from_millis(20))
            .delivery_timeout(Duration::from_secs(5)),
    )
    .await
    .unwrap();
    warm_metadata(&producer).await;

    let rec = ProduceRecord::to("t")
        .value(vec![b'r'; 100])
        .header("retry-header", "retry-value");
    let md = producer.send(rec).await.unwrap();
    assert_eq!(md.topic, "t");
    assert_eq!(
        producer.metrics().bytes_buffered,
        0,
        "retried record must release permit exactly once on success"
    );
    producer.close().await.unwrap();
}

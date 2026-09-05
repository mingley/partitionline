//! KL-02 slice: encode / socket / task ownership honesty (not a 2×/24h RSS close).
//!
//! `bytes_buffered` holds key+value from accept through broker ack (including while
//! the Produce request is on the wire). `requests_in_flight` counts Produce requests
//! awaiting a response. Encode scratch and OS sockets are outside `buffer_memory`.
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

use partitionline::{Acks, ProduceRecord, Producer, ProducerConfig};
use std::time::Duration;

async fn warm_metadata(producer: &Producer) {
    let _md = producer
        .send(ProduceRecord::to("t").value(&b""[..]))
        .await
        .unwrap();
    producer.flush().await.unwrap();
    assert_eq!(producer.metrics().bytes_buffered, 0);
    assert_eq!(producer.metrics().requests_in_flight, 0);
}

#[tokio::test]
async fn bytes_buffered_held_across_inflight_until_flush() {
    let mock = common::Mock::start().await;
    let producer = Producer::new(
        ProducerConfig::bootstrap([mock.addr.clone()])
            .linger(Duration::ZERO)
            .acks(Acks::All)
            .max_in_flight(1),
    )
    .await
    .unwrap();
    warm_metadata(&producer).await;

    let payload = vec![b'z'; 64];
    producer
        .try_send(ProduceRecord::to("t").value(payload))
        .unwrap();

    // Reservation is held from accept; may already be queued or on the wire.
    let buffered = producer.metrics().bytes_buffered;
    assert!(
        buffered >= 64,
        "expected key+value reservation still held before flush; got {buffered}"
    );

    producer.flush().await.unwrap();
    assert_eq!(
        producer.metrics().bytes_buffered,
        0,
        "flush/ack must release buffer_memory reservation"
    );
    assert_eq!(
        producer.metrics().requests_in_flight,
        0,
        "no Produce requests should remain in flight after flush"
    );
    producer.clone().close().await.unwrap();
}

#[tokio::test]
async fn requests_in_flight_zero_after_acks0_local_complete() {
    let mock = common::Mock::start().await;
    let producer = Producer::new(
        ProducerConfig::bootstrap([mock.addr.clone()])
            .linger(Duration::ZERO)
            .acks(Acks::None),
    )
    .await
    .unwrap();
    warm_metadata(&producer).await;

    let _md = producer
        .send(ProduceRecord::to("t").value(&b"acks0"[..]))
        .await
        .unwrap();
    // acks=0 completes locally; counter must not stick above zero after settle.
    producer.flush().await.unwrap();
    assert_eq!(producer.metrics().requests_in_flight, 0);
    assert_eq!(producer.metrics().bytes_buffered, 0);
    producer.clone().close().await.unwrap();
}

#[tokio::test]
async fn close_drains_ownership_counters() {
    let mock = common::Mock::start().await;
    let producer = Producer::new(
        ProducerConfig::bootstrap([mock.addr.clone()])
            .linger(Duration::ZERO)
            .acks(Acks::All),
    )
    .await
    .unwrap();
    warm_metadata(&producer).await;

    for _ in 0..8 {
        let _ = producer.try_send(ProduceRecord::to("t").value(&b"x"[..]));
    }
    producer.clone().close().await.unwrap();
    assert_eq!(producer.metrics().bytes_buffered, 0);
    assert_eq!(producer.metrics().requests_in_flight, 0);
}

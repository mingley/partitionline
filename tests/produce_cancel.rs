//! KL-02 slice: produce cancellation / shutdown outcome contract (mock).
//!
//! Dropping a `send` future must not be read as "never written". Buffered work
//! can still reach the broker; the caller's outcome is ambiguous until
//! `flush`/`close` settles delivery.
#![expect(
    dead_code,
    reason = "tests/common mock helpers are shared; this file uses a subset"
)]

mod common;

use partitionline::error;
use partitionline::{Error, ProduceRecord, Producer, ProducerConfig};
use std::time::Duration;
use tokio::time::{sleep, timeout};

#[tokio::test]
async fn send_completes_with_record_metadata() {
    let mock = common::Mock::start().await;
    let producer = Producer::new(
        ProducerConfig::bootstrap([mock.addr.clone()]).linger(Duration::ZERO),
    )
    .await
    .unwrap();
    let md = producer
        .send(ProduceRecord::to("t").value(&b"ok"[..]))
        .await
        .unwrap();
    assert_eq!(md.topic, "t");
    assert_eq!(md.partition, 0);
    assert!(!mock.produce_nodes().is_empty());
    assert_eq!(producer.metrics().bytes_buffered, 0);
    producer.close().await.unwrap();
}

#[tokio::test]
async fn send_fails_when_broker_returns_produce_error() {
    let mock = common::Mock::start().await;
    mock.set_produce_error(error::INVALID_RECORD);
    let producer = Producer::new(
        ProducerConfig::bootstrap([mock.addr.clone()])
            .linger(Duration::ZERO)
            .delivery_timeout(Duration::from_secs(2))
            .max_block(Duration::from_secs(2)),
    )
    .await
    .unwrap();
    let err = producer
        .send(ProduceRecord::to("t").value(&b"fail"[..]))
        .await
        .expect_err("broker produce error must fail the send future");
    assert!(
        !matches!(err, Error::Closed),
        "failed delivery is not Closed; got {err:?}"
    );
    assert_eq!(
        producer.metrics().bytes_buffered,
        0,
        "failed records must release buffer_memory"
    );
    producer.close().await.unwrap();
}

#[tokio::test]
async fn dropping_send_future_while_buffered_is_ambiguous_but_still_delivers() {
    let mock = common::Mock::start().await;
    // Long linger keeps the record buffered so we can drop before the wire send.
    let producer = Producer::new(
        ProducerConfig::bootstrap([mock.addr.clone()])
            .linger(Duration::from_secs(30))
            .delivery_timeout(Duration::from_secs(10)),
    )
    .await
    .unwrap();

    let mut send_fut = Box::pin(producer.send(ProduceRecord::to("t").value(&b"ambig"[..])));
    timeout(Duration::from_secs(5), async {
        loop {
            tokio::select! {
                biased;
                res = &mut send_fut => {
                    panic!("send completed before linger expiry: {res:?}");
                }
                _ = sleep(Duration::from_millis(10)) => {
                    if producer.metrics().bytes_buffered > 0 {
                        break;
                    }
                }
            }
        }
    })
    .await
    .expect("record should enter buffer_memory before linger fires");

    // Drop the caller future: outcome is ambiguous; worker must keep the record.
    drop(send_fut);

    producer.flush().await.unwrap();
    assert!(
        !mock.produce_nodes().is_empty(),
        "dropping send must not imply the record was never written"
    );
    assert_eq!(
        producer.metrics().bytes_buffered,
        0,
        "flush must release buffer after delivery"
    );
    producer.close().await.unwrap();
}

#[tokio::test]
async fn send_after_close_returns_closed() {
    let mock = common::Mock::start().await;
    let producer = Producer::new(
        ProducerConfig::bootstrap([mock.addr.clone()]).linger(Duration::ZERO),
    )
    .await
    .unwrap();
    let other = producer.clone();
    producer.close().await.unwrap();
    let err = other
        .send(ProduceRecord::to("t").value(&b"late"[..]))
        .await
        .expect_err("send after close must fail");
    assert!(
        matches!(err, Error::Closed),
        "expected Closed after shutdown, got {err:?}"
    );
    let err = other.try_send(ProduceRecord::to("t").value(&b"late2"[..]));
    assert!(
        matches!(err, Err(Error::Closed) | Err(Error::QueueFull)),
        "try_send after close must not silently enqueue; got {err:?}"
    );
}

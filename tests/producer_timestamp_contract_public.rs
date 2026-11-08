//! Mock-observed timestamp contracts for the isolated producer candidate.

mod common;

use partitionline::protocol::api_keys::PRODUCE;
use partitionline::{Acks, Compression, ProduceRecord, Producer, ProducerConfig, RecordMetadata};
use std::future::{poll_fn, Future};
use std::task::Poll;
use std::time::Duration;

const LIMIT: Duration = Duration::from_secs(20);
const EXPLICIT: i64 = 1_234_567;
const LOG_APPEND: i64 = 9_876_543;

#[expect(clippy::expect_used, reason = "fixture failures should fail this test")]
async fn pair(producer: &Producer, prefix: &'static str) -> (RecordMetadata, RecordMetadata) {
    let mut implicit = Box::pin(
        producer.send(
            ProduceRecord::to("t")
                .partition(0)
                .value(format!("{prefix}-implicit")),
        ),
    );
    let mut explicit = Box::pin(
        producer.send(
            ProduceRecord::to("t")
                .partition(0)
                .timestamp(EXPLICIT)
                .value(format!("{prefix}-explicit")),
        ),
    );
    assert!(poll_fn(|cx| Poll::Ready(implicit.as_mut().poll(cx)))
        .await
        .is_pending());
    assert!(poll_fn(|cx| Poll::Ready(explicit.as_mut().poll(cx)))
        .await
        .is_pending());
    let (a, b) = tokio::time::timeout(LIMIT, async {
        tokio::join!(implicit.as_mut(), explicit.as_mut())
    })
    .await
    .expect("pair deadline");
    (a.expect("implicit receipt"), b.expect("explicit receipt"))
}

#[expect(
    clippy::expect_used,
    clippy::indexing_slicing,
    reason = "fixture failures and missing observations should fail this test"
)]
async fn acknowledged(version: i16, compression: Compression) {
    let mock = common::Mock::start().await;
    mock.set_topic_partitions("t", 1);
    mock.enable_produce_timestamp_observation();
    mock.set_api_max(PRODUCE, version);
    let producer = Producer::new(
        ProducerConfig::bootstrap([mock.addr.clone()])
            .idempotent(true)
            .connections(1)
            .batch_records(2)
            .linger(Duration::ZERO)
            .compression(compression)
            .retry_backoff(Duration::from_millis(100))
            .retry_backoff_max(Duration::from_millis(100))
            .request_timeout(Duration::from_secs(10))
            .delivery_timeout(Duration::from_secs(30)),
    )
    .await
    .expect("producer setup");
    let warm = tokio::time::timeout(
        LIMIT,
        producer.send(
            ProduceRecord::to("t")
                .partition(0)
                .timestamp(42)
                .value("warm"),
        ),
    )
    .await
    .expect("warm deadline")
    .expect("warm result");
    assert_eq!((warm.offset(), warm.timestamp()), (0, 42));
    assert_eq!(mock.last_produce_version(), Some(version));
    mock.set_produce_response_delay(Duration::from_millis(100));

    let (ordinary, ordinary_explicit) = pair(&producer, "ordinary").await;
    assert_eq!((ordinary.offset(), ordinary_explicit.offset()), (1, 2));
    let attempts = mock.produce_timestamp_attempts();
    assert_eq!(attempts.len(), 2);
    let ordinary_sent = &attempts[1];
    assert_eq!(ordinary_sent.2, 1);
    assert_eq!(ordinary_sent.4.len(), 2);
    assert_eq!(ordinary_sent.3, ordinary_sent.4[0]);
    assert_eq!(ordinary_sent.4[1], EXPLICIT);
    assert_eq!(ordinary.timestamp(), ordinary_sent.4[0]);
    assert_eq!(ordinary_explicit.timestamp(), EXPLICIT);
    assert_eq!(
        mock.log_timestamps("t", 0),
        vec![(0, 42), (1, ordinary_sent.4[0]), (2, EXPLICIT)]
    );

    mock.set_produce_drop_response_times(1);
    let (retry_receipt, retry_explicit) = pair(&producer, "retry").await;
    assert_eq!((retry_receipt.offset(), retry_explicit.offset()), (3, 4));
    let attempts = mock.produce_timestamp_attempts();
    assert_eq!(attempts.len(), 4);
    let first = &attempts[2];
    let retry = &attempts[3];
    assert_eq!((first.0, first.1, first.2), (retry.0, retry.1, retry.2));
    assert_eq!(first.2, 3);
    assert_eq!(first.4.len(), 2);
    assert_eq!(first.3, first.4[0]);
    assert_eq!(first.4[1], EXPLICIT);
    assert_eq!(retry.3, first.3);
    assert_eq!(retry.4, first.4);
    assert_eq!(retry_receipt.timestamp(), first.4[0]);
    assert_eq!(retry_explicit.timestamp(), EXPLICIT);
    assert_eq!(
        mock.log_timestamps("t", 0),
        vec![
            (0, 42),
            (1, ordinary_sent.4[0]),
            (2, EXPLICIT),
            (3, first.4[0]),
            (4, EXPLICIT)
        ]
    );

    // This fixture overrides only the response; it does not model a broker
    // rewriting the log timestamp.
    mock.set_produce_logappend_override(LOG_APPEND);
    let broker_time = tokio::time::timeout(
        LIMIT,
        producer.send(
            ProduceRecord::to("t")
                .partition(0)
                .timestamp(EXPLICIT)
                .value("broker-time"),
        ),
    )
    .await
    .expect("log append deadline")
    .expect("log append result");
    assert_eq!(
        (broker_time.offset(), broker_time.timestamp()),
        (5, LOG_APPEND)
    );
    assert_eq!(mock.log_timestamps("t", 0).last(), Some(&(5, EXPLICIT)));
    println!("TIMESTAMP_ACK version={version} compression={compression:?} mixed=ok retry=ok broker_override=ok");
    let _close = producer.close_timeout(Duration::from_secs(2)).await;
}

#[expect(
    clippy::expect_used,
    clippy::indexing_slicing,
    reason = "fixture failures and missing observations should fail this test"
)]
async fn unacknowledged(version: i16, compression: Compression) {
    let mock = common::Mock::start().await;
    mock.set_topic_partitions("t", 1);
    mock.enable_produce_timestamp_observation();
    mock.set_api_max(PRODUCE, version);
    let producer = Producer::new(
        ProducerConfig::bootstrap([mock.addr.clone()])
            .acks(Acks::None)
            .connections(1)
            .batch_records(2)
            .linger(Duration::ZERO)
            .compression(compression)
            .request_timeout(Duration::from_secs(10))
            .delivery_timeout(Duration::from_secs(30)),
    )
    .await
    .expect("producer setup");
    // Prepare the route without relying on a previous acks=0 response.
    let warm = tokio::time::timeout(
        LIMIT,
        producer.send(
            ProduceRecord::to("t")
                .partition(0)
                .timestamp(42)
                .value("warm"),
        ),
    )
    .await
    .expect("warm deadline")
    .expect("warm result");
    assert_eq!((warm.offset(), warm.timestamp()), (-1, 42));
    tokio::time::timeout(LIMIT, async {
        while mock.produce_timestamp_attempts().is_empty() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("warm mock observation");
    let (implicit, explicit) = pair(&producer, "acks0").await;
    assert_eq!((implicit.offset(), explicit.offset()), (-1, -1));
    tokio::time::timeout(LIMIT, async {
        while mock.produce_timestamp_attempts().len() < 2 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("acks0 mock observation");
    let attempts = mock.produce_timestamp_attempts();
    assert_eq!(attempts.len(), 2);
    let sent = &attempts[1];
    assert_eq!(sent.4.len(), 2);
    assert_eq!(sent.3, sent.4[0]);
    assert_eq!(sent.4[1], EXPLICIT);
    assert_eq!(implicit.timestamp(), sent.4[0]);
    assert_eq!(explicit.timestamp(), EXPLICIT);
    println!("TIMESTAMP_ACKS0 version={version} compression={compression:?} mixed=ok");
    let _close = producer.close_timeout(Duration::from_secs(2)).await;
}

#[tokio::test(flavor = "current_thread")]
async fn producer_timestamp_contracts_across_encoders() {
    let variants = [
        (3, Compression::None),
        (3, Compression::Gzip),
        (9, Compression::None),
        (9, Compression::Gzip),
        (9, Compression::Snappy),
        (9, Compression::Lz4),
        (13, Compression::None),
        (13, Compression::Gzip),
    ];
    for (version, compression) in variants {
        acknowledged(version, compression).await;
        unacknowledged(version, compression).await;
    }
    #[cfg(feature = "zstd")]
    {
        acknowledged(9, Compression::Zstd).await;
        unacknowledged(9, Compression::Zstd).await;
    }
}

//! Test-only mock observation; the producer source is unchanged.
#![cfg(producer_timestamp_witness)]

mod common;

use partitionline::{ProduceRecord, Producer, ProducerConfig};
use std::time::Duration;

const LIMIT: Duration = Duration::from_secs(20);

#[tokio::test(flavor = "current_thread")]
async fn producer_retry_timestamp_observation() {
    let mock = common::Mock::start().await;
    mock.set_topic_partitions("t", 1);
    let producer = Producer::new(
        ProducerConfig::bootstrap([mock.addr.clone()])
            .idempotent(true)
            .connections(1)
            .batch_records(1)
            .linger(Duration::ZERO)
            .retry_backoff(Duration::from_millis(500))
            .retry_backoff_max(Duration::from_millis(500))
            .request_timeout(Duration::from_secs(10))
            .delivery_timeout(Duration::from_secs(30)),
    )
    .await
    .expect("producer setup");
    let warm = tokio::time::timeout(
        LIMIT,
        producer.send(ProduceRecord::to("t").partition(0).timestamp(42).value("warm")),
    )
    .await
    .expect("warm deadline")
    .expect("warm result");
    assert_eq!((warm.offset(), warm.timestamp()), (0, 42));
    mock.set_produce_response_delay(Duration::from_millis(200));
    let ordinary = tokio::time::timeout(
        LIMIT,
        producer.send(ProduceRecord::to("t").partition(0).value("ordinary")),
    )
    .await
    .expect("ordinary deadline")
    .expect("ordinary result");
    assert_eq!(ordinary.offset(), 1);
    let ordinary_attempts = mock.produce_timestamp_attempts();
    assert_eq!(ordinary_attempts.len(), 2);
    let ordinary_original = ordinary_attempts[1];
    assert_eq!(ordinary_original.2, 1);
    assert_eq!(ordinary_original.4, Some(ordinary_original.3));
    assert_eq!(mock.log_timestamps("t", 0), vec![(0, 42), (1, ordinary_original.3)]);
    assert_ne!(ordinary.timestamp(), ordinary_original.3, "receipt is sampled after first encoding");
    println!(
        "TIMESTAMP_FIRST original={} receipt={} log={} offset=1",
        ordinary_original.3,
        ordinary.timestamp(),
        ordinary_original.3
    );

    mock.set_produce_drop_response_times(1);
    let implicit = tokio::time::timeout(
        LIMIT,
        producer.send(ProduceRecord::to("t").partition(0).value("implicit")),
    )
    .await
    .expect("implicit deadline")
    .expect("implicit result");
    assert_eq!(implicit.offset(), 2);
    let attempts = mock.produce_timestamp_attempts();
    assert_eq!(attempts.len(), 4);
    let original = attempts[2];
    let retry = attempts[3];
    assert_eq!((original.0, original.1, original.2), (retry.0, retry.1, retry.2));
    assert_eq!(original.2, 2);
    assert_eq!(original.4, Some(original.3));
    assert_eq!(retry.4, Some(retry.3));
    assert_ne!(original.3, retry.3, "implicit timestamp changed across retry");
    let log = mock.log_timestamps("t", 0);
    assert_eq!(log, vec![(0, 42), (1, ordinary_original.3), (2, original.3)]);
    assert_ne!(implicit.timestamp(), original.3, "receipt is not the stored create time");
    assert_ne!(implicit.timestamp(), retry.3, "receipt is sampled after retry encoding");
    println!(
        "TIMESTAMP_IMPLICIT original={} retry={} receipt={} log={} offset=2",
        original.3,
        retry.3,
        implicit.timestamp(),
        log[2].1
    );

    const EXPLICIT: i64 = 1_234_567;
    mock.set_produce_drop_response_times(1);
    let explicit = tokio::time::timeout(
        LIMIT,
        producer.send(ProduceRecord::to("t").partition(0).timestamp(EXPLICIT).value("explicit")),
    )
    .await
    .expect("explicit deadline")
    .expect("explicit result");
    assert_eq!((explicit.offset(), explicit.timestamp()), (3, EXPLICIT));
    let attempts = mock.produce_timestamp_attempts();
    assert_eq!(attempts.len(), 6);
    let explicit_original = attempts[4];
    let explicit_retry = attempts[5];
    assert_eq!(
        (explicit_original.0, explicit_original.1, explicit_original.2),
        (explicit_retry.0, explicit_retry.1, explicit_retry.2)
    );
    assert_eq!(explicit_original.2, 3);
    assert_eq!((explicit_original.3, explicit_original.4), (EXPLICIT, Some(EXPLICIT)));
    assert_eq!((explicit_retry.3, explicit_retry.4), (EXPLICIT, Some(EXPLICIT)));
    assert_eq!(mock.log_timestamps("t", 0), vec![(0, 42), (1, ordinary_original.3), (2, original.3), (3, EXPLICIT)]);
    println!("TIMESTAMP_EXPLICIT original=1234567 retry=1234567 receipt=1234567 log=1234567 offset=3");
    let _close = producer.close_timeout(Duration::from_secs(2)).await;
}

//! Matched mock producer timing driver for an isolated candidate.
mod common;

use bytes::Bytes;
use partitionline::{ProduceRecord, Producer, ProducerConfig};
use std::time::{Duration, Instant};

const BATCH: usize = 4096;

#[tokio::test(flavor = "current_thread")]
#[ignore]
async fn timestamp_producer_sample() {
    let mode = std::env::var("PL_BENCH_MODE").expect("PL_BENCH_MODE");
    assert!(mode == "implicit" || mode == "explicit");
    let rounds: usize = std::env::var("PL_BENCH_ROUNDS")
        .expect("PL_BENCH_ROUNDS")
        .parse()
        .expect("integer rounds");
    assert!((1..=1024).contains(&rounds));
    let mock = common::Mock::start().await;
    mock.set_topic_partitions("t", 1);
    let producer = Producer::new(
        ProducerConfig::bootstrap([mock.addr.clone()])
            .idempotent(true)
            .connections(1)
            .batch_records(BATCH)
            .batch_bytes(2 * 1024 * 1024)
            .max_request_size(2 * 1024 * 1024)
            .buffer_memory(0)
            .linger(Duration::ZERO)
            .request_timeout(Duration::from_secs(30))
            .delivery_timeout(Duration::from_secs(60)),
    )
    .await
    .expect("producer setup");
    let warm = producer
        .send(ProduceRecord::to("t").partition(0).timestamp(42).value("warm"))
        .await
        .expect("warm");
    assert_eq!(warm.offset(), 0);

    let started = Instant::now();
    for batch in 0..rounds {
        for item in 0..BATCH {
            let index = u64::try_from(batch * BATCH + item).unwrap();
            let record = ProduceRecord::to("t")
                .partition(0)
                .value(Bytes::copy_from_slice(&index.to_le_bytes()));
            let record = if mode == "explicit" { record.timestamp(1234567) } else { record };
            producer.try_send(record).expect("accepted batch");
        }
        producer.flush_timeout(Duration::from_secs(30)).await.expect("flush");
    }
    let elapsed = started.elapsed();
    let total = rounds * BATCH;
    let log = mock.log_values("t", 0);
    assert_eq!(log.len(), total + 1);
    assert_eq!(log.first(), Some(&(0, Some(b"warm".to_vec()))));
    for (index, entry) in log.iter().skip(1).enumerate() {
        let expected = u64::try_from(index).unwrap().to_le_bytes();
        assert_eq!(entry.0, i64::try_from(index + 1).unwrap());
        assert_eq!(entry.1.as_deref(), Some(expected.as_slice()));
    }
    let metrics = producer.metrics();
    assert_eq!(metrics.records_acked, u64::try_from(total + 1).unwrap());
    assert_eq!(metrics.produce_errors, 0);
    assert_eq!(metrics.bytes_buffered, 0);
    println!("TIMESTAMP_BENCH_PRODUCER mode={mode} rounds={rounds} records={total} elapsed_ns={}", elapsed.as_nanos());
    let _close = producer.close_timeout(Duration::from_secs(5)).await;
}

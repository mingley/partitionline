//! Exploratory same-client completion-draft timing against a local mock.
//! The mock uses this repository's codec, so this is not an independent
//! Kafka comparison or a production performance qualification.

#![expect(
    clippy::unwrap_used,
    clippy::panic,
    clippy::indexing_slicing,
    dead_code,
    reason = "qualification-only timing fixture"
)]

mod common;

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use partitionline::interceptor::ProducerInterceptor;
use partitionline::{Error, ProduceRecord, Producer, ProducerConfig, RecordMetadata};

const RECORDS: usize = 4096;

#[derive(Clone)]
struct Audit {
    acknowledged: Arc<AtomicUsize>,
    errors: Arc<AtomicUsize>,
}

impl ProducerInterceptor for Audit {
    fn on_ack(&self, _metadata: &RecordMetadata) {
        let _previous = self.acknowledged.fetch_add(1, Ordering::SeqCst);
    }

    fn on_error(&self, _error: &Error) {
        let _previous = self.errors.fetch_add(1, Ordering::SeqCst);
    }
}

#[tokio::test(flavor = "current_thread")]
async fn timed_admission_and_flush_with_mock_readback() {
    let mock = common::Mock::start().await;
    mock.set_topic_partitions("completion-timing", 1);
    let acknowledged = Arc::new(AtomicUsize::new(0));
    let errors = Arc::new(AtomicUsize::new(0));
    let config = ProducerConfig::bootstrap([mock.addr.clone()])
        .connections(1)
        .batch_records(8192)
        .batch_bytes(1_000_000)
        .linger(Duration::from_secs(30))
        .request_timeout(Duration::from_secs(15))
        .delivery_timeout(Duration::from_secs(30))
        .interceptor(Audit {
            acknowledged: Arc::clone(&acknowledged),
            errors: Arc::clone(&errors),
        });
    let producer = Producer::new(config).await.unwrap();
    let warmup_deadline = Instant::now() + Duration::from_secs(10);
    loop {
        match producer.try_send(
            ProduceRecord::to("completion-timing")
                .partition(0)
                .value("warm"),
        ) {
            Ok(()) => break,
            Err(Error::QueueFull) if Instant::now() < warmup_deadline => {
                tokio::task::yield_now().await;
            }
            Err(error) => panic!("warmup admission failed: {error:?}"),
        }
    }
    producer.flush_timeout(Duration::from_secs(15)).await.unwrap();
    assert_eq!(acknowledged.load(Ordering::SeqCst), 1);
    assert_eq!(errors.load(Ordering::SeqCst), 0);
    assert_eq!(producer.metrics().bytes_buffered, 0);

    let records = (0..RECORDS)
        .map(|index| {
            let value = u64::try_from(index).unwrap().to_le_bytes().to_vec();
            ProduceRecord::to("completion-timing")
                .partition(0)
                .value(value)
        })
        .collect::<Vec<_>>();

    let admission_start = Instant::now();
    for record in records {
        producer.try_send(record).unwrap();
    }
    let admission_ns = admission_start.elapsed().as_nanos();
    let flush_start = Instant::now();
    producer.flush_timeout(Duration::from_secs(15)).await.unwrap();
    let flush_ns = flush_start.elapsed().as_nanos();

    let metrics = producer.metrics();
    assert_eq!(metrics.records_queued, u64::try_from(RECORDS + 1).unwrap());
    assert_eq!(metrics.records_acked, u64::try_from(RECORDS + 1).unwrap());
    assert_eq!(metrics.produce_errors, 0);
    assert_eq!(metrics.bytes_buffered, 0);
    assert_eq!(acknowledged.load(Ordering::SeqCst), RECORDS + 1);
    assert_eq!(errors.load(Ordering::SeqCst), 0);

    let actual = mock.log_values("completion-timing", 0);
    assert_eq!(actual.len(), RECORDS + 1);
    assert_eq!(actual[0], (0, Some(b"warm".to_vec())));
    for (index, (offset, value)) in actual.iter().enumerate().skip(1) {
        assert_eq!(*offset, i64::try_from(index).unwrap());
        let expected = u64::try_from(index - 1).unwrap().to_le_bytes();
        assert_eq!(value.as_deref(), Some(expected.as_slice()));
    }
    producer.close_timeout(Duration::from_secs(15)).await.unwrap();
    println!(
        "TIMING_JSON {{\"records\":{RECORDS},\"admission_ns\":{admission_ns},\"flush_ns\":{flush_ns}}}"
    );
}

//! Controlled public regression for accepted producer work during worker cancellation.
//! The witness gate delays an existing sequence-stamp yield; it changes scheduling.
#![cfg(sequence_owner_witness)]

mod common;

use partitionline::producer::sequence_owner_witness as gate;
use partitionline::{
    Error, ProduceRecord, Producer, ProducerConfig, ProducerInterceptor, RecordMetadata,
};
use std::future::{poll_fn, Future};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::task::Poll;
use std::time::Duration;

const COUNT: usize = 8193;
const LIMIT: Duration = Duration::from_secs(30);

#[derive(Clone, Default)]
struct Counts {
    acks: Arc<AtomicUsize>,
    errors: Arc<AtomicUsize>,
}

impl ProducerInterceptor for Counts {
    fn on_ack(&self, _metadata: &RecordMetadata) {
        let _ = self.acks.fetch_add(1, Ordering::SeqCst);
    }

    fn on_error(&self, _error: &Error) {
        let _ = self.errors.fetch_add(1, Ordering::SeqCst);
    }
}

struct ReleaseGate;

impl Drop for ReleaseGate {
    fn drop(&mut self) {
        gate::release();
    }
}

#[tokio::test(flavor = "current_thread")]
async fn accepted_records_settle_after_worker_cancellation() {
    let case = std::env::var("PL_OWNER_CASE").unwrap_or_else(|_| "abort".to_string());
    let admission = std::env::var("PL_ADMISSION").unwrap_or_else(|_| "all".to_string());
    assert!(matches!(case.as_str(), "abort" | "close"));
    assert!(matches!(admission.as_str(), "all" | "mixed"));
    let mock = common::Mock::start().await;
    mock.set_topic_partitions("t", 1);
    let counts = Counts::default();
    let producer = Producer::new(
        ProducerConfig::bootstrap([mock.addr.clone()])
            .idempotent(true)
            .connections(1)
            .batch_records(COUNT)
            .batch_bytes(2 * 1024 * 1024)
            .max_request_size(2 * 1024 * 1024)
            .buffer_memory(0)
            .linger(Duration::ZERO)
            .request_timeout(Duration::from_secs(20))
            .delivery_timeout(Duration::from_secs(120))
            .max_block(Duration::from_secs(20))
            .interceptor(counts.clone()),
    )
    .await
    .expect("producer setup");
    let warm = tokio::time::timeout(
        LIMIT,
        producer.send(ProduceRecord::to("t").partition(0).value(&b"warm"[..])),
    )
    .await
    .expect("warmup deadline")
    .expect("warmup result");
    assert_eq!(warm.offset, 0);
    assert_eq!(counts.acks.load(Ordering::SeqCst), 1);
    assert_eq!(mock.produce_sequences().len(), 1);

    gate::arm();
    let _release = ReleaseGate;
    let mut deliveries = Vec::new();
    for index in 0..COUNT {
        let value = u64::try_from(index).expect("index fits").to_le_bytes();
        let record = ProduceRecord::to("t").partition(0).value(value.to_vec());
        if admission == "all" || index == 0 {
            let mut delivery = Box::pin(producer.send(record));
            let first_poll = poll_fn(|cx| Poll::Ready(delivery.as_mut().poll(cx))).await;
            assert!(first_poll.is_pending(), "send must be admitted and await its receipt");
            deliveries.push((index, delivery));
        } else {
            producer.try_send(record).expect("try_send before worker runs");
        }
    }
    let before = producer.metrics();
    assert_eq!(before.records_queued, u64::try_from(COUNT + 1).expect("count fits"));
    assert_eq!(before.bytes_buffered, u64::try_from(COUNT * 8).expect("bytes fit"));
    tokio::time::timeout(LIMIT, gate::reached())
        .await
        .expect("sequence gate was not reached");
    assert_eq!(gate::snapshot(), (1, COUNT, 8192, true));
    assert_eq!(mock.log_len("t", 0), 1, "the gated batch has not been written");
    assert_eq!(mock.produce_sequences().len(), 1);

    let workers = producer.test_worker_tasks();
    assert_eq!(workers.len(), 1);
    let worker = workers.first().expect("one worker");
    if case == "abort" {
        worker.abort();
    } else {
        let result = tokio::time::timeout(
            LIMIT,
            producer.clone().close_timeout(Duration::from_millis(150)),
        )
        .await
        .expect("close deadline");
        println!("REGRESSION_CLOSE_RESULT {result:?}");
    }
    tokio::time::timeout(LIMIT, async {
        while !worker.is_finished() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("old worker stop deadline");

    let outcomes = tokio::time::timeout(Duration::from_secs(10), async {
        let mut results = Vec::with_capacity(deliveries.len());
        for (index, delivery) in &mut deliveries {
            results.push((*index, delivery.as_mut().await));
        }
        results
    })
    .await
    .expect("REGRESSION_RECEIPTS_DID_NOT_SETTLE");
    let receipt_ok = outcomes.iter().filter(|(_, result)| result.is_ok()).count();
    let receipt_err = outcomes.len() - receipt_ok;
    let settled = tokio::time::timeout(Duration::from_secs(4), async {
        loop {
            let acks = counts.acks.load(Ordering::SeqCst).saturating_sub(1);
            let errors = counts.errors.load(Ordering::SeqCst);
            if acks.saturating_add(errors) >= COUNT && producer.metrics().bytes_buffered == 0 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await;
    let acks = counts.acks.load(Ordering::SeqCst).saturating_sub(1);
    let errors = counts.errors.load(Ordering::SeqCst);
    let buffered = producer.metrics().bytes_buffered;
    println!(
        "OWNERSHIP_REGRESSION case={case} admission={admission} receipts={} ok={receipt_ok} err={receipt_err} acks={acks} errors={errors} bytes={buffered}",
        outcomes.len()
    );
    assert!(
        settled.is_ok() && acks.saturating_add(errors) == COUNT && buffered == 0,
        "OWNERSHIP_REGRESSION accepted records must each reach a terminal callback and release bytes"
    );
    assert!(acks >= receipt_ok && errors >= receipt_err);
    if admission == "all" {
        assert_eq!((acks, errors), (receipt_ok, receipt_err));
    }

    let log = mock.log_values("t", 0);
    for (index, outcome) in &outcomes {
        if let Ok(metadata) = outcome {
            let at = usize::try_from(metadata.offset).expect("nonnegative offset");
            let expected = u64::try_from(*index).expect("index fits").to_le_bytes();
            assert_eq!(
                log.get(at),
                Some(&(metadata.offset, Some(expected.to_vec()))),
                "a successful receipt must match the mock's retained value"
            );
        }
    }

    drop(outcomes);
    drop(deliveries);
    if case == "abort" {
        let later = tokio::time::timeout(
            Duration::from_secs(5),
            producer.send(ProduceRecord::to("t").partition(0).value(&b"after000"[..])),
        )
        .await;
        if let Ok(Err(error)) = later {
            assert_ne!(
                error.broker_code(),
                Some(partitionline::error::OUT_OF_ORDER_SEQUENCE_NUMBER),
                "REGRESSION_LATER_SEQUENCE_GAP"
            );
        }
    }
    let _close = producer.close_timeout(Duration::from_secs(2)).await;
}

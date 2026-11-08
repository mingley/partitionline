//! Public flush history at an instrumented, real retry handoff.
//!
//! The producer patch used by this qualification adds only a cfg-gated timing
//! observer. The mock appends one idempotent Produce, closes its socket before
//! replying, and the observer pauses the actual retired worker after the
//! existing cooperative retry split and before retry accounting.

#![cfg(flush_retirement_witness)]

mod common;

use partitionline::producer::flush_retirement_witness as gate;
use partitionline::{
    Error, ProduceRecord, Producer, ProducerConfig, ProducerInterceptor, RecordMetadata,
};
use std::future::{poll_fn, Future};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::task::Poll;
use std::time::Duration;

#[derive(Clone)]
struct CallbackCounts {
    acknowledgements: Arc<AtomicUsize>,
    errors: Arc<AtomicUsize>,
}

impl ProducerInterceptor for CallbackCounts {
    fn on_ack(&self, _metadata: &RecordMetadata) {
        let _previous = self.acknowledgements.fetch_add(1, Ordering::SeqCst);
    }

    fn on_error(&self, _error: &Error) {
        let _previous = self.errors.fetch_add(1, Ordering::SeqCst);
    }
}

struct ReleaseGate;

impl Drop for ReleaseGate {
    fn drop(&mut self) {
        gate::release();
    }
}

#[tokio::test(flavor = "current_thread")]
async fn public_flush_can_complete_before_retired_worker_retry_handoff() {
    const TARGET: &[u8] = b"target";
    let mock = common::Mock::start().await;
    mock.set_topic_partitions("t", 1);
    let callbacks = CallbackCounts {
        acknowledgements: Arc::new(AtomicUsize::new(0)),
        errors: Arc::new(AtomicUsize::new(0)),
    };
    let producer = tokio::time::timeout(
        Duration::from_secs(15),
        Producer::new(
            ProducerConfig::bootstrap([mock.addr.clone()])
                .client_id("flush-retirement-witness")
                .connections(1)
                .linger(Duration::ZERO)
                .idempotent(true)
                .request_timeout(Duration::from_secs(10))
                .delivery_timeout(Duration::from_secs(30))
                .max_block(Duration::from_secs(10))
                .interceptor(callbacks.clone()),
        ),
    )
    .await
    .expect("producer setup deadline")
    .expect("producer setup");
    let warm = tokio::time::timeout(
        Duration::from_secs(10),
        producer.send(ProduceRecord::to("t").partition(0).value(&b"warm"[..])),
    )
    .await
    .expect("warmup deadline")
    .expect("warmup acknowledgement");
    assert_eq!(warm.offset, 0);
    assert_eq!(callbacks.acknowledgements.load(Ordering::SeqCst), 1);
    assert_eq!(mock.log_len("t", 0), 1);
    assert_eq!(mock.produce_batches().len(), 1);

    // A replacement worker must negotiate ApiVersions. Delay that reply so the
    // observer can inspect the empty published-worker map while the old one is
    // paused. The delay does not change producer state.
    mock.set_api_versions_delay(Duration::from_secs(5));
    mock.set_produce_drop_response_times(1);
    gate::install();
    let release_on_drop = ReleaseGate;

    let mut delivery = Box::pin(producer.send(ProduceRecord::to("t").partition(0).value(TARGET)));
    let first = poll_fn(|cx| Poll::Ready(delivery.as_mut().poll(cx))).await;
    assert!(first.is_pending(), "delivery must remain outstanding");
    let admitted = producer.metrics();
    assert_eq!(admitted.records_queued, 2, "target was admitted");
    assert_eq!(admitted.records_acked, 1);
    assert_eq!(admitted.produce_errors, 0);
    assert_eq!(admitted.bytes_buffered, TARGET.len() as u64);

    tokio::time::timeout(Duration::from_secs(10), gate::observed())
        .await
        .expect("real retriable read did not reach retired handoff");
    assert!(
        producer.test_worker_tasks().is_empty(),
        "replacement already published"
    );
    assert_eq!(producer.retries_in_flight(), 0);
    assert_eq!(mock.log_len("t", 0), 2, "mock appended the target");
    assert_eq!(
        mock.log_values("t", 0),
        vec![(0, Some(b"warm".to_vec())), (1, Some(TARGET.to_vec()))]
    );
    assert_eq!(
        mock.produce_batches().len(),
        2,
        "warmup and target were sent"
    );
    assert_eq!(callbacks.acknowledgements.load(Ordering::SeqCst), 1);
    assert_eq!(callbacks.errors.load(Ordering::SeqCst), 0);
    let before_flush = producer.metrics();
    assert_eq!(before_flush.records_queued, 2);
    assert_eq!(before_flush.records_acked, 1);
    assert_eq!(before_flush.produce_errors, 0);
    assert_eq!(before_flush.bytes_buffered, TARGET.len() as u64);
    let pending = poll_fn(|cx| Poll::Ready(delivery.as_mut().poll(cx))).await;
    assert!(pending.is_pending(), "target receipt finished before flush");

    // No worker is published and no retry has been counted. This must return
    // Ok in the pinned baseline even while the actual old worker owns target.
    let mut flush = Box::pin(producer.flush_timeout(Duration::from_secs(8)));
    let flush_poll = poll_fn(|cx| Poll::Ready(flush.as_mut().poll(cx))).await;
    assert!(matches!(flush_poll, Poll::Ready(Ok(()))), "{flush_poll:?}");
    drop(flush);
    let still_pending = poll_fn(|cx| Poll::Ready(delivery.as_mut().poll(cx))).await;
    assert!(
        still_pending.is_pending(),
        "target receipt finished during flush"
    );
    assert_eq!(producer.metrics().bytes_buffered, TARGET.len() as u64);

    // Resume the ordinary retry and independently verify the idempotent
    // broker log and the per-record terminal acknowledgement.
    mock.set_api_versions_delay(Duration::ZERO);
    gate::release();
    drop(release_on_drop);
    let result = tokio::time::timeout(Duration::from_secs(20), delivery)
        .await
        .expect("retry delivery deadline")
        .expect("retry terminal result");
    assert_eq!(result.partition, 0);
    assert_eq!(result.offset, 1);
    let after = producer.metrics();
    assert_eq!(after.records_queued, 2);
    assert_eq!(after.records_acked, 2);
    assert_eq!(after.produce_errors, 0);
    assert_eq!(after.bytes_buffered, 0);
    assert_eq!(callbacks.acknowledgements.load(Ordering::SeqCst), 2);
    assert_eq!(callbacks.errors.load(Ordering::SeqCst), 0);
    assert_eq!(
        mock.log_len("t", 0),
        2,
        "idempotent retry must not duplicate"
    );
    assert_eq!(
        mock.log_values("t", 0),
        vec![(0, Some(b"warm".to_vec())), (1, Some(TARGET.to_vec()))]
    );
    let batches = mock.produce_batches();
    assert_eq!(batches.len(), 3, "warmup, dropped reply, and retry");
    assert!(batches
        .iter()
        .all(|(topic, partition, count, _)| { topic == "t" && *partition == 0 && *count == 1 }));
    let sequences = mock.produce_sequences();
    assert_eq!(sequences.len(), 3);
    assert_eq!(
        sequences[1], sequences[2],
        "retry keeps producer identity and sequence"
    );

    println!("PUBLIC_RETIREMENT_HISTORY admitted=1 broker_appended=1 response_dropped=1 retired=1 retries_before_handoff=0 flush=ok receipt_during_flush=pending terminal=ack callbacks=1 target_log_entries=1");

    tokio::time::timeout(
        Duration::from_secs(12),
        producer.close_timeout(Duration::from_secs(10)),
    )
    .await
    .expect("close harness deadline")
    .expect("close after witness");
}

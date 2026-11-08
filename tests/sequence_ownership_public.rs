//! Public producer sequence-ownership witness; qualification-only.
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
const VALUE_SIZE: u64 = 8;
const LIMIT: Duration = Duration::from_secs(30);

#[derive(Clone, Default)]
struct Counts {
    acks: Arc<AtomicUsize>,
    errors: Arc<AtomicUsize>,
}

impl ProducerInterceptor for Counts {
    fn on_ack(&self, _metadata: &RecordMetadata) {
        let _previous = self.acks.fetch_add(1, Ordering::SeqCst);
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
async fn public_batch_sequence_worker_cancellation() {
    let mode = std::env::var("PL_SEQUENCE_MODE").unwrap_or_else(|_| "resume".to_string());
    assert!(matches!(mode.as_str(), "resume" | "abort" | "close"));
    let mock = common::Mock::start().await;
    mock.set_topic_partitions("t", 1);
    let callbacks = Counts::default();
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
            .interceptor(callbacks.clone()),
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
    assert_eq!(callbacks.acks.load(Ordering::SeqCst), 1);
    let warm_sequences = mock.produce_sequences();
    assert_eq!(warm_sequences.len(), 1);
    let identity = (producer.__test_producer_id(), producer.__test_producer_epoch());
    assert_eq!((warm_sequences[0].0, warm_sequences[0].1), identity);
    assert_eq!((warm_sequences[0].2, warm_sequences[0].3), (0, 1));

    gate::arm();
    let _release = ReleaseGate;
    let mut delivery =
        Box::pin(producer.send(ProduceRecord::to("t").partition(0).value(&b"target00"[..])));
    let first = poll_fn(|cx| Poll::Ready(delivery.as_mut().poll(cx))).await;
    assert!(first.is_pending(), "first record must be admitted and await delivery");
    for index in 0..COUNT - 1 {
        let value = u64::try_from(index).expect("index fits u64").to_le_bytes();
        producer
            .try_send(ProduceRecord::to("t").partition(0).value(value.to_vec()))
            .expect("remaining records admitted before worker runs");
    }
    let before = producer.metrics();
    assert_eq!(before.records_queued, 1 + u64::try_from(COUNT).expect("count fits"));
    assert_eq!(before.records_acked, 1);
    assert_eq!(before.produce_errors, 0);
    assert_eq!(before.bytes_buffered, u64::try_from(COUNT).expect("count fits") * VALUE_SIZE);

    tokio::time::timeout(LIMIT, gate::reached())
        .await
        .expect("worker did not reach the 8192-record stamp yield");
    assert_eq!(gate::snapshot(), (1, COUNT, 8192, true));
    assert_eq!(
        (producer.__test_producer_id(), producer.__test_producer_epoch()),
        identity
    );
    assert_eq!(mock.log_len("t", 0), 1, "target batch must not yet be written");
    assert_eq!(mock.produce_sequences().len(), 1);
    assert_eq!(callbacks.acks.load(Ordering::SeqCst), 1);
    assert_eq!(callbacks.errors.load(Ordering::SeqCst), 0);

    if mode == "resume" {
        gate::release();
        let result = tokio::time::timeout(LIMIT, delivery.as_mut())
            .await
            .expect("target delivery deadline")
            .expect("target receipt");
        assert_eq!(result.offset, 1);
        tokio::time::timeout(LIMIT, producer.flush())
            .await
            .expect("flush deadline")
            .expect("flush");
        let metrics = producer.metrics();
        assert_eq!(metrics.records_acked, 1 + u64::try_from(COUNT).expect("count fits"));
        assert_eq!(metrics.produce_errors, 0);
        assert_eq!(metrics.bytes_buffered, 0);
        assert_eq!(callbacks.acks.load(Ordering::SeqCst), COUNT + 1);
        assert_eq!(callbacks.errors.load(Ordering::SeqCst), 0);
        let log = mock.log_values("t", 0);
        assert_eq!(log.len(), COUNT + 1);
        assert_eq!(log.first(), Some(&(0, Some(b"warm".to_vec()))));
        assert_eq!(log.get(1), Some(&(1, Some(b"target00".to_vec()))));
        for (index, (offset, value)) in log.iter().skip(2).enumerate() {
            assert_eq!(*offset, i64::try_from(index + 2).expect("offset fits"));
            let expected = u64::try_from(index).expect("index fits").to_le_bytes();
            assert_eq!(value.as_deref(), Some(expected.as_slice()));
        }
        let seqs = mock.produce_sequences();
        assert_eq!(seqs.len(), 2);
        assert_eq!((seqs[1].2, seqs[1].3), (1, i32::try_from(COUNT).expect("count fits")));
        println!("SEQUENCE_PUBLIC_CONTROL resumed=1 receipts=ack callbacks={} bytes=0 batch_base=1 batch_count={COUNT}", COUNT);
    } else {
        let workers = producer.test_worker_tasks();
        assert_eq!(workers.len(), 1);
        let worker = workers.first().expect("one worker");
        let old_task_id = worker.id();
        if mode == "abort" {
            worker.abort();
        } else {
            let result = tokio::time::timeout(
                LIMIT,
                producer.clone().close_timeout(Duration::from_millis(150)),
            )
            .await
            .expect("public close deadline");
            assert!(matches!(result, Err(Error::Timeout)), "close result: {result:?}");
        }
        tokio::time::timeout(LIMIT, async {
            while !worker.is_finished() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("worker abort deadline");
        let result = tokio::time::timeout(LIMIT, delivery.as_mut())
            .await
            .expect("receipt completion after task abort");
        assert!(matches!(result, Err(Error::Closed)), "receipt result: {result:?}");
        let after = producer.metrics();
        assert_eq!(after.records_queued, before.records_queued);
        assert_eq!(after.records_acked, 1);
        assert_eq!(after.produce_errors, 0);
        assert_eq!(after.bytes_buffered, before.bytes_buffered);
        assert_eq!(callbacks.acks.load(Ordering::SeqCst), 1);
        assert_eq!(callbacks.errors.load(Ordering::SeqCst), 0);
        assert_eq!(mock.log_len("t", 0), 1);

        if mode == "close" {
            assert!(matches!(
                producer.try_send(ProduceRecord::to("t").partition(0).value(&b"later000"[..])),
                Err(Error::Closed)
            ));
            println!(
                "SEQUENCE_PUBLIC_CLOSE close=timeout worker_aborted=1 receipt=closed callbacks_missing={COUNT} bytes_retained={}",
                before.bytes_buffered
            );
            return;
        }

        // Hold the mock response after it has recorded the new Produce so
        // the actual replacement task can be observed before handling error 45.
        mock.set_produce_response_delay(Duration::from_secs(2));
        let mut later_delivery =
            Box::pin(producer.send(ProduceRecord::to("t").partition(0).value(&b"later000"[..])));
        assert!(poll_fn(|cx| Poll::Ready(later_delivery.as_mut().poll(cx))).await.is_pending());
        tokio::time::timeout(LIMIT, async {
            tokio::select! {
                biased;
                () = async {
                    while mock.produce_sequences().len() < 2 {
                        tokio::time::sleep(Duration::from_millis(1)).await;
                    }
                } => {}
                result = &mut later_delivery => {
                    panic!("later delivery finished before mock observation: {result:?}");
                }
            }
        })
        .await
        .expect("replacement worker did not write a later batch");
        let replacement_workers = producer.test_worker_tasks();
        assert!(
            replacement_workers
                .iter()
                .any(|worker| worker.id() != old_task_id && !worker.is_finished()),
            "a different live worker must own the later Produce"
        );
        let later = tokio::time::timeout(LIMIT, later_delivery.as_mut())
            .await
            .expect("replacement-worker delivery deadline")
            .expect_err("the mock enforces contiguous idempotent sequences");
        assert_eq!(
            later.broker_code(),
            Some(partitionline::error::OUT_OF_ORDER_SEQUENCE_NUMBER)
        );
        let seqs = mock.produce_sequences();
        assert_eq!(seqs.len(), 2);
        assert_eq!(seqs[0].0, seqs[1].0);
        assert_eq!(seqs[0].1, seqs[1].1);
        assert_eq!((seqs[0].2, seqs[0].3), (0, 1));
        assert_eq!((seqs[1].2, seqs[1].3), (8194, 1));
        assert_eq!(mock.log_len("t", 0), 1);
        assert_eq!(callbacks.acks.load(Ordering::SeqCst), 1);
        assert_eq!(callbacks.errors.load(Ordering::SeqCst), 1);
        assert_eq!(producer.metrics().bytes_buffered, before.bytes_buffered);
        drop(later_delivery);
        println!("SEQUENCE_PUBLIC_ABORT worker_aborted=1 receipt=closed callbacks_missing={COUNT} bytes_retained={} later_base=8194 mock_error=45", before.bytes_buffered);
    }
    drop(delivery);
    let _close = producer.close_timeout(Duration::from_secs(2)).await;
}

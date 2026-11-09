//! Public mock control of an experimental whole-batch retry handoff.
//! The gate changes scheduling and the test does not establish Kafka behavior.
#![cfg(sealed_retry_witness)]

mod common;

use partitionline::producer::sealed_retry_witness as gate;
use partitionline::{Error, ProduceRecord, Producer, ProducerConfig, ProducerInterceptor, RecordMetadata};
use std::future::{poll_fn, Future};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::task::Poll;
use std::time::Duration;

const COUNT: usize = 8193;
const LIMIT: Duration = Duration::from_secs(40);

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

#[expect(
    clippy::expect_used,
    clippy::unwrap_used,
    clippy::indexing_slicing,
    reason = "This public control fails immediately when its setup or observations differ."
)]
async fn history(flush_held: bool) {
    let mock = common::Mock::start().await;
    mock.set_topic_partitions("t", 1);
    let counts = Counts::default();
    let producer = Producer::new_bounded(
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
        COUNT + 8,
    )
    .await
    .expect("producer setup");
    let warm = tokio::time::timeout(
        LIMIT,
        producer.send(ProduceRecord::to("t").partition(0).value(&b"warm"[..])),
    )
    .await
    .expect("warm deadline")
    .expect("warm result");
    assert_eq!(warm.offset, 0);

    mock.set_produce_drop_response_times(1);
    gate::arm();
    let _release = ReleaseGate;
    let mut first = Box::pin(
        producer.send(ProduceRecord::to("t").partition(0).value(&b"first"[..])),
    );
    assert!(poll_fn(|cx| Poll::Ready(first.as_mut().poll(cx))).await.is_pending());
    for index in 1..COUNT - 1 {
        let value = u64::try_from(index).unwrap().to_le_bytes();
        producer
            .try_send(ProduceRecord::to("t").partition(0).value(value.to_vec()))
            .expect("middle record admitted");
    }
    let mut last = Box::pin(
        producer.send(ProduceRecord::to("t").partition(0).value(&b"last"[..])),
    );
    assert!(poll_fn(|cx| Poll::Ready(last.as_mut().poll(cx))).await.is_pending());
    tokio::time::timeout(LIMIT, gate::reached())
        .await
        .expect("atomic retry handoff not reached");

    let original = mock.produce_sequences();
    assert_eq!(original.len(), 2, "only warmup and original before handoff");
    assert_eq!((original[1].2, original[1].3), (1, 8193));
    assert_eq!(mock.log_len("t", 0), COUNT + 1);

    let flushing = if flush_held {
        let p = producer.clone();
        let task = tokio::spawn(async move { p.flush_timeout(Duration::from_secs(20)).await });
        tokio::time::sleep(Duration::from_millis(300)).await;
        assert!(!task.is_finished(), "flush returned while retained batch was held");
        assert_eq!(mock.produce_sequences().len(), 2, "retry emitted before handoff");
        Some(task)
    } else {
        None
    };
    gate::release();
    let (one, end) = tokio::time::timeout(LIMIT, async {
        tokio::join!(first.as_mut(), last.as_mut())
    })
    .await
    .expect("retry receipts deadline");
    assert_eq!(one.expect("first receipt").offset, 1);
    assert_eq!(end.expect("last receipt").offset, 8193);
    if let Some(flushing) = flushing {
        let result = tokio::time::timeout(LIMIT, flushing)
            .await
            .expect("flush deadline")
            .expect("flush task");
        assert!(result.is_ok(), "flush returned {result:?}");
    }
    let attempts = mock.produce_sequences();
    assert!(attempts.len() >= 3, "expected a retry");
    for retry in attempts.iter().skip(2) {
        assert_eq!(retry, &original[1], "retry changed identity, base or count");
    }
    assert_eq!(mock.log_len("t", 0), COUNT + 1);
    assert_eq!(counts.acks.load(Ordering::SeqCst), COUNT + 1);
    assert_eq!(counts.errors.load(Ordering::SeqCst), 0);
    println!("SEALED_RETRY flush_held={flush_held} original={:?} attempts={} log_once=1", original[1], attempts.len() - 2);
    drop(first);
    drop(last);
    let _close = producer.close_timeout(Duration::from_secs(2)).await;
}

#[tokio::test(flavor = "current_thread")]
async fn retained_retry_batch_remains_whole_during_flush() {
    history(false).await;
    println!("SEALED_RETRY_RELEASED_CONTROL_OK");
    history(true).await;
}

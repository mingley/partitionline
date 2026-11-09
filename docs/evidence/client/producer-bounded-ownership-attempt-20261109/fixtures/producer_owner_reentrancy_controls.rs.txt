//! Public callback and barrier controls for producer ownership work.
//!
//! These exercise the existing producer and mock broker. They do not prove
//! cancellation safety or idempotent delivery under arbitrary schedules.

mod common;

use parking_lot::{Condvar, Mutex};
use partitionline::{ProduceRecord, Producer, ProducerConfig, ProducerInterceptor, RecordMetadata};
use std::sync::atomic::{AtomicBool, AtomicU8, Ordering};
use std::sync::Arc;
use std::time::Duration;

const LIMIT: Duration = Duration::from_secs(10);

#[derive(Default)]
struct Reenter {
    producer: Mutex<Option<Producer>>,
    armed: AtomicBool,
    result: AtomicU8,
}

struct ReenterInterceptor(Arc<Reenter>);

impl ProducerInterceptor for ReenterInterceptor {
    fn on_ack(&self, _metadata: &RecordMetadata) {
        let state = &self.0;
        if !state.armed.swap(false, Ordering::SeqCst) {
            return;
        }
        // Drop this mutex before calling the producer: the control must test
        // the producer's own locks, not an application lock held by the test.
        let producer = state.producer.lock().clone();
        let accepted = producer.is_some_and(|producer| {
            producer
                .try_send(ProduceRecord::to("t").partition(0).value(&b"nested"[..]))
                .is_ok()
        });
        state
            .result
            .store(if accepted { 1 } else { 2 }, Ordering::SeqCst);
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn callback_can_enqueue_another_record() {
    let mock = common::Mock::start().await;
    mock.set_topic_partitions("t", 1);
    let reenter = Arc::new(Reenter::default());
    let producer = Producer::new(
        ProducerConfig::bootstrap([mock.addr.clone()])
            .idempotent(true)
            .connections(1)
            .linger(Duration::ZERO)
            .request_timeout(Duration::from_secs(5))
            .delivery_timeout(Duration::from_secs(10))
            .interceptor(ReenterInterceptor(Arc::clone(&reenter))),
    )
    .await
    .expect("producer setup");
    let warm = producer
        .send(ProduceRecord::to("t").partition(0).value(&b"warm"[..]))
        .await
        .expect("warmup");
    assert_eq!(warm.offset, 0);
    *reenter.producer.lock() = Some(producer.clone());
    reenter.armed.store(true, Ordering::SeqCst);

    let trigger = tokio::time::timeout(
        LIMIT,
        producer.send(ProduceRecord::to("t").partition(0).value(&b"trigger"[..])),
    )
    .await
    .expect("trigger receipt deadline")
    .expect("trigger receipt");
    assert_eq!(trigger.offset, 1);
    assert_eq!(
        reenter.result.load(Ordering::SeqCst),
        1,
        "a callback should be able to enqueue into an available queue"
    );
    tokio::time::timeout(LIMIT, producer.flush_timeout(Duration::from_secs(5)))
        .await
        .expect("flush deadline")
        .expect("flush result");
    assert_eq!(
        mock.log_values("t", 0),
        vec![
            (0, Some(b"warm".to_vec())),
            (1, Some(b"trigger".to_vec())),
            (2, Some(b"nested".to_vec()))
        ]
    );
    // Break the callback's intentional Producer reference cycle.
    drop(reenter.producer.lock().take());
    producer
        .close_timeout(Duration::from_secs(5))
        .await
        .expect("close");
}

#[derive(Default)]
struct Gate {
    armed: AtomicBool,
    entered: AtomicBool,
    timed_out: AtomicBool,
    open: Mutex<bool>,
    changed: Condvar,
}

impl Gate {
    fn release(&self) {
        *self.open.lock() = true;
        let _notified = self.changed.notify_all();
    }

    #[expect(
        clippy::expect_used,
        reason = "A test gate must fail if the callback does not enter."
    )]
    async fn wait_entered(&self) {
        tokio::time::timeout(LIMIT, async {
            while !self.entered.load(Ordering::SeqCst) {
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        })
        .await
        .expect("callback entry deadline");
    }
}

struct BlockAck(Arc<Gate>);

impl ProducerInterceptor for BlockAck {
    fn on_ack(&self, _metadata: &RecordMetadata) {
        let gate = &self.0;
        if !gate.armed.swap(false, Ordering::SeqCst) {
            return;
        }
        gate.entered.store(true, Ordering::SeqCst);
        let mut open = gate.open.lock();
        while !*open {
            if gate
                .changed
                .wait_for(&mut open, Duration::from_secs(5))
                .timed_out()
            {
                gate.timed_out.store(true, Ordering::SeqCst);
                break;
            }
        }
    }
}

struct ReleaseOnDrop(Arc<Gate>);

impl Drop for ReleaseOnDrop {
    fn drop(&mut self) {
        self.0.release();
    }
}

#[expect(
    clippy::expect_used,
    reason = "The fixture must fail if producer setup fails."
)]
async fn gated_producer() -> (common::Mock, Producer, Arc<Gate>, ReleaseOnDrop) {
    let mock = common::Mock::start().await;
    mock.set_topic_partitions("t", 1);
    let gate = Arc::new(Gate::default());
    let release = ReleaseOnDrop(Arc::clone(&gate));
    let producer = Producer::new(
        ProducerConfig::bootstrap([mock.addr.clone()])
            .idempotent(true)
            .connections(1)
            .linger(Duration::ZERO)
            .request_timeout(Duration::from_secs(5))
            .delivery_timeout(Duration::from_secs(10))
            .interceptor(BlockAck(Arc::clone(&gate))),
    )
    .await
    .expect("producer setup");
    let warm = producer
        .send(ProduceRecord::to("t").partition(0).value(&b"warm"[..]))
        .await
        .expect("warmup");
    assert_eq!(warm.offset, 0);
    gate.armed.store(true, Ordering::SeqCst);
    (mock, producer, gate, release)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cancelling_one_flush_does_not_complete_another_early() {
    let (mock, producer, gate, _release) = gated_producer().await;
    let sender = {
        let producer = producer.clone();
        tokio::spawn(async move {
            producer
                .send(ProduceRecord::to("t").partition(0).value(&b"held"[..]))
                .await
        })
    };
    gate.wait_entered().await;
    let cancelled_flush = {
        let producer = producer.clone();
        tokio::spawn(async move { producer.flush_timeout(Duration::from_secs(5)).await })
    };
    let retained_flush = {
        let producer = producer.clone();
        tokio::spawn(async move { producer.flush_timeout(Duration::from_secs(5)).await })
    };
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert!(!sender.is_finished(), "receipt preceded the callback");
    assert!(
        !retained_flush.is_finished(),
        "flush completed while its callback was blocked"
    );
    cancelled_flush.abort();
    gate.release();

    let metadata = tokio::time::timeout(LIMIT, sender)
        .await
        .expect("receipt deadline")
        .expect("receipt task")
        .expect("receipt");
    assert_eq!(metadata.offset, 1);
    tokio::time::timeout(LIMIT, retained_flush)
        .await
        .expect("flush deadline")
        .expect("flush task")
        .expect("flush");
    assert!(!gate.timed_out.load(Ordering::SeqCst));
    assert_eq!(mock.log_len("t", 0), 2);
    assert_eq!(producer.metrics().bytes_buffered, 0);
    producer
        .close_timeout(Duration::from_secs(5))
        .await
        .expect("close");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn close_waits_for_a_blocked_callback_when_its_budget_allows() {
    let (mock, producer, gate, _release) = gated_producer().await;
    let sender = {
        let producer = producer.clone();
        tokio::spawn(async move {
            producer
                .send(ProduceRecord::to("t").partition(0).value(&b"held"[..]))
                .await
        })
    };
    gate.wait_entered().await;
    let closer = {
        let producer = producer.clone();
        tokio::spawn(async move { producer.close_timeout(Duration::from_secs(5)).await })
    };
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert!(!sender.is_finished(), "receipt preceded the callback");
    assert!(
        !closer.is_finished(),
        "close completed while its callback was blocked"
    );
    gate.release();

    let metadata = tokio::time::timeout(LIMIT, sender)
        .await
        .expect("receipt deadline")
        .expect("receipt task")
        .expect("receipt");
    assert_eq!(metadata.offset, 1);
    tokio::time::timeout(LIMIT, closer)
        .await
        .expect("close deadline")
        .expect("close task")
        .expect("close");
    assert!(!gate.timed_out.load(Ordering::SeqCst));
    assert_eq!(mock.log_len("t", 0), 2);
    assert_eq!(producer.metrics().bytes_buffered, 0);
}

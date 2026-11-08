//! Baseline witnesses for a documented producer flush reporting limitation.
//!
//! These tests intentionally expect the behavior at the pinned source:
//! an accepted record expires with a per-record Timeout while flush returns Ok.
//! They are qualification controls, not a statement of the desired contract.

mod common;

use partitionline::{Error, ProduceRecord, Producer, ProducerConfig, ProducerInterceptor};
use std::future::{poll_fn, Future};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{mpsc, Arc};
use std::task::Poll;
use std::time::{Duration, Instant};

#[derive(Clone)]
struct ErrorCounts {
    all: Arc<AtomicUsize>,
    timeouts: Arc<AtomicUsize>,
}

impl ProducerInterceptor for ErrorCounts {
    fn on_error(&self, error: &Error) {
        let _previous = self.all.fetch_add(1, Ordering::SeqCst);
        if matches!(error, Error::Timeout) {
            let _previous = self.timeouts.fetch_add(1, Ordering::SeqCst);
        }
    }
}

#[derive(Clone, Copy)]
enum Admission {
    TrySend,
    Send,
}

impl Admission {
    fn name(self) -> &'static str {
        match self {
            Self::TrySend => "try_send",
            Self::Send => "send",
        }
    }
}

async fn baseline_history(admission: Admission) {
    const DELIVERY_TIMEOUT: Duration = Duration::from_secs(2);
    const HOLD: Duration = Duration::from_millis(2_250);
    const TARGET: &[u8] = b"target";

    let mock = common::Mock::start().await;
    mock.set_topic_partitions("t", 1);
    let counts = ErrorCounts {
        all: Arc::new(AtomicUsize::new(0)),
        timeouts: Arc::new(AtomicUsize::new(0)),
    };
    let producer = tokio::time::timeout(
        Duration::from_secs(8),
        Producer::new(
            ProducerConfig::bootstrap([mock.addr.clone()])
                .connections(1)
                .linger(Duration::ZERO)
                .request_timeout(Duration::from_secs(1))
                .delivery_timeout(DELIVERY_TIMEOUT)
                .max_block(Duration::from_secs(3))
                .interceptor(counts.clone()),
        ),
    )
    .await
    .expect("producer setup deadline")
    .expect("producer setup");

    // A real successful delivery makes metadata and the partition worker ready.
    let warm = tokio::time::timeout(
        Duration::from_secs(5),
        producer.send(ProduceRecord::to("t").partition(0).value(&b"warm"[..])),
    )
    .await
    .expect("warmup deadline")
    .expect("warmup delivery");
    assert_eq!(warm.partition, 0);
    assert_eq!(warm.offset, 0);
    assert_eq!(mock.log_len("t", 0), 1);
    assert_eq!(mock.produce_batches().len(), 1);

    let target = ProduceRecord::to("t").partition(0).value(TARGET);
    let mut delivery = match admission {
        Admission::TrySend => {
            producer.try_send(target).expect("target admitted");
            None
        }
        Admission::Send => {
            let mut future = Box::pin(producer.send(target));
            let first = poll_fn(|cx| Poll::Ready(future.as_mut().poll(cx))).await;
            assert!(first.is_pending(), "send must wait for its delivery result");
            Some(future)
        }
    };

    // The current-thread runtime cannot poll the worker during this synchronous
    // section. Metrics prove that send's Pending poll reached actual admission.
    let admitted = producer.metrics();
    assert_eq!(admitted.records_queued, 2);
    assert_eq!(admitted.records_acked, 1);
    assert_eq!(admitted.produce_errors, 0);
    assert_eq!(admitted.bytes_buffered, TARGET.len() as u64);
    assert_eq!(counts.all.load(Ordering::SeqCst), 0);
    assert_eq!(mock.log_len("t", 0), 1);

    // A bounded test-only scheduler stall makes the accepted record expire
    // before the worker can run. The sender stays live so recv_timeout cannot
    // return Disconnected instead of waiting for the timeout.
    let (hold_sender, hold_receiver) = mpsc::channel::<()>();
    let hold_start = Instant::now();
    assert!(matches!(
        hold_receiver.recv_timeout(HOLD),
        Err(mpsc::RecvTimeoutError::Timeout)
    ));
    let held = hold_start.elapsed();
    drop(hold_sender);
    assert!(held >= DELIVERY_TIMEOUT, "hold was too short: {held:?}");

    let flush_started = Instant::now();
    let flushed = tokio::time::timeout(
        Duration::from_secs(5),
        producer.flush_timeout(Duration::from_secs(10)),
    )
    .await
    .expect("flush did not finish within the harness deadline");
    let flush_elapsed = flush_started.elapsed();

    // This assertion pins today's documented limitation; a future fix should
    // deliberately replace this witness with the stronger desired contract.
    assert!(
        flushed.is_ok(),
        "baseline expected flush Ok after delivery expiry, got {flushed:?}"
    );
    if let Some(future) = delivery.take() {
        let outcome = tokio::time::timeout(Duration::from_secs(2), future)
            .await
            .expect("per-record result deadline");
        assert!(matches!(outcome, Err(Error::Timeout)), "{outcome:?}");
    }
    drop(delivery);

    let after = producer.metrics();
    assert_eq!(after.records_queued, 2);
    assert_eq!(after.records_acked, 1);
    assert_eq!(after.produce_errors, 1);
    assert_eq!(after.bytes_buffered, 0);
    assert_eq!(counts.all.load(Ordering::SeqCst), 1);
    assert_eq!(counts.timeouts.load(Ordering::SeqCst), 1);
    assert_eq!(mock.log_len("t", 0), 1, "target was not appended");
    assert_eq!(mock.produce_batches().len(), 1, "only warmup was sent");
    assert!(flush_elapsed < Duration::from_secs(5));

    println!(
        "PUBLIC_FLUSH_HISTORY mode={} admission=accepted delivery=timeout callback=1 queued=2 acked=1 errors=1 buffered=0 appended=1 produce_batches=1 flush=ok",
        admission.name()
    );

    tokio::time::timeout(
        Duration::from_secs(5),
        producer.close_timeout(Duration::from_secs(3)),
    )
    .await
    .expect("close harness deadline")
    .expect("close after witness");
}

#[tokio::test(flavor = "current_thread")]
async fn baseline_try_send_expiry_is_missing_from_flush() {
    baseline_history(Admission::TrySend).await;
}

#[tokio::test(flavor = "current_thread")]
async fn baseline_send_expiry_is_missing_from_flush() {
    baseline_history(Admission::Send).await;
}

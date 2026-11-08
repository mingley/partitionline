//! Public producer histories for an isolated flush completion barrier.
//!
//! The cfg-gated counter in the baseline is a fixture marker after flush has
//! queued worker controls, not a general linearization point. A candidate must
//! instead place its own observer at its actual cutoff. Mock response gates
//! deliberately extend scheduling windows and do not change producer state.

#![cfg(flush_barrier_witness)]

mod common;

use partitionline::producer::flush_barrier_witness as marker;
use partitionline::{
    Error, ProduceRecord, Producer, ProducerConfig, ProducerInterceptor, RecordMetadata,
};
use std::future::{poll_fn, Future};
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::Poll;
use std::time::Duration;

const DEADLINE: Duration = Duration::from_secs(10);

#[derive(Clone, Default)]
struct Callbacks {
    acknowledgements: Arc<Mutex<Vec<(String, i32, i64, i32)>>>,
    errors: Arc<Mutex<Vec<Option<i16>>>>,
}

impl ProducerInterceptor for Callbacks {
    fn on_ack(&self, metadata: &RecordMetadata) {
        self.acknowledgements.lock().unwrap().push((
            metadata.topic.to_string(),
            metadata.partition,
            metadata.offset,
            metadata.serialized_value_size,
        ));
    }

    fn on_error(&self, error: &Error) {
        self.errors.lock().unwrap().push(error.broker_code());
    }
}

struct Release(Arc<common::ProduceResponseGate>);

impl Drop for Release {
    fn drop(&mut self) {
        self.0.release();
    }
}

async fn poll_once<F: Future>(mut future: Pin<&mut F>) -> Poll<F::Output> {
    poll_fn(|cx| Poll::Ready(future.as_mut().poll(cx))).await
}

async fn wait_for_marker<F: Future<Output = partitionline::Result<()>>>(
    mut flush: Pin<&mut F>,
    target: usize,
) {
    tokio::time::timeout(
        DEADLINE,
        poll_fn(|cx| match flush.as_mut().poll(cx) {
            Poll::Ready(result) => panic!("flush completed before controlled release: {result:?}"),
            Poll::Pending if marker::observed_cutoffs() >= target => Poll::Ready(()),
            Poll::Pending => Poll::Pending,
        }),
    )
    .await
    .expect("flush did not reach the fixture marker");
}

async fn setup(partitions: i32, warm: &[i32]) -> (common::Mock, Producer, Callbacks) {
    let mock = common::Mock::start().await;
    mock.set_topic_partitions("t", partitions);
    let callbacks = Callbacks::default();
    let producer = tokio::time::timeout(
        DEADLINE,
        Producer::new(
            ProducerConfig::bootstrap([mock.addr.clone()])
                .client_id("flush-barrier-witness")
                .connections(2)
                .linger(Duration::ZERO)
                .idempotent(false)
                .request_timeout(Duration::from_secs(10))
                .delivery_timeout(Duration::from_secs(30))
                .max_block(Duration::from_secs(10))
                .interceptor(callbacks.clone()),
        ),
    )
    .await
    .expect("producer setup deadline")
    .expect("producer setup");
    for &partition in warm {
        let metadata = tokio::time::timeout(
            DEADLINE,
            producer.send(
                ProduceRecord::to("t")
                    .partition(partition)
                    .value(&b"warm"[..]),
            ),
        )
        .await
        .expect("warmup deadline")
        .expect("warmup acknowledgement");
        assert_eq!((metadata.partition, metadata.offset), (partition, 0));
    }
    (mock, producer, callbacks)
}

async fn close(producer: Producer) {
    tokio::time::timeout(
        Duration::from_secs(12),
        producer.close_timeout(Duration::from_secs(10)),
    )
    .await
    .expect("close deadline")
    .expect("close");
}

async fn entered(gate: &common::ProduceResponseGate) {
    tokio::time::timeout(DEADLINE, gate.entered())
        .await
        .expect("broker did not hold Produce response");
}

#[tokio::test(flavor = "current_thread")]
async fn overlapping_flushes_wait_for_accepted_record() {
    let (mock, producer, callbacks) = setup(1, &[0]).await;
    let gate = mock.gate_next_produce_response();
    let release = Release(Arc::clone(&gate));
    let mut delivery =
        Box::pin(producer.send(ProduceRecord::to("t").partition(0).value(&b"alpha"[..])));
    assert!(poll_once(delivery.as_mut()).await.is_pending());
    assert_eq!(producer.metrics().records_queued, 2);
    entered(&gate).await;

    let initial = marker::observed_cutoffs();
    let mut first = Box::pin(producer.flush_timeout(Duration::from_secs(20)));
    let mut second = Box::pin(producer.flush_timeout(Duration::from_secs(20)));
    wait_for_marker(first.as_mut(), initial + 1).await;
    wait_for_marker(second.as_mut(), initial + 2).await;
    assert!(poll_once(delivery.as_mut()).await.is_pending());
    assert_eq!(callbacks.acknowledgements.lock().unwrap().len(), 1);

    gate.release();
    drop(release);
    tokio::time::timeout(DEADLINE, first.as_mut())
        .await
        .expect("first flush deadline")
        .expect("first flush");
    assert_eq!(
        *callbacks.acknowledgements.lock().unwrap(),
        vec![("t".to_string(), 0, 0, 4), ("t".to_string(), 0, 1, 5)]
    );
    let metadata = match poll_once(delivery.as_mut()).await {
        Poll::Ready(Ok(metadata)) => metadata,
        other => panic!("flush returned before target receipt: {other:?}"),
    };
    assert_eq!((metadata.partition, metadata.offset), (0, 1));
    tokio::time::timeout(DEADLINE, second.as_mut())
        .await
        .expect("second flush deadline")
        .expect("second flush");
    assert!(callbacks.errors.lock().unwrap().is_empty());
    assert_eq!(producer.metrics().bytes_buffered, 0);
    assert_eq!(
        mock.log_values("t", 0),
        vec![(0, Some(b"warm".to_vec())), (1, Some(b"alpha".to_vec())),]
    );
    drop(delivery);
    drop(first);
    drop(second);
    close(producer).await;
    println!("PUBLIC_BARRIER_OVERLAP record=ack callbacks=1 flushes=2");
}

#[tokio::test(flavor = "current_thread")]
async fn cancelled_flush_does_not_consume_other_waiter_error() {
    let (mock, producer, callbacks) = setup(1, &[0]).await;
    mock.set_produce_error_times(partitionline::error::MESSAGE_TOO_LARGE, 1);
    let gate = mock.gate_next_produce_response();
    let release = Release(Arc::clone(&gate));
    let mut delivery =
        Box::pin(producer.send(ProduceRecord::to("t").partition(0).value(&b"alpha"[..])));
    assert!(poll_once(delivery.as_mut()).await.is_pending());
    assert_eq!(producer.metrics().records_queued, 2);
    entered(&gate).await;

    let initial = marker::observed_cutoffs();
    let mut first = Box::pin(producer.flush_timeout(Duration::from_secs(20)));
    let mut second = Box::pin(producer.flush_timeout(Duration::from_secs(20)));
    wait_for_marker(first.as_mut(), initial + 1).await;
    wait_for_marker(second.as_mut(), initial + 2).await;
    assert!(poll_once(delivery.as_mut()).await.is_pending());
    drop(first);
    gate.release();
    drop(release);

    let delivered = tokio::time::timeout(DEADLINE, delivery.as_mut())
        .await
        .expect("delivery deadline")
        .expect_err("the broker rejected the record");
    assert_eq!(
        delivered.broker_code(),
        Some(partitionline::error::MESSAGE_TOO_LARGE)
    );
    assert_eq!(
        *callbacks.errors.lock().unwrap(),
        vec![Some(partitionline::error::MESSAGE_TOO_LARGE)]
    );
    let result = tokio::time::timeout(DEADLINE, second.as_mut())
        .await
        .expect("second flush deadline");
    match result {
        Err(error) if error.broker_code() == Some(partitionline::error::MESSAGE_TOO_LARGE) => {}
        Ok(()) => panic!("BASELINE_CANCELLED_ERROR_LOST"),
        Err(error) => panic!("unexpected second flush error: {error:?}"),
    }
    assert_eq!(callbacks.acknowledgements.lock().unwrap().len(), 1);
    assert_eq!(producer.metrics().bytes_buffered, 0);
    assert_eq!(mock.log_values("t", 0), vec![(0, Some(b"warm".to_vec()))]);
    drop(delivery);
    drop(second);
    close(producer).await;
    println!(
        "PUBLIC_BARRIER_CANCELLED record=broker_error callback=1 surviving_flush=broker_error"
    );
}

#[tokio::test(flavor = "current_thread")]
async fn later_record_has_independent_completion() {
    let (mock, producer, callbacks) = setup(2, &[0, 1]).await;
    let first_gate = mock.gate_next_produce_response();
    let first_release = Release(Arc::clone(&first_gate));
    let mut first_delivery =
        Box::pin(producer.send(ProduceRecord::to("t").partition(0).value(&b"alpha"[..])));
    assert!(poll_once(first_delivery.as_mut()).await.is_pending());
    assert_eq!(producer.metrics().records_queued, 3);
    entered(&first_gate).await;

    let initial = marker::observed_cutoffs();
    let mut flush = Box::pin(producer.flush_timeout(Duration::from_secs(20)));
    wait_for_marker(flush.as_mut(), initial + 1).await;

    let second_gate = mock.gate_next_produce_response();
    let second_release = Release(Arc::clone(&second_gate));
    let mut second_delivery =
        Box::pin(producer.send(ProduceRecord::to("t").partition(1).value(&b"beta"[..])));
    assert!(poll_once(second_delivery.as_mut()).await.is_pending());
    assert_eq!(producer.metrics().records_queued, 4);
    entered(&second_gate).await;
    assert!(poll_once(first_delivery.as_mut()).await.is_pending());
    assert!(poll_once(second_delivery.as_mut()).await.is_pending());

    first_gate.release();
    drop(first_release);
    let first = tokio::time::timeout(DEADLINE, first_delivery.as_mut())
        .await
        .expect("first record deadline")
        .expect("first record acknowledgement");
    assert_eq!((first.partition, first.offset), (0, 1));
    assert!(callbacks
        .acknowledgements
        .lock()
        .unwrap()
        .contains(&("t".to_string(), 0, 1, 5)));
    assert!(poll_once(second_delivery.as_mut()).await.is_pending());

    let early = tokio::time::timeout(Duration::from_millis(200), flush.as_mut()).await;
    match &early {
        Ok(Ok(())) => println!("PUBLIC_BARRIER_POST_CUTOFF flush_before_later_record=true"),
        Ok(Err(error)) => panic!("unexpected flush error: {error:?}"),
        Err(_) => println!("PUBLIC_BARRIER_POST_CUTOFF flush_before_later_record=false"),
    }
    assert!(poll_once(second_delivery.as_mut()).await.is_pending());
    second_gate.release();
    drop(second_release);
    let second = tokio::time::timeout(DEADLINE, second_delivery.as_mut())
        .await
        .expect("second record deadline")
        .expect("second record acknowledgement");
    assert_eq!((second.partition, second.offset), (1, 1));
    if early.is_err() {
        tokio::time::timeout(DEADLINE, flush.as_mut())
            .await
            .expect("flush deadline")
            .expect("flush result");
    }
    let acknowledgements = callbacks.acknowledgements.lock().unwrap().clone();
    assert_eq!(acknowledgements.len(), 4);
    assert!(acknowledgements.contains(&("t".to_string(), 1, 1, 4)));
    assert!(callbacks.errors.lock().unwrap().is_empty());
    assert_eq!(producer.metrics().bytes_buffered, 0);
    assert_eq!(
        mock.log_values("t", 0),
        vec![(0, Some(b"warm".to_vec())), (1, Some(b"alpha".to_vec()))]
    );
    assert_eq!(
        mock.log_values("t", 1),
        vec![(0, Some(b"warm".to_vec())), (1, Some(b"beta".to_vec()))]
    );
    drop(first_delivery);
    drop(second_delivery);
    drop(flush);
    close(producer).await;
}

#[tokio::test(flavor = "current_thread")]
async fn flush_deadline_does_not_finish_record() {
    let (mock, producer, callbacks) = setup(1, &[0]).await;
    let gate = mock.gate_next_produce_response();
    let release = Release(Arc::clone(&gate));
    let mut delivery =
        Box::pin(producer.send(ProduceRecord::to("t").partition(0).value(&b"alpha"[..])));
    assert!(poll_once(delivery.as_mut()).await.is_pending());
    assert_eq!(producer.metrics().records_queued, 2);
    entered(&gate).await;

    let initial = marker::observed_cutoffs();
    let mut flush = Box::pin(producer.flush_timeout(Duration::from_millis(300)));
    wait_for_marker(flush.as_mut(), initial + 1).await;
    let error = tokio::time::timeout(DEADLINE, flush.as_mut())
        .await
        .expect("flush caller deadline")
        .expect_err("held response must outlast flush deadline");
    assert!(matches!(error, Error::Timeout));
    assert!(poll_once(delivery.as_mut()).await.is_pending());
    assert_eq!(callbacks.acknowledgements.lock().unwrap().len(), 1);
    assert!(callbacks.errors.lock().unwrap().is_empty());
    assert_eq!(producer.metrics().bytes_buffered, 5);
    drop(flush);

    gate.release();
    drop(release);
    let metadata = tokio::time::timeout(DEADLINE, delivery.as_mut())
        .await
        .expect("delivery deadline")
        .expect("delivery acknowledgement");
    assert_eq!((metadata.partition, metadata.offset), (0, 1));
    assert_eq!(
        *callbacks.acknowledgements.lock().unwrap(),
        vec![("t".to_string(), 0, 0, 4), ("t".to_string(), 0, 1, 5)]
    );
    tokio::time::timeout(DEADLINE, producer.flush_timeout(Duration::from_secs(8)))
        .await
        .expect("subsequent flush deadline")
        .expect("subsequent flush");
    assert_eq!(producer.metrics().bytes_buffered, 0);
    assert!(callbacks.errors.lock().unwrap().is_empty());
    drop(delivery);
    close(producer).await;
    println!("PUBLIC_BARRIER_DEADLINE flush=timeout delivery=ack callback=1");
}

struct CallbackGate {
    entered: tokio::sync::Notify,
    released: Mutex<bool>,
    wake: std::sync::Condvar,
}

impl CallbackGate {
    fn new() -> Self {
        Self {
            entered: tokio::sync::Notify::new(),
            released: Mutex::new(false),
            wake: std::sync::Condvar::new(),
        }
    }

    fn hold(&self) {
        let released = self.released.lock().unwrap();
        self.entered.notify_one();
        let (released, _) = self
            .wake
            .wait_timeout_while(released, Duration::from_secs(15), |released| !*released)
            .unwrap();
        assert!(*released, "callback gate release timed out");
    }

    fn release(&self) {
        *self.released.lock().unwrap() = true;
        self.wake.notify_all();
    }
}

struct ReleaseCallback(Arc<CallbackGate>);

impl Drop for ReleaseCallback {
    fn drop(&mut self) {
        self.0.release();
    }
}

#[derive(Clone)]
struct BlockingCallbacks {
    inner: Callbacks,
    gate: Arc<CallbackGate>,
}

impl ProducerInterceptor for BlockingCallbacks {
    fn on_ack(&self, metadata: &RecordMetadata) {
        if metadata.partition == 0 && metadata.offset == 1 {
            self.gate.hold();
        }
        self.inner.on_ack(metadata);
    }

    fn on_error(&self, error: &Error) {
        self.inner.on_error(error);
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn blocked_callback_keeps_flush_and_receipt_pending() {
    let mock = common::Mock::start().await;
    mock.set_topic_partitions("t", 1);
    let callbacks = Callbacks::default();
    let gate = Arc::new(CallbackGate::new());
    let release = ReleaseCallback(Arc::clone(&gate));
    let producer = tokio::time::timeout(
        DEADLINE,
        Producer::new(
            ProducerConfig::bootstrap([mock.addr.clone()])
                .client_id("flush-barrier-witness")
                .connections(1)
                .linger(Duration::ZERO)
                .idempotent(false)
                .request_timeout(Duration::from_secs(10))
                .delivery_timeout(Duration::from_secs(30))
                .max_block(Duration::from_secs(10))
                .interceptor(BlockingCallbacks {
                    inner: callbacks.clone(),
                    gate: Arc::clone(&gate),
                }),
        ),
    )
    .await
    .expect("producer setup deadline")
    .expect("producer setup");
    let warm = tokio::time::timeout(
        DEADLINE,
        producer.send(ProduceRecord::to("t").partition(0).value(&b"warm"[..])),
    )
    .await
    .expect("warmup deadline")
    .expect("warmup acknowledgement");
    assert_eq!(warm.offset, 0);

    let mut delivery =
        Box::pin(producer.send(ProduceRecord::to("t").partition(0).value(&b"alpha"[..])));
    assert!(poll_once(delivery.as_mut()).await.is_pending());
    assert_eq!(producer.metrics().records_queued, 2);
    tokio::time::timeout(DEADLINE, gate.entered.notified())
        .await
        .expect("target callback did not enter");
    assert!(poll_once(delivery.as_mut()).await.is_pending());
    assert_eq!(callbacks.acknowledgements.lock().unwrap().len(), 1);

    let mut flush = Box::pin(producer.flush_timeout(Duration::from_millis(300)));
    let error = tokio::time::timeout(Duration::from_secs(3), flush.as_mut())
        .await
        .expect("flush did not respect caller deadline")
        .expect_err("flush must wait for target callback");
    assert!(matches!(error, Error::Timeout));
    assert!(poll_once(delivery.as_mut()).await.is_pending());
    assert_eq!(callbacks.acknowledgements.lock().unwrap().len(), 1);
    assert!(callbacks.errors.lock().unwrap().is_empty());
    drop(flush);

    gate.release();
    drop(release);
    let result = tokio::time::timeout(DEADLINE, delivery.as_mut())
        .await
        .expect("delivery deadline after callback release")
        .expect("target acknowledgement");
    assert_eq!((result.partition, result.offset), (0, 1));
    assert_eq!(
        *callbacks.acknowledgements.lock().unwrap(),
        vec![("t".to_string(), 0, 0, 4), ("t".to_string(), 0, 1, 5)]
    );
    tokio::time::timeout(DEADLINE, producer.flush_timeout(Duration::from_secs(8)))
        .await
        .expect("subsequent flush deadline")
        .expect("subsequent flush");
    assert_eq!(producer.metrics().bytes_buffered, 0);
    assert!(callbacks.errors.lock().unwrap().is_empty());
    drop(delivery);
    close(producer).await;
    println!("PUBLIC_BARRIER_CALLBACK blocked=1 flush=timeout receipt=pending then=ack");
}

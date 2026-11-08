//! Public shutdown histories for an isolated admission-seal experiment.
//!
//! The mock deliberately holds a Produce response. A cfg-gated observer marks
//! close entry on the baseline and the actual admission seal on the experiment.
//! These controls do not establish complete shutdown or cancellation semantics.

#![cfg(flush_shutdown_witness)]

mod common;

use partitionline::producer::flush_shutdown_witness as marker;
use partitionline::{
    Error, ProduceRecord, Producer, ProducerConfig, ProducerInterceptor, RecordMetadata,
};
use std::future::{poll_fn, Future};
use std::pin::Pin;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::task::Poll;
use std::time::Duration;

const WAIT: Duration = Duration::from_secs(12);

#[derive(Clone, Default)]
struct Callbacks {
    sends: Arc<AtomicUsize>,
    closes: Arc<AtomicUsize>,
    acknowledgements: Arc<Mutex<Vec<(String, i32, i64, i32)>>>,
    errors: Arc<Mutex<Vec<Option<i16>>>>,
}

impl ProducerInterceptor for Callbacks {
    fn on_send(&self, record: ProduceRecord) -> ProduceRecord {
        let _previous = self.sends.fetch_add(1, Ordering::SeqCst);
        record
    }

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

    fn close(&self) {
        let _previous = self.closes.fetch_add(1, Ordering::SeqCst);
    }
}

struct Release(Arc<common::ProduceResponseGate>);

impl Drop for Release {
    fn drop(&mut self) {
        self.0.release();
    }
}

fn candidate() -> bool {
    match std::env::var("WITNESS_EXPECT").as_deref() {
        Ok("sealed") => true,
        Ok("baseline") => false,
        other => panic!("unexpected WITNESS_EXPECT: {other:?}"),
    }
}

async fn poll_once<F: Future>(mut future: Pin<&mut F>) -> Poll<F::Output> {
    poll_fn(|cx| Poll::Ready(future.as_mut().poll(cx))).await
}

async fn wait_for_close<F: Future<Output = partitionline::Result<()>>>(
    mut close: Pin<&mut F>,
    target: usize,
) {
    tokio::time::timeout(
        WAIT,
        poll_fn(|cx| match close.as_mut().poll(cx) {
            Poll::Ready(result) => panic!("close completed while record was held: {result:?}"),
            Poll::Pending if marker::observed_closes() >= target => Poll::Ready(()),
            Poll::Pending => Poll::Pending,
        }),
    )
    .await
    .expect("close did not reach the fixture marker");
}

async fn setup(buffer_memory: usize) -> (common::Mock, Producer, Callbacks) {
    let mock = common::Mock::start().await;
    mock.set_topic_partitions("t", 1);
    let callbacks = Callbacks::default();
    let producer = tokio::time::timeout(
        WAIT,
        Producer::new(
            ProducerConfig::bootstrap([mock.addr.clone()])
                .client_id("shutdown-witness")
                .connections(1)
                .linger(Duration::ZERO)
                .idempotent(false)
                .buffer_memory(buffer_memory)
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
        WAIT,
        producer.send(ProduceRecord::to("t").partition(0).value(&b"warm"[..])),
    )
    .await
    .expect("warmup deadline")
    .expect("warmup acknowledgement");
    assert_eq!((warm.partition, warm.offset), (0, 0));
    (mock, producer, callbacks)
}

async fn entered(gate: &common::ProduceResponseGate) {
    tokio::time::timeout(WAIT, gate.entered())
        .await
        .expect("broker did not hold Produce response");
}

fn expect_post_close_poll(result: Poll<partitionline::Result<RecordMetadata>>) -> bool {
    match (candidate(), result) {
        (true, Poll::Ready(Err(Error::Closed))) => false,
        (false, Poll::Pending) => true,
        (sealed, result) => panic!("unexpected post-marker send: sealed={sealed} {result:?}"),
    }
}

async fn close_history(cancel_first: bool) {
    let (mock, producer, callbacks) = setup(0).await;
    let gate = mock.gate_next_produce_response();
    let release = Release(Arc::clone(&gate));
    let mut delivery =
        Box::pin(producer.send(ProduceRecord::to("t").partition(0).value(&b"alpha"[..])));
    assert!(poll_once(delivery.as_mut()).await.is_pending());
    assert_eq!(producer.metrics().records_queued, 2);
    assert_eq!(producer.metrics().bytes_buffered, 5);
    entered(&gate).await;
    assert_eq!(
        mock.log_values("t", 0),
        vec![(0, Some(b"warm".to_vec())), (1, Some(b"alpha".to_vec()))]
    );

    let initial = marker::observed_closes();
    let closing_handle = producer.clone();
    let mut close = Some(Box::pin(closing_handle.close()));
    wait_for_close(close.as_mut().expect("close exists").as_mut(), initial + 1).await;
    assert!(poll_once(delivery.as_mut()).await.is_pending());
    assert_eq!(callbacks.acknowledgements.lock().unwrap().len(), 1);
    if cancel_first {
        drop(close.take());
    }

    let mut late = Box::pin(producer.send(ProduceRecord::to("t").partition(0).value(&b"late"[..])));
    let late_accepted = expect_post_close_poll(poll_once(late.as_mut()).await);
    assert_eq!(
        producer.metrics().records_queued,
        if late_accepted { 3 } else { 2 }
    );
    assert_eq!(
        callbacks.sends.load(Ordering::SeqCst),
        if late_accepted { 3 } else { 2 }
    );
    assert_eq!(callbacks.closes.load(Ordering::SeqCst), 0);

    gate.release();
    drop(release);
    let metadata = tokio::time::timeout(WAIT, delivery.as_mut())
        .await
        .expect("target delivery deadline")
        .expect("target acknowledgement");
    assert_eq!((metadata.partition, metadata.offset), (0, 1));
    assert!(callbacks
        .acknowledgements
        .lock()
        .unwrap()
        .contains(&("t".to_string(), 0, 1, 5)));
    if late_accepted {
        let late_metadata = tokio::time::timeout(WAIT, late.as_mut())
            .await
            .expect("late delivery deadline")
            .expect("late acknowledgement");
        assert_eq!((late_metadata.partition, late_metadata.offset), (0, 2));
    }
    drop(late);
    drop(delivery);

    if !cancel_first {
        let mut close = close.take().expect("close exists");
        tokio::time::timeout(WAIT, close.as_mut())
            .await
            .expect("close deadline")
            .expect("close result");
        drop(close);
    } else {
        tokio::time::timeout(
            WAIT,
            producer.clone().close_timeout(Duration::from_secs(10)),
        )
        .await
        .expect("second close deadline")
        .expect("second close result");
    }
    assert_eq!(callbacks.closes.load(Ordering::SeqCst), 1);
    assert!(callbacks.errors.lock().unwrap().is_empty());
    assert_eq!(producer.metrics().bytes_buffered, 0);
    let expected = if late_accepted {
        vec![
            (0, Some(b"warm".to_vec())),
            (1, Some(b"alpha".to_vec())),
            (2, Some(b"late".to_vec())),
        ]
    } else {
        vec![(0, Some(b"warm".to_vec())), (1, Some(b"alpha".to_vec()))]
    };
    assert_eq!(mock.log_values("t", 0), expected);
    let acks = callbacks.acknowledgements.lock().unwrap().clone();
    let expected_acks = if late_accepted {
        vec![
            ("t".to_string(), 0, 0, 4),
            ("t".to_string(), 0, 1, 5),
            ("t".to_string(), 0, 2, 4),
        ]
    } else {
        vec![("t".to_string(), 0, 0, 4), ("t".to_string(), 0, 1, 5)]
    };
    assert_eq!(acks, expected_acks);
    let after_close = tokio::time::timeout(
        WAIT,
        producer.send(ProduceRecord::to("t").partition(0).value(&b"closed"[..])),
    )
    .await
    .expect("post-close send deadline");
    assert!(matches!(after_close, Err(Error::Closed)));
    println!("PUBLIC_SHUTDOWN cancel_first={cancel_first} late_accepted={late_accepted} target_ack=1 close_callback=1 bytes=0");
}

#[tokio::test(flavor = "current_thread")]
async fn close_seals_before_draining_the_accepted_record() {
    close_history(false).await;
}

#[tokio::test(flavor = "current_thread")]
async fn cancelled_close_preserves_the_experiment_seal() {
    close_history(true).await;
}

#[tokio::test(flavor = "current_thread")]
async fn pre_admission_buffer_waiter_is_checked_at_the_seal() {
    let (mock, producer, callbacks) = setup(1024).await;
    let gate = mock.gate_next_produce_response();
    let release = Release(Arc::clone(&gate));
    let mut first =
        Box::pin(producer.send(ProduceRecord::to("t").partition(0).value(vec![b'a'; 600])));
    assert!(poll_once(first.as_mut()).await.is_pending());
    assert_eq!(producer.metrics().records_queued, 2);
    assert_eq!(producer.metrics().bytes_buffered, 600);
    entered(&gate).await;

    let initial_waits = marker::observed_buffer_waits();
    let mut waiting =
        Box::pin(producer.send(ProduceRecord::to("t").partition(0).value(vec![b'b'; 600])));
    assert!(poll_once(waiting.as_mut()).await.is_pending());
    assert!(marker::observed_buffer_waits() > initial_waits);
    assert_eq!(callbacks.sends.load(Ordering::SeqCst), 3);
    assert_eq!(producer.metrics().records_queued, 2);
    assert_eq!(producer.metrics().bytes_buffered, 600);

    let initial = marker::observed_closes();
    let mut close = Box::pin(producer.clone().close_timeout(Duration::from_secs(20)));
    wait_for_close(close.as_mut(), initial + 1).await;
    assert!(poll_once(waiting.as_mut()).await.is_pending());
    assert!(
        tokio::time::timeout(Duration::from_millis(200), waiting.as_mut())
            .await
            .is_err(),
        "buffer waiter resolved before the accepted record released bytes"
    );
    assert_eq!(producer.metrics().records_queued, 2);
    gate.release();
    drop(release);

    let first_metadata = tokio::time::timeout(WAIT, first.as_mut())
        .await
        .expect("accepted record deadline")
        .expect("accepted record acknowledgement");
    assert_eq!((first_metadata.partition, first_metadata.offset), (0, 1));
    let waiting_result = tokio::time::timeout(WAIT, waiting.as_mut())
        .await
        .expect("pre-admission waiter deadline");
    match (candidate(), waiting_result) {
        (true, Err(Error::Closed)) => {}
        (false, Ok(metadata)) => assert_eq!((metadata.partition, metadata.offset), (0, 2)),
        (sealed, result) => panic!("unexpected waiter result: sealed={sealed} {result:?}"),
    }
    drop(first);
    drop(waiting);
    tokio::time::timeout(WAIT, close.as_mut())
        .await
        .expect("close deadline")
        .expect("close result");
    drop(close);

    assert_eq!(
        producer.metrics().records_queued,
        if candidate() { 2 } else { 3 }
    );
    assert_eq!(producer.metrics().bytes_buffered, 0);
    assert_eq!(callbacks.closes.load(Ordering::SeqCst), 1);
    assert!(callbacks.errors.lock().unwrap().is_empty());
    let acks = callbacks.acknowledgements.lock().unwrap().clone();
    assert!(acks.contains(&("t".to_string(), 0, 1, 600)));
    assert_eq!(acks.len(), if candidate() { 2 } else { 3 });
    if !candidate() {
        assert!(acks.contains(&("t".to_string(), 0, 2, 600)));
    }
    let expected = if candidate() {
        vec![(0, Some(b"warm".to_vec())), (1, Some(vec![b'a'; 600]))]
    } else {
        vec![
            (0, Some(b"warm".to_vec())),
            (1, Some(vec![b'a'; 600])),
            (2, Some(vec![b'b'; 600])),
        ]
    };
    assert_eq!(mock.log_values("t", 0), expected);
    println!(
        "PUBLIC_SHUTDOWN_PRE_ADMISSION sealed={} waiter_ack={} pending_200ms=true bytes=0",
        candidate(),
        !candidate()
    );
}

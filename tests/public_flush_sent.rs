//! Public flush error publication witness; qualification-only.
#![cfg(flush_sent_witness)]

mod common;

use partitionline::producer::{flush_sent_witness as marker, PreSendFault};
use partitionline::{
    Error, ProduceRecord, Producer, ProducerConfig, ProducerInterceptor, RecordMetadata,
};
use std::future::{poll_fn, Future};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::task::Poll;
use std::time::Duration;

const LIMIT: Duration = Duration::from_secs(15);

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

#[tokio::test(flavor = "current_thread")]
async fn public_flush_error_published_before_observation() {
    let consume = std::env::var("PL_CONSUME_FIRST").as_deref() == Ok("1");
    let mock = common::Mock::start().await;
    let callbacks = Counts::default();
    let producer = Producer::new(
        ProducerConfig::bootstrap([mock.addr.clone()])
            .idempotent(false)
            .connections(1)
            .linger(Duration::ZERO)
            .delivery_timeout(Duration::from_secs(30))
            .request_timeout(Duration::from_secs(10))
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
    producer.inject_pre_send_fault(PreSendFault::Encode);
    let failure = tokio::time::timeout(
        LIMIT,
        producer.send(ProduceRecord::to("t").partition(0).value(&b"target"[..])),
    )
    .await
    .expect("target deadline")
    .expect_err("injected encoding failure");
    assert!(matches!(failure, Error::Protocol(_)));
    assert_eq!(callbacks.acks.load(Ordering::SeqCst), 1);
    assert_eq!(callbacks.errors.load(Ordering::SeqCst), 1);
    assert_eq!(producer.metrics().bytes_buffered, 0);
    assert_eq!(mock.log_values("t", 0), vec![(0, Some(b"warm".to_vec()))]);

    let mut first = Box::pin(producer.flush_timeout(Duration::from_secs(10)));
    let initial = poll_fn(|cx| Poll::Ready(first.as_mut().poll(cx))).await;
    assert!(initial.is_pending(), "first flush must wait for its worker response");
    tokio::time::timeout(LIMIT, marker::observed())
        .await
        .expect("worker did not successfully send the flush error");
    // The test holds first without polling it between the marker and the branch.
    if consume {
        let observed = tokio::time::timeout(LIMIT, first.as_mut())
            .await
            .expect("first flush deadline")
            .expect_err("first flush must observe the published worker error");
        assert!(matches!(observed, Error::Protocol(_)));
        tokio::time::timeout(LIMIT, producer.flush_timeout(Duration::from_secs(10)))
            .await
            .expect("following flush deadline")
            .expect("following flush after error observation");
        println!("FLUSH_ERROR_CONSUMED send=ok first=protocol following=ok");
    } else {
        // First still owns the unpolled result while this overlapping flush runs.
        tokio::time::timeout(LIMIT, producer.flush_timeout(Duration::from_secs(10)))
            .await
            .expect("overlapping flush deadline")
            .expect("baseline second flush returns success");
        drop(first);
        tokio::time::timeout(LIMIT, producer.flush_timeout(Duration::from_secs(10)))
            .await
            .expect("following flush deadline")
            .expect("baseline error no longer recorded");
        assert_eq!(callbacks.errors.load(Ordering::SeqCst), 1);
        assert_eq!(producer.metrics().bytes_buffered, 0);
        println!("FLUSH_ERROR_UNOBSERVED send=ok first=unpolled_dropped overlapping=ok following=ok");
        let _close = producer.close_timeout(Duration::from_secs(2)).await;
        return;
    }
    drop(first);
    let _close = producer.close_timeout(Duration::from_secs(2)).await;
}

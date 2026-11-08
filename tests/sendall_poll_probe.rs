//! Diagnostic of a single public send_all poll on a current-thread runtime.
mod common;

use partitionline::{ProduceRecord, Producer, ProducerConfig, ProducerInterceptor, RecordMetadata};
use std::future::{poll_fn, Future};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::task::Poll;
use std::time::{Duration, Instant};

#[derive(Clone, Default)]
struct Counts {
    acks: Arc<AtomicUsize>,
    errors: Arc<AtomicUsize>,
}
impl ProducerInterceptor for Counts {
    fn on_ack(&self, _metadata: &RecordMetadata) {
        let _previous = self.acks.fetch_add(1, Ordering::SeqCst);
    }
    fn on_error(&self, _error: &partitionline::Error) {
        let _previous = self.errors.fetch_add(1, Ordering::SeqCst);
    }
}

#[tokio::test(flavor = "current_thread")]
async fn public_send_all_first_poll_diagnostic() {
    let count: usize = std::env::var("PL_RECORDS")
        .expect("PL_RECORDS")
        .parse()
        .expect("record count");
    assert!((1..8192).contains(&count));
    let mock = common::Mock::start().await;
    let callbacks = Counts::default();
    let producer = Producer::new(
        ProducerConfig::bootstrap([mock.addr.clone()])
            .idempotent(false)
            .connections(1)
            .batch_records(count)
            .batch_bytes(2 * 1024 * 1024)
            .max_request_size(2 * 1024 * 1024)
            .buffer_memory(0)
            .linger(Duration::ZERO)
            .request_timeout(Duration::from_secs(60))
            .delivery_timeout(Duration::from_secs(120))
            .max_block(Duration::from_secs(60))
            .interceptor(callbacks.clone()),
    )
    .await
    .expect("setup");
    let warm = producer
        .send(ProduceRecord::to("t").partition(0).value(&b"warm"[..]))
        .await
        .expect("warmup");
    assert_eq!(warm.offset, 0);
    let mut records = Vec::with_capacity(count);
    for index in 0..count {
        let value = u64::try_from(index).expect("index fits").to_le_bytes();
        records.push(ProduceRecord::to("t").partition(0).value(value.to_vec()));
    }

    let mut timer = Box::pin(tokio::time::sleep(Duration::from_millis(5)));
    assert!(poll_fn(|cx| Poll::Ready(timer.as_mut().poll(cx))).await.is_pending());
    let mut delivery = Box::pin(producer.send_all(records));
    let start = Instant::now();
    let first = poll_fn(|cx| Poll::Ready(delivery.as_mut().poll(cx))).await;
    let poll_elapsed = start.elapsed();
    assert!(first.is_pending());
    let queued = producer.metrics().records_queued;
    let timer_due = poll_fn(|cx| Poll::Ready(timer.as_mut().poll(cx))).await.is_ready();
    println!(
        "SENDALL_POLL records={count} first_poll_ns={} queued_after_poll={queued} five_ms_timer_due={timer_due}",
        poll_elapsed.as_nanos()
    );

    let metadata = tokio::time::timeout(Duration::from_secs(120), delivery.as_mut())
        .await
        .expect("send_all deadline")
        .expect("send_all result");
    assert_eq!(metadata.len(), count);
    for (index, item) in metadata.iter().enumerate() {
        assert_eq!(item.offset, i64::try_from(index + 1).expect("offset fits"));
        assert_eq!(item.partition, 0);
    }
    producer.flush().await.expect("flush");
    assert_eq!(producer.metrics().bytes_buffered, 0);
    assert_eq!(producer.metrics().produce_errors, 0);
    assert_eq!(callbacks.acks.load(Ordering::SeqCst), count + 1);
    assert_eq!(callbacks.errors.load(Ordering::SeqCst), 0);
    let log = mock.log_values("t", 0);
    assert_eq!(log.len(), count + 1);
    assert_eq!(log.first(), Some(&(0, Some(b"warm".to_vec()))));
    for (index, (offset, value)) in log.iter().skip(1).enumerate() {
        assert_eq!(*offset, i64::try_from(index + 1).expect("offset fits"));
        let expected = u64::try_from(index).expect("index fits").to_le_bytes();
        assert_eq!(value.as_deref(), Some(expected.as_slice()));
    }
    drop(delivery);
    producer.close().await.expect("close");
}

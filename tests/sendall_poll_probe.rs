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
    assert!((1..=8193).contains(&count));
    let topic_len: usize = std::env::var("PL_TOPIC_LEN")
        .unwrap_or_else(|_| "1".to_string())
        .parse()
        .expect("topic length");
    assert!((1..=249).contains(&topic_len));
    let topic = "t".repeat(topic_len);
    let linger_ms: u64 = std::env::var("PL_LINGER_MS")
        .unwrap_or_else(|_| "0".to_string())
        .parse()
        .expect("linger");
    assert!(linger_ms == 0 || linger_ms == 5);
    let mock = common::Mock::start().await;
    mock.set_topic_partitions(&topic, 1);
    let callbacks = Counts::default();
    let use_interceptor = std::env::var("PL_INTERCEPTOR").as_deref() == Ok("1");
    let config = ProducerConfig::bootstrap([mock.addr.clone()])
        .idempotent(false)
        .connections(1)
        .batch_records(count)
        .batch_bytes(2 * 1024 * 1024)
        .max_request_size(2 * 1024 * 1024)
        .buffer_memory(0)
        .linger(Duration::from_millis(linger_ms))
        .request_timeout(Duration::from_secs(60))
        .delivery_timeout(Duration::from_secs(120))
        .max_block(Duration::from_secs(60));
    let config = if use_interceptor {
        config.interceptor(callbacks.clone())
    } else {
        config
    };
    let producer = Producer::new(config).await.expect("setup");
    let warm = producer
        .send(ProduceRecord::to(topic.as_str()).partition(0).value(&b"warm"[..]))
        .await
        .expect("warmup");
    assert_eq!(warm.offset, 0);
    let mut records = Vec::with_capacity(count);
    for index in 0..count {
        let value = u64::try_from(index).expect("index fits").to_le_bytes();
        records.push(ProduceRecord::to(topic.as_str()).partition(0).value(value.to_vec()));
    }

    let timer_deadline = Instant::now() + Duration::from_millis(5);
    let mut timer = Box::pin(tokio::time::sleep_until(tokio::time::Instant::from_std(timer_deadline)));
    assert!(poll_fn(|cx| Poll::Ready(timer.as_mut().poll(cx))).await.is_pending());
    let mut delivery = Box::pin(producer.send_all(records));
    let start = Instant::now();
    let first = poll_fn(|cx| Poll::Ready(delivery.as_mut().poll(cx))).await;
    let poll_elapsed = start.elapsed();
    assert!(first.is_pending());
    let queued = producer.metrics().records_queued;
    let timer_ready = poll_fn(|cx| Poll::Ready(timer.as_mut().poll(cx))).await.is_ready();
    let wall_deadline_passed = Instant::now() >= timer_deadline;
    println!(
        "SENDALL_POLL records={count} topic_bytes={topic_len} linger_ms={linger_ms} interceptor={use_interceptor} first_poll_ns={} queued_after_poll={queued} wall_5ms_passed={wall_deadline_passed} timer_poll_ready={timer_ready}",
        poll_elapsed.as_nanos()
    );

    let mut max_poll = poll_elapsed;
    let mut polls = 1usize;
    let metadata = tokio::time::timeout(Duration::from_secs(120), poll_fn(|cx| {
        let started = Instant::now();
        let result = delivery.as_mut().poll(cx);
        max_poll = max_poll.max(started.elapsed());
        polls += 1;
        result
    }))
        .await
        .expect("send_all deadline")
        .expect("send_all result");
    let complete_elapsed = start.elapsed();
    println!(
        "SENDALL_COMPLETE records={count} topic_bytes={topic_len} linger_ms={linger_ms} interceptor={use_interceptor} polls={polls} max_poll_ns={} complete_ns={}",
        max_poll.as_nanos(),
        complete_elapsed.as_nanos()
    );
    assert_eq!(metadata.len(), count);
    for (index, item) in metadata.iter().enumerate() {
        assert_eq!(item.offset, i64::try_from(index + 1).expect("offset fits"));
        assert_eq!(item.partition, 0);
    }
    producer.flush().await.expect("flush");
    assert_eq!(producer.metrics().bytes_buffered, 0);
    assert_eq!(producer.metrics().produce_errors, 0);
    assert_eq!(callbacks.acks.load(Ordering::SeqCst), if use_interceptor { count + 1 } else { 0 });
    assert_eq!(callbacks.errors.load(Ordering::SeqCst), 0);
    let batches = mock.produce_batches();
    let sizes: Vec<i32> = batches.iter().skip(1).map(|(_, _, count, _)| *count).collect();
    assert_eq!(
        sizes.iter().map(|size| i64::from(*size)).sum::<i64>(),
        i64::try_from(count).expect("count fits")
    );
    println!("SENDALL_BATCHES records={count} topic_bytes={topic_len} linger_ms={linger_ms} interceptor={use_interceptor} sizes={sizes:?}");
    let log = mock.log_values(&topic, 0);
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

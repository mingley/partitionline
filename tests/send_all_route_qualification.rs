//! Instrumented qualification controls for isolated candidate 3fd30f13.
//! These mocks, counters and scheduling gates are not a throughput benchmark.
#![cfg(send_all_qualification)]
mod common;

use partitionline::interceptor::ProducerInterceptor;
use partitionline::producer::send_all_qualification as q;
use partitionline::{Error, Partitioner, ProduceRecord, Producer, ProducerConfig, RecordMetadata, StickyPartitioner};
use std::future::{poll_fn, Future};
use std::sync::{atomic::{AtomicUsize, Ordering}, Arc};
use std::task::Poll;
use std::time::Duration;

const LIMIT: Duration = Duration::from_secs(180);

fn config(mock: &common::Mock) -> ProducerConfig {
    ProducerConfig::bootstrap([mock.addr.clone()])
        .connections(1)
        .idempotent(false)
        .linger(Duration::from_millis(1))
        .batch_records(256)
        .metadata_max_age(Duration::from_secs(3600))
        .request_timeout(Duration::from_secs(120))
        .delivery_timeout(Duration::from_secs(120))
        .max_block(Duration::from_secs(120))
}

fn records(topic: &str, n: usize) -> Vec<ProduceRecord> {
    (0..n).map(|i| ProduceRecord::to(topic.to_owned()).partition(0).value((i as u64).to_le_bytes().to_vec())).collect()
}

async fn warm(producer: &Producer, topic: &str, partition: i32) {
    let md = producer.send(ProduceRecord::to(topic.to_owned()).partition(partition).value(b"warm".to_vec())).await.unwrap();
    assert_eq!(md.offset, 0);
}

fn verify(mock: &common::Mock, topic: &str, partition: i32, outputs: &[RecordMetadata], ids: &[usize], initial: i64) {
    assert_eq!(outputs.len(), ids.len());
    let log = mock.log_values(topic, partition);
    assert_eq!(log.len(), ids.len() + initial as usize);
    for (i, id) in ids.iter().copied().enumerate() {
        assert_eq!(outputs[i].partition, partition);
        assert_eq!(outputs[i].offset, initial + i as i64);
        let (offset, value) = &log[i + initial as usize];
        assert_eq!(*offset, initial + i as i64);
        assert_eq!(value.as_deref(), Some((id as u64).to_le_bytes().as_slice()));
    }
}

async fn simple_case(n: usize) {
    let mock = common::Mock::start().await;
    let producer = Producer::new(config(&mock)).await.unwrap();
    warm(&producer, "t", 0).await;
    q::reset();
    let out = tokio::time::timeout(LIMIT, producer.send_all(records("t", n))).await.unwrap().unwrap();
    let snap = q::snapshot();
    eprintln!("n={n} snapshot={snap:?}");
    verify(&mock, "t", 0, &out, &(0..n).collect::<Vec<_>>(), 1);
    if (32..8192).contains(&n) {
        assert_eq!(snap.shortcut, n);
        assert_eq!(snap.fallback, 0);
        assert_eq!(snap.yields, (n - 1) / 512);
    } else {
        assert_eq!(snap.shortcut, 0);
        assert_eq!(snap.fallback, n);
        assert_eq!(snap.yields, 0);
    }
}

#[tokio::test(flavor = "current_thread")]
async fn public_size_boundaries_and_readback() {
    for n in [31, 32, 8191, 8192] {
        simple_case(n).await;
    }
}

#[tokio::test(flavor = "current_thread")]
async fn maximum_topic_and_oversized_rejection() {
    let mock = common::Mock::start().await;
    let producer = Producer::new(config(&mock)).await.unwrap();
    let topic = "a".repeat(249);
    warm(&producer, &topic, 0).await;
    q::reset();
    let out = tokio::time::timeout(LIMIT, producer.send_all(records(&topic, 32))).await.unwrap().unwrap();
    let snap = q::snapshot();
    assert_eq!(snap.shortcut, 32);
    assert_eq!(snap.fallback, 0);
    verify(&mock, &topic, 0, &out, &(0..32).collect::<Vec<_>>(), 1);

    let mock2 = common::Mock::start().await;
    let zero_age = Producer::new(config(&mock2).metadata_max_age(Duration::ZERO)).await.unwrap();
    q::reset();
    let too_long = "a".repeat(250);
    let err = tokio::time::timeout(LIMIT, zero_age.send_all(records(&too_long, 32))).await.unwrap().unwrap_err();
    let snap = q::snapshot();
    eprintln!("oversized={err:?}, snapshot={snap:?}");
    assert!(matches!(err, Error::Protocol(_)));
    assert_eq!(snap.shortcut, 0);
    assert_eq!(snap.fallback, 1);
    assert_eq!(zero_age.metrics().records_queued, 0);
    assert_eq!(mock2.log_len(&too_long, 0), 0);
}

struct P0(Arc<AtomicUsize>);
impl Partitioner for P0 {
    fn partition(&self, _topic: &str, _key: Option<&[u8]>, _n: i32) -> i32 {
        let _ = self.0.fetch_add(1, Ordering::SeqCst);
        0
    }
}

#[tokio::test(flavor = "current_thread")]
async fn mixed_and_implicit_routes_fall_back() {
    let mock = common::Mock::start().await;
    mock.set_topic_partitions("t", 2);
    let calls = Arc::new(AtomicUsize::new(0));
    let producer = Producer::new(config(&mock).partitioner(P0(Arc::clone(&calls)))).await.unwrap();
    warm(&producer, "t", 0).await;
    warm(&producer, "t", 1).await;
    q::reset();
    let mixed: Vec<_> = (0..32).map(|i| ProduceRecord::to("t").partition((i % 2) as i32).value((i as u64).to_le_bytes().to_vec())).collect();
    let out = tokio::time::timeout(LIMIT, producer.send_all(mixed)).await.unwrap().unwrap();
    let snap = q::snapshot();
    assert_eq!((snap.shortcut, snap.fallback), (0, 32));
    for p in 0..2 {
        let md: Vec<_> = out.iter().filter(|m| m.partition == p).cloned().collect();
        let ids: Vec<_> = (0..32).filter(|i| i % 2 == p as usize).collect();
        verify(&mock, "t", p, &md, &ids, 1);
    }

    let mock2 = common::Mock::start().await;
    let calls2 = Arc::new(AtomicUsize::new(0));
    let producer2 = Producer::new(config(&mock2).partitioner(P0(Arc::clone(&calls2)))).await.unwrap();
    warm(&producer2, "t", 0).await;
    q::reset();
    let implicit: Vec<_> = (0..32).map(|i| ProduceRecord::to("t").value((i as u64).to_le_bytes().to_vec())).collect();
    let out = tokio::time::timeout(LIMIT, producer2.send_all(implicit)).await.unwrap().unwrap();
    let snap = q::snapshot();
    assert_eq!((snap.shortcut, snap.fallback), (0, 32));
    assert!(calls2.load(Ordering::SeqCst) > 0);
    verify(&mock2, "t", 0, &out, &(0..32).collect::<Vec<_>>(), 1);
}

struct Rewrite;
impl ProducerInterceptor for Rewrite {
    fn on_send(&self, rec: ProduceRecord) -> ProduceRecord { rec.partition(1) }
}

#[tokio::test(flavor = "current_thread")]
async fn interceptor_and_sticky_fall_back() {
    let mock = common::Mock::start().await;
    mock.set_topic_partitions("t", 2);
    let producer = Producer::new(config(&mock).interceptor(Rewrite)).await.unwrap();
    warm(&producer, "t", 0).await;
    q::reset();
    let out = tokio::time::timeout(LIMIT, producer.send_all(records("t", 32))).await.unwrap().unwrap();
    let snap = q::snapshot();
    assert_eq!((snap.shortcut, snap.fallback), (0, 32));
    verify(&mock, "t", 1, &out, &(0..32).collect::<Vec<_>>(), 1);
    assert_eq!(mock.log_len("t", 0), 0);

    let mock2 = common::Mock::start().await;
    let sticky = Producer::new(config(&mock2).partitioner(StickyPartitioner::new())).await.unwrap();
    warm(&sticky, "t", 0).await;
    q::reset();
    let out = tokio::time::timeout(LIMIT, sticky.send_all(records("t", 32))).await.unwrap().unwrap();
    let snap = q::snapshot();
    assert_eq!((snap.shortcut, snap.fallback), (0, 32));
    verify(&mock2, "t", 0, &out, &(0..32).collect::<Vec<_>>(), 1);
}

#[tokio::test(flavor = "current_thread")]
async fn zero_metadata_age_uses_fallback() {
    let mock = common::Mock::start().await;
    let producer = Producer::new(config(&mock).metadata_max_age(Duration::ZERO)).await.unwrap();
    warm(&producer, "t", 0).await;
    q::reset();
    let out = tokio::time::timeout(LIMIT, producer.send_all(records("t", 32))).await.unwrap().unwrap();
    let snap = q::snapshot();
    assert_eq!((snap.shortcut, snap.fallback), (0, 32));
    verify(&mock, "t", 0, &out, &(0..32).collect::<Vec<_>>(), 1);
}

// Dropping a test future while it is held at a gate should not hang the test.
struct ReleaseOnDrop(bool);
impl Drop for ReleaseOnDrop { fn drop(&mut self) { if self.0 { q::release(); } } }

#[tokio::test(flavor = "current_thread")]
async fn retired_worker_then_replacement() {
    let mock = common::Mock::start().await;
    let producer = Producer::new(config(&mock)).await.unwrap();
    warm(&producer, "t", 0).await;
    let old = producer.test_worker_tasks();
    assert_eq!(old.len(), 1);
    let old_id = old[0].id();
    q::reset();
    q::arm(1);
    let mut guard = ReleaseOnDrop(true);
    let task_producer = producer.clone();
    let task = tokio::spawn(async move { task_producer.send_all(records("t", 64)).await });
    tokio::time::timeout(LIMIT, q::reached()).await.unwrap();
    old[0].abort();
    tokio::time::timeout(LIMIT, async { while !old[0].is_finished() { tokio::task::yield_now().await; } }).await.unwrap();
    q::release();
    guard.0 = false;
    let out = tokio::time::timeout(LIMIT, task).await.unwrap().unwrap().unwrap();
    let snap = q::snapshot();
    eprintln!("retired snapshot={snap:?}");
    assert!(snap.fallback >= 1);
    assert!(snap.shortcut >= 1);
    assert!(producer.test_worker_tasks().iter().any(|task| task.id() != old_id));
    verify(&mock, "t", 0, &out, &(0..64).collect::<Vec<_>>(), 1);
}

#[tokio::test(flavor = "current_thread")]
async fn metadata_changes_before_first_nudge() {
    let mock = common::Mock::start_two_node().await;
    let producer = Producer::new(config(&mock)).await.unwrap();
    warm(&producer, "t", 0).await;
    q::reset();
    q::arm(1);
    let mut guard = ReleaseOnDrop(true);
    let p = producer.clone();
    let task = tokio::spawn(async move { p.send_all(records("t", 64)).await });
    tokio::time::timeout(LIMIT, q::reached()).await.unwrap();
    let before = mock.metadata_calls();
    mock.set_partition_leader("t", 0, 1);
    let infos = producer.partitions_for("t").await.unwrap();
    assert!(mock.metadata_calls() > before);
    assert!(infos.iter().any(|part| part.partition == 0 && part.leader == 1));
    q::release();
    guard.0 = false;
    let out = tokio::time::timeout(LIMIT, task).await.unwrap().unwrap().unwrap();
    let snap = q::snapshot();
    eprintln!("changed metadata snapshot={snap:?}");
    assert!(snap.shortcut + snap.fallback >= 64);
    verify(&mock, "t", 0, &out, &(0..64).collect::<Vec<_>>(), 1);
}

#[tokio::test(flavor = "current_thread")]
async fn partial_admission_cancellation() {
    let mock = common::Mock::start().await;
    let producer = Producer::new(config(&mock)).await.unwrap();
    warm(&producer, "t", 0).await;
    q::reset();
    q::arm(2);
    let mut future = Box::pin(tokio::task::unconstrained(producer.send_all(records("t", 1024))));
    assert!(poll_fn(|cx| Poll::Ready(future.as_mut().poll(cx))).await.is_pending());
    tokio::time::timeout(LIMIT, q::reached()).await.unwrap();
    let before = producer.metrics();
    assert_eq!(before.records_queued, 513);
    assert_eq!(q::snapshot().yields, 1);
    drop(future);
    tokio::time::timeout(LIMIT, async {
        loop {
            let m = producer.metrics();
            if m.records_acked == 513 && m.bytes_buffered == 0 { break; }
            tokio::task::yield_now().await;
        }
    }).await.unwrap();
    let after = producer.metrics();
    assert_eq!(after.records_queued, 513);
    assert_eq!(after.produce_errors, 0);
    let log = mock.log_values("t", 0);
    assert_eq!(log.len(), 513);
    for (i, (offset, value)) in log.iter().skip(1).enumerate() {
        assert_eq!(*offset, (i + 1) as i64);
        assert_eq!(value.as_deref(), Some((i as u64).to_le_bytes().as_slice()));
    }
}

#[tokio::test(flavor = "current_thread")]
async fn full_then_draining_connect_queue() {
    let mock = common::Mock::start().await;
    let producer = Producer::new(config(&mock)).await.unwrap();
    warm(&producer, "t", 0).await;
    q::reset();
    let filled = producer.__qualification_fill_connect_queue();
    assert!(filled > 0);
    assert_eq!(producer.__qualification_connect_capacity(), 0);
    q::arm(2);
    let mut guard = ReleaseOnDrop(true);
    let mut future = Box::pin(tokio::task::unconstrained(producer.send_all(records("t", 1024))));
    assert!(poll_fn(|cx| Poll::Ready(future.as_mut().poll(cx))).await.is_pending());
    tokio::time::timeout(LIMIT, q::reached()).await.unwrap();
    assert_eq!(producer.metrics().records_queued, 513);
    let before = q::snapshot();
    assert!(before.nudge_full > 0, "{before:?}");
    tokio::time::timeout(LIMIT, async {
        while producer.__qualification_connect_capacity() == 0 { tokio::task::yield_now().await; }
    }).await.unwrap();
    q::release();
    guard.0 = false;
    let out = tokio::time::timeout(LIMIT, future).await.unwrap().unwrap();
    let after = q::snapshot();
    eprintln!("queue before={before:?}, after={after:?}, filled={filled}");
    assert!(after.nudge_sent > before.nudge_sent);
    verify(&mock, "t", 0, &out, &(0..1024).collect::<Vec<_>>(), 1);
}

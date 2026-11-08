//! Isolated public-path cancellation witness. This test is not part of the candidate.
mod common;

use bytes::Bytes;
use partitionline::{Error, ProduceRecord, Producer, ProducerConfig, ProducerInterceptor, RecordMetadata, StickyPartitioner};
use std::future::Future;
use std::sync::{
    atomic::{AtomicBool, AtomicUsize, Ordering},
    mpsc, Arc,
};
use std::task::{Context, Waker};
use std::time::{Duration, Instant};

const TOTAL: usize = 1025;
const VALUE_BYTES: u64 = 8;

struct Gate {
    entered: mpsc::Sender<Instant>,
    release: parking_lot::Mutex<Option<mpsc::Receiver<()>>>,
}

struct Owner {
    bytes: [u8; 8],
    index: usize,
    drops: Arc<Vec<AtomicUsize>>,
    gate: Option<Arc<Gate>>,
}
impl AsRef<[u8]> for Owner {
    fn as_ref(&self) -> &[u8] { &self.bytes }
}
impl Drop for Owner {
    fn drop(&mut self) {
        let _previous = self.drops[self.index].fetch_add(1, Ordering::SeqCst);
        if let Some(gate) = &self.gate {
            let _sent = gate.entered.send(Instant::now());
            let receiver = gate.release.lock().take();
            if let Some(receiver) = receiver {
                let _released = receiver.recv();
            }
        }
    }
}
#[derive(Clone, Default)]
struct Callbacks {
    offsets: Arc<parking_lot::Mutex<Vec<i64>>>,
    errors: Arc<AtomicUsize>,
}
impl ProducerInterceptor for Callbacks {
    fn on_ack(&self, metadata: &RecordMetadata) {
        self.offsets.lock().push(metadata.offset);
    }
    fn on_error(&self, _error: &Error) {
        let _previous = self.errors.fetch_add(1, Ordering::SeqCst);
    }
}
fn make_records(n: usize, drops: &Arc<Vec<AtomicUsize>>, gate: Option<(usize, Arc<Gate>)>) -> Vec<ProduceRecord> {
    (0..n).map(|index| {
        let value = Bytes::from_owner(Owner {
            bytes: u64::try_from(index).unwrap().to_le_bytes(),
            index,
            drops: Arc::clone(drops),
            gate: gate.as_ref().and_then(|(target, gate)| (*target == index).then(|| Arc::clone(gate))),
        });
        ProduceRecord::to("t").partition(0).value(value)
    }).collect()
}
fn drops(n: usize) -> Arc<Vec<AtomicUsize>> {
    Arc::new((0..n).map(|_| AtomicUsize::new(0)).collect())
}
fn assert_drops(drops: &[AtomicUsize], first: usize) {
    for (i, count) in drops.iter().enumerate() {
        assert_eq!(count.load(Ordering::SeqCst), usize::from(i < first), "owner {i}");
    }
}
fn config(mock: &common::Mock, cap: usize) -> ProducerConfig {
    ProducerConfig::bootstrap([mock.addr.clone()])
        .connections(1)
        .idempotent(false)
        .batch_records(TOTAL + 1)
        .batch_bytes(2 * 1024 * 1024)
        .max_request_size(2 * 1024 * 1024)
        .buffer_memory(cap)
        .linger(Duration::ZERO)
        .metadata_max_age(Duration::from_secs(600))
        .delivery_timeout(Duration::from_secs(120))
        .request_timeout(Duration::from_secs(30))
        .max_block(Duration::from_secs(30))
}
async fn warm(mock: &common::Mock, config: ProducerConfig) -> Producer {
    let producer = Producer::new(config).await.unwrap();
    let receipt = producer.send(ProduceRecord::to("t").partition(0).value("warm")).await.unwrap();
    assert_eq!(receipt.offset, 0);
    producer.flush().await.unwrap();
    let metrics = producer.metrics();
    assert_eq!(metrics.records_queued, 1);
    assert_eq!(metrics.records_acked, 1);
    assert_eq!(metrics.produce_errors, 0);
    assert_eq!(metrics.bytes_buffered, 0);
    assert_eq!(mock.log_len("t", 0), 1);
    producer
}
fn expected_accepted() -> usize {
    std::env::var("PL_EXPECTED_ACCEPTED").unwrap().parse().unwrap()
}
fn assert_metrics(producer: &Producer, accepted: usize, acked: usize) {
    let metrics = producer.metrics();
    assert_eq!(metrics.records_queued, u64::try_from(1 + accepted).unwrap());
    assert_eq!(metrics.records_acked, u64::try_from(1 + acked).unwrap());
    assert_eq!(metrics.produce_errors, 0);
    assert_eq!(metrics.bytes_buffered, u64::try_from(accepted - acked).unwrap() * VALUE_BYTES);
}
fn assert_log(mock: &common::Mock, accepted: usize) {
    let log = mock.log_values("t", 0);
    assert_eq!(log.len(), 1 + accepted);
    assert_eq!(log.first(), Some(&(0, Some(b"warm".to_vec()))));
    for (index, (offset, value)) in log.iter().skip(1).enumerate() {
        assert_eq!(*offset, i64::try_from(index + 1).unwrap());
        let expected = u64::try_from(index).unwrap().to_le_bytes();
        assert_eq!(value.as_deref(), Some(expected.as_slice()), "value for record {index}");
    }
}
async fn cancel_and_check(sticky: bool, callbacks: bool) {
    let mock = common::Mock::start().await;
    let hooks = Callbacks::default();
    let mut config = config(&mock, 0);
    if sticky { config = config.partitioner(StickyPartitioner::new()); }
    if callbacks { config = config.interceptor(hooks.clone()); }
    let producer = warm(&mock, config).await;
    let expected = if sticky || callbacks { TOTAL } else { expected_accepted() };
    let owners = drops(TOTAL);
    let records = make_records(TOTAL, &owners, None);
    let mut send = Box::pin(tokio::task::unconstrained(producer.send_all(records)));
    let mut cx = Context::from_waker(Waker::noop());
    let poll = send.as_mut().poll(&mut cx);
    assert!(poll.is_pending(), "first poll should be pending");
    assert_metrics(&producer, expected, 0);
    assert_eq!(mock.log_len("t", 0), 1);
    assert_drops(&owners, expected);
    drop(send);
    assert_metrics(&producer, expected, 0);
    assert_drops(&owners, TOTAL);
    tokio::time::timeout(Duration::from_secs(30), producer.flush()).await.unwrap().unwrap();
    assert_metrics(&producer, expected, expected);
    assert_log(&mock, expected);
    if callbacks {
        let offsets = hooks.offsets.lock().clone();
        let expected_offsets: Vec<i64> = (0..=i64::try_from(expected).unwrap()).collect();
        assert_eq!(offsets, expected_offsets);
        assert_eq!(hooks.errors.load(Ordering::SeqCst), 0);
    }
    println!("CANCEL_PUBLIC sticky={sticky} callbacks={callbacks} total={TOTAL} accepted={expected} accepted_ids=0..{expected} caller_inputs_discarded={} acked={expected} bytes=0", TOTAL - expected);
    producer.close_timeout(Duration::from_secs(30)).await.unwrap();
}

#[tokio::test(flavor = "current_thread")]
async fn candidate_yield_cancellation_preserves_the_accepted_prefix() {
    cancel_and_check(false, false).await;
}
#[tokio::test(flavor = "current_thread")]
async fn interceptor_fallback_preserves_all_accepted_records_and_callbacks() {
    cancel_and_check(false, true).await;
}
#[tokio::test(flavor = "current_thread")]
async fn sticky_fallback_preserves_all_accepted_records() {
    cancel_and_check(true, false).await;
}
#[tokio::test(flavor = "current_thread")]
async fn completed_no_interceptor_send_all_returns_each_receipt() {
    let mock = common::Mock::start().await;
    let producer = warm(&mock, config(&mock, 0)).await;
    let owners = drops(TOTAL);
    let receipts = tokio::time::timeout(Duration::from_secs(30), producer.send_all(make_records(TOTAL, &owners, None))).await.unwrap().unwrap();
    assert_eq!(receipts.len(), TOTAL);
    for (index, receipt) in receipts.iter().enumerate() {
        assert_eq!(receipt.partition, 0);
        assert_eq!(receipt.offset, i64::try_from(index + 1).unwrap());
    }
    producer.flush().await.unwrap();
    assert_metrics(&producer, TOTAL, TOTAL);
    assert_log(&mock, TOTAL);
    assert_drops(&owners, TOTAL);
    println!("COMPLETED_PUBLIC receipts={TOTAL} ordered=true errors=0 bytes=0");
    producer.close_timeout(Duration::from_secs(30)).await.unwrap();
}

// Register the timer before the cancellation. The release thread is external
// to the single-thread runtime, so a blocking owner cannot deadlock this test.
async fn blocking_cancel(existing_buffer_wait: bool) {
    let mock = common::Mock::start().await;
    let count = if existing_buffer_wait { 128 } else { TOTAL };
    let accepted = if existing_buffer_wait { 32 } else { 512 };
    let producer = warm(&mock, config(&mock, if existing_buffer_wait { 256 } else { 0 })).await;
    let ticked = Arc::new(AtomicBool::new(false));
    let deadline = Instant::now() + Duration::from_secs(2);
    let (registered_tx, registered_rx) = tokio::sync::oneshot::channel();
    let task_ticked = Arc::clone(&ticked);
    let ticker = tokio::spawn(async move {
        let mut timer = Box::pin(tokio::time::sleep_until(tokio::time::Instant::from_std(deadline)));
        let mut registered = Some(registered_tx);
        std::future::poll_fn(|cx| {
            let state = timer.as_mut().poll(cx);
            if let Some(registered) = registered.take() {
                let _sent = registered.send(());
            }
            state
        }).await;
        task_ticked.store(true, Ordering::SeqCst);
    });
    registered_rx.await.unwrap();
    let (entered_tx, entered_rx) = mpsc::channel();
    let (release_tx, release_rx) = mpsc::channel();
    let gate = Arc::new(Gate { entered: entered_tx, release: parking_lot::Mutex::new(Some(release_rx)) });
    let watch_ticked = Arc::clone(&ticked);
    let watchdog = std::thread::spawn(move || {
        let entered = entered_rx.recv_timeout(Duration::from_secs(15)).unwrap();
        std::thread::sleep(Duration::from_secs(3));
        let released = Instant::now();
        let ticked_before_release = watch_ticked.load(Ordering::SeqCst);
        let _sent = release_tx.send(());
        (entered, released, ticked_before_release)
    });
    let owners = drops(count);
    let records = make_records(count, &owners, Some((accepted, Arc::clone(&gate))));
    let mut send = Box::pin(tokio::task::unconstrained(producer.send_all(records)));
    let mut cx = Context::from_waker(Waker::noop());
    let poll = send.as_mut().poll(&mut cx);
    assert!(poll.is_pending(), "control must stop at the targeted await");
    assert_metrics(&producer, accepted, 0);
    assert_eq!(mock.log_len("t", 0), 1);
    assert_drops(&owners, accepted);
    assert!(Instant::now() < deadline, "setup exceeded pre-registered timer deadline");
    let started = Instant::now();
    drop(send);
    let held = started.elapsed();
    assert_metrics(&producer, accepted, 0);
    assert_drops(&owners, count);
    let (entered, released, ticked_before_release) = watchdog.join().unwrap();
    assert!(entered < deadline && deadline < released);
    assert!(!ticked_before_release, "timer task ran while current runtime was inside Drop");
    assert!(held >= Duration::from_millis(2900));
    tokio::time::timeout(Duration::from_secs(5), ticker).await.unwrap().unwrap();
    assert!(ticked.load(Ordering::SeqCst));
    tokio::time::timeout(Duration::from_secs(30), producer.flush()).await.unwrap().unwrap();
    assert_metrics(&producer, accepted, accepted);
    assert_log(&mock, accepted);
    println!("BLOCKING_DROP existing_buffer_wait={existing_buffer_wait} accepted={accepted} held_ms={} timer_fired_before_release={ticked_before_release}", held.as_millis());
    producer.close_timeout(Duration::from_secs(30)).await.unwrap();
}
#[tokio::test(flavor = "current_thread")]
async fn existing_buffer_wait_already_exposes_blocking_owner_drop() {
    blocking_cancel(true).await;
}
#[tokio::test(flavor = "current_thread")]
async fn candidate_yield_exposes_blocking_unaccepted_owner_drop() {
    if expected_accepted() == 512 {
        blocking_cancel(false).await;
    } else {
        println!("BLOCKING_DROP candidate_yield not applicable: baseline admitted all records");
    }
}

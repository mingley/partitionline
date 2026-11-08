use super::*;
use std::task::{Context, Poll};

async fn setup(
    cfg: ProducerConfig,
) -> (Arc<Shared>, tokio::net::TcpListener, tokio::net::TcpStream) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap().to_string();
    let meta = BrokerConn::connect(&addr, "failure-cleanup-test", Duration::from_secs(1))
        .await
        .unwrap();
    let (peer, _) = listener.accept().await.unwrap();
    (test_shared(cfg, meta), listener, peer)
}

fn records(
    shared: &Shared,
    count: usize,
    mixed_topics: bool,
) -> (
    Vec<Pending>,
    Vec<oneshot::Receiver<Result<RecordMetadata>>>,
    Arc<StickyRecordBudget>,
    u64,
) {
    let topic_a: Arc<str> = Arc::from("failure-a");
    let topic_b: Arc<str> = Arc::from("failure-b");
    let budget = Arc::new(StickyRecordBudget::new(count.max(1)));
    let mut pending = Vec::with_capacity(count);
    let mut receivers = Vec::with_capacity(count);
    let mut bytes = 0;
    for index in 0..count {
        let (tx, rx) = oneshot::channel();
        let now = Instant::now();
        let mut rec = ProduceRecord::to("failure-a").partition(0);
        rec.topic = Arc::clone(if mixed_topics && index % 2 != 0 {
            &topic_b
        } else {
            &topic_a
        });
        if index % 2 == 0 {
            rec.value = Some(Bytes::from_static(b"x"));
            bytes += 1;
        }
        pending.push(Pending {
            sticky_record: Some(budget.acquire().unwrap()),
            sticky: None,
            rec,
            tx: Some(tx),
            seq: None,
            batch_base_seq: None,
            batch_len: 1,
            epoch_gen: 0,
            deadline: now,
            queued_at: now,
            retry: 0,
            skip_meta_refresh: false,
            retry_after: now,
        });
        receivers.push(rx);
    }
    assert!(shared.try_reserve_buffer(bytes));
    (pending, receivers, budget, bytes)
}

fn poll_once<F: Future>(future: Pin<&mut F>) -> Poll<F::Output> {
    let mut context = Context::from_waker(std::task::Waker::noop());
    future.poll(&mut context)
}

async fn assert_timeouts(receivers: Vec<oneshot::Receiver<Result<RecordMetadata>>>) {
    for receiver in receivers {
        assert!(matches!(receiver.await, Ok(Err(Error::Timeout))));
    }
}

struct CountErrors(Arc<AtomicUsize>);
impl crate::interceptor::ProducerInterceptor for CountErrors {
    fn on_error(&self, _: &Error) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}

struct PanicErrors(Arc<AtomicUsize>);
impl crate::interceptor::ProducerInterceptor for PanicErrors {
    fn on_error(&self, _: &Error) {
        self.0.fetch_add(1, Ordering::SeqCst);
        panic!("failure interceptor");
    }
}

#[tokio::test(flavor = "current_thread")]
async fn unpolled_helper_releases_and_reports_each_record() {
    let calls = Arc::new(AtomicUsize::new(0));
    let (shared, _listener, _peer) =
        setup(ProducerConfig::default().interceptor(CountErrors(Arc::clone(&calls)))).await;
    let (batch, receivers, budget, bytes) = records(&shared, 3, true);
    assert_eq!(bytes, 2);
    let cleanup = fail_kept_pendings(&shared, batch, Error::Timeout);
    drop(cleanup);
    assert_timeouts(receivers).await;
    assert_eq!(shared.buffered_bytes.load(Ordering::SeqCst), 0);
    assert_eq!(budget.used.load(Ordering::SeqCst), 0);
    assert_eq!(shared.m_errors.load(Ordering::SeqCst), 3);
    assert_eq!(calls.load(Ordering::SeqCst), 3);
}

#[tokio::test(flavor = "current_thread")]
async fn cancellation_during_sum_and_after_a_callback_chunk_is_exact_once() {
    let calls = Arc::new(AtomicUsize::new(0));
    let (shared, _listener, _peer) =
        setup(ProducerConfig::default().interceptor(CountErrors(Arc::clone(&calls)))).await;

    let (batch, receivers, budget, bytes) = records(&shared, 1, false);
    let mut cleanup = Box::pin(fail_kept_pendings(&shared, batch, Error::Timeout));
    assert!(matches!(poll_once(cleanup.as_mut()), Poll::Pending));
    assert_eq!(shared.buffered_bytes.load(Ordering::SeqCst), bytes);
    drop(cleanup);
    assert_timeouts(receivers).await;
    assert_eq!(shared.buffered_bytes.load(Ordering::SeqCst), 0);
    assert_eq!(budget.used.load(Ordering::SeqCst), 0);
    assert_eq!(calls.load(Ordering::SeqCst), 1);

    let count = 8 * 1024 + 3;
    let (batch, receivers, budget, _) = records(&shared, count, true);
    let mut cleanup = Box::pin(fail_kept_pendings(&shared, batch, Error::Timeout));
    assert!(matches!(poll_once(cleanup.as_mut()), Poll::Pending));
    assert!(matches!(poll_once(cleanup.as_mut()), Poll::Pending));
    assert!(matches!(poll_once(cleanup.as_mut()), Poll::Pending));
    assert_eq!(calls.load(Ordering::SeqCst), 1 + 8 * 1024);
    assert_eq!(shared.buffered_bytes.load(Ordering::SeqCst), 0);
    assert_eq!(budget.used.load(Ordering::SeqCst), 3);
    drop(cleanup);
    assert_timeouts(receivers).await;
    assert_eq!(shared.buffered_bytes.load(Ordering::SeqCst), 0);
    assert_eq!(budget.used.load(Ordering::SeqCst), 0);
    assert_eq!(calls.load(Ordering::SeqCst), 1 + count);
    assert_eq!(shared.m_errors.load(Ordering::SeqCst), (1 + count) as u64);
}

#[tokio::test(flavor = "current_thread")]
async fn normal_completion_reuses_the_batch_and_preserves_error() {
    let calls = Arc::new(AtomicUsize::new(0));
    let (shared, _listener, _peer) =
        setup(ProducerConfig::default().interceptor(CountErrors(Arc::clone(&calls)))).await;
    let (batch, receivers, budget, _) = records(&shared, 4, false);
    let capacity = batch.capacity();
    let empty = fail_kept_pendings(&shared, batch, Error::broker(17, "test")).await;
    assert!(empty.is_empty());
    assert!(empty.capacity() >= capacity);
    for receiver in receivers {
        assert!(matches!(receiver.await, Ok(Err(Error::Broker { code: 17, .. }))));
    }
    assert_eq!(shared.buffered_bytes.load(Ordering::SeqCst), 0);
    assert_eq!(budget.used.load(Ordering::SeqCst), 0);
    assert_eq!(shared.m_errors.load(Ordering::SeqCst), 4);
    assert_eq!(calls.load(Ordering::SeqCst), 4);
}

#[tokio::test(flavor = "current_thread")]
async fn callback_unwind_reports_all_records_and_propagates_first_panic() {
    let calls = Arc::new(AtomicUsize::new(0));
    let (shared, _listener, _peer) =
        setup(ProducerConfig::default().interceptor(PanicErrors(Arc::clone(&calls)))).await;
    let (batch, receivers, budget, _) = records(&shared, 3, true);
    let mut cleanup = Box::pin(fail_kept_pendings(&shared, batch, Error::Timeout));
    assert!(matches!(poll_once(cleanup.as_mut()), Poll::Pending));
    let panic = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let _ = poll_once(cleanup.as_mut());
    }));
    assert!(panic.is_err());
    drop(panic);
    drop(cleanup);
    assert_timeouts(receivers).await;
    assert_eq!(calls.load(Ordering::SeqCst), 3);
    assert_eq!(budget.used.load(Ordering::SeqCst), 0);
    assert_eq!(shared.buffered_bytes.load(Ordering::SeqCst), 0);
    assert_eq!(shared.m_errors.load(Ordering::SeqCst), 3);
}

#[tokio::test(flavor = "current_thread")]
async fn callback_panic_during_cancellation_does_not_interrupt_cleanup() {
    let calls = Arc::new(AtomicUsize::new(0));
    let (shared, _listener, _peer) =
        setup(ProducerConfig::default().interceptor(PanicErrors(Arc::clone(&calls)))).await;
    let (batch, receivers, budget, _) = records(&shared, 3, false);
    let cleanup = fail_kept_pendings(&shared, batch, Error::Timeout);
    assert!(std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| drop(cleanup))).is_ok());
    assert_timeouts(receivers).await;
    assert_eq!(calls.load(Ordering::SeqCst), 3);
    assert_eq!(budget.used.load(Ordering::SeqCst), 0);
    assert_eq!(shared.buffered_bytes.load(Ordering::SeqCst), 0);
    assert_eq!(shared.m_errors.load(Ordering::SeqCst), 3);
}

#[derive(Debug)]
struct PanicDisplay;
impl fmt::Display for PanicDisplay {
    fn fmt(&self, _: &mut fmt::Formatter<'_>) -> fmt::Result {
        panic!("delivery error display");
    }
}
impl std::error::Error for PanicDisplay {}

#[tokio::test(flavor = "current_thread")]
async fn error_clone_unwind_still_reports_and_releases_all_records() {
    let (shared, _listener, _peer) = setup(ProducerConfig::default()).await;
    let (batch, receivers, budget, _) = records(&shared, 3, false);
    let error = Error::Io(std::io::Error::other(PanicDisplay));
    let mut cleanup = Box::pin(fail_kept_pendings(&shared, batch, error));
    assert!(matches!(poll_once(cleanup.as_mut()), Poll::Pending));
    let panic = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let _ = poll_once(cleanup.as_mut());
    }));
    assert!(panic.is_err());
    drop(panic);
    drop(cleanup);
    for receiver in receivers {
        assert!(matches!(receiver.await, Ok(Err(Error::Protocol(_)))));
    }
    assert_eq!(budget.used.load(Ordering::SeqCst), 0);
    assert_eq!(shared.buffered_bytes.load(Ordering::SeqCst), 0);
    assert_eq!(shared.m_errors.load(Ordering::SeqCst), 3);
}

#[tokio::test(flavor = "current_thread")]
async fn unpolled_worker_batch_wrapper_owns_cleanup() {
    let (shared, listener, _peer) = setup(ProducerConfig::default()).await;
    let addr = listener.local_addr().unwrap().to_string();
    let conn = BrokerConn::connect(&addr, "failure-cleanup-worker", Duration::from_secs(1))
        .await
        .unwrap();
    let (_worker_peer, _) = listener.accept().await.unwrap();
    let (_data_tx, data_rx) = mpsc::channel(1);
    let (_ctrl_tx, ctrl_rx) = mpsc::channel(1);
    let mut worker = Worker {
        id: Arc::new(()),
        admission: Arc::new(WorkerAdmission::new()),
        node_id: 1,
        throttle: shared.broker_throttle(1),
        conn,
        data: data_rx,
        ctrl: ctrl_rx,
        shared: Arc::clone(&shared),
        write_buf: BytesMut::new(),
        pending: Vec::new(),
        spare_batch: Vec::new(),
        pending_estimate: 0,
        earliest_deadline: None,
        in_flight: VecDeque::new(),
        fail: None,
        compress_scratch: CompressScratch::default(),
        group_scratch: ProduceGroupScratch::default(),
        ack_index: AckIndex::default(),
        ack_slots: Vec::new(),
        spare_acks: Vec::new(),
        ack_requested: HashMap::new(),
        ack_seen: HashMap::new(),
        ack_tracker: None,
        _live: WorkerLease::register(&shared),
    };
    let (batch, receivers, budget, _) = records(&shared, 3, false);
    let cleanup = worker.fail_kept_batch(batch, Error::Timeout);
    drop(cleanup);
    assert_timeouts(receivers).await;
    assert_eq!(budget.used.load(Ordering::SeqCst), 0);
    assert_eq!(shared.buffered_bytes.load(Ordering::SeqCst), 0);
    assert_eq!(shared.m_errors.load(Ordering::SeqCst), 3);
}

struct PanicWaker;
impl std::task::Wake for PanicWaker {
    fn wake(self: Arc<Self>) {
        panic!("test wake");
    }
}

#[tokio::test(flavor = "current_thread")]
async fn panicking_waker_does_not_double_release_bytes() {
    let (shared, _listener, _peer) = setup(ProducerConfig::default()).await;
    let notified = shared.buffer_nudge.notified();
    tokio::pin!(notified);
    let waker = std::task::Waker::from(Arc::new(PanicWaker));
    let mut context = Context::from_waker(&waker);
    assert!(matches!(notified.as_mut().poll(&mut context), Poll::Pending));

    let (batch, receivers, budget, _) = records(&shared, 3, false);
    let mut cleanup = Box::pin(fail_kept_pendings(&shared, batch, Error::Timeout));
    assert!(matches!(poll_once(cleanup.as_mut()), Poll::Pending));
    let panic = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let _ = poll_once(cleanup.as_mut());
    }));
    assert!(panic.is_err());
    drop(panic);
    drop(cleanup);
    drop(notified);
    assert_timeouts(receivers).await;
    assert_eq!(budget.used.load(Ordering::SeqCst), 0);
    assert_eq!(shared.buffered_bytes.load(Ordering::SeqCst), 0);
    assert_eq!(shared.m_errors.load(Ordering::SeqCst), 3);
}

#[tokio::test(flavor = "current_thread")]
async fn all_sequenced_sticky_cohorts_retire_before_the_first_yield() {
    let cfg = ProducerConfig::default()
        .partitioner(crate::partitioner::StickyPartitioner::new())
        .batch_records(32)
        .batch_bytes(1_000_000);
    let (shared, _listener, _peer) = setup(cfg).await;
    let topics: [Arc<str>; 2] = [Arc::from("failure-a"), Arc::from("failure-b")];
    {
        let mut cluster = shared.cluster.lock();
        for topic in &topics {
            let _ = cluster.leaders.insert(Arc::clone(topic), vec![1]);
        }
    }
    let worker = Arc::new(());
    let mut tags = Vec::new();
    {
        let cluster = shared.cluster.lock();
        let mut policy = shared.sticky.as_ref().unwrap().lock();
        for topic in &topics {
            let plan = policy.plan(topic, 0, &worker, 1, &cluster);
            let tag = plan.tag;
            assert_ne!(tag.id, 0);
            policy.commit(topic, 0, &worker, true, plan, &cluster);
            let next = policy.plan(topic, 0, &worker, 1, &cluster);
            assert_eq!(next.tag.id, tag.id);
            policy.commit(topic, 0, &worker, true, next, &cluster);
            assert_eq!(policy.members(tag), 2);
            assert!(!policy.cancelled(tag));
            tags.push(tag);
        }
    }
    let (mut batch, receivers, budget, _) = records(&shared, 8 * 1024 + 1, false);
    batch[0].rec.topic = Arc::clone(&topics[0]);
    batch[0].sticky = Some(tags[0]);
    batch[0].seq = Some(0);
    batch[8 * 1024].rec.topic = Arc::clone(&topics[1]);
    batch[8 * 1024].sticky = Some(tags[1]);
    batch[8 * 1024].seq = Some(0);
    let mut cleanup = Box::pin(fail_kept_pendings(&shared, batch, Error::Timeout));
    assert!(matches!(poll_once(cleanup.as_mut()), Poll::Pending));
    {
        let policy = shared.sticky.as_ref().unwrap().lock();
        for tag in &tags {
            assert!(policy.cancelled(*tag));
            assert_eq!(policy.members(*tag), 1);
        }
    }
    drop(cleanup);
    assert_timeouts(receivers).await;
    assert_eq!(budget.used.load(Ordering::SeqCst), 0);
    assert_eq!(shared.buffered_bytes.load(Ordering::SeqCst), 0);
    let cluster = shared.cluster.lock();
    let mut policy = shared.sticky.as_ref().unwrap().lock();
    for tag in tags {
        policy.release(tag, 1, &cluster);
    }
}

#[tokio::test(flavor = "current_thread")]
async fn panicking_result_waker_keeps_all_outcomes_on_cancel_and_unwind() {
    for cancel in [true, false] {
        let (shared, _listener, _peer) = setup(ProducerConfig::default()).await;
        let (batch, mut receivers, budget, _) = records(&shared, 3, true);
        let waker = std::task::Waker::from(Arc::new(PanicWaker));
        let mut context = Context::from_waker(&waker);
        assert!(matches!(Pin::new(&mut receivers[0]).poll(&mut context), Poll::Pending));
        let mut cleanup = Box::pin(fail_kept_pendings(&shared, batch, Error::Timeout));
        if cancel {
            assert!(std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                drop(cleanup);
            }))
            .is_ok());
        } else {
            assert!(matches!(poll_once(cleanup.as_mut()), Poll::Pending));
            let panic = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                let _ = poll_once(cleanup.as_mut());
            }));
            assert!(panic.is_err());
            drop(panic);
            drop(cleanup);
        }
        assert_timeouts(receivers).await;
        assert_eq!(budget.used.load(Ordering::SeqCst), 0);
        assert_eq!(shared.buffered_bytes.load(Ordering::SeqCst), 0);
        assert_eq!(shared.m_errors.load(Ordering::SeqCst), 3);
    }
}

#[tokio::test(flavor = "current_thread")]
async fn closed_receiver_does_not_skip_other_outcomes_or_accounting() {
    let calls = Arc::new(AtomicUsize::new(0));
    let (shared, _listener, _peer) =
        setup(ProducerConfig::default().interceptor(CountErrors(Arc::clone(&calls)))).await;
    let (batch, mut receivers, budget, _) = records(&shared, 3, true);
    drop(receivers.remove(1));
    drop(fail_kept_pendings(&shared, batch, Error::Timeout));
    assert_timeouts(receivers).await;
    assert_eq!(calls.load(Ordering::SeqCst), 3);
    assert_eq!(budget.used.load(Ordering::SeqCst), 0);
    assert_eq!(shared.buffered_bytes.load(Ordering::SeqCst), 0);
    assert_eq!(shared.m_errors.load(Ordering::SeqCst), 3);
}

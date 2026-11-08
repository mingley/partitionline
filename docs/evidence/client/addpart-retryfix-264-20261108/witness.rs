use super::*;
use crate::protocol::txn::{
    encode_add_partitions_to_txn_topics_response, AddPartitionsToTxnPartitionResult,
    AddPartitionsToTxnTopicResult,
};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

fn topic(name: &str, parts: &[(i32, i16)]) -> AddPartitionsToTxnTopicResult {
    AddPartitionsToTxnTopicResult {
        topic: name.to_owned(),
        partitions: parts
            .iter()
            .map(|(partition, error)| AddPartitionsToTxnPartitionResult::error(*partition, *error))
            .collect(),
    }
}

fn encoded(version: i16, topics: &[AddPartitionsToTxnTopicResult]) -> BytesMut {
    let mut body = BytesMut::new();
    encode_add_partitions_to_txn_topics_response(&mut body, version, topics).unwrap();
    body
}

async fn shared_fixture(tid: &str, backoff: Duration) -> (Arc<Shared>, tokio::net::TcpListener, tokio::net::TcpStream) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap().to_string();
    let meta = BrokerConn::connect(&addr, "add-witness-meta", Duration::from_secs(2))
        .await
        .unwrap();
    let (meta_peer, _) = listener.accept().await.unwrap();
    let cfg = ProducerConfig::default()
        .transactional_id(tid)
        .request_timeout(Duration::from_secs(2))
        .retry_backoff(backoff)
        .retry_backoff_max(backoff);
    let shared = test_shared(cfg, meta);
    (shared, listener, meta_peer)
}

async fn test_worker(tid: &str, backoff: Duration) -> (Worker, tokio::net::TcpStream, tokio::net::TcpStream, tokio::net::TcpStream) {
    let (shared, listener, meta_peer) = shared_fixture(tid, backoff).await;
    let addr = listener.local_addr().unwrap().to_string();
    let mut conn = BrokerConn::connect(&addr, "add-coverage-worker", Duration::from_secs(2))
        .await
        .unwrap();
    let (worker_peer, _) = listener.accept().await.unwrap();
    conn.set_produce_version(3);
    let coordinator =
        BrokerConn::connect(&addr, "add-coverage-coordinator", Duration::from_secs(2))
            .await
            .unwrap();
    let (coord_peer, _) = listener.accept().await.unwrap();
    let versions = ApiVersionsResponse {
        api_keys: vec![ApiVersion {
            api_key: ADD_PARTITIONS_TO_TXN,
            min_version: 0,
            max_version: 0,
        }],
        ..Default::default()
    };
    *shared.txn.lock().await = Some(coordinator_connection(coordinator, &versions));
    shared.in_txn.store(true, Ordering::SeqCst);
    shared.producer_id.store(9, Ordering::SeqCst);
    shared.producer_epoch.store(2, Ordering::SeqCst);
    let (_data, data) = mpsc::channel(1);
    let (_ctrl, ctrl) = mpsc::channel(1);
    let admission = Arc::new(WorkerAdmission::for_producer(Arc::clone(&shared.closed)));
    let worker = Worker {
        id: Arc::new(()),
        admission,
        node_id: 1,
        throttle: shared.broker_throttle(1),
        conn,
        data,
        ctrl,
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
    (worker, coord_peer, meta_peer, worker_peer)
}

async fn respond(peer: &mut tokio::net::TcpStream, results: &[(i32, i16)], tid: &str) {
    let mut size = [0u8; 4];
    let _read = peer.read_exact(&mut size).await.unwrap();
    let length = usize::try_from(i32::from_be_bytes(size)).unwrap();
    assert!((8..=4096).contains(&length));
    let mut request = vec![0u8; length];
    let _read = peer.read_exact(&mut request).await.unwrap();
    let mut input = request.as_slice();
    let header = crate::protocol::header::decode_request_header(&mut input).unwrap();
    assert_eq!(
        (header.api_key, header.api_version),
        (ADD_PARTITIONS_TO_TXN, 0)
    );
    let (id, pid, epoch, request_topics) =
        crate::protocol::txn::decode_add_partitions_to_txn_request(&mut input, 0).unwrap();
    assert_eq!((id.as_str(), pid, epoch), (tid, 9, 2));
    assert!(input.is_empty());
    assert_eq!(request_topics.len(), 1);
    let requested = request_topics.first().unwrap();
    assert_eq!(requested.topic, "a");
    assert_eq!(requested.partitions.len(), 2);
    assert!(requested.partitions.contains(&0) && requested.partitions.contains(&1));
    let body = encoded(0, &[topic("a", results)]);
    let size = i32::try_from(body.len() + 4).unwrap();
    peer.write_all(&size.to_be_bytes()).await.unwrap();
    peer.write_all(&header.correlation_id.to_be_bytes())
        .await
        .unwrap();
    peer.write_all(&body).await.unwrap();
}

static CROSSED: AtomicUsize = AtomicUsize::new(0);
static AFTER_DISPOSITION: AtomicBool = AtomicBool::new(false);

pub(super) async fn checkpoint(shared: &Shared, deadline: Instant) {
    let tid = shared.cfg.transactional_id.as_deref();
    if matches!(tid, Some("deadline-cross" | "deadline-fatal" | "deadline-cancel")) {
        let _old = CROSSED.fetch_add(1, Ordering::SeqCst);
        if tid == Some("deadline-fatal") {
            shared.set_txn_fatal(Error::broker(error::PRODUCER_FENCED, "earlier fence"));
        }
        let after = tokio::time::Instant::from_std(deadline) + Duration::from_millis(25);
        tokio::time::sleep_until(after).await;
    }
}

pub(super) async fn after_disposition(shared: &Shared) {
    if shared.cfg.transactional_id.as_deref() == Some("deadline-cancel") {
        AFTER_DISPOSITION.store(true, Ordering::SeqCst);
        std::future::pending::<()>().await;
    }
}

fn queue(worker: &mut Worker, partition: i32) -> oneshot::Receiver<Result<RecordMetadata>> {
    let rec = ProduceRecord::to("a").partition(partition).value(&b"x"[..]);
    assert!(worker.shared.try_reserve_buffer(rec_bytes(&rec)));
    let (tx, rx) = oneshot::channel();
    let now = Instant::now();
    worker.push_pending(Pending {
        sticky_record: None,
        sticky: None,
        rec,
        tx: Some(tx),
        seq: None,
        batch_base_seq: None,
        batch_len: 0,
        epoch_gen: 0,
        deadline: now + Duration::from_secs(30),
        queued_at: now,
        retry: 0,
        skip_meta_refresh: false,
        retry_after: now,
    });
    rx
}

async fn check_case(cross: bool, fatal: bool) {
    tokio::time::timeout(Duration::from_secs(10), async {
        let tid = if fatal { "deadline-fatal" } else if cross { "deadline-cross" } else { "deadline-natural" };
        let backoff = if cross { Duration::from_millis(5) } else { Duration::from_secs(5) };
        let (mut worker, mut peer, _meta_peer, _produce_peer) = test_worker(tid, backoff).await;
        let rx0 = queue(&mut worker, 0);
        let rx1 = queue(&mut worker, 1);
        assert_eq!(worker.shared.buffered_bytes.load(Ordering::Relaxed), 2);
        let server = async {
            respond(
                &mut peer,
                &[
                    (0, error::CONCURRENT_TRANSACTIONS),
                    (1, error::OPERATION_NOT_ATTEMPTED),
                ],
                tid,
            )
            .await;
        };
        let (result, ()) = tokio::join!(worker.fire(), server);
        if cross {
            assert!(matches!(result, Err(Error::Timeout)));
            assert!(matches!(rx0.await.unwrap(), Err(Error::Timeout)));
            assert!(matches!(rx1.await.unwrap(), Err(Error::Timeout)));
            assert_eq!(CROSSED.load(Ordering::SeqCst), 1);
        } else {
            assert_eq!(result.unwrap_err().broker_code(), Some(error::CONCURRENT_TRANSACTIONS));
            assert_eq!(rx0.await.unwrap().unwrap_err().broker_code(), Some(error::CONCURRENT_TRANSACTIONS));
            assert_eq!(rx1.await.unwrap().unwrap_err().broker_code(), Some(error::CONCURRENT_TRANSACTIONS));
        }
        assert_eq!(worker.shared.buffered_bytes.load(Ordering::Relaxed), 0);
        assert!(worker.pending.is_empty());
        assert!(worker.in_flight.is_empty());
        assert!(worker.shared.txn_partitions.lock().is_empty());
        assert!(worker.shared.txn_added.lock().is_empty());
        {
            let seqs = worker.shared.seqs.lock();
            let parts = seqs.get("a").unwrap();
            assert_eq!(parts.get(&0), Some(&0));
            assert_eq!(parts.get(&1), Some(&0));
        }
        let state = worker.shared.fail_on_txn_error();
        println!(
            "PL_ADD_RETRY_DEADLINE cross={cross} crossed={} buffer=0 pending=0 sequences=rewound state={state:?}",
            CROSSED.load(Ordering::SeqCst)
        );
        assert!(state.is_err(), "PL_ADD_RETRY_REQUIRES_ABORT");
        if fatal {
            assert_eq!(worker.shared.fail_on_txn_fatal().unwrap_err().broker_code(), Some(error::PRODUCER_FENCED));
        }
    })
    .await
    .expect("deadline control timed out");
}

#[tokio::test(flavor = "current_thread")]
async fn natural_exhaustion_requires_abort() {
    check_case(false, false).await;
}

#[tokio::test(flavor = "current_thread")]
async fn post_check_deadline_exhaustion_requires_abort() {
    check_case(true, false).await;
}

#[tokio::test(flavor = "current_thread")]
async fn prior_fatal_is_preserved_at_retry_timeout() {
    check_case(true, true).await;
}

#[tokio::test(flavor = "current_thread")]
async fn cancel_after_disposition_releases_worker_owned_records() {
    tokio::time::timeout(Duration::from_secs(10), async {
        let tid = "deadline-cancel";
        let (mut worker, mut peer, _meta_peer, _produce_peer) =
            test_worker(tid, Duration::from_millis(5)).await;
        let rx0 = queue(&mut worker, 0);
        let rx1 = queue(&mut worker, 1);
        let shared = Arc::clone(&worker.shared);
        let task = tokio::spawn(async move { worker.fire().await });
        respond(
            &mut peer,
            &[
                (0, error::CONCURRENT_TRANSACTIONS),
                (1, error::OPERATION_NOT_ATTEMPTED),
            ],
            tid,
        )
        .await;
        while !AFTER_DISPOSITION.load(Ordering::SeqCst) {
            tokio::task::yield_now().await;
        }
        assert!(shared.fail_on_txn_error().is_err());
        assert_eq!(shared.buffered_bytes.load(Ordering::Relaxed), 2);
        task.abort();
        assert!(task.await.unwrap_err().is_cancelled());
        let result0 = rx0.await;
        let result1 = rx1.await;
        let remaining = shared.buffered_bytes.load(Ordering::Relaxed);
        let retained = shared.txn_partitions.lock().values().map(|p| p.len()).sum::<usize>();
        let state_after = shared.fail_on_txn_error();
        println!(
            "PL_ADD_CANCEL_OWNERSHIP buffered={remaining} retained={retained} first={result0:?} second={result1:?} state={state_after:?}"
        );
        assert!(state_after.is_err());
        assert_eq!(remaining, 0, "PL_ADD_CANCEL_OWNERSHIP");
        assert!(matches!(result0, Ok(Err(Error::Timeout))));
        assert!(matches!(result1, Ok(Err(Error::Timeout))));
        assert!(shared.fail_on_txn_error().is_err());
    })
    .await
    .expect("cancel control timed out");
}

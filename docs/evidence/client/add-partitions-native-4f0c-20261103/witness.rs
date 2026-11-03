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

async fn shared_fixture() -> (Arc<Shared>, tokio::net::TcpListener, tokio::net::TcpStream) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap().to_string();
    let meta = BrokerConn::connect(&addr, "add-witness-meta", Duration::from_secs(2))
        .await
        .unwrap();
    let (meta_peer, _) = listener.accept().await.unwrap();
    let shared = test_shared(ProducerConfig::default().transactional_id("test-txn"), meta);
    (shared, listener, meta_peer)
}

async fn test_worker() -> (Worker, tokio::net::TcpStream, tokio::net::TcpStream) {
    let (shared, listener, meta_peer) = shared_fixture().await;
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
    drop(worker_peer);
    (worker, coord_peer, meta_peer)
}

async fn respond(peer: &mut tokio::net::TcpStream, results: &[(i32, i16)]) {
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
    assert_eq!((id.as_str(), pid, epoch), ("test-txn", 9, 2));
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

#[tokio::test(flavor = "current_thread")]
async fn missing_response_must_not_mark_partitions_added() {
    tokio::time::timeout(Duration::from_secs(5), async {
        let (worker, mut peer, _meta_peer) = test_worker().await;
        let groups = vec![
            (Arc::from("a"), 0, Vec::new()),
            (Arc::from("a"), 1, Vec::new()),
        ];
        let server = async { respond(&mut peer, &[(0, 0)]).await };
        let (result, ()) = tokio::join!(worker.add_txn_partitions(&groups), server);
        assert!(
            matches!(&result, Err(Error::Protocol(_))),
            "WITNESS_MISSING_EXPECTS_PROTOCOL: {result:?}"
        );
        assert!(worker.shared.txn_fatal.load(Ordering::SeqCst));
        assert!(worker.shared.txn_added.lock().is_empty());
    })
    .await
    .expect("missing response witness timed out");
}

#[tokio::test(flavor = "current_thread")]
async fn mixed_load_and_fence_must_record_fence() {
    tokio::time::timeout(Duration::from_secs(5), async {
        let (worker, mut peer, _meta_peer) = test_worker().await;
        let groups = vec![
            (Arc::from("a"), 0, Vec::new()),
            (Arc::from("a"), 1, Vec::new()),
        ];
        let server = async {
            respond(
                &mut peer,
                &[
                    (0, error::COORDINATOR_LOAD_IN_PROGRESS),
                    (1, error::PRODUCER_FENCED),
                ],
            )
            .await
        };
        let (result, ()) = tokio::join!(worker.add_txn_partitions(&groups), server);
        let code = result.as_ref().err().and_then(Error::broker_code);
        assert_eq!(
            code,
            Some(error::PRODUCER_FENCED),
            "WITNESS_FENCE_EXPECTS_BROKER: {result:?}"
        );
        assert!(worker.shared.txn_fatal.load(Ordering::SeqCst));
        assert!(worker.shared.txn_added.lock().is_empty());
    })
    .await
    .expect("mixed fence witness timed out");
}

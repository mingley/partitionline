//! Consumer fetch semantics and regression test suite.
//!
//! Uses the promoted loopback fixture from `tests/common/fetch_fixture.rs`.
//! In KL03-01, this suite exercises:
//! 1. The passing control scenario (aligned batch where first record offset equals requested offset).
//! 2. Fixture-level tests proving whole batches, control markers, and partial-retry error injection
//!    are constructed without pre-filtering away the behaviors under test.
//!
//! Note: Diagnostic defect cases A01-A05 remain in docs/audits/2026-09-21-consumer-probes.rs
//! and will be promoted to this suite alongside their respective repairs in KL03-02 through KL03-06.
#![expect(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::let_underscore_must_use,
    clippy::cast_possible_truncation,
    clippy::cast_possible_wrap,
    clippy::cast_sign_loss,
    clippy::type_complexity,
    clippy::unnecessary_map_or,
    clippy::too_many_arguments,
    clippy::allow_attributes_without_reason,
    reason = "integration tests use test assertions and unwrap on mock sockets"
)]

#[path = "common/fetch_fixture.rs"]
mod fetch_fixture;

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use bytes::BytesMut;
use partitionline::error::{NOT_LEADER_OR_FOLLOWER, OFFSET_OUT_OF_RANGE};
use partitionline::protocol::api::{
    encode_api_versions_response, encode_metadata_response, ApiVersion, ApiVersionsResponse,
    Broker, MetadataResponse, PartitionMetadata, TopicMetadata,
};
use partitionline::protocol::api_keys::{API_VERSIONS, FETCH, METADATA};
use partitionline::protocol::fetch::{
    decode_fetch_request, encode_fetch_response, FetchPartition, FetchTopic, FetchedPartition,
    FetchedTopic,
};
use partitionline::protocol::header::{
    decode_request_header, decode_response_header, encode_request_header, encode_response_header,
    RequestHeader,
};
use partitionline::protocol::records::{
    ControlRecordType, EndTransactionMarker, Record, RecordBatch,
};
use partitionline::{Consumer, ConsumerConfig, IsolationLevel, TopicPartition};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::oneshot;
use tokio::task::JoinSet;

/// Control scenario: a batch whose first record offset equals the requested
/// assignment offset (0). The current client already handles this correctly,
/// so the missing lower-bound filter (A01) is not what makes it pass.
///
/// Asserts the delivered offsets and that tasks shut down cleanly.
#[tokio::test]
async fn control_scenario_aligned_batch_delivers_and_shuts_down() {
    let mut broker =
        fetch_fixture::FixtureBroker::start(fetch_fixture::Scenario::AlignedBatch).await;
    let mut consumer = Consumer::new(broker.config())
        .await
        .expect("consumer starts successfully");

    consumer
        .assign("t", 0, 0)
        .await
        .expect("assign partition 0 at offset 0");

    let records = consumer.fetch().await.expect("fetch succeeds");
    assert_eq!(
        records.iter().map(|r| r.offset).collect::<Vec<_>>(),
        vec![0, 1],
        "delivered records must match batch offsets"
    );
    assert_eq!(records[0].value.as_deref(), Some(&b"control-0"[..]));
    assert_eq!(records[1].value.as_deref(), Some(&b"control-1"[..]));

    consumer.close().await.expect("consumer closes cleanly");

    broker.shutdown().await;
    assert!(
        broker.is_finished(),
        "listener task must be finished after shutdown"
    );
    assert_eq!(
        broker.active_connections(),
        0,
        "all connection tasks must be terminated"
    );
}

/// Fixture test: prove the fixture builds a whole batch containing records BEFORE
/// the requested offset (fetch_offset: 2, batch contains offsets 0, 1, 2) and that
/// it does NOT pre-filter those records out.
#[test]
fn fixture_builds_whole_batch_with_records_before_requested_offset_without_prefiltering() {
    let requested_offset = 2;
    let request_topics = vec![FetchTopic {
        topic: "t".to_string(),
        topic_id: [0u8; 16],
        partitions: vec![FetchPartition::partition_data(
            0,
            requested_offset,
            0,
            1024 * 1024,
            Some(0),
            None,
        )],
    }];

    let response = fetch_fixture::build_fetch_response(
        fetch_fixture::Scenario::WholeBatch,
        &request_topics,
        0,
    );

    assert_eq!(response.len(), 1);
    assert_eq!(response[0].topic, "t");
    assert_eq!(response[0].partitions.len(), 1);
    let part = &response[0].partitions[0];
    assert_eq!(part.partition, 0);
    assert_eq!(part.error_code, 0);
    assert_eq!(part.records.len(), 1, "fixture must return the batch");

    let batch = &part.records[0];
    assert_eq!(batch.base_offset, 0, "batch starts at offset 0");
    assert_eq!(
        batch.records.len(),
        3,
        "batch has 3 records: offsets 0, 1, 2"
    );

    let record_offsets: Vec<i64> = batch
        .records
        .iter()
        .map(|r| batch.base_offset + r.offset)
        .collect();
    assert_eq!(record_offsets, vec![0, 1, 2]);

    // Check that records with offsets < requested_offset are present
    let records_before_requested: Vec<i64> = record_offsets
        .iter()
        .copied()
        .filter(|&off| off < requested_offset)
        .collect();
    assert_eq!(
        records_before_requested,
        vec![0, 1],
        "fixture must retain records before requested offset 2 without pre-filtering"
    );

    // Verify wire encoding succeeds
    let mut encoded = BytesMut::new();
    encode_fetch_response(&mut encoded, 12, &response).expect("wire encoding succeeds");
    assert!(!encoded.is_empty(), "wire payload must be non-empty");
}

/// Fixture test: prove the fixture builds an ABORT/COMMIT control-marker sequence
/// with aborted transaction metadata for PID 7.
#[test]
fn fixture_builds_abort_then_commit_control_marker_sequence() {
    let request_topics = vec![FetchTopic {
        topic: "t".to_string(),
        topic_id: [0u8; 16],
        partitions: vec![FetchPartition::partition_data(
            0,
            0,
            0,
            1024 * 1024,
            Some(0),
            None,
        )],
    }];

    let response = fetch_fixture::build_fetch_response(
        fetch_fixture::Scenario::AbortThenCommit,
        &request_topics,
        0,
    );

    assert_eq!(response.len(), 1);
    let part = &response[0].partitions[0];
    assert_eq!(part.partition, 0);
    assert_eq!(part.error_code, 0);

    // Aborted transactions list
    assert_eq!(
        part.aborted_transactions,
        vec![(7, 0)],
        "must declare aborted transaction for PID 7 starting at offset 0"
    );

    // Sequence of 4 batches: aborted data, ABORT marker, committed data, COMMIT marker
    assert_eq!(part.records.len(), 4, "must contain 4 batches in sequence");

    // 0: Aborted data batch
    assert_eq!(part.records[0].base_offset, 0);
    assert!(part.records[0].is_transactional());
    assert!(!part.records[0].is_control_batch());
    assert_eq!(part.records[0].producer_id, 7);

    // 1: ABORT marker
    assert_eq!(part.records[1].base_offset, 1);
    assert!(part.records[1].is_control_batch());
    assert_eq!(part.records[1].producer_id, 7);

    // 2: Committed data batch
    assert_eq!(part.records[2].base_offset, 2);
    assert!(part.records[2].is_transactional());
    assert!(!part.records[2].is_control_batch());
    assert_eq!(part.records[2].producer_id, 7);

    // 3: COMMIT marker
    assert_eq!(part.records[3].base_offset, 3);
    assert!(part.records[3].is_control_batch());
    assert_eq!(part.records[3].producer_id, 7);

    // Verify wire encoding succeeds
    let mut encoded = BytesMut::new();
    encode_fetch_response(&mut encoded, 12, &response).expect("wire encoding succeeds");
    assert!(!encoded.is_empty(), "wire payload must be non-empty");
}

/// Fixture test: prove the fixture builds a one-partition retriable error beside
/// a successful partition without dropping either partition.
#[test]
fn fixture_builds_one_partition_retry_beside_successful_partition() {
    let request_topics = vec![FetchTopic {
        topic: "t".to_string(),
        topic_id: [0u8; 16],
        partitions: vec![
            FetchPartition::partition_data(0, 0, 0, 1024 * 1024, Some(0), None),
            FetchPartition::partition_data(1, 0, 0, 1024 * 1024, Some(0), None),
        ],
    }];

    // Attempt 0: partition 0 succeeds with data; partition 1 returns retriable error
    let response_attempt0 = fetch_fixture::build_fetch_response(
        fetch_fixture::Scenario::PartialRetry,
        &request_topics,
        0,
    );

    assert_eq!(response_attempt0.len(), 1);
    let parts0 = &response_attempt0[0].partitions;
    assert_eq!(parts0.len(), 2, "both partitions must be returned");

    let p0 = parts0
        .iter()
        .find(|p| p.partition == 0)
        .expect("partition 0 present");
    assert_eq!(p0.error_code, 0);
    assert_eq!(p0.records.len(), 1, "partition 0 has records on attempt 0");

    let p1 = parts0
        .iter()
        .find(|p| p.partition == 1)
        .expect("partition 1 present");
    assert_eq!(p1.error_code, NOT_LEADER_OR_FOLLOWER);
    assert!(p1.records.is_empty(), "partition 1 has no records on error");

    // Attempt 1: partition 1 retried and succeeds with data
    let response_attempt1 = fetch_fixture::build_fetch_response(
        fetch_fixture::Scenario::PartialRetry,
        &request_topics,
        1,
    );
    let parts1 = &response_attempt1[0].partitions;
    let p1_retry = parts1
        .iter()
        .find(|p| p.partition == 1)
        .expect("partition 1 present");
    assert_eq!(p1_retry.error_code, 0, "error cleared on retry");
    assert_eq!(
        p1_retry.records.len(),
        1,
        "partition 1 has records on retry"
    );

    // Verify wire encoding succeeds
    let mut encoded = BytesMut::new();
    encode_fetch_response(&mut encoded, 12, &response_attempt0).expect("wire encoding succeeds");
    assert!(!encoded.is_empty(), "wire payload must be non-empty");
}

/// Fixture test: prove the fixture builds an OUT_OF_RANGE response with log boundaries.
#[test]
fn fixture_builds_out_of_range_partition_response() {
    let request_topics = vec![FetchTopic {
        topic: "t".to_string(),
        topic_id: [0u8; 16],
        partitions: vec![FetchPartition::partition_data(
            0,
            0,
            0,
            1024 * 1024,
            Some(0),
            None,
        )],
    }];

    let response = fetch_fixture::build_fetch_response(
        fetch_fixture::Scenario::OutOfRange,
        &request_topics,
        0,
    );

    let part = &response[0].partitions[0];
    assert_eq!(part.error_code, OFFSET_OUT_OF_RANGE);
    assert_eq!(part.log_start_offset, 10);
    assert_eq!(part.high_watermark, 20);
    assert_eq!(part.last_stable_offset, 20);

    let mut encoded = BytesMut::new();
    encode_fetch_response(&mut encoded, 12, &response).expect("wire encoding succeeds");
    assert!(!encoded.is_empty());
}

/// Loopback test: verify ephemeral port binding, wire frame handshake, and task shutdown.
#[tokio::test]
async fn fixture_broker_wire_handshake_and_bounded_shutdown() {
    let mut broker =
        fetch_fixture::FixtureBroker::start(fetch_fixture::Scenario::AlignedBatch).await;
    assert!(!broker.is_finished(), "broker should be running");
    assert_eq!(broker.active_connections(), 0);

    let mut stream = TcpStream::connect(broker.addr())
        .await
        .expect("connect to ephemeral port");

    let header = RequestHeader {
        api_key: API_VERSIONS,
        api_version: 0,
        correlation_id: 42,
        client_id: Some("handshake-client".to_string()),
    };
    let mut req_bytes = BytesMut::new();
    encode_request_header(&mut req_bytes, &header).expect("encode header");
    stream
        .write_i32(i32::try_from(req_bytes.len()).unwrap())
        .await
        .unwrap();
    stream.write_all(&req_bytes).await.unwrap();

    let size = stream.read_i32().await.expect("read response size");
    assert!(size > 0 && size < 1024 * 1024);
    let mut resp_bytes = vec![0u8; size as usize];
    let _ = stream
        .read_exact(&mut resp_bytes)
        .await
        .expect("read response");
    let mut resp_slice = resp_bytes.as_slice();
    let resp_header =
        decode_response_header(&mut resp_slice, API_VERSIONS, 0).expect("decode response header");
    assert_eq!(resp_header.correlation_id, 42);

    drop(stream);

    broker.shutdown().await;
    assert!(broker.is_finished(), "listener task must finish");
    assert_eq!(
        broker.active_connections(),
        0,
        "all connection tasks must be terminated"
    );
}

/// Drop test: verify that dropping the broker aborts listener and connection tasks.
#[tokio::test]
async fn fixture_broker_drop_aborts_tasks() {
    let broker = fetch_fixture::FixtureBroker::start(fetch_fixture::Scenario::AlignedBatch).await;
    let addr = broker.addr().to_string();
    let mut stream = TcpStream::connect(&addr).await.expect("connect to broker");

    // Drop the broker, aborting listener and connection tasks
    drop(broker);

    // Reading from stream should result in EOF (0 bytes) or ConnectionReset since broker tasks aborted
    let mut buf = [0u8; 1];
    let res = stream.read(&mut buf).await;
    match res {
        Ok(0) => {} // EOF
        Err(e)
            if matches!(
                e.kind(),
                std::io::ErrorKind::ConnectionReset | std::io::ErrorKind::UnexpectedEof
            ) => {}
        other => panic!("expected EOF or reset after broker drop, got: {:?}", other),
    }
}

/// Audit defect A03 reproduction / regression test:
/// When partition 0 succeeds with data while partition 1 returns a retriable error
/// (NOT_LEADER_OR_FOLLOWER), the consumer retries partition 1. The records from partition 0
/// must be preserved across the retry round, and both partition records must be returned
/// exactly once without duplicates.
#[tokio::test]
async fn mixed_partition_retry_must_preserve_successful_records() {
    let mut broker =
        fetch_fixture::FixtureBroker::start(fetch_fixture::Scenario::PartialRetry).await;
    let mut consumer = Consumer::new(broker.config())
        .await
        .expect("consumer starts successfully");

    consumer
        .assign_many([(("t", 0), 0), (("t", 1), 0)])
        .await
        .expect("assign partitions 0 and 1 at offset 0");

    let records = consumer.fetch().await.expect("fetch succeeds");
    assert_eq!(
        records
            .iter()
            .map(|record| (record.partition, record.offset))
            .collect::<Vec<_>>(),
        vec![(0, 0), (1, 0)],
        "both partition records must be returned exactly once"
    );

    assert_eq!(
        consumer.positions(),
        vec![
            (TopicPartition::new("t", 0), 1),
            (TopicPartition::new("t", 1), 1),
        ],
        "positions must advance to the next fetch offset after delivery"
    );

    consumer.close().await.expect("consumer closes cleanly");
    broker.shutdown().await;
}

struct TwoNodeCluster {
    addr0: String,
    shutdown_tx0: Option<oneshot::Sender<()>>,
    shutdown_tx1: Option<oneshot::Sender<()>>,
    task0: Option<tokio::task::JoinHandle<()>>,
    task1: Option<tokio::task::JoinHandle<()>>,
}

impl TwoNodeCluster {
    fn config(&self) -> ConsumerConfig {
        ConsumerConfig::bootstrap([self.addr0.clone()])
            .request_timeout(Duration::from_secs(2))
            .retry_backoff(Duration::from_millis(1))
    }

    async fn shutdown(&mut self) {
        if let Some(tx) = self.shutdown_tx0.take() {
            let _ = tx.send(());
        }
        if let Some(tx) = self.shutdown_tx1.take() {
            let _ = tx.send(());
        }
        if let Some(task) = self.task0.take() {
            let _ = task.await;
        }
        if let Some(task) = self.task1.take() {
            let _ = task.await;
        }
    }
}

async fn spawn_two_node_cluster<F0, F1>(
    leader0: i32,
    leader1: i32,
    replicas1: Vec<i32>,
    rack0: Option<String>,
    rack1: Option<String>,
    handler0: F0,
    handler1: F1,
) -> TwoNodeCluster
where
    F0: Fn(&[FetchTopic], usize) -> Option<Vec<FetchedTopic>> + Send + Sync + 'static,
    F1: Fn(&[FetchTopic], usize) -> Option<Vec<FetchedTopic>> + Send + Sync + 'static,
{
    let listener0 = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind listener 0");
    let listener1 = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind listener 1");
    let port0 = listener0.local_addr().unwrap().port();
    let port1 = listener1.local_addr().unwrap().port();
    let addr0 = format!("127.0.0.1:{port0}");

    let (tx0, rx0) = oneshot::channel();
    let (tx1, rx1) = oneshot::channel();

    let task0 = spawn_mock_node(
        listener0,
        rx0,
        port0,
        port1,
        leader0,
        leader1,
        replicas1.clone(),
        rack0.clone(),
        rack1.clone(),
        Arc::new(handler0),
    );

    let task1 = spawn_mock_node(
        listener1,
        rx1,
        port0,
        port1,
        leader0,
        leader1,
        replicas1,
        rack0,
        rack1,
        Arc::new(handler1),
    );

    TwoNodeCluster {
        addr0,
        shutdown_tx0: Some(tx0),
        shutdown_tx1: Some(tx1),
        task0: Some(task0),
        task1: Some(task1),
    }
}

fn spawn_mock_node<F>(
    listener: TcpListener,
    mut shutdown_rx: oneshot::Receiver<()>,
    port0: u16,
    port1: u16,
    leader0: i32,
    leader1: i32,
    replicas1: Vec<i32>,
    rack0: Option<String>,
    rack1: Option<String>,
    handler: Arc<F>,
) -> tokio::task::JoinHandle<()>
where
    F: Fn(&[FetchTopic], usize) -> Option<Vec<FetchedTopic>> + Send + Sync + 'static,
{
    let attempts = Arc::new(AtomicUsize::new(0));
    tokio::spawn(async move {
        let mut conns = JoinSet::new();
        loop {
            tokio::select! {
                _ = &mut shutdown_rx => {
                    conns.shutdown().await;
                    break;
                }
                accepted = listener.accept() => {
                    let (mut socket, _) = match accepted {
                        Ok(s) => s,
                        Err(_) => break,
                    };
                    let attempts = Arc::clone(&attempts);
                    let handler = Arc::clone(&handler);
                    let rack0 = rack0.clone();
                    let rack1 = rack1.clone();
                    let replicas1 = replicas1.clone();
                    let _ = conns.spawn(async move {
                        let io_deadline = Duration::from_secs(5);
                        loop {
                            let size = match tokio::time::timeout(io_deadline, socket.read_i32()).await {
                                Ok(Ok(size)) => match usize::try_from(size) {
                                    Ok(s) => s,
                                    Err(_) => return,
                                },
                                _ => return,
                            };
                            if size > 16 * 1024 * 1024 {
                                return;
                            }
                            let mut frame = vec![0u8; size];
                            if tokio::time::timeout(io_deadline, socket.read_exact(&mut frame)).await.is_err() {
                                return;
                            }
                            let mut req_slice = frame.as_slice();
                            let header = match decode_request_header(&mut req_slice) {
                                Ok(h) => h,
                                Err(_) => return,
                            };
                            let mut response = BytesMut::new();
                            if encode_response_header(&mut response, header.api_key, header.api_version, header.correlation_id).is_err() {
                                return;
                            }
                            match header.api_key {
                                API_VERSIONS => {
                                    let api_keys = [(API_VERSIONS, 0, 4), (METADATA, 1, 9), (FETCH, 4, 12)]
                                        .into_iter()
                                        .map(|(api_key, min_version, max_version)| ApiVersion { api_key, min_version, max_version })
                                        .collect();
                                    if encode_api_versions_response(&mut response, header.api_version, &ApiVersionsResponse { api_keys, ..Default::default() }).is_err() {
                                        return;
                                    }
                                }
                                METADATA => {
                                    let partitions = vec![
                                        PartitionMetadata::new(0, 0, Some(leader0), Some(0), vec![0], vec![0], Vec::new()),
                                        PartitionMetadata::new(0, 1, Some(leader1), Some(0), replicas1.clone(), vec![leader1], Vec::new()),
                                    ];
                                    let metadata = MetadataResponse {
                                        throttle_time_ms: 0,
                                        brokers: vec![
                                            Broker::new(0, "127.0.0.1", i32::from(port0), rack0.clone()),
                                            Broker::new(1, "127.0.0.1", i32::from(port1), rack1.clone()),
                                        ],
                                        cluster_id: Some("two-node-cluster".into()),
                                        controller_id: 0,
                                        topics: vec![TopicMetadata::new(0, "t", false, partitions)],
                                        cluster_authorized_operations: i32::MIN,
                                        error_code: 0,
                                    };
                                    if encode_metadata_response(&mut response, header.api_version, &metadata).is_err() {
                                        return;
                                    }
                                }
                                FETCH => {
                                    let (_, _, topics, ..) = match decode_fetch_request(&mut req_slice, header.api_version) {
                                        Ok(d) => d,
                                        Err(_) => return,
                                    };
                                    let attempt = attempts.fetch_add(1, Ordering::SeqCst);
                                    let fetched = match handler(&topics, attempt) {
                                        Some(f) => f,
                                        None => {
                                            // Close socket without responding (simulates transport failure)
                                            return;
                                        }
                                    };
                                    if encode_fetch_response(&mut response, header.api_version, &fetched).is_err() {
                                        return;
                                    }
                                }
                                _ => return,
                            }
                            let len = match i32::try_from(response.len()) {
                                Ok(l) => l,
                                Err(_) => return,
                            };
                            let write_ok = tokio::time::timeout(io_deadline, async {
                                socket.write_i32(len).await?;
                                socket.write_all(&response).await?;
                                socket.flush().await
                            }).await;
                            if write_ok.is_err() || write_ok.unwrap().is_err() {
                                return;
                            }
                        }
                    });
                }
                finished = conns.join_next(), if !conns.is_empty() => {
                    let _ = finished;
                }
            }
        }
    })
}

/// Verify that when partition 0 succeeds on node 0 while partition 1 encounters
/// a transport failure (connection drop) on node 1, the consumer preserves partition 0's
/// records across the transport retry round and returns both partition records exactly once.
#[tokio::test]
async fn mixed_partition_retry_with_transport_failure_must_preserve_successful_records() {
    let mut cluster = spawn_two_node_cluster(
        0,
        1,
        vec![1],
        None,
        None,
        |_topics, _attempt| {
            // Node 0 succeeds with partition 0 records
            Some(vec![FetchedTopic {
                topic: "t".to_string(),
                topic_id: [0u8; 16],
                partitions: vec![{
                    let mut part = FetchedPartition::partition_response(0, 0);
                    part.records = vec![fetch_fixture::data_batch(0, &[b"p0-transport"], None)];
                    part
                }],
            }])
        },
        |_topics, attempt| {
            if attempt == 0 {
                // Attempt 0: transport failure (drop connection without response)
                None
            } else {
                // Attempt 1: retry succeeds with partition 1 records
                Some(vec![FetchedTopic {
                    topic: "t".to_string(),
                    topic_id: [0u8; 16],
                    partitions: vec![{
                        let mut part = FetchedPartition::partition_response(1, 0);
                        part.records = vec![fetch_fixture::data_batch(0, &[b"p1-transport"], None)];
                        part
                    }],
                }])
            }
        },
    )
    .await;

    let mut consumer = Consumer::new(cluster.config())
        .await
        .expect("consumer starts");
    consumer
        .assign_many([(("t", 0), 0), (("t", 1), 0)])
        .await
        .expect("assign partitions");

    let records = consumer.fetch().await.expect("fetch succeeds after retry");
    assert_eq!(
        records
            .iter()
            .map(|r| (r.partition, r.offset))
            .collect::<Vec<_>>(),
        vec![(0, 0), (1, 0)],
        "both partition records must be returned exactly once"
    );

    assert_eq!(
        consumer.positions(),
        vec![
            (TopicPartition::new("t", 0), 1),
            (TopicPartition::new("t", 1), 1),
        ],
        "positions must advance after delivery"
    );

    consumer.close().await.expect("consumer closes cleanly");
    cluster.shutdown().await;
}

/// Verify that when partition 0 succeeds on leader node 0 while partition 1 is
/// redirected to preferred replica node 1 (via KIP-392 preferred_read_replica),
/// the consumer preserves partition 0's records across the redirect round and
/// returns both partition records exactly once.
#[tokio::test]
async fn mixed_partition_retry_with_preferred_replica_redirect_must_preserve_successful_records() {
    let mut cluster = spawn_two_node_cluster(
        0,
        0, // Both partitions initially led by node 0
        vec![0, 1],
        Some("r0".to_string()),
        Some("r1".to_string()),
        |_topics, _attempt| {
            // Node 0 succeeds for partition 0, but redirects partition 1 to replica 1
            Some(vec![FetchedTopic {
                topic: "t".to_string(),
                topic_id: [0u8; 16],
                partitions: vec![
                    {
                        let mut p0 = FetchedPartition::partition_response(0, 0);
                        p0.records = vec![fetch_fixture::data_batch(0, &[b"p0-redirect"], None)];
                        p0
                    },
                    {
                        let mut p1 = FetchedPartition::partition_response(1, 0);
                        p1.preferred_read_replica = 1;
                        p1
                    },
                ],
            }])
        },
        |_topics, _attempt| {
            // Preferred replica node 1 serves partition 1
            Some(vec![FetchedTopic {
                topic: "t".to_string(),
                topic_id: [0u8; 16],
                partitions: vec![{
                    let mut p1 = FetchedPartition::partition_response(1, 0);
                    p1.records = vec![fetch_fixture::data_batch(0, &[b"p1-redirect"], None)];
                    p1
                }],
            }])
        },
    )
    .await;

    let mut cfg = cluster.config();
    cfg.rack = Some("r1".to_string());
    let mut consumer = Consumer::new(cfg).await.expect("consumer starts");
    consumer
        .assign_many([(("t", 0), 0), (("t", 1), 0)])
        .await
        .expect("assign partitions");

    let records = consumer
        .fetch()
        .await
        .expect("fetch succeeds after redirect");
    assert_eq!(
        records
            .iter()
            .map(|r| (r.partition, r.offset))
            .collect::<Vec<_>>(),
        vec![(0, 0), (1, 0)],
        "both partition records must be returned exactly once"
    );

    assert_eq!(
        consumer.positions(),
        vec![
            (TopicPartition::new("t", 0), 1),
            (TopicPartition::new("t", 1), 1),
        ],
        "positions must advance after delivery"
    );

    consumer.close().await.expect("consumer closes cleanly");
    cluster.shutdown().await;
}

/// Verify that if a fetch is aborted (e.g. by wakeup or fatal error) after a subset
/// of partitions received records, positions are NOT advanced past the discarded records.
#[tokio::test]
async fn mixed_partition_retry_aborted_must_not_advance_positions_for_discarded_records() {
    let mut broker =
        fetch_fixture::FixtureBroker::start(fetch_fixture::Scenario::PartialRetry).await;
    let mut consumer = Consumer::new(broker.config())
        .await
        .expect("consumer starts successfully");

    consumer
        .assign_many([(("t", 0), 0), (("t", 1), 0)])
        .await
        .expect("assign partitions 0 and 1 at offset 0");

    // Wakeup consumer before or during fetch so it aborts
    consumer.wakeup();
    let result = consumer.fetch().await;
    assert!(result.is_err(), "fetch must fail when aborted by wakeup");

    // Positions must NOT advance past data discarded from application delivery
    assert_eq!(
        consumer.positions(),
        vec![
            (TopicPartition::new("t", 0), 0),
            (TopicPartition::new("t", 1), 0),
        ],
        "positions must remain at offset 0 when fetch is aborted"
    );

    consumer.close().await.expect("consumer closes cleanly");
    broker.shutdown().await;
}

fn custom_batch(
    offset: i64,
    values: &[&[u8]],
    producer_id: i64,
    producer_epoch: i16,
    sequence: Option<i32>,
    is_transactional: bool,
) -> RecordBatch {
    let records = values
        .iter()
        .map(|value| Record {
            offset: 0,
            timestamp: 0,
            key: None,
            value: Some(bytes::Bytes::copy_from_slice(value)),
            headers: Vec::new(),
        })
        .collect();
    let mut batch = RecordBatch::from_records(records).with_transactional(is_transactional);
    batch.base_offset = offset;
    batch.producer_id = producer_id;
    batch.producer_epoch = producer_epoch;
    if let Some(seq) = sequence {
        batch.base_sequence = seq;
    }
    batch
}

fn custom_marker(
    offset: i64,
    control: ControlRecordType,
    producer_id: i64,
    producer_epoch: i16,
) -> RecordBatch {
    RecordBatch::with_end_transaction_marker(
        offset,
        0,
        0,
        producer_id,
        producer_epoch,
        &EndTransactionMarker::new(control, 0).expect("valid control marker"),
    )
    .expect("valid end transaction marker batch")
}

/// Audit defect A02 reproduction / regression test:
/// PID 7 aborts a transaction at offset 0 (ABORT marker at offset 1),
/// then commits another transaction at offset 2 (COMMIT marker at offset 3).
/// In `read_committed` mode, the consumer must return the committed record at offset 2,
/// and must never return control records or aborted records.
#[tokio::test]
async fn committed_transaction_after_abort_for_same_pid_must_be_visible() {
    let mut broker =
        fetch_fixture::FixtureBroker::start(fetch_fixture::Scenario::AbortThenCommit).await;
    let mut cfg = broker.config();
    cfg.isolation_level = IsolationLevel::ReadCommitted;
    let mut consumer = Consumer::new(cfg)
        .await
        .expect("consumer starts successfully");

    consumer
        .assign("t", 0, 0)
        .await
        .expect("assign topic t partition 0 at offset 0");
    let records = consumer.fetch().await.expect("fetch succeeds");

    assert_eq!(
        records.iter().map(|r| r.offset).collect::<Vec<_>>(),
        vec![2],
        "read_committed must deliver the later committed record (offset 2) after an abort under PID 7"
    );
    assert_eq!(
        records[0].value.as_deref(),
        Some(&b"committed"[..]),
        "delivered record value must match the committed batch"
    );
    assert_eq!(
        consumer.positions(),
        vec![(TopicPartition::new("t", 0), 4)],
        "fetch cursor must advance past the COMMIT marker (offset 4)"
    );

    consumer.close().await.expect("consumer closes cleanly");
    broker.shutdown().await;
}

/// Verify handling of multiple aborted intervals under the same PID as well as across
/// different PIDs:
/// - PID 7 aborts at offset 0 (ABORT marker 1)
/// - PID 7 commits at offset 2 (COMMIT marker 3)
/// - PID 7 aborts again at offset 4 (ABORT marker 5)
/// - PID 7 commits again at offset 6 (COMMIT marker 7)
/// - PID 8 commits at offset 8 (COMMIT marker 9)
/// - PID 8 aborts at offset 10 (ABORT marker 11)
///
/// In `read_committed` mode: only committed records (offsets 2, 6, 8) must be delivered.
/// Neither aborted data records (0, 4, 10) nor control records (1, 3, 5, 7, 9, 11) must appear.
#[tokio::test]
async fn read_committed_multiple_aborted_intervals_and_pids() {
    let mut broker =
        fetch_fixture::FixtureBroker::start_with_handler("t", 1, |_topics, _attempt| {
            vec![FetchedTopic {
                topic: "t".to_string(),
                topic_id: [0u8; 16],
                partitions: vec![{
                    let mut part = FetchedPartition::partition_response(0, 0);
                    part.high_watermark = 12;
                    part.last_stable_offset = 12;
                    part.aborted_transactions = vec![(7, 0), (7, 4), (8, 10)];
                    part.records = vec![
                        custom_batch(0, &[b"p7-aborted-0"], 7, 0, Some(0), true),
                        custom_marker(1, ControlRecordType::Abort, 7, 0),
                        custom_batch(2, &[b"p7-committed-2"], 7, 0, Some(1), true),
                        custom_marker(3, ControlRecordType::Commit, 7, 0),
                        custom_batch(4, &[b"p7-aborted-4"], 7, 0, Some(2), true),
                        custom_marker(5, ControlRecordType::Abort, 7, 0),
                        custom_batch(6, &[b"p7-committed-6"], 7, 0, Some(3), true),
                        custom_marker(7, ControlRecordType::Commit, 7, 0),
                        custom_batch(8, &[b"p8-committed-8"], 8, 0, Some(0), true),
                        custom_marker(9, ControlRecordType::Commit, 8, 0),
                        custom_batch(10, &[b"p8-aborted-10"], 8, 0, Some(1), true),
                        custom_marker(11, ControlRecordType::Abort, 8, 0),
                    ];
                    part
                }],
            }]
        })
        .await;

    let mut cfg = broker.config();
    cfg.isolation_level = IsolationLevel::ReadCommitted;
    let mut consumer = Consumer::new(cfg)
        .await
        .expect("consumer starts successfully");

    consumer.assign("t", 0, 0).await.expect("assign partition");
    let records = consumer.fetch().await.expect("fetch succeeds");

    assert_eq!(
        records.iter().map(|r| r.offset).collect::<Vec<_>>(),
        vec![2, 6, 8],
        "must return committed records for multiple intervals and PIDs"
    );
    assert_eq!(records[0].value.as_deref(), Some(&b"p7-committed-2"[..]));
    assert_eq!(records[1].value.as_deref(), Some(&b"p7-committed-6"[..]));
    assert_eq!(records[2].value.as_deref(), Some(&b"p8-committed-8"[..]));
    assert_eq!(
        consumer.positions(),
        vec![(TopicPartition::new("t", 0), 12)],
        "cursor must advance to offset 12 after all batches"
    );

    consumer.close().await.expect("consumer closes cleanly");
    broker.shutdown().await;
}

/// Verify that non-transactional batches are NOT suppressed merely because they share
/// a producer ID that is currently in the aborted transactions list.
#[tokio::test]
async fn nontransactional_records_sharing_aborted_pid_must_be_visible() {
    let mut broker =
        fetch_fixture::FixtureBroker::start_with_handler("t", 1, |_topics, _attempt| {
            vec![FetchedTopic {
                topic: "t".to_string(),
                topic_id: [0u8; 16],
                partitions: vec![{
                    let mut part = FetchedPartition::partition_response(0, 0);
                    part.high_watermark = 5;
                    part.last_stable_offset = 5;
                    part.aborted_transactions = vec![(7, 0)];
                    part.records = vec![
                        // Transactional batch for PID 7, aborted
                        custom_batch(0, &[b"p7-aborted-0"], 7, 0, Some(0), true),
                        // Non-transactional batch for PID 7 (must NOT be suppressed)
                        custom_batch(1, &[b"p7-nontransactional-1"], 7, 0, None, false),
                        // ABORT marker for PID 7
                        custom_marker(2, ControlRecordType::Abort, 7, 0),
                        // Transactional batch for PID 7, committed
                        custom_batch(3, &[b"p7-committed-3"], 7, 0, Some(1), true),
                        // COMMIT marker for PID 7
                        custom_marker(4, ControlRecordType::Commit, 7, 0),
                    ];
                    part
                }],
            }]
        })
        .await;

    let mut cfg = broker.config();
    cfg.isolation_level = IsolationLevel::ReadCommitted;
    let mut consumer = Consumer::new(cfg)
        .await
        .expect("consumer starts successfully");

    consumer.assign("t", 0, 0).await.expect("assign partition");
    let records = consumer.fetch().await.expect("fetch succeeds");

    assert_eq!(
        records.iter().map(|r| r.offset).collect::<Vec<_>>(),
        vec![1, 3],
        "must deliver non-transactional record 1 and committed record 3"
    );
    assert_eq!(
        records[0].value.as_deref(),
        Some(&b"p7-nontransactional-1"[..])
    );
    assert_eq!(records[1].value.as_deref(), Some(&b"p7-committed-3"[..]));
    assert_eq!(
        consumer.positions(),
        vec![(TopicPartition::new("t", 0), 5)],
        "cursor must advance to offset 5"
    );

    consumer.close().await.expect("consumer closes cleanly");
    broker.shutdown().await;
}

/// Verify that an aborted transaction spanning across multiple fetches correctly filters
/// aborted records in the first fetch and returns the subsequent committed record after
/// the ABORT marker in the second fetch.
#[tokio::test]
async fn aborted_transaction_spanning_across_fetches_must_return_subsequent_committed_record() {
    let mut broker =
        fetch_fixture::FixtureBroker::start_with_handler("t", 1, |topics, _attempt| {
            let fetch_offset = topics
                .first()
                .and_then(|t| t.partitions.first())
                .map_or(0, |p| p.fetch_offset);

            vec![FetchedTopic {
                topic: "t".to_string(),
                topic_id: [0u8; 16],
                partitions: vec![{
                    let mut part = FetchedPartition::partition_response(0, 0);
                    part.high_watermark = 4;
                    part.last_stable_offset = 4;
                    if fetch_offset == 0 {
                        // Fetch 1: only returns the aborted data batch; no marker yet
                        part.aborted_transactions = vec![(7, 0)];
                        part.records = vec![custom_batch(
                            0,
                            &[b"p7-aborted-span-0"],
                            7,
                            0,
                            Some(0),
                            true,
                        )];
                    } else if fetch_offset == 1 {
                        // Fetch 2: returns the ABORT marker, then committed data and COMMIT marker
                        part.aborted_transactions = vec![(7, 0)];
                        part.records = vec![
                            custom_marker(1, ControlRecordType::Abort, 7, 0),
                            custom_batch(2, &[b"p7-committed-span-2"], 7, 0, Some(1), true),
                            custom_marker(3, ControlRecordType::Commit, 7, 0),
                        ];
                    }
                    part
                }],
            }]
        })
        .await;

    let mut cfg = broker.config();
    cfg.isolation_level = IsolationLevel::ReadCommitted;
    let mut consumer = Consumer::new(cfg)
        .await
        .expect("consumer starts successfully");

    consumer.assign("t", 0, 0).await.expect("assign partition");

    // Fetch 1: aborted batch only -> no records returned, position advances to 1
    let records1 = consumer.fetch().await.expect("fetch 1 succeeds");
    assert!(
        records1.is_empty(),
        "aborted record in fetch 1 must not be delivered"
    );
    assert_eq!(
        consumer.positions(),
        vec![(TopicPartition::new("t", 0), 1)],
        "position must advance to 1 after aborted batch in fetch 1"
    );

    // Fetch 2: abort marker consumed, committed batch delivered -> offset 2 returned
    let records2 = consumer.fetch().await.expect("fetch 2 succeeds");
    assert_eq!(
        records2.iter().map(|r| r.offset).collect::<Vec<_>>(),
        vec![2],
        "fetch 2 must return the committed record at offset 2"
    );
    assert_eq!(
        records2[0].value.as_deref(),
        Some(&b"p7-committed-span-2"[..])
    );
    assert_eq!(
        consumer.positions(),
        vec![(TopicPartition::new("t", 0), 4)],
        "position must advance to 4 after COMMIT marker in fetch 2"
    );

    consumer.close().await.expect("consumer closes cleanly");
    broker.shutdown().await;
}

/// Verify that in `read_committed` mode, consumption stops at the `last_stable_offset`
/// boundary. Batches/records at or after `last_stable_offset` must not be returned and
/// the consumer's position must not advance past `last_stable_offset`.
#[tokio::test]
async fn read_committed_stops_at_last_stable_offset_boundary() {
    let mut broker =
        fetch_fixture::FixtureBroker::start_with_handler("t", 1, |_topics, _attempt| {
            vec![FetchedTopic {
                topic: "t".to_string(),
                topic_id: [0u8; 16],
                partitions: vec![{
                    let mut part = FetchedPartition::partition_response(0, 0);
                    // LSO is 4, high watermark is 6
                    part.high_watermark = 6;
                    part.last_stable_offset = 4;
                    part.aborted_transactions = vec![(7, 0)];
                    part.records = vec![
                        // Offsets 0..3 are below LSO (4)
                        custom_batch(0, &[b"p7-aborted-0"], 7, 0, Some(0), true),
                        custom_marker(1, ControlRecordType::Abort, 7, 0),
                        custom_batch(2, &[b"p7-committed-2"], 7, 0, Some(1), true),
                        custom_marker(3, ControlRecordType::Commit, 7, 0),
                        // Offsets 4..5 are at/above LSO (4) -> uncommitted data
                        custom_batch(4, &[b"p8-uncommitted-4"], 8, 0, Some(0), true),
                        custom_batch(5, &[b"p8-uncommitted-5"], 8, 0, Some(1), true),
                    ];
                    part
                }],
            }]
        })
        .await;

    let mut cfg = broker.config();
    cfg.isolation_level = IsolationLevel::ReadCommitted;
    let mut consumer = Consumer::new(cfg)
        .await
        .expect("consumer starts successfully");

    consumer.assign("t", 0, 0).await.expect("assign partition");
    let records = consumer.fetch().await.expect("fetch succeeds");

    assert_eq!(
        records.iter().map(|r| r.offset).collect::<Vec<_>>(),
        vec![2],
        "must only return committed records below LSO (offset 2)"
    );
    assert_eq!(records[0].value.as_deref(), Some(&b"p7-committed-2"[..]));
    assert_eq!(
        consumer.positions(),
        vec![(TopicPartition::new("t", 0), 4)],
        "cursor must stop at LSO boundary (offset 4) and not advance past it"
    );

    consumer.close().await.expect("consumer closes cleanly");
    broker.shutdown().await;
}

/// Audit defect A01 reproduction / regression test:
/// Assign or seek to offset 2 when a broker returns a batch containing offsets 0, 1, 2.
/// The consumer must return only offset 2 and advance its position to 3.
#[tokio::test]
async fn seek_inside_batch_must_not_return_earlier_records() {
    let mut broker = fetch_fixture::FixtureBroker::start(fetch_fixture::Scenario::WholeBatch).await;
    let mut consumer = Consumer::new(broker.config())
        .await
        .expect("consumer starts successfully");

    // Case 1: Initial assign at offset 2
    consumer
        .assign("t", 0, 2)
        .await
        .expect("assign topic t partition 0 at offset 2");
    let records = consumer.fetch().await.expect("fetch succeeds");

    assert_eq!(
        records.iter().map(|r| r.offset).collect::<Vec<_>>(),
        vec![2],
        "must return only offset 2 when assigned at offset 2 inside a [0, 1, 2] batch"
    );
    assert_eq!(records[0].value.as_deref(), Some(&b"c"[..]));
    assert_eq!(
        consumer.positions(),
        vec![(TopicPartition::new("t", 0), 3)],
        "cursor must advance to offset 3"
    );

    // Case 2: Seek to offset 1 inside the same batch
    consumer.seek("t", 0, 1).expect("seek to offset 1");
    let records = consumer.fetch().await.expect("fetch succeeds after seek");
    assert_eq!(
        records.iter().map(|r| r.offset).collect::<Vec<_>>(),
        vec![1, 2],
        "must return offsets [1, 2] when seeking to offset 1 inside a [0, 1, 2] batch"
    );
    assert_eq!(records[0].value.as_deref(), Some(&b"b"[..]));
    assert_eq!(records[1].value.as_deref(), Some(&b"c"[..]));
    assert_eq!(
        consumer.positions(),
        vec![(TopicPartition::new("t", 0), 3)],
        "cursor must advance to offset 3"
    );

    // Case 3: Seek to offset 0 (start of batch)
    consumer.seek("t", 0, 0).expect("seek to offset 0");
    let records = consumer
        .fetch()
        .await
        .expect("fetch succeeds after seek to 0");
    assert_eq!(
        records.iter().map(|r| r.offset).collect::<Vec<_>>(),
        vec![0, 1, 2],
        "must return all offsets [0, 1, 2] when seeking to offset 0"
    );
    assert_eq!(
        consumer.positions(),
        vec![(TopicPartition::new("t", 0), 3)],
        "cursor must advance to offset 3"
    );

    consumer.close().await.expect("consumer closes cleanly");
    broker.shutdown().await;
}

/// Verify that the lower-bound fetch offset filter operates correctly on
/// compressed batches (Gzip, Snappy, and Lz4).
#[tokio::test]
async fn fetch_offset_filter_compressed_batches() {
    for compression in [
        partitionline::Compression::Gzip,
        partitionline::Compression::Snappy,
        partitionline::Compression::Lz4,
    ] {
        let mut broker =
            fetch_fixture::FixtureBroker::start_with_handler("t", 1, move |_topics, _attempt| {
                vec![FetchedTopic {
                    topic: "t".to_string(),
                    topic_id: [0u8; 16],
                    partitions: vec![{
                        let mut part = FetchedPartition::partition_response(0, 0);
                        part.high_watermark = 3;
                        part.last_stable_offset = 3;
                        part.records = vec![fetch_fixture::data_batch(
                            0,
                            &[b"comp-0", b"comp-1", b"comp-2"],
                            None,
                        )
                        .with_compression(compression)];
                        part
                    }],
                }]
            })
            .await;

        let mut consumer = Consumer::new(broker.config())
            .await
            .expect("consumer starts successfully");

        // Seek/assign to offset 2 inside compressed batch [0, 1, 2]
        consumer
            .assign("t", 0, 2)
            .await
            .expect("assign partition 0 at offset 2");
        let records = consumer.fetch().await.expect("fetch succeeds");

        assert_eq!(
            records.iter().map(|r| r.offset).collect::<Vec<_>>(),
            vec![2],
            "compressed batch with {compression:?} must return only offset 2 when requested at offset 2"
        );
        assert_eq!(records[0].value.as_deref(), Some(&b"comp-2"[..]));
        assert_eq!(
            consumer.positions(),
            vec![(TopicPartition::new("t", 0), 3)],
            "cursor must advance to offset 3 for {compression:?}"
        );

        // Seek to offset 1 inside the compressed batch
        consumer.seek("t", 0, 1).expect("seek to offset 1");
        let records = consumer.fetch().await.expect("fetch succeeds");
        assert_eq!(
            records.iter().map(|r| r.offset).collect::<Vec<_>>(),
            vec![1, 2],
            "compressed batch with {compression:?} must return [1, 2] when seeking to offset 1"
        );

        consumer.close().await.expect("consumer closes cleanly");
        broker.shutdown().await;
    }
}

/// Verify fetch offset filtering across sparse and compacted offsets.
/// Tests gaps between batches, seeking into gaps, and seeking to the end.
#[tokio::test]
async fn fetch_offset_filter_sparse_and_compacted_offsets() {
    let mut broker =
        fetch_fixture::FixtureBroker::start_with_handler("t", 1, |_topics, _attempt| {
            vec![FetchedTopic {
                topic: "t".to_string(),
                topic_id: [0u8; 16],
                partitions: vec![{
                    let mut part = FetchedPartition::partition_response(0, 0);
                    part.high_watermark = 12;
                    part.last_stable_offset = 12;
                    // Three batches with compacted gaps between them:
                    // Batch 0: offsets 0, 1
                    // Batch 1: offsets 5, 6 (offsets 2, 3, 4 compacted away)
                    // Batch 2: offsets 10, 11 (offsets 7, 8, 9 compacted away)
                    part.records = vec![
                        fetch_fixture::data_batch(0, &[b"gap-0", b"gap-1"], None),
                        fetch_fixture::data_batch(5, &[b"gap-5", b"gap-6"], None),
                        fetch_fixture::data_batch(10, &[b"gap-10", b"gap-11"], None),
                    ];
                    part
                }],
            }]
        })
        .await;

    let mut consumer = Consumer::new(broker.config())
        .await
        .expect("consumer starts successfully");

    // Case 1: Seek to offset 5 -> drops batch 0 (0, 1), returns batches 1 and 2 (5, 6, 10, 11)
    consumer
        .assign("t", 0, 5)
        .await
        .expect("assign at offset 5");
    let records = consumer.fetch().await.expect("fetch succeeds");
    assert_eq!(
        records.iter().map(|r| r.offset).collect::<Vec<_>>(),
        vec![5, 6, 10, 11],
        "must drop batch 0 and deliver records from batch 1 and 2"
    );
    assert_eq!(
        consumer.positions(),
        vec![(TopicPartition::new("t", 0), 12)],
        "cursor must advance to offset 12"
    );

    // Case 2: Seek to offset 2 (landing in the compacted gap 2..5)
    // Must drop batch 0 (0, 1), and return batches 1 and 2 (5, 6, 10, 11)
    consumer.seek("t", 0, 2).expect("seek to offset 2");
    let records = consumer.fetch().await.expect("fetch succeeds");
    assert_eq!(
        records.iter().map(|r| r.offset).collect::<Vec<_>>(),
        vec![5, 6, 10, 11],
        "must drop batch 0 and deliver from first available offset >= 2 (5)"
    );
    assert_eq!(
        consumer.positions(),
        vec![(TopicPartition::new("t", 0), 12)],
        "cursor must advance to offset 12"
    );

    // Case 3: Seek to offset 7 (landing in the compacted gap 7..10)
    // Must drop batches 0 and 1, and return batch 2 (10, 11)
    consumer.seek("t", 0, 7).expect("seek to offset 7");
    let records = consumer.fetch().await.expect("fetch succeeds");
    assert_eq!(
        records.iter().map(|r| r.offset).collect::<Vec<_>>(),
        vec![10, 11],
        "must drop batches 0 and 1, delivering from first available offset >= 7 (10)"
    );
    assert_eq!(
        consumer.positions(),
        vec![(TopicPartition::new("t", 0), 12)],
        "cursor must advance to offset 12"
    );

    // Case 4: Seek to offset 12 (at end of all batches)
    consumer.seek("t", 0, 12).expect("seek to offset 12");
    let records = consumer.fetch().await.expect("fetch succeeds");
    assert!(
        records.is_empty(),
        "must return empty when seeking past all batch offsets"
    );
    assert_eq!(
        consumer.positions(),
        vec![(TopicPartition::new("t", 0), 12)],
        "cursor must remain at offset 12 without regressing"
    );

    consumer.close().await.expect("consumer closes cleanly");
    broker.shutdown().await;
}

/// Verify that fetch offset filtering applies per-partition across multiple partitions
/// in the same fetch request/response.
#[tokio::test]
async fn fetch_offset_filter_multiple_partitions() {
    let mut broker =
        fetch_fixture::FixtureBroker::start_with_handler("t", 3, |_topics, _attempt| {
            vec![FetchedTopic {
                topic: "t".to_string(),
                topic_id: [0u8; 16],
                partitions: (0..3)
                    .map(|p| {
                        let mut part = FetchedPartition::partition_response(p, 0);
                        part.high_watermark = 3;
                        part.last_stable_offset = 3;
                        let val0 = format!("p{p}-0");
                        let val1 = format!("p{p}-1");
                        let val2 = format!("p{p}-2");
                        part.records = vec![fetch_fixture::data_batch(
                            0,
                            &[val0.as_bytes(), val1.as_bytes(), val2.as_bytes()],
                            None,
                        )];
                        part
                    })
                    .collect(),
            }]
        })
        .await;

    let mut consumer = Consumer::new(broker.config())
        .await
        .expect("consumer starts successfully");

    // Assign partition 0 at 2, partition 1 at 1, partition 2 at 0
    consumer
        .assign_many([(("t", 0), 2), (("t", 1), 1), (("t", 2), 0)])
        .await
        .expect("assign 3 partitions at different offsets");

    let records = consumer.fetch().await.expect("fetch succeeds");

    let p0_offsets: Vec<_> = records
        .iter()
        .filter(|r| r.partition == 0)
        .map(|r| r.offset)
        .collect();
    let p1_offsets: Vec<_> = records
        .iter()
        .filter(|r| r.partition == 1)
        .map(|r| r.offset)
        .collect();
    let p2_offsets: Vec<_> = records
        .iter()
        .filter(|r| r.partition == 2)
        .map(|r| r.offset)
        .collect();

    assert_eq!(
        p0_offsets,
        vec![2],
        "partition 0 (requested 2) must return [2]"
    );
    assert_eq!(
        p1_offsets,
        vec![1, 2],
        "partition 1 (requested 1) must return [1, 2]"
    );
    assert_eq!(
        p2_offsets,
        vec![0, 1, 2],
        "partition 2 (requested 0) must return [0, 1, 2]"
    );

    assert_eq!(
        consumer.positions(),
        vec![
            (TopicPartition::new("t", 0), 3),
            (TopicPartition::new("t", 1), 3),
            (TopicPartition::new("t", 2), 3),
        ],
        "positions for all 3 partitions must advance to offset 3"
    );

    consumer.close().await.expect("consumer closes cleanly");
    broker.shutdown().await;
}

/// Verify that fetch offset filtering does NOT bypass aborted transaction filtering
/// or control record filtering (KL03-03 contract).
#[tokio::test]
async fn fetch_offset_filter_must_not_bypass_aborted_or_control_filtering() {
    let mut broker =
        fetch_fixture::FixtureBroker::start_with_handler("t", 1, |_topics, _attempt| {
            vec![FetchedTopic {
                topic: "t".to_string(),
                topic_id: [0u8; 16],
                partitions: vec![{
                    let mut part = FetchedPartition::partition_response(0, 0);
                    part.high_watermark = 6;
                    part.last_stable_offset = 6;
                    part.aborted_transactions = vec![(7, 0)];
                    part.records = vec![
                        // PID 7 aborted batch at offset 0
                        custom_batch(0, &[b"p7-aborted-0"], 7, 0, Some(0), true),
                        // ABORT marker at offset 1
                        custom_marker(1, ControlRecordType::Abort, 7, 0),
                        // PID 7 committed batch at offsets 2, 3, 4
                        custom_batch(
                            2,
                            &[b"p7-comm-2", b"p7-comm-3", b"p7-comm-4"],
                            7,
                            0,
                            Some(1),
                            true,
                        ),
                        // COMMIT marker at offset 5
                        custom_marker(5, ControlRecordType::Commit, 7, 0),
                    ];
                    part
                }],
            }]
        })
        .await;

    let mut cfg = broker.config();
    cfg.isolation_level = IsolationLevel::ReadCommitted;
    let mut consumer = Consumer::new(cfg)
        .await
        .expect("consumer starts successfully");

    // Case 1: Seek/assign to offset 3 inside the committed batch [2, 3, 4]
    // Record 0 (aborted) and marker 1 (abort) are before offset 3.
    // Record 2 is committed, but before offset 3 -> must be dropped.
    // Records 3 and 4 are committed and >= offset 3 -> must be delivered.
    // Marker 5 is control marker -> must NOT be delivered.
    consumer
        .assign("t", 0, 3)
        .await
        .expect("assign at offset 3");
    let records = consumer.fetch().await.expect("fetch succeeds");

    assert_eq!(
        records.iter().map(|r| r.offset).collect::<Vec<_>>(),
        vec![3, 4],
        "must deliver only committed records at or above requested offset 3"
    );
    assert_eq!(records[0].value.as_deref(), Some(&b"p7-comm-3"[..]));
    assert_eq!(records[1].value.as_deref(), Some(&b"p7-comm-4"[..]));
    assert_eq!(
        consumer.positions(),
        vec![(TopicPartition::new("t", 0), 6)],
        "cursor must advance to offset 6 past the commit marker"
    );

    // Case 2: Seek to offset 1
    // Offset 0 is aborted. Offset 1 is abort marker.
    // Offsets 2, 3, 4 are committed and >= 1 -> must be delivered.
    // Neither aborted record nor control records must appear.
    consumer.seek("t", 0, 1).expect("seek to offset 1");
    let records = consumer
        .fetch()
        .await
        .expect("fetch succeeds after seek to 1");
    assert_eq!(
        records.iter().map(|r| r.offset).collect::<Vec<_>>(),
        vec![2, 3, 4],
        "must deliver committed records [2, 3, 4] and skip abort marker and commit marker"
    );
    assert_eq!(
        consumer.positions(),
        vec![(TopicPartition::new("t", 0), 6)],
        "cursor must advance to offset 6"
    );

    consumer.close().await.expect("consumer closes cleanly");
    broker.shutdown().await;
}

/// Verify that dropping records below the requested offset does NOT regress the consumer
/// position and does NOT lose later valid records.
#[tokio::test]
async fn fetch_offset_filter_must_not_regress_position_or_lose_later_records() {
    let mut broker = fetch_fixture::FixtureBroker::start_with_handler("t", 1, |topics, attempt| {
        let fetch_offset = topics
            .first()
            .and_then(|t| t.partitions.first())
            .map_or(0, |p| p.fetch_offset);

        vec![FetchedTopic {
            topic: "t".to_string(),
            topic_id: [0u8; 16],
            partitions: vec![{
                let mut part = FetchedPartition::partition_response(0, 0);
                part.high_watermark = 8;
                part.last_stable_offset = 8;
                if attempt == 0 {
                    // Attempt 0: broker sends a stale batch with offsets 0, 1, 2
                    // even though consumer requested offset 5.
                    part.records = vec![fetch_fixture::data_batch(
                        0,
                        &[b"stale-0", b"stale-1", b"stale-2"],
                        None,
                    )];
                } else {
                    // Attempt 1: broker sends the expected records starting at fetch_offset
                    part.records = vec![fetch_fixture::data_batch(
                        fetch_offset,
                        &[b"valid-5", b"valid-6"],
                        None,
                    )];
                }
                part
            }],
        }]
    })
    .await;

    let mut consumer = Consumer::new(broker.config())
        .await
        .expect("consumer starts successfully");

    consumer
        .assign("t", 0, 5)
        .await
        .expect("assign at offset 5");

    // Fetch 1: broker returns stale batch [0, 1, 2]
    // All records are < 5, so none must be delivered.
    let records1 = consumer.fetch().await.expect("fetch 1 succeeds");
    assert!(
        records1.is_empty(),
        "stale records below requested offset must not be delivered"
    );
    assert_eq!(
        consumer.positions(),
        vec![(TopicPartition::new("t", 0), 5)],
        "cursor must NOT regress to stale batch offsets; must stay at 5"
    );

    // Fetch 2: broker now returns valid records starting at 5
    // Later records [5, 6] must NOT be lost!
    let records2 = consumer.fetch().await.expect("fetch 2 succeeds");
    assert_eq!(
        records2.iter().map(|r| r.offset).collect::<Vec<_>>(),
        vec![5, 6],
        "must deliver records [5, 6] without losing them"
    );
    assert_eq!(records2[0].value.as_deref(), Some(&b"valid-5"[..]));
    assert_eq!(records2[1].value.as_deref(), Some(&b"valid-6"[..]));
    assert_eq!(
        consumer.positions(),
        vec![(TopicPartition::new("t", 0), 7)],
        "cursor must advance to offset 7"
    );

    consumer.close().await.expect("consumer closes cleanly");
    broker.shutdown().await;
}

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

mod common;

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
use partitionline::{
    AutoOffsetReset, Consumer, ConsumerConfig, IsolationLevel, OffsetAndMetadata, ProduceRecord,
    Producer, ProducerConfig, TopicPartition,
};
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

/// Audit defect A04 reproduction / regression test:
/// When auto.offset.reset is None, OFFSET_OUT_OF_RANGE must return an explicit error
/// without silently advancing or changing the consumer position.
#[tokio::test]
async fn out_of_range_with_reset_none_must_fail_without_advancing() {
    let mut broker = fetch_fixture::FixtureBroker::start(fetch_fixture::Scenario::OutOfRange).await;
    let mut consumer = Consumer::new(broker.config().auto_offset_reset(AutoOffsetReset::None))
        .await
        .expect("consumer starts successfully");

    consumer
        .assign("t", 0, 0)
        .await
        .expect("assign partition 0 at offset 0");

    let result = consumer.fetch().await;
    assert!(
        result.is_err(),
        "fetch must fail with explicit error when auto.offset.reset is None; positions={:?}",
        consumer.positions()
    );
    assert_eq!(
        consumer.positions(),
        vec![(TopicPartition::new("t", 0), 0)],
        "positions must remain unchanged after out-of-range error with reset None"
    );

    consumer.close().await.expect("consumer closes cleanly");
    broker.shutdown().await;
}

/// Verify that with auto.offset.reset=Earliest, OFFSET_OUT_OF_RANGE resolves the leader
/// log start offset, clears last fetched epoch, and purges pending/abort state.
#[tokio::test]
async fn out_of_range_with_reset_earliest_resolves_log_start_and_clears_stale_state() {
    let last_epoch_sent = Arc::new(AtomicUsize::new(0));
    let last_epoch_capture = Arc::clone(&last_epoch_sent);
    let fetch_offset_sent = Arc::new(AtomicUsize::new(0));
    let fetch_offset_capture = Arc::clone(&fetch_offset_sent);

    let mut broker =
        fetch_fixture::FixtureBroker::start_with_handler("t", 1, move |topics, attempt| {
            let p = &topics[0].partitions[0];
            last_epoch_capture.store(
                if p.last_fetched_epoch < 0 {
                    999 // sentinel for negative / cleared epoch
                } else {
                    p.last_fetched_epoch as usize
                },
                Ordering::SeqCst,
            );
            fetch_offset_capture.store(p.fetch_offset as usize, Ordering::SeqCst);

            vec![FetchedTopic {
                topic: "t".to_string(),
                topic_id: [0u8; 16],
                partitions: vec![{
                    let mut part = FetchedPartition::partition_response(0, 0);
                    if attempt == 0 {
                        part.error_code = OFFSET_OUT_OF_RANGE;
                        part.log_start_offset = 10;
                        part.high_watermark = 20;
                        part.last_stable_offset = 20;
                    } else {
                        part.high_watermark = 12;
                        part.last_stable_offset = 12;
                        part.records = vec![fetch_fixture::data_batch(10, &[b"earliest-10"], None)];
                    }
                    part
                }],
            }]
        })
        .await;

    let mut consumer = Consumer::new(broker.config().auto_offset_reset(AutoOffsetReset::Earliest))
        .await
        .expect("consumer starts successfully");

    consumer
        .assign("t", 0, 0)
        .await
        .expect("assign at offset 0");
    consumer
        .seek_with_metadata(("t", 0), OffsetAndMetadata::new(0).with_leader_epoch(7))
        .expect("seek with leader epoch 7");

    // Fetch 1: triggers OFFSET_OUT_OF_RANGE, must jump to log start offset 10 and clear epoch
    let recs1 = consumer.fetch().await.expect("fetch 1 succeeds via reset");
    assert!(
        recs1.is_empty(),
        "out-of-range fetch should return empty records on reset"
    );
    assert_eq!(
        consumer.positions(),
        vec![(TopicPartition::new("t", 0), 10)],
        "position must advance to log start offset 10"
    );

    // Fetch 2: next fetch must use fetch_offset 10 and cleared last_fetched_epoch (-1 / sentinel 999)
    let recs2 = consumer.fetch().await.expect("fetch 2 succeeds");
    assert_eq!(recs2.len(), 1);
    assert_eq!(recs2[0].offset, 10);
    assert_eq!(fetch_offset_sent.load(Ordering::SeqCst), 10);
    assert_eq!(
        last_epoch_sent.load(Ordering::SeqCst),
        999,
        "last fetched epoch must be cleared after out-of-range reset"
    );

    consumer.close().await.expect("consumer closes cleanly");
    broker.shutdown().await;
}

/// Verify that with auto.offset.reset=Latest, OFFSET_OUT_OF_RANGE resolves the leader
/// high watermark, clears last fetched epoch, and purges pending/abort state.
#[tokio::test]
async fn out_of_range_with_reset_latest_resolves_high_watermark_and_clears_stale_state() {
    let last_epoch_sent = Arc::new(AtomicUsize::new(0));
    let last_epoch_capture = Arc::clone(&last_epoch_sent);
    let fetch_offset_sent = Arc::new(AtomicUsize::new(0));
    let fetch_offset_capture = Arc::clone(&fetch_offset_sent);

    let mut broker =
        fetch_fixture::FixtureBroker::start_with_handler("t", 1, move |topics, attempt| {
            let p = &topics[0].partitions[0];
            last_epoch_capture.store(
                if p.last_fetched_epoch < 0 {
                    999
                } else {
                    p.last_fetched_epoch as usize
                },
                Ordering::SeqCst,
            );
            fetch_offset_capture.store(p.fetch_offset as usize, Ordering::SeqCst);

            vec![FetchedTopic {
                topic: "t".to_string(),
                topic_id: [0u8; 16],
                partitions: vec![{
                    let mut part = FetchedPartition::partition_response(0, 0);
                    if attempt == 0 {
                        part.error_code = OFFSET_OUT_OF_RANGE;
                        part.log_start_offset = 10;
                        part.high_watermark = 20;
                        part.last_stable_offset = 20;
                    } else {
                        part.high_watermark = 21;
                        part.last_stable_offset = 21;
                        part.records = vec![fetch_fixture::data_batch(20, &[b"latest-20"], None)];
                    }
                    part
                }],
            }]
        })
        .await;

    let mut consumer = Consumer::new(broker.config().auto_offset_reset(AutoOffsetReset::Latest))
        .await
        .expect("consumer starts successfully");

    consumer
        .assign("t", 0, 0)
        .await
        .expect("assign at offset 0");
    consumer
        .seek_with_metadata(("t", 0), OffsetAndMetadata::new(0).with_leader_epoch(5))
        .expect("seek with leader epoch 5");

    // Fetch 1: triggers OFFSET_OUT_OF_RANGE, must jump to high watermark 20 (not log start 10)
    let recs1 = consumer.fetch().await.expect("fetch 1 succeeds via reset");
    assert!(recs1.is_empty());
    assert_eq!(
        consumer.positions(),
        vec![(TopicPartition::new("t", 0), 20)],
        "position must advance to high watermark 20 for Latest reset"
    );

    // Fetch 2: next fetch must use fetch_offset 20 and cleared last_fetched_epoch (-1 / sentinel 999)
    let recs2 = consumer.fetch().await.expect("fetch 2 succeeds");
    assert_eq!(recs2.len(), 1);
    assert_eq!(recs2[0].offset, 20);
    assert_eq!(fetch_offset_sent.load(Ordering::SeqCst), 20);
    assert_eq!(
        last_epoch_sent.load(Ordering::SeqCst),
        999,
        "last fetched epoch must be cleared after out-of-range reset"
    );

    consumer.close().await.expect("consumer closes cleanly");
    broker.shutdown().await;
}

/// Verify that when a preferred replica returns OFFSET_OUT_OF_RANGE, the consumer
/// does not immediately perform a destructive reset, but retries fetching from the leader.
/// If the leader has the requested offset, records are successfully delivered.
#[tokio::test]
async fn out_of_range_preferred_replica_retries_at_leader_before_destructive_reset() {
    let leader_fetch_count = Arc::new(AtomicUsize::new(0));
    let leader_fetch_capture = Arc::clone(&leader_fetch_count);

    let mut cluster = spawn_two_node_cluster(
        0, // Leader is node 0
        0,
        vec![0, 1],
        Some("r0".to_string()),
        Some("r1".to_string()),
        move |_topics, _attempt| {
            let count = leader_fetch_capture.fetch_add(1, Ordering::SeqCst);
            if count == 0 {
                // Round 0: Leader redirects to preferred replica 1
                Some(vec![FetchedTopic {
                    topic: "t".to_string(),
                    topic_id: [0u8; 16],
                    partitions: vec![{
                        let mut p0 = FetchedPartition::partition_response(0, 0);
                        p0.preferred_read_replica = 1;
                        p0
                    }],
                }])
            } else {
                // Round 2 (after preferred replica failed with out-of-range):
                // Leader serves the data at requested offset 0
                Some(vec![FetchedTopic {
                    topic: "t".to_string(),
                    topic_id: [0u8; 16],
                    partitions: vec![{
                        let mut p0 = FetchedPartition::partition_response(0, 0);
                        p0.high_watermark = 1;
                        p0.records = vec![fetch_fixture::data_batch(0, &[b"from-leader"], None)];
                        p0
                    }],
                }])
            }
        },
        |_topics, _attempt| {
            // Round 1: Preferred replica node 1 returns OFFSET_OUT_OF_RANGE
            Some(vec![FetchedTopic {
                topic: "t".to_string(),
                topic_id: [0u8; 16],
                partitions: vec![{
                    let mut p0 = FetchedPartition::partition_response(0, OFFSET_OUT_OF_RANGE);
                    p0.log_start_offset = 10;
                    p0.high_watermark = 20;
                    p0
                }],
            }])
        },
    )
    .await;

    let mut cfg = cluster.config();
    cfg.rack = Some("r1".to_string());
    cfg.auto_offset_reset = AutoOffsetReset::Earliest;

    let mut consumer = Consumer::new(cfg).await.expect("consumer starts");
    consumer
        .assign("t", 0, 0)
        .await
        .expect("assign partition 0 at offset 0");

    let records = consumer.fetch().await.expect("fetch succeeds from leader");
    assert_eq!(
        records.iter().map(|r| r.offset).collect::<Vec<_>>(),
        vec![0],
        "must deliver offset 0 from leader instead of resetting to replica log start 10"
    );
    assert_eq!(records[0].value.as_deref(), Some(&b"from-leader"[..]));
    assert_eq!(
        consumer.positions(),
        vec![(TopicPartition::new("t", 0), 1)],
        "position must advance to 1"
    );

    consumer.close().await.expect("consumer closes cleanly");
    cluster.shutdown().await;
}

/// Verify that when a preferred replica returns OFFSET_OUT_OF_RANGE, and the leader
/// ALSO returns OFFSET_OUT_OF_RANGE, the reset policy is applied.
#[tokio::test]
async fn out_of_range_preferred_replica_resets_if_leader_also_out_of_range() {
    let leader_fetch_count = Arc::new(AtomicUsize::new(0));
    let leader_fetch_capture = Arc::clone(&leader_fetch_count);

    let mut cluster = spawn_two_node_cluster(
        0, // Leader is node 0
        0,
        vec![0, 1],
        Some("r0".to_string()),
        Some("r1".to_string()),
        move |_topics, _attempt| {
            let count = leader_fetch_capture.fetch_add(1, Ordering::SeqCst);
            if count == 0 {
                // Round 0: Leader redirects to preferred replica 1
                Some(vec![FetchedTopic {
                    topic: "t".to_string(),
                    topic_id: [0u8; 16],
                    partitions: vec![{
                        let mut p0 = FetchedPartition::partition_response(0, 0);
                        p0.preferred_read_replica = 1;
                        p0
                    }],
                }])
            } else {
                // Round 2: Leader also returns OFFSET_OUT_OF_RANGE
                Some(vec![FetchedTopic {
                    topic: "t".to_string(),
                    topic_id: [0u8; 16],
                    partitions: vec![{
                        let mut p0 = FetchedPartition::partition_response(0, OFFSET_OUT_OF_RANGE);
                        p0.log_start_offset = 10;
                        p0.high_watermark = 20;
                        p0
                    }],
                }])
            }
        },
        |_topics, _attempt| {
            // Round 1: Preferred replica node 1 returns OFFSET_OUT_OF_RANGE
            Some(vec![FetchedTopic {
                topic: "t".to_string(),
                topic_id: [0u8; 16],
                partitions: vec![{
                    let mut p0 = FetchedPartition::partition_response(0, OFFSET_OUT_OF_RANGE);
                    p0.log_start_offset = 10;
                    p0.high_watermark = 20;
                    p0
                }],
            }])
        },
    )
    .await;

    let mut cfg = cluster.config();
    cfg.rack = Some("r1".to_string());
    cfg.auto_offset_reset = AutoOffsetReset::Earliest;

    let mut consumer = Consumer::new(cfg).await.expect("consumer starts");
    consumer
        .assign("t", 0, 0)
        .await
        .expect("assign partition 0 at offset 0");

    let records = consumer.fetch().await.expect("fetch succeeds via reset");
    assert!(records.is_empty());
    assert_eq!(
        consumer.positions(),
        vec![(TopicPartition::new("t", 0), 10)],
        "position must reset to log start 10 once leader confirms out-of-range"
    );

    consumer.close().await.expect("consumer closes cleanly");
    cluster.shutdown().await;
}

/// Verify that with max_poll_records, consumer.position() reports the delivered position
/// (the next consumable record offset), while consumer.fetch_cursor() reports the
/// ahead-of-delivery broker fetch cursor.
#[tokio::test]
async fn max_poll_records_distinguishes_delivered_position_and_fetch_cursor() {
    let mut broker =
        fetch_fixture::FixtureBroker::start_with_handler("t", 1, |_topics, _attempt| {
            vec![FetchedTopic {
                topic: "t".to_string(),
                topic_id: [0u8; 16],
                partitions: vec![{
                    let mut part = FetchedPartition::partition_response(0, 0);
                    part.high_watermark = 3;
                    part.last_stable_offset = 3;
                    part.records = vec![fetch_fixture::data_batch(
                        0,
                        &[b"rec-0", b"rec-1", b"rec-2"],
                        None,
                    )];
                    part
                }],
            }]
        })
        .await;

    let mut cfg = broker.config();
    cfg.max_poll_records = Some(1);
    let mut consumer = Consumer::new(cfg)
        .await
        .expect("consumer starts successfully");

    consumer
        .assign("t", 0, 0)
        .await
        .expect("assign at offset 0");
    assert_eq!(consumer.position("t", 0).unwrap(), 0);
    assert_eq!(consumer.fetch_cursor("t", 0).unwrap(), 0);
    assert_eq!(consumer.positions(), vec![(TopicPartition::new("t", 0), 0)]);
    assert_eq!(
        consumer.fetch_cursors(),
        vec![(TopicPartition::new("t", 0), 0)]
    );

    // Fetch 1: broker returns [0, 1, 2], max_poll_records=1 delivers [0]
    let recs1 = consumer.fetch().await.expect("fetch 1 succeeds");
    assert_eq!(recs1.iter().map(|r| r.offset).collect::<Vec<_>>(), vec![0]);
    assert_eq!(
        consumer.position("t", 0).unwrap(),
        1,
        "delivered position must be 1 after consuming offset 0"
    );
    assert_eq!(
        consumer.fetch_cursor("t", 0).unwrap(),
        3,
        "fetch cursor must be 3 after fetching whole batch"
    );
    assert_eq!(consumer.positions(), vec![(TopicPartition::new("t", 0), 1)]);
    assert_eq!(
        consumer.fetch_cursors(),
        vec![(TopicPartition::new("t", 0), 3)]
    );

    // Fetch 2: drains [1] from buffer without broker request
    let recs2 = consumer.fetch().await.expect("fetch 2 succeeds");
    assert_eq!(recs2.iter().map(|r| r.offset).collect::<Vec<_>>(), vec![1]);
    assert_eq!(
        consumer.position("t", 0).unwrap(),
        2,
        "delivered position must be 2 after consuming offset 1"
    );
    assert_eq!(consumer.fetch_cursor("t", 0).unwrap(), 3);

    // Fetch 3: drains [2] from buffer
    let recs3 = consumer.fetch().await.expect("fetch 3 succeeds");
    assert_eq!(recs3.iter().map(|r| r.offset).collect::<Vec<_>>(), vec![2]);
    assert_eq!(
        consumer.position("t", 0).unwrap(),
        3,
        "delivered position must be 3 after consuming offset 2 (buffer now empty)"
    );
    assert_eq!(consumer.fetch_cursor("t", 0).unwrap(), 3);

    consumer.close().await.expect("consumer closes cleanly");
    broker.shutdown().await;
}

/// Verify that a control-only batch advances both delivered position and fetch cursor
/// even though no records are delivered to the application.
#[tokio::test]
async fn control_only_batch_advances_delivered_position_and_fetch_cursor() {
    let mut broker =
        fetch_fixture::FixtureBroker::start_with_handler("t", 1, |_topics, attempt| {
            vec![FetchedTopic {
                topic: "t".to_string(),
                topic_id: [0u8; 16],
                partitions: vec![{
                    let mut part = FetchedPartition::partition_response(0, 0);
                    if attempt == 0 {
                        part.high_watermark = 1;
                        part.last_stable_offset = 1;
                        // Control-only batch: transaction abort marker at offset 0
                        part.records = vec![custom_marker(0, ControlRecordType::Abort, 7, 0)];
                    } else {
                        part.high_watermark = 2;
                        part.last_stable_offset = 2;
                        part.records = vec![fetch_fixture::data_batch(1, &[b"data-1"], None)];
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
        .assign("t", 0, 0)
        .await
        .expect("assign at offset 0");

    // Fetch 1: control marker only, no user records returned
    let recs1 = consumer.fetch().await.expect("fetch 1 succeeds");
    assert!(
        recs1.is_empty(),
        "control record must not be delivered as a data record"
    );
    assert_eq!(
        consumer.position("t", 0).unwrap(),
        1,
        "position must advance to 1 past the control record"
    );
    assert_eq!(
        consumer.fetch_cursor("t", 0).unwrap(),
        1,
        "fetch cursor must advance to 1 past the control record"
    );

    // Fetch 2: data record at offset 1
    let recs2 = consumer.fetch().await.expect("fetch 2 succeeds");
    assert_eq!(recs2.iter().map(|r| r.offset).collect::<Vec<_>>(), vec![1]);
    assert_eq!(
        consumer.position("t", 0).unwrap(),
        2,
        "position must advance to 2 after delivering offset 1"
    );
    assert_eq!(consumer.fetch_cursor("t", 0).unwrap(), 2);

    consumer.close().await.expect("consumer closes cleanly");
    broker.shutdown().await;
}

/// KL03-22: each leader connection negotiates its own Fetch version. A newer
/// bootstrap must not force v17 on an older leader, and an older bootstrap
/// must not cap a newer leader below v17.
#[tokio::test]
async fn fetch_mixed_version_both_bootstrap_leader_orders() {
    let mock = common::Mock::start_two_node().await;
    mock.set_node_api_max(1, FETCH, 17);
    mock.set_node_api_max(2, FETCH, 11);
    mock.set_topic_partitions("t", 1);
    let producer =
        Producer::new(ProducerConfig::bootstrap([mock.addr.clone()]).linger(Duration::ZERO))
            .await
            .unwrap();
    let _ = producer
        .send(ProduceRecord::to("t").value(&b"a"[..]))
        .await
        .unwrap();
    let _ = producer
        .send(ProduceRecord::to("t").value(&b"b"[..]))
        .await
        .unwrap();
    producer.close().await.unwrap();

    // Order 1: newer bootstrap (node 1), older leader (node 2).
    mock.set_partition_leader("t", 0, 2);
    let mut c1 = Consumer::new(ConsumerConfig::bootstrap([mock.addr.clone()]).max_wait_ms(10))
        .await
        .unwrap();
    c1.assign("t", 0, 0).await.unwrap();
    let recs = c1.fetch().await.unwrap();
    assert_eq!(
        recs.iter().map(|r| r.offset).collect::<Vec<_>>(),
        vec![0, 1],
        "fetch from older leader must decode both records"
    );
    assert_eq!(
        mock.last_fetch_version_for_node(2),
        Some(11),
        "newer bootstrap must not force an unsupported Fetch schema on older leader 2"
    );
    c1.close().await.unwrap();

    // Order 2: older bootstrap (node 2), newer leader (node 1).
    mock.set_partition_leader("t", 0, 1);
    let node2_addr = mock.broker_addr(2).expect("node 2 address");
    let mut c2 = Consumer::new(ConsumerConfig::bootstrap([node2_addr]).max_wait_ms(10))
        .await
        .unwrap();
    c2.assign("t", 0, 0).await.unwrap();
    let recs = c2.fetch().await.unwrap();
    assert_eq!(
        recs.iter().map(|r| r.offset).collect::<Vec<_>>(),
        vec![0, 1],
        "fetch from newer leader must decode both records"
    );
    assert_eq!(
        mock.last_fetch_version_for_node(1),
        Some(17),
        "older bootstrap must not cap newer leader 1 below its supported Fetch version"
    );
    c2.close().await.unwrap();
}

/// KL03-22: leader movement and reconnection refresh the peer's Fetch
/// version; topic, epoch and offset fields survive the version change.
#[tokio::test]
async fn fetch_mixed_version_leader_movement_and_reconnect() {
    let mock = common::Mock::start_two_node().await;
    mock.set_node_api_max(1, FETCH, 17);
    mock.set_node_api_max(2, FETCH, 11);
    mock.set_topic_partitions("t", 1);
    let producer =
        Producer::new(ProducerConfig::bootstrap([mock.addr.clone()]).linger(Duration::ZERO))
            .await
            .unwrap();
    for v in [&b"a"[..], &b"b"[..], &b"c"[..]] {
        let _ = producer
            .send(ProduceRecord::to("t").value(v))
            .await
            .unwrap();
    }
    producer.close().await.unwrap();

    mock.set_partition_leader("t", 0, 1);
    let mut consumer =
        Consumer::new(ConsumerConfig::bootstrap([mock.addr.clone()]).max_wait_ms(10))
            .await
            .unwrap();
    consumer.assign("t", 0, 0).await.unwrap();
    let recs = consumer.fetch().await.unwrap();
    assert_eq!(recs.len(), 3);
    assert_eq!(mock.last_fetch_version_for_node(1), Some(17));

    // Leader movement to the older node: the next fetch renegotiates down.
    mock.set_partition_leader("t", 0, 2);
    consumer.seek("t", 0, 0).unwrap();
    let recs = consumer.fetch().await.unwrap();
    assert_eq!(
        recs.iter().map(|r| r.offset).collect::<Vec<_>>(),
        vec![0, 1, 2],
        "records must survive the move to the older leader"
    );
    assert_eq!(
        mock.last_fetch_version_for_node(2),
        Some(11),
        "movement to the older leader must renegotiate Fetch down to v11"
    );

    // Reconnection refreshes capabilities: node 2 upgrades to Fetch v12.
    mock.drop_node_connections(2);
    mock.set_node_api_max(2, FETCH, 12);
    consumer.seek("t", 0, 0).unwrap();
    let recs = consumer.fetch().await.unwrap();
    assert_eq!(recs.len(), 3);
    assert_eq!(
        mock.last_fetch_version_for_node(2),
        Some(12),
        "reconnect must refresh the peer Fetch version to v12"
    );
    consumer.close().await.unwrap();
}

/// KL03-22: one fetch across two leaders speaks each leader's version on the
/// multi-node path and decodes both responses.
#[tokio::test]
async fn fetch_mixed_version_multi_partition_distinct_leaders() {
    let mock = common::Mock::start_two_node().await;
    mock.set_node_api_max(1, FETCH, 17);
    mock.set_node_api_max(2, FETCH, 11);
    mock.set_topic_partitions("t", 2);
    let producer =
        Producer::new(ProducerConfig::bootstrap([mock.addr.clone()]).linger(Duration::ZERO))
            .await
            .unwrap();
    let _ = producer
        .send(ProduceRecord::to("t").partition(0).value(&b"p0"[..]))
        .await
        .unwrap();
    let _ = producer
        .send(ProduceRecord::to("t").partition(1).value(&b"p1"[..]))
        .await
        .unwrap();
    producer.close().await.unwrap();

    mock.set_partition_leader("t", 0, 1);
    mock.set_partition_leader("t", 1, 2);
    let mut consumer =
        Consumer::new(ConsumerConfig::bootstrap([mock.addr.clone()]).max_wait_ms(10))
            .await
            .unwrap();
    consumer.assign("t", 0, 0).await.unwrap();
    consumer.assign("t", 1, 0).await.unwrap();
    let recs = consumer.fetch().await.unwrap();
    let mut got: Vec<(i32, i64)> = recs.iter().map(|r| (r.partition, r.offset)).collect();
    got.sort();
    assert_eq!(got, vec![(0, 0), (1, 0)]);
    assert_eq!(
        mock.last_fetch_version_for_node(1),
        Some(17),
        "newer leader must be fetched at v17"
    );
    assert_eq!(
        mock.last_fetch_version_for_node(2),
        Some(11),
        "older leader must be fetched at v11 in the same call"
    );
    consumer.close().await.unwrap();
}

/// KL03-22: a preferred (follower) replica negotiates its own Fetch version
/// too: the redirect fetch must not reuse the leader's newer schema.
#[tokio::test]
async fn fetch_mixed_version_preferred_replica_uses_replica_version() {
    let mock = common::Mock::start_two_node().await;
    mock.set_node_api_max(1, FETCH, 17);
    mock.set_node_api_max(2, FETCH, 11);
    mock.set_topic_partitions("t", 1);
    let producer =
        Producer::new(ProducerConfig::bootstrap([mock.addr.clone()]).linger(Duration::ZERO))
            .await
            .unwrap();
    let _ = producer
        .send(ProduceRecord::to("t").value(&b"v"[..]))
        .await
        .unwrap();
    producer.close().await.unwrap();

    // Node 1 (rack r1) leads; the r2 consumer is redirected to node 2.
    mock.set_partition_leader("t", 0, 1);
    let mut consumer = Consumer::new(
        ConsumerConfig::bootstrap([mock.addr.clone()])
            .max_wait_ms(10)
            .rack("r2"),
    )
    .await
    .unwrap();
    consumer.assign("t", 0, 0).await.unwrap();
    let recs = consumer.fetch().await.unwrap();
    assert_eq!(
        recs.iter().map(|r| r.offset).collect::<Vec<_>>(),
        vec![0],
        "redirected fetch must return the record"
    );
    assert_eq!(
        mock.last_fetch_version_for_node(1),
        Some(17),
        "leader leg must speak v17"
    );
    assert_eq!(
        mock.last_fetch_version_for_node(2),
        Some(11),
        "preferred-replica leg must negotiate the replica's v11, not the leader's v17"
    );
    consumer.close().await.unwrap();
}

/// A successful response's quota applies to the next request, not delivery of
/// the records already returned by that response.
#[tokio::test]
async fn fetch_throttle_delays_next_request_without_delaying_returned_records() {
    let mock = common::Mock::start().await;
    let producer =
        Producer::new(ProducerConfig::bootstrap([mock.addr.clone()]).linger(Duration::ZERO))
            .await
            .unwrap();
    let first_ack = producer
        .send(ProduceRecord::to("t").value("first"))
        .await
        .unwrap();
    assert_eq!(first_ack.offset, 0);
    mock.set_fetch_throttles(1, [350]);
    let mut consumer = Consumer::new(ConsumerConfig::bootstrap([mock.addr.clone()]))
        .await
        .unwrap();
    consumer.assign("t", 0, 0).await.unwrap();
    let first = consumer.fetch().await.unwrap();
    assert_eq!(first.len(), 1);
    assert_eq!(first[0].offset, 0);
    let second_ack = producer
        .send(ProduceRecord::to("t").value("second"))
        .await
        .unwrap();
    assert_eq!(second_ack.offset, 1);
    let start = std::time::Instant::now();
    let second = consumer.fetch().await.unwrap();
    assert_eq!(second.len(), 1);
    assert_eq!(second[0].offset, 1);
    assert!(
        start.elapsed() >= Duration::from_millis(300),
        "Fetch quota was discarded"
    );
    assert_eq!(mock.fetch_nodes(), [1, 1]);
    consumer.close().await.unwrap();
    producer.close().await.unwrap();
}

#[tokio::test]
async fn fetch_throttle_other_broker_progress_and_legacy_version() {
    let mock = common::Mock::start_two_node().await;
    mock.set_topic_partitions("t", 2);
    mock.set_partition_leader("t", 0, 1);
    mock.set_partition_leader("t", 1, 2);
    mock.set_node_api_max(1, FETCH, 17);
    mock.set_node_api_max(2, FETCH, 7);
    let producer =
        Producer::new(ProducerConfig::bootstrap([mock.addr.clone()]).linger(Duration::ZERO))
            .await
            .unwrap();
    for partition in 0..2 {
        let ack = producer
            .send(ProduceRecord::to("t").partition(partition).value("initial"))
            .await
            .unwrap();
        assert_eq!((ack.partition, ack.offset), (partition, 0));
    }
    mock.set_fetch_throttles(1, [1000]);
    mock.set_fetch_throttles(2, [60_000]);
    let mut consumer = Consumer::new(ConsumerConfig::bootstrap([mock.addr.clone()]))
        .await
        .unwrap();
    consumer
        .assign_many([(("t", 0), 0), (("t", 1), 0)])
        .await
        .unwrap();
    let initial = consumer.fetch().await.unwrap();
    assert_eq!(initial.len(), 2);
    for (partition, value) in [(0, "slow"), (1, "fast")] {
        let ack = producer
            .send(ProduceRecord::to("t").partition(partition).value(value))
            .await
            .unwrap();
        assert_eq!((ack.partition, ack.offset), (partition, 1));
    }
    let start = std::time::Instant::now();
    let fast = tokio::time::timeout(Duration::from_millis(300), consumer.fetch())
        .await
        .expect("an unrelated/legacy broker must remain fetchable")
        .unwrap();
    assert_eq!(
        fast.iter()
            .map(|r| (r.partition, r.offset))
            .collect::<Vec<_>>(),
        [(1, 1)]
    );
    assert_eq!(fast[0].value.as_deref(), Some(b"fast".as_slice()));
    assert_eq!(
        mock.fetch_nodes().iter().filter(|node| **node == 1).count(),
        1
    );
    assert_eq!(consumer.metrics().throttle.responses, 1);
    assert_eq!(consumer.metrics().throttle.requested_millis, 1000);
    consumer.pause([TopicPartition::new("t", 1)]);
    let slow = consumer
        .fetch_timeout(Duration::from_secs(2))
        .await
        .unwrap();
    assert_eq!(
        slow.iter()
            .map(|r| (r.partition, r.offset))
            .collect::<Vec<_>>(),
        [(0, 1)]
    );
    assert_eq!(slow[0].value.as_deref(), Some(b"slow".as_slice()));
    assert!(start.elapsed() >= Duration::from_millis(900));
    assert_eq!(
        consumer.positions(),
        [
            (TopicPartition::new("t", 0), 2),
            (TopicPartition::new("t", 1), 2)
        ]
    );
    consumer.close().await.unwrap();
    producer.close().await.unwrap();
}

#[tokio::test]
async fn fetch_throttle_version_boundary_zero_and_invalid_values() {
    for version in [4, 7, 8, 11, 12, 13, 17] {
        let mock = common::Mock::start().await;
        mock.set_node_api_max(1, FETCH, version);
        let producer =
            Producer::new(ProducerConfig::bootstrap([mock.addr.clone()]).linger(Duration::ZERO))
                .await
                .unwrap();
        let ack = producer
            .send(ProduceRecord::to("t").value("x"))
            .await
            .unwrap();
        assert_eq!(ack.offset, 0);
        mock.set_fetch_throttles(1, [if version < 8 { 60_000 } else { 150 }, 0, -1, i32::MIN]);
        let mut consumer = Consumer::new(ConsumerConfig::bootstrap([mock.addr.clone()]))
            .await
            .unwrap();
        consumer.assign("t", 0, 0).await.unwrap();
        let first = consumer.fetch().await.unwrap();
        assert_eq!(first.len(), 1);
        let start = std::time::Instant::now();
        for _ in 0..3 {
            let records = tokio::time::timeout(Duration::from_secs(1), consumer.fetch())
                .await
                .unwrap()
                .unwrap();
            assert!(records.is_empty());
        }
        assert_eq!(mock.last_fetch_version_for_node(1), Some(version));
        let stats = consumer.metrics().throttle;
        if version >= 8 {
            assert!(start.elapsed() >= Duration::from_millis(100));
            assert_eq!(
                (
                    stats.responses,
                    stats.requested_millis,
                    stats.max_millis,
                    stats.invalid_responses
                ),
                (1, 150, 150, 2)
            );
        } else {
            assert_eq!(stats, partitionline::metrics::ThrottleStats::default());
        }
        assert_eq!(consumer.positions(), [(TopicPartition::new("t", 0), 1)]);
        consumer.close().await.unwrap();
        producer.close().await.unwrap();
    }
}

#[tokio::test]
async fn fetch_throttle_delivers_pending_records_and_bounds_one_shot_wait() {
    let mock = common::Mock::start().await;
    let producer =
        Producer::new(ProducerConfig::bootstrap([mock.addr.clone()]).linger(Duration::ZERO))
            .await
            .unwrap();
    for value in ["a", "b", "c"] {
        let ack = producer
            .send(ProduceRecord::to("t").value(value))
            .await
            .unwrap();
        assert_eq!(ack.partition, 0);
    }
    mock.set_fetch_throttles(1, [i32::MAX]);
    let mut consumer =
        Consumer::new(ConsumerConfig::bootstrap([mock.addr.clone()]).max_poll_records(1))
            .await
            .unwrap();
    consumer.assign("t", 0, 0).await.unwrap();
    for offset in 0..3 {
        let records = tokio::time::timeout(
            Duration::from_millis(300),
            consumer.fetch_timeout(Duration::ZERO),
        )
        .await
        .unwrap()
        .unwrap();
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].offset, offset);
        assert_eq!(
            consumer.positions(),
            [(TopicPartition::new("t", 0), offset + 1)]
        );
    }
    let start = std::time::Instant::now();
    let empty = tokio::time::timeout(
        Duration::from_millis(300),
        consumer.fetch_timeout(Duration::from_millis(40)),
    )
    .await
    .unwrap()
    .unwrap();
    assert!(empty.is_empty());
    assert!(start.elapsed() >= Duration::from_millis(30));
    assert_eq!(mock.fetch_nodes(), [1]);
    assert_eq!(consumer.metrics().records_fetched, 3);
    assert_eq!(
        consumer.metrics().throttle.requested_millis,
        u64::try_from(i32::MAX).unwrap()
    );
    consumer.close().await.unwrap();
    producer.close().await.unwrap();
}

#[tokio::test]
async fn fetch_throttle_wakeup_preserves_positions_and_quota() {
    let mock = common::Mock::start().await;
    let producer =
        Producer::new(ProducerConfig::bootstrap([mock.addr.clone()]).linger(Duration::ZERO))
            .await
            .unwrap();
    let ack = producer
        .send(ProduceRecord::to("t").value("first"))
        .await
        .unwrap();
    assert_eq!(ack.offset, 0);
    mock.set_fetch_throttles(1, [1000]);
    let mut consumer = Consumer::new(ConsumerConfig::bootstrap([mock.addr.clone()]))
        .await
        .unwrap();
    consumer.assign("t", 0, 0).await.unwrap();
    assert_eq!(consumer.fetch().await.unwrap().len(), 1);
    let handle = consumer.wakeup_handle();
    let wake = tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(30)).await;
        handle.wakeup();
    });
    let result = tokio::time::timeout(
        Duration::from_millis(300),
        consumer.fetch_timeout(Duration::from_secs(2)),
    )
    .await
    .unwrap();
    assert!(
        matches!(result, Err(partitionline::Error::Wakeup)),
        "{result:?}"
    );
    wake.await.unwrap();
    assert_eq!(consumer.positions(), [(TopicPartition::new("t", 0), 1)]);
    assert!(consumer
        .fetch_timeout(Duration::ZERO)
        .await
        .unwrap()
        .is_empty());
    assert_eq!(mock.fetch_nodes(), [1]);
    assert_eq!(consumer.metrics().fetch_errors, 0); // Wakeup retains its existing separate outcome.
    consumer.close().await.unwrap();
    producer.close().await.unwrap();
}

#[tokio::test]
async fn fetch_throttle_wait_consumes_request_deadline() {
    let mock = common::Mock::start().await;
    let producer =
        Producer::new(ProducerConfig::bootstrap([mock.addr.clone()]).linger(Duration::ZERO))
            .await
            .unwrap();
    let ack = producer
        .send(ProduceRecord::to("t").value("first"))
        .await
        .unwrap();
    assert_eq!(ack.offset, 0);
    mock.set_fetch_throttles(1, [1000]);
    let mut consumer = Consumer::new(
        ConsumerConfig::bootstrap([mock.addr.clone()])
            .request_timeout(Duration::from_millis(100))
            .max_wait_ms(2000),
    )
    .await
    .unwrap();
    consumer.assign("t", 0, 0).await.unwrap();
    assert_eq!(consumer.fetch().await.unwrap().len(), 1);
    let start = std::time::Instant::now();
    let result = tokio::time::timeout(Duration::from_millis(300), consumer.fetch())
        .await
        .unwrap();
    assert!(
        matches!(result, Err(partitionline::Error::Timeout)),
        "{result:?}"
    );
    assert!(start.elapsed() >= Duration::from_millis(75));
    assert_eq!(mock.fetch_nodes(), [1]);
    assert_eq!(consumer.positions(), [(TopicPartition::new("t", 0), 1)]);
    consumer.close().await.unwrap();
    producer.close().await.unwrap();
}

#[tokio::test]
async fn fetch_throttle_caller_cancellation_does_not_mutate_default_wait() {
    let mock = common::Mock::start().await;
    let producer =
        Producer::new(ProducerConfig::bootstrap([mock.addr.clone()]).linger(Duration::ZERO))
            .await
            .unwrap();
    let ack = producer
        .send(ProduceRecord::to("t").value("first"))
        .await
        .unwrap();
    assert_eq!(ack.offset, 0);
    mock.set_fetch_throttles(1, [350]);
    let mut consumer = Consumer::new(ConsumerConfig::bootstrap([mock.addr.clone()]))
        .await
        .unwrap();
    consumer.assign("t", 0, 0).await.unwrap();
    assert_eq!(consumer.fetch().await.unwrap().len(), 1);
    let cancelled = tokio::time::timeout(
        Duration::from_millis(30),
        consumer.fetch_timeout(Duration::from_millis(40)),
    )
    .await;
    assert!(
        cancelled.is_err(),
        "caller must be able to cancel a quota wait"
    );
    assert_eq!(mock.fetch_nodes(), [1]);
    let ack = producer
        .send(ProduceRecord::to("t").value("second"))
        .await
        .unwrap();
    assert_eq!(ack.offset, 1);
    let next = consumer.fetch().await.unwrap();
    assert_eq!(
        next.len(),
        1,
        "cancelled one-shot wait must not shrink default 500ms wait"
    );
    assert_eq!(next[0].offset, 1);
    assert_eq!(consumer.positions(), [(TopicPartition::new("t", 0), 2)]);
    consumer.close().await.unwrap();
    producer.close().await.unwrap();
}

#[tokio::test]
async fn fetch_throttle_observes_peer_body_discarded_at_buffer_limit() {
    let mock = common::Mock::start_two_node().await;
    mock.set_topic_partitions("t", 3);
    for partition in 0..3 {
        mock.set_partition_leader("t", partition, if partition < 2 { 1 } else { 2 });
    }
    let producer =
        Producer::new(ProducerConfig::bootstrap([mock.addr.clone()]).linger(Duration::ZERO))
            .await
            .unwrap();
    for partition in 0..3 {
        let ack = producer
            .send(
                ProduceRecord::to("t")
                    .partition(partition)
                    .value("oversized"),
            )
            .await
            .unwrap();
        assert_eq!((ack.partition, ack.offset), (partition, 0));
    }
    mock.set_fetch_throttles(2, [1000]);
    let mut consumer =
        Consumer::new(ConsumerConfig::bootstrap([mock.addr.clone()]).buffer_memory(1))
            .await
            .unwrap();
    consumer
        .assign_many([(("t", 0), 0), (("t", 1), 0), (("t", 2), 0)])
        .await
        .unwrap();
    let first = consumer.fetch().await.unwrap();
    assert_eq!(
        first
            .iter()
            .map(|r| (r.partition, r.offset))
            .collect::<Vec<_>>(),
        [(0, 0)]
    );
    assert_eq!(consumer.metrics().throttle.responses, 1);
    let next = consumer.fetch().await.unwrap();
    assert_eq!(
        next.iter()
            .map(|r| (r.partition, r.offset))
            .collect::<Vec<_>>(),
        [(1, 0)]
    );
    assert_eq!(
        mock.fetch_nodes().iter().filter(|node| **node == 2).count(),
        1
    );
    consumer.pause([TopicPartition::new("t", 0), TopicPartition::new("t", 1)]);
    let remaining = consumer
        .fetch_timeout(Duration::from_secs(2))
        .await
        .unwrap();
    assert_eq!(
        remaining
            .iter()
            .map(|r| (r.partition, r.offset))
            .collect::<Vec<_>>(),
        [(2, 0)]
    );
    assert_eq!(
        consumer.positions(),
        [
            (TopicPartition::new("t", 0), 1),
            (TopicPartition::new("t", 1), 1),
            (TopicPartition::new("t", 2), 1)
        ]
    );
    consumer.close().await.unwrap();
    producer.close().await.unwrap();
}

#[tokio::test]
async fn fetch_throttle_preserves_interval_during_idle_reconnection() {
    let mock = common::Mock::start().await;
    let producer =
        Producer::new(ProducerConfig::bootstrap([mock.addr.clone()]).linger(Duration::ZERO))
            .await
            .unwrap();
    let ack = producer
        .send(ProduceRecord::to("t").value("first"))
        .await
        .unwrap();
    assert_eq!(ack.offset, 0);
    mock.set_fetch_throttles(1, [350]);
    let mut consumer = Consumer::new(
        ConsumerConfig::bootstrap([mock.addr.clone()])
            .connections_max_idle(Duration::from_millis(30)),
    )
    .await
    .unwrap();
    consumer.assign("t", 0, 0).await.unwrap();
    assert_eq!(consumer.fetch().await.unwrap().len(), 1);
    mock.set_node_api_max(1, FETCH, 7);
    let ack = producer
        .send(ProduceRecord::to("t").value("second"))
        .await
        .unwrap();
    assert_eq!(ack.offset, 1);
    let start = std::time::Instant::now();
    let next = consumer.fetch().await.unwrap();
    assert_eq!(next.len(), 1);
    assert_eq!(next[0].offset, 1);
    assert!(start.elapsed() >= Duration::from_millis(300));
    assert_eq!(mock.last_fetch_version_for_node(1), Some(7));
    consumer.close().await.unwrap();
    producer.close().await.unwrap();
}

#[tokio::test]
async fn fetch_throttle_interval_starts_at_each_response_completion() {
    let mock = common::Mock::start_two_node().await;
    mock.set_topic_partitions("t", 2);
    mock.set_partition_leader("t", 0, 1);
    mock.set_partition_leader("t", 1, 2);
    let producer =
        Producer::new(ProducerConfig::bootstrap([mock.addr.clone()]).linger(Duration::ZERO))
            .await
            .unwrap();
    for partition in 0..2 {
        let ack = producer
            .send(ProduceRecord::to("t").partition(partition).value("initial"))
            .await
            .unwrap();
        assert_eq!(ack.offset, 0);
    }
    mock.set_fetch_throttles(1, [400]);
    mock.set_fetch_delay_once(2, Duration::from_millis(800));
    let mut consumer = Consumer::new(ConsumerConfig::bootstrap([mock.addr.clone()]))
        .await
        .unwrap();
    consumer
        .assign_many([(("t", 0), 0), (("t", 1), 0)])
        .await
        .unwrap();
    assert_eq!(consumer.fetch().await.unwrap().len(), 2);
    for partition in 0..2 {
        let ack = producer
            .send(ProduceRecord::to("t").partition(partition).value("after"))
            .await
            .unwrap();
        assert_eq!(ack.offset, 1);
    }
    // Broker 1's 400ms quota expired while broker 2's first response waited.
    // Its clock must not restart when the aggregate result is processed.
    let next = consumer.fetch().await.unwrap();
    let mut identities = next
        .iter()
        .map(|r| (r.partition, r.offset))
        .collect::<Vec<_>>();
    identities.sort();
    assert_eq!(identities, [(0, 1), (1, 1)]);
    consumer.close().await.unwrap();
    producer.close().await.unwrap();
}

#[tokio::test]
async fn incremental_fetch_sessions_reduce_unchanged_request_bytes_across_versions() {
    for version in [4, 6, 7, 8, 11, 12, 13, 17] {
        let mock = common::Mock::start().await;
        mock.set_topic_partitions("t", 128);
        mock.set_api_max(FETCH, version);
        mock.enable_fetch_sessions([1]);
        let mut consumer = Consumer::new(ConsumerConfig::bootstrap([mock.addr.clone()]))
            .await
            .unwrap();
        consumer
            .assign_many((0..128).map(|p| (("t", p), 0)))
            .await
            .unwrap();
        for _ in 0..3 {
            assert!(consumer.fetch().await.unwrap().is_empty());
        }
        let requests = mock.fetch_session_requests();
        assert_eq!(requests.len(), 3);
        assert_eq!(
            requests[0]
                .4
                .iter()
                .map(|t| t.partitions.len())
                .sum::<usize>(),
            128
        );
        if version >= 7 {
            assert_eq!(
                requests[0].3,
                partitionline::protocol::fetch::FetchMetadata::INITIAL
            );
            assert!(requests[1].3.session_id() > 0);
            assert_eq!(requests[1].3.epoch(), 1);
            assert_eq!(requests[2].3.epoch(), 2);
            assert_eq!(requests[1].3.session_id(), requests[2].3.session_id());
            assert!(requests[1].4.is_empty() && requests[2].4.is_empty());
            assert!(
                requests[1].2 * 20 < requests[0].2,
                "unchanged request must shrink by over 95%"
            );
        } else {
            for request in &requests {
                assert_eq!(
                    request.3,
                    partitionline::protocol::fetch::FetchMetadata::LEGACY
                );
                assert_eq!(request.4[0].partitions.len(), 128);
                assert_eq!(request.2, requests[0].2);
            }
        }
        if version >= 13 {
            assert!(requests[0].4[0].topic.is_empty());
            assert_ne!(requests[0].4[0].topic_id, [0; 16]);
        } else {
            assert_eq!(requests[0].4[0].topic, "t");
        }
        eprintln!("KL05-06 Fetch v{version}: initial={} bytes unchanged={} bytes partitions=128 epochs={:?}",
            requests[0].2, requests[1].2, requests.iter().map(|r| r.3.epoch()).collect::<Vec<_>>());
        consumer.close().await.unwrap();
    }
}

#[tokio::test]
async fn incremental_fetch_sessions_deliver_new_records_and_only_send_changed_offsets() {
    let mock = common::Mock::start().await;
    mock.set_topic_partitions("t", 4);
    mock.enable_fetch_sessions([1]);
    let mut consumer = Consumer::new(ConsumerConfig::bootstrap([mock.addr.clone()]))
        .await
        .unwrap();
    consumer
        .assign_many((0..4).map(|p| (("t", p), 0)))
        .await
        .unwrap();
    assert!(consumer.fetch().await.unwrap().is_empty());
    let producer =
        Producer::new(ProducerConfig::bootstrap([mock.addr.clone()]).linger(Duration::ZERO))
            .await
            .unwrap();
    let ack = producer
        .send(
            ProduceRecord::to("t")
                .partition(2)
                .key("key")
                .value("value"),
        )
        .await
        .unwrap();
    assert_eq!(ack.offset, 0);
    let records = consumer.fetch().await.unwrap();
    assert_eq!(records.len(), 1);
    assert_eq!((records[0].partition, records[0].offset), (2, 0));
    assert_eq!(records[0].key.as_deref(), Some(b"key".as_slice()));
    assert_eq!(records[0].value.as_deref(), Some(b"value".as_slice()));
    assert!(consumer.fetch().await.unwrap().is_empty());
    assert!(consumer.fetch().await.unwrap().is_empty());
    let requests = mock.fetch_session_requests();
    assert!(
        requests[1].4.is_empty(),
        "new broker records must arrive even with no changed request partitions"
    );
    assert_eq!(requests[2].4.len(), 1);
    assert_eq!(requests[2].4[0].partitions.len(), 1);
    assert_eq!(requests[2].4[0].partitions[0].partition, 2);
    assert_eq!(requests[2].4[0].partitions[0].fetch_offset, 1);
    assert!(requests[3].4.is_empty());
    assert_eq!(consumer.position("t", 2).unwrap(), 1);
    consumer.close().await.unwrap();
    producer.close().await.unwrap();
}

#[tokio::test]
async fn incremental_fetch_sessions_forget_paused_partitions_and_add_resumed_partitions() {
    for version in [7, 12, 13, 17] {
        let mock = common::Mock::start().await;
        mock.set_topic_partitions("t", 4);
        mock.set_api_max(FETCH, version);
        mock.enable_fetch_sessions([1]);
        let mut consumer = Consumer::new(ConsumerConfig::bootstrap([mock.addr.clone()]))
            .await
            .unwrap();
        consumer
            .assign_many((0..4).map(|p| (("t", p), 0)))
            .await
            .unwrap();
        assert!(consumer.fetch().await.unwrap().is_empty());
        consumer.pause([("t", 1)]);
        assert!(consumer.fetch().await.unwrap().is_empty());
        consumer.resume([("t", 1)]);
        assert!(consumer.fetch().await.unwrap().is_empty());
        let requests = mock.fetch_session_requests();
        assert_eq!(requests[1].5.len(), 1);
        assert_eq!(requests[1].5[0].partitions, [1]);
        assert!(requests[1].4.is_empty());
        assert_eq!(requests[2].4[0].partitions.len(), 1);
        assert_eq!(requests[2].4[0].partitions[0].partition, 1);
        assert!(requests[2].5.is_empty());
        if version >= 13 {
            assert_ne!(requests[1].5[0].topic_id, [0; 16]);
        } else {
            assert_eq!(requests[1].5[0].topic, "t");
        }
        consumer.close().await.unwrap();
    }
}

#[tokio::test]
async fn incremental_fetch_sessions_are_broker_local_with_legacy_peer_fallback() {
    let mock = common::Mock::start_two_node().await;
    mock.set_topic_partitions("t", 2);
    mock.set_partition_leader("t", 0, 1);
    mock.set_partition_leader("t", 1, 2);
    mock.set_node_api_max(1, FETCH, 17);
    mock.set_node_api_max(2, FETCH, 6);
    mock.enable_fetch_sessions([1, 2]);
    let mut consumer = Consumer::new(ConsumerConfig::bootstrap([mock.addr.clone()]))
        .await
        .unwrap();
    consumer
        .assign_many([(("t", 0), 0), (("t", 1), 0)])
        .await
        .unwrap();
    assert!(consumer.fetch().await.unwrap().is_empty());
    assert!(consumer.fetch().await.unwrap().is_empty());
    let requests = mock.fetch_session_requests();
    let current: Vec<_> = requests.iter().filter(|r| r.0 == 1).collect();
    let legacy: Vec<_> = requests.iter().filter(|r| r.0 == 2).collect();
    assert_eq!(current.len(), 2);
    assert_eq!(legacy.len(), 2);
    assert_eq!(current[1].3.epoch(), 1);
    assert!(current[1].4.is_empty());
    for request in legacy {
        assert_eq!(
            request.3,
            partitionline::protocol::fetch::FetchMetadata::LEGACY
        );
        assert_eq!(request.4[0].partitions.len(), 1);
    }
    consumer.close().await.unwrap();
}

fn session_fault_body(code: i16, session: i32, topics: &[FetchedTopic]) -> Vec<u8> {
    let mut body = BytesMut::new();
    partitionline::protocol::fetch::encode_fetch_response_with_endpoints(
        &mut body,
        17,
        topics,
        code,
        session,
        &[],
    )
    .unwrap();
    body.to_vec()
}

#[tokio::test]
async fn fetch_session_recovery_top_level_errors_retry_without_applying_poison_data() {
    for code in [
        partitionline::error::FETCH_SESSION_ID_NOT_FOUND,
        partitionline::error::INVALID_FETCH_SESSION_EPOCH,
        partitionline::error::FETCH_SESSION_TOPIC_ID_ERROR,
    ] {
        let mock = common::Mock::start().await;
        mock.enable_fetch_sessions([1]);
        let mut consumer = Consumer::new(
            ConsumerConfig::bootstrap([mock.addr.clone()]).retry_backoff(Duration::from_millis(1)),
        )
        .await
        .unwrap();
        consumer.assign("t", 0, 0).await.unwrap();
        assert!(consumer.fetch().await.unwrap().is_empty());
        let session = mock.fetch_session_id(1).unwrap();
        let producer =
            Producer::new(ProducerConfig::bootstrap([mock.addr.clone()]).linger(Duration::ZERO))
                .await
                .unwrap();
        assert_eq!(
            producer
                .send(ProduceRecord::to("t").partition(0).value("real"))
                .await
                .unwrap()
                .offset,
            0
        );
        let batch = custom_batch(0, &[b"poison"], -1, -1, None, false);
        let mut partition = FetchedPartition::partition_response(0, 0);
        partition.high_watermark = 1;
        partition.last_stable_offset = 1;
        partition.log_start_offset = 0;
        partition.records = vec![batch];
        let topics = [FetchedTopic {
            topic: "t".into(),
            topic_id: mock.topic_id("t"),
            partitions: vec![partition],
        }];
        mock.set_fetch_session_raw_responses(1, [session_fault_body(code, 0, &topics)]);
        let records = consumer.fetch().await.unwrap();
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].offset, 0);
        assert_eq!(records[0].value.as_deref(), Some(b"real".as_slice()));
        assert_eq!(consumer.position("t", 0).unwrap(), 1);
        let requests = mock.fetch_session_requests();
        assert_eq!(requests.len(), 3);
        assert_eq!(
            requests[1].3,
            partitionline::protocol::fetch::FetchMetadata::new(session, 1)
        );
        assert!(requests[2].3.is_full());
        assert_eq!(requests[2].4[0].partitions[0].fetch_offset, 0);
        assert_eq!(consumer.metrics().fetch_errors, 0);
        consumer.close().await.unwrap();
        producer.close().await.unwrap();
    }
}

#[tokio::test]
async fn fetch_session_recovery_broker_restart_recreates_session_without_losing_offsets() {
    let mock = common::Mock::start().await;
    mock.enable_fetch_sessions([1]);
    let mut consumer = Consumer::new(
        ConsumerConfig::bootstrap([mock.addr.clone()]).retry_backoff(Duration::from_millis(1)),
    )
    .await
    .unwrap();
    consumer.assign("t", 0, 0).await.unwrap();
    assert!(consumer.fetch().await.unwrap().is_empty());
    let old = mock.fetch_session_id(1).unwrap();
    mock.reset_fetch_sessions(1);
    let producer =
        Producer::new(ProducerConfig::bootstrap([mock.addr.clone()]).linger(Duration::ZERO))
            .await
            .unwrap();
    assert_eq!(
        producer
            .send(ProduceRecord::to("t").partition(0).value("after-restart"))
            .await
            .unwrap()
            .offset,
        0
    );
    let rows = consumer.fetch().await.unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].offset, 0);
    assert_eq!(rows[0].value.as_deref(), Some(b"after-restart".as_slice()));
    assert_ne!(mock.fetch_session_id(1).unwrap(), old);
    let requests = mock.fetch_session_requests();
    assert_eq!(requests.len(), 3);
    assert_eq!(
        requests[2].3,
        partitionline::protocol::fetch::FetchMetadata::INITIAL
    );
    consumer.close().await.unwrap();
    assert_eq!(mock.remembered_fetch_partitions(1), 0);
    assert_eq!(mock.fetch_session_requests().last().unwrap().3.epoch(), -1);
    producer.close().await.unwrap();
}

#[tokio::test]
async fn fetch_session_recovery_recreated_topic_reassignment_discards_previous_id_buffer() {
    let mock = common::Mock::start().await;
    mock.enable_fetch_sessions([1]);
    let producer =
        Producer::new(ProducerConfig::bootstrap([mock.addr.clone()]).linger(Duration::ZERO))
            .await
            .unwrap();
    for (offset, value) in ["old-0", "old-1"].into_iter().enumerate() {
        assert_eq!(
            producer
                .send(ProduceRecord::to("t").partition(0).value(value))
                .await
                .unwrap()
                .offset,
            offset as i64
        );
    }
    let mut consumer =
        Consumer::new(ConsumerConfig::bootstrap([mock.addr.clone()]).max_poll_records(1))
            .await
            .unwrap();
    consumer.assign("t", 0, 0).await.unwrap();
    let first = consumer.fetch().await.unwrap();
    assert_eq!(first[0].value.as_deref(), Some(b"old-0".as_slice()));
    mock.recreate_topic_id("t", [7; 16]);
    let new_producer =
        Producer::new(ProducerConfig::bootstrap([mock.addr.clone()]).linger(Duration::ZERO))
            .await
            .unwrap();
    for (offset, value) in ["new-0", "new-1", "new-2"].into_iter().enumerate() {
        assert_eq!(
            new_producer
                .send(ProduceRecord::to("t").partition(0).value(value))
                .await
                .unwrap()
                .offset,
            offset as i64
        );
    }
    consumer.assign_many([(("t", 0), 2)]).await.unwrap();
    let rows = consumer.fetch().await.unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].offset, 2);
    assert_eq!(rows[0].value.as_deref(), Some(b"new-2".as_slice()));
    assert_eq!(consumer.position("t", 0).unwrap(), 3);
    consumer.close().await.unwrap();
    producer.close().await.unwrap();
    new_producer.close().await.unwrap();
}

#[tokio::test]
async fn fetch_session_recovery_rejects_incomplete_full_and_extra_incremental_partitions() {
    for malformed in ["missing-full", "extra", "duplicate", "unknown-topic-id"] {
        let mock = common::Mock::start().await;
        mock.enable_fetch_sessions([1]);
        let mut consumer = Consumer::new(
            ConsumerConfig::bootstrap([mock.addr.clone()]).retry_backoff(Duration::from_millis(1)),
        )
        .await
        .unwrap();
        consumer.assign("t", 0, 0).await.unwrap();
        let session = if malformed == "missing-full" {
            91
        } else {
            assert!(consumer.fetch().await.unwrap().is_empty());
            mock.fetch_session_id(1).unwrap()
        };
        let topics = match malformed {
            "missing-full" => vec![],
            _ => vec![FetchedTopic {
                topic: "t".into(),
                topic_id: if malformed == "unknown-topic-id" {
                    [9; 16]
                } else {
                    mock.topic_id("t")
                },
                partitions: if malformed == "duplicate" {
                    vec![FetchedPartition::partition_response(0, 0); 2]
                } else {
                    vec![FetchedPartition::partition_response(
                        if malformed == "extra" { 3 } else { 0 },
                        0,
                    )]
                },
            }],
        };
        mock.set_fetch_session_raw_responses(1, [session_fault_body(0, session, &topics)]);
        assert!(consumer.fetch().await.unwrap().is_empty(), "{malformed}");
        assert_eq!(consumer.position("t", 0).unwrap(), 0);
        let requests = mock.fetch_session_requests();
        assert!(requests.last().unwrap().3.is_full(), "{malformed}");
        assert_eq!(requests.last().unwrap().4[0].partitions[0].fetch_offset, 0);
        consumer.close().await.unwrap();
    }
}

#[tokio::test]
async fn fetch_session_recovery_truncated_or_wrong_identity_response_forces_next_full_request() {
    for invalid in ["truncated", "negative-id", "changed-id"] {
        let mock = common::Mock::start().await;
        mock.enable_fetch_sessions([1]);
        let mut consumer = Consumer::new(ConsumerConfig::bootstrap([mock.addr.clone()]))
            .await
            .unwrap();
        consumer.assign("t", 0, 0).await.unwrap();
        assert!(consumer.fetch().await.unwrap().is_empty());
        let session = mock.fetch_session_id(1).unwrap();
        let raw = match invalid {
            "truncated" => vec![0; 8],
            "negative-id" => session_fault_body(0, -1, &[]),
            _ => session_fault_body(0, session + 1, &[]),
        };
        mock.set_fetch_session_raw_responses(1, [raw]);
        assert!(
            matches!(
                consumer.fetch().await,
                Err(partitionline::Error::Protocol(_))
            ),
            "{invalid}"
        );
        assert_eq!(consumer.position("t", 0).unwrap(), 0);
        assert!(consumer.fetch().await.unwrap().is_empty());
        assert!(mock.fetch_session_requests().last().unwrap().3.is_full());
        consumer.close().await.unwrap();
    }
}

#[tokio::test]
async fn fetch_session_recovery_repeated_errors_are_bounded_by_original_request_deadline() {
    let mock = common::Mock::start().await;
    mock.enable_fetch_sessions([1]);
    let mut consumer = Consumer::new(
        ConsumerConfig::bootstrap([mock.addr.clone()])
            .request_timeout(Duration::from_millis(100))
            .retry_backoff(Duration::from_millis(20)),
    )
    .await
    .unwrap();
    consumer.assign("t", 0, 0).await.unwrap();
    assert!(consumer.fetch().await.unwrap().is_empty());
    mock.set_fetch_session_raw_responses(1, (0..64).map(|_| session_fault_body(70, 0, &[])));
    let started = std::time::Instant::now();
    let result = tokio::time::timeout(Duration::from_millis(500), consumer.fetch())
        .await
        .unwrap();
    assert!(
        matches!(result, Err(partitionline::Error::Timeout)),
        "{result:?}"
    );
    assert!(started.elapsed() >= Duration::from_millis(80));
    assert!(started.elapsed() < Duration::from_millis(400));
    assert_eq!(consumer.position("t", 0).unwrap(), 0);
    assert!((2..=7).contains(&mock.fetch_session_requests().len()));
    consumer.close_timeout(Duration::ZERO).await.unwrap();
}

#[tokio::test]
async fn fetch_session_recovery_peer_error_preserves_healthy_records_exactly_once() {
    let mock = common::Mock::start_two_node().await;
    mock.set_topic_partitions("t", 2);
    for p in 0..2 {
        mock.set_partition_leader("t", p, p + 1);
    }
    mock.enable_fetch_sessions([1, 2]);
    let mut consumer = Consumer::new(
        ConsumerConfig::bootstrap([mock.addr.clone()]).retry_backoff(Duration::from_millis(1)),
    )
    .await
    .unwrap();
    consumer
        .assign_many((0..2).map(|p| (("t", p), 0)))
        .await
        .unwrap();
    assert!(consumer.fetch().await.unwrap().is_empty());
    let producer =
        Producer::new(ProducerConfig::bootstrap([mock.addr.clone()]).linger(Duration::ZERO))
            .await
            .unwrap();
    for p in 0..2 {
        assert_eq!(
            producer
                .send(
                    ProduceRecord::to("t")
                        .partition(p)
                        .value(format!("peer-{p}"))
                )
                .await
                .unwrap()
                .offset,
            0
        );
    }
    mock.set_fetch_session_raw_responses(2, [session_fault_body(71, 0, &[])]);
    let rows = consumer.fetch().await.unwrap();
    let mut identities = rows
        .iter()
        .map(|r| (r.partition, r.offset, r.value.clone()))
        .collect::<Vec<_>>();
    identities.sort();
    assert_eq!(identities.len(), 2);
    for (p, identity) in identities.iter().enumerate() {
        assert_eq!((identity.0, identity.1), (p as i32, 0));
        assert_eq!(identity.2.as_deref(), Some(format!("peer-{p}").as_bytes()));
        assert_eq!(consumer.position("t", p as i32).unwrap(), 1);
    }
    assert!(consumer.fetch().await.unwrap().is_empty());
    assert_eq!(consumer.metrics().records_fetched, 2);
    consumer.close().await.unwrap();
    producer.close().await.unwrap();
}

#[tokio::test]
async fn fetch_session_recovery_pause_all_unassign_and_leader_move_retire_server_state() {
    let mock = common::Mock::start_two_node().await;
    mock.set_topic_partitions("t", 2);
    mock.enable_fetch_sessions([1, 2]);
    mock.set_partition_leader("t", 0, 1);
    mock.set_partition_leader("t", 1, 1);
    let mut consumer = Consumer::new(ConsumerConfig::bootstrap([mock.addr.clone()]))
        .await
        .unwrap();
    consumer
        .assign_many((0..2).map(|p| (("t", p), 0)))
        .await
        .unwrap();
    assert!(consumer.fetch().await.unwrap().is_empty());
    assert_eq!(mock.remembered_fetch_partitions(1), 2);
    consumer.pause([("t", 0), ("t", 1)]);
    assert!(consumer.fetch().await.unwrap().is_empty());
    assert_eq!(mock.remembered_fetch_partitions(1), 0);
    consumer.resume([("t", 0), ("t", 1)]);
    assert!(consumer.fetch().await.unwrap().is_empty());
    assert_eq!(mock.remembered_fetch_partitions(1), 2);
    mock.set_partition_leader("t", 0, 2);
    consumer.assign_many([(("t", 0), 0)]).await.unwrap();
    assert!(consumer.fetch().await.unwrap().is_empty());
    assert_eq!(mock.remembered_fetch_partitions(1), 0);
    assert_eq!(mock.remembered_fetch_partitions(2), 1);
    consumer.unassign();
    consumer.assign("t", 1, 0).await.unwrap();
    assert!(consumer.fetch().await.unwrap().is_empty());
    assert_eq!(mock.remembered_fetch_partitions(2), 0);
    assert_eq!(mock.remembered_fetch_partitions(1), 1);
    consumer.close().await.unwrap();
    assert_eq!(mock.remembered_fetch_partitions(1), 0);
}

#[tokio::test]
async fn fetch_session_recovery_cancelled_round_and_wakeup_recreate_without_position_advance() {
    for wakeup in [false, true] {
        let mock = common::Mock::start().await;
        mock.enable_fetch_sessions([1]);
        let mut consumer = Consumer::new(ConsumerConfig::bootstrap([mock.addr.clone()]))
            .await
            .unwrap();
        consumer.assign("t", 0, 0).await.unwrap();
        assert!(consumer.fetch().await.unwrap().is_empty());
        mock.set_fetch_delay_once(1, Duration::from_millis(500));
        if wakeup {
            let handle = consumer.wakeup_handle();
            let wake = tokio::spawn(async move {
                tokio::time::sleep(Duration::from_millis(20)).await;
                handle.wakeup();
            });
            assert!(matches!(
                consumer.fetch().await,
                Err(partitionline::Error::Wakeup)
            ));
            wake.await.unwrap();
        } else {
            assert!(
                tokio::time::timeout(Duration::from_millis(20), consumer.fetch())
                    .await
                    .is_err()
            );
        }
        assert_eq!(consumer.position("t", 0).unwrap(), 0);
        assert!(consumer.fetch().await.unwrap().is_empty());
        let requests = mock.fetch_session_requests();
        assert!(requests.last().unwrap().3.is_full());
        assert_eq!(requests.last().unwrap().4[0].partitions[0].fetch_offset, 0);
        consumer.close().await.unwrap();
    }
}

#[tokio::test]
async fn fetch_session_recovery_close_uses_one_budget_across_stalled_brokers_and_zero_is_immediate()
{
    for budget in [Duration::ZERO, Duration::from_millis(50)] {
        let mock = common::Mock::start_two_node().await;
        mock.set_topic_partitions("t", 2);
        for p in 0..2 {
            mock.set_partition_leader("t", p, p + 1);
        }
        mock.enable_fetch_sessions([1, 2]);
        let mut consumer = Consumer::new(ConsumerConfig::bootstrap([mock.addr.clone()]))
            .await
            .unwrap();
        consumer
            .assign_many((0..2).map(|p| (("t", p), 0)))
            .await
            .unwrap();
        assert!(consumer.fetch().await.unwrap().is_empty());
        for node in [1, 2] {
            mock.set_fetch_delay_once(node, Duration::from_millis(500));
        }
        let before = mock.fetch_session_requests().len();
        let start = std::time::Instant::now();
        tokio::time::timeout(Duration::from_millis(200), consumer.close_timeout(budget))
            .await
            .unwrap()
            .unwrap();
        assert!(start.elapsed() < Duration::from_millis(150));
        if budget.is_zero() {
            assert_eq!(mock.fetch_session_requests().len(), before);
        }
    }
}

#[tokio::test]
async fn fetch_session_recovery_group_close_retires_session_without_automatic_offset_commit() {
    let mock = common::Mock::start().await;
    mock.enable_fetch_sessions([1]);
    let mut group = partitionline::ConsumerGroup::join(
        ConsumerConfig::bootstrap([mock.addr.clone()])
            .auto_commit(true)
            .max_wait_ms(10),
        "session-retirement",
        "t",
    )
    .await
    .unwrap();
    assert!(group.poll().await.unwrap().is_empty());
    assert!(mock.remembered_fetch_partitions(1) > 0);
    let commits = mock.offset_commit_calls();
    group.close().await.unwrap();
    assert_eq!(mock.remembered_fetch_partitions(1), 0);
    assert_eq!(mock.offset_commit_calls(), commits);
    assert_eq!(mock.fetch_session_requests().last().unwrap().3.epoch(), -1);
}

#[tokio::test]
async fn fetch_session_recovery_empty_throttled_full_response_declines_session_until_quota_expires()
{
    let mock = common::Mock::start().await;
    mock.enable_fetch_sessions([1]);
    let mut consumer = Consumer::new(ConsumerConfig::bootstrap([mock.addr.clone()]))
        .await
        .unwrap();
    consumer.assign("t", 0, 0).await.unwrap();
    let mut raw = session_fault_body(0, 0, &[]);
    raw[..4].copy_from_slice(&80_i32.to_be_bytes());
    mock.set_fetch_session_raw_responses(1, [raw]);
    assert!(consumer.fetch().await.unwrap().is_empty());
    let started = std::time::Instant::now();
    assert!(consumer.fetch().await.unwrap().is_empty());
    assert!(started.elapsed() >= Duration::from_millis(60));
    assert_eq!(
        mock.fetch_session_requests().last().unwrap().3,
        partitionline::protocol::fetch::FetchMetadata::INITIAL
    );
    assert_eq!(consumer.position("t", 0).unwrap(), 0);
    consumer.close().await.unwrap();
}

struct LiveFetchObservation {
    phase: usize,
    version: i16,
    session: i32,
    epoch: i32,
    changed: Vec<(i32, i64)>,
    forgotten: Vec<i32>,
    response_session: i32,
    response_error: i16,
    request_bytes: usize,
}

async fn observed_frame(stream: &mut TcpStream) -> std::io::Result<Vec<u8>> {
    let size = stream.read_i32().await?;
    assert!((0..=64 * 1024 * 1024).contains(&size));
    let mut frame = vec![0; size as usize];
    let _read = stream.read_exact(&mut frame).await?;
    Ok(frame)
}

async fn forward_observed_frame(stream: &mut TcpStream, frame: &[u8]) -> std::io::Result<()> {
    stream.write_i32(frame.len() as i32).await?;
    stream.write_all(frame).await?;
    Ok(())
}

async fn observe_fetch_connection(
    mut client: TcpStream,
    backend: String,
    phase: Arc<AtomicUsize>,
    history: Arc<parking_lot::Mutex<Vec<LiveFetchObservation>>>,
) {
    let mut broker = TcpStream::connect(backend).await.unwrap();
    loop {
        let request = match observed_frame(&mut client).await {
            Ok(frame) => frame,
            Err(_) => return,
        };
        let at_phase = phase.load(Ordering::SeqCst);
        let mut body = request.as_slice();
        let header = decode_request_header(&mut body).unwrap();
        let fetch = if header.api_key == FETCH {
            let size = body.len();
            let (_, _, topics, _, metadata, forgotten, ..) =
                decode_fetch_request(&mut body, header.api_version).unwrap();
            assert!(body.is_empty());
            Some((
                size,
                metadata,
                topics
                    .into_iter()
                    .flat_map(|t| {
                        t.partitions
                            .into_iter()
                            .map(|p| (p.partition, p.fetch_offset))
                    })
                    .collect::<Vec<_>>(),
                forgotten
                    .into_iter()
                    .flat_map(|t| t.partitions)
                    .collect::<Vec<_>>(),
            ))
        } else {
            None
        };
        if forward_observed_frame(&mut broker, &request).await.is_err() {
            return;
        }
        let response = match observed_frame(&mut broker).await {
            Ok(frame) => frame,
            Err(_) => return,
        };
        if let Some((size, metadata, mut changed, mut forgotten)) = fetch {
            let mut body = response.as_slice();
            let response_header =
                decode_response_header(&mut body, FETCH, header.api_version).unwrap();
            assert_eq!(response_header.correlation_id, header.correlation_id);
            let (_, _, error, session, _) = partitionline::protocol::fetch::decode_fetch_response(
                &mut body,
                header.api_version,
            )
            .unwrap();
            assert!(body.is_empty());
            changed.sort_unstable();
            forgotten.sort_unstable();
            history.lock().push(LiveFetchObservation {
                phase: at_phase,
                version: header.api_version,
                session: metadata.session_id(),
                epoch: metadata.epoch(),
                changed,
                forgotten,
                response_session: session,
                response_error: error,
                request_bytes: size,
            });
        }
        if forward_observed_frame(&mut client, &response)
            .await
            .is_err()
        {
            return;
        }
    }
}

async fn live_session_records(
    consumer: &mut Consumer,
    topic: &str,
    phase: usize,
    expected: &[(i32, i64)],
) {
    let rows = tokio::time::timeout(Duration::from_secs(8), async {
        let mut rows = Vec::new();
        while rows.len() < expected.len() {
            rows.extend(consumer.fetch().await.unwrap());
        }
        rows
    })
    .await
    .unwrap();
    let mut identities = Vec::new();
    for row in &rows {
        let key = String::from_utf8(row.key.clone().unwrap().to_vec()).unwrap();
        let value = String::from_utf8(row.value.clone().unwrap().to_vec()).unwrap();
        assert_eq!(row.topic, topic);
        assert_eq!(key, row.partition.to_string());
        assert_eq!(value, format!("KL05-07/{}/{}", row.partition, row.offset));
        identities.push((row.partition, row.offset));
        println!(
            "PL_FETCH_RECORD\t{phase}\t{}\t{}\t{key}\t{value}",
            row.partition, row.offset
        );
    }
    identities.sort_unstable();
    assert_eq!(identities, expected);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires the owned digest-pinned KL05-07 broker and explicit observer ports"]
async fn live_fetch_session_recovery_required() {
    assert_eq!(std::env::var("REQUIRE_BROKER").unwrap(), "1");
    let topic = std::env::var("PL_FETCH_TOPIC").unwrap();
    let source = std::env::var("PL_FETCH_SOURCE_SHA").unwrap();
    let bootstrap = std::env::var("PL_FETCH_PROXY").unwrap();
    let backend = std::env::var("PL_FETCH_BACKEND").unwrap();
    assert!(topic.starts_with("plfetch-recovery-"));
    assert!(bootstrap.starts_with("127.0.0.1:"));
    assert!(backend.starts_with("127.0.0.1:"));
    let listener = TcpListener::bind(&bootstrap).await.unwrap();
    let phase = Arc::new(AtomicUsize::new(0));
    let history = Arc::new(parking_lot::Mutex::new(Vec::new()));
    let (stop, mut stopping) = oneshot::channel();
    let proxy_phase = phase.clone();
    let proxy_history = history.clone();
    let proxy = tokio::spawn(async move {
        let mut tasks = JoinSet::new();
        loop {
            tokio::select! {
                _ = &mut stopping => break,
                accepted = listener.accept() => {
                    let (client, _) = accepted.unwrap();
                    let _task = tasks.spawn(observe_fetch_connection(client, backend.clone(), proxy_phase.clone(), proxy_history.clone()));
                }
                Some(result) = tasks.join_next(), if !tasks.is_empty() => result.unwrap(),
            }
        }
        tasks.abort_all();
        while let Some(result) = tasks.join_next().await {
            if let Err(error) = result {
                assert!(error.is_cancelled(), "{error}");
            }
        }
    });
    let producer =
        Producer::new(ProducerConfig::bootstrap([bootstrap.clone()]).linger(Duration::ZERO))
            .await
            .unwrap();
    for p in 0..32 {
        let value = format!("KL05-07/{p}/0");
        let ack = producer
            .send(
                ProduceRecord::to(topic.as_str())
                    .partition(p)
                    .key(p.to_string())
                    .value(value.clone()),
            )
            .await
            .unwrap();
        assert_eq!((ack.partition, ack.offset), (p, 0));
        println!("PL_FETCH_ACK\t{p}\t0\t{p}\t{value}");
    }
    let mut consumer = Consumer::new(
        ConsumerConfig::bootstrap([bootstrap])
            .connections_max_idle(Duration::from_secs(10))
            .request_timeout(Duration::from_secs(3))
            .max_wait_ms(100)
            .max_poll_records(1000),
    )
    .await
    .unwrap();
    consumer
        .assign_many((0..32).map(|p| ((topic.as_str(), p), 0)))
        .await
        .unwrap();
    live_session_records(
        &mut consumer,
        &topic,
        0,
        &(0..32).map(|p| (p, 0)).collect::<Vec<_>>(),
    )
    .await;
    phase.store(1, Ordering::SeqCst);
    assert!(consumer.fetch().await.unwrap().is_empty());
    phase.store(2, Ordering::SeqCst);
    assert!(consumer.fetch().await.unwrap().is_empty());
    phase.store(3, Ordering::SeqCst);
    consumer.pause((16..32).map(|p| (topic.as_str(), p)));
    for p in 0..32 {
        let value = format!("KL05-07/{p}/1");
        let ack = producer
            .send(
                ProduceRecord::to(topic.as_str())
                    .partition(p)
                    .key(p.to_string())
                    .value(value.clone()),
            )
            .await
            .unwrap();
        assert_eq!((ack.partition, ack.offset), (p, 1));
        println!("PL_FETCH_ACK\t{p}\t1\t{p}\t{value}");
    }
    live_session_records(
        &mut consumer,
        &topic,
        3,
        &(0..16).map(|p| (p, 1)).collect::<Vec<_>>(),
    )
    .await;
    for p in 16..32 {
        assert_eq!(consumer.position(&topic, p).unwrap(), 1);
    }
    phase.store(4, Ordering::SeqCst);
    consumer.resume((16..32).map(|p| (topic.as_str(), p)));
    live_session_records(
        &mut consumer,
        &topic,
        4,
        &(16..32).map(|p| (p, 1)).collect::<Vec<_>>(),
    )
    .await;
    for p in 0..32 {
        assert_eq!(consumer.position(&topic, p).unwrap(), 2);
    }
    phase.store(5, Ordering::SeqCst);
    tokio::time::sleep(Duration::from_secs(11)).await;
    let value = "KL05-07/0/2";
    let ack = producer
        .send(
            ProduceRecord::to(topic.as_str())
                .partition(0)
                .key("0")
                .value(value),
        )
        .await
        .unwrap();
    assert_eq!((ack.partition, ack.offset), (0, 2));
    println!("PL_FETCH_ACK\t0\t2\t0\t{value}");
    live_session_records(&mut consumer, &topic, 5, &[(0, 2)]).await;
    for p in 0..32 {
        println!(
            "PL_FETCH_POSITION\t{p}\t{}",
            consumer.position(&topic, p).unwrap()
        );
    }
    phase.store(6, Ordering::SeqCst);
    consumer
        .close_timeout(Duration::from_secs(2))
        .await
        .unwrap();
    producer.close().await.unwrap();
    stop.send(()).unwrap();
    proxy.await.unwrap();
    for row in history.lock().iter() {
        let changed = row
            .changed
            .iter()
            .map(|(p, o)| format!("{p}:{o}"))
            .collect::<Vec<_>>()
            .join(",");
        let forgotten = row
            .forgotten
            .iter()
            .map(i32::to_string)
            .collect::<Vec<_>>()
            .join(",");
        println!(
            "PL_FETCH_WIRE\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{changed}\t{forgotten}",
            row.phase,
            row.version,
            row.session,
            row.epoch,
            row.response_session,
            row.response_error,
            row.request_bytes
        );
    }
    println!("PL_FETCH_SOURCE\t{source}");
    println!("PL_FETCH_TOPIC\t{topic}");
    println!("PL_FETCH_COMPLETE");
}

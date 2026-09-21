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

#[path = "common/fetch_fixture.rs"]
mod fetch_fixture;

use bytes::BytesMut;
use partitionline::error::{NOT_LEADER_OR_FOLLOWER, OFFSET_OUT_OF_RANGE};
use partitionline::protocol::api_keys::API_VERSIONS;
use partitionline::protocol::fetch::{encode_fetch_response, FetchPartition, FetchTopic};
use partitionline::protocol::header::{
    decode_response_header, encode_request_header, RequestHeader,
};
use partitionline::Consumer;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

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

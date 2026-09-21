//! KL02-08: Bounded consumer fetch, decode, and pending-record memory budget.
//!
//! Acceptance criteria:
//! - Slow processing and `max_poll_records` cannot grow unbounded prefetched
//!   memory across partitions and brokers.
//! - Preserve progress for a valid first batch larger than the soft fetch limit,
//!   with the existing 64 MiB hard decode ceiling from KL02-03.
//! - Pause, resume, seek, and drop release or retain buffers per
//!   docs/resource-contract.md and must not commit undelivered data.
//! - Do not regress KL03-02 through KL03-07.
//! - No unsafe code. No new dependencies. Do not claim an RSS bound.

#![expect(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::let_underscore_must_use,
    clippy::cast_possible_truncation,
    clippy::cast_possible_wrap,
    clippy::type_complexity,
    clippy::unnecessary_map_or,
    clippy::allow_attributes_without_reason,
    reason = "integration tests use test assertions and unwrap on mock sockets"
)]

#[path = "common/fetch_fixture.rs"]
mod fetch_fixture;

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use bytes::{Bytes, BytesMut};
use partitionline::protocol::api::{
    encode_api_versions_response, encode_metadata_response, ApiVersion, ApiVersionsResponse,
    Broker, MetadataResponse, PartitionMetadata, TopicMetadata,
};
use partitionline::protocol::api_keys::{API_VERSIONS, FETCH, METADATA};
use partitionline::protocol::fetch::{
    decode_fetch_request, encode_fetch_response, FetchTopic, FetchedPartition, FetchedTopic,
};
use partitionline::protocol::header::{decode_request_header, encode_response_header};
use partitionline::protocol::records::{
    Record, RecordBatch, DEFAULT_MAX_RECORD_BATCH_DECODE_BYTES,
};
use partitionline::{Consumer, ConsumerConfig, TopicPartition};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::sync::oneshot;
use tokio::task::{JoinHandle, JoinSet};

struct MultiNodeCluster {
    addr0: String,
    shutdown_txs: Vec<oneshot::Sender<()>>,
    tasks: Vec<JoinHandle<()>>,
}

impl MultiNodeCluster {
    fn config(&self) -> ConsumerConfig {
        ConsumerConfig::bootstrap([self.addr0.clone()])
            .request_timeout(Duration::from_secs(2))
            .retry_backoff(Duration::from_millis(1))
    }

    async fn shutdown(&mut self) {
        for tx in self.shutdown_txs.drain(..) {
            let _ = tx.send(());
        }
        for task in self.tasks.drain(..) {
            let _ = task.await;
        }
    }
}

async fn spawn_two_broker_cluster<F0, F1>(handler0: F0, handler1: F1) -> MultiNodeCluster
where
    F0: Fn(&[FetchTopic], usize) -> Option<Vec<FetchedTopic>> + Send + Sync + 'static,
    F1: Fn(&[FetchTopic], usize) -> Option<Vec<FetchedTopic>> + Send + Sync + 'static,
{
    spawn_cluster(0, 1, handler0, handler1).await
}

async fn spawn_cluster<F0, F1>(
    leader0: i32,
    leader1: i32,
    handler0: F0,
    handler1: F1,
) -> MultiNodeCluster
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

    let task0 = spawn_mock_broker(
        listener0,
        rx0,
        port0,
        port1,
        leader0,
        leader1,
        Arc::new(handler0),
    );

    let task1 = spawn_mock_broker(
        listener1,
        rx1,
        port0,
        port1,
        leader0,
        leader1,
        Arc::new(handler1),
    );

    MultiNodeCluster {
        addr0,
        shutdown_txs: vec![tx0, tx1],
        tasks: vec![task0, task1],
    }
}

fn spawn_mock_broker<F>(
    listener: TcpListener,
    mut shutdown_rx: oneshot::Receiver<()>,
    port0: u16,
    port1: u16,
    leader0: i32,
    leader1: i32,
    handler: Arc<F>,
) -> JoinHandle<()>
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
                            if size > 64 * 1024 * 1024 {
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
                                        PartitionMetadata::new(0, 1, Some(leader1), Some(0), vec![1], vec![1], Vec::new()),
                                    ];
                                    let metadata = MetadataResponse {
                                        throttle_time_ms: 0,
                                        brokers: vec![
                                            Broker::new(0, "127.0.0.1", i32::from(port0), None),
                                            Broker::new(1, "127.0.0.1", i32::from(port1), None),
                                        ],
                                        cluster_id: Some("budget-cluster".into()),
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
                                        None => return,
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

fn build_batch_with_record_sizes(
    base_offset: i64,
    count: usize,
    record_size: usize,
) -> RecordBatch {
    let payload = vec![b'v'; record_size];
    let records = (0..count)
        .map(|_| Record {
            offset: 0,
            timestamp: 0,
            key: None,
            value: Some(Bytes::copy_from_slice(&payload)),
            headers: Vec::new(),
        })
        .collect();
    let mut batch = RecordBatch::from_records(records);
    batch.base_offset = base_offset;
    batch
}

/// 1. Slow processing and max_poll_records cannot grow unbounded prefetched memory
/// across partitions and brokers.
#[tokio::test]
async fn slow_processing_and_max_poll_records_bounds_memory_across_brokers() {
    // Two brokers:
    // Node 0 leads partition 0. It serves 5 records of 400 bytes each (~2000 bytes).
    // Node 1 leads partition 1. It serves 5 records of 400 bytes each (~2000 bytes).
    // Budget is 2500 bytes.
    // max_poll_records is 2.
    let mut cluster = spawn_two_broker_cluster(
        |topics, _attempt| {
            // Node 0: serves partition 0 if requested
            let p0 = topics
                .iter()
                .find(|t| t.topic == "t")?
                .partitions
                .iter()
                .find(|p| p.partition == 0)?;
            if p0.fetch_offset == 0 {
                let mut part = FetchedPartition::partition_response(0, 0);
                part.high_watermark = 5;
                part.records = vec![build_batch_with_record_sizes(0, 5, 400)];
                Some(vec![FetchedTopic {
                    topic: "t".to_string(),
                    topic_id: [0u8; 16],
                    partitions: vec![part],
                }])
            } else {
                let mut part = FetchedPartition::partition_response(0, 0);
                part.high_watermark = 5;
                Some(vec![FetchedTopic {
                    topic: "t".to_string(),
                    topic_id: [0u8; 16],
                    partitions: vec![part],
                }])
            }
        },
        |topics, _attempt| {
            // Node 1: serves partition 1 if requested
            let p1 = topics
                .iter()
                .find(|t| t.topic == "t")?
                .partitions
                .iter()
                .find(|p| p.partition == 1)?;
            if p1.fetch_offset == 0 {
                let mut part = FetchedPartition::partition_response(1, 0);
                part.high_watermark = 5;
                part.records = vec![build_batch_with_record_sizes(0, 5, 400)];
                Some(vec![FetchedTopic {
                    topic: "t".to_string(),
                    topic_id: [0u8; 16],
                    partitions: vec![part],
                }])
            } else {
                let mut part = FetchedPartition::partition_response(1, 0);
                part.high_watermark = 5;
                Some(vec![FetchedTopic {
                    topic: "t".to_string(),
                    topic_id: [0u8; 16],
                    partitions: vec![part],
                }])
            }
        },
    )
    .await;

    let cfg = cluster.config().max_poll_records(2).buffer_memory(2500);

    let mut consumer = Consumer::new(cfg).await.expect("consumer starts");
    consumer
        .assign_many([(("t", 0), 0), (("t", 1), 0)])
        .await
        .expect("assign partitions 0 and 1");

    // Fetch 1: Node 0 returns 5 records (2000 bytes).
    // Node 1 would add another 2000 bytes (total 4000 > 2500 budget),
    // so Node 1 is NOT decoded in round 1!
    // Consumer delivers 2 records from Node 0. 3 records (~1200 bytes) stay buffered.
    let recs1 = consumer.fetch().await.expect("fetch 1 succeeds");
    assert_eq!(recs1.len(), 2);
    assert_eq!(recs1[0].partition, 0);
    assert_eq!(recs1[0].offset, 0);
    assert_eq!(recs1[1].offset, 1);

    // Verify buffer memory bound is respected!
    let buffered = consumer.buffered_bytes();
    assert!(
        buffered > 0 && buffered <= 2500,
        "buffered bytes {buffered} must be within budget 2500"
    );
    assert_eq!(buffered, 3 * 400, "3 records of 400 bytes buffered");

    // Delivered position of partition 0 is 2; fetch cursor is 5.
    // Partition 1 was not decoded, so its delivered position is 0 and fetch cursor is 0.
    assert_eq!(consumer.position("t", 0).unwrap(), 2);
    assert_eq!(consumer.position("t", 1).unwrap(), 0);
    assert_eq!(consumer.fetch_cursor("t", 0).unwrap(), 5);
    assert_eq!(consumer.fetch_cursor("t", 1).unwrap(), 0);

    // Fetch 2: Drains 2 more records from pending queue without broker fetch.
    let recs2 = consumer.fetch().await.expect("fetch 2 succeeds");
    assert_eq!(recs2.len(), 2);
    assert_eq!(recs2[0].offset, 2);
    assert_eq!(recs2[1].offset, 3);
    assert_eq!(consumer.buffered_bytes(), 400);
    assert_eq!(consumer.position("t", 0).unwrap(), 4);

    // Fetch 3: Drains the 5th record from pending queue.
    let recs3 = consumer.fetch().await.expect("fetch 3 succeeds");
    assert_eq!(recs3.len(), 1);
    assert_eq!(recs3[0].offset, 4);
    assert_eq!(consumer.buffered_bytes(), 0);
    assert_eq!(consumer.position("t", 0).unwrap(), 5);

    // Fetch 4: Pending queue is empty! Now consumer fetches Node 1 (partition 1).
    // Node 1 returns 5 records (2000 bytes <= 2500 budget).
    // Consumer delivers 2 records (offsets 0, 1), 3 stay buffered.
    let recs4 = consumer.fetch().await.expect("fetch 4 succeeds");
    assert_eq!(recs4.len(), 2);
    assert_eq!(recs4[0].partition, 1);
    assert_eq!(recs4[0].offset, 0);
    assert_eq!(recs4[1].offset, 1);
    assert_eq!(consumer.buffered_bytes(), 3 * 400);
    assert_eq!(consumer.position("t", 1).unwrap(), 2);
    assert_eq!(consumer.fetch_cursor("t", 1).unwrap(), 5);

    // Fetch 5: Drains 2 records from partition 1.
    let recs5 = consumer.fetch().await.expect("fetch 5 succeeds");
    assert_eq!(recs5.len(), 2);
    assert_eq!(recs5[0].offset, 2);
    assert_eq!(recs5[1].offset, 3);
    assert_eq!(consumer.buffered_bytes(), 400);

    // Fetch 6: Drains final record from partition 1.
    let recs6 = consumer.fetch().await.expect("fetch 6 succeeds");
    assert_eq!(recs6.len(), 1);
    assert_eq!(recs6[0].offset, 4);
    assert_eq!(consumer.buffered_bytes(), 0);
    assert_eq!(consumer.position("t", 1).unwrap(), 5);

    // Total records delivered across both partitions: 10 records, in order, zero duplicates.
    consumer.close().await.expect("close succeeds");
    cluster.shutdown().await;
}

/// 2. Preserve progress for a valid first batch larger than the soft fetch limit,
/// with the existing 64 MiB hard decode ceiling from KL02-03.
#[tokio::test]
async fn oversized_first_batch_larger_than_soft_limit_preserves_progress() {
    // Broker serves a valid batch of 5 records x 1000 bytes = 5000 bytes.
    // Consumer configures soft limit buffer_memory(1500) and max_partition_fetch_bytes(1500).
    // The first batch (5000 bytes) is strictly larger than the soft fetch limit (1500 bytes),
    // but <= 64 MiB hard decode ceiling.
    let mut cluster = spawn_two_broker_cluster(
        |topics, _attempt| {
            let p0 = topics
                .iter()
                .find(|t| t.topic == "t")?
                .partitions
                .iter()
                .find(|p| p.partition == 0)?;
            if p0.fetch_offset == 0 {
                let mut part = FetchedPartition::partition_response(0, 0);
                part.high_watermark = 5;
                part.records = vec![build_batch_with_record_sizes(0, 5, 1000)];
                Some(vec![FetchedTopic {
                    topic: "t".to_string(),
                    topic_id: [0u8; 16],
                    partitions: vec![part],
                }])
            } else {
                let mut part = FetchedPartition::partition_response(0, 0);
                part.high_watermark = 5;
                Some(vec![FetchedTopic {
                    topic: "t".to_string(),
                    topic_id: [0u8; 16],
                    partitions: vec![part],
                }])
            }
        },
        |_topics, _attempt| None,
    )
    .await;

    let cfg = cluster
        .config()
        .max_partition_fetch_bytes(1500)
        .fetch_buffer_bytes(1500)
        .max_poll_records(1);

    let mut consumer = Consumer::new(cfg).await.expect("consumer starts");
    consumer.assign("t", 0, 0).await.expect("assign succeeds");

    // Fetch round 1: oversized first batch MUST be accepted to preserve progress!
    let recs = consumer
        .fetch()
        .await
        .expect("oversized first batch must be accepted");
    assert_eq!(recs.len(), 1, "max_poll_records(1) delivers first record");
    assert_eq!(recs[0].offset, 0);

    // The remaining 4 records (4000 bytes) stay buffered, even though 4000 > 1500 budget!
    assert_eq!(consumer.buffered_bytes(), 4 * 1000);
    assert_eq!(consumer.position("t", 0).unwrap(), 1);
    assert_eq!(consumer.fetch_cursor("t", 0).unwrap(), 5);

    // Consume the remaining records one by one
    for expected_offset in 1..5 {
        let r = consumer
            .fetch()
            .await
            .expect("draining pending oversized batch");
        assert_eq!(r.len(), 1);
        assert_eq!(r[0].offset, expected_offset);
        assert_eq!(consumer.position("t", 0).unwrap(), expected_offset + 1);
    }
    assert_eq!(consumer.buffered_bytes(), 0);

    consumer.close().await.expect("close succeeds");
    cluster.shutdown().await;
}

/// 3. Verify that batches exceeding the 64 MiB hard decode ceiling fail cleanly.
#[test]
fn batch_exceeding_hard_decode_ceiling_fails_cleanly() {
    use partitionline::protocol::records::{
        decode_record_batches_with_limit, encode_record_batch, Compression,
    };

    assert_eq!(DEFAULT_MAX_RECORD_BATCH_DECODE_BYTES, 64 * 1024 * 1024);

    let rec = Record {
        offset: 0,
        timestamp: 100,
        key: None,
        value: Some(Bytes::from(vec![b'x'; 10_000])),
        headers: Vec::new(),
    };

    for compression in [Compression::Gzip, Compression::Snappy, Compression::Lz4] {
        let batch = RecordBatch::from_records(vec![rec.clone()]).with_compression(compression);
        let mut encoded = BytesMut::new();
        encode_record_batch(&mut encoded, &batch).expect("encode batch");

        // Decode with limit below payload size fails with decode ceiling error
        let mut cur = encoded.as_ref();
        let res = decode_record_batches_with_limit(&mut cur, 5_000);
        assert!(
            res.is_err(),
            "compression {compression} should fail under budget"
        );
        let err_msg = res.unwrap_err().to_string();
        assert!(
            err_msg.contains("maximum allowable size"),
            "expected maximum allowable size error, got: {err_msg}"
        );

        // Decode with limit >= payload size succeeds
        let mut cur = encoded.as_ref();
        let res = decode_record_batches_with_limit(&mut cur, DEFAULT_MAX_RECORD_BATCH_DECODE_BYTES);
        assert!(
            res.is_ok(),
            "compression {compression} should pass under 64 MiB ceiling"
        );
    }
}

/// 4. Pause holds buffers, retains memory accounting, and does not commit undelivered data.
#[tokio::test]
async fn pause_holds_buffers_and_does_not_commit_undelivered() {
    let mut cluster = spawn_two_broker_cluster(
        |topics, _attempt| {
            let p0 = topics
                .iter()
                .find(|t| t.topic == "t")?
                .partitions
                .iter()
                .find(|p| p.partition == 0)?;
            if p0.fetch_offset == 0 {
                let mut part = FetchedPartition::partition_response(0, 0);
                part.high_watermark = 5;
                part.records = vec![build_batch_with_record_sizes(0, 5, 200)];
                Some(vec![FetchedTopic {
                    topic: "t".to_string(),
                    topic_id: [0u8; 16],
                    partitions: vec![part],
                }])
            } else {
                let mut part = FetchedPartition::partition_response(0, 0);
                part.high_watermark = 5;
                Some(vec![FetchedTopic {
                    topic: "t".to_string(),
                    topic_id: [0u8; 16],
                    partitions: vec![part],
                }])
            }
        },
        |_topics, _attempt| None,
    )
    .await;

    let cfg = cluster.config().max_poll_records(2).buffer_memory(1500);

    let mut consumer = Consumer::new(cfg).await.expect("consumer starts");
    consumer.assign("t", 0, 0).await.expect("assign succeeds");

    // Fetch 1: broker returns 5 records. 2 delivered, 3 buffered.
    let recs1 = consumer.fetch().await.expect("fetch 1 succeeds");
    assert_eq!(recs1.len(), 2);
    assert_eq!(consumer.buffered_bytes(), 3 * 200);
    assert_eq!(consumer.position("t", 0).unwrap(), 2);

    // Pause partition 0
    consumer.pause([("t", 0)]);
    assert_eq!(consumer.paused(), vec![TopicPartition::new("t", 0)]);

    // Buffered records are retained in memory!
    assert_eq!(consumer.buffered_bytes(), 3 * 200);

    // Position still reflects delivered position (2), NOT fetch cursor (5)!
    // Committing positions at this moment will NOT commit undelivered records.
    assert_eq!(consumer.position("t", 0).unwrap(), 2);
    assert_eq!(consumer.fetch_cursor("t", 0).unwrap(), 5);

    // Fetch while paused: drain_pending skips paused records, returning empty.
    let recs_paused = consumer.fetch().await.expect("fetch while paused succeeds");
    assert!(
        recs_paused.is_empty(),
        "must not deliver records from paused partition"
    );
    assert_eq!(
        consumer.buffered_bytes(),
        3 * 200,
        "buffers remain retained while paused"
    );

    // Resume partition 0
    consumer.resume([("t", 0)]);
    assert!(consumer.paused().is_empty());

    // Fetch 2: immediately drains 2 records from the retained buffer!
    let recs2 = consumer
        .fetch()
        .await
        .expect("fetch 2 succeeds after resume");
    assert_eq!(recs2.len(), 2);
    assert_eq!(recs2[0].offset, 2);
    assert_eq!(recs2[1].offset, 3);
    assert_eq!(consumer.buffered_bytes(), 200);
    assert_eq!(consumer.position("t", 0).unwrap(), 4);

    consumer.close().await.expect("close succeeds");
    cluster.shutdown().await;
}

/// 5. Seek releases buffer and resets delivered position without committing undelivered data.
#[tokio::test]
async fn seek_releases_buffer_and_does_not_commit_undelivered() {
    let mut cluster = spawn_two_broker_cluster(
        |topics, _attempt| {
            let p0 = topics
                .iter()
                .find(|t| t.topic == "t")?
                .partitions
                .iter()
                .find(|p| p.partition == 0)?;
            if p0.fetch_offset == 0 {
                let mut part = FetchedPartition::partition_response(0, 0);
                part.high_watermark = 5;
                part.records = vec![build_batch_with_record_sizes(0, 5, 300)];
                Some(vec![FetchedTopic {
                    topic: "t".to_string(),
                    topic_id: [0u8; 16],
                    partitions: vec![part],
                }])
            } else if p0.fetch_offset == 10 {
                let mut part = FetchedPartition::partition_response(0, 0);
                part.high_watermark = 12;
                part.records = vec![build_batch_with_record_sizes(10, 2, 300)];
                Some(vec![FetchedTopic {
                    topic: "t".to_string(),
                    topic_id: [0u8; 16],
                    partitions: vec![part],
                }])
            } else {
                let mut part = FetchedPartition::partition_response(0, 0);
                part.high_watermark = 12;
                Some(vec![FetchedTopic {
                    topic: "t".to_string(),
                    topic_id: [0u8; 16],
                    partitions: vec![part],
                }])
            }
        },
        |_topics, _attempt| None,
    )
    .await;

    let cfg = cluster.config().max_poll_records(2).buffer_memory(2000);

    let mut consumer = Consumer::new(cfg).await.expect("consumer starts");
    consumer.assign("t", 0, 0).await.expect("assign succeeds");

    // Fetch 1: 2 records delivered, 3 buffered.
    let recs1 = consumer.fetch().await.expect("fetch 1 succeeds");
    assert_eq!(recs1.len(), 2);
    assert_eq!(consumer.buffered_bytes(), 3 * 300);
    assert_eq!(consumer.position("t", 0).unwrap(), 2);

    // Seek to offset 10: drops buffered records and releases memory immediately!
    consumer.seek("t", 0, 10).expect("seek succeeds");
    assert_eq!(
        consumer.buffered_bytes(),
        0,
        "seek must release buffered bytes"
    );
    assert_eq!(
        consumer.position("t", 0).unwrap(),
        10,
        "position updated to seek offset"
    );
    assert_eq!(
        consumer.fetch_cursor("t", 0).unwrap(),
        10,
        "fetch cursor updated to seek offset"
    );

    // Fetch 2: reads from new offset 10!
    let recs2 = consumer.fetch().await.expect("fetch 2 succeeds");
    assert_eq!(recs2.len(), 2);
    assert_eq!(recs2[0].offset, 10);
    assert_eq!(recs2[1].offset, 11);
    assert_eq!(consumer.buffered_bytes(), 0);
    assert_eq!(consumer.position("t", 0).unwrap(), 12);

    consumer.close().await.expect("close succeeds");
    cluster.shutdown().await;
}

/// 6. Multi-partition single broker budget saturation stops decoding when budget is reached.
#[tokio::test]
async fn multi_partition_single_broker_budget_stops_at_ceiling() {
    let mut cluster = spawn_cluster(
        0,
        0,
        |topics, _attempt| {
            let t = topics.iter().find(|t| t.topic == "t")?;
            let mut parts = Vec::new();
            for req in &t.partitions {
                if req.partition == 0 && req.fetch_offset == 0 {
                    let mut part = FetchedPartition::partition_response(0, 0);
                    part.high_watermark = 3;
                    part.records = vec![build_batch_with_record_sizes(0, 3, 500)]; // 1500 bytes
                    parts.push(part);
                } else if req.partition == 1 && req.fetch_offset == 0 {
                    let mut part = FetchedPartition::partition_response(1, 0);
                    part.high_watermark = 3;
                    part.records = vec![build_batch_with_record_sizes(0, 3, 500)]; // 1500 bytes
                    parts.push(part);
                } else {
                    let mut part = FetchedPartition::partition_response(req.partition, 0);
                    part.high_watermark = 3;
                    parts.push(part);
                }
            }
            Some(vec![FetchedTopic {
                topic: "t".to_string(),
                topic_id: [0u8; 16],
                partitions: parts,
            }])
        },
        |_topics, _attempt| None,
    )
    .await;

    // Both partition 0 and partition 1 are led by node 0.
    // Partition 0 is 1500 bytes; Partition 1 is 1500 bytes.
    // Budget is 2000 bytes.
    // In round 1: Partition 0 is decoded (1500 bytes).
    // Partition 1 would bring total to 3000 > 2000, so Partition 1 is NOT decoded!
    let cfg = cluster.config().buffer_memory(2000);

    let mut consumer = Consumer::new(cfg).await.expect("consumer starts");
    consumer
        .assign_many([(("t", 0), 0), (("t", 1), 0)])
        .await
        .expect("assign succeeds");

    let recs1 = consumer.fetch().await.expect("fetch 1 succeeds");
    assert_eq!(recs1.len(), 3, "partition 0's 3 records delivered");
    for rec in &recs1 {
        assert_eq!(rec.partition, 0);
    }
    assert_eq!(consumer.position("t", 0).unwrap(), 3);
    assert_eq!(
        consumer.position("t", 1).unwrap(),
        0,
        "partition 1 not consumed in round 1"
    );

    // Round 2: Partition 1 is now fetched and decoded!
    let recs2 = consumer.fetch().await.expect("fetch 2 succeeds");
    assert_eq!(recs2.len(), 3, "partition 1's 3 records delivered");
    for rec in &recs2 {
        assert_eq!(rec.partition, 1);
    }
    assert_eq!(consumer.position("t", 0).unwrap(), 3);
    assert_eq!(consumer.position("t", 1).unwrap(), 3);

    consumer.close().await.expect("close succeeds");
    cluster.shutdown().await;
}

/// 7. Dropping consumer or clearing assignment releases all buffered records and memory.
#[tokio::test]
async fn drop_and_clear_releases_all_memory() {
    let mut cluster = spawn_two_broker_cluster(
        |topics, _attempt| {
            let p0 = topics
                .iter()
                .find(|t| t.topic == "t")?
                .partitions
                .iter()
                .find(|p| p.partition == 0)?;
            if p0.fetch_offset == 0 {
                let mut part = FetchedPartition::partition_response(0, 0);
                part.high_watermark = 5;
                part.records = vec![build_batch_with_record_sizes(0, 5, 200)];
                Some(vec![FetchedTopic {
                    topic: "t".to_string(),
                    topic_id: [0u8; 16],
                    partitions: vec![part],
                }])
            } else {
                None
            }
        },
        |_topics, _attempt| None,
    )
    .await;

    let cfg = cluster.config().max_poll_records(1).buffer_memory(2000);

    let mut consumer = Consumer::new(cfg).await.expect("consumer starts");
    consumer.assign("t", 0, 0).await.expect("assign succeeds");

    let recs = consumer.fetch().await.expect("fetch succeeds");
    assert_eq!(recs.len(), 1);
    assert_eq!(consumer.buffered_bytes(), 4 * 200);

    // Reassigning to empty clears assignment and drops buffered records
    consumer
        .assign_many(Vec::<((&str, i32), i64)>::new())
        .await
        .expect("assign empty succeeds");
    assert_eq!(consumer.buffered_bytes(), 0);
    assert!(consumer.assignment().is_empty());

    // consumer close drops all handles
    consumer.close().await.expect("close succeeds");
    cluster.shutdown().await;
}

/// 8. Zero buffer memory disables the aggregate limit (backward compatibility).
#[tokio::test]
async fn zero_buffer_memory_allows_unbounded_buffering() {
    let mut cluster = spawn_cluster(
        0,
        0,
        |topics, _attempt| {
            let t = topics.iter().find(|t| t.topic == "t")?;
            let mut parts = Vec::new();
            for req in &t.partitions {
                if req.fetch_offset == 0 {
                    let mut part = FetchedPartition::partition_response(req.partition, 0);
                    part.high_watermark = 3;
                    part.records = vec![build_batch_with_record_sizes(0, 3, 500)];
                    parts.push(part);
                }
            }
            Some(vec![FetchedTopic {
                topic: "t".to_string(),
                topic_id: [0u8; 16],
                partitions: parts,
            }])
        },
        |_topics, _attempt| None,
    )
    .await;

    // buffer_memory = 0 means disabled/unbounded
    let cfg = cluster.config().buffer_memory(0);

    let mut consumer = Consumer::new(cfg).await.expect("consumer starts");
    consumer
        .assign_many([(("t", 0), 0), (("t", 1), 0)])
        .await
        .expect("assign succeeds");

    let recs = consumer.fetch().await.expect("fetch succeeds");
    assert_eq!(
        recs.len(),
        6,
        "both partitions decoded when buffer_memory is 0"
    );
    assert_eq!(consumer.position("t", 0).unwrap(), 3);
    assert_eq!(consumer.position("t", 1).unwrap(), 3);

    consumer.close().await.expect("close succeeds");
    cluster.shutdown().await;
}

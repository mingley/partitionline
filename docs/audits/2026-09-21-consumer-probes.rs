//! Diagnostic fixtures for source cb7e97d3b92a8555aea34d59266a2990c206395f.
//!
//! Run through the temporary test target documented in 2026-09-21.md.
//! All five expected-behavior assertions fail at the audited source revision.
//! This is not an automatically discovered test target or a live-broker oracle.
//! Promote individual cases into maintained tests as their fixes land.

#[expect(
    dead_code,
    reason = "shared integration mock; this probe uses a subset"
)]
#[path = "../../tests/common/mod.rs"]
mod common;

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
    ControlRecordType, EndTransactionMarker, Record, RecordBatch,
};
use partitionline::{
    AutoOffsetReset, Consumer, ConsumerConfig, ConsumerGroup, IsolationLevel, ProduceRecord,
    Producer, ProducerConfig, TopicPartition,
};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::task::{JoinHandle, JoinSet};

#[derive(Clone, Copy)]
enum Scenario {
    WholeBatch,
    AbortThenCommit,
    OutOfRange,
    PartialRetry,
}

struct FixtureBroker {
    addr: String,
    task: JoinHandle<()>,
}

impl Drop for FixtureBroker {
    fn drop(&mut self) {
        self.task.abort();
    }
}

fn data_batch(offset: i64, values: &[&'static [u8]], sequence: Option<i32>) -> RecordBatch {
    let records = values
        .iter()
        .map(|value| Record {
            offset: 0,
            timestamp: 0,
            key: None,
            value: Some(Bytes::from_static(value)),
            headers: Vec::new(),
        })
        .collect();
    let mut batch = RecordBatch::from_records(records).with_transactional(sequence.is_some());
    batch.base_offset = offset;
    if let Some(sequence) = sequence {
        batch.producer_id = 7;
        batch.producer_epoch = 0;
        batch.base_sequence = sequence;
    }
    batch
}

fn marker(offset: i64, control: ControlRecordType) -> RecordBatch {
    RecordBatch::with_end_transaction_marker(
        offset,
        0,
        0,
        7,
        0,
        &EndTransactionMarker::new(control, 0).unwrap(),
    )
    .unwrap()
}

fn fetch_fixture(scenario: Scenario, topics: &[FetchTopic], attempt: usize) -> Vec<FetchedTopic> {
    topics
        .iter()
        .map(|topic| FetchedTopic {
            topic: topic.topic.clone(),
            topic_id: topic.topic_id,
            partitions: topic
                .partitions
                .iter()
                .map(|request| {
                    let mut part = FetchedPartition::partition_response(request.partition, 0);
                    part.high_watermark = 4;
                    part.last_stable_offset = 4;
                    part.log_start_offset = 0;
                    match scenario {
                        Scenario::WholeBatch => {
                            if request.fetch_offset <= 2 {
                                part.records = vec![data_batch(0, &[b"a", b"b", b"c"], None)];
                            }
                        }
                        Scenario::AbortThenCommit => {
                            part.aborted_transactions = vec![(7, 0)];
                            part.records = vec![
                                data_batch(0, &[b"aborted"], Some(0)),
                                marker(1, ControlRecordType::Abort),
                                data_batch(2, &[b"committed"], Some(1)),
                                marker(3, ControlRecordType::Commit),
                            ];
                        }
                        Scenario::OutOfRange => {
                            part.error_code = partitionline::error::OFFSET_OUT_OF_RANGE;
                            part.log_start_offset = 10;
                            part.high_watermark = 20;
                            part.last_stable_offset = 20;
                        }
                        Scenario::PartialRetry => {
                            if request.partition == 1 && attempt == 0 {
                                part.error_code = partitionline::error::NOT_LEADER_OR_FOLLOWER;
                            } else if request.fetch_offset == 0 {
                                part.records = vec![data_batch(0, &[b"keep-me"], None)];
                            }
                        }
                    }
                    part
                })
                .collect(),
        })
        .collect()
}

async fn start(scenario: Scenario) -> FixtureBroker {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let addr = address.to_string();
    let attempts = Arc::new(AtomicUsize::new(0));
    let task = tokio::spawn(async move {
        let mut connections = JoinSet::new();
        loop {
            tokio::select! {
                accepted = listener.accept() => {
                    let (mut socket, _) = accepted.unwrap();
                    let attempts = Arc::clone(&attempts);
                    let _handle = connections.spawn(async move {
                        loop {
                            let size = match socket.read_i32().await {
                                Ok(size) => usize::try_from(size).unwrap(),
                                Err(error) if matches!(error.kind(), std::io::ErrorKind::UnexpectedEof | std::io::ErrorKind::ConnectionReset) => return,
                                Err(error) => panic!("fixture read: {error}"),
                            };
                            assert!(size < 1024 * 1024);
                            let mut frame = vec![0; size];
                            let _read = socket.read_exact(&mut frame).await.unwrap();
                            let mut request = frame.as_slice();
                            let header = decode_request_header(&mut request).unwrap();
                            let mut response = BytesMut::new();
                            encode_response_header(&mut response, header.api_key, header.api_version, header.correlation_id).unwrap();
                            match header.api_key {
                                API_VERSIONS => {
                                    let api_keys = [(API_VERSIONS, 0, 4), (METADATA, 1, 9), (FETCH, 4, 12)]
                                        .into_iter()
                                        .map(|(api_key, min_version, max_version)| ApiVersion { api_key, min_version, max_version })
                                        .collect();
                                    encode_api_versions_response(&mut response, header.api_version, &ApiVersionsResponse { api_keys, ..Default::default() }).unwrap();
                                }
                                METADATA => {
                                    let count = if matches!(scenario, Scenario::PartialRetry) { 2 } else { 1 };
                                    let partitions = (0..count)
                                        .map(|partition| PartitionMetadata::new(0, partition, Some(0), Some(0), vec![0], vec![0], Vec::new()))
                                        .collect();
                                    let metadata = MetadataResponse {
                                        throttle_time_ms: 0,
                                        brokers: vec![Broker::new(0, "127.0.0.1", i32::from(address.port()), None)],
                                        cluster_id: Some("audit-fixture".into()),
                                        controller_id: 0,
                                        topics: vec![TopicMetadata::new(0, "t", false, partitions)],
                                        cluster_authorized_operations: i32::MIN,
                                        error_code: 0,
                                    };
                                    encode_metadata_response(&mut response, header.api_version, &metadata).unwrap();
                                }
                                FETCH => {
                                    let (_, _, topics, ..) = decode_fetch_request(&mut request, header.api_version).unwrap();
                                    let attempt = attempts.fetch_add(1, Ordering::SeqCst);
                                    encode_fetch_response(&mut response, header.api_version, &fetch_fixture(scenario, &topics, attempt)).unwrap();
                                }
                                api => panic!("unexpected fixture API {api}"),
                            }
                            socket.write_i32(i32::try_from(response.len()).unwrap()).await.unwrap();
                            socket.write_all(&response).await.unwrap();
                        }
                    });
                }
                finished = connections.join_next(), if !connections.is_empty() => {
                    finished.unwrap().unwrap();
                }
            }
        }
    });
    FixtureBroker { addr, task }
}

fn config(broker: &FixtureBroker) -> ConsumerConfig {
    ConsumerConfig::bootstrap([broker.addr.clone()])
        .request_timeout(Duration::from_secs(2))
        .retry_backoff(Duration::from_millis(1))
}

#[tokio::test]
async fn seek_inside_batch_must_not_return_earlier_records() {
    let broker = start(Scenario::WholeBatch).await;
    let mut consumer = Consumer::new(config(&broker)).await.unwrap();
    consumer.assign("t", 0, 2).await.unwrap();
    let records = consumer.fetch().await.unwrap();
    assert_eq!(
        records
            .iter()
            .map(|record| record.offset)
            .collect::<Vec<_>>(),
        vec![2]
    );
}

#[tokio::test]
async fn committed_transaction_after_abort_for_same_pid_must_be_visible() {
    let broker = start(Scenario::AbortThenCommit).await;
    let mut consumer = Consumer::new(config(&broker).isolation(IsolationLevel::ReadCommitted))
        .await
        .unwrap();
    consumer.assign("t", 0, 0).await.unwrap();
    let records = consumer.fetch().await.unwrap();
    assert_eq!(
        records
            .iter()
            .map(|record| record.offset)
            .collect::<Vec<_>>(),
        vec![2]
    );
}

#[tokio::test]
async fn out_of_range_with_reset_none_must_fail_without_advancing() {
    let broker = start(Scenario::OutOfRange).await;
    let mut consumer = Consumer::new(config(&broker).auto_offset_reset(AutoOffsetReset::None))
        .await
        .unwrap();
    consumer.assign("t", 0, 0).await.unwrap();
    let result = consumer.fetch().await;
    assert!(
        result.is_err(),
        "unexpected success; positions={:?}",
        consumer.positions()
    );
    assert_eq!(consumer.positions(), vec![(TopicPartition::new("t", 0), 0)]);
}

#[tokio::test]
async fn mixed_partition_retry_must_preserve_successful_records() {
    let broker = start(Scenario::PartialRetry).await;
    let mut consumer = Consumer::new(config(&broker)).await.unwrap();
    consumer
        .assign_many([(("t", 0), 0), (("t", 1), 0)])
        .await
        .unwrap();
    let records = consumer.fetch().await.unwrap();
    assert_eq!(
        records
            .iter()
            .map(|record| (record.partition, record.offset))
            .collect::<Vec<_>>(),
        vec![(0, 0), (1, 0)]
    );
}

#[tokio::test]
async fn commit_after_capped_poll_must_not_commit_buffered_records() {
    let broker = common::Mock::start().await;
    let producer =
        Producer::new(ProducerConfig::bootstrap([broker.addr.clone()]).linger(Duration::ZERO))
            .await
            .unwrap();
    let _metadata = producer
        .send_all([
            ProduceRecord::to("t").value(&b"a"[..]),
            ProduceRecord::to("t").value(&b"b"[..]),
            ProduceRecord::to("t").value(&b"c"[..]),
        ])
        .await
        .unwrap();
    producer.close().await.unwrap();
    let mut cfg = ConsumerConfig::bootstrap([broker.addr.clone()]).auto_commit(false);
    cfg.max_poll_records = Some(1);
    let mut group = ConsumerGroup::join(cfg, "audit-capped-poll", "t")
        .await
        .unwrap();
    let first = group.poll().await.unwrap();
    assert_eq!(
        first.iter().map(|record| record.offset).collect::<Vec<_>>(),
        vec![0]
    );
    group.commit().await.unwrap();
    group.leave().await.unwrap();
    let mut replacement = ConsumerGroup::join(
        ConsumerConfig::bootstrap([broker.addr.clone()]).auto_commit(false),
        "audit-capped-poll",
        "t",
    )
    .await
    .unwrap();
    let remaining = replacement.poll().await.unwrap();
    replacement.leave().await.unwrap();
    assert_eq!(
        remaining
            .iter()
            .map(|record| record.offset)
            .collect::<Vec<_>>(),
        vec![1, 2]
    );
}

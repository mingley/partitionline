//! Deterministic loopback consumer fetch fixture.
//!
//! Extracted from diagnostic audit probes (2026-09-21-consumer-probes.rs).
//! Provides an ephemeral-port loopback Kafka broker with bounded deadlines,
//! explicit task ownership/termination, and exact batch/control-marker construction
//! without pre-filtering away behaviors under test.
#![allow(dead_code, unreachable_pub)]

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use bytes::{Bytes, BytesMut};
use partitionline::error::{NOT_LEADER_OR_FOLLOWER, OFFSET_OUT_OF_RANGE};
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
use partitionline::ConsumerConfig;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::sync::oneshot;
use tokio::task::{JoinHandle, JoinSet};

/// Test scenario for the fetch fixture broker.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Scenario {
    /// Control scenario: batch starts at the requested fetch offset, so the
    /// current client handles it correctly without lower-bound filtering.
    AlignedBatch,
    /// Audit defect A01: broker returns full batch containing offsets 0, 1, 2
    /// even when requested offset is 2.
    WholeBatch,
    /// Audit defect A02: PID 7 aborts at offset 0 (ABORT marker 1), then
    /// commits at offset 2 (COMMIT marker 3).
    AbortThenCommit,
    /// Audit defect A04: broker reports OFFSET_OUT_OF_RANGE with log start 10.
    OutOfRange,
    /// Audit defect A03: partition 1 returns retriable NOT_LEADER_OR_FOLLOWER
    /// on attempt 0 while partition 0 succeeds with data.
    PartialRetry,
}

/// Helper to build a data `RecordBatch` at a given base offset.
pub fn data_batch(offset: i64, values: &[&[u8]], sequence: Option<i32>) -> RecordBatch {
    let records = values
        .iter()
        .map(|value| Record {
            offset: 0,
            timestamp: 0,
            key: None,
            value: Some(Bytes::copy_from_slice(value)),
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

/// Helper to build an end-transaction control marker batch (ABORT or COMMIT).
pub fn marker(offset: i64, control: ControlRecordType) -> RecordBatch {
    RecordBatch::with_end_transaction_marker(
        offset,
        0,
        0,
        7,
        0,
        &EndTransactionMarker::new(control, 0).expect("valid control marker"),
    )
    .expect("valid end transaction marker batch")
}

/// Construct `FetchedTopic` records for a given scenario, topics, and attempt count.
///
/// Crucially, this does NOT pre-filter records against `request.fetch_offset`
/// (unlike `tests/common/mod.rs`), so whole batches, control markers, and
/// out-of-order records are faithfully delivered to the wire.
pub fn build_fetch_response(
    scenario: Scenario,
    topics: &[FetchTopic],
    attempt: usize,
) -> Vec<FetchedTopic> {
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
                        Scenario::AlignedBatch => {
                            if request.fetch_offset == 0 {
                                part.high_watermark = 2;
                                part.last_stable_offset = 2;
                                part.records =
                                    vec![data_batch(0, &[b"control-0", b"control-1"], None)];
                            }
                        }
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
                            part.error_code = OFFSET_OUT_OF_RANGE;
                            part.log_start_offset = 10;
                            part.high_watermark = 20;
                            part.last_stable_offset = 20;
                        }
                        Scenario::PartialRetry => {
                            if request.partition == 1 && attempt == 0 {
                                part.error_code = NOT_LEADER_OR_FOLLOWER;
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

/// Bounded loopback Kafka broker running on an ephemeral port.
///
/// Owns its listener and connection tasks. Shuts them down cleanly on drop
/// or explicit [`Self::shutdown`].
pub struct FixtureBroker {
    addr: String,
    port: u16,
    shutdown_tx: Option<oneshot::Sender<()>>,
    task: Option<JoinHandle<()>>,
    active_conns: Arc<AtomicUsize>,
    attempts: Arc<AtomicUsize>,
}

impl FixtureBroker {
    /// Start a new fixture broker bound to `127.0.0.1:0` running the given scenario.
    pub async fn start(scenario: Scenario) -> Self {
        let partition_count = if matches!(scenario, Scenario::PartialRetry) {
            2
        } else {
            1
        };
        Self::start_internal(
            "t",
            partition_count,
            Arc::new(move |topics, attempt| build_fetch_response(scenario, topics, attempt)),
        )
        .await
    }

    /// Start a fixture broker with custom topic, partition count, and fetch responder function.
    pub async fn start_with_handler<F>(topic: &str, partition_count: usize, handler: F) -> Self
    where
        F: Fn(&[FetchTopic], usize) -> Vec<FetchedTopic> + Send + Sync + 'static,
    {
        Self::start_internal(topic, partition_count, Arc::new(handler)).await
    }

    async fn start_internal(
        topic: &str,
        partition_count: usize,
        fetch_handler: Arc<dyn Fn(&[FetchTopic], usize) -> Vec<FetchedTopic> + Send + Sync>,
    ) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind ephemeral loopback port");
        let address = listener.local_addr().expect("local addr");
        let port = address.port();
        let addr = address.to_string();
        let attempts = Arc::new(AtomicUsize::new(0));
        let active_conns = Arc::new(AtomicUsize::new(0));
        let (shutdown_tx, mut shutdown_rx) = oneshot::channel();

        let topic_name = topic.to_string();
        let active_conns_outer = Arc::clone(&active_conns);
        let attempts_outer = Arc::clone(&attempts);

        let task = tokio::spawn(async move {
            let mut connections = JoinSet::new();
            loop {
                tokio::select! {
                    _ = &mut shutdown_rx => {
                        connections.shutdown().await;
                        break;
                    }
                    accepted = listener.accept() => {
                        let (mut socket, _) = match accepted {
                            Ok(stream) => stream,
                            Err(_) => break,
                        };
                        let _ = active_conns_outer.fetch_add(1, Ordering::SeqCst);
                        let active_conns_inner = Arc::clone(&active_conns_outer);
                        let attempts = Arc::clone(&attempts_outer);
                        let handler = Arc::clone(&fetch_handler);
                        let topic_name = topic_name.clone();
                        let partition_count = partition_count;

                        let _handle = connections.spawn(async move {
                            struct ConnGuard(Arc<AtomicUsize>);
                            impl Drop for ConnGuard {
                                fn drop(&mut self) {
                                    let _ = self.0.fetch_sub(1, Ordering::SeqCst);
                                }
                            }
                            let _guard = ConnGuard(active_conns_inner);

                            let io_deadline = Duration::from_secs(5);
                            loop {
                                let size = match tokio::time::timeout(io_deadline, socket.read_i32()).await {
                                    Ok(Ok(size)) => match usize::try_from(size) {
                                        Ok(size) => size,
                                        Err(_) => return,
                                    },
                                    Ok(Err(error)) if matches!(
                                        error.kind(),
                                        std::io::ErrorKind::UnexpectedEof | std::io::ErrorKind::ConnectionReset
                                    ) => return,
                                    _ => return,
                                };
                                if size > 16 * 1024 * 1024 {
                                    return;
                                }
                                let mut frame = vec![0u8; size];
                                if tokio::time::timeout(io_deadline, socket.read_exact(&mut frame)).await.is_err() {
                                    return;
                                }
                                let mut request = frame.as_slice();
                                let header = match decode_request_header(&mut request) {
                                    Ok(h) => h,
                                    Err(_) => return,
                                };
                                let mut response = BytesMut::new();
                                if encode_response_header(
                                    &mut response,
                                    header.api_key,
                                    header.api_version,
                                    header.correlation_id,
                                ).is_err() {
                                    return;
                                }
                                match header.api_key {
                                    API_VERSIONS => {
                                        let api_keys = [(API_VERSIONS, 0, 4), (METADATA, 1, 9), (FETCH, 4, 12)]
                                            .into_iter()
                                            .map(|(api_key, min_version, max_version)| ApiVersion {
                                                api_key,
                                                min_version,
                                                max_version,
                                            })
                                            .collect();
                                        if encode_api_versions_response(
                                            &mut response,
                                            header.api_version,
                                            &ApiVersionsResponse {
                                                api_keys,
                                                ..Default::default()
                                            },
                                        ).is_err() {
                                            return;
                                        }
                                    }
                                    METADATA => {
                                        let partitions = (0..partition_count as i32)
                                            .map(|partition| PartitionMetadata::new(
                                                0,
                                                partition,
                                                Some(0),
                                                Some(0),
                                                vec![0],
                                                vec![0],
                                                Vec::new(),
                                            ))
                                            .collect();
                                        let metadata = MetadataResponse {
                                            throttle_time_ms: 0,
                                            brokers: vec![Broker::new(0, "127.0.0.1", i32::from(address.port()), None)],
                                            cluster_id: Some("fetch-fixture".into()),
                                            controller_id: 0,
                                            topics: vec![TopicMetadata::new(0, &topic_name, false, partitions)],
                                            cluster_authorized_operations: i32::MIN,
                                            error_code: 0,
                                        };
                                        if encode_metadata_response(&mut response, header.api_version, &metadata).is_err() {
                                            return;
                                        }
                                    }
                                    FETCH => {
                                        let (_, _, topics, ..) = match decode_fetch_request(&mut request, header.api_version) {
                                            Ok(decoded) => decoded,
                                            Err(_) => return,
                                        };
                                        let attempt = attempts.fetch_add(1, Ordering::SeqCst);
                                        let fetched_topics = handler(&topics, attempt);
                                        if encode_fetch_response(&mut response, header.api_version, &fetched_topics).is_err() {
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
                    finished = connections.join_next(), if !connections.is_empty() => {
                        let _ = finished;
                    }
                }
            }
        });

        Self {
            addr,
            port,
            shutdown_tx: Some(shutdown_tx),
            task: Some(task),
            active_conns,
            attempts,
        }
    }

    /// The socket address string `127.0.0.1:<port>`.
    pub fn addr(&self) -> &str {
        &self.addr
    }

    /// The assigned ephemeral port.
    pub fn port(&self) -> u16 {
        self.port
    }

    /// Number of fetch attempts processed.
    pub fn attempt_count(&self) -> usize {
        self.attempts.load(Ordering::SeqCst)
    }

    /// Number of currently active connection tasks.
    pub fn active_connections(&self) -> usize {
        self.active_conns.load(Ordering::SeqCst)
    }

    /// Whether the main listener task has completed.
    pub fn is_finished(&self) -> bool {
        self.task.as_ref().map_or(true, |t| t.is_finished())
    }

    /// Pre-configured [`ConsumerConfig`] with fast timeouts and retry backoff.
    pub fn config(&self) -> ConsumerConfig {
        ConsumerConfig::bootstrap([self.addr.clone()])
            .request_timeout(Duration::from_secs(2))
            .retry_backoff(Duration::from_millis(1))
    }

    /// Shut down the listener and all spawned connection tasks, waiting for completion.
    pub async fn shutdown(&mut self) {
        if let Some(tx) = self.shutdown_tx.take() {
            let _ = tx.send(());
        }
        if let Some(mut task) = self.task.take() {
            let _ = (&mut task).await;
        }
    }
}

impl Drop for FixtureBroker {
    fn drop(&mut self) {
        if let Some(tx) = self.shutdown_tx.take() {
            let _ = tx.send(());
        }
        if let Some(task) = self.task.take() {
            task.abort();
        }
    }
}

//! KL01-22: independently replay delivered-position fetch regressions.
//!
//! Consumes committed Apache kafka-clients Fetch v12 and OffsetCommit /
//! OffsetFetch v8 bodies. The five `audit-regression-a0*` rows stay
//! `failed` in `cases.json` as frozen historical pins. Local
//! `fetch_fixture` / `consumer_fetch_semantics` constructions are not the
//! oracle: the loopback broker serves the Apache response bytes and
//! advertises FETCH max v12.

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing,
    reason = "integration-test helpers; clippy.toml allow-*-in-tests covers #[test] only"
)]

use std::path::{Path, PathBuf};
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
    decode_fetch_request, decode_fetch_response, FetchedPartition,
};
use partitionline::protocol::group::{
    decode_offset_commit_request_data, decode_offset_commit_response_data,
    decode_offset_fetch_request_data, decode_offset_fetch_response_data,
    encode_offset_commit_request_data, encode_offset_commit_response_data,
    encode_offset_fetch_request_data, encode_offset_fetch_response_data, OffsetLimits,
    OffsetTopicIdentity,
};
use partitionline::protocol::header::{decode_request_header, encode_response_header};
use partitionline::{AutoOffsetReset, Consumer, IsolationLevel, TopicPartition};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::sync::oneshot;
use tokio::task::{JoinHandle, JoinSet};

const FETCH_VERSION: i16 = 12;
const OFFSET_COMMIT_VERSION: i16 = 8;
const OFFSET_FETCH_VERSION: i16 = 8;
const PEERS: &[&str] = &["3.9.1", "4.1.0", "4.1.2", "4.2.1", "4.3.1"];
const REPLAY_PEERS: &[&str] = &["4.1.0", "4.3.1"];
const REGISTERED: &[&str] = &[
    "audit-regression-a01-offset-filter",
    "audit-regression-a02-abort-interval",
    "audit-regression-a03-partial-failure",
    "audit-regression-a04-offset-out-of-range",
    "audit-regression-a05-cursor-commit",
];
const AUDIT_PIN: &str = "cb7e97d3b92a8555aea34d59266a2990c206395f";

fn fixture_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/fetch_delivery_regressions")
}

fn leftover(buf: &[u8], what: &str) {
    assert!(buf.is_empty(), "{what}: leftover {} bytes", buf.len());
}

async fn read(path: &Path) -> Vec<u8> {
    tokio::fs::read(path)
        .await
        .unwrap_or_else(|error| panic!("read {}: {error}", path.display()))
}

fn hex_bytes(bytes: &[u8]) -> String {
    bytes.iter().fold(String::new(), |mut out, byte| {
        use std::fmt::Write as _;
        write!(&mut out, "{byte:02x}").unwrap();
        out
    })
}

struct RawBroker {
    addr: String,
    shutdown_tx: Option<oneshot::Sender<()>>,
    task: Option<JoinHandle<()>>,
    last_fetch_version: Arc<AtomicUsize>,
}

impl RawBroker {
    async fn start(topic: &str, partition_count: usize, responses: Vec<Vec<u8>>) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind ephemeral loopback port");
        let address = listener.local_addr().expect("local addr");
        let addr = address.to_string();
        let last_fetch_version = Arc::new(AtomicUsize::new(0));
        let (shutdown_tx, mut shutdown_rx) = oneshot::channel();
        let topic_name = topic.to_string();
        let last_fetch_version_outer = Arc::clone(&last_fetch_version);
        let responses = Arc::new(responses);
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
                        let last_fetch_version = Arc::clone(&last_fetch_version_outer);
                        let responses = Arc::clone(&responses);
                        let topic_name = topic_name.clone();
                        let attempts = Arc::new(AtomicUsize::new(0));
                        let _handle = connections.spawn(async move {
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
                                        let api_keys = [(API_VERSIONS, 0, 4), (METADATA, 1, 9), (FETCH, 4, FETCH_VERSION)]
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
                                        let partitions = (0..i32::try_from(partition_count).unwrap())
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
                                        let mut topic = TopicMetadata::new(0, &topic_name, false, partitions);
                                        topic.topic_id = [1; 16];
                                        let metadata = MetadataResponse {
                                            throttle_time_ms: 0,
                                            brokers: vec![Broker::new(0, "127.0.0.1", i32::from(address.port()), None)],
                                            cluster_id: Some("fetch-delivery".into()),
                                            controller_id: 0,
                                            topics: vec![topic],
                                            cluster_authorized_operations: i32::MIN,
                                            error_code: 0,
                                        };
                                        if encode_metadata_response(&mut response, header.api_version, &metadata).is_err() {
                                            return;
                                        }
                                    }
                                    FETCH => {
                                        last_fetch_version.store(
                                            usize::try_from(header.api_version).expect("nonnegative Fetch version"),
                                            Ordering::SeqCst,
                                        );
                                        if decode_fetch_request(&mut request, header.api_version).is_err() {
                                            return;
                                        }
                                        let attempt = attempts.fetch_add(1, Ordering::SeqCst);
                                        let body = responses
                                            .get(attempt)
                                            .or_else(|| responses.last())
                                            .cloned()
                                            .unwrap_or_default();
                                        response.extend_from_slice(&body);
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
            shutdown_tx: Some(shutdown_tx),
            task: Some(task),
            last_fetch_version,
        }
    }

    fn config(&self) -> partitionline::ConsumerConfig {
        partitionline::ConsumerConfig::bootstrap([self.addr.clone()])
            .request_timeout(Duration::from_secs(2))
            .retry_backoff(Duration::from_millis(1))
    }

    fn last_fetch_version(&self) -> Option<i16> {
        i16::try_from(self.last_fetch_version.load(Ordering::SeqCst))
            .ok()
            .filter(|v| *v != 0)
    }

    async fn shutdown(&mut self) {
        if let Some(tx) = self.shutdown_tx.take() {
            let _ = tx.send(());
        }
        if let Some(task) = self.task.take() {
            let _ = task.await;
        }
    }
}

impl Drop for RawBroker {
    fn drop(&mut self) {
        if let Some(tx) = self.shutdown_tx.take() {
            let _ = tx.send(());
        }
        if let Some(task) = self.task.take() {
            task.abort();
        }
    }
}

fn decode_fetch_req(bytes: &[u8]) -> partitionline::protocol::fetch::DecodedFetchRequest {
    let mut cur = bytes;
    let decoded = decode_fetch_request(&mut cur, FETCH_VERSION).unwrap();
    leftover(cur, "Fetch request");
    decoded
}

fn decode_fetch_resp(bytes: &[u8]) -> Vec<FetchedPartition> {
    let mut cur = bytes;
    let (topics, _, error, _, _) = decode_fetch_response(&mut cur, FETCH_VERSION).unwrap();
    leftover(cur, "Fetch response");
    assert_eq!(error, 0);
    assert_eq!(topics.len(), 1);
    assert_eq!(topics[0].topic, "t");
    topics[0].partitions.clone()
}

fn prefixes_must_fail_fetch(bytes: &[u8], request: bool) {
    for prefix in 0..bytes.len() {
        let mut cur = &bytes[..prefix];
        let result = if request {
            decode_fetch_request(&mut cur, FETCH_VERSION).err()
        } else {
            decode_fetch_response(&mut cur, FETCH_VERSION).err()
        };
        assert!(result.is_some(), "prefix {prefix} decoded");
    }
}

#[tokio::test]
async fn apache_fetch_bodies_decode_leftover_empty() {
    let root = fixture_root();
    let manifest = String::from_utf8(read(&root.join("manifest.json")).await).unwrap();
    assert!(manifest.contains("\"task\": \"KL01-22\""));
    assert!(manifest.contains("\"total_bodies\": 75"));
    let mut bodies = 0u32;
    for peer in PEERS {
        let dir = root.join(peer);
        let a01_req = read(&dir.join("a01-offset-filter-request.bin")).await;
        let a01_resp = read(&dir.join("a01-offset-filter-response.bin")).await;
        prefixes_must_fail_fetch(&a01_req, true);
        prefixes_must_fail_fetch(&a01_resp, false);
        let (isolation, _, topics, ..) = decode_fetch_req(&a01_req);
        assert_eq!(isolation, 0);
        assert_eq!(topics[0].partitions[0].fetch_offset, 2);
        let parts = decode_fetch_resp(&a01_resp);
        assert_eq!(parts[0].error_code, 0);
        assert_eq!(parts[0].records.len(), 1);
        assert_eq!(parts[0].records[0].records.len(), 3);
        bodies += 2;

        let a02_req = read(&dir.join("a02-abort-interval-request.bin")).await;
        let a02_resp = read(&dir.join("a02-abort-interval-response.bin")).await;
        let (isolation, _, topics, ..) = decode_fetch_req(&a02_req);
        assert_eq!(isolation, 1);
        assert_eq!(topics[0].partitions[0].fetch_offset, 0);
        let parts = decode_fetch_resp(&a02_resp);
        assert_eq!(parts[0].aborted_transactions, vec![(7, 0)]);
        assert_eq!(parts[0].records.len(), 4);
        bodies += 2;

        let a03_req = read(&dir.join("a03-partial-failure-request.bin")).await;
        let a03a = read(&dir.join("a03-partial-failure-attempt0-response.bin")).await;
        let a03b = read(&dir.join("a03-partial-failure-attempt1-response.bin")).await;
        let (_, _, topics, ..) = decode_fetch_req(&a03_req);
        assert_eq!(topics[0].partitions.len(), 2);
        let attempt0 = decode_fetch_resp(&a03a);
        assert_eq!(attempt0[0].error_code, 0);
        assert_eq!(attempt0[1].error_code, NOT_LEADER_OR_FOLLOWER);
        let attempt1 = decode_fetch_resp(&a03b);
        assert_eq!(attempt1[0].error_code, 0);
        assert_eq!(attempt1[1].error_code, 0);
        bodies += 3;

        let a04_resp = read(&dir.join("a04-offset-out-of-range-response.bin")).await;
        let parts = decode_fetch_resp(&a04_resp);
        assert_eq!(parts[0].error_code, OFFSET_OUT_OF_RANGE);
        assert_eq!(parts[0].log_start_offset, 10);
        assert_eq!(parts[0].high_watermark, 20);
        bodies += 2;

        let a05_resp = read(&dir.join("a05-cursor-commit-response.bin")).await;
        let parts = decode_fetch_resp(&a05_resp);
        assert_eq!(parts[0].records[0].records.len(), 3);
        bodies += 2;
        let _ = read(&dir.join("a05-offset-commit-request.bin")).await;
        let _ = read(&dir.join("a05-offset-commit-response.bin")).await;
        let _ = read(&dir.join("a05-offset-fetch-request.bin")).await;
        let _ = read(&dir.join("a05-offset-fetch-response.bin")).await;
        bodies += 4;
    }
    assert_eq!(bodies, 75, "five peers × 15 Apache bodies");
}

#[tokio::test]
async fn apache_offset_commit_fetch_bodies_reencode() {
    let limits = OffsetLimits::default();
    for peer in PEERS {
        let dir = fixture_root().join(peer);
        let commit_req = read(&dir.join("a05-offset-commit-request.bin")).await;
        let commit_resp = read(&dir.join("a05-offset-commit-response.bin")).await;
        let fetch_req = read(&dir.join("a05-offset-fetch-request.bin")).await;
        let fetch_resp = read(&dir.join("a05-offset-fetch-response.bin")).await;
        for prefix in 0..commit_req.len() {
            assert!(
                decode_offset_commit_request_data(
                    &commit_req[..prefix],
                    OFFSET_COMMIT_VERSION,
                    limits
                )
                .is_err(),
                "{peer} OffsetCommit request prefix {prefix}"
            );
        }
        let decoded_commit =
            decode_offset_commit_request_data(&commit_req, OFFSET_COMMIT_VERSION, limits).unwrap();
        assert_eq!(decoded_commit.group_id, "pl-delivery");
        assert_eq!(decoded_commit.member_id, "member-a");
        assert_eq!(decoded_commit.generation_id_or_member_epoch, 1);
        assert_eq!(
            decoded_commit.topics[0].identity,
            OffsetTopicIdentity::Name("t".into())
        );
        assert_eq!(decoded_commit.topics[0].partitions[0].committed_offset, 1);
        assert_eq!(
            encode_offset_commit_request_data(&decoded_commit, OFFSET_COMMIT_VERSION, limits)
                .unwrap(),
            commit_req
        );
        let decoded_commit_resp =
            decode_offset_commit_response_data(&commit_resp, OFFSET_COMMIT_VERSION, limits)
                .unwrap();
        assert_eq!(decoded_commit_resp.topics[0].partitions[0].error_code, 0);
        assert_eq!(
            encode_offset_commit_response_data(&decoded_commit_resp, OFFSET_COMMIT_VERSION, limits)
                .unwrap(),
            commit_resp
        );

        let decoded_fetch =
            decode_offset_fetch_request_data(&fetch_req, OFFSET_FETCH_VERSION, limits).unwrap();
        assert_eq!(decoded_fetch.groups[0].group_id, "pl-delivery");
        assert_eq!(
            decoded_fetch.groups[0].topics.as_ref().unwrap()[0].identity,
            OffsetTopicIdentity::Name("t".into())
        );
        assert_eq!(
            encode_offset_fetch_request_data(&decoded_fetch, OFFSET_FETCH_VERSION, limits).unwrap(),
            fetch_req
        );
        let decoded_fetch_resp =
            decode_offset_fetch_response_data(&fetch_resp, OFFSET_FETCH_VERSION, limits).unwrap();
        assert_eq!(
            decoded_fetch_resp.groups[0].topics[0].partitions[0].committed_offset,
            1
        );
        assert_eq!(
            encode_offset_fetch_response_data(&decoded_fetch_resp, OFFSET_FETCH_VERSION, limits)
                .unwrap(),
            fetch_resp
        );
    }
}

#[tokio::test]
async fn apache_fetch_bodies_replay_public_consumer_histories() {
    for peer in REPLAY_PEERS {
        let dir = fixture_root().join(peer);
        let expected = String::from_utf8(read(&dir.join("expected.json")).await).unwrap();
        assert!(expected.contains("\"a01_delivered_offsets\":[2]"));
        assert!(expected.contains("\"a02_delivered_offsets\":[2]"));
        assert!(expected.contains("\"a03_delivered_partitions\":[[0,0],[1,0]]"));
        assert!(expected.contains("\"a04_error\":1"));
        assert!(expected.contains("\"a04_position\":0"));
        assert!(expected.contains("\"a05_first_poll\":[0]"));
        assert!(expected.contains("\"a05_position\":1"));
        assert!(expected.contains("\"a05_fetch_cursor\":3"));
        assert!(expected.contains("\"a05_commit_offset\":1"));

        let a01 = read(&dir.join("a01-offset-filter-response.bin")).await;
        let mut broker = RawBroker::start("t", 1, vec![a01]).await;
        let mut consumer = Consumer::new(broker.config()).await.unwrap();
        consumer.assign("t", 0, 2).await.unwrap();
        let records = consumer.fetch().await.unwrap();
        assert_eq!(
            records.iter().map(|r| r.offset).collect::<Vec<_>>(),
            vec![2],
            "{peer} A01"
        );
        assert_eq!(records[0].value.as_deref(), Some(&b"c"[..]));
        assert_eq!(broker.last_fetch_version(), Some(FETCH_VERSION));
        consumer.close().await.unwrap();
        broker.shutdown().await;

        let a02 = read(&dir.join("a02-abort-interval-response.bin")).await;
        let mut broker = RawBroker::start("t", 1, vec![a02]).await;
        let mut consumer = Consumer::new(broker.config().isolation(IsolationLevel::ReadCommitted))
            .await
            .unwrap();
        consumer.assign("t", 0, 0).await.unwrap();
        let records = consumer.fetch().await.unwrap();
        assert_eq!(
            records.iter().map(|r| r.offset).collect::<Vec<_>>(),
            vec![2],
            "{peer} A02"
        );
        assert_eq!(records[0].value.as_deref(), Some(&b"committed"[..]));
        assert_eq!(broker.last_fetch_version(), Some(FETCH_VERSION));
        consumer.close().await.unwrap();
        broker.shutdown().await;

        let a03a = read(&dir.join("a03-partial-failure-attempt0-response.bin")).await;
        let a03b = read(&dir.join("a03-partial-failure-attempt1-response.bin")).await;
        let mut broker = RawBroker::start("t", 2, vec![a03a, a03b]).await;
        let mut consumer = Consumer::new(broker.config()).await.unwrap();
        consumer
            .assign_many([(("t", 0), 0), (("t", 1), 0)])
            .await
            .unwrap();
        let records = consumer.fetch().await.unwrap();
        assert_eq!(
            records
                .iter()
                .map(|r| (r.partition, r.offset))
                .collect::<Vec<_>>(),
            vec![(0, 0), (1, 0)],
            "{peer} A03"
        );
        assert_eq!(broker.last_fetch_version(), Some(FETCH_VERSION));
        consumer.close().await.unwrap();
        broker.shutdown().await;

        let a04 = read(&dir.join("a04-offset-out-of-range-response.bin")).await;
        let mut broker = RawBroker::start("t", 1, vec![a04]).await;
        let mut consumer = Consumer::new(broker.config().auto_offset_reset(AutoOffsetReset::None))
            .await
            .unwrap();
        consumer.assign("t", 0, 0).await.unwrap();
        let error = consumer.fetch().await.expect_err("{peer} A04 must fail");
        assert_eq!(error.broker_code(), Some(OFFSET_OUT_OF_RANGE), "{peer} A04");
        assert_eq!(consumer.position("t", 0).unwrap(), 0, "{peer} A04 position");
        assert_eq!(broker.last_fetch_version(), Some(FETCH_VERSION));
        consumer.close().await.unwrap();
        broker.shutdown().await;

        let a05 = read(&dir.join("a05-cursor-commit-response.bin")).await;
        let mut broker = RawBroker::start("t", 1, vec![a05]).await;
        let mut consumer = Consumer::new(broker.config().max_poll_records(1))
            .await
            .unwrap();
        consumer.assign("t", 0, 0).await.unwrap();
        let records = consumer.fetch().await.unwrap();
        assert_eq!(
            records.iter().map(|r| r.offset).collect::<Vec<_>>(),
            vec![0],
            "{peer} A05"
        );
        assert_eq!(consumer.position("t", 0).unwrap(), 1, "{peer} A05 position");
        assert_eq!(
            consumer.fetch_cursor("t", 0).unwrap(),
            3,
            "{peer} A05 fetch_cursor"
        );
        assert_eq!(consumer.positions(), vec![(TopicPartition::new("t", 0), 1)]);
        assert_eq!(broker.last_fetch_version(), Some(FETCH_VERSION));
        consumer.close().await.unwrap();
        broker.shutdown().await;
    }
}

#[test]
fn frozen_audit_dispositions_stay_failed() {
    let cases = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/conformance/cases.json");
    let text = std::fs::read_to_string(cases).unwrap();
    for case_id in REGISTERED {
        assert!(
            text.contains(&format!("\"id\": \"{case_id}\""))
                && text.contains(&format!("\"source_pin\": \"{AUDIT_PIN}\""))
                && text.contains("\"disposition\": \"failed\"")
                && text.contains("\"adapter\": \"audit-probe\""),
            "{case_id} must remain the frozen failed audit pin"
        );
    }
}

#[test]
fn rust_fetch_and_offset_output_decodes_with_pinned_apache_when_jars_present() {
    let jars = PathBuf::from("/tmp/partitionline-conformance");
    let slf4j = jars.join("slf4j-api-1.7.36.jar");
    let source = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests/conformance/java/ConformanceFetchDeliveryRegressions.java");
    if !jars.join("kafka-clients-4.1.2.jar").exists() || !slf4j.exists() || !source.exists() {
        println!("pinned kafka-clients jars not present; skipping live Apache parse");
        return;
    }
    let mut parsed = 0u32;
    for peer in PEERS {
        let jar = jars.join(format!("kafka-clients-{peer}.jar"));
        if !jar.exists() {
            continue;
        }
        let classes = PathBuf::from("/tmp").join(format!("fetch-delivery-classes-{peer}"));
        std::fs::create_dir_all(&classes).unwrap();
        let cp = format!("{}:{}", jar.display(), slf4j.display());
        let compile = std::process::Command::new("java")
            .args([
                "--add-modules",
                "jdk.compiler",
                "com.sun.tools.javac.Main",
                "-source",
                "21",
                "-target",
                "21",
                "-cp",
                &cp,
                "-d",
                classes.to_str().unwrap(),
                source.to_str().unwrap(),
            ])
            .output()
            .expect("javac");
        assert!(
            compile.status.success(),
            "compile {peer}: {}",
            String::from_utf8_lossy(&compile.stderr)
        );
        let dir = fixture_root().join(peer);
        let cells = [
            (
                "fetch-response",
                FETCH_VERSION,
                dir.join("a01-offset-filter-response.bin"),
                "\"first_error\":0",
            ),
            (
                "fetch-response",
                FETCH_VERSION,
                dir.join("a04-offset-out-of-range-response.bin"),
                "\"first_error\":1",
            ),
            (
                "offset-commit",
                OFFSET_COMMIT_VERSION,
                dir.join("a05-offset-commit-request.bin"),
                "\"committed_offset\":1",
            ),
            (
                "offset-fetch",
                OFFSET_FETCH_VERSION,
                dir.join("a05-offset-fetch-response.bin"),
                "\"committed_offset\":1",
            ),
        ];
        for (kind, version, path, field) in cells {
            if !path.exists() {
                continue;
            }
            let bytes = std::fs::read(&path).unwrap();
            let run_cp = format!(
                "{}:{}:{}",
                classes.display(),
                jar.display(),
                slf4j.display()
            );
            let out = std::process::Command::new("java")
                .args([
                    "-cp",
                    &run_cp,
                    "ConformanceFetchDeliveryRegressions",
                    "decode-rust",
                    jar.to_str().unwrap(),
                    kind,
                    &version.to_string(),
                    &hex_bytes(&bytes),
                ])
                .output()
                .expect("java decode-rust");
            let stdout = String::from_utf8_lossy(&out.stdout);
            assert!(
                out.status.success() && stdout.contains("\"ok\":true") && stdout.contains(field),
                "{peer} {kind}: {} {stdout}",
                String::from_utf8_lossy(&out.stderr)
            );
            parsed += 1;
        }
    }
    assert_eq!(parsed, 20, "five peers × four named reverse-parse cells");
}

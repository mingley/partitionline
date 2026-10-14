//! KL01-23: independently qualify historical ordinary broker/record-event cells.
//!
//! Consumes committed Apache kafka-clients Produce v9, Fetch v12,
//! ListOffsets v5, and Metadata v9 bodies for a 25-record one-partition
//! topic `pl-ordinary`, plus Apache-shaped verifiable JSON events. The
//! three registered rows stay `local_consistency` in `cases.json`. Local
//! `verifiable_contract` mock-broker output is not the oracle.

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
use partitionline::protocol::api::{
    decode_metadata_request, decode_metadata_response, decode_produce_request,
    decode_produce_response, encode_api_versions_response, encode_metadata_response, ApiVersion,
    ApiVersionsResponse, Broker, MetadataResponse, PartitionMetadata, TopicMetadata,
};
use partitionline::protocol::api_keys::{API_VERSIONS, FETCH, METADATA};
use partitionline::protocol::fetch::{decode_fetch_request, decode_fetch_response};
use partitionline::protocol::header::{decode_request_header, encode_response_header};
use partitionline::protocol::offsets::{
    decode_list_offsets_request, decode_list_offsets_response, decode_list_offsets_topics_request,
    decode_list_offsets_topics_response,
};
use partitionline::{Consumer, TopicPartition};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::sync::oneshot;
use tokio::task::{JoinHandle, JoinSet};

const PRODUCE_VERSION: i16 = 9;
const FETCH_VERSION: i16 = 12;
const LIST_OFFSETS_VERSION: i16 = 5;
const METADATA_VERSION: i16 = 9;
const RECORD_COUNT: i64 = 25;
const TOPIC: &str = "pl-ordinary";
const PEERS: &[&str] = &["3.9.1", "4.1.0", "4.1.2", "4.2.1", "4.3.1"];
const REPLAY_PEERS: &[&str] = &["4.1.0", "4.3.1"];
const REGISTERED: &[&str] = &[
    "broker-cell-historical-apache-3-9-1",
    "broker-cell-historical-apache-4-1-0",
    "verifiable-produce-consume-3-9-1",
];
const AUDIT_PIN: &str = "cb7e97d3b92a8555aea34d59266a2990c206395f";

fn fixture_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/historical_ordinary")
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
    async fn start(topic: &str, fetch_body: Vec<u8>) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind ephemeral loopback port");
        let address = listener.local_addr().expect("local addr");
        let addr = address.to_string();
        let last_fetch_version = Arc::new(AtomicUsize::new(0));
        let (shutdown_tx, mut shutdown_rx) = oneshot::channel();
        let topic_name = topic.to_string();
        let last_fetch_version_outer = Arc::clone(&last_fetch_version);
        let fetch_body = Arc::new(fetch_body);
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
                        let fetch_body = Arc::clone(&fetch_body);
                        let topic_name = topic_name.clone();
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
                                        let partitions = vec![PartitionMetadata::new(
                                            0,
                                            0,
                                            Some(0),
                                            Some(0),
                                            vec![0],
                                            vec![0],
                                            Vec::new(),
                                        )];
                                        let mut topic = TopicMetadata::new(0, &topic_name, false, partitions);
                                        topic.topic_id = [1; 16];
                                        let metadata = MetadataResponse {
                                            throttle_time_ms: 0,
                                            brokers: vec![Broker::new(0, "127.0.0.1", i32::from(address.port()), None)],
                                            cluster_id: Some("pl-ordinary".into()),
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
                                        response.extend_from_slice(&fetch_body);
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
            if tx.send(()).is_err() {
                // Listener already gone.
            }
        }
        if let Some(task) = self.task.take() {
            drop(task.await);
        }
    }
}

impl Drop for RawBroker {
    fn drop(&mut self) {
        if let Some(tx) = self.shutdown_tx.take() {
            if tx.send(()).is_err() {
                // Listener already gone.
            }
        }
        if let Some(task) = self.task.take() {
            task.abort();
        }
    }
}

fn prefixes_must_fail(bytes: &[u8], what: &str, mut decode: impl FnMut(&[u8]) -> bool) {
    for prefix in 0..bytes.len() {
        assert!(decode(&bytes[..prefix]), "{what} prefix {prefix} decoded");
    }
}

#[tokio::test]
async fn apache_ordinary_bodies_decode_leftover_empty() {
    let root = fixture_root();
    let manifest = String::from_utf8(read(&root.join("manifest.json")).await).unwrap();
    assert!(manifest.contains("\"task\": \"KL01-23\""));
    assert!(manifest.contains("\"total_bodies\": 40"));
    assert!(manifest.contains("\"topic\": \"pl-ordinary\""));
    let mut bodies = 0u32;
    for peer in PEERS {
        let dir = root.join(peer);
        let produce_req = read(&dir.join("produce-request.bin")).await;
        let produce_resp = read(&dir.join("produce-response.bin")).await;
        prefixes_must_fail(&produce_req, &format!("{peer} Produce request"), |buf| {
            decode_produce_request(&mut &buf[..], PRODUCE_VERSION).is_err()
        });
        prefixes_must_fail(&produce_resp, &format!("{peer} Produce response"), |buf| {
            decode_produce_response(&mut &buf[..], PRODUCE_VERSION).is_err()
        });
        let mut cur = produce_req.as_slice();
        let (txn, acks, timeout_ms, topics) =
            decode_produce_request(&mut cur, PRODUCE_VERSION).unwrap();
        leftover(cur, &format!("{peer} Produce request"));
        assert!(txn.is_none());
        assert_eq!(acks, -1);
        assert_eq!(timeout_ms, 30_000);
        assert_eq!(topics[0].topic, TOPIC);
        assert_eq!(topics[0].partitions[0].records.records.len(), 25);
        assert_eq!(
            topics[0].partitions[0].records.records[0].value.as_deref(),
            Some(&b"0"[..])
        );
        assert_eq!(
            topics[0].partitions[0].records.records[24].value.as_deref(),
            Some(&b"24"[..])
        );
        let mut cur = produce_resp.as_slice();
        let (parts, _, throttle) = decode_produce_response(&mut cur, PRODUCE_VERSION).unwrap();
        leftover(cur, &format!("{peer} Produce response"));
        assert_eq!(throttle, 0);
        assert_eq!(parts[0].base_offset, 0);
        assert_eq!(parts[0].error_code, 0);
        bodies += 2;

        let fetch_req = read(&dir.join("fetch-request.bin")).await;
        let fetch_resp = read(&dir.join("fetch-response.bin")).await;
        prefixes_must_fail(&fetch_req, &format!("{peer} Fetch request"), |buf| {
            decode_fetch_request(&mut &buf[..], FETCH_VERSION).is_err()
        });
        prefixes_must_fail(&fetch_resp, &format!("{peer} Fetch response"), |buf| {
            decode_fetch_response(&mut &buf[..], FETCH_VERSION).is_err()
        });
        let mut cur = fetch_req.as_slice();
        let (isolation, _, topics, ..) = decode_fetch_request(&mut cur, FETCH_VERSION).unwrap();
        leftover(cur, &format!("{peer} Fetch request"));
        assert_eq!(isolation, 0);
        assert_eq!(topics[0].topic, TOPIC);
        assert_eq!(topics[0].partitions[0].fetch_offset, 0);
        let mut cur = fetch_resp.as_slice();
        let (topics, _, error, _, _) = decode_fetch_response(&mut cur, FETCH_VERSION).unwrap();
        leftover(cur, &format!("{peer} Fetch response"));
        assert_eq!(error, 0);
        assert_eq!(topics[0].topic, TOPIC);
        assert_eq!(topics[0].partitions[0].high_watermark, RECORD_COUNT);
        assert_eq!(topics[0].partitions[0].records[0].records.len(), 25);
        bodies += 2;

        let list_req = read(&dir.join("list-offsets-request.bin")).await;
        let list_resp = read(&dir.join("list-offsets-response.bin")).await;
        prefixes_must_fail(&list_req, &format!("{peer} ListOffsets request"), |buf| {
            decode_list_offsets_topics_request(&mut &buf[..], LIST_OFFSETS_VERSION).is_err()
        });
        prefixes_must_fail(&list_resp, &format!("{peer} ListOffsets response"), |buf| {
            decode_list_offsets_topics_response(&mut &buf[..], LIST_OFFSETS_VERSION).is_err()
        });
        let mut cur = list_req.as_slice();
        let (isolation, name, partition, epoch, timestamp) =
            decode_list_offsets_request(&mut cur, LIST_OFFSETS_VERSION).unwrap();
        leftover(cur, &format!("{peer} ListOffsets request"));
        assert_eq!(isolation, 0);
        assert_eq!(name, TOPIC);
        assert_eq!(partition, 0);
        assert_eq!(epoch, -1);
        assert_eq!(timestamp, -1);
        let mut cur = list_resp.as_slice();
        let decoded = decode_list_offsets_response(&mut cur, LIST_OFFSETS_VERSION).unwrap();
        leftover(cur, &format!("{peer} ListOffsets response"));
        assert_eq!(decoded.offset, RECORD_COUNT);
        assert_eq!(decoded.timestamp, -1);
        bodies += 2;

        let meta_req = read(&dir.join("metadata-request.bin")).await;
        let meta_resp = read(&dir.join("metadata-response.bin")).await;
        prefixes_must_fail(&meta_req, &format!("{peer} Metadata request"), |buf| {
            decode_metadata_request(&mut &buf[..], METADATA_VERSION).is_err()
        });
        prefixes_must_fail(&meta_resp, &format!("{peer} Metadata response"), |buf| {
            decode_metadata_response(&mut &buf[..], METADATA_VERSION).is_err()
        });
        let mut cur = meta_req.as_slice();
        let (topics, allow_auto, ..) = decode_metadata_request(&mut cur, METADATA_VERSION).unwrap();
        leftover(cur, &format!("{peer} Metadata request"));
        assert_eq!(topics.as_deref(), Some(&[TOPIC.to_string()][..]));
        assert!(!allow_auto);
        let mut cur = meta_resp.as_slice();
        let metadata = decode_metadata_response(&mut cur, METADATA_VERSION).unwrap();
        leftover(cur, &format!("{peer} Metadata response"));
        assert_eq!(metadata.cluster_id.as_deref(), Some("pl-ordinary-cluster"));
        assert_eq!(metadata.controller_id, 1);
        assert_eq!(metadata.topics[0].name.as_deref(), Some(TOPIC));
        assert_eq!(metadata.topics[0].partitions[0].leader_id, 1);
        bodies += 2;
    }
    assert_eq!(bodies, 40, "five peers × 8 Apache bodies");
}

#[tokio::test]
async fn apache_verifiable_events_match_pinned_contract() {
    for peer in PEERS {
        let dir = fixture_root().join(peer);
        let events = String::from_utf8(read(&dir.join("events.json")).await).unwrap();
        let expected = String::from_utf8(read(&dir.join("expected.json")).await).unwrap();
        assert!(events.contains("\"json_property_order\":[\"timestamp\",\"name\"]"));
        assert!(events.contains(&format!("\"topic\":\"{TOPIC}\"")));
        assert_eq!(
            events.matches("\"name\":\"producer_send_success\"").count(),
            25,
            "{peer} producer_send_success"
        );
        assert_eq!(
            events.matches("\"name\":\"record_data\"").count(),
            25,
            "{peer} record_data"
        );
        assert!(events.contains("\"name\":\"startup_complete\""));
        assert!(events.contains("\"name\":\"shutdown_complete\""));
        assert!(events.contains("\"name\":\"tool_data\""));
        assert!(events.contains("\"name\":\"records_consumed\""));
        assert!(events.contains("\"name\":\"offsets_committed\""));
        assert!(events.contains("\"name\":\"partitions_assigned\""));
        assert!(events.contains("\"success\":true"));
        assert!(events.contains("\"offset\":25"));
        assert!(events.contains("\"minOffset\":0"));
        assert!(events.contains("\"maxOffset\":24"));
        assert!(
            !events.contains("\"error\":"),
            "{peer} successful commit omits error"
        );
        assert!(expected.contains("\"record_count\":25"));
        assert!(expected.contains("\"produce_base_offset\":0"));
        assert!(expected.contains("\"list_offsets_offset\":25"));
        assert!(expected.contains("\"verifiable_commit_offset\":25"));
        assert!(
            events.contains("{\"timestamp\":1710000000000,\"name\":\"startup_complete\"}"),
            "{peer} Jackson timestamp/name order"
        );
    }
}

#[tokio::test]
async fn apache_fetch_replay_public_consumer_25_records() {
    for peer in REPLAY_PEERS {
        let dir = fixture_root().join(peer);
        let fetch = read(&dir.join("fetch-response.bin")).await;
        let mut broker = RawBroker::start(TOPIC, fetch).await;
        let mut consumer = Consumer::new(broker.config()).await.unwrap();
        consumer.assign(TOPIC, 0, 0).await.unwrap();
        let records = consumer.fetch().await.unwrap();
        assert_eq!(records.len(), 25, "{peer} delivered {}", records.len());
        let offsets: Vec<i64> = records.iter().map(|r| r.offset).collect();
        assert_eq!(offsets, (0..25).collect::<Vec<_>>(), "{peer} offsets");
        assert_eq!(records[0].value.as_deref(), Some(&b"0"[..]));
        assert_eq!(records[24].value.as_deref(), Some(&b"24"[..]));
        assert_eq!(consumer.position(TOPIC, 0).unwrap(), 25, "{peer} position");
        assert_eq!(
            consumer.positions(),
            vec![(TopicPartition::new(TOPIC, 0), 25)]
        );
        assert_eq!(broker.last_fetch_version(), Some(FETCH_VERSION));
        consumer.close().await.unwrap();
        broker.shutdown().await;
    }
}

#[tokio::test]
async fn frozen_ordinary_dispositions_stay_local_consistency() {
    let cases = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/conformance/cases.json");
    let text = String::from_utf8(read(&cases).await).unwrap();
    for case_id in REGISTERED {
        assert!(
            text.contains(&format!("\"id\": \"{case_id}\""))
                && text.contains(&format!("\"source_pin\": \"{AUDIT_PIN}\""))
                && text.contains("\"disposition\": \"local_consistency\""),
            "{case_id} must remain the frozen local_consistency pin"
        );
    }
    assert!(
        !text.contains("verifiable-produce-consume-live-4-1-2")
            || text.contains("\"id\": \"verifiable-produce-consume-live-4-1-2\""),
        "KL01-17 live row must remain present and untouched"
    );
}

#[tokio::test]
async fn rust_ordinary_output_decodes_with_pinned_apache_when_jars_present() {
    let jars = PathBuf::from("/tmp/partitionline-conformance");
    let slf4j = jars.join("slf4j-api-1.7.36.jar");
    let source = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests/conformance/java/ConformanceHistoricalOrdinary.java");
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
        let classes = PathBuf::from("/tmp").join(format!("historical-ordinary-classes-{peer}"));
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
                "produce-request",
                PRODUCE_VERSION,
                dir.join("produce-request.bin"),
                "\"acks\":-1",
            ),
            (
                "fetch-response",
                FETCH_VERSION,
                dir.join("fetch-response.bin"),
                "\"high_watermark\":25",
            ),
            (
                "list-offsets-response",
                LIST_OFFSETS_VERSION,
                dir.join("list-offsets-response.bin"),
                "\"offset\":25",
            ),
            (
                "metadata-response",
                METADATA_VERSION,
                dir.join("metadata-response.bin"),
                "\"leader_id\":1",
            ),
        ];
        for (kind, version, path, field) in cells {
            if !path.exists() {
                continue;
            }
            let bytes = read(&path).await;
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
                    "ConformanceHistoricalOrdinary",
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

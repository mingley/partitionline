//! KL01-26: independently qualify the finite verifiable Producer option profile.
//!
//! Consumes committed Apache kafka-clients Produce v9 bodies for the
//! hashed VerifiableProducer option mapping (six records, throughput 5,
//! value-prefix 42, repeating-keys 2, create-time 1650000000000). The
//! registered `verifiable-producer-options-3-9-1` row stays
//! `local_consistency` in `cases.json`. Local `verifiable_contract` mock
//! output is not the oracle.

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

use parking_lot::Mutex;

use bytes::BytesMut;
use partitionline::protocol::api::{
    decode_produce_request, decode_produce_response, encode_api_versions_response,
    encode_metadata_response, encode_produce_response, ApiVersion, ApiVersionsResponse, Broker,
    MetadataResponse, PartitionMetadata, ProducePartitionResponse, TopicMetadata,
};
use partitionline::protocol::api_keys::{API_VERSIONS, METADATA, PRODUCE};
use partitionline::protocol::header::{decode_request_header, encode_response_header};
use partitionline::{ProduceRecord, Producer, ProducerConfig};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::sync::oneshot;
use tokio::task::{JoinHandle, JoinSet};

const PRODUCE_VERSION: i16 = 9;
const TOPIC: &str = "pl-options";
const RECORD_COUNT: usize = 6;
const CREATE_TIME: i64 = 1_650_000_000_000;
const KEYS: [&str; 6] = ["0", "1", "0", "1", "0", "1"];
const VALUES: [&str; 6] = ["42.0", "42.1", "42.2", "42.3", "42.4", "42.5"];
const PEERS: &[&str] = &["3.9.1", "4.1.0", "4.1.2", "4.2.1", "4.3.1"];
const REPLAY_PEERS: &[&str] = &["4.1.0", "4.3.1"];
const REGISTERED: &[&str] = &["verifiable-producer-options-3-9-1"];
const AUDIT_PIN: &str = "3e1803560e081d44f8e5ae4a3e4b449f69ff0a54";

fn fixture_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/verifiable_producer_options")
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
    last_produce_version: Arc<AtomicUsize>,
    produce_requests: Arc<Mutex<Vec<Vec<u8>>>>,
}

impl RawBroker {
    async fn start(topic: &str) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind ephemeral loopback port");
        let address = listener.local_addr().expect("local addr");
        let addr = address.to_string();
        let last_produce_version = Arc::new(AtomicUsize::new(0));
        let produce_requests = Arc::new(Mutex::new(Vec::new()));
        let produce_attempts = Arc::new(AtomicUsize::new(0));
        let (shutdown_tx, mut shutdown_rx) = oneshot::channel();
        let topic_name = topic.to_string();
        let last_produce_version_outer = Arc::clone(&last_produce_version);
        let produce_requests_outer = Arc::clone(&produce_requests);
        let produce_attempts_outer = Arc::clone(&produce_attempts);
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
                        let last_produce_version = Arc::clone(&last_produce_version_outer);
                        let produce_requests = Arc::clone(&produce_requests_outer);
                        let produce_attempts = Arc::clone(&produce_attempts_outer);
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
                                        let api_keys = [
                                            (API_VERSIONS, 0, 4),
                                            (METADATA, 1, 9),
                                            (PRODUCE, 3, PRODUCE_VERSION),
                                        ]
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
                                            cluster_id: Some("pl-options".into()),
                                            controller_id: 0,
                                            topics: vec![topic],
                                            cluster_authorized_operations: i32::MIN,
                                            error_code: 0,
                                        };
                                        if encode_metadata_response(&mut response, header.api_version, &metadata).is_err() {
                                            return;
                                        }
                                    }
                                    PRODUCE => {
                                        last_produce_version.store(
                                            usize::try_from(header.api_version).expect("nonnegative Produce version"),
                                            Ordering::SeqCst,
                                        );
                                        produce_requests.lock().push(request.to_vec());
                                        if decode_produce_request(&mut request, header.api_version).is_err() {
                                            return;
                                        }
                                        let attempt = produce_attempts.fetch_add(1, Ordering::SeqCst);
                                        let offset = i64::try_from(attempt).unwrap_or(i64::MAX);
                                        let part = ProducePartitionResponse::partition_response_with_offsets(
                                            topic_name.clone(),
                                            0,
                                            0,
                                            offset,
                                            1_700_000_000_000,
                                            0,
                                        );
                                        if encode_produce_response(&mut response, header.api_version, &[part]).is_err() {
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
            shutdown_tx: Some(shutdown_tx),
            task: Some(task),
            last_produce_version,
            produce_requests,
        }
    }

    fn producer_config(&self) -> ProducerConfig {
        ProducerConfig::bootstrap([self.addr.clone()])
            .linger(Duration::ZERO)
            .request_timeout(Duration::from_millis(800))
            .retry_backoff(Duration::from_millis(1))
    }

    fn last_produce_version(&self) -> Option<i16> {
        i16::try_from(self.last_produce_version.load(Ordering::SeqCst))
            .ok()
            .filter(|v| *v != 0)
    }

    fn produce_requests(&self) -> Vec<Vec<u8>> {
        self.produce_requests.lock().clone()
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

fn prefixes_must_fail(bytes: &[u8], what: &str) {
    for prefix in 0..bytes.len() {
        assert!(
            decode_produce_request(&mut &bytes[..prefix], PRODUCE_VERSION).is_err(),
            "{what} prefix {prefix} decoded"
        );
    }
}

fn assert_option_records(
    topics: &[partitionline::protocol::api::ProduceTopicData],
    count: usize,
    what: &str,
) {
    assert_eq!(topics[0].topic, TOPIC, "{what}");
    assert_eq!(
        topics[0].partitions[0].records.records.len(),
        count,
        "{what}"
    );
    for (i, record) in topics[0].partitions[0].records.records.iter().enumerate() {
        assert_eq!(
            record.key.as_deref(),
            Some(KEYS[i].as_bytes()),
            "{what} key {i}"
        );
        assert_eq!(
            record.value.as_deref(),
            Some(VALUES[i].as_bytes()),
            "{what} value {i}"
        );
        assert_eq!(record.timestamp, CREATE_TIME, "{what} create-time {i}");
    }
}

#[tokio::test]
async fn apache_option_bodies_decode_leftover_empty() {
    let root = fixture_root();
    let manifest = String::from_utf8(read(&root.join("manifest.json")).await).unwrap();
    assert!(manifest.contains("\"task\": \"KL01-26\""));
    assert!(manifest.contains("\"total_bodies\": 40"));
    assert!(manifest.contains("\"topic\": \"pl-options\""));
    let mut bodies = 0u32;
    for peer in PEERS {
        let dir = root.join(peer);
        let expected = String::from_utf8(read(&dir.join("expected.json")).await).unwrap();
        assert!(expected.contains("\"value_prefix\":42"));
        assert!(expected.contains("\"repeating_keys\":2"));
        assert!(expected.contains("\"message_create_time\":1650000000000"));
        assert!(expected.contains("\"42.0\""));
        let options = String::from_utf8(read(&dir.join("options.json")).await).unwrap();
        assert!(options.contains("\"max-messages\":-1"));
        assert!(options.contains("\"throughput\":-1"));
        let events = String::from_utf8(read(&dir.join("events.json")).await).unwrap();
        assert!(events.contains("\"json_property_order\":[\"timestamp\",\"name\"]"));
        assert!(events.contains("\"target_throughput\":5"));
        assert!(events.contains("\"key\":\"0\",\"value\":\"42.0\""));
        assert!(events.contains("\"key\":\"1\",\"value\":\"42.5\""));

        let req = read(&dir.join("produce-request.bin")).await;
        let resp = read(&dir.join("produce-response.bin")).await;
        prefixes_must_fail(&req, &format!("{peer} profile request"));
        prefixes_must_fail(&resp, &format!("{peer} profile response"));
        let mut cur = req.as_slice();
        let (txn, acks, timeout_ms, topics) =
            decode_produce_request(&mut cur, PRODUCE_VERSION).unwrap();
        leftover(cur, &format!("{peer} profile request"));
        assert!(txn.is_none());
        assert_eq!(acks, -1);
        assert_eq!(timeout_ms, 30_000);
        assert_option_records(&topics, RECORD_COUNT, &format!("{peer} profile"));
        let mut cur = resp.as_slice();
        let (parts, _, throttle) = decode_produce_response(&mut cur, PRODUCE_VERSION).unwrap();
        leftover(cur, &format!("{peer} profile response"));
        assert_eq!(throttle, 0);
        assert_eq!(parts[0].base_offset, 0);
        bodies += 2;

        for i in 0..RECORD_COUNT {
            let path = dir.join(format!("r{i:02}-request.bin"));
            let bytes = read(&path).await;
            prefixes_must_fail(&bytes, &format!("{peer} r{i:02}"));
            let mut cur = bytes.as_slice();
            let (_, acks, _, topics) = decode_produce_request(&mut cur, PRODUCE_VERSION).unwrap();
            leftover(cur, &format!("{peer} r{i:02}"));
            assert_eq!(acks, -1);
            assert_eq!(topics[0].partitions[0].records.records.len(), 1);
            assert_eq!(
                topics[0].partitions[0].records.records[0].key.as_deref(),
                Some(KEYS[i].as_bytes())
            );
            assert_eq!(
                topics[0].partitions[0].records.records[0].value.as_deref(),
                Some(VALUES[i].as_bytes())
            );
            assert_eq!(
                topics[0].partitions[0].records.records[0].timestamp,
                CREATE_TIME
            );
            bodies += 1;
        }
    }
    assert_eq!(bodies, 40, "five peers × 8 Apache bodies");
}

#[tokio::test]
async fn apache_option_profile_replays_public_producer() {
    for peer in REPLAY_PEERS {
        let mut broker = RawBroker::start(TOPIC).await;
        let producer = Producer::new(broker.producer_config()).await.unwrap();
        for i in 0..RECORD_COUNT {
            let md = producer
                .send(
                    ProduceRecord::to(TOPIC)
                        .key(KEYS[i].as_bytes())
                        .value(VALUES[i].as_bytes())
                        .timestamp(CREATE_TIME),
                )
                .await
                .unwrap();
            assert_eq!(md.offset, i64::try_from(i).unwrap(), "{peer} offset {i}");
        }
        assert_eq!(broker.last_produce_version(), Some(PRODUCE_VERSION));
        let captured = broker.produce_requests();
        assert_eq!(captured.len(), RECORD_COUNT, "{peer} produce requests");
        for (i, body) in captured.iter().enumerate() {
            let mut cur = body.as_slice();
            let (_, _, _, topics) = decode_produce_request(&mut cur, PRODUCE_VERSION).unwrap();
            leftover(cur, &format!("{peer} captured r{i:02}"));
            assert_eq!(topics[0].partitions[0].records.records.len(), 1);
            assert_eq!(
                topics[0].partitions[0].records.records[0].key.as_deref(),
                Some(KEYS[i].as_bytes()),
                "{peer} captured key {i}"
            );
            assert_eq!(
                topics[0].partitions[0].records.records[0].value.as_deref(),
                Some(VALUES[i].as_bytes()),
                "{peer} captured value {i}"
            );
            assert_eq!(
                topics[0].partitions[0].records.records[0].timestamp, CREATE_TIME,
                "{peer} captured create-time {i}"
            );
        }
        assert_eq!(producer.metrics().records_acked, 6);
        producer.close().await.unwrap();
        broker.shutdown().await;
    }
}

#[tokio::test]
async fn frozen_option_disposition_stays_local_consistency() {
    let cases = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/conformance/cases.json");
    let text = String::from_utf8(read(&cases).await).unwrap();
    for case_id in REGISTERED {
        assert!(
            text.contains(&format!("\"id\": \"{case_id}\""))
                && text.contains(&format!("\"source_pin\": \"{AUDIT_PIN}\""))
                && text.contains("\"disposition\": \"local_consistency\"")
                && text.contains("\"adapter\": \"verifiable-scenario\""),
            "{case_id} must remain the frozen local_consistency pin"
        );
    }
}

#[tokio::test]
async fn rust_option_output_decodes_with_pinned_apache_when_jars_present() {
    let jars = PathBuf::from("/tmp/partitionline-conformance");
    let slf4j = jars.join("slf4j-api-1.7.36.jar");
    let source = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests/conformance/java/ConformanceVerifiableProducerOptions.java");
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
        let classes =
            PathBuf::from("/tmp").join(format!("verifiable-producer-options-classes-{peer}"));
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
                dir.join("produce-request.bin"),
                "\"records\":6",
            ),
            (
                "produce-request",
                dir.join("r00-request.bin"),
                "\"first_value\":\"42.0\"",
            ),
            (
                "produce-request",
                dir.join("r05-request.bin"),
                "\"first_value\":\"42.5\"",
            ),
            (
                "produce-response",
                dir.join("produce-response.bin"),
                "\"base_offset\":0",
            ),
        ];
        for (kind, path, field) in cells {
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
                    "ConformanceVerifiableProducerOptions",
                    "decode-rust",
                    jar.to_str().unwrap(),
                    kind,
                    &PRODUCE_VERSION.to_string(),
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

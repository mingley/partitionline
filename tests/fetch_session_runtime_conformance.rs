//! KL01-24: independently qualify incremental Fetch session runtime.
//!
//! Consumes committed Apache kafka-clients Fetch v12 full, incremental,
//! forgotten, and reset bodies. The registered
//! `feature-fetch-incremental-session-runtime` row stays `not_run` in
//! `cases.json`. Local `FetchSessionOracle` / mock-broker constructions
//! are not the oracle: leftover-empty decode reads Apache bytes, and the
//! loopback broker serves those bytes while advertising FETCH max v12.

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
use partitionline::error::{FETCH_SESSION_ID_NOT_FOUND, INVALID_FETCH_SESSION_EPOCH};
use partitionline::protocol::api::{
    encode_api_versions_response, encode_metadata_response, ApiVersion, ApiVersionsResponse,
    Broker, MetadataResponse, PartitionMetadata, TopicMetadata,
};
use partitionline::protocol::api_keys::{API_VERSIONS, FETCH, METADATA};
use partitionline::protocol::fetch::{decode_fetch_request, decode_fetch_response, FetchMetadata};
use partitionline::protocol::header::{decode_request_header, encode_response_header};
use partitionline::Consumer;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::sync::oneshot;
use tokio::task::{JoinHandle, JoinSet};

const FETCH_VERSION: i16 = 12;
const SESSION_ID: i32 = 91;
const TOPIC: &str = "pl-session";
const PEERS: &[&str] = &["3.9.1", "4.1.0", "4.1.2", "4.2.1", "4.3.1"];
const REPLAY_PEERS: &[&str] = &["4.1.0", "4.3.1"];
const REGISTERED: &[&str] = &["feature-fetch-incremental-session-runtime"];
const AUDIT_PIN: &str = "cb7e97d3b92a8555aea34d59266a2990c206395f";

fn fixture_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/fetch_session_runtime")
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

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct SessionHit {
    session_id: i32,
    epoch: i32,
    partitions: usize,
    forgotten: usize,
}

struct RawBroker {
    addr: String,
    shutdown_tx: Option<oneshot::Sender<()>>,
    task: Option<JoinHandle<()>>,
    last_fetch_version: Arc<AtomicUsize>,
    sessions: Arc<Mutex<Vec<SessionHit>>>,
}

impl RawBroker {
    async fn start(topic: &str, partition_count: usize, responses: Vec<Vec<u8>>) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind ephemeral loopback port");
        let address = listener.local_addr().expect("local addr");
        let addr = address.to_string();
        let last_fetch_version = Arc::new(AtomicUsize::new(0));
        let sessions = Arc::new(Mutex::new(Vec::new()));
        let attempts = Arc::new(AtomicUsize::new(0));
        let (shutdown_tx, mut shutdown_rx) = oneshot::channel();
        let topic_name = topic.to_string();
        let last_fetch_version_outer = Arc::clone(&last_fetch_version);
        let sessions_outer = Arc::clone(&sessions);
        let attempts_outer = Arc::clone(&attempts);
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
                        let sessions = Arc::clone(&sessions_outer);
                        let attempts = Arc::clone(&attempts_outer);
                        let responses = Arc::clone(&responses);
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
                                            cluster_id: Some("pl-session".into()),
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
                                        match decode_fetch_request(&mut request, header.api_version) {
                                            Ok((_, _, topics, _, session, forgotten, ..)) => {
                                                let partitions = topics.iter().map(|t| t.partitions.len()).sum();
                                                let forgotten = forgotten.iter().map(|t| t.partitions.len()).sum();
                                                sessions.lock().push(SessionHit {
                                                    session_id: session.session_id(),
                                                    epoch: session.epoch(),
                                                    partitions,
                                                    forgotten,
                                                });
                                            }
                                            Err(_) => return,
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
            sessions,
        }
    }

    fn config(&self) -> partitionline::ConsumerConfig {
        partitionline::ConsumerConfig::bootstrap([self.addr.clone()])
            .request_timeout(Duration::from_millis(800))
            .retry_backoff(Duration::from_millis(1))
    }

    fn last_fetch_version(&self) -> Option<i16> {
        i16::try_from(self.last_fetch_version.load(Ordering::SeqCst))
            .ok()
            .filter(|v| *v != 0)
    }

    fn sessions(&self) -> Vec<SessionHit> {
        self.sessions.lock().clone()
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

fn prefixes_must_fail(bytes: &[u8], what: &str, request: bool) {
    for prefix in 0..bytes.len() {
        let mut cur = &bytes[..prefix];
        let result = if request {
            decode_fetch_request(&mut cur, FETCH_VERSION).err()
        } else {
            decode_fetch_response(&mut cur, FETCH_VERSION).err()
        };
        assert!(result.is_some(), "{what} prefix {prefix} decoded");
    }
}

#[tokio::test]
async fn apache_session_bodies_decode_leftover_empty() {
    let root = fixture_root();
    let manifest = String::from_utf8(read(&root.join("manifest.json")).await).unwrap();
    assert!(manifest.contains("\"task\": \"KL01-24\""));
    assert!(manifest.contains("\"total_bodies\": 50"));
    assert!(manifest.contains("\"topic\": \"pl-session\""));
    let mut bodies = 0u32;
    for peer in PEERS {
        let dir = root.join(peer);
        let expected = String::from_utf8(read(&dir.join("expected.json")).await).unwrap();
        assert!(expected.contains("\"s01_session\":[0,0]"));
        assert!(expected.contains("\"s02_session\":[91,1]"));
        assert!(expected.contains("\"s03_forgotten\":[1]"));
        assert!(expected.contains("\"s04_error\":70"));
        assert!(expected.contains("\"s05_session\":[0,0]"));
        assert!(expected.contains("\"s06_error\":71"));
        assert!(expected.contains("\"s07_session\":[91,0]"));

        let s01_req = read(&dir.join("s01-full-request.bin")).await;
        let s01_resp = read(&dir.join("s01-full-response.bin")).await;
        prefixes_must_fail(&s01_req, &format!("{peer} s01 request"), true);
        prefixes_must_fail(&s01_resp, &format!("{peer} s01 response"), false);
        let mut cur = s01_req.as_slice();
        let (_, _, topics, _, session, forgotten, ..) =
            decode_fetch_request(&mut cur, FETCH_VERSION).unwrap();
        leftover(cur, &format!("{peer} s01 request"));
        assert_eq!(session, FetchMetadata::INITIAL);
        assert_ne!(session, FetchMetadata::LEGACY);
        assert_eq!(topics[0].topic, TOPIC);
        assert_eq!(topics[0].partitions.len(), 2);
        assert!(forgotten.is_empty());
        let mut cur = s01_resp.as_slice();
        let (topics, _, error, session_id, _) =
            decode_fetch_response(&mut cur, FETCH_VERSION).unwrap();
        leftover(cur, &format!("{peer} s01 response"));
        assert_eq!(error, 0);
        assert_eq!(session_id, SESSION_ID);
        assert_eq!(topics[0].partitions.len(), 2);
        bodies += 2;

        let s02_req = read(&dir.join("s02-incremental-request.bin")).await;
        let s02_resp = read(&dir.join("s02-incremental-response.bin")).await;
        let mut cur = s02_req.as_slice();
        let (_, _, topics, _, session, forgotten, ..) =
            decode_fetch_request(&mut cur, FETCH_VERSION).unwrap();
        leftover(cur, &format!("{peer} s02 request"));
        assert_eq!(session, FetchMetadata::new(SESSION_ID, 1));
        assert!(topics.is_empty());
        assert!(forgotten.is_empty());
        let mut cur = s02_resp.as_slice();
        let (topics, _, error, session_id, _) =
            decode_fetch_response(&mut cur, FETCH_VERSION).unwrap();
        leftover(cur, &format!("{peer} s02 response"));
        assert_eq!(error, 0);
        assert_eq!(session_id, SESSION_ID);
        assert!(topics.is_empty());
        bodies += 2;

        let s03_req = read(&dir.join("s03-forgotten-request.bin")).await;
        let s03_resp = read(&dir.join("s03-forgotten-response.bin")).await;
        let mut cur = s03_req.as_slice();
        let (_, _, topics, _, session, forgotten, ..) =
            decode_fetch_request(&mut cur, FETCH_VERSION).unwrap();
        leftover(cur, &format!("{peer} s03 request"));
        assert_eq!(session, FetchMetadata::new(SESSION_ID, 2));
        assert!(topics.is_empty());
        assert_eq!(forgotten[0].topic, TOPIC);
        assert_eq!(forgotten[0].partitions, vec![1]);
        let mut cur = s03_resp.as_slice();
        let (_, _, error, session_id, _) = decode_fetch_response(&mut cur, FETCH_VERSION).unwrap();
        leftover(cur, &format!("{peer} s03 response"));
        assert_eq!(error, 0);
        assert_eq!(session_id, SESSION_ID);
        bodies += 2;

        let s04 = read(&dir.join("s04-not-found-response.bin")).await;
        let mut cur = s04.as_slice();
        let (_, _, error, session_id, _) = decode_fetch_response(&mut cur, FETCH_VERSION).unwrap();
        leftover(cur, &format!("{peer} s04 response"));
        assert_eq!(error, FETCH_SESSION_ID_NOT_FOUND);
        assert_eq!(session_id, 0);
        bodies += 1;

        let s05 = read(&dir.join("s05-after-not-found-request.bin")).await;
        let mut cur = s05.as_slice();
        let (_, _, topics, _, session, ..) = decode_fetch_request(&mut cur, FETCH_VERSION).unwrap();
        leftover(cur, &format!("{peer} s05 request"));
        assert_eq!(session, FetchMetadata::INITIAL);
        assert_eq!(topics[0].partitions.len(), 2);
        bodies += 1;

        let s06 = read(&dir.join("s06-invalid-epoch-response.bin")).await;
        let mut cur = s06.as_slice();
        let (_, _, error, session_id, _) = decode_fetch_response(&mut cur, FETCH_VERSION).unwrap();
        leftover(cur, &format!("{peer} s06 response"));
        assert_eq!(error, INVALID_FETCH_SESSION_EPOCH);
        assert_eq!(session_id, 0);
        bodies += 1;

        let s07 = read(&dir.join("s07-after-invalid-epoch-request.bin")).await;
        let mut cur = s07.as_slice();
        let (_, _, topics, _, session, ..) = decode_fetch_request(&mut cur, FETCH_VERSION).unwrap();
        leftover(cur, &format!("{peer} s07 request"));
        assert_eq!(
            session,
            FetchMetadata::new(SESSION_ID, FetchMetadata::INITIAL_EPOCH)
        );
        assert!(session.is_full());
        assert_eq!(topics[0].partitions.len(), 2);
        bodies += 1;
    }
    assert_eq!(bodies, 50, "five peers × 10 Apache bodies");
}

#[tokio::test]
async fn apache_session_bodies_replay_public_consumer_histories() {
    for peer in REPLAY_PEERS {
        let dir = fixture_root().join(peer);
        let full = read(&dir.join("s01-full-response.bin")).await;
        let not_found = read(&dir.join("s04-not-found-response.bin")).await;
        let mut broker = RawBroker::start(TOPIC, 2, vec![full, not_found]).await;
        let mut consumer = Consumer::new(broker.config()).await.unwrap();
        consumer
            .assign_many([((TOPIC, 0), 0), ((TOPIC, 1), 0)])
            .await
            .unwrap();
        let records = consumer.fetch().await.unwrap();
        assert_eq!(records.len(), 2, "{peer} full open records");
        let first = broker.sessions();
        assert!(
            first.iter().any(|hit| {
                hit.session_id == 0
                    && hit.epoch == FetchMetadata::INITIAL_EPOCH
                    && hit.partitions == 2
            }),
            "{peer} first request must be INITIAL, not LEGACY: {first:?}"
        );
        assert!(
            first
                .iter()
                .all(|hit| hit.epoch != FetchMetadata::FINAL_EPOCH),
            "{peer} must not send LEGACY epoch: {first:?}"
        );
        assert_eq!(broker.last_fetch_version(), Some(FETCH_VERSION));

        let second = tokio::time::timeout(Duration::from_millis(800), consumer.fetch()).await;
        let hits = broker.sessions();
        assert!(
            hits.iter()
                .any(|hit| hit.session_id == SESSION_ID && hit.epoch == 1),
            "{peer} incremental request missing: {hits:?} second={second:?}"
        );
        assert!(
            hits.iter()
                .any(|hit| hit.session_id == 0 && hit.epoch == FetchMetadata::INITIAL_EPOCH),
            "{peer} NOT_FOUND must reset to INITIAL: {hits:?}"
        );
        consumer.close().await.unwrap();
        broker.shutdown().await;
    }
}

#[tokio::test]
async fn frozen_session_disposition_stays_not_run() {
    let cases = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/conformance/cases.json");
    let text = String::from_utf8(read(&cases).await).unwrap();
    for case_id in REGISTERED {
        assert!(
            text.contains(&format!("\"id\": \"{case_id}\""))
                && text.contains(&format!("\"source_pin\": \"{AUDIT_PIN}\""))
                && text.contains("\"disposition\": \"not_run\"")
                && text.contains("\"adapter\": \"none\""),
            "{case_id} must remain the frozen not_run pin"
        );
    }
}

#[tokio::test]
async fn rust_session_output_decodes_with_pinned_apache_when_jars_present() {
    let jars = PathBuf::from("/tmp/partitionline-conformance");
    let slf4j = jars.join("slf4j-api-1.7.36.jar");
    let source = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests/conformance/java/ConformanceFetchSessionRuntime.java");
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
        let classes = PathBuf::from("/tmp").join(format!("fetch-session-runtime-classes-{peer}"));
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
                "fetch-request",
                dir.join("s01-full-request.bin"),
                "\"session_id\":0",
            ),
            (
                "fetch-request",
                dir.join("s02-incremental-request.bin"),
                "\"epoch\":1",
            ),
            (
                "fetch-request",
                dir.join("s03-forgotten-request.bin"),
                "\"forgotten\":1",
            ),
            (
                "fetch-response",
                dir.join("s04-not-found-response.bin"),
                "\"error\":70",
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
                    "ConformanceFetchSessionRuntime",
                    "decode-rust",
                    jar.to_str().unwrap(),
                    kind,
                    &FETCH_VERSION.to_string(),
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

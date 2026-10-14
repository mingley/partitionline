//! KL01-25: independently qualify Produce/Fetch quota throttle runtime.
//!
//! Consumes committed Apache kafka-clients Produce v9 and Fetch v12
//! ThrottleTimeMs bodies. The registered
//! `feature-produce-fetch-quota-throttle-runtime` row stays `not_run` in
//! `cases.json`. Local throttle unit tests are not the oracle: leftover-empty
//! decode reads Apache bytes, and the loopback broker serves those bytes
//! while advertising PRODUCE max v9 and FETCH max v12.

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
use std::time::{Duration, Instant};

use parking_lot::Mutex;

use bytes::BytesMut;
use partitionline::protocol::api::{
    decode_produce_request, decode_produce_response, encode_api_versions_response,
    encode_metadata_response, ApiVersion, ApiVersionsResponse, Broker, MetadataResponse,
    PartitionMetadata, ProduceResponse, TopicMetadata,
};
use partitionline::protocol::api_keys::{API_VERSIONS, FETCH, METADATA, PRODUCE};
use partitionline::protocol::fetch::{
    decode_fetch_request, decode_fetch_response, FetchMetadata, FetchResponse,
};
use partitionline::protocol::header::{decode_request_header, encode_response_header};
use partitionline::{Consumer, ProduceRecord, Producer, ProducerConfig};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::sync::oneshot;
use tokio::task::{JoinHandle, JoinSet};

const PRODUCE_VERSION: i16 = 9;
const FETCH_VERSION: i16 = 12;
const SESSION_ID: i32 = 91;
const TOPIC: &str = "pl-quota";
const PRODUCE_QUOTA_MS: i32 = 250;
const FETCH_QUOTA_MS: i32 = 80;
const PEERS: &[&str] = &["3.9.1", "4.1.0", "4.1.2", "4.2.1", "4.3.1"];
const REPLAY_PEERS: &[&str] = &["4.1.0", "4.3.1"];
const REGISTERED: &[&str] = &["feature-produce-fetch-quota-throttle-runtime"];
const AUDIT_PIN: &str = "cb7e97d3b92a8555aea34d59266a2990c206395f";

fn fixture_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/quota_throttle_runtime")
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
}

struct RawBroker {
    addr: String,
    shutdown_tx: Option<oneshot::Sender<()>>,
    task: Option<JoinHandle<()>>,
    last_produce_version: Arc<AtomicUsize>,
    last_fetch_version: Arc<AtomicUsize>,
    sessions: Arc<Mutex<Vec<SessionHit>>>,
}

impl RawBroker {
    async fn start(topic: &str, produce: Vec<Vec<u8>>, fetch: Vec<Vec<u8>>) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind ephemeral loopback port");
        let address = listener.local_addr().expect("local addr");
        let addr = address.to_string();
        let last_produce_version = Arc::new(AtomicUsize::new(0));
        let last_fetch_version = Arc::new(AtomicUsize::new(0));
        let sessions = Arc::new(Mutex::new(Vec::new()));
        let produce_attempts = Arc::new(AtomicUsize::new(0));
        let fetch_attempts = Arc::new(AtomicUsize::new(0));
        let (shutdown_tx, mut shutdown_rx) = oneshot::channel();
        let topic_name = topic.to_string();
        let last_produce_version_outer = Arc::clone(&last_produce_version);
        let last_fetch_version_outer = Arc::clone(&last_fetch_version);
        let sessions_outer = Arc::clone(&sessions);
        let produce_attempts_outer = Arc::clone(&produce_attempts);
        let fetch_attempts_outer = Arc::clone(&fetch_attempts);
        let produce = Arc::new(produce);
        let fetch = Arc::new(fetch);
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
                        let last_fetch_version = Arc::clone(&last_fetch_version_outer);
                        let sessions = Arc::clone(&sessions_outer);
                        let produce_attempts = Arc::clone(&produce_attempts_outer);
                        let fetch_attempts = Arc::clone(&fetch_attempts_outer);
                        let produce = Arc::clone(&produce);
                        let fetch = Arc::clone(&fetch);
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
                                            (FETCH, 4, FETCH_VERSION),
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
                                            cluster_id: Some("pl-quota".into()),
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
                                        if decode_produce_request(&mut request, header.api_version).is_err() {
                                            return;
                                        }
                                        let attempt = produce_attempts.fetch_add(1, Ordering::SeqCst);
                                        let body = produce
                                            .get(attempt)
                                            .or_else(|| produce.last())
                                            .cloned()
                                            .unwrap_or_default();
                                        response.extend_from_slice(&body);
                                    }
                                    FETCH => {
                                        last_fetch_version.store(
                                            usize::try_from(header.api_version).expect("nonnegative Fetch version"),
                                            Ordering::SeqCst,
                                        );
                                        match decode_fetch_request(&mut request, header.api_version) {
                                            Ok((_, _, topics, _, session, ..)) => {
                                                let partitions = topics.iter().map(|t| t.partitions.len()).sum();
                                                sessions.lock().push(SessionHit {
                                                    session_id: session.session_id(),
                                                    epoch: session.epoch(),
                                                    partitions,
                                                });
                                            }
                                            Err(_) => return,
                                        }
                                        let attempt = fetch_attempts.fetch_add(1, Ordering::SeqCst);
                                        let body = fetch
                                            .get(attempt)
                                            .or_else(|| fetch.last())
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
            last_produce_version,
            last_fetch_version,
            sessions,
        }
    }

    fn producer_config(&self) -> ProducerConfig {
        ProducerConfig::bootstrap([self.addr.clone()])
            .linger(Duration::ZERO)
            .request_timeout(Duration::from_millis(800))
            .retry_backoff(Duration::from_millis(1))
    }

    fn consumer_config(&self) -> partitionline::ConsumerConfig {
        partitionline::ConsumerConfig::bootstrap([self.addr.clone()])
            .request_timeout(Duration::from_millis(800))
            .retry_backoff(Duration::from_millis(1))
    }

    fn last_produce_version(&self) -> Option<i16> {
        i16::try_from(self.last_produce_version.load(Ordering::SeqCst))
            .ok()
            .filter(|v| *v != 0)
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

fn prefixes_must_fail(bytes: &[u8], what: &str, mut decode: impl FnMut(&[u8]) -> bool) {
    for prefix in 0..bytes.len() {
        assert!(decode(&bytes[..prefix]), "{what} prefix {prefix} decoded");
    }
}

#[tokio::test]
async fn apache_quota_bodies_decode_leftover_empty() {
    let root = fixture_root();
    let manifest = String::from_utf8(read(&root.join("manifest.json")).await).unwrap();
    assert!(manifest.contains("\"task\": \"KL01-25\""));
    assert!(manifest.contains("\"total_bodies\": 40"));
    assert!(manifest.contains("\"topic\": \"pl-quota\""));
    assert!(ProduceResponse::should_client_throttle(PRODUCE_VERSION));
    assert!(!ProduceResponse::should_client_throttle(5));
    assert!(FetchResponse::should_client_throttle(FETCH_VERSION));
    assert!(!FetchResponse::should_client_throttle(7));
    let mut bodies = 0u32;
    for peer in PEERS {
        let dir = root.join(peer);
        let expected = String::from_utf8(read(&dir.join("expected.json")).await).unwrap();
        assert!(expected.contains("\"produce_quota_throttle\":250"));
        assert!(expected.contains("\"produce_negative_throttle\":-1"));
        assert!(expected.contains("\"fetch_quota_throttle\":80"));
        assert!(expected.contains("\"fetch_empty_throttled\":80"));
        assert!(expected.contains("\"produce_should_client_throttle_v9\":true"));
        assert!(expected.contains("\"produce_should_client_throttle_v5\":false"));
        assert!(expected.contains("\"fetch_should_client_throttle_v12\":true"));
        assert!(expected.contains("\"fetch_should_client_throttle_v7\":false"));

        let p01_req = read(&dir.join("p01-produce-request.bin")).await;
        let p01_resp = read(&dir.join("p01-produce-zero-response.bin")).await;
        prefixes_must_fail(&p01_req, &format!("{peer} Produce request"), |buf| {
            decode_produce_request(&mut &buf[..], PRODUCE_VERSION).is_err()
        });
        prefixes_must_fail(&p01_resp, &format!("{peer} Produce zero"), |buf| {
            decode_produce_response(&mut &buf[..], PRODUCE_VERSION).is_err()
        });
        let mut cur = p01_req.as_slice();
        let (txn, acks, timeout_ms, topics) =
            decode_produce_request(&mut cur, PRODUCE_VERSION).unwrap();
        leftover(cur, &format!("{peer} Produce request"));
        assert!(txn.is_none());
        assert_eq!(acks, -1);
        assert_eq!(timeout_ms, 30_000);
        assert_eq!(topics[0].topic, TOPIC);
        assert_eq!(topics[0].partitions[0].records.records.len(), 1);
        assert_eq!(
            topics[0].partitions[0].records.records[0].value.as_deref(),
            Some(&b"q"[..])
        );
        let mut cur = p01_resp.as_slice();
        let (parts, _, throttle) = decode_produce_response(&mut cur, PRODUCE_VERSION).unwrap();
        leftover(cur, &format!("{peer} Produce zero"));
        assert_eq!(throttle, 0);
        assert_eq!(parts[0].base_offset, 0);
        assert_eq!(parts[0].error_code, 0);
        bodies += 2;

        let p02 = read(&dir.join("p02-produce-quota-response.bin")).await;
        let mut cur = p02.as_slice();
        let (parts, _, throttle) = decode_produce_response(&mut cur, PRODUCE_VERSION).unwrap();
        leftover(cur, &format!("{peer} Produce quota"));
        assert_eq!(throttle, PRODUCE_QUOTA_MS);
        assert_eq!(parts[0].topic, TOPIC);
        bodies += 1;

        let p03 = read(&dir.join("p03-produce-negative-response.bin")).await;
        let mut cur = p03.as_slice();
        let (_, _, throttle) = decode_produce_response(&mut cur, PRODUCE_VERSION).unwrap();
        leftover(cur, &format!("{peer} Produce negative"));
        assert_eq!(throttle, -1);
        bodies += 1;

        let f01_req = read(&dir.join("f01-fetch-request.bin")).await;
        let f01_resp = read(&dir.join("f01-fetch-zero-response.bin")).await;
        prefixes_must_fail(&f01_req, &format!("{peer} Fetch request"), |buf| {
            decode_fetch_request(&mut &buf[..], FETCH_VERSION).is_err()
        });
        prefixes_must_fail(&f01_resp, &format!("{peer} Fetch zero"), |buf| {
            decode_fetch_response(&mut &buf[..], FETCH_VERSION).is_err()
        });
        let mut cur = f01_req.as_slice();
        let (_, _, topics, _, session, forgotten, ..) =
            decode_fetch_request(&mut cur, FETCH_VERSION).unwrap();
        leftover(cur, &format!("{peer} Fetch request"));
        assert_eq!(session, FetchMetadata::INITIAL);
        assert_ne!(session, FetchMetadata::LEGACY);
        assert_eq!(topics[0].topic, TOPIC);
        assert_eq!(topics[0].partitions.len(), 1);
        assert!(forgotten.is_empty());
        let mut cur = f01_resp.as_slice();
        let (topics, _, error, session_id, throttle) =
            decode_fetch_response(&mut cur, FETCH_VERSION).unwrap();
        leftover(cur, &format!("{peer} Fetch zero"));
        assert_eq!(error, 0);
        assert_eq!(session_id, SESSION_ID);
        assert_eq!(throttle, 0);
        assert_eq!(topics[0].partitions[0].records[0].records.len(), 1);
        bodies += 2;

        let f02 = read(&dir.join("f02-fetch-quota-response.bin")).await;
        let mut cur = f02.as_slice();
        let (topics, _, error, session_id, throttle) =
            decode_fetch_response(&mut cur, FETCH_VERSION).unwrap();
        leftover(cur, &format!("{peer} Fetch quota"));
        assert_eq!(error, 0);
        assert_eq!(session_id, SESSION_ID);
        assert_eq!(throttle, FETCH_QUOTA_MS);
        assert_eq!(topics[0].partitions.len(), 1);
        bodies += 1;

        let f03 = read(&dir.join("f03-empty-throttled-response.bin")).await;
        let mut cur = f03.as_slice();
        let (topics, _, error, session_id, throttle) =
            decode_fetch_response(&mut cur, FETCH_VERSION).unwrap();
        leftover(cur, &format!("{peer} empty throttled"));
        assert_eq!(error, 0);
        assert_eq!(session_id, SESSION_ID);
        assert_eq!(throttle, FETCH_QUOTA_MS);
        assert!(topics.is_empty());
        bodies += 1;
    }
    assert_eq!(bodies, 40, "five peers × 8 Apache bodies");
}

#[tokio::test]
async fn apache_quota_bodies_replay_public_histories() {
    for peer in REPLAY_PEERS {
        let dir = fixture_root().join(peer);
        let produce_quota = read(&dir.join("p02-produce-quota-response.bin")).await;
        let produce_zero = read(&dir.join("p01-produce-zero-response.bin")).await;
        let produce_neg = read(&dir.join("p03-produce-negative-response.bin")).await;
        let fetch_quota = read(&dir.join("f02-fetch-quota-response.bin")).await;
        let fetch_zero = read(&dir.join("f01-fetch-zero-response.bin")).await;
        let empty_throttled = read(&dir.join("f03-empty-throttled-response.bin")).await;

        let mut broker = RawBroker::start(
            TOPIC,
            vec![produce_quota.clone(), produce_zero.clone()],
            Vec::new(),
        )
        .await;
        let producer = Producer::new(broker.producer_config()).await.unwrap();
        let first = producer
            .send(ProduceRecord::to(TOPIC).value(&b"q"[..]))
            .await
            .unwrap();
        assert_eq!(first.offset, 0, "{peer} produce quota offset");
        assert_eq!(broker.last_produce_version(), Some(PRODUCE_VERSION));
        let throttle = producer.metrics().throttle;
        assert_eq!(throttle.responses, 1, "{peer} produce quota responses");
        assert_eq!(throttle.requested_millis, 250);
        assert_eq!(throttle.max_millis, 250);
        assert_eq!(throttle.invalid_responses, 0);
        let started = Instant::now();
        let second = producer
            .send(ProduceRecord::to(TOPIC).value(&b"q"[..]))
            .await
            .unwrap();
        let waited = started.elapsed();
        assert_eq!(second.offset, 0, "{peer} produce after quota");
        assert!(
            waited >= Duration::from_millis(200),
            "{peer} produce quota wait too short: {waited:?}"
        );
        let after_zero = producer.metrics().throttle;
        assert_eq!(after_zero.responses, 1, "{peer} zero must not count");
        producer.close().await.unwrap();
        broker.shutdown().await;

        let mut broker = RawBroker::start(TOPIC, vec![produce_neg, produce_zero], Vec::new()).await;
        let producer = Producer::new(broker.producer_config()).await.unwrap();
        let first_neg = producer
            .send(ProduceRecord::to(TOPIC).value(&b"q"[..]))
            .await
            .unwrap();
        assert_eq!(first_neg.offset, 0, "{peer} produce negative offset");
        let neg = producer.metrics().throttle;
        assert_eq!(neg.responses, 0, "{peer} negative is not a quota");
        assert_eq!(neg.invalid_responses, 1, "{peer} negative invalid count");
        let started = Instant::now();
        let second_neg = producer
            .send(ProduceRecord::to(TOPIC).value(&b"q"[..]))
            .await
            .unwrap();
        assert_eq!(second_neg.offset, 0, "{peer} produce after negative");
        assert!(
            started.elapsed() < Duration::from_millis(150),
            "{peer} negative must not unmute a wait: {:?}",
            started.elapsed()
        );
        producer.close().await.unwrap();
        broker.shutdown().await;

        let mut broker = RawBroker::start(
            TOPIC,
            Vec::new(),
            vec![fetch_quota.clone(), fetch_zero.clone()],
        )
        .await;
        let mut consumer = Consumer::new(broker.consumer_config()).await.unwrap();
        consumer.assign_many([((TOPIC, 0), 0)]).await.unwrap();
        let records = consumer.fetch().await.unwrap();
        assert_eq!(records.len(), 1, "{peer} fetch quota records");
        assert_eq!(broker.last_fetch_version(), Some(FETCH_VERSION));
        let first = broker.sessions();
        assert!(
            first.iter().any(|hit| {
                hit.session_id == 0
                    && hit.epoch == FetchMetadata::INITIAL_EPOCH
                    && hit.partitions == 1
            }),
            "{peer} first fetch must be INITIAL: {first:?}"
        );
        let fetch_metrics = consumer.metrics().throttle;
        assert_eq!(fetch_metrics.responses, 1, "{peer} fetch quota responses");
        assert_eq!(fetch_metrics.requested_millis, 80);
        let started = Instant::now();
        let second = tokio::time::timeout(Duration::from_millis(800), consumer.fetch()).await;
        let waited = started.elapsed();
        assert!(
            waited >= Duration::from_millis(50),
            "{peer} fetch quota wait too short: {waited:?} second={second:?}"
        );
        consumer.close().await.unwrap();
        broker.shutdown().await;

        let mut broker =
            RawBroker::start(TOPIC, Vec::new(), vec![empty_throttled, fetch_zero]).await;
        let mut consumer = Consumer::new(broker.consumer_config()).await.unwrap();
        consumer.assign_many([((TOPIC, 0), 0)]).await.unwrap();
        let first_fetch = tokio::time::timeout(Duration::from_millis(800), consumer.fetch()).await;
        assert!(
            first_fetch.is_ok(),
            "{peer} empty throttled full fetch: {first_fetch:?}"
        );
        let _second = tokio::time::timeout(Duration::from_millis(800), consumer.fetch()).await;
        let hits = broker.sessions();
        assert!(
            hits.iter()
                .any(|hit| hit.session_id == 0 && hit.epoch == FetchMetadata::INITIAL_EPOCH),
            "{peer} empty throttled full starts INITIAL: {hits:?}"
        );
        assert!(
            hits.iter()
                .filter(|hit| hit.session_id == 0 && hit.epoch == FetchMetadata::INITIAL_EPOCH)
                .count()
                >= 2,
            "{peer} empty throttled full must reset to INITIAL, not incremental: {hits:?}"
        );
        assert!(
            hits.iter()
                .all(|hit| hit.epoch != FetchMetadata::FINAL_EPOCH),
            "{peer} must not send LEGACY epoch: {hits:?}"
        );
        consumer.close().await.unwrap();
        broker.shutdown().await;
    }
}

#[tokio::test]
async fn frozen_quota_disposition_stays_not_run() {
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
async fn rust_quota_output_decodes_with_pinned_apache_when_jars_present() {
    let jars = PathBuf::from("/tmp/partitionline-conformance");
    let slf4j = jars.join("slf4j-api-1.7.36.jar");
    let source = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests/conformance/java/ConformanceQuotaThrottleRuntime.java");
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
        let classes = PathBuf::from("/tmp").join(format!("quota-throttle-runtime-classes-{peer}"));
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
                dir.join("p01-produce-request.bin"),
                "\"acks\":-1",
            ),
            (
                "produce-response",
                PRODUCE_VERSION,
                dir.join("p02-produce-quota-response.bin"),
                "\"throttle\":250",
            ),
            (
                "fetch-response",
                FETCH_VERSION,
                dir.join("f02-fetch-quota-response.bin"),
                "\"throttle\":80",
            ),
            (
                "fetch-response",
                FETCH_VERSION,
                dir.join("f03-empty-throttled-response.bin"),
                "\"topics\":0",
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
                    "ConformanceQuotaThrottleRuntime",
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

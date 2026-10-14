//! KL01-27: independently qualify finite verifiable reset and autocommit
//! profiles.
//!
//! Consumes committed Apache kafka-clients Fetch v12, ListOffsets v5,
//! OffsetCommit v8 and OffsetFetch v8 bodies for hashed VerifiableConsumer
//! `--reset-policy` / `--enable-autocommit` cells. The registered
//! `verifiable-consumer-reset-policies-3-9-1` and
//! `verifiable-consumer-autocommit-3-9-1` rows stay `local_consistency`
//! in `cases.json`. Local `verifiable_contract` mock output is not the
//! oracle.

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
    encode_api_versions_response, encode_metadata_response, ApiVersion, ApiVersionsResponse,
    Broker, MetadataResponse, PartitionMetadata, TopicMetadata,
};
use partitionline::protocol::api_keys::{API_VERSIONS, FETCH, LIST_OFFSETS, METADATA};
use partitionline::protocol::fetch::{decode_fetch_request, decode_fetch_response};
use partitionline::protocol::group::{
    decode_offset_commit_request_data, decode_offset_commit_response_data,
    decode_offset_fetch_request_data, decode_offset_fetch_response_data,
    encode_offset_commit_request_data, encode_offset_commit_response_data,
    encode_offset_fetch_request_data, encode_offset_fetch_response_data, OffsetLimits,
    OffsetTopicIdentity,
};
use partitionline::protocol::header::{decode_request_header, encode_response_header};
use partitionline::protocol::offsets::{
    decode_list_offsets_request, decode_list_offsets_topics_request,
    decode_list_offsets_topics_response,
};
use partitionline::{AutoOffsetReset, Consumer, TopicPartition, LATEST_TIMESTAMP};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::sync::oneshot;
use tokio::task::{JoinHandle, JoinSet};

const FETCH_VERSION: i16 = 12;
const LIST_OFFSETS_VERSION: i16 = 5;
const OFFSET_COMMIT_VERSION: i16 = 8;
const OFFSET_FETCH_VERSION: i16 = 8;
const TOPIC: &str = "pl-offsets";
const GROUP: &str = "pl-offsets";
const PEERS: &[&str] = &["3.9.1", "4.1.0", "4.1.2", "4.2.1", "4.3.1"];
const REPLAY_PEERS: &[&str] = &["4.1.0", "4.3.1"];
const REGISTERED: &[&str] = &[
    "verifiable-consumer-reset-policies-3-9-1",
    "verifiable-consumer-autocommit-3-9-1",
];
const AUDIT_PIN: &str = "3e1803560e081d44f8e5ae4a3e4b449f69ff0a54";
const NAMED_PAIRS: &[&str] = &[
    "list-offsets-latest",
    "offset-fetch-empty",
    "offset-fetch-stored",
    "fetch-new",
    "fetch-resume",
    "fetch-first",
    "offset-commit-next-2",
    "offset-commit-interval-1",
];

fn fixture_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/verifiable_consumer_offsets")
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
    last_list_offsets_version: Arc<AtomicUsize>,
}

impl RawBroker {
    async fn start(topic: &str, list_offsets: Vec<u8>, fetch: Vec<u8>) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind ephemeral loopback port");
        let address = listener.local_addr().expect("local addr");
        let addr = address.to_string();
        let last_fetch_version = Arc::new(AtomicUsize::new(0));
        let last_list_offsets_version = Arc::new(AtomicUsize::new(0));
        let (shutdown_tx, mut shutdown_rx) = oneshot::channel();
        let topic_name = topic.to_string();
        let last_fetch_version_outer = Arc::clone(&last_fetch_version);
        let last_list_offsets_version_outer = Arc::clone(&last_list_offsets_version);
        let list_offsets = Arc::new(list_offsets);
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
                        let last_fetch_version = Arc::clone(&last_fetch_version_outer);
                        let last_list_offsets_version = Arc::clone(&last_list_offsets_version_outer);
                        let list_offsets = Arc::clone(&list_offsets);
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
                                            (LIST_OFFSETS, 1, LIST_OFFSETS_VERSION),
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
                                            cluster_id: Some("pl-offsets".into()),
                                            controller_id: 0,
                                            topics: vec![topic],
                                            cluster_authorized_operations: i32::MIN,
                                            error_code: 0,
                                        };
                                        if encode_metadata_response(&mut response, header.api_version, &metadata).is_err() {
                                            return;
                                        }
                                    }
                                    LIST_OFFSETS => {
                                        last_list_offsets_version.store(
                                            usize::try_from(header.api_version)
                                                .expect("nonnegative ListOffsets version"),
                                            Ordering::SeqCst,
                                        );
                                        if decode_list_offsets_request(&mut request, header.api_version).is_err() {
                                            return;
                                        }
                                        response.extend_from_slice(&list_offsets);
                                    }
                                    FETCH => {
                                        last_fetch_version.store(
                                            usize::try_from(header.api_version).expect("nonnegative Fetch version"),
                                            Ordering::SeqCst,
                                        );
                                        if decode_fetch_request(&mut request, header.api_version).is_err() {
                                            return;
                                        }
                                        response.extend_from_slice(&fetch);
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
            last_list_offsets_version,
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

    fn last_list_offsets_version(&self) -> Option<i16> {
        i16::try_from(self.last_list_offsets_version.load(Ordering::SeqCst))
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

fn prefixes_must_fail_fetch(bytes: &[u8], request: bool, what: &str) {
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

fn prefixes_must_fail_list_offsets(bytes: &[u8], request: bool, what: &str) {
    for prefix in 0..bytes.len() {
        let mut cur = &bytes[..prefix];
        let result = if request {
            decode_list_offsets_topics_request(&mut cur, LIST_OFFSETS_VERSION).err()
        } else {
            decode_list_offsets_topics_response(&mut cur, LIST_OFFSETS_VERSION).err()
        };
        assert!(result.is_some(), "{what} prefix {prefix} decoded");
    }
}

#[tokio::test]
async fn apache_offset_bodies_decode_leftover_empty() {
    let root = fixture_root();
    let manifest = String::from_utf8(read(&root.join("manifest.json")).await).unwrap();
    assert!(manifest.contains("\"task\": \"KL01-27\""));
    assert!(manifest.contains("\"total_bodies\": 80"));
    assert!(manifest.contains("\"topic\": \"pl-offsets\""));
    let limits = OffsetLimits::default();
    let mut bodies = 0u32;
    for peer in PEERS {
        let dir = root.join(peer);
        let expected = String::from_utf8(read(&dir.join("expected.json")).await).unwrap();
        assert!(expected.contains("\"delivered_value\":\"new\""));
        assert!(expected.contains("\"commit_offset\":2"));
        assert!(expected.contains("\"delivered_value\":\"after-commit\""));
        assert!(expected.contains("\"interval_commit_offset\":1"));
        assert!(expected.contains("\"close_commit\":false"));
        assert!(expected.contains("\"offsets_committed_events\":0"));
        let profiles = String::from_utf8(read(&dir.join("profiles.json")).await).unwrap();
        assert!(profiles.contains("\"default_reset_policy\":\"earliest\""));
        assert!(profiles.contains("\"enable_autocommit_default\":false"));
        assert!(profiles.contains("\"autocommit_omits_offsets_committed\":true"));
        let events = String::from_utf8(read(&dir.join("events.json")).await).unwrap();
        assert!(events.contains("\"json_property_order\":[\"timestamp\",\"name\"]"));
        assert!(events.contains("\"value\":\"new\""));
        assert!(events.contains("\"value\":\"after-commit\""));
        assert!(events.contains("\"value\":\"first\""));
        assert!(events.contains("\"value\":\"second\""));
        assert!(events.contains("\"offset\":2"));
        let autocommit_events = events
            .split_once("\"autocommit\":")
            .map(|(_, rest)| rest)
            .expect("autocommit events");
        assert!(
            !autocommit_events.contains("\"offsets_committed\""),
            "{peer} autocommit must suppress offsets_committed"
        );

        let list_req = read(&dir.join("list-offsets-latest-request.bin")).await;
        let list_resp = read(&dir.join("list-offsets-latest-response.bin")).await;
        prefixes_must_fail_list_offsets(&list_req, true, &format!("{peer} list-offsets request"));
        prefixes_must_fail_list_offsets(
            &list_resp,
            false,
            &format!("{peer} list-offsets response"),
        );
        let mut cur = list_req.as_slice();
        let (isolation, topic, _, _, timestamp) =
            decode_list_offsets_request(&mut cur, LIST_OFFSETS_VERSION).unwrap();
        leftover(cur, &format!("{peer} list-offsets request"));
        assert_eq!(isolation, 0);
        assert_eq!(topic, TOPIC);
        assert_eq!(timestamp, LATEST_TIMESTAMP);
        let mut cur = list_resp.as_slice();
        let (topics, throttle) =
            decode_list_offsets_topics_response(&mut cur, LIST_OFFSETS_VERSION).unwrap();
        leftover(cur, &format!("{peer} list-offsets response"));
        assert_eq!(throttle, 0);
        assert_eq!(topics[0].partitions[0].offset, 1);
        bodies += 2;

        let empty_req = read(&dir.join("offset-fetch-empty-request.bin")).await;
        let empty_resp = read(&dir.join("offset-fetch-empty-response.bin")).await;
        let stored_req = read(&dir.join("offset-fetch-stored-request.bin")).await;
        let stored_resp = read(&dir.join("offset-fetch-stored-response.bin")).await;
        for prefix in 0..empty_req.len() {
            assert!(
                decode_offset_fetch_request_data(
                    &empty_req[..prefix],
                    OFFSET_FETCH_VERSION,
                    limits
                )
                .is_err(),
                "{peer} OffsetFetch empty request prefix {prefix}"
            );
        }
        let decoded_empty =
            decode_offset_fetch_request_data(&empty_req, OFFSET_FETCH_VERSION, limits).unwrap();
        assert_eq!(decoded_empty.groups[0].group_id, GROUP);
        assert_eq!(
            encode_offset_fetch_request_data(&decoded_empty, OFFSET_FETCH_VERSION, limits).unwrap(),
            empty_req
        );
        let decoded_empty_resp =
            decode_offset_fetch_response_data(&empty_resp, OFFSET_FETCH_VERSION, limits).unwrap();
        assert_eq!(
            decoded_empty_resp.groups[0].topics[0].partitions[0].committed_offset,
            -1
        );
        assert_eq!(
            encode_offset_fetch_response_data(&decoded_empty_resp, OFFSET_FETCH_VERSION, limits)
                .unwrap(),
            empty_resp
        );
        let decoded_stored =
            decode_offset_fetch_request_data(&stored_req, OFFSET_FETCH_VERSION, limits).unwrap();
        assert_eq!(
            encode_offset_fetch_request_data(&decoded_stored, OFFSET_FETCH_VERSION, limits)
                .unwrap(),
            stored_req
        );
        let decoded_stored_resp =
            decode_offset_fetch_response_data(&stored_resp, OFFSET_FETCH_VERSION, limits).unwrap();
        assert_eq!(
            decoded_stored_resp.groups[0].topics[0].partitions[0].committed_offset,
            2
        );
        assert_eq!(
            encode_offset_fetch_response_data(&decoded_stored_resp, OFFSET_FETCH_VERSION, limits)
                .unwrap(),
            stored_resp
        );
        bodies += 4;

        for (stem, fetch_offset, value) in [
            ("fetch-new", 1_i64, "new"),
            ("fetch-resume", 2, "after-commit"),
            ("fetch-first", 0, "first"),
        ] {
            let req = read(&dir.join(format!("{stem}-request.bin"))).await;
            let resp = read(&dir.join(format!("{stem}-response.bin"))).await;
            prefixes_must_fail_fetch(&req, true, &format!("{peer} {stem} request"));
            prefixes_must_fail_fetch(&resp, false, &format!("{peer} {stem} response"));
            let mut cur = req.as_slice();
            let (_, _, topics, ..) = decode_fetch_request(&mut cur, FETCH_VERSION).unwrap();
            leftover(cur, &format!("{peer} {stem} request"));
            assert_eq!(topics[0].partitions[0].fetch_offset, fetch_offset);
            let mut cur = resp.as_slice();
            let (topics, _, error, _, _) = decode_fetch_response(&mut cur, FETCH_VERSION).unwrap();
            leftover(cur, &format!("{peer} {stem} response"));
            assert_eq!(error, 0);
            assert_eq!(
                topics[0].partitions[0].records[0].records[0]
                    .value
                    .as_deref(),
                Some(value.as_bytes())
            );
            bodies += 2;
        }

        for (stem, committed) in [
            ("offset-commit-next-2", 2_i64),
            ("offset-commit-interval-1", 1),
        ] {
            let req = read(&dir.join(format!("{stem}-request.bin"))).await;
            let resp = read(&dir.join(format!("{stem}-response.bin"))).await;
            for prefix in 0..req.len() {
                assert!(
                    decode_offset_commit_request_data(
                        &req[..prefix],
                        OFFSET_COMMIT_VERSION,
                        limits
                    )
                    .is_err(),
                    "{peer} {stem} request prefix {prefix}"
                );
            }
            let decoded =
                decode_offset_commit_request_data(&req, OFFSET_COMMIT_VERSION, limits).unwrap();
            assert_eq!(decoded.group_id, GROUP);
            assert_eq!(decoded.member_id, "member-a");
            assert_eq!(decoded.generation_id_or_member_epoch, 1);
            assert_eq!(
                decoded.topics[0].identity,
                OffsetTopicIdentity::Name(TOPIC.into())
            );
            assert_eq!(decoded.topics[0].partitions[0].committed_offset, committed);
            assert_eq!(
                encode_offset_commit_request_data(&decoded, OFFSET_COMMIT_VERSION, limits).unwrap(),
                req
            );
            let decoded_resp =
                decode_offset_commit_response_data(&resp, OFFSET_COMMIT_VERSION, limits).unwrap();
            assert_eq!(decoded_resp.topics[0].partitions[0].error_code, 0);
            assert_eq!(
                encode_offset_commit_response_data(&decoded_resp, OFFSET_COMMIT_VERSION, limits)
                    .unwrap(),
                resp
            );
            bodies += 2;
        }

        for stem in NAMED_PAIRS {
            assert!(dir.join(format!("{stem}-request.bin")).exists());
            assert!(dir.join(format!("{stem}-response.bin")).exists());
        }
    }
    assert_eq!(bodies, 80, "five peers × 16 Apache bodies");
}

#[tokio::test]
async fn apache_reset_autocommit_replay_public_consumer() {
    for peer in REPLAY_PEERS {
        let dir = fixture_root().join(peer);
        let list = read(&dir.join("list-offsets-latest-response.bin")).await;
        let fetch_new = read(&dir.join("fetch-new-response.bin")).await;
        let mut broker = RawBroker::start(TOPIC, list.clone(), fetch_new).await;
        let mut consumer =
            Consumer::new(broker.config().auto_offset_reset(AutoOffsetReset::Latest))
                .await
                .unwrap();
        consumer
            .assign_partitions([TopicPartition::new(TOPIC, 0)])
            .await
            .unwrap();
        let records = consumer.fetch().await.unwrap();
        assert_eq!(
            records.iter().map(|r| r.offset).collect::<Vec<_>>(),
            vec![1],
            "{peer} latest offsets"
        );
        assert_eq!(
            records[0].value.as_deref(),
            Some(&b"new"[..]),
            "{peer} latest"
        );
        assert_eq!(
            consumer.position(TOPIC, 0).unwrap(),
            2,
            "{peer} latest next"
        );
        assert_eq!(
            broker.last_list_offsets_version(),
            Some(LIST_OFFSETS_VERSION)
        );
        assert_eq!(broker.last_fetch_version(), Some(FETCH_VERSION));
        consumer.close().await.unwrap();
        broker.shutdown().await;

        let mut broker = RawBroker::start(TOPIC, list.clone(), Vec::new()).await;
        let mut consumer = Consumer::new(broker.config().auto_offset_reset(AutoOffsetReset::None))
            .await
            .unwrap();
        let error = consumer
            .assign_partitions([TopicPartition::new(TOPIC, 0)])
            .await
            .expect_err("{peer} none without stored offset must fail");
        assert!(
            error.to_string().contains("auto.offset.reset=none"),
            "{peer} none-missing: {error}"
        );
        consumer.close().await.unwrap();
        broker.shutdown().await;

        let fetch_resume = read(&dir.join("fetch-resume-response.bin")).await;
        let mut broker = RawBroker::start(TOPIC, list.clone(), fetch_resume).await;
        let mut consumer = Consumer::new(broker.config()).await.unwrap();
        consumer.assign(TOPIC, 0, 2).await.unwrap();
        let records = consumer.fetch().await.unwrap();
        assert_eq!(
            records.iter().map(|r| r.offset).collect::<Vec<_>>(),
            vec![2],
            "{peer} none-resume offsets"
        );
        assert_eq!(
            records[0].value.as_deref(),
            Some(&b"after-commit"[..]),
            "{peer} none-resume"
        );
        assert_eq!(
            consumer.position(TOPIC, 0).unwrap(),
            3,
            "{peer} resume next"
        );
        assert_eq!(broker.last_fetch_version(), Some(FETCH_VERSION));
        consumer.close().await.unwrap();
        broker.shutdown().await;

        let fetch_first = read(&dir.join("fetch-first-response.bin")).await;
        let mut broker = RawBroker::start(TOPIC, list, fetch_first).await;
        let mut consumer = Consumer::new(broker.config()).await.unwrap();
        consumer.assign(TOPIC, 0, 0).await.unwrap();
        let records = consumer.fetch().await.unwrap();
        assert_eq!(
            records.iter().map(|r| r.offset).collect::<Vec<_>>(),
            vec![0],
            "{peer} autocommit first offsets"
        );
        assert_eq!(
            records[0].value.as_deref(),
            Some(&b"first"[..]),
            "{peer} autocommit first"
        );
        assert_eq!(
            consumer.position(TOPIC, 0).unwrap(),
            1,
            "{peer} autocommit interval store target"
        );
        assert_eq!(broker.last_fetch_version(), Some(FETCH_VERSION));
        consumer.close().await.unwrap();
        broker.shutdown().await;
    }
}

#[tokio::test]
async fn frozen_offset_dispositions_stay_local_consistency() {
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
async fn rust_offset_output_decodes_with_pinned_apache_when_jars_present() {
    let jars = PathBuf::from("/tmp/partitionline-conformance");
    let slf4j = jars.join("slf4j-api-1.7.36.jar");
    let source = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests/conformance/java/ConformanceVerifiableConsumerOffsets.java");
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
            PathBuf::from("/tmp").join(format!("verifiable-consumer-offsets-classes-{peer}"));
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
                "list-offsets-response",
                dir.join("list-offsets-latest-response.bin"),
                LIST_OFFSETS_VERSION,
                "\"offset\":1",
            ),
            (
                "offset-fetch",
                dir.join("offset-fetch-empty-response.bin"),
                OFFSET_FETCH_VERSION,
                "\"committed_offset\":-1",
            ),
            (
                "offset-fetch",
                dir.join("offset-fetch-stored-response.bin"),
                OFFSET_FETCH_VERSION,
                "\"committed_offset\":2",
            ),
            (
                "fetch-response",
                dir.join("fetch-new-response.bin"),
                FETCH_VERSION,
                "\"first_value\":\"new\"",
            ),
            (
                "offset-commit",
                dir.join("offset-commit-next-2-request.bin"),
                OFFSET_COMMIT_VERSION,
                "\"committed_offset\":2",
            ),
        ];
        for (kind, path, version, field) in cells {
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
                    "ConformanceVerifiableConsumerOffsets",
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
    assert_eq!(parsed, 25, "five peers × five named reverse-parse cells");
}

//! KL01-29: independently qualify public share-offset alter/delete.
//!
//! Consumes committed Apache kafka-clients AlterShareGroupOffsets and
//! DeleteShareGroupOffsets v0 bodies. The six registered
//! `current-public-api-091/092-v0-*` rows stay `not_run` in
//! `cases.json`. `tests/common` Mock and `full_surface.rs` coordinator
//! hops are not the oracle. 3.9.1 has no share-offset message classes.

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing,
    reason = "integration-test helpers; clippy.toml allow-*-in-tests covers #[test] only"
)]

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicIsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use bytes::BytesMut;
use partitionline::protocol::admin::{
    decode_alter_share_group_offsets_request, decode_alter_share_group_offsets_response,
    decode_delete_share_group_offsets_request, decode_delete_share_group_offsets_response,
    encode_alter_share_group_offsets_request, encode_alter_share_group_offsets_response,
    encode_delete_share_group_offsets_request, encode_delete_share_group_offsets_response,
};
use partitionline::protocol::api::{
    encode_api_versions_response, encode_metadata_response, ApiVersion, ApiVersionsResponse,
    Broker, MetadataResponse, PartitionMetadata, TopicMetadata,
};
use partitionline::protocol::api_keys::{
    ALTER_SHARE_GROUP_OFFSETS, API_VERSIONS, CREATE_TOPICS, DELETE_SHARE_GROUP_OFFSETS,
    DELETE_TOPICS, FIND_COORDINATOR, METADATA,
};
use partitionline::protocol::group::{
    decode_find_coordinator_request, encode_find_coordinator_response, COORDINATOR_GROUP,
};
use partitionline::protocol::header::{decode_request_header, encode_response_header};
use partitionline::{
    Admin, AdminConfig, AlterShareGroupOffsetsPartition, AlterShareGroupOffsetsTopic,
    DeleteShareGroupOffsetsTopic,
};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::sync::oneshot;
use tokio::task::{JoinHandle, JoinSet};

const ALTER_VERSION: i16 = 0;
const DELETE_VERSION: i16 = 0;
const TOPIC: &str = "pl-share";
const GROUP: &str = "pl-share";
const START_OFFSET: i64 = 7;
const INVALID_REQUEST: i16 = 42;
const GROUP_ID_NOT_FOUND: i16 = 69;
const TOPIC_ID: [u8; 16] = [1; 16];
const PEERS: &[&str] = &["3.9.1", "4.1.0", "4.1.2", "4.2.1", "4.3.1"];
const SPEAKING: &[&str] = &["4.1.0", "4.1.2", "4.2.1", "4.3.1"];
const REPLAY_PEERS: &[&str] = &["4.1.0", "4.3.1"];
const REGISTERED: &[&str] = &[
    "current-public-api-091-v0-4-1-2",
    "current-public-api-091-v0-4-2-1",
    "current-public-api-091-v0-4-3-1",
    "current-public-api-092-v0-4-1-2",
    "current-public-api-092-v0-4-2-1",
    "current-public-api-092-v0-4-3-1",
];
const AUDIT_PIN: &str = "f0ca05da9c04942b211e2f56ddc615b9860cdea2";
const NAMED_PAIRS: &[&str] = &["alter-ok", "alter-invalid", "delete-ok", "delete-unknown"];
const UNSUPPORTED_REASON: &str = "3.9.1 kafka-clients has no AlterShareGroupOffsetsRequestData";

fn fixture_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/share_offset_mutations")
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

fn last_opt(atom: &AtomicIsize) -> Option<i16> {
    let stored = atom.load(Ordering::SeqCst);
    if stored < 0 {
        None
    } else {
        i16::try_from(stored).ok()
    }
}

struct RawBroker {
    addr: String,
    shutdown_tx: Option<oneshot::Sender<()>>,
    task: Option<JoinHandle<()>>,
    last_find_version: Arc<AtomicIsize>,
    last_find_key_type: Arc<AtomicIsize>,
    last_alter_version: Arc<AtomicIsize>,
    last_delete_version: Arc<AtomicIsize>,
}

impl RawBroker {
    async fn start(alter: Vec<u8>, delete: Vec<u8>) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind ephemeral loopback port");
        let address = listener.local_addr().expect("local addr");
        let addr = address.to_string();
        let last_find_version = Arc::new(AtomicIsize::new(-1));
        let last_find_key_type = Arc::new(AtomicIsize::new(-1));
        let last_alter_version = Arc::new(AtomicIsize::new(-1));
        let last_delete_version = Arc::new(AtomicIsize::new(-1));
        let (shutdown_tx, mut shutdown_rx) = oneshot::channel();
        let last_find_version_outer = Arc::clone(&last_find_version);
        let last_find_key_type_outer = Arc::clone(&last_find_key_type);
        let last_alter_version_outer = Arc::clone(&last_alter_version);
        let last_delete_version_outer = Arc::clone(&last_delete_version);
        let alter = Arc::new(alter);
        let delete = Arc::new(delete);
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
                        let last_find_version = Arc::clone(&last_find_version_outer);
                        let last_find_key_type = Arc::clone(&last_find_key_type_outer);
                        let last_alter_version = Arc::clone(&last_alter_version_outer);
                        let last_delete_version = Arc::clone(&last_delete_version_outer);
                        let alter = Arc::clone(&alter);
                        let delete = Arc::clone(&delete);
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
                                            (METADATA, 0, 13),
                                            (CREATE_TOPICS, 0, 7),
                                            (DELETE_TOPICS, 0, 6),
                                            (FIND_COORDINATOR, 0, 0),
                                            (ALTER_SHARE_GROUP_OFFSETS, 0, 0),
                                            (DELETE_SHARE_GROUP_OFFSETS, 0, 0),
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
                                        let mut topic = TopicMetadata::new(0, TOPIC, false, partitions);
                                        topic.topic_id = TOPIC_ID;
                                        let metadata = MetadataResponse {
                                            throttle_time_ms: 0,
                                            brokers: vec![Broker::new(0, "127.0.0.1", i32::from(address.port()), None)],
                                            cluster_id: Some("pl-share".into()),
                                            controller_id: 0,
                                            topics: vec![topic],
                                            cluster_authorized_operations: i32::MIN,
                                            error_code: 0,
                                        };
                                        if encode_metadata_response(&mut response, header.api_version, &metadata).is_err() {
                                            return;
                                        }
                                    }
                                    FIND_COORDINATOR => {
                                        last_find_version.store(isize::from(header.api_version), Ordering::SeqCst);
                                        let decoded = decode_find_coordinator_request(&mut request, header.api_version);
                                        let Ok((key, key_type)) = decoded else { return };
                                        leftover(request, "FindCoordinator request");
                                        if key != GROUP || key_type != COORDINATOR_GROUP {
                                            return;
                                        }
                                        last_find_key_type.store(isize::from(key_type), Ordering::SeqCst);
                                        if encode_find_coordinator_response(
                                            &mut response,
                                            header.api_version,
                                            0,
                                            "127.0.0.1",
                                            i32::from(address.port()),
                                            GROUP,
                                        ).is_err() {
                                            return;
                                        }
                                    }
                                    ALTER_SHARE_GROUP_OFFSETS => {
                                        last_alter_version.store(isize::from(header.api_version), Ordering::SeqCst);
                                        let Ok((group, topics)) = decode_alter_share_group_offsets_request(&mut request) else {
                                            return;
                                        };
                                        leftover(request, "AlterShareGroupOffsets request");
                                        if group != GROUP
                                            || topics.first().is_none_or(|topic| {
                                                topic.topic_name != TOPIC
                                                    || topic.partitions.first().is_none_or(|part| {
                                                        part.partition_index != 0 || part.start_offset != START_OFFSET
                                                    })
                                            })
                                        {
                                            return;
                                        }
                                        response.extend_from_slice(&alter);
                                    }
                                    DELETE_SHARE_GROUP_OFFSETS => {
                                        last_delete_version.store(isize::from(header.api_version), Ordering::SeqCst);
                                        let Ok((group, topics)) = decode_delete_share_group_offsets_request(&mut request) else {
                                            return;
                                        };
                                        leftover(request, "DeleteShareGroupOffsets request");
                                        if group != GROUP
                                            || topics.first().is_none_or(|topic| topic.topic_name != TOPIC)
                                        {
                                            return;
                                        }
                                        response.extend_from_slice(&delete);
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
            last_find_version,
            last_find_key_type,
            last_alter_version,
            last_delete_version,
        }
    }

    fn config(&self) -> AdminConfig {
        AdminConfig::bootstrap([self.addr.clone()])
            .request_timeout(Duration::from_millis(800))
            .retry_backoff(Duration::from_millis(1))
            .retry_backoff_max(Duration::from_millis(1))
    }

    fn last_find_version(&self) -> Option<i16> {
        last_opt(&self.last_find_version)
    }

    fn last_find_key_type(&self) -> Option<i16> {
        last_opt(&self.last_find_key_type)
    }

    fn last_alter_version(&self) -> Option<i16> {
        last_opt(&self.last_alter_version)
    }

    fn last_delete_version(&self) -> Option<i16> {
        last_opt(&self.last_delete_version)
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

fn prefixes_must_fail_alter(bytes: &[u8], request: bool, what: &str) {
    for prefix in 0..bytes.len() {
        let mut cur = &bytes[..prefix];
        let result = if request {
            decode_alter_share_group_offsets_request(&mut cur).err()
        } else {
            decode_alter_share_group_offsets_response(&mut cur).err()
        };
        assert!(result.is_some(), "{what} prefix {prefix} decoded");
    }
}

fn prefixes_must_fail_delete(bytes: &[u8], request: bool, what: &str) {
    for prefix in 0..bytes.len() {
        let mut cur = &bytes[..prefix];
        let result = if request {
            decode_delete_share_group_offsets_request(&mut cur).err()
        } else {
            decode_delete_share_group_offsets_response(&mut cur).err()
        };
        assert!(result.is_some(), "{what} prefix {prefix} decoded");
    }
}

#[tokio::test]
async fn apache_share_offset_bodies_decode_leftover_empty() {
    let root = fixture_root();
    let manifest = String::from_utf8(read(&root.join("manifest.json")).await).unwrap();
    assert!(manifest.contains("\"task\": \"KL01-29\""));
    assert!(manifest.contains("\"total_bodies\": 32"));
    assert!(manifest.contains("\"total_unsupported_receipts\": 8"));
    assert!(manifest.contains("\"topic\": \"pl-share\""));
    assert!(manifest.contains("\"group_id\": \"pl-share\""));

    let historical = root.join("3.9.1");
    let identity = String::from_utf8(read(&historical.join("identity.json")).await).unwrap();
    assert!(identity.contains("\"generated_bodies\": 0"));
    assert!(identity.contains("\"unsupported_receipts\": 8"));
    let unsupported = String::from_utf8(read(&historical.join("unsupported.json")).await).unwrap();
    assert!(unsupported.contains(UNSUPPORTED_REASON));
    for stem in NAMED_PAIRS {
        assert!(
            !historical.join(format!("{stem}-request.bin")).exists(),
            "3.9.1 must not invent {stem} bodies"
        );
        assert!(
            !historical.join(format!("{stem}-response.bin")).exists(),
            "3.9.1 must not invent {stem} bodies"
        );
    }

    let mut bodies = 0u32;
    for peer in SPEAKING {
        let dir = root.join(peer);
        let expected = String::from_utf8(read(&dir.join("expected.json")).await).unwrap();
        assert!(expected.contains("\"alter_start_offset\":7"));
        assert!(expected.contains("\"alter_invalid_error\":42"));
        assert!(expected.contains("\"delete_unknown_error\":69"));
        assert!(expected.contains("\"delete_topics_are_names_only\":true"));
        assert!(expected.contains("\"coordinator_key_type\":0"));

        for stem in ["alter-ok", "alter-invalid"] {
            let req = read(&dir.join(format!("{stem}-request.bin"))).await;
            let resp = read(&dir.join(format!("{stem}-response.bin"))).await;
            prefixes_must_fail_alter(&req, true, &format!("{peer} {stem} request"));
            prefixes_must_fail_alter(&resp, false, &format!("{peer} {stem} response"));
            let mut cur = req.as_slice();
            let (group, topics) = decode_alter_share_group_offsets_request(&mut cur).unwrap();
            leftover(cur, &format!("{peer} {stem} request"));
            assert_eq!(group, GROUP);
            assert_eq!(topics[0].topic_name, TOPIC);
            assert_eq!(topics[0].partitions[0].partition_index, 0);
            assert_eq!(topics[0].partitions[0].start_offset, START_OFFSET);
            let mut encoded = BytesMut::new();
            encode_alter_share_group_offsets_request(&mut encoded, &group, &topics).unwrap();
            assert_eq!(
                encoded.as_ref(),
                req.as_slice(),
                "{peer} {stem} request reencode"
            );

            let mut cur = resp.as_slice();
            let decoded = decode_alter_share_group_offsets_response(&mut cur).unwrap();
            leftover(cur, &format!("{peer} {stem} response"));
            assert_eq!(decoded.throttle_time_ms, 0);
            assert_eq!(resp[4], 0);
            if stem == "alter-ok" {
                assert_eq!(decoded.error_code, 0);
                assert_eq!(decoded.topics[0].topic_name, TOPIC);
                assert_eq!(decoded.topics[0].topic_id, TOPIC_ID);
                assert_eq!(decoded.topics[0].partitions[0].partition_index, 0);
                assert_eq!(decoded.topics[0].partitions[0].error_code, 0);
                assert_eq!(resp[4], 0);
                assert_eq!(resp[5], 0);
            } else {
                assert_eq!(decoded.error_code, INVALID_REQUEST);
                assert!(decoded.topics.is_empty());
                assert_eq!(resp[4..6], 42i16.to_be_bytes());
            }
            let mut encoded = BytesMut::new();
            encode_alter_share_group_offsets_response(&mut encoded, &decoded).unwrap();
            assert_eq!(
                encoded.as_ref(),
                resp.as_slice(),
                "{peer} {stem} response reencode"
            );
            bodies += 2;
        }

        for stem in ["delete-ok", "delete-unknown"] {
            let req = read(&dir.join(format!("{stem}-request.bin"))).await;
            let resp = read(&dir.join(format!("{stem}-response.bin"))).await;
            prefixes_must_fail_delete(&req, true, &format!("{peer} {stem} request"));
            prefixes_must_fail_delete(&resp, false, &format!("{peer} {stem} response"));
            let mut cur = req.as_slice();
            let (group, topics) = decode_delete_share_group_offsets_request(&mut cur).unwrap();
            leftover(cur, &format!("{peer} {stem} request"));
            assert_eq!(group, GROUP);
            assert_eq!(topics[0].topic_name, TOPIC);
            let mut encoded = BytesMut::new();
            encode_delete_share_group_offsets_request(&mut encoded, &group, &topics).unwrap();
            assert_eq!(
                encoded.as_ref(),
                req.as_slice(),
                "{peer} {stem} request reencode"
            );

            let mut cur = resp.as_slice();
            let decoded = decode_delete_share_group_offsets_response(&mut cur).unwrap();
            leftover(cur, &format!("{peer} {stem} response"));
            assert_eq!(decoded.throttle_time_ms, 0);
            if stem == "delete-ok" {
                assert_eq!(decoded.error_code, 0);
                assert_eq!(decoded.topics[0].topic_name, TOPIC);
                assert_eq!(decoded.topics[0].topic_id, TOPIC_ID);
                assert_eq!(decoded.topics[0].error_code, 0);
                assert_eq!(resp[4..6], 0i16.to_be_bytes());
            } else {
                assert_eq!(decoded.error_code, GROUP_ID_NOT_FOUND);
                assert!(decoded.topics.is_empty());
                assert_eq!(resp[4..6], 69i16.to_be_bytes());
            }
            let mut encoded = BytesMut::new();
            encode_delete_share_group_offsets_response(&mut encoded, &decoded).unwrap();
            assert_eq!(
                encoded.as_ref(),
                resp.as_slice(),
                "{peer} {stem} response reencode"
            );
            bodies += 2;
        }

        for stem in NAMED_PAIRS {
            assert!(dir.join(format!("{stem}-request.bin")).exists());
            assert!(dir.join(format!("{stem}-response.bin")).exists());
        }
    }
    assert_eq!(bodies, 32, "four speaking peers × 8 Apache bodies");
    let _ = PEERS;
}

#[tokio::test]
async fn apache_share_offset_replay_public_admin() {
    for peer in REPLAY_PEERS {
        let dir = fixture_root().join(peer);
        let alter_ok = read(&dir.join("alter-ok-response.bin")).await;
        let delete_ok = read(&dir.join("delete-ok-response.bin")).await;
        let mut broker = RawBroker::start(alter_ok, delete_ok).await;
        let mut admin = Admin::new(broker.config()).await.unwrap();
        let altered = admin
            .alter_share_group_offsets(
                GROUP,
                &[AlterShareGroupOffsetsTopic::new(
                    TOPIC,
                    vec![AlterShareGroupOffsetsPartition::new(0, START_OFFSET)],
                )],
            )
            .await
            .unwrap();
        assert_eq!(altered.error_code(), 0, "{peer} alter-ok");
        assert_eq!(altered.topics()[0].topic_name(), TOPIC);
        assert_eq!(altered.topics()[0].topic_id(), TOPIC_ID);
        assert_eq!(altered.topics()[0].partitions()[0].partition_index(), 0);
        assert_eq!(altered.topics()[0].partitions()[0].error_code(), 0);
        assert_eq!(
            broker.last_find_version(),
            Some(0),
            "{peer} FindCoordinator v0"
        );
        assert_eq!(
            broker.last_find_key_type(),
            Some(i16::from(COORDINATOR_GROUP)),
            "{peer} key_type=0 GROUP, not SHARE"
        );
        assert_eq!(broker.last_alter_version(), Some(ALTER_VERSION));

        let deleted = admin
            .delete_share_group_offsets(GROUP, &[DeleteShareGroupOffsetsTopic::new(TOPIC)])
            .await
            .unwrap();
        assert_eq!(deleted.error_code(), 0, "{peer} delete-ok");
        assert_eq!(deleted.topics()[0].topic_name(), TOPIC);
        assert_eq!(deleted.topics()[0].topic_id(), TOPIC_ID);
        assert_eq!(deleted.topics()[0].error_code(), 0);
        assert_eq!(broker.last_delete_version(), Some(DELETE_VERSION));
        admin.close().await.unwrap();
        broker.shutdown().await;

        let alter_invalid = read(&dir.join("alter-invalid-response.bin")).await;
        let delete_unknown = read(&dir.join("delete-unknown-response.bin")).await;
        let mut broker = RawBroker::start(alter_invalid, delete_unknown).await;
        let mut admin = Admin::new(broker.config()).await.unwrap();
        let altered = admin
            .alter_share_group_offsets(
                GROUP,
                &[AlterShareGroupOffsetsTopic::new(
                    TOPIC,
                    vec![AlterShareGroupOffsetsPartition::new(0, START_OFFSET)],
                )],
            )
            .await
            .unwrap();
        assert_eq!(
            altered.error_code(),
            INVALID_REQUEST,
            "{peer} alter-invalid is the public body, not a retry"
        );
        assert!(
            altered.topics().is_empty(),
            "{peer} alter-invalid responses empty"
        );
        assert_eq!(
            broker.last_find_key_type(),
            Some(i16::from(COORDINATOR_GROUP))
        );

        let deleted = admin
            .delete_share_group_offsets(GROUP, &[DeleteShareGroupOffsetsTopic::new(TOPIC)])
            .await
            .unwrap();
        assert_eq!(
            deleted.error_code(),
            GROUP_ID_NOT_FOUND,
            "{peer} delete-unknown is the public body, not a retry"
        );
        assert!(
            deleted.topics().is_empty(),
            "{peer} delete-unknown responses empty"
        );
        admin.close().await.unwrap();
        broker.shutdown().await;
    }
}

#[tokio::test]
async fn frozen_share_offset_dispositions_stay_not_run() {
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
async fn rust_share_offset_output_decodes_with_pinned_apache_when_jars_present() {
    let jars = PathBuf::from("/tmp/partitionline-conformance");
    let slf4j = jars.join("slf4j-api-1.7.36.jar");
    let source = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests/conformance/java/ConformanceShareOffsetMutations.java");
    if !jars.join("kafka-clients-4.1.2.jar").exists() || !slf4j.exists() || !source.exists() {
        println!("pinned kafka-clients jars not present; skipping live Apache parse");
        return;
    }
    let mut parsed = 0u32;
    for peer in SPEAKING {
        let jar = jars.join(format!("kafka-clients-{peer}.jar"));
        if !jar.exists() {
            continue;
        }
        let classes = PathBuf::from("/tmp").join(format!("share-offset-mutations-classes-{peer}"));
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
                "alter-request",
                dir.join("alter-ok-request.bin"),
                ALTER_VERSION,
                "\"start_offset\":7",
            ),
            (
                "alter-response",
                dir.join("alter-ok-response.bin"),
                ALTER_VERSION,
                "\"error_code\":0",
            ),
            (
                "delete-request",
                dir.join("delete-ok-request.bin"),
                DELETE_VERSION,
                "\"topics\":1",
            ),
            (
                "delete-response",
                dir.join("delete-unknown-response.bin"),
                DELETE_VERSION,
                "\"error_code\":69",
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
                    "ConformanceShareOffsetMutations",
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
    assert_eq!(
        parsed, 16,
        "four speaking peers × four named reverse-parse cells"
    );
}

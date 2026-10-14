//! KL01-20: independent Metadata v1–v13 wire qualification.
//!
//! Consumes committed Apache kafka-clients fixtures offline. Historical
//! 3.9.1 v13 cells stay unsupported receipts. Local round-trips in
//! `protocol_oracles` are not promoted.

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing,
    reason = "integration-test helpers; clippy.toml allow-*-in-tests covers #[test] only"
)]

use std::path::{Path, PathBuf};

use bytes::BytesMut;
use partitionline::protocol::api::{
    decode_metadata_request, decode_metadata_request_topics, decode_metadata_response,
    encode_metadata_request, encode_metadata_request_topics, encode_metadata_request_with,
    encode_metadata_response, Broker, MetadataRequestTopic, MetadataResponse, PartitionMetadata,
    TopicMetadata,
};
use partitionline::protocol::records::RecordBatch;

const CRATE_MIN: i16 = 1;
const CRATE_MAX: i16 = 13;
const CANONICAL_ID: [u8; 16] = [0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 17];
const PEERS: &[&str] = &["3.9.1", "4.1.0", "4.1.2", "4.2.1", "4.3.1"];

fn fixture_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/metadata_wire")
}

fn leftover(buf: &[u8], what: &str) {
    assert!(buf.is_empty(), "{what}: leftover {} bytes", buf.len());
}

fn cells_for(version: i16) -> &'static [&'static str] {
    if version >= 9 {
        &["empty", "canonical", "errors", "tagged"]
    } else {
        &["empty", "canonical", "errors"]
    }
}

fn peer_supported(peer: &str, version: i16) -> bool {
    match peer {
        "3.9.1" => (1..=12).contains(&version),
        _ => (1..=13).contains(&version),
    }
}

async fn read(path: &Path) -> Vec<u8> {
    tokio::fs::read(path)
        .await
        .unwrap_or_else(|error| panic!("read {}: {error}", path.display()))
}

fn decode_request(
    version: i16,
    bytes: &[u8],
) -> (Option<Vec<MetadataRequestTopic>>, bool, bool, bool) {
    let mut cur = bytes;
    let decoded = decode_metadata_request_topics(&mut cur, version).unwrap();
    leftover(cur, &format!("v{version} request"));
    decoded
}

fn decode_response(version: i16, bytes: &[u8]) -> MetadataResponse {
    let mut cur = bytes;
    let decoded = decode_metadata_response(&mut cur, version).unwrap();
    leftover(cur, &format!("v{version} response"));
    decoded
}

fn check_empty_request(version: i16, bytes: &[u8]) {
    let (topics, allow_auto, include_topic, include_cluster) = decode_request(version, bytes);
    assert!(topics.is_none(), "empty cell is all-topics");
    assert!(allow_auto);
    assert!(!include_topic);
    assert!(!include_cluster);
    let mut encoded = BytesMut::new();
    encode_metadata_request(&mut encoded, version, None, true).unwrap();
    assert_eq!(encoded.as_ref(), bytes, "empty v{version} request bytes");
}

fn check_populated_request(version: i16, cell: &str, bytes: &[u8]) {
    let (topics, allow_auto, include_topic, include_cluster) = decode_request(version, bytes);
    let topics = topics.expect("populated cell lists topics");
    assert_eq!(topics.len(), 1);
    assert!(!include_cluster);
    if cell == "errors" {
        assert_eq!(topics[0].name.as_deref(), Some("pl-missing"));
        assert_eq!(allow_auto, version < 4);
        assert_eq!(include_topic, version >= 8);
    } else {
        assert_eq!(topics[0].name.as_deref(), Some("pl-metadata"));
        assert!(allow_auto);
        assert!(!include_topic);
    }
    if version >= 10 {
        assert_eq!(topics[0].topic_id, [0; 16]);
    }
}

fn check_empty_response(version: i16, bytes: &[u8]) {
    let resp = decode_response(version, bytes);
    if version >= 3 {
        assert_eq!(resp.throttle_time_ms, 37);
    } else {
        assert_eq!(resp.throttle_time_ms, 0);
    }
    assert!(resp.brokers.is_empty());
    assert!(resp.topics.is_empty());
    assert_eq!(resp.controller_id, MetadataResponse::NO_CONTROLLER_ID);
    assert_eq!(resp.error_code, 0);
    let mut encoded = BytesMut::new();
    encode_metadata_response(
        &mut encoded,
        version,
        &MetadataResponse {
            throttle_time_ms: 37,
            brokers: Vec::new(),
            cluster_id: None,
            controller_id: MetadataResponse::NO_CONTROLLER_ID,
            topics: Vec::new(),
            cluster_authorized_operations: MetadataResponse::AUTHORIZED_OPERATIONS_OMITTED,
            error_code: 0,
        },
    )
    .unwrap();
    assert_eq!(encoded.as_ref(), bytes, "empty v{version} response bytes");
}

fn check_populated_response(version: i16, cell: &str, bytes: &[u8]) {
    let resp = decode_response(version, bytes);
    if cell == "canonical" || cell == "tagged" {
        if version >= 3 {
            assert_eq!(resp.throttle_time_ms, 42);
        }
        assert_eq!(resp.brokers.len(), 1);
        assert_eq!(resp.brokers[0].node_id, 1);
        assert_eq!(resp.brokers[0].host, "broker");
        assert_eq!(resp.brokers[0].port, 9092);
        if version >= 1 {
            assert_eq!(resp.controller_id, 1);
        }
        if version >= 2 {
            assert_eq!(resp.cluster_id.as_deref(), Some("pl-cluster"));
        }
        assert_eq!(resp.topics.len(), 1);
        assert_eq!(resp.topics[0].error_code, 0);
        assert_eq!(resp.topics[0].name.as_deref(), Some("pl-metadata"));
        assert_eq!(resp.topics[0].partitions.len(), 1);
        assert_eq!(resp.topics[0].partitions[0].leader_id, 1);
        if version >= 7 {
            assert_eq!(resp.topics[0].partitions[0].leader_epoch, 7);
        } else {
            assert_eq!(
                resp.topics[0].partitions[0].leader_epoch,
                RecordBatch::NO_PARTITION_LEADER_EPOCH
            );
        }
        if version >= 10 {
            assert_eq!(resp.topics[0].topic_id, CANONICAL_ID);
        }
        return;
    }
    assert_eq!(cell, "errors");
    if version >= 3 {
        assert_eq!(resp.throttle_time_ms, 60);
    }
    assert_eq!(resp.topics.len(), 1);
    assert_eq!(resp.topics[0].error_code, 3);
    assert_eq!(resp.topics[0].name.as_deref(), Some("pl-missing"));
    assert!(resp.topics[0].partitions.is_empty());
}

fn rust_encode_matches_request(version: i16, cell: &str, apache: &[u8]) {
    if cell == "tagged" {
        return;
    }
    let (topics, allow_auto, include_topic, include_cluster) = decode_request(version, apache);
    let mut encoded = BytesMut::new();
    encode_metadata_request_topics(
        &mut encoded,
        version,
        topics.as_deref(),
        allow_auto,
        include_topic,
    )
    .unwrap();
    let _ = include_cluster;
    assert_eq!(
        encoded.as_ref(),
        apache,
        "request re-encode v{version} {cell}"
    );
}

fn rust_encode_matches_response(version: i16, cell: &str, apache: &[u8]) {
    if cell == "tagged" {
        return;
    }
    let resp = decode_response(version, apache);
    let mut encoded = BytesMut::new();
    encode_metadata_response(&mut encoded, version, &resp).unwrap();
    assert_eq!(
        encoded.as_ref(),
        apache,
        "response re-encode v{version} {cell}"
    );
}

#[tokio::test]
async fn apache_metadata_wire_fixtures_decode_offline() {
    let root = fixture_root();
    let manifest = String::from_utf8(read(&root.join("manifest.json")).await).unwrap();
    assert!(manifest.contains("\"task\": \"KL01-20\""));
    let mut bodies = 0u32;
    let mut receipts = 0u32;
    for peer in PEERS {
        for version in CRATE_MIN..=CRATE_MAX {
            if !peer_supported(peer, version) {
                let receipt = root
                    .join(peer)
                    .join(format!("unsupported-v{version:02}.json"));
                let text = String::from_utf8(read(&receipt).await).unwrap();
                assert!(
                    text.contains("unsupported")
                        && text.contains(&format!("\"version\":{version}")),
                    "{peer} v{version} receipt: {text}"
                );
                receipts += 1;
                continue;
            }
            for cell in cells_for(version) {
                let req = read(
                    &root
                        .join(peer)
                        .join(format!("v{version:02}-{cell}-request.bin")),
                )
                .await;
                let resp = read(
                    &root
                        .join(peer)
                        .join(format!("v{version:02}-{cell}-response.bin")),
                )
                .await;
                assert!(!req.is_empty() && !resp.is_empty());
                for prefix in 0..req.len() {
                    assert!(
                        decode_metadata_request(&mut &req[..prefix], version).is_err(),
                        "{peer} v{version} {cell} request prefix {prefix} decoded"
                    );
                }
                for prefix in 0..resp.len() {
                    assert!(
                        decode_metadata_response(&mut &resp[..prefix], version).is_err(),
                        "{peer} v{version} {cell} response prefix {prefix} decoded"
                    );
                }
                if *cell == "empty" {
                    check_empty_request(version, &req);
                    check_empty_response(version, &resp);
                } else {
                    check_populated_request(version, cell, &req);
                    check_populated_response(version, cell, &resp);
                    rust_encode_matches_request(version, cell, &req);
                    rust_encode_matches_response(version, cell, &resp);
                }
                bodies += 2;
            }
        }
    }
    assert_eq!(bodies, 432, "five peers × generated Metadata bodies");
    assert_eq!(receipts, 1, "3.9.1 v13 remains unsupported");
    assert!(manifest.contains("\"total_bodies\": 432"));
    assert!(manifest.contains("\"total_unsupported_receipts\": 1"));
}

#[test]
fn crate_builder_refuses_metadata_v0() {
    assert!(
        partitionline::protocol::api::MetadataRequest::build(0, None, true).is_err(),
        "crate Builder must refuse Metadata v0"
    );
    assert!(
        partitionline::protocol::api::MetadataRequest::build(1, None, false).is_err(),
        "allowAutoTopicCreation false is unsupported below v4"
    );
    let mut buf = BytesMut::new();
    encode_metadata_request(&mut buf, 1, None, true).unwrap();
    assert!(decode_metadata_request(&mut &[][..], 1).is_err());
    assert!(decode_metadata_response(&mut &[][..], 1).is_err());
}

fn rust_bodies(version: i16) -> (BytesMut, BytesMut) {
    let allow_auto = version < 4;
    let include_topic = version >= 8;
    let mut req = BytesMut::new();
    encode_metadata_request_with(
        &mut req,
        version,
        Some(&["pl-missing".to_string()]),
        allow_auto,
        include_topic,
    )
    .unwrap();
    let resp = MetadataResponse {
        throttle_time_ms: 60,
        brokers: Vec::new(),
        cluster_id: None,
        controller_id: MetadataResponse::NO_CONTROLLER_ID,
        topics: vec![TopicMetadata::error(3, Some("pl-missing"), [0; 16])],
        cluster_authorized_operations: MetadataResponse::AUTHORIZED_OPERATIONS_OMITTED,
        error_code: 0,
    };
    let mut encoded = BytesMut::new();
    encode_metadata_response(&mut encoded, version, &resp).unwrap();
    (req, encoded)
}

fn hex_bytes(bytes: &[u8]) -> String {
    bytes.iter().fold(String::new(), |mut out, byte| {
        use std::fmt::Write as _;
        write!(&mut out, "{byte:02x}").unwrap();
        out
    })
}

#[test]
fn rust_metadata_output_decodes_with_pinned_apache_when_jars_present() {
    let jars = PathBuf::from("/tmp/partitionline-conformance");
    let slf4j = jars.join("slf4j-api-1.7.36.jar");
    let source = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests/conformance/java/ConformanceMetadataWire.java");
    if !jars.join("kafka-clients-4.1.2.jar").exists() || !slf4j.exists() || !source.exists() {
        println!(
            "pinned kafka-clients jars not present; skipping live Apache parse of Rust bodies"
        );
        return;
    }
    let mut parsed = 0u32;
    for peer in PEERS {
        let jar = jars.join(format!("kafka-clients-{peer}.jar"));
        if !jar.exists() {
            continue;
        }
        let classes = PathBuf::from("/tmp").join(format!("metadata-wire-classes-{peer}"));
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
        for version in CRATE_MIN..=CRATE_MAX {
            if !peer_supported(peer, version) {
                continue;
            }
            let (req, resp) = rust_bodies(version);
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
                    "ConformanceMetadataWire",
                    "decode-rust",
                    jar.to_str().unwrap(),
                    &version.to_string(),
                    &hex_bytes(&req),
                    &hex_bytes(&resp),
                ])
                .output()
                .expect("java decode-rust");
            let stdout = String::from_utf8_lossy(&out.stdout);
            assert!(
                out.status.success(),
                "{peer} v{version} parse failed: {} {stdout}",
                String::from_utf8_lossy(&out.stderr)
            );
            let throttle = if version >= 3 { 60 } else { 0 };
            assert!(
                stdout.contains("\"ok\":true")
                    && stdout.contains(&format!("\"version\":{version}"))
                    && stdout.contains("\"request_topics\":1")
                    && stdout.contains(&format!("\"throttle_time_ms\":{throttle}"))
                    && stdout.contains("\"response_topics\":1"),
                "{peer} v{version} fields: {stdout}"
            );
            parsed += 1;
        }
    }
    assert_eq!(parsed, 64, "3.9.1 v1-v12 plus four current peers v1-v13");
}

#[test]
fn rust_canonical_response_roundtrip_matches_gates() {
    for version in CRATE_MIN..=CRATE_MAX {
        let part = PartitionMetadata::new(
            0,
            0,
            Some(1),
            if version >= 7 { Some(7) } else { None },
            vec![1],
            vec![1],
            Vec::new(),
        );
        let topic = if version >= 10 {
            TopicMetadata::with_topic_id(
                0,
                "pl-metadata",
                CANONICAL_ID,
                false,
                vec![part],
                MetadataResponse::AUTHORIZED_OPERATIONS_OMITTED,
            )
        } else {
            TopicMetadata::new(0, "pl-metadata", false, vec![part])
        };
        let resp = MetadataResponse {
            throttle_time_ms: 42,
            brokers: vec![Broker::new(1, "broker", 9092, None)],
            cluster_id: if version >= 2 {
                Some("pl-cluster".into())
            } else {
                None
            },
            controller_id: 1,
            topics: vec![topic],
            cluster_authorized_operations: MetadataResponse::AUTHORIZED_OPERATIONS_OMITTED,
            error_code: 0,
        };
        let mut buf = BytesMut::new();
        encode_metadata_response(&mut buf, version, &resp).unwrap();
        let decoded = decode_response(version, buf.as_ref());
        assert_eq!(decoded.topics[0].name.as_deref(), Some("pl-metadata"));
        assert_eq!(decoded.brokers[0].host, "broker");
        if version >= 7 {
            assert_eq!(decoded.topics[0].partitions[0].leader_epoch, 7);
        }
    }
}

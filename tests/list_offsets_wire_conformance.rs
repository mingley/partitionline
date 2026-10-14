//! KL01-21: independent ListOffsets v1–v11 wire qualification.
//!
//! Consumes committed Apache kafka-clients fixtures offline. Historical
//! 3.9.1 v10/v11 and 4.1.x v11 cells stay unsupported receipts. Local
//! round-trips in `protocol_oracles` are not promoted. The registered
//! `proto-unsupported-list-offsets-v11-client-cap` row stays unsupported
//! in `cases.json` even though this crate speaks v11.

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing,
    reason = "integration-test helpers; clippy.toml allow-*-in-tests covers #[test] only"
)]

use std::path::{Path, PathBuf};

use bytes::BytesMut;
use partitionline::protocol::offsets::{
    decode_list_offsets_request, decode_list_offsets_response, decode_list_offsets_topics_request,
    decode_list_offsets_topics_response, encode_list_offsets_request,
    encode_list_offsets_topics_request, encode_list_offsets_topics_request_with_replica_id,
    encode_list_offsets_topics_response_with_throttle, ListOffsetsPartition,
    ListOffsetsPartitionRequest, ListOffsetsResponsePartition, ListOffsetsTopicRequest,
    ListOffsetsTopicResponse, CONSUMER_REPLICA_ID, EARLIEST_TIMESTAMP, LATEST_TIMESTAMP,
};
use partitionline::protocol::records::RecordBatch;

const CRATE_MIN: i16 = 1;
const CRATE_MAX: i16 = 11;
const PEERS: &[&str] = &["3.9.1", "4.1.0", "4.1.2", "4.2.1", "4.3.1"];

fn fixture_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/list_offsets_wire")
}

fn leftover(buf: &[u8], what: &str) {
    assert!(buf.is_empty(), "{what}: leftover {} bytes", buf.len());
}

fn cells_for(version: i16) -> &'static [&'static str] {
    if version >= 6 {
        &["empty", "canonical", "errors", "tagged"]
    } else {
        &["empty", "canonical", "errors"]
    }
}

fn peer_supported(peer: &str, version: i16) -> bool {
    match peer {
        "3.9.1" => (1..=9).contains(&version),
        "4.1.0" | "4.1.2" => (1..=10).contains(&version),
        _ => (1..=11).contains(&version),
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
) -> (i8, Vec<ListOffsetsTopicRequest>, Option<i32>, i32) {
    let mut cur = bytes;
    let decoded = decode_list_offsets_topics_request(&mut cur, version).unwrap();
    leftover(cur, &format!("v{version} request"));
    decoded
}

fn decode_response(version: i16, bytes: &[u8]) -> (Vec<ListOffsetsTopicResponse>, i32) {
    let mut cur = bytes;
    let decoded = decode_list_offsets_topics_response(&mut cur, version).unwrap();
    leftover(cur, &format!("v{version} response"));
    decoded
}

fn check_empty_request(version: i16, bytes: &[u8]) {
    let (isolation, topics, timeout, replica) = decode_request(version, bytes);
    assert!(topics.is_empty(), "empty cell lists no topics");
    assert_eq!(replica, CONSUMER_REPLICA_ID);
    assert_eq!(isolation, 0);
    if version >= 10 {
        assert_eq!(timeout, Some(0));
    } else {
        assert!(timeout.is_none());
    }
    let mut encoded = BytesMut::new();
    encode_list_offsets_topics_request(&mut encoded, version, 0, &[], 0).unwrap();
    assert_eq!(encoded.as_ref(), bytes, "empty v{version} request bytes");
}

fn check_populated_request(version: i16, cell: &str, bytes: &[u8]) {
    let (isolation, topics, timeout, replica) = decode_request(version, bytes);
    assert_eq!(replica, CONSUMER_REPLICA_ID);
    assert_eq!(topics.len(), 1);
    assert_eq!(topics[0].partitions.len(), 1);
    let part = &topics[0].partitions[0];
    assert_eq!(part.partition, 0);
    if cell == "errors" {
        assert_eq!(topics[0].name, "pl-missing");
        assert_eq!(part.timestamp, EARLIEST_TIMESTAMP);
        if version >= 2 {
            assert_eq!(isolation, 1);
        } else {
            assert_eq!(isolation, 0);
        }
        if version >= 10 {
            assert_eq!(timeout, Some(60));
        } else {
            assert!(timeout.is_none());
        }
        assert_eq!(
            part.current_leader_epoch,
            RecordBatch::NO_PARTITION_LEADER_EPOCH
        );
    } else {
        assert_eq!(topics[0].name, "pl-list-offsets");
        assert_eq!(part.timestamp, LATEST_TIMESTAMP);
        assert_eq!(isolation, 0);
        if version >= 10 {
            assert_eq!(timeout, Some(1500));
        } else {
            assert!(timeout.is_none());
        }
        if version >= 4 {
            assert_eq!(part.current_leader_epoch, 7);
        } else {
            assert_eq!(
                part.current_leader_epoch,
                RecordBatch::NO_PARTITION_LEADER_EPOCH
            );
        }
    }
}

fn check_empty_response(version: i16, bytes: &[u8]) {
    let (topics, throttle) = decode_response(version, bytes);
    if version >= 2 {
        assert_eq!(throttle, 37);
    } else {
        assert_eq!(throttle, 0);
    }
    assert!(topics.is_empty());
    let mut encoded = BytesMut::new();
    encode_list_offsets_topics_response_with_throttle(&mut encoded, version, &[], 37).unwrap();
    assert_eq!(encoded.as_ref(), bytes, "empty v{version} response bytes");
}

fn check_populated_response(version: i16, cell: &str, bytes: &[u8]) {
    let (topics, throttle) = decode_response(version, bytes);
    assert_eq!(topics.len(), 1);
    assert_eq!(topics[0].partitions.len(), 1);
    let part = &topics[0].partitions[0];
    assert_eq!(part.partition_index, 0);
    if cell == "canonical" || cell == "tagged" {
        if version >= 2 {
            assert_eq!(throttle, 42);
        } else {
            assert_eq!(throttle, 0);
        }
        assert_eq!(topics[0].name, "pl-list-offsets");
        assert_eq!(part.error_code, 0);
        assert_eq!(part.timestamp, LATEST_TIMESTAMP);
        assert_eq!(part.offset, 12);
        if version >= 4 {
            assert_eq!(part.leader_epoch, 7);
        } else {
            assert_eq!(part.leader_epoch, ListOffsetsPartition::UNKNOWN_EPOCH);
        }
        return;
    }
    assert_eq!(cell, "errors");
    if version >= 2 {
        assert_eq!(throttle, 60);
    } else {
        assert_eq!(throttle, 0);
    }
    assert_eq!(topics[0].name, "pl-missing");
    assert_eq!(part.error_code, 3);
    assert_eq!(part.timestamp, ListOffsetsPartition::UNKNOWN_TIMESTAMP);
    assert_eq!(part.offset, ListOffsetsPartition::UNKNOWN_OFFSET);
    assert_eq!(part.leader_epoch, ListOffsetsPartition::UNKNOWN_EPOCH);
}

fn rust_encode_matches_request(version: i16, cell: &str, apache: &[u8]) {
    if cell == "tagged" {
        return;
    }
    let (isolation, topics, timeout, replica) = decode_request(version, apache);
    let mut encoded = BytesMut::new();
    encode_list_offsets_topics_request_with_replica_id(
        &mut encoded,
        version,
        isolation,
        &topics,
        timeout.unwrap_or(0),
        replica,
    )
    .unwrap();
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
    let (topics, throttle) = decode_response(version, apache);
    let mut encoded = BytesMut::new();
    encode_list_offsets_topics_response_with_throttle(&mut encoded, version, &topics, throttle)
        .unwrap();
    assert_eq!(
        encoded.as_ref(),
        apache,
        "response re-encode v{version} {cell}"
    );
}

#[tokio::test]
async fn apache_list_offsets_wire_fixtures_decode_offline() {
    let root = fixture_root();
    let manifest = String::from_utf8(read(&root.join("manifest.json")).await).unwrap();
    assert!(manifest.contains("\"task\": \"KL01-21\""));
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
                        decode_list_offsets_topics_request(&mut &req[..prefix], version).is_err(),
                        "{peer} v{version} {cell} request prefix {prefix} decoded"
                    );
                }
                for prefix in 0..resp.len() {
                    assert!(
                        decode_list_offsets_topics_response(&mut &resp[..prefix], version).is_err(),
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
    assert_eq!(bodies, 358, "five peers × generated ListOffsets bodies");
    assert_eq!(
        receipts, 4,
        "3.9.1 v10/v11 and 4.1.x v11 remain unsupported"
    );
    assert!(manifest.contains("\"total_bodies\": 358"));
    assert!(manifest.contains("\"total_unsupported_receipts\": 4"));
}

#[test]
fn crate_encoder_refuses_list_offsets_v12() {
    let mut buf = BytesMut::new();
    assert!(
        encode_list_offsets_request(&mut buf, 12, 0, "t", 0, 0, LATEST_TIMESTAMP, 0).is_err(),
        "crate must refuse ListOffsets v12+"
    );
    assert!(decode_list_offsets_request(&mut &[][..], 1).is_err());
    assert!(decode_list_offsets_response(&mut &[][..], 1).is_err());
}

fn rust_bodies(version: i16) -> (BytesMut, BytesMut) {
    let isolation = if version >= 2 { 1 } else { 0 };
    let timeout = if version >= 10 { 60 } else { 0 };
    let topics = [ListOffsetsTopicRequest::new(
        "pl-missing",
        vec![ListOffsetsPartitionRequest::new(
            0,
            RecordBatch::NO_PARTITION_LEADER_EPOCH,
            EARLIEST_TIMESTAMP,
        )],
    )];
    let mut req = BytesMut::new();
    encode_list_offsets_topics_request(&mut req, version, isolation, &topics, timeout).unwrap();
    let resp = [ListOffsetsTopicResponse::new(
        "pl-missing",
        vec![ListOffsetsResponsePartition::error(0, 3)],
    )];
    let mut encoded = BytesMut::new();
    encode_list_offsets_topics_response_with_throttle(&mut encoded, version, &resp, 60).unwrap();
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
fn rust_list_offsets_output_decodes_with_pinned_apache_when_jars_present() {
    let jars = PathBuf::from("/tmp/partitionline-conformance");
    let slf4j = jars.join("slf4j-api-1.7.36.jar");
    let source = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests/conformance/java/ConformanceListOffsetsWire.java");
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
        let classes = PathBuf::from("/tmp").join(format!("list-offsets-wire-classes-{peer}"));
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
                    "ConformanceListOffsetsWire",
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
            let isolation = if version >= 2 { 1 } else { 0 };
            let timeout = if version >= 10 { 60 } else { 0 };
            let throttle = if version >= 2 { 60 } else { 0 };
            assert!(
                stdout.contains("\"ok\":true")
                    && stdout.contains(&format!("\"version\":{version}"))
                    && stdout.contains("\"request_topics\":1")
                    && stdout.contains("\"replica_id\":-1")
                    && stdout.contains(&format!("\"isolation_level\":{isolation}"))
                    && stdout.contains(&format!("\"timeout_ms\":{timeout}"))
                    && stdout.contains(&format!("\"throttle_time_ms\":{throttle}"))
                    && stdout.contains("\"response_topics\":1")
                    && stdout.contains("\"first_error_code\":3"),
                "{peer} v{version} fields: {stdout}"
            );
            parsed += 1;
        }
    }
    assert_eq!(
        parsed, 51,
        "3.9.1 v1-v9 plus 4.1.x v1-v10 plus 4.2.1/4.3.1 v1-v11"
    );
}

#[test]
fn rust_canonical_response_roundtrip_matches_gates() {
    for version in CRATE_MIN..=CRATE_MAX {
        let topic = ListOffsetsTopicResponse::new(
            "pl-list-offsets",
            vec![ListOffsetsResponsePartition::new(
                0,
                ListOffsetsPartition::ok(LATEST_TIMESTAMP, 12, if version >= 4 { 7 } else { -1 }),
            )],
        );
        let mut buf = BytesMut::new();
        encode_list_offsets_topics_response_with_throttle(
            &mut buf,
            version,
            std::slice::from_ref(&topic),
            42,
        )
        .unwrap();
        let (decoded, throttle) = decode_response(version, buf.as_ref());
        assert_eq!(decoded[0].name, "pl-list-offsets");
        assert_eq!(decoded[0].partitions[0].offset, 12);
        if version >= 2 {
            assert_eq!(throttle, 42);
        } else {
            assert_eq!(throttle, 0);
        }
        if version >= 4 {
            assert_eq!(decoded[0].partitions[0].leader_epoch, 7);
        }
    }
}

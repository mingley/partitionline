//! KL01-18: independent Produce v3–v13 wire qualification.
//!
//! Consumes committed Apache kafka-clients fixtures offline. Historical
//! 3.9.1 v12/v13 cells stay unsupported receipts. Local round-trips in
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
    decode_produce_request, decode_produce_request_with_topic_ids, decode_produce_response,
    decode_produce_response_with_topic_ids, encode_produce_request,
    encode_produce_request_with_topic_ids, encode_produce_response_with_endpoints,
    encode_produce_response_with_throttle, encode_produce_response_with_topic_ids,
    MetadataResponse, NodeEndpoint, ProducePartitionData, ProducePartitionResponse,
    ProduceRecordError, ProduceTopicData,
};
use partitionline::protocol::records::RecordBatch;

const CRATE_MIN: i16 = 3;
const CRATE_MAX: i16 = 13;
const CANONICAL_ID: [u8; 16] = [0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 17];
const TXN_TOPIC_ID: [u8; 16] = [0, 0, 0, 0, 0, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0, 19];
const PEERS: &[&str] = &["3.9.1", "4.1.0", "4.1.2", "4.2.1", "4.3.1"];

fn fixture_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/produce_wire")
}

fn leftover(buf: &[u8], what: &str) {
    assert!(buf.is_empty(), "{what}: leftover {} bytes", buf.len());
}

fn cells_for(version: i16) -> &'static [&'static str] {
    if version >= 9 {
        &["empty", "canonical", "txn_errors", "tagged"]
    } else {
        &["empty", "canonical", "txn_errors"]
    }
}

fn peer_supported(peer: &str, version: i16) -> bool {
    match peer {
        "3.9.1" => (3..=11).contains(&version),
        _ => (3..=13).contains(&version),
    }
}

fn topic_id(version: i16, cell: &str) -> [u8; 16] {
    if cell == "txn_errors" {
        TXN_TOPIC_ID
    } else {
        let _ = version;
        CANONICAL_ID
    }
}

fn topic_name(cell: &str) -> &'static str {
    if cell == "txn_errors" {
        "pl-txn"
    } else {
        "pl-produce"
    }
}

async fn read(path: &Path) -> Vec<u8> {
    tokio::fs::read(path)
        .await
        .unwrap_or_else(|error| panic!("read {}: {error}", path.display()))
}

fn check_empty_request(version: i16, bytes: &[u8]) {
    if version == 13 {
        let mut cur = bytes;
        let (txn, acks, timeout, topics, ids) =
            decode_produce_request_with_topic_ids(&mut cur).unwrap();
        leftover(cur, "empty v13 request");
        assert!(txn.is_none());
        assert_eq!(acks, -1);
        assert_eq!(timeout, 1234);
        assert!(topics.is_empty());
        assert!(ids.is_empty());
        let mut encoded = BytesMut::new();
        encode_produce_request_with_topic_ids(&mut encoded, None, -1, 1234, &[], &[]).unwrap();
        assert_eq!(encoded.as_ref(), bytes, "empty v13 request bytes");
        return;
    }
    let mut cur = bytes;
    let (txn, acks, timeout, topics) = decode_produce_request(&mut cur, version).unwrap();
    leftover(cur, &format!("empty v{version} request"));
    assert!(txn.is_none());
    assert_eq!(acks, -1);
    assert_eq!(timeout, 1234);
    assert!(topics.is_empty());
    let mut encoded = BytesMut::new();
    encode_produce_request(&mut encoded, version, None, -1, 1234, &[]).unwrap();
    assert_eq!(encoded.as_ref(), bytes, "empty v{version} request bytes");
}

fn check_populated_request(version: i16, cell: &str, bytes: &[u8]) {
    let expected_name = topic_name(cell);
    let expected_id = topic_id(version, cell);
    let (txn, acks, timeout, topics, ids) = if version == 13 {
        let mut cur = bytes;
        let decoded = decode_produce_request_with_topic_ids(&mut cur).unwrap();
        leftover(cur, &format!("v13 {cell} request"));
        decoded
    } else {
        let mut cur = bytes;
        let (txn, acks, timeout, topics) = decode_produce_request(&mut cur, version).unwrap();
        leftover(cur, &format!("v{version} {cell} request"));
        (txn, acks, timeout, topics, Vec::new())
    };
    if cell == "txn_errors" {
        assert_eq!(txn.as_deref(), Some("txn-pl"));
        assert_eq!(acks, 1);
        assert_eq!(timeout, 3000);
        assert_eq!(topics.len(), 1);
        assert_eq!(topics[0].partitions.len(), 2);
        assert_eq!(topics[0].partitions[0].index, 0);
        assert_eq!(topics[0].partitions[1].index, 3);
    } else {
        assert!(txn.is_none());
        assert_eq!(acks, -1);
        assert_eq!(timeout, 1234);
        assert_eq!(topics.len(), 1);
        assert_eq!(topics[0].partitions.len(), 1);
        assert_eq!(topics[0].partitions[0].index, 0);
    }
    if version == 13 {
        assert!(topics[0].topic.is_empty());
        assert_eq!(ids, vec![expected_id]);
    } else {
        assert_eq!(topics[0].topic, expected_name);
        assert!(ids.is_empty());
    }
    assert!(
        topics[0].partitions[0].records.records.is_empty(),
        "null records decode empty"
    );
}

fn check_empty_response(version: i16, bytes: &[u8]) {
    let (parts, endpoints, throttle, ids) = decode_response(version, bytes);
    assert_eq!(throttle, 37);
    assert!(parts.is_empty());
    assert!(endpoints.is_empty());
    assert!(ids.is_empty() || version == 13);
    if version == 13 {
        let mut encoded = BytesMut::new();
        encode_produce_response_with_topic_ids(&mut encoded, &[], &[], 37, &[]).unwrap();
        assert_eq!(encoded.as_ref(), bytes, "empty v13 response bytes");
    } else {
        let mut encoded = BytesMut::new();
        encode_produce_response_with_throttle(&mut encoded, version, &[], 37).unwrap();
        assert_eq!(encoded.as_ref(), bytes, "empty v{version} response bytes");
    }
}

fn decode_response(
    version: i16,
    bytes: &[u8],
) -> (
    Vec<ProducePartitionResponse>,
    Vec<NodeEndpoint>,
    i32,
    Vec<[u8; 16]>,
) {
    if version == 13 {
        let mut cur = bytes;
        let decoded = decode_produce_response_with_topic_ids(&mut cur).unwrap();
        leftover(cur, "v13 response");
        decoded
    } else {
        let mut cur = bytes;
        let (parts, endpoints, throttle) = decode_produce_response(&mut cur, version).unwrap();
        leftover(cur, &format!("v{version} response"));
        (parts, endpoints, throttle, Vec::new())
    }
}

fn check_populated_response(version: i16, cell: &str, bytes: &[u8]) {
    let (parts, endpoints, throttle, ids) = decode_response(version, bytes);
    let expected_name = topic_name(cell);
    let expected_id = topic_id(version, cell);
    if cell == "canonical" || cell == "tagged" {
        assert_eq!(throttle, 42);
        assert_eq!(parts.len(), 1);
        if version < 13 {
            assert_eq!(parts[0].topic, expected_name);
        } else {
            assert!(parts[0].topic.is_empty());
            assert_eq!(ids, vec![expected_id]);
        }
        assert_eq!(parts[0].partition, 0);
        assert_eq!(parts[0].error_code, 0);
        assert_eq!(parts[0].base_offset, 100);
        assert_eq!(parts[0].log_append_time_ms, 1_710_000_000_000);
        if version >= 5 {
            assert_eq!(parts[0].log_start_offset, 0);
        } else {
            assert_eq!(
                parts[0].log_start_offset,
                ProducePartitionResponse::INVALID_OFFSET
            );
        }
        assert!(parts[0].record_errors.is_empty());
        assert_eq!(parts[0].error_message, None);
        if cell == "tagged" && version >= 10 {
            assert_eq!(parts[0].current_leader_id, 7);
            assert_eq!(parts[0].current_leader_epoch, 9);
            assert_eq!(endpoints.len(), 1);
            assert_eq!(endpoints[0].node_id, 7);
            assert_eq!(endpoints[0].host, "broker");
            assert_eq!(endpoints[0].port, 9092);
            assert_eq!(endpoints[0].rack, None);
        } else {
            assert_eq!(parts[0].current_leader_id, MetadataResponse::NO_LEADER_ID);
            assert_eq!(
                parts[0].current_leader_epoch,
                RecordBatch::NO_PARTITION_LEADER_EPOCH
            );
            assert!(endpoints.is_empty());
        }
        return;
    }
    assert_eq!(cell, "txn_errors");
    assert_eq!(throttle, 60);
    assert_eq!(parts.len(), 2);
    if version < 13 {
        assert_eq!(parts[0].topic, expected_name);
        assert_eq!(parts[1].topic, expected_name);
    } else {
        assert_eq!(ids, vec![expected_id, expected_id]);
    }
    assert_eq!(parts[0].partition, 0);
    assert_eq!(parts[0].error_code, 0);
    assert_eq!(parts[0].base_offset, 200);
    assert_eq!(parts[0].log_append_time_ms, 1_710_000_001_000);
    if version >= 5 {
        assert_eq!(parts[0].log_start_offset, 50);
    } else {
        assert_eq!(
            parts[0].log_start_offset,
            ProducePartitionResponse::INVALID_OFFSET
        );
    }
    assert_eq!(parts[1].partition, 3);
    assert_eq!(parts[1].error_code, 6);
    assert_eq!(parts[1].base_offset, -1);
    if version >= 8 {
        assert_eq!(
            parts[1].record_errors,
            vec![ProduceRecordError::new(0, Some("invalid".into()))]
        );
        assert_eq!(parts[1].error_message.as_deref(), Some("not leader"));
    } else {
        assert!(parts[1].record_errors.is_empty());
        assert_eq!(parts[1].error_message, None);
    }
    if version >= 10 {
        assert_eq!(parts[1].current_leader_id, 7);
        assert_eq!(parts[1].current_leader_epoch, 9);
        assert_eq!(endpoints.len(), 1);
        assert_eq!(endpoints[0].host, "broker");
    } else {
        assert_eq!(parts[1].current_leader_id, MetadataResponse::NO_LEADER_ID);
        assert!(endpoints.is_empty());
    }
}

fn rust_encode_matches_response(version: i16, cell: &str, apache: &[u8]) {
    // Tagged cells keep unknown tags that Rust skips. Name-based public
    // helpers cannot emit both NodeEndpoints and a non-zero throttle.
    if cell == "tagged" || (cell == "txn_errors" && (10..=12).contains(&version)) {
        return;
    }
    let (mut parts, endpoints, throttle, ids) = decode_response(version, apache);
    let mut encoded = BytesMut::new();
    if version == 13 {
        for (part, id) in parts.iter_mut().zip(ids.iter()) {
            part.topic = if *id == TXN_TOPIC_ID {
                "pl-txn".into()
            } else {
                "pl-produce".into()
            };
        }
        let mut identities = Vec::new();
        for (part, id) in parts.iter().zip(ids.iter()) {
            if !identities.iter().any(|(name, _)| name == &part.topic) {
                identities.push((part.topic.clone(), *id));
            }
        }
        encode_produce_response_with_topic_ids(
            &mut encoded,
            &parts,
            &endpoints,
            throttle,
            &identities,
        )
        .unwrap();
    } else {
        encode_produce_response_with_throttle(&mut encoded, version, &parts, throttle).unwrap();
    }
    assert_eq!(
        encoded.as_ref(),
        apache,
        "response re-encode v{version} {cell}"
    );
}

#[tokio::test]
async fn apache_produce_wire_fixtures_decode_offline() {
    let root = fixture_root();
    let manifest = String::from_utf8(read(&root.join("manifest.json")).await).unwrap();
    assert!(manifest.contains("\"task\": \"KL01-18\""));
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
                let stem = root.join(peer).join(format!("v{version:02}-{cell}"));
                let req =
                    read(&stem.with_file_name(format!("v{version:02}-{cell}-request.bin"))).await;
                let resp =
                    read(&stem.with_file_name(format!("v{version:02}-{cell}-response.bin"))).await;
                assert!(!req.is_empty() && !resp.is_empty());
                for prefix in 0..req.len() {
                    let slice = &req[..prefix];
                    if version == 13 {
                        assert!(decode_produce_request_with_topic_ids(&mut &slice[..]).is_err());
                    } else {
                        assert!(decode_produce_request(&mut &slice[..], version).is_err());
                    }
                }
                for prefix in 0..resp.len() {
                    let slice = &resp[..prefix];
                    if version == 13 {
                        assert!(decode_produce_response_with_topic_ids(&mut &slice[..]).is_err());
                    } else {
                        assert!(decode_produce_response(&mut &slice[..], version).is_err());
                    }
                }
                if *cell == "empty" {
                    check_empty_request(version, &req);
                    check_empty_response(version, &resp);
                } else {
                    check_populated_request(version, cell, &req);
                    check_populated_response(version, cell, &resp);
                    rust_encode_matches_response(version, cell, &resp);
                }
                bodies += 2;
            }
        }
    }
    assert_eq!(bodies, 364, "five peers × generated Produce bodies");
    assert_eq!(receipts, 2, "3.9.1 v12 and v13 remain unsupported");
    assert!(manifest.contains("\"total_bodies\": 364"));
    assert!(manifest.contains("\"total_unsupported_receipts\": 2"));
}

#[test]
fn crate_refuses_unspoken_produce_versions() {
    let mut buf = BytesMut::new();
    for version in [0, 1, 2, 14, 15] {
        buf.clear();
        assert!(
            encode_produce_request(&mut buf, version, None, -1, 1234, &[]).is_err(),
            "crate must refuse Produce v{version}"
        );
        assert!(decode_produce_request(&mut &[][..], version).is_err());
        assert!(decode_produce_response(&mut &[][..], version).is_err());
    }
    let mut buf = BytesMut::new();
    encode_produce_request(&mut buf, 9, None, -1, 1234, &[]).unwrap();
    assert!(decode_produce_request(&mut buf.as_ref(), 13).is_err());
}

#[test]
fn rust_empty_and_txn_response_roundtrip_matches_gates() {
    for version in CRATE_MIN..=12 {
        let mut part =
            ProducePartitionResponse::partition_response_with_offsets("pl-txn", 3, 6, -1, -1, -1);
        if version >= 8 {
            part.record_errors = vec![ProduceRecordError::new(0, Some("invalid".into()))];
            part.error_message = Some("not leader".into());
        }
        if version >= 10 {
            part.current_leader_id = 7;
            part.current_leader_epoch = 9;
        }
        let endpoints = if version >= 10 {
            vec![NodeEndpoint::new(7, "broker", 9092, None)]
        } else {
            Vec::new()
        };
        let mut buf = BytesMut::new();
        if endpoints.is_empty() {
            encode_produce_response_with_throttle(&mut buf, version, &[part.clone()], 60).unwrap();
        } else {
            encode_produce_response_with_endpoints(&mut buf, version, &[part.clone()], &endpoints)
                .unwrap();
        }
        let mut cur = buf.as_ref();
        let (decoded, got_eps, throttle) = decode_produce_response(&mut cur, version).unwrap();
        leftover(cur, "local gate response");
        assert_eq!(decoded[0].error_code, 6);
        if version >= 8 {
            assert_eq!(decoded[0].error_message.as_deref(), Some("not leader"));
        } else {
            assert_eq!(decoded[0].error_message, None);
        }
        if version >= 10 {
            assert_eq!(decoded[0].current_leader_id, 7);
            assert_eq!(got_eps.len(), 1);
        } else {
            assert_eq!(decoded[0].current_leader_id, MetadataResponse::NO_LEADER_ID);
            assert!(got_eps.is_empty());
            assert_eq!(throttle, 60);
        }
    }
}

#[test]
fn rust_v13_requires_nonzero_topic_ids() {
    let topic = ProduceTopicData {
        topic: "pl-produce".into(),
        partitions: vec![ProducePartitionData {
            index: 0,
            records: RecordBatch::from_records(vec![]),
        }],
    };
    let mut buf = BytesMut::new();
    assert!(encode_produce_request_with_topic_ids(
        &mut buf,
        None,
        -1,
        1234,
        std::slice::from_ref(&topic),
        &[("pl-produce".into(), [0; 16])],
    )
    .is_err());
    assert!(encode_produce_request(&mut buf, 13, None, -1, 1234, &[topic]).is_err());
    assert!(decode_produce_request(&mut &[][..], 13).is_err());
    assert!(decode_produce_response(&mut &[][..], 13).is_err());
}

fn hex_bytes(bytes: &[u8]) -> String {
    bytes.iter().fold(String::new(), |mut out, byte| {
        use std::fmt::Write as _;
        write!(&mut out, "{byte:02x}").unwrap();
        out
    })
}

fn rust_bodies(version: i16) -> (BytesMut, BytesMut) {
    let topic = ProduceTopicData {
        topic: topic_name("txn_errors").into(),
        partitions: vec![
            ProducePartitionData {
                index: 0,
                records: RecordBatch::from_records(vec![]),
            },
            ProducePartitionData {
                index: 3,
                records: RecordBatch::from_records(vec![]),
            },
        ],
    };
    let mut req = BytesMut::new();
    let mut resp = BytesMut::new();
    let mut part = ProducePartitionResponse::partition_response_with_offsets(
        "pl-txn",
        0,
        0,
        200,
        1_710_000_001_000,
        50,
    );
    if version >= 10 {
        part.current_leader_id = 7;
        part.current_leader_epoch = 9;
    }
    if version == 13 {
        encode_produce_request_with_topic_ids(
            &mut req,
            Some("txn-pl"),
            1,
            3000,
            &[topic],
            &[("pl-txn".into(), TXN_TOPIC_ID)],
        )
        .unwrap();
        encode_produce_response_with_topic_ids(
            &mut resp,
            &[part],
            &[],
            60,
            &[("pl-txn".into(), TXN_TOPIC_ID)],
        )
        .unwrap();
    } else {
        encode_produce_request(&mut req, version, Some("txn-pl"), 1, 3000, &[topic]).unwrap();
        encode_produce_response_with_throttle(&mut resp, version, &[part], 60).unwrap();
    }
    (req, resp)
}

#[test]
fn rust_produce_output_decodes_with_pinned_apache_when_jars_present() {
    let jars = PathBuf::from("/tmp/partitionline-conformance");
    let slf4j = jars.join("slf4j-api-1.7.36.jar");
    let source = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests/conformance/java/ConformanceProduceWire.java");
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
        let classes = PathBuf::from("/tmp").join(format!("produce-wire-classes-{peer}"));
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
                    "ConformanceProduceWire",
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
            assert!(
                stdout.contains("\"ok\":true")
                    && stdout.contains(&format!("\"version\":{version}"))
                    && stdout.contains("\"transactional_id\":\"txn-pl\"")
                    && stdout.contains("\"timeout_ms\":3000")
                    && stdout.contains("\"throttle_time_ms\":60"),
                "{peer} v{version} fields: {stdout}"
            );
            parsed += 1;
        }
    }
    assert_eq!(parsed, 53, "3.9.1 v3-v11 plus four current peers v3-v13");
}

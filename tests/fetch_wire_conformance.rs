//! KL01-19: independent Fetch v4–v18 wire qualification.
//!
//! Consumes committed Apache kafka-clients fixtures offline. Historical
//! 3.9.1 v18 cells stay unsupported receipts. Local round-trips in
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
use partitionline::protocol::api::{MetadataResponse, NodeEndpoint};
use partitionline::protocol::epoch::EpochEndOffset;
use partitionline::protocol::fetch::{
    decode_fetch_request, decode_fetch_request_with_high_watermarks, decode_fetch_response,
    encode_fetch_request, encode_fetch_request_with_forgotten, encode_fetch_response_with_throttle,
    DecodedFetchRequest, FetchMetadata, FetchPartition, FetchReplicaHighWatermarks, FetchResponse,
    FetchTopic, FetchedPartition, FetchedTopic, ForgottenTopic, CONSUMER_REPLICA_ID,
    FETCH_CRATE_MAX_VERSION, INVALID_LOG_START_OFFSET, REPLICA_HIGH_WATERMARK_NOT_SUPPORTED,
};
use partitionline::protocol::records::RecordBatch;

const CRATE_MIN: i16 = 4;
const CANONICAL_ID: [u8; 16] = [0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 17];
const SESSION_ID: [u8; 16] = [0, 0, 0, 0, 0, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0, 19];
const FORGOTTEN_ID: [u8; 16] = [0, 0, 0, 0, 0, 0, 0, 3, 0, 0, 0, 0, 0, 0, 0, 23];
const PEERS: &[&str] = &["3.9.1", "4.1.0", "4.1.2", "4.2.1", "4.3.1"];

fn fixture_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/fetch_wire")
}

fn leftover(buf: &[u8], what: &str) {
    assert!(buf.is_empty(), "{what}: leftover {} bytes", buf.len());
}

fn cells_for(version: i16) -> &'static [&'static str] {
    if version >= 12 {
        &["empty", "canonical", "session_errors", "tagged"]
    } else {
        &["empty", "canonical", "session_errors"]
    }
}

fn peer_supported(peer: &str, version: i16) -> bool {
    match peer {
        "3.9.1" => (4..=17).contains(&version),
        _ => (4..=18).contains(&version),
    }
}

fn topic_id(cell: &str) -> [u8; 16] {
    if cell == "session_errors" {
        SESSION_ID
    } else {
        CANONICAL_ID
    }
}

fn topic_name(cell: &str) -> &'static str {
    if cell == "session_errors" {
        "pl-fetch-session"
    } else {
        "pl-fetch"
    }
}

async fn read(path: &Path) -> Vec<u8> {
    tokio::fs::read(path)
        .await
        .unwrap_or_else(|error| panic!("read {}: {error}", path.display()))
}

fn decode_request(version: i16, bytes: &[u8]) -> DecodedFetchRequest {
    let mut cur = bytes;
    let decoded = decode_fetch_request(&mut cur, version).unwrap();
    leftover(cur, &format!("v{version} request"));
    decoded
}

fn decode_request_hw(
    version: i16,
    bytes: &[u8],
) -> (DecodedFetchRequest, FetchReplicaHighWatermarks) {
    let mut cur = bytes;
    let decoded = decode_fetch_request_with_high_watermarks(&mut cur, version).unwrap();
    leftover(cur, &format!("v{version} request+hw"));
    decoded
}

fn decode_response(
    version: i16,
    bytes: &[u8],
) -> (Vec<FetchedTopic>, Vec<NodeEndpoint>, i16, i32, i32) {
    let mut cur = bytes;
    let decoded = decode_fetch_response(&mut cur, version).unwrap();
    leftover(cur, &format!("v{version} response"));
    decoded
}

fn check_empty_request(version: i16, bytes: &[u8]) {
    let (
        isolation,
        max_bytes,
        topics,
        rack,
        session,
        forgotten,
        max_wait,
        min_bytes,
        replica_id,
        replica_epoch,
        cluster_id,
    ) = decode_request(version, bytes);
    assert_eq!(isolation, 0);
    assert_eq!(max_bytes, 10_485_760);
    assert!(topics.is_empty());
    assert!(rack.is_empty());
    assert_eq!(session, FetchMetadata::LEGACY);
    assert!(forgotten.is_empty());
    assert_eq!(max_wait, 500);
    assert_eq!(min_bytes, 1);
    assert_eq!(replica_id, CONSUMER_REPLICA_ID);
    assert_eq!(replica_epoch, -1);
    assert_eq!(cluster_id, None);
    let mut encoded = BytesMut::new();
    encode_fetch_request(&mut encoded, version, 500, 1, 10_485_760, 0, &[], None).unwrap();
    assert_eq!(encoded.as_ref(), bytes, "empty v{version} request bytes");
}

fn check_populated_request(version: i16, cell: &str, bytes: &[u8]) {
    let (
        (
            isolation,
            max_bytes,
            topics,
            rack,
            session,
            forgotten,
            max_wait,
            min_bytes,
            replica_id,
            replica_epoch,
            cluster_id,
        ),
        high_watermarks,
    ) = decode_request_hw(version, bytes);
    let expected_name = topic_name(cell);
    let expected_id = topic_id(cell);
    assert_eq!(replica_id, CONSUMER_REPLICA_ID);
    assert_eq!(replica_epoch, -1);
    assert_eq!(cluster_id, None);
    assert_eq!(topics.len(), 1);
    if version >= 13 {
        assert!(topics[0].topic.is_empty());
        assert_eq!(topics[0].topic_id, expected_id);
    } else {
        assert_eq!(topics[0].topic, expected_name);
        assert_eq!(topics[0].topic_id, [0; 16]);
    }
    if cell == "session_errors" {
        assert_eq!(isolation, 1);
        assert_eq!(max_wait, 5000);
        assert_eq!(min_bytes, 1);
        assert_eq!(max_bytes, 10_485_760);
        assert_eq!(topics[0].partitions.len(), 2);
        assert_eq!(topics[0].partitions[0].partition, 0);
        assert_eq!(topics[0].partitions[0].fetch_offset, 100);
        assert_eq!(topics[0].partitions[1].partition, 3);
        assert_eq!(topics[0].partitions[1].fetch_offset, 200);
        if version >= 5 {
            assert_eq!(topics[0].partitions[0].log_start_offset, 50);
            assert_eq!(
                topics[0].partitions[1].log_start_offset,
                INVALID_LOG_START_OFFSET
            );
        } else {
            assert_eq!(
                topics[0].partitions[0].log_start_offset,
                INVALID_LOG_START_OFFSET
            );
        }
        if version >= 9 {
            assert_eq!(topics[0].partitions[0].current_leader_epoch, 7);
        } else {
            assert_eq!(
                topics[0].partitions[0].current_leader_epoch,
                RecordBatch::NO_PARTITION_LEADER_EPOCH
            );
        }
        if version >= 12 {
            assert_eq!(topics[0].partitions[0].last_fetched_epoch, 6);
        } else {
            assert_eq!(
                topics[0].partitions[0].last_fetched_epoch,
                RecordBatch::NO_PARTITION_LEADER_EPOCH
            );
        }
        if version >= 7 {
            assert_eq!(session, FetchMetadata::new(42, 3));
            assert_eq!(forgotten.len(), 1);
            assert_eq!(forgotten[0].partitions, vec![1]);
            if version >= 13 {
                assert_eq!(forgotten[0].topic_id, FORGOTTEN_ID);
            } else {
                assert_eq!(forgotten[0].topic, "pl-forgotten");
            }
        } else {
            assert_eq!(session, FetchMetadata::LEGACY);
            assert!(forgotten.is_empty());
        }
        if version >= 11 {
            assert_eq!(rack, "rack-a");
        } else {
            assert!(rack.is_empty());
        }
    } else {
        assert_eq!(isolation, 0);
        assert_eq!(max_wait, 500);
        assert_eq!(min_bytes, 1);
        assert_eq!(max_bytes, 10_485_760);
        assert_eq!(session, FetchMetadata::LEGACY);
        assert!(forgotten.is_empty());
        assert!(rack.is_empty());
        assert_eq!(topics[0].partitions.len(), 1);
        assert_eq!(topics[0].partitions[0].partition, 0);
        assert_eq!(topics[0].partitions[0].fetch_offset, 0);
        assert_eq!(topics[0].partitions[0].partition_max_bytes, 1_048_576);
        if version >= 5 {
            assert_eq!(
                topics[0].partitions[0].log_start_offset,
                INVALID_LOG_START_OFFSET
            );
        }
    }
    if version >= 18 {
        assert_eq!(high_watermarks.len(), 1);
        if cell == "tagged" {
            assert_eq!(high_watermarks[0], vec![99]);
        } else {
            assert_eq!(
                high_watermarks[0],
                vec![REPLICA_HIGH_WATERMARK_NOT_SUPPORTED; topics[0].partitions.len()]
            );
        }
    }
}

fn check_empty_response(version: i16, bytes: &[u8]) {
    let (topics, endpoints, error_code, session_id, throttle) = decode_response(version, bytes);
    assert_eq!(throttle, 37);
    assert_eq!(error_code, 0);
    assert_eq!(session_id, FetchMetadata::INVALID_SESSION_ID);
    assert!(topics.is_empty());
    assert!(endpoints.is_empty());
    let mut encoded = BytesMut::new();
    encode_fetch_response_with_throttle(&mut encoded, version, &[], 37).unwrap();
    assert_eq!(encoded.as_ref(), bytes, "empty v{version} response bytes");
}

fn check_populated_response(version: i16, cell: &str, bytes: &[u8]) {
    let (topics, endpoints, error_code, session_id, throttle) = decode_response(version, bytes);
    let expected_name = topic_name(cell);
    let expected_id = topic_id(cell);
    assert_eq!(topics.len(), 1);
    if version >= 13 {
        assert!(topics[0].topic.is_empty());
        assert_eq!(topics[0].topic_id, expected_id);
    } else {
        assert_eq!(topics[0].topic, expected_name);
        assert_eq!(topics[0].topic_id, [0; 16]);
    }
    if cell == "canonical" || cell == "tagged" {
        assert_eq!(throttle, 42);
        assert_eq!(error_code, 0);
        assert_eq!(session_id, FetchMetadata::INVALID_SESSION_ID);
        assert_eq!(topics[0].partitions.len(), 1);
        assert_eq!(topics[0].partitions[0].partition, 0);
        assert_eq!(topics[0].partitions[0].error_code, 0);
        assert_eq!(topics[0].partitions[0].high_watermark, 50);
        assert_eq!(topics[0].partitions[0].last_stable_offset, 45);
        assert!(topics[0].partitions[0].records.is_empty());
        if version >= 5 {
            assert_eq!(topics[0].partitions[0].log_start_offset, 0);
        } else {
            assert_eq!(
                topics[0].partitions[0].log_start_offset,
                FetchedPartition::INVALID_LOG_START_OFFSET
            );
        }
        if cell == "tagged" && version >= 12 {
            assert_eq!(topics[0].partitions[0].current_leader_id, 7);
            assert_eq!(topics[0].partitions[0].current_leader_epoch, 9);
        } else {
            assert_eq!(
                topics[0].partitions[0].current_leader_id,
                MetadataResponse::NO_LEADER_ID
            );
            assert_eq!(
                topics[0].partitions[0].current_leader_epoch,
                RecordBatch::NO_PARTITION_LEADER_EPOCH
            );
        }
        if cell == "tagged" && version >= 16 {
            assert_eq!(endpoints.len(), 1);
            assert_eq!(endpoints[0].node_id, 7);
            assert_eq!(endpoints[0].host, "broker");
            assert_eq!(endpoints[0].port, 9092);
            assert_eq!(endpoints[0].rack, None);
        } else {
            assert!(endpoints.is_empty());
        }
        return;
    }
    assert_eq!(cell, "session_errors");
    assert_eq!(throttle, 60);
    assert_eq!(error_code, 0);
    if version >= 7 {
        assert_eq!(session_id, 42);
    } else {
        assert_eq!(session_id, FetchMetadata::INVALID_SESSION_ID);
    }
    assert_eq!(topics[0].partitions.len(), 2);
    assert_eq!(topics[0].partitions[0].partition, 0);
    assert_eq!(topics[0].partitions[0].error_code, 0);
    assert_eq!(topics[0].partitions[0].high_watermark, 250);
    assert_eq!(topics[0].partitions[0].last_stable_offset, 240);
    if version >= 5 {
        assert_eq!(topics[0].partitions[0].log_start_offset, 100);
    } else {
        assert_eq!(
            topics[0].partitions[0].log_start_offset,
            FetchedPartition::INVALID_LOG_START_OFFSET
        );
    }
    if version >= 11 {
        assert_eq!(topics[0].partitions[0].preferred_read_replica, 7);
    } else {
        assert_eq!(
            topics[0].partitions[0].preferred_read_replica,
            FetchedPartition::INVALID_PREFERRED_REPLICA_ID
        );
    }
    assert_eq!(topics[0].partitions[1].partition, 3);
    assert_eq!(topics[0].partitions[1].error_code, 3);
    assert_eq!(topics[0].partitions[1].high_watermark, -1);
    assert_eq!(topics[0].partitions[1].aborted_transactions, vec![(7, 10)]);
    if version >= 12 {
        assert_eq!(topics[0].partitions[1].current_leader_id, 7);
        assert_eq!(topics[0].partitions[1].current_leader_epoch, 9);
    } else {
        assert_eq!(
            topics[0].partitions[1].current_leader_id,
            MetadataResponse::NO_LEADER_ID
        );
    }
    if version >= 16 {
        assert_eq!(endpoints.len(), 1);
        assert_eq!(endpoints[0].host, "broker");
    } else {
        assert!(endpoints.is_empty());
    }
}

fn rust_encode_matches_request(version: i16, cell: &str, apache: &[u8]) {
    if cell == "tagged" {
        return;
    }
    let (isolation, max_bytes, topics, rack, session, forgotten, max_wait, min_bytes, _, _, _) =
        decode_request(version, apache);
    let mut encoded = BytesMut::new();
    encode_fetch_request_with_forgotten(
        &mut encoded,
        version,
        max_wait,
        min_bytes,
        max_bytes,
        isolation,
        &topics,
        if rack.is_empty() {
            None
        } else {
            Some(rack.as_str())
        },
        session,
        &forgotten,
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
    let (topics, endpoints, error_code, session_id, throttle) = decode_response(version, apache);
    let mut entries = Vec::new();
    for topic in &topics {
        let name = topic.topic.as_str();
        for part in &topic.partitions {
            entries.push((topic.topic_id, name, part.partition, part.clone()));
        }
    }
    let mut encoded = BytesMut::new();
    FetchResponse::of_with_endpoints(
        &mut encoded,
        version,
        error_code,
        throttle,
        session_id,
        &entries,
        &endpoints,
    )
    .unwrap();
    assert_eq!(
        encoded.as_ref(),
        apache,
        "response re-encode v{version} {cell}"
    );
}

#[tokio::test]
async fn apache_fetch_wire_fixtures_decode_offline() {
    let root = fixture_root();
    let manifest = String::from_utf8(read(&root.join("manifest.json")).await).unwrap();
    assert!(manifest.contains("\"task\": \"KL01-19\""));
    let mut bodies = 0u32;
    let mut receipts = 0u32;
    for peer in PEERS {
        for version in CRATE_MIN..=FETCH_CRATE_MAX_VERSION {
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
                    let slice = &req[..prefix];
                    assert!(
                        decode_fetch_request(&mut &slice[..], version).is_err(),
                        "{peer} v{version} {cell} request prefix {prefix} decoded"
                    );
                }
                for prefix in 0..resp.len() {
                    let slice = &resp[..prefix];
                    assert!(
                        decode_fetch_response(&mut &slice[..], version).is_err(),
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
    assert_eq!(bodies, 512, "five peers × generated Fetch bodies");
    assert_eq!(receipts, 1, "3.9.1 v18 remains unsupported");
    assert!(manifest.contains("\"total_bodies\": 512"));
    assert!(manifest.contains("\"total_unsupported_receipts\": 1"));
}

#[test]
fn crate_refuses_unspoken_fetch_versions() {
    let mut buf = BytesMut::new();
    for version in [0, 1, 2, 3, 19, 20] {
        buf.clear();
        assert!(
            encode_fetch_request(&mut buf, version, 500, 1, 1024, 0, &[], None).is_err(),
            "crate must refuse Fetch v{version}"
        );
        assert!(decode_fetch_request(&mut &[][..], version).is_err());
        assert!(decode_fetch_response(&mut &[][..], version).is_err());
        assert!(decode_fetch_request_with_high_watermarks(&mut &[][..], version).is_err());
    }
    let mut buf = BytesMut::new();
    encode_fetch_request(&mut buf, 9, 500, 1, 1024, 0, &[], None).unwrap();
    assert!(decode_fetch_request(&mut buf.as_ref(), 18).is_err());
}

fn fetched(
    partition: i32,
    error_code: i16,
    high_watermark: i64,
    last_stable_offset: i64,
    log_start_offset: i64,
    aborted: Vec<(i64, i64)>,
    preferred_read_replica: i32,
    current_leader_id: i32,
    current_leader_epoch: i32,
) -> FetchedPartition {
    FetchedPartition {
        partition,
        error_code,
        high_watermark,
        last_stable_offset,
        log_start_offset,
        aborted_transactions: aborted,
        preferred_read_replica,
        current_leader_id,
        current_leader_epoch,
        diverging_epoch: EpochEndOffset::UNDEFINED_EPOCH,
        diverging_end_offset: EpochEndOffset::UNDEFINED_EPOCH_OFFSET,
        snapshot_end_offset: EpochEndOffset::UNDEFINED_EPOCH_OFFSET,
        snapshot_epoch: EpochEndOffset::UNDEFINED_EPOCH,
        records: Vec::new(),
    }
}

#[test]
fn rust_session_response_roundtrip_matches_gates() {
    for version in CRATE_MIN..=FETCH_CRATE_MAX_VERSION {
        let ok = fetched(
            0,
            0,
            250,
            240,
            100,
            Vec::new(),
            if version >= 11 {
                7
            } else {
                FetchedPartition::INVALID_PREFERRED_REPLICA_ID
            },
            MetadataResponse::NO_LEADER_ID,
            RecordBatch::NO_PARTITION_LEADER_EPOCH,
        );
        let failed = fetched(
            3,
            3,
            -1,
            -1,
            FetchedPartition::INVALID_LOG_START_OFFSET,
            vec![(7, 10)],
            FetchedPartition::INVALID_PREFERRED_REPLICA_ID,
            if version >= 12 {
                7
            } else {
                MetadataResponse::NO_LEADER_ID
            },
            if version >= 12 {
                9
            } else {
                RecordBatch::NO_PARTITION_LEADER_EPOCH
            },
        );
        let topic_id = if version >= 13 { SESSION_ID } else { [0; 16] };
        let name = if version >= 13 {
            ""
        } else {
            "pl-fetch-session"
        };
        let endpoints = if version >= 16 {
            vec![NodeEndpoint::new(7, "broker", 9092, None)]
        } else {
            Vec::new()
        };
        let mut buf = BytesMut::new();
        FetchResponse::of_with_endpoints(
            &mut buf,
            version,
            0,
            60,
            if version >= 7 { 42 } else { 0 },
            &[(topic_id, name, 0, ok), (topic_id, name, 3, failed)],
            &endpoints,
        )
        .unwrap();
        let (decoded, got_eps, error_code, session_id, throttle) =
            decode_response(version, buf.as_ref());
        assert_eq!(error_code, 0);
        assert_eq!(throttle, 60);
        assert_eq!(decoded[0].partitions[1].error_code, 3);
        assert_eq!(decoded[0].partitions[1].aborted_transactions, vec![(7, 10)]);
        if version >= 7 {
            assert_eq!(session_id, 42);
        }
        if version >= 11 {
            assert_eq!(decoded[0].partitions[0].preferred_read_replica, 7);
        }
        if version >= 12 {
            assert_eq!(decoded[0].partitions[1].current_leader_id, 7);
        }
        if version >= 16 {
            assert_eq!(got_eps.len(), 1);
        } else {
            assert!(got_eps.is_empty());
        }
    }
}

fn hex_bytes(bytes: &[u8]) -> String {
    bytes.iter().fold(String::new(), |mut out, byte| {
        use std::fmt::Write as _;
        write!(&mut out, "{byte:02x}").unwrap();
        out
    })
}

fn rust_bodies(version: i16) -> (BytesMut, BytesMut) {
    let name = topic_name("session_errors");
    let id = if version >= 13 { SESSION_ID } else { [0; 16] };
    let topic = FetchTopic {
        topic: if version >= 13 {
            String::new()
        } else {
            name.into()
        },
        topic_id: id,
        partitions: vec![
            FetchPartition::partition_data(
                0,
                100,
                if version >= 5 {
                    50
                } else {
                    INVALID_LOG_START_OFFSET
                },
                1_048_576,
                if version >= 9 { Some(7) } else { None },
                if version >= 12 { Some(6) } else { None },
            ),
            FetchPartition::partition_data(3, 200, INVALID_LOG_START_OFFSET, 1_048_576, None, None),
        ],
    };
    let forgotten = if version >= 7 {
        vec![ForgottenTopic {
            topic: if version >= 13 {
                String::new()
            } else {
                "pl-forgotten".into()
            },
            topic_id: if version >= 13 { FORGOTTEN_ID } else { [0; 16] },
            partitions: vec![1],
        }]
    } else {
        Vec::new()
    };
    let mut req = BytesMut::new();
    encode_fetch_request_with_forgotten(
        &mut req,
        version,
        5000,
        1,
        10_485_760,
        1,
        std::slice::from_ref(&topic),
        if version >= 11 { Some("rack-a") } else { None },
        if version >= 7 {
            FetchMetadata::new(42, 3)
        } else {
            FetchMetadata::LEGACY
        },
        &forgotten,
    )
    .unwrap();
    let ok = fetched(
        0,
        0,
        250,
        240,
        100,
        Vec::new(),
        if version >= 11 {
            7
        } else {
            FetchedPartition::INVALID_PREFERRED_REPLICA_ID
        },
        MetadataResponse::NO_LEADER_ID,
        RecordBatch::NO_PARTITION_LEADER_EPOCH,
    );
    let failed = fetched(
        3,
        3,
        -1,
        -1,
        FetchedPartition::INVALID_LOG_START_OFFSET,
        vec![(7, 10)],
        FetchedPartition::INVALID_PREFERRED_REPLICA_ID,
        if version >= 12 {
            7
        } else {
            MetadataResponse::NO_LEADER_ID
        },
        if version >= 12 {
            9
        } else {
            RecordBatch::NO_PARTITION_LEADER_EPOCH
        },
    );
    let endpoints = if version >= 16 {
        vec![NodeEndpoint::new(7, "broker", 9092, None)]
    } else {
        Vec::new()
    };
    let display_name = if version >= 13 { "" } else { name };
    let mut resp = BytesMut::new();
    FetchResponse::of_with_endpoints(
        &mut resp,
        version,
        0,
        60,
        if version >= 7 { 42 } else { 0 },
        &[(id, display_name, 0, ok), (id, display_name, 3, failed)],
        &endpoints,
    )
    .unwrap();
    (req, resp)
}

#[test]
fn rust_fetch_output_decodes_with_pinned_apache_when_jars_present() {
    let jars = PathBuf::from("/tmp/partitionline-conformance");
    let slf4j = jars.join("slf4j-api-1.7.36.jar");
    let source = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests/conformance/java/ConformanceFetchWire.java");
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
        let classes = PathBuf::from("/tmp").join(format!("fetch-wire-classes-{peer}"));
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
        for version in CRATE_MIN..=FETCH_CRATE_MAX_VERSION {
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
                    "ConformanceFetchWire",
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
                    && stdout.contains("\"max_wait_ms\":5000")
                    && stdout.contains("\"isolation_level\":1")
                    && stdout.contains("\"request_topics\":1")
                    && stdout.contains("\"throttle_time_ms\":60")
                    && stdout.contains("\"response_topics\":1"),
                "{peer} v{version} fields: {stdout}"
            );
            parsed += 1;
        }
    }
    assert_eq!(parsed, 74, "3.9.1 v4-v17 plus four current peers v4-v18");
}

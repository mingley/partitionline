//! KL-01 protocol oracles: Produce, Fetch, Metadata, and ListOffsets
//! decoded required fields vs pinned Kafka 3.9.1 and 4.1.0.
//!
//! Drives shipped `partitionline::protocol` encode/decode. Compares
//! semantic required fields, not raw frames, client IDs, or correlation IDs.
//! Fixture cells load from `tests/fixtures/protocol_oracles/`. A missing
//! cell, empty identity, or unclassified skip fails the test.

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing,
    reason = "integration-test helpers; clippy.toml allow-*-in-tests covers #[test] only"
)]

use std::collections::BTreeSet;
use std::time::Duration;

use bytes::{Bytes, BytesMut};
use partitionline::error::{NOT_LEADER_OR_FOLLOWER, UNKNOWN_TOPIC_OR_PARTITION};
use partitionline::net::BrokerConn;
use partitionline::protocol::api::{
    decode_api_versions_handshake, decode_metadata_response, decode_produce_response,
    encode_api_versions_request, encode_metadata_request, encode_metadata_response,
    encode_produce_request, encode_produce_response, encode_produce_response_with_throttle, Broker,
    MetadataResponse, PartitionMetadata, ProducePartitionData, ProducePartitionResponse,
    ProduceRecordError, ProduceTopicData, TopicMetadata,
};
use partitionline::protocol::api_keys::{
    pick_version, API_VERSIONS, FETCH, LIST_OFFSETS, METADATA, PRODUCE,
};
use partitionline::protocol::epoch::EpochEndOffset;
use partitionline::protocol::fetch::{
    decode_fetch_response, encode_fetch_request, encode_fetch_response,
    encode_fetch_response_with_throttle, FetchPartition, FetchTopic, FetchedPartition,
    FetchedTopic,
};
use partitionline::protocol::offsets::{
    decode_list_offsets_topics_response, encode_list_offsets_request,
    encode_list_offsets_topics_response, encode_list_offsets_topics_response_with_throttle,
    ListOffsetsPartition, ListOffsetsResponsePartition, ListOffsetsTopicResponse, LATEST_TIMESTAMP,
};
use partitionline::protocol::records::{Record, RecordBatch};

const MATRIX_REL: &str = "tests/fixtures/protocol_oracles/matrix.json";
const MATRIX_JSON: &str = include_str!("fixtures/protocol_oracles/matrix.json");
const APIS: [&str; 4] = ["Produce", "Fetch", "Metadata", "ListOffsets"];
const PINS: [&str; 2] = ["3.9.1", "4.1.0"];
const THROTTLE_MS: i32 = 42;

fn crate_spoken(api: &str) -> Vec<i16> {
    match api {
        "Produce" => (3..=12).collect(),
        "Fetch" => (4..=17).collect(),
        "Metadata" => (1..=13).collect(),
        "ListOffsets" => (1..=10).collect(),
        other => panic!("unknown API {other}"),
    }
}

/// Crate spoken ∩ Apache `validVersions` for the pin (not a guess).
fn expected_pin_supported(api: &str, pin: &str) -> Vec<i16> {
    match (api, pin) {
        // 3.9.1 ProduceRequest.json validVersions 0-11.
        ("Produce", "3.9.1") => (3..=11).collect(),
        // 4.1.0 ProduceRequest.json validVersions 3-13; crate speaks 3-12.
        ("Produce", "4.1.0") => (3..=12).collect(),
        // 3.9.1 FetchRequest.json validVersions 0-17 (v17 is KIP-853).
        ("Fetch", "3.9.1") => (4..=17).collect(),
        // 4.1.0 FetchRequest.json validVersions 4-18; crate speaks 4-17.
        ("Fetch", "4.1.0") => (4..=17).collect(),
        // 3.9.1 MetadataRequest.json validVersions 0-12.
        ("Metadata", "3.9.1") => (1..=12).collect(),
        // 4.1.0 MetadataRequest.json validVersions 0-13.
        ("Metadata", "4.1.0") => (1..=13).collect(),
        // 3.9.1 ListOffsetsRequest.json validVersions 0-9.
        ("ListOffsets", "3.9.1") => (1..=9).collect(),
        // 4.1.0 ListOffsetsRequest.json validVersions 1-10.
        ("ListOffsets", "4.1.0") => (1..=10).collect(),
        other => panic!("unknown cell {other:?}"),
    }
}

fn expected_identity(pin: &str) -> String {
    format!("fixture:apache/kafka:{pin}")
}

fn leftover_empty(buf: &[u8], what: &str) {
    assert!(buf.is_empty(), "{what}: leftover {} bytes", buf.len());
}

#[test]
fn advertised_matrix_semantic_oracles() {
    let cells = load_cells(MATRIX_JSON);
    let mut seen = BTreeSet::new();
    for cell in &cells {
        assert!(
            !cell.identity.trim().is_empty(),
            "{} {} identity must be a non-empty string",
            cell.api,
            cell.pin
        );
        assert!(
            !cell.skip,
            "{} {} unclassified skip is a failure",
            cell.api, cell.pin
        );
        assert_eq!(
            cell.identity,
            expected_identity(&cell.pin),
            "{} {} fixture identity",
            cell.api,
            cell.pin
        );
        assert_eq!(
            cell.crate_spoken,
            crate_spoken(&cell.api),
            "{} {} crate_spoken must match in-tree spoken range",
            cell.api,
            cell.pin
        );
        assert_eq!(
            cell.pin_supported,
            expected_pin_supported(&cell.api, &cell.pin),
            "{} {} pin_supported must match crate ∩ Apache validVersions",
            cell.api,
            cell.pin
        );
        let spoken: BTreeSet<i16> = cell.crate_spoken.iter().copied().collect();
        let supported: BTreeSet<i16> = cell.pin_supported.iter().copied().collect();
        let classified: BTreeSet<i16> = cell.classified_diffs.iter().map(|d| d.version).collect();
        let missing: BTreeSet<i16> = spoken.difference(&supported).copied().collect();
        assert_eq!(
            missing, classified,
            "{} {} classified_diffs must be exactly crate-spoken versions the pin does not support",
            cell.api, cell.pin
        );
        for diff in &cell.classified_diffs {
            assert!(
                !diff.reason.trim().is_empty(),
                "{} {} classified diff v{} needs a reason",
                cell.api,
                cell.pin,
                diff.version
            );
        }
        let inserted = seen.insert((cell.api.clone(), cell.pin.clone()));
        assert!(inserted, "duplicate cell {} {}", cell.api, cell.pin);
        println!(
            "protocol_oracles: api={} pin={} identity={} versions={:?}",
            cell.api, cell.pin, cell.identity, cell.pin_supported
        );
        match cell.api.as_str() {
            "Produce" => produce_oracles(cell),
            "Fetch" => fetch_oracles(cell),
            "Metadata" => metadata_oracles(cell),
            "ListOffsets" => list_offsets_oracles(cell),
            other => panic!("unknown API {other}"),
        }
    }
    for api in APIS {
        for pin in PINS {
            assert!(
                seen.contains(&(api.to_string(), pin.to_string())),
                "advertised cell {api} {pin} missing from {MATRIX_REL}"
            );
        }
    }
    assert_eq!(seen.len(), 8, "expected 8 advertised cells");
}

fn produce_oracles(cell: &Cell) {
    for version in &cell.pin_supported {
        produce_roundtrip(*version, true);
        produce_roundtrip(*version, false);
    }
}

fn produce_roundtrip(version: i16, gated_present: bool) {
    let success = produce_success(gated_present);
    let unknown = ProducePartitionResponse::partition_response(
        "missing-topic",
        0,
        UNKNOWN_TOPIC_OR_PARTITION,
    );
    let not_leader =
        ProducePartitionResponse::partition_response("ok-topic", 1, NOT_LEADER_OR_FOLLOWER);
    let parts = vec![success, unknown, not_leader];
    let mut buf = BytesMut::new();
    encode_produce_response_with_throttle(&mut buf, version, &parts, THROTTLE_MS).unwrap();
    let mut cur = buf.as_ref();
    let (decoded, _endpoints, throttle) = decode_produce_response(&mut cur, version).unwrap();
    leftover_empty(cur, &format!("Produce v{version}"));
    assert_eq!(throttle, THROTTLE_MS, "Produce v{version} throttle");
    assert_eq!(decoded.len(), 3, "Produce v{version} partition count");

    let got = produce_part(&decoded, "ok-topic", 0);
    assert_eq!(got.error_code, 0);
    assert_eq!(got.base_offset, 42);
    assert_eq!(got.log_append_time_ms, 1_700_000_000_000);
    if version >= 5 {
        assert_eq!(got.log_start_offset, 7);
    } else {
        assert_eq!(
            got.log_start_offset,
            ProducePartitionResponse::INVALID_OFFSET
        );
    }
    if version >= 8 && gated_present {
        assert_eq!(
            got.record_errors,
            vec![ProduceRecordError::new(0, Some("bad".into()))]
        );
        assert_eq!(got.error_message.as_deref(), Some("batch dropped"));
    } else {
        assert!(
            got.record_errors.is_empty(),
            "Produce v{version} record_errors default"
        );
        assert_eq!(
            got.error_message, None,
            "Produce v{version} error_message JSON default null"
        );
    }
    if version >= 10 && gated_present {
        assert_eq!(got.current_leader_id, 1);
        assert_eq!(got.current_leader_epoch, 5);
    } else {
        assert_eq!(got.current_leader_id, MetadataResponse::NO_LEADER_ID);
        assert_eq!(
            got.current_leader_epoch,
            RecordBatch::NO_PARTITION_LEADER_EPOCH
        );
    }

    assert_eq!(
        produce_part(&decoded, "missing-topic", 0).error_code,
        UNKNOWN_TOPIC_OR_PARTITION
    );
    assert_eq!(
        produce_part(&decoded, "ok-topic", 1).error_code,
        NOT_LEADER_OR_FOLLOWER
    );

    let mut conv = BytesMut::new();
    encode_produce_response(&mut conv, version, &parts).unwrap();
    let mut cur = conv.as_ref();
    let (_, _, throttle0) = decode_produce_response(&mut cur, version).unwrap();
    leftover_empty(cur, &format!("Produce v{version} convenience throttle"));
    assert_eq!(
        throttle0, 0,
        "convenience encode writes JSON default throttle 0"
    );
}

fn produce_part<'a>(
    parts: &'a [ProducePartitionResponse],
    topic: &str,
    partition: i32,
) -> &'a ProducePartitionResponse {
    parts
        .iter()
        .find(|p| p.topic == topic && p.partition == partition)
        .unwrap_or_else(|| panic!("missing Produce {topic}-{partition}"))
}

fn produce_success(gated_present: bool) -> ProducePartitionResponse {
    let mut part = ProducePartitionResponse::partition_response_with_offsets(
        "ok-topic",
        0,
        0,
        42,
        1_700_000_000_000,
        7,
    );
    if gated_present {
        part.record_errors = vec![ProduceRecordError::new(0, Some("bad".into()))];
        part.error_message = Some("batch dropped".into());
        part.current_leader_id = 1;
        part.current_leader_epoch = 5;
    }
    part
}

fn fetch_oracles(cell: &Cell) {
    for version in &cell.pin_supported {
        fetch_roundtrip(*version, true);
        fetch_roundtrip(*version, false);
    }
}

fn fetch_roundtrip(version: i16, gated_present: bool) {
    let topic_id = [0x11; 16];
    let topic_name = "ok-topic";
    let topics = vec![FetchedTopic {
        topic: topic_name.into(),
        topic_id,
        partitions: vec![
            fetch_success(gated_present),
            FetchedPartition::partition_response(1, UNKNOWN_TOPIC_OR_PARTITION),
            FetchedPartition::partition_response(2, NOT_LEADER_OR_FOLLOWER),
        ],
    }];
    let mut buf = BytesMut::new();
    encode_fetch_response_with_throttle(&mut buf, version, &topics, THROTTLE_MS).unwrap();
    let mut cur = buf.as_ref();
    let (decoded, _endpoints, error_code, _session, throttle) =
        decode_fetch_response(&mut cur, version).unwrap();
    leftover_empty(cur, &format!("Fetch v{version}"));
    assert_eq!(throttle, THROTTLE_MS, "Fetch v{version} throttle");
    assert_eq!(error_code, 0);
    assert_eq!(decoded.len(), 1);
    if version >= 13 {
        assert!(
            decoded[0].topic.is_empty(),
            "Fetch v{version} uses topic id"
        );
        assert_eq!(decoded[0].topic_id, topic_id);
    } else {
        assert_eq!(decoded[0].topic, topic_name);
        assert_eq!(decoded[0].topic_id, [0u8; 16]);
    }
    assert_eq!(decoded[0].partitions.len(), 3);

    let got = &decoded[0].partitions[0];
    assert_eq!(got.partition, 0);
    assert_eq!(got.error_code, 0);
    assert_eq!(got.high_watermark, 10);
    assert_eq!(got.last_stable_offset, 9);
    if version >= 5 {
        assert_eq!(got.log_start_offset, 1);
    } else {
        assert_eq!(
            got.log_start_offset,
            FetchedPartition::INVALID_LOG_START_OFFSET
        );
    }
    assert_eq!(got.aborted_transactions, vec![(99, 2)]);
    if version >= 11 && gated_present {
        assert_eq!(got.preferred_read_replica, 3);
    } else {
        assert_eq!(
            got.preferred_read_replica,
            FetchedPartition::INVALID_PREFERRED_REPLICA_ID
        );
    }
    if version >= 12 && gated_present {
        assert_eq!(got.current_leader_id, 1);
        assert_eq!(got.current_leader_epoch, 4);
        assert_eq!(got.diverging_epoch, 2);
        assert_eq!(got.diverging_end_offset, 8);
        assert_eq!(got.snapshot_end_offset, 6);
        assert_eq!(got.snapshot_epoch, 2);
    } else {
        assert_eq!(got.current_leader_id, MetadataResponse::NO_LEADER_ID);
        assert_eq!(
            got.current_leader_epoch,
            RecordBatch::NO_PARTITION_LEADER_EPOCH
        );
        assert_eq!(got.diverging_epoch, EpochEndOffset::UNDEFINED_EPOCH);
        assert_eq!(
            got.diverging_end_offset,
            EpochEndOffset::UNDEFINED_EPOCH_OFFSET
        );
        assert_eq!(
            got.snapshot_end_offset,
            EpochEndOffset::UNDEFINED_EPOCH_OFFSET
        );
        assert_eq!(got.snapshot_epoch, EpochEndOffset::UNDEFINED_EPOCH);
    }

    assert_eq!(decoded[0].partitions[1].partition, 1);
    assert_eq!(
        decoded[0].partitions[1].error_code,
        UNKNOWN_TOPIC_OR_PARTITION
    );
    assert_eq!(decoded[0].partitions[2].partition, 2);
    assert_eq!(decoded[0].partitions[2].error_code, NOT_LEADER_OR_FOLLOWER);

    let mut conv = BytesMut::new();
    encode_fetch_response(&mut conv, version, &topics).unwrap();
    let mut cur = conv.as_ref();
    let (_, _, _, _, throttle0) = decode_fetch_response(&mut cur, version).unwrap();
    leftover_empty(cur, &format!("Fetch v{version} convenience throttle"));
    assert_eq!(throttle0, 0);
}

fn fetch_success(gated_present: bool) -> FetchedPartition {
    let mut part = FetchedPartition::partition_response(0, 0);
    part.high_watermark = 10;
    part.last_stable_offset = 9;
    part.log_start_offset = 1;
    part.aborted_transactions = vec![(99, 2)];
    if gated_present {
        part.preferred_read_replica = 3;
        part.current_leader_id = 1;
        part.current_leader_epoch = 4;
        part.diverging_epoch = 2;
        part.diverging_end_offset = 8;
        part.snapshot_end_offset = 6;
        part.snapshot_epoch = 2;
    }
    part
}

fn metadata_oracles(cell: &Cell) {
    for version in &cell.pin_supported {
        metadata_roundtrip(*version, true);
        metadata_roundtrip(*version, false);
    }
}

fn metadata_roundtrip(version: i16, gated_present: bool) {
    let resp = metadata_body(gated_present);
    let mut buf = BytesMut::new();
    encode_metadata_response(&mut buf, version, &resp).unwrap();
    let mut cur = buf.as_ref();
    let decoded = decode_metadata_response(&mut cur, version).unwrap();
    leftover_empty(cur, &format!("Metadata v{version}"));

    if version >= 3 {
        assert_eq!(decoded.throttle_time_ms, THROTTLE_MS);
    } else {
        assert_eq!(decoded.throttle_time_ms, 0);
    }
    assert_eq!(decoded.brokers.len(), 1);
    assert_eq!(decoded.brokers[0].node_id, 1);
    assert_eq!(decoded.brokers[0].host, "localhost");
    assert_eq!(decoded.brokers[0].port, 9092);
    if version >= 1 {
        assert_eq!(decoded.brokers[0].rack.as_deref(), Some("rack-a"));
        assert_eq!(decoded.controller_id, 1);
    } else {
        assert_eq!(decoded.controller_id, MetadataResponse::NO_CONTROLLER_ID);
    }
    if version >= 2 && gated_present {
        assert_eq!(decoded.cluster_id.as_deref(), Some("cluster-x"));
    } else {
        assert_eq!(decoded.cluster_id, None);
    }
    assert_eq!(decoded.topics.len(), 2);
    let ok = &decoded.topics[0];
    assert_eq!(ok.error_code, 0);
    assert_eq!(ok.name.as_deref(), Some("ok-topic"));
    if version >= 1 {
        assert!(!ok.is_internal);
    }
    assert_eq!(ok.partitions.len(), 1);
    let p = &ok.partitions[0];
    assert_eq!(p.error_code, 0);
    assert_eq!(p.partition_index, 0);
    assert_eq!(p.leader_id, 1);
    assert_eq!(p.replica_nodes, vec![1, 2]);
    assert_eq!(p.isr_nodes, vec![1]);
    if version >= 7 && gated_present {
        assert_eq!(p.leader_epoch, 4);
    } else {
        assert_eq!(p.leader_epoch, RecordBatch::NO_PARTITION_LEADER_EPOCH);
    }
    if version >= 5 {
        assert_eq!(p.offline_replicas, vec![2]);
    } else {
        assert!(p.offline_replicas.is_empty());
    }
    let err = &decoded.topics[1];
    assert_eq!(err.error_code, UNKNOWN_TOPIC_OR_PARTITION);
    assert_eq!(err.name.as_deref(), Some("missing-topic"));
    assert_eq!(decoded.error_code, 0);
}

fn metadata_body(gated_present: bool) -> MetadataResponse {
    let epoch = if gated_present { Some(4) } else { None };
    MetadataResponse {
        throttle_time_ms: THROTTLE_MS,
        brokers: vec![Broker::new(1, "localhost", 9092, Some("rack-a".into()))],
        cluster_id: gated_present.then(|| "cluster-x".into()),
        controller_id: 1,
        topics: vec![
            TopicMetadata::new(
                0,
                "ok-topic",
                false,
                vec![PartitionMetadata::new(
                    0,
                    0,
                    Some(1),
                    epoch,
                    vec![1, 2],
                    vec![1],
                    vec![2],
                )],
            ),
            TopicMetadata::error(UNKNOWN_TOPIC_OR_PARTITION, Some("missing-topic"), [0; 16]),
        ],
        cluster_authorized_operations: MetadataResponse::AUTHORIZED_OPERATIONS_OMITTED,
        error_code: 0,
    }
}

fn list_offsets_oracles(cell: &Cell) {
    for version in &cell.pin_supported {
        list_offsets_roundtrip(*version, true);
        list_offsets_roundtrip(*version, false);
    }
}

fn list_offsets_roundtrip(version: i16, gated_present: bool) {
    let epoch = if gated_present {
        6
    } else {
        ListOffsetsPartition::UNKNOWN_EPOCH
    };
    let topics = vec![ListOffsetsTopicResponse::new(
        "ok-topic",
        vec![
            ListOffsetsResponsePartition::new(
                0,
                ListOffsetsPartition::ok(1_700_000_000_000, 15, epoch),
            ),
            ListOffsetsResponsePartition::error(1, UNKNOWN_TOPIC_OR_PARTITION),
            ListOffsetsResponsePartition::error(2, NOT_LEADER_OR_FOLLOWER),
        ],
    )];
    let mut buf = BytesMut::new();
    encode_list_offsets_topics_response_with_throttle(&mut buf, version, &topics, THROTTLE_MS)
        .unwrap();
    let mut cur = buf.as_ref();
    let (decoded, throttle) = decode_list_offsets_topics_response(&mut cur, version).unwrap();
    leftover_empty(cur, &format!("ListOffsets v{version}"));
    if version >= 2 {
        assert_eq!(throttle, THROTTLE_MS);
    } else {
        assert_eq!(throttle, 0);
    }
    assert_eq!(decoded.len(), 1);
    assert_eq!(decoded[0].name, "ok-topic");
    assert_eq!(decoded[0].partitions.len(), 3);

    let ok = &decoded[0].partitions[0];
    assert_eq!(ok.partition_index, 0);
    assert_eq!(ok.error_code, 0);
    assert_eq!(ok.timestamp, 1_700_000_000_000);
    assert_eq!(ok.offset, 15);
    if version >= 4 && gated_present {
        assert_eq!(ok.leader_epoch, 6);
    } else {
        assert_eq!(ok.leader_epoch, ListOffsetsPartition::UNKNOWN_EPOCH);
    }
    assert_eq!(
        decoded[0].partitions[1].error_code,
        UNKNOWN_TOPIC_OR_PARTITION
    );
    assert_eq!(decoded[0].partitions[2].error_code, NOT_LEADER_OR_FOLLOWER);

    let mut conv = BytesMut::new();
    encode_list_offsets_topics_response(&mut conv, version, &topics).unwrap();
    let mut cur = conv.as_ref();
    let (_, throttle0) = decode_list_offsets_topics_response(&mut cur, version).unwrap();
    leftover_empty(cur, &format!("ListOffsets v{version} convenience throttle"));
    assert_eq!(throttle0, 0);
}

#[tokio::test]
#[ignore = "live broker; scripts/ci-protocol-oracles.sh REQUIRE_BROKER=1"]
async fn live_broker_decoded_semantics() {
    let identity = std::env::var("PROTOCOL_ORACLES_IDENTITY").unwrap_or_default();
    assert!(
        !identity.trim().is_empty(),
        "live cell identity must be a non-empty string (broker-identity.sh stamp)"
    );
    let bootstrap = std::env::var("KAFKA_BOOTSTRAP").unwrap_or_else(|_| "127.0.0.1:9092".into());
    println!("protocol_oracles live: identity={identity} bootstrap={bootstrap}");
    let timeout = Duration::from_secs(15);
    let mut conn = BrokerConn::connect(&bootstrap, "pl-protocol-oracles", timeout)
        .await
        .unwrap_or_else(|e| panic!("live broker connect {bootstrap}: {e}"));
    let av_body = conn
        .roundtrip(
            API_VERSIONS,
            3,
            |buf| encode_api_versions_request(buf, 3, "partitionline", "0.1.0"),
            timeout,
        )
        .await
        .unwrap();
    let av = decode_api_versions_handshake(&av_body, 3).unwrap();
    assert_eq!(av.error_code, 0, "ApiVersions error");

    let pin = pin_from_identity(&identity);
    live_metadata(&mut conn, &av, pin.as_deref(), timeout).await;
    live_produce(&mut conn, &av, pin.as_deref(), timeout).await;
    live_fetch(&mut conn, &av, pin.as_deref(), timeout).await;
    live_list_offsets(&mut conn, &av, pin.as_deref(), timeout).await;
}

fn pin_from_identity(identity: &str) -> Option<String> {
    if identity.contains("3.9.1") {
        Some("3.9.1".into())
    } else if identity.contains("4.1.0") {
        Some("4.1.0".into())
    } else {
        None
    }
}

fn broker_range(av: &partitionline::protocol::api::ApiVersionsResponse, key: i16) -> (i16, i16) {
    av.api_version(key)
        .map(|k| (k.min_version, k.max_version))
        .unwrap_or((0, -1))
}

fn live_version(
    av: &partitionline::protocol::api::ApiVersionsResponse,
    api: &str,
    key: i16,
    pin: Option<&str>,
) -> i16 {
    let crate_versions = crate_spoken(api);
    let client_min = *crate_versions.first().unwrap();
    let client_max = if let Some(pin) = pin {
        *expected_pin_supported(api, pin).last().unwrap()
    } else {
        *crate_versions.last().unwrap()
    };
    let (bmin, bmax) = broker_range(av, key);
    pick_version(bmin, bmax, client_min, client_max).unwrap_or_else(|| {
        panic!("{api} no overlapping version crate={client_min}-{client_max} broker={bmin}-{bmax}")
    })
}

async fn live_metadata(
    conn: &mut BrokerConn,
    av: &partitionline::protocol::api::ApiVersionsResponse,
    pin: Option<&str>,
    timeout: Duration,
) {
    let version = live_version(av, "Metadata", METADATA, pin);
    let names = ["pl-oracle-missing-topic"];
    let body = conn
        .roundtrip(
            METADATA,
            version,
            |buf| encode_metadata_request(buf, version, Some(&names.map(str::to_string)), false),
            timeout,
        )
        .await
        .unwrap();
    let mut cur = body.as_ref();
    let decoded = decode_metadata_response(&mut cur, version).unwrap();
    leftover_empty(cur, &format!("live Metadata v{version}"));
    assert!(
        !decoded.brokers.is_empty(),
        "live Metadata brokers required"
    );
    for b in &decoded.brokers {
        assert!(!b.host.is_empty(), "live Metadata broker host");
        assert!(b.port > 0, "live Metadata broker port");
    }
    if version >= 1 {
        assert_ne!(
            decoded.controller_id,
            MetadataResponse::NO_CONTROLLER_ID,
            "live Metadata controller_id"
        );
    }
    if version >= 2 {
        assert!(
            decoded.cluster_id.as_ref().is_some_and(|s| !s.is_empty()),
            "live Metadata cluster_id"
        );
    }
    assert!(
        decoded
            .topics
            .iter()
            .any(|t| t.error_code == UNKNOWN_TOPIC_OR_PARTITION
                || t.name.as_deref() == Some("pl-oracle-missing-topic")),
        "live Metadata unknown topic row"
    );
    println!(
        "protocol_oracles live Metadata v{version} brokers={} controller={} throttle={}",
        decoded.brokers.len(),
        decoded.controller_id,
        decoded.throttle_time_ms
    );
}

async fn live_produce(
    conn: &mut BrokerConn,
    av: &partitionline::protocol::api::ApiVersionsResponse,
    pin: Option<&str>,
    timeout: Duration,
) {
    let version = live_version(av, "Produce", PRODUCE, pin);
    let rec = Record {
        offset: 0,
        timestamp: 0,
        key: None,
        value: Some(Bytes::from_static(b"oracle")),
        headers: vec![],
    };
    let topics = vec![ProduceTopicData {
        topic: "pl-oracle-missing-topic".into(),
        partitions: vec![ProducePartitionData {
            index: 0,
            records: RecordBatch::from_records(vec![rec]),
        }],
    }];
    let body = conn
        .roundtrip(
            PRODUCE,
            version,
            |buf| encode_produce_request(buf, version, None, 1, 5000, &topics),
            timeout,
        )
        .await
        .unwrap();
    let mut cur = body.as_ref();
    let (parts, _endpoints, _throttle) = decode_produce_response(&mut cur, version).unwrap();
    leftover_empty(cur, &format!("live Produce v{version}"));
    assert!(!parts.is_empty(), "live Produce partitions");
    let p = &parts[0];
    assert_eq!(p.topic, "pl-oracle-missing-topic");
    assert_eq!(p.partition, 0);
    // Unknown topic or success if auto-create is on — both are decoded semantics.
    if p.error_code == 0 {
        assert!(p.base_offset >= 0, "live Produce success base_offset");
        if version >= 5 {
            assert!(p.log_start_offset >= 0, "live Produce log_start_offset");
        }
    } else {
        assert_ne!(p.error_code, 0);
    }
    if version >= 8 && p.error_code != 0 {
        // error_message may be null (JSON default) even on error.
        let _ = &p.error_message;
        let _ = &p.record_errors;
    }
    if version >= 10 {
        assert!(
            p.current_leader_id == MetadataResponse::NO_LEADER_ID || p.current_leader_id >= 0,
            "live Produce current_leader_id"
        );
    }
    println!(
        "protocol_oracles live Produce v{version} error_code={} base_offset={}",
        p.error_code, p.base_offset
    );
}

async fn live_fetch(
    conn: &mut BrokerConn,
    av: &partitionline::protocol::api::ApiVersionsResponse,
    pin: Option<&str>,
    timeout: Duration,
) {
    let version = live_version(av, "Fetch", FETCH, pin);
    let topics = vec![FetchTopic {
        topic: "pl-oracle-missing-topic".into(),
        topic_id: [0; 16],
        partitions: vec![FetchPartition::partition_data(0, 0, -1, 1024, None, None)],
    }];
    let body = conn
        .roundtrip(
            FETCH,
            version,
            |buf| encode_fetch_request(buf, version, 50, 1, 1024, 0, &topics, None),
            timeout,
        )
        .await
        .unwrap();
    let mut cur = body.as_ref();
    let (decoded, _endpoints, top_err, _session, _throttle) =
        decode_fetch_response(&mut cur, version).unwrap();
    leftover_empty(cur, &format!("live Fetch v{version}"));
    if version >= 13 {
        // Topic id zeros → unknown topic id or empty topics; still decoded.
        assert!(
            top_err != 0
                || decoded.iter().any(|t| t
                    .partitions
                    .iter()
                    .any(|p| p.error_code != 0 || p.partition == 0)),
            "live Fetch v{version} required partition fields"
        );
    } else {
        assert!(!decoded.is_empty(), "live Fetch topics");
        let p = &decoded[0].partitions[0];
        assert_eq!(p.partition, 0);
        let _ = p.error_code;
        let _ = p.high_watermark;
        let _ = p.last_stable_offset;
        if version >= 5 {
            let _ = p.log_start_offset;
        }
        if version >= 11 {
            let _ = p.preferred_read_replica;
        }
        let _ = &p.aborted_transactions;
    }
    println!(
        "protocol_oracles live Fetch v{version} topics={}",
        decoded.len()
    );
}

async fn live_list_offsets(
    conn: &mut BrokerConn,
    av: &partitionline::protocol::api::ApiVersionsResponse,
    pin: Option<&str>,
    timeout: Duration,
) {
    let version = live_version(av, "ListOffsets", LIST_OFFSETS, pin);
    let body = conn
        .roundtrip(
            LIST_OFFSETS,
            version,
            |buf| {
                encode_list_offsets_request(
                    buf,
                    version,
                    0,
                    "pl-oracle-missing-topic",
                    0,
                    RecordBatch::NO_PARTITION_LEADER_EPOCH,
                    LATEST_TIMESTAMP,
                    1000,
                )
            },
            timeout,
        )
        .await
        .unwrap();
    let mut cur = body.as_ref();
    let (topics, _throttle) = decode_list_offsets_topics_response(&mut cur, version).unwrap();
    leftover_empty(cur, &format!("live ListOffsets v{version}"));
    assert!(!topics.is_empty(), "live ListOffsets topics");
    let p = &topics[0].partitions[0];
    assert_eq!(p.partition_index, 0);
    let _ = p.error_code;
    let _ = p.timestamp;
    let _ = p.offset;
    if version >= 4 {
        let _ = p.leader_epoch;
    }
    println!(
        "protocol_oracles live ListOffsets v{version} error_code={} offset={}",
        p.error_code, p.offset
    );
}

struct Cell {
    api: String,
    pin: String,
    identity: String,
    crate_spoken: Vec<i16>,
    pin_supported: Vec<i16>,
    classified_diffs: Vec<ClassifiedDiff>,
    skip: bool,
}

struct ClassifiedDiff {
    version: i16,
    reason: String,
}

fn load_cells(raw: &str) -> Vec<Cell> {
    let json = parse_json(raw);
    let obj = json.as_object();
    let cells = obj
        .iter()
        .find(|(k, _)| k == "cells")
        .unwrap_or_else(|| panic!("{MATRIX_REL} missing cells"))
        .1
        .as_array();
    cells
        .iter()
        .map(|c| {
            let o = c.as_object();
            Cell {
                api: field_str(o, "api"),
                pin: field_str(o, "pin"),
                identity: field_str(o, "identity"),
                crate_spoken: field_i16s(o, "crate_spoken"),
                pin_supported: field_i16s(o, "pin_supported"),
                classified_diffs: field_diffs(o),
                skip: field_bool(o, "skip").unwrap_or(false),
            }
        })
        .collect()
}

fn field_str(obj: &[(String, Json)], key: &str) -> String {
    obj.iter()
        .find(|(k, _)| k == key)
        .unwrap_or_else(|| panic!("missing {key}"))
        .1
        .as_str()
        .to_string()
}

fn field_i16s(obj: &[(String, Json)], key: &str) -> Vec<i16> {
    obj.iter()
        .find(|(k, _)| k == key)
        .unwrap_or_else(|| panic!("missing {key}"))
        .1
        .as_array()
        .iter()
        .map(|v| i16::try_from(v.as_i64()).expect("version fits i16"))
        .collect()
}

fn field_bool(obj: &[(String, Json)], key: &str) -> Option<bool> {
    obj.iter().find(|(k, _)| k == key).map(|(_, v)| v.as_bool())
}

fn field_diffs(obj: &[(String, Json)]) -> Vec<ClassifiedDiff> {
    let Some((_, v)) = obj.iter().find(|(k, _)| k == "classified_diffs") else {
        return Vec::new();
    };
    v.as_array()
        .iter()
        .map(|d| {
            let o = d.as_object();
            ClassifiedDiff {
                version: i16::try_from(field_i64(o, "version")).expect("diff version"),
                reason: field_str(o, "reason"),
            }
        })
        .collect()
}

fn field_i64(obj: &[(String, Json)], key: &str) -> i64 {
    obj.iter()
        .find(|(k, _)| k == key)
        .unwrap_or_else(|| panic!("missing {key}"))
        .1
        .as_i64()
}

#[derive(Clone)]
enum Json {
    Null,
    Bool(bool),
    Int(i64),
    Str(String),
    Arr(Vec<Json>),
    Obj(Vec<(String, Json)>),
}

impl Json {
    fn as_object(&self) -> &[(String, Json)] {
        match self {
            Self::Obj(o) => o,
            _ => panic!("expected object"),
        }
    }
    fn as_array(&self) -> &[Json] {
        match self {
            Self::Arr(a) => a,
            _ => panic!("expected array"),
        }
    }
    fn as_str(&self) -> &str {
        match self {
            Self::Str(s) => s,
            _ => panic!("expected string"),
        }
    }
    fn as_i64(&self) -> i64 {
        match self {
            Self::Int(n) => *n,
            _ => panic!("expected int"),
        }
    }
    fn as_bool(&self) -> bool {
        match self {
            Self::Bool(b) => *b,
            _ => panic!("expected bool"),
        }
    }
}

struct Parser<'a> {
    s: &'a [u8],
    i: usize,
}

impl<'a> Parser<'a> {
    fn new(s: &'a str) -> Self {
        Self {
            s: s.as_bytes(),
            i: 0,
        }
    }

    fn peek(&self) -> Option<u8> {
        self.s.get(self.i).copied()
    }

    fn bump(&mut self) -> u8 {
        let b = self.s[self.i];
        self.i += 1;
        b
    }

    fn skip_ws(&mut self) {
        while matches!(self.peek(), Some(b' ' | b'\n' | b'\r' | b'\t')) {
            self.i += 1;
        }
    }

    fn parse_value(&mut self) -> Json {
        self.skip_ws();
        match self.peek() {
            Some(b'{') => self.parse_object(),
            Some(b'[') => self.parse_array(),
            Some(b'"') => Json::Str(self.parse_string()),
            Some(b't') => {
                self.expect(b"true");
                Json::Bool(true)
            }
            Some(b'f') => {
                self.expect(b"false");
                Json::Bool(false)
            }
            Some(b'n') => {
                self.expect(b"null");
                Json::Null
            }
            Some(b'-' | b'0'..=b'9') => Json::Int(self.parse_int()),
            other => panic!("unexpected json at {}: {other:?}", self.i),
        }
    }

    fn expect(&mut self, lit: &[u8]) {
        for b in lit {
            assert_eq!(self.bump(), *b, "json literal");
        }
    }

    fn parse_object(&mut self) -> Json {
        assert_eq!(self.bump(), b'{');
        let mut out = Vec::new();
        loop {
            self.skip_ws();
            if self.peek() == Some(b'}') {
                self.i += 1;
                break;
            }
            if !out.is_empty() {
                assert_eq!(self.bump(), b',', "object comma");
                self.skip_ws();
                if self.peek() == Some(b'}') {
                    self.i += 1;
                    break;
                }
            }
            let key = self.parse_string();
            self.skip_ws();
            assert_eq!(self.bump(), b':', "object colon");
            let val = self.parse_value();
            out.push((key, val));
        }
        Json::Obj(out)
    }

    fn parse_array(&mut self) -> Json {
        assert_eq!(self.bump(), b'[');
        let mut out = Vec::new();
        loop {
            self.skip_ws();
            if self.peek() == Some(b']') {
                self.i += 1;
                break;
            }
            if !out.is_empty() {
                assert_eq!(self.bump(), b',', "array comma");
                self.skip_ws();
                if self.peek() == Some(b']') {
                    self.i += 1;
                    break;
                }
            }
            out.push(self.parse_value());
        }
        Json::Arr(out)
    }

    fn parse_string(&mut self) -> String {
        self.skip_ws();
        assert_eq!(self.bump(), b'"');
        let mut out = String::new();
        loop {
            match self.bump() {
                b'"' => break,
                b'\\' => match self.bump() {
                    b'"' => out.push('"'),
                    b'\\' => out.push('\\'),
                    b'/' => out.push('/'),
                    b'n' => out.push('\n'),
                    b'r' => out.push('\r'),
                    b't' => out.push('\t'),
                    b'u' => {
                        let mut hex = [0u8; 4];
                        hex[0] = self.bump();
                        hex[1] = self.bump();
                        hex[2] = self.bump();
                        hex[3] = self.bump();
                        let n =
                            u32::from_str_radix(std::str::from_utf8(&hex).unwrap(), 16).unwrap();
                        out.push(char::from_u32(n).unwrap());
                    }
                    other => panic!("bad escape {other}"),
                },
                c => out.push(char::from(c)),
            }
        }
        out
    }

    fn parse_int(&mut self) -> i64 {
        self.skip_ws();
        let start = self.i;
        if self.peek() == Some(b'-') {
            self.i += 1;
        }
        while matches!(self.peek(), Some(b'0'..=b'9')) {
            self.i += 1;
        }
        std::str::from_utf8(&self.s[start..self.i])
            .unwrap()
            .parse()
            .expect("json int")
    }
}

fn parse_json(s: &str) -> Json {
    let mut p = Parser::new(s);
    let v = p.parse_value();
    p.skip_ws();
    assert_eq!(p.i, p.s.len(), "json leftover");
    v
}

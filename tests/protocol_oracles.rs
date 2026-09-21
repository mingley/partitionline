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
    decode_api_versions_handshake, decode_metadata_response, decode_produce_request,
    decode_produce_response, encode_api_versions_request, encode_metadata_request,
    encode_metadata_response, encode_produce_request, encode_produce_response,
    encode_produce_response_with_throttle, Broker, MetadataResponse, NodeEndpoint,
    PartitionMetadata, ProducePartitionData, ProducePartitionResponse, ProduceRecordError,
    ProduceTopicData, TopicMetadata,
};
use partitionline::protocol::api_keys::{
    pick_version, API_VERSIONS, FETCH, LIST_OFFSETS, METADATA, PRODUCE,
};
use partitionline::protocol::epoch::EpochEndOffset;
use partitionline::protocol::fetch::{
    decode_fetch_request, decode_fetch_response, encode_fetch_request,
    encode_fetch_request_with_cluster_id, encode_fetch_request_with_replica_id,
    encode_fetch_request_with_replica_state, encode_fetch_request_with_session,
    encode_fetch_response, encode_fetch_response_with_endpoints,
    encode_fetch_response_with_throttle, FetchMetadata, FetchPartition, FetchTopic,
    FetchedPartition, FetchedTopic, CONSUMER_REPLICA_ID, INVALID_LOG_START_OFFSET,
};
use partitionline::protocol::offsets::{
    decode_list_offsets_topics_response, encode_list_offsets_request,
    encode_list_offsets_topics_response, encode_list_offsets_topics_response_with_throttle,
    ListOffsetsPartition, ListOffsetsResponsePartition, ListOffsetsTopicResponse, LATEST_TIMESTAMP,
};
use partitionline::protocol::records::{
    self, ControlRecordType, EndTransactionMarker, Record, RecordBatch,
};

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

/// Consume the committed Apache 3.9.1 Produce v9 smoke pair without Java or network.
#[test]
fn apache_produce_v9_smoke_fixture_decodes() {
    const REQ: &[u8] = include_bytes!("fixtures/protocol_oracles/smoke_produce_v9_request.bin");
    const RESP: &[u8] = include_bytes!("fixtures/protocol_oracles/smoke_produce_v9_response.bin");
    let (txn, acks, timeout_ms, topics) =
        decode_produce_request(&mut &REQ[..], 9).expect("apache produce request");
    assert!(txn.is_none());
    assert_eq!(acks, 1);
    assert_eq!(timeout_ms, 5000);
    assert_eq!(topics.len(), 1);
    assert_eq!(topics[0].topic, "smoke-topic");
    assert_eq!(topics[0].partitions.len(), 1);
    assert_eq!(topics[0].partitions[0].index, 0);

    let (parts, _endpoints, throttle_ms) =
        decode_produce_response(&mut &RESP[..], 9).expect("apache produce response");
    assert_eq!(throttle_ms, 42);
    assert_eq!(parts.len(), 1);
    assert_eq!(parts[0].topic, "smoke-topic");
    assert_eq!(parts[0].partition, 0);
    assert_eq!(parts[0].error_code, 0);
    assert_eq!(parts[0].base_offset, 100);
    assert_eq!(parts[0].log_append_time_ms, 1_700_000_000_000);
    assert_eq!(parts[0].log_start_offset, 0);
}

/// KL01-04: Independent Produce wire fixtures covering advertised version boundaries.
///
/// Consumes committed Apache Kafka 3.9.1 binary fixtures offline without requiring
/// Java or network access. Verifies classic/flexible transition, null transactionalId/records,
/// throttle placement, partition errors, and current-leader tags.
#[test]
fn apache_produce_boundary_fixtures_decode_offline() {
    // 1. Produce v3: oldest supported version, classic format, null txn id, null records, partition error
    {
        const REQ: &[u8] =
            include_bytes!("fixtures/protocol_oracles/produce_v3_classic_request.bin");
        const RESP: &[u8] =
            include_bytes!("fixtures/protocol_oracles/produce_v3_classic_response.bin");

        let (txn, acks, timeout_ms, topics) =
            decode_produce_request(&mut &REQ[..], 3).expect("produce v3 request");
        assert!(txn.is_none(), "v3 null transactional_id");
        assert_eq!(acks, -1);
        assert_eq!(timeout_ms, 5000);
        assert_eq!(topics.len(), 1);
        assert_eq!(topics[0].topic, "produce-v3-classic");
        assert_eq!(topics[0].partitions.len(), 2);
        assert_eq!(topics[0].partitions[0].index, 0);
        assert!(
            topics[0].partitions[0].records.records.is_empty(),
            "v3 null records decodes empty batch"
        );
        assert_eq!(topics[0].partitions[1].index, 1);
        assert!(
            topics[0].partitions[1].records.records.is_empty(),
            "v3 null records decodes empty batch"
        );

        let (parts, endpoints, throttle_ms) =
            decode_produce_response(&mut &RESP[..], 3).expect("produce v3 response");
        assert_eq!(throttle_ms, 25, "v3 throttle placement after responses");
        assert!(endpoints.is_empty(), "v3 no node endpoints");
        assert_eq!(parts.len(), 2);
        assert_eq!(parts[0].topic, "produce-v3-classic");
        assert_eq!(parts[0].partition, 0);
        assert_eq!(parts[0].error_code, 0);
        assert_eq!(parts[0].base_offset, 100);
        assert_eq!(parts[0].log_append_time_ms, 1_710_000_000_000);
        assert_eq!(
            parts[0].log_start_offset,
            ProducePartitionResponse::INVALID_OFFSET,
            "v3 log_start_offset omitted below v5"
        );
        assert_eq!(parts[1].partition, 1);
        assert_eq!(parts[1].error_code, UNKNOWN_TOPIC_OR_PARTITION);
        assert_eq!(parts[1].base_offset, -1);
    }

    // 2. Produce v5: classic format, boundary for log_start_offset gate (v5+)
    {
        const REQ: &[u8] =
            include_bytes!("fixtures/protocol_oracles/produce_v5_start_offset_request.bin");
        const RESP: &[u8] =
            include_bytes!("fixtures/protocol_oracles/produce_v5_start_offset_response.bin");

        let (txn, acks, timeout_ms, topics) =
            decode_produce_request(&mut &REQ[..], 5).expect("produce v5 request");
        assert_eq!(
            txn.as_deref(),
            Some("txn-produce-v5"),
            "v5 non-null transactional_id"
        );
        assert_eq!(acks, 1);
        assert_eq!(timeout_ms, 3000);
        assert_eq!(topics.len(), 1);
        assert_eq!(topics[0].topic, "produce-v5-start-offset");
        assert_eq!(topics[0].partitions.len(), 1);

        let (parts, endpoints, throttle_ms) =
            decode_produce_response(&mut &RESP[..], 5).expect("produce v5 response");
        assert_eq!(throttle_ms, 40, "v5 throttle placement");
        assert!(endpoints.is_empty());
        assert_eq!(parts.len(), 1);
        assert_eq!(parts[0].topic, "produce-v5-start-offset");
        assert_eq!(parts[0].partition, 0);
        assert_eq!(parts[0].error_code, 0);
        assert_eq!(parts[0].base_offset, 200);
        assert_eq!(parts[0].log_append_time_ms, 1_710_000_001_000);
        assert_eq!(
            parts[0].log_start_offset, 50,
            "v5 log_start_offset present on wire"
        );
    }

    // 3. Produce v8: classic format, boundary before flexible transition, with record_errors and error_message
    {
        const REQ: &[u8] =
            include_bytes!("fixtures/protocol_oracles/produce_v8_classic_errors_request.bin");
        const RESP: &[u8] =
            include_bytes!("fixtures/protocol_oracles/produce_v8_classic_errors_response.bin");

        let (txn, acks, timeout_ms, topics) =
            decode_produce_request(&mut &REQ[..], 8).expect("produce v8 request");
        assert_eq!(txn.as_deref(), Some("txn-produce-v8"));
        assert_eq!(acks, -1);
        assert_eq!(timeout_ms, 7500);
        assert_eq!(topics.len(), 1);
        assert_eq!(topics[0].topic, "produce-v8-classic-errors");
        assert_eq!(topics[0].partitions.len(), 2);

        let (parts, endpoints, throttle_ms) =
            decode_produce_response(&mut &RESP[..], 8).expect("produce v8 response");
        assert_eq!(throttle_ms, 60, "v8 throttle placement");
        assert!(endpoints.is_empty());
        assert_eq!(parts.len(), 2);

        assert_eq!(parts[0].partition, 0);
        assert_eq!(parts[0].error_code, 0);
        assert_eq!(parts[0].base_offset, 300);
        assert_eq!(parts[0].log_append_time_ms, 1_710_000_002_000);
        assert_eq!(parts[0].log_start_offset, 120);
        assert!(parts[0].record_errors.is_empty());
        assert_eq!(parts[0].error_message, None);

        assert_eq!(parts[1].partition, 1);
        assert_eq!(parts[1].error_code, NOT_LEADER_OR_FOLLOWER);
        assert_eq!(parts[1].base_offset, -1);
        assert_eq!(
            parts[1].error_message.as_deref(),
            Some("Not leader for partition")
        );
        assert_eq!(parts[1].record_errors.len(), 2);
        assert_eq!(parts[1].record_errors[0].batch_index, 0);
        assert_eq!(
            parts[1].record_errors[0].message.as_deref(),
            Some("Record invalid")
        );
        assert_eq!(parts[1].record_errors[1].batch_index, 1);
        assert_eq!(
            parts[1].record_errors[1].message.as_deref(),
            Some("Offset out of range")
        );
    }

    // 4. Produce v9: flexible transition boundary with null transactional_id, null records, and partition errors
    {
        const REQ: &[u8] =
            include_bytes!("fixtures/protocol_oracles/produce_v9_flexible_nulls_request.bin");
        const RESP: &[u8] =
            include_bytes!("fixtures/protocol_oracles/produce_v9_flexible_nulls_response.bin");

        let (txn, acks, timeout_ms, topics) =
            decode_produce_request(&mut &REQ[..], 9).expect("produce v9 request");
        assert!(txn.is_none(), "v9 flexible null transactional_id");
        assert_eq!(acks, 1);
        assert_eq!(timeout_ms, 4000);
        assert_eq!(topics.len(), 1);
        assert_eq!(topics[0].topic, "produce-v9-flexible-nulls");
        assert_eq!(topics[0].partitions.len(), 2);

        let (parts, endpoints, throttle_ms) =
            decode_produce_response(&mut &RESP[..], 9).expect("produce v9 response");
        assert_eq!(throttle_ms, 99, "v9 throttle placement");
        assert!(endpoints.is_empty());
        assert_eq!(parts.len(), 2);

        assert_eq!(parts[0].partition, 0);
        assert_eq!(parts[0].error_code, 0);
        assert_eq!(parts[0].base_offset, 400);
        assert_eq!(parts[0].log_append_time_ms, 1_710_000_003_000);
        assert_eq!(parts[0].log_start_offset, 150);

        assert_eq!(parts[1].partition, 1);
        assert_eq!(parts[1].error_code, UNKNOWN_TOPIC_OR_PARTITION);
        assert_eq!(parts[1].base_offset, -1);
        assert_eq!(parts[1].error_message.as_deref(), Some("Unknown topic"));
        assert_eq!(parts[1].record_errors.len(), 1);
        assert_eq!(parts[1].record_errors[0].batch_index, 0);
        assert_eq!(
            parts[1].record_errors[0].message.as_deref(),
            Some("Invalid batch")
        );
    }

    // 5. Produce v10: flexible format, boundary for current-leader tagged field (present vs omitted/default)
    {
        const REQ: &[u8] =
            include_bytes!("fixtures/protocol_oracles/produce_v10_current_leader_request.bin");
        const RESP: &[u8] =
            include_bytes!("fixtures/protocol_oracles/produce_v10_current_leader_response.bin");

        let (txn, acks, timeout_ms, topics) =
            decode_produce_request(&mut &REQ[..], 10).expect("produce v10 request");
        assert_eq!(txn.as_deref(), Some("txn-produce-v10"));
        assert_eq!(acks, -1);
        assert_eq!(timeout_ms, 6000);
        assert_eq!(topics.len(), 1);
        assert_eq!(topics[0].topic, "produce-v10-current-leader");

        let (parts, endpoints, throttle_ms) =
            decode_produce_response(&mut &RESP[..], 10).expect("produce v10 response");
        assert_eq!(throttle_ms, 150, "v10 throttle placement");
        assert!(endpoints.is_empty());
        assert_eq!(parts.len(), 2);

        // Partition 0: present current-leader tag
        assert_eq!(parts[0].partition, 0);
        assert_eq!(parts[0].error_code, 0);
        assert_eq!(parts[0].base_offset, 500);
        assert_eq!(parts[0].log_append_time_ms, 1_710_000_004_000);
        assert_eq!(parts[0].log_start_offset, 200);
        assert_eq!(
            parts[0].current_leader_id, 3,
            "v10 current_leader_id present"
        );
        assert_eq!(
            parts[0].current_leader_epoch, 12,
            "v10 current_leader_epoch present"
        );

        // Partition 1: omitted / default current-leader tag
        assert_eq!(parts[1].partition, 1);
        assert_eq!(parts[1].error_code, NOT_LEADER_OR_FOLLOWER);
        assert_eq!(parts[1].base_offset, -1);
        assert_eq!(
            parts[1].error_message.as_deref(),
            Some("Broker not leader")
        );
        assert_eq!(
            parts[1].current_leader_id,
            MetadataResponse::NO_LEADER_ID,
            "v10 current_leader omitted default"
        );
        assert_eq!(
            parts[1].current_leader_epoch,
            RecordBatch::NO_PARTITION_LEADER_EPOCH,
            "v10 current_leader omitted default"
        );
    }

    // 6. Produce v11: flexible format, highest version supported by Apache Kafka 3.9.1
    {
        const REQ: &[u8] =
            include_bytes!("fixtures/protocol_oracles/produce_v11_max_3_9_1_request.bin");
        const RESP: &[u8] =
            include_bytes!("fixtures/protocol_oracles/produce_v11_max_3_9_1_response.bin");

        let (txn, acks, timeout_ms, topics) =
            decode_produce_request(&mut &REQ[..], 11).expect("produce v11 request");
        assert_eq!(txn.as_deref(), Some("txn-produce-v11"));
        assert_eq!(acks, -1);
        assert_eq!(timeout_ms, 10000);
        assert_eq!(topics.len(), 1);
        assert_eq!(topics[0].topic, "produce-v11-max");

        let (parts, endpoints, throttle_ms) =
            decode_produce_response(&mut &RESP[..], 11).expect("produce v11 response");
        assert_eq!(throttle_ms, 200, "v11 throttle placement");
        assert!(endpoints.is_empty());
        assert_eq!(parts.len(), 1);

        assert_eq!(parts[0].partition, 0);
        assert_eq!(parts[0].error_code, 0);
        assert_eq!(parts[0].base_offset, 600);
        assert_eq!(parts[0].log_append_time_ms, 1_710_000_005_000);
        assert_eq!(parts[0].log_start_offset, 250);
        assert_eq!(parts[0].current_leader_id, 5);
        assert_eq!(parts[0].current_leader_epoch, 20);
        assert_eq!(
            parts[0].error_message.as_deref(),
            Some("transaction abortable error")
        );
        assert_eq!(parts[0].record_errors.len(), 1);
        assert_eq!(parts[0].record_errors[0].batch_index, 0);
        assert_eq!(
            parts[0].record_errors[0].message.as_deref(),
            Some("aborted txn")
        );
    }
}

/// KL01-04: Negative and mutation tests.
///
/// Verifies that mutating field order or required version gates fails decoding.
#[test]
fn produce_version_gate_and_field_order_mutations_fail() {
    const V3_RESP: &[u8] =
        include_bytes!("fixtures/protocol_oracles/produce_v3_classic_response.bin");
    const V5_RESP: &[u8] =
        include_bytes!("fixtures/protocol_oracles/produce_v5_start_offset_response.bin");
    const V8_RESP: &[u8] =
        include_bytes!("fixtures/protocol_oracles/produce_v8_classic_errors_response.bin");
    const V9_RESP: &[u8] =
        include_bytes!("fixtures/protocol_oracles/produce_v9_flexible_nulls_response.bin");
    const V10_RESP: &[u8] =
        include_bytes!("fixtures/protocol_oracles/produce_v10_current_leader_response.bin");

    // Mutation 1: Version gate mutation for log_start_offset (gate is v5+).
    // Decoding v3 response (no log_start_offset on wire) with v5 decoder fails (buffer underflow).
    let mut cur = V3_RESP;
    assert!(
        decode_produce_response(&mut cur, 5).is_err(),
        "decoding v3 response as v5 must fail because log_start_offset is missing"
    );

    // Mutation 2: Decoding v5 response with v4 decoder ignores log_start_offset and leaves unconsumed bytes.
    let mut cur = V5_RESP;
    let _ = decode_produce_response(&mut cur, 4).expect("decodes without log_start_offset");
    assert!(
        !cur.is_empty(),
        "decoding v5 response with v4 decoder must leave unconsumed log_start_offset bytes"
    );

    // Mutation 3: Version gate mutation for record_errors / error_message (gate is v8+).
    // Decoding v8 response with v7 decoder ignores record_errors and error_message, leaving unconsumed bytes.
    let mut cur = V8_RESP;
    let _ = decode_produce_response(&mut cur, 7).expect("decodes without v8 fields");
    assert!(
        !cur.is_empty(),
        "decoding v8 response with v7 decoder must leave unconsumed record_errors bytes"
    );

    // Mutation 4: Flexible version gate mutation (gate is v9+).
    // Decoding v8 classic response as v9 flexible must fail.
    let mut cur = V8_RESP;
    assert!(
        decode_produce_response(&mut cur, 9).is_err(),
        "decoding v8 classic response as v9 flexible must fail with format mismatch"
    );

    // Decoding v9 flexible response as v8 classic must fail.
    let mut cur = V9_RESP;
    assert!(
        decode_produce_response(&mut cur, 8).is_err(),
        "decoding v9 flexible response as v8 classic must fail with format mismatch"
    );

    // Mutation 5: Version gate mutation for current_leader (gate is v10+).
    // Decoding v10 response as v9 ignores current_leader tag and decodes default sentinels.
    let mut cur = V10_RESP;
    let (parts, _, _) = decode_produce_response(&mut cur, 9).expect("v9 decodes without leader tag");
    assert_eq!(
        parts[0].current_leader_id,
        MetadataResponse::NO_LEADER_ID,
        "v9 gate must not populate current_leader_id"
    );

    // Mutation 6: Mutating field order in ProduceResponse (e.g. putting throttle_time_ms before responses).
    // Wire format for ProduceResponse is responses followed by throttle_time_ms.
    // If throttle_time_ms (e.g. 25i32) is placed before responses array, decode fails.
    let mut mutated_order = BytesMut::new();
    mutated_order.extend_from_slice(&25i32.to_be_bytes()); // mutated throttle first
    mutated_order.extend_from_slice(&1i32.to_be_bytes()); // topic count = 1
    mutated_order.extend_from_slice(&(2i16).to_be_bytes()); // string len = 2
    mutated_order.extend_from_slice(b"t1");
    mutated_order.extend_from_slice(&0i32.to_be_bytes()); // part count = 0
    let mut cur = mutated_order.as_ref();
    assert!(
        decode_produce_response(&mut cur, 3).is_err(),
        "mutating ProduceResponse field order (throttle before responses) must fail"
    );
}

/// Helper to detect if Java toolchain and conformance jars are available.
fn java_conformance_classpath() -> Option<(String, String)> {
    let java_ok = std::process::Command::new("java")
        .arg("-version")
        .output()
        .map(|o| o.status.success())
        .unwrap_or(false);
    if !java_ok {
        return None;
    }

    let cache_dir = std::env::var("CONFORMANCE_CACHE_DIR")
        .unwrap_or_else(|_| "/tmp/partitionline-conformance".into());
    let kafka_jar = format!("{cache_dir}/kafka-clients-3.9.1.jar");
    let slf4j_jar = format!("{cache_dir}/slf4j-api-1.7.36.jar");
    let classes_dir = format!("{cache_dir}/classes");

    if !std::path::Path::new(&kafka_jar).exists()
        || !std::path::Path::new(&slf4j_jar).exists()
        || !std::path::Path::new(&classes_dir).exists()
    {
        return None;
    }

    let cp = format!("{kafka_jar}:{slf4j_jar}:{classes_dir}");
    Some(("java".into(), cp))
}

/// KL01-04: Decode Rust output with Apache where Java is available.
///
/// Encodes Produce requests and responses in Rust across the advertised version boundaries,
/// then runs Apache Kafka's `ProduceRequestData.read` and `ProduceResponseData.read` in Java
/// to verify that Apache successfully decodes partitionline wire output.
#[test]
fn rust_produce_output_decodes_with_apache_when_java_available() {
    let Some((java_bin, cp)) = java_conformance_classpath() else {
        println!("java toolchain / conformance jars not available; skipping live Apache decode of Rust output");
        return;
    };

    let versions: [(i16, Option<&str>, i32); 6] = [
        (3, None, 25),
        (5, Some("txn-v5"), 40),
        (8, Some("txn-v8"), 60),
        (9, None, 99),
        (10, Some("txn-v10"), 150),
        (11, Some("txn-v11"), 200),
    ];

    for (version, txn_id, throttle_ms) in versions {
        // 1. Rust encodes ProduceRequest
        let mut req_buf = BytesMut::new();
        let topic = ProduceTopicData {
            topic: "rust-topic".into(),
            partitions: vec![ProducePartitionData {
                index: 0,
                records: RecordBatch::from_records(vec![]),
            }],
        };
        encode_produce_request(&mut req_buf, version, txn_id, -1, 5000, &[topic])
            .expect("encode produce request in Rust");
        let req_hex: String = req_buf.iter().map(|b| format!("{b:02x}")).collect();

        let req_out = std::process::Command::new(&java_bin)
            .args([
                "-cp",
                &cp,
                "org.apache.kafka.conformance.FixtureGenerator",
                "--decode-rust",
                "req",
                &version.to_string(),
                &req_hex,
            ])
            .output()
            .expect("execute java decode-rust req");
        assert!(
            req_out.status.success(),
            "Apache Java failed to decode Rust ProduceRequest v{version}: {}",
            String::from_utf8_lossy(&req_out.stderr)
        );
        let req_stdout = String::from_utf8_lossy(&req_out.stdout);
        assert!(
            req_stdout.contains(&format!("OK: req v{version}")),
            "Apache output confirmation: {req_stdout}"
        );

        // 2. Rust encodes ProduceResponse
        let mut resp_buf = BytesMut::new();
        let mut part = ProducePartitionResponse::partition_response_with_offsets(
            "rust-topic",
            0,
            0,
            100,
            1_710_000_000_000,
            50,
        );
        if version >= 8 {
            part.error_message = Some("msg".into());
            part.record_errors = vec![ProduceRecordError::new(0, Some("bad".into()))];
        }
        if version >= 10 {
            part.current_leader_id = 2;
            part.current_leader_epoch = 7;
        }
        encode_produce_response_with_throttle(&mut resp_buf, version, &[part], throttle_ms)
            .expect("encode produce response in Rust");
        let resp_hex: String = resp_buf.iter().map(|b| format!("{b:02x}")).collect();

        let resp_out = std::process::Command::new(&java_bin)
            .args([
                "-cp",
                &cp,
                "org.apache.kafka.conformance.FixtureGenerator",
                "--decode-rust",
                "resp",
                &version.to_string(),
                &resp_hex,
            ])
            .output()
            .expect("execute java decode-rust resp");
        assert!(
            resp_out.status.success(),
            "Apache Java failed to decode Rust ProduceResponse v{version}: {}",
            String::from_utf8_lossy(&resp_out.stderr)
        );
        let resp_stdout = String::from_utf8_lossy(&resp_out.stdout);
        assert!(
            resp_stdout.contains(&format!("OK: resp v{version}")),
            "Apache output confirmation: {resp_stdout}"
        );
    }
}

/// KL01-05: Independent Fetch wire fixtures covering advertised version boundaries.
///
/// Consumes committed Apache Kafka 3.9.1 binary fixtures offline without requiring
/// Java or network access. Verifies classic and flexible formats, topic names vs topic IDs,
/// session metadata, forgotten topics, follower replica fetch, replicaState transition,
/// aborted transactions, preferred read replica, DivergingEpoch, CurrentLeader, SnapshotId,
/// node endpoints, replica directory ID, unknown tagged fields, and realistic record batches
/// containing records before requested offset and ABORT/COMMIT marker sequences.
#[test]
fn apache_fetch_boundary_fixtures_decode_offline() {
    // 1. Fetch v4: classic wire format (oldest spoken, topic name, untagged replicaId = -1, omitted logStartOffset)
    {
        const REQ: &[u8] =
            include_bytes!("fixtures/protocol_oracles/fetch_v4_classic_request.bin");
        const RESP: &[u8] =
            include_bytes!("fixtures/protocol_oracles/fetch_v4_classic_response.bin");

        let (isolation, max_bytes, topics, rack, session, forgotten, max_wait_ms, min_bytes, replica_id, replica_epoch, cluster_id) =
            decode_fetch_request(&mut &REQ[..], 4).expect("fetch v4 request");
        assert_eq!(isolation, 0);
        assert_eq!(max_bytes, 10485760);
        assert_eq!(max_wait_ms, 500);
        assert_eq!(min_bytes, 1);
        assert_eq!(replica_id, CONSUMER_REPLICA_ID);
        assert_eq!(replica_epoch, -1);
        assert!(rack.is_empty());
        assert_eq!(session.session_id(), 0);
        assert_eq!(session.epoch(), -1);
        assert!(forgotten.is_empty());
        assert!(cluster_id.is_none());
        assert_eq!(topics.len(), 1);
        assert_eq!(topics[0].topic, "fetch-v4-classic");
        assert_eq!(topics[0].topic_id, [0u8; 16]);
        assert_eq!(topics[0].partitions.len(), 2);
        assert_eq!(topics[0].partitions[0].partition, 0);
        assert_eq!(topics[0].partitions[0].fetch_offset, 0);
        assert_eq!(topics[0].partitions[0].partition_max_bytes, 1048576);
        assert_eq!(topics[0].partitions[0].log_start_offset, INVALID_LOG_START_OFFSET);
        assert_eq!(topics[0].partitions[1].partition, 1);
        assert_eq!(topics[0].partitions[1].fetch_offset, 100);

        let (topics_resp, endpoints, error_code, session_id, throttle_ms) =
            decode_fetch_response(&mut &RESP[..], 4).expect("fetch v4 response");
        assert_eq!(throttle_ms, 25);
        assert_eq!(error_code, 0);
        assert_eq!(session_id, 0);
        assert!(endpoints.is_empty());
        assert_eq!(topics_resp.len(), 1);
        assert_eq!(topics_resp[0].topic, "fetch-v4-classic");
        assert_eq!(topics_resp[0].partitions.len(), 2);
        assert_eq!(topics_resp[0].partitions[0].partition, 0);
        assert_eq!(topics_resp[0].partitions[0].error_code, 0);
        assert_eq!(topics_resp[0].partitions[0].high_watermark, 50);
        assert_eq!(topics_resp[0].partitions[0].last_stable_offset, 45);
        assert_eq!(
            topics_resp[0].partitions[0].log_start_offset,
            FetchedPartition::INVALID_LOG_START_OFFSET,
            "v4 log_start_offset omitted below v5"
        );
        assert!(topics_resp[0].partitions[0].records.is_empty());
        assert_eq!(topics_resp[0].partitions[1].partition, 1);
        assert_eq!(topics_resp[0].partitions[1].error_code, UNKNOWN_TOPIC_OR_PARTITION);
        assert_eq!(topics_resp[0].partitions[1].high_watermark, -1);
    }

    // 2. Fetch v5: classic wire format, follower replica fetch, logStartOffset gate present
    {
        const REQ: &[u8] =
            include_bytes!("fixtures/protocol_oracles/fetch_v5_start_offset_request.bin");
        const RESP: &[u8] =
            include_bytes!("fixtures/protocol_oracles/fetch_v5_start_offset_response.bin");

        let (_, _, topics, _, _, _, max_wait_ms, _, replica_id, _, _) =
            decode_fetch_request(&mut &REQ[..], 5).expect("fetch v5 request");
        assert_eq!(replica_id, 2, "v5 replica fetcher id");
        assert_eq!(max_wait_ms, 1000);
        assert_eq!(topics.len(), 1);
        assert_eq!(topics[0].topic, "fetch-v5-start-offset");
        assert_eq!(topics[0].partitions[0].fetch_offset, 150);
        assert_eq!(topics[0].partitions[0].log_start_offset, 100, "v5 log_start_offset present on wire");

        let (topics_resp, _, _, _, throttle_ms) =
            decode_fetch_response(&mut &RESP[..], 5).expect("fetch v5 response");
        assert_eq!(throttle_ms, 35);
        assert_eq!(topics_resp.len(), 1);
        assert_eq!(topics_resp[0].topic, "fetch-v5-start-offset");
        assert_eq!(topics_resp[0].partitions[0].high_watermark, 250);
        assert_eq!(topics_resp[0].partitions[0].last_stable_offset, 240);
        assert_eq!(topics_resp[0].partitions[0].log_start_offset, 100, "v5 response log_start_offset");
    }

    // 3. Fetch v7: classic wire format, session metadata gate (sessionId, sessionEpoch, forgottenTopicsData), abortedTransactions
    {
        const REQ: &[u8] =
            include_bytes!("fixtures/protocol_oracles/fetch_v7_session_request.bin");
        const RESP: &[u8] =
            include_bytes!("fixtures/protocol_oracles/fetch_v7_session_response.bin");

        let (isolation, _, topics, _, session, forgotten, _, _, _, _, _) =
            decode_fetch_request(&mut &REQ[..], 7).expect("fetch v7 request");
        assert_eq!(isolation, 1, "read_committed isolation");
        assert_eq!(session.session_id(), 42, "v7 session_id");
        assert_eq!(session.epoch(), 3, "v7 session_epoch");
        assert_eq!(topics[0].topic, "fetch-v7-session");
        assert_eq!(forgotten.len(), 1, "v7 forgotten topics");
        assert_eq!(forgotten[0].topic, "forgotten-v7");
        assert_eq!(forgotten[0].partitions, vec![0, 1]);

        let (topics_resp, _, error_code, session_id, throttle_ms) =
            decode_fetch_response(&mut &RESP[..], 7).expect("fetch v7 response");
        assert_eq!(throttle_ms, 45);
        assert_eq!(error_code, 0);
        assert_eq!(session_id, 42, "v7 response session_id");
        assert_eq!(topics_resp[0].topic, "fetch-v7-session");
        assert_eq!(
            topics_resp[0].partitions[0].aborted_transactions,
            vec![(12345, 10), (67890, 40)],
            "v7 aborted transactions list"
        );
    }

    // 4. Fetch v11: classic format boundary before flexible, rackId, preferredReadReplica, realistic record batches
    {
        const REQ: &[u8] =
            include_bytes!("fixtures/protocol_oracles/fetch_v11_batches_records_request.bin");
        const RESP: &[u8] =
            include_bytes!("fixtures/protocol_oracles/fetch_v11_batches_records_response.bin");

        let (_, _, topics, rack, session, _, _, _, _, _, _) =
            decode_fetch_request(&mut &REQ[..], 11).expect("fetch v11 request");
        assert_eq!(rack, "rack-east-az1", "v11 rackId");
        assert_eq!(session.session_id(), 100);
        assert_eq!(session.epoch(), 1);
        assert_eq!(topics[0].partitions[0].fetch_offset, 102, "requested fetch offset 102");
        assert_eq!(topics[0].partitions[0].current_leader_epoch, 5);
        assert_eq!(topics[0].partitions[0].log_start_offset, 100);

        let (topics_resp, _, error_code, session_id, throttle_ms) =
            decode_fetch_response(&mut &RESP[..], 11).expect("fetch v11 response");
        assert_eq!(throttle_ms, 55);
        assert_eq!(error_code, 0);
        assert_eq!(session_id, 100);
        let part = &topics_resp[0].partitions[0];
        assert_eq!(part.preferred_read_replica, 4, "v11 preferred_read_replica");
        assert_eq!(part.aborted_transactions, vec![(9001, 104)]);
        assert_eq!(part.records.len(), 5, "5 realistic record batches");

        // Batch 0: regular batch, base offset 100, 4 records (100, 101, 102, 103)
        // Records 100 and 101 are before the requested offset 102
        let b0 = &part.records[0];
        assert_eq!(b0.base_offset, 100);
        assert_eq!(b0.last_offset(), 103);
        assert!(!b0.is_transactional());
        assert!(!b0.is_control_batch());
        assert_eq!(b0.records.len(), 4);
        assert_eq!(b0.records[0].offset, 100);
        assert_eq!(b0.records[0].key.as_deref(), Some(b"k100".as_slice()));
        assert_eq!(b0.records[0].value.as_deref(), Some(b"v100".as_slice()));
        assert_eq!(b0.records[1].offset, 101);
        assert_eq!(b0.records[2].offset, 102);
        assert_eq!(b0.records[3].offset, 103);

        // Batch 1: transactional batch, producerId 9001, epoch 1, offsets 104, 105
        let b1 = &part.records[1];
        assert_eq!(b1.base_offset, 104);
        assert_eq!(b1.last_offset(), 105);
        assert!(b1.is_transactional());
        assert!(!b1.is_control_batch());
        assert_eq!(b1.producer_id, 9001);
        assert_eq!(b1.producer_epoch, 1);
        assert_eq!(b1.records.len(), 2);
        assert_eq!(b1.records[0].key.as_deref(), Some(b"tx-k1".as_slice()));
        assert_eq!(b1.records[1].key.as_deref(), Some(b"tx-k2".as_slice()));

        // Batch 2: control batch with ABORT marker at offset 106
        let b2 = &part.records[2];
        assert_eq!(b2.base_offset, 106);
        assert_eq!(b2.last_offset(), 106);
        assert!(b2.is_transactional());
        assert!(b2.is_control_batch(), "batch 2 must be control batch");
        assert_eq!(b2.records.len(), 1);
        let m2 = EndTransactionMarker::deserialize(&b2.records[0]).expect("abort marker");
        assert_eq!(m2.control_type(), ControlRecordType::Abort);
        assert_eq!(m2.coordinator_epoch(), 1);

        // Batch 3: transactional batch, producerId 9001, epoch 1, offset 107
        let b3 = &part.records[3];
        assert_eq!(b3.base_offset, 107);
        assert_eq!(b3.last_offset(), 107);
        assert!(b3.is_transactional());
        assert!(!b3.is_control_batch());
        assert_eq!(b3.records.len(), 1);
        assert_eq!(b3.records[0].key.as_deref(), Some(b"tx-k3".as_slice()));

        // Batch 4: control batch with COMMIT marker at offset 108
        let b4 = &part.records[4];
        assert_eq!(b4.base_offset, 108);
        assert_eq!(b4.last_offset(), 108);
        assert!(b4.is_transactional());
        assert!(b4.is_control_batch(), "batch 4 must be control batch");
        assert_eq!(b4.records.len(), 1);
        let m4 = EndTransactionMarker::deserialize(&b4.records[0]).expect("commit marker");
        assert_eq!(m4.control_type(), ControlRecordType::Commit);
        assert_eq!(m4.coordinator_epoch(), 1);
    }

    // 5. Fetch v12: flexible wire format boundary, compact strings/arrays/bytes, clusterId, partition tags (DivergingEpoch, CurrentLeader, SnapshotId), unknown tags
    {
        const REQ: &[u8] =
            include_bytes!("fixtures/protocol_oracles/fetch_v12_flexible_tags_request.bin");
        const RESP: &[u8] =
            include_bytes!("fixtures/protocol_oracles/fetch_v12_flexible_tags_response.bin");

        let (_, _, topics, rack, session, _, _, _, _, _, cluster_id) =
            decode_fetch_request(&mut &REQ[..], 12).expect("fetch v12 request");
        assert_eq!(cluster_id.as_deref(), Some("cluster-v12"), "v12 cluster_id tag 0");
        assert_eq!(rack, "rack-west-1");
        assert_eq!(session.session_id(), 200);
        assert_eq!(session.epoch(), 2);
        assert_eq!(topics[0].topic, "fetch-v12-flexible");
        let p = &topics[0].partitions[0];
        assert_eq!(p.partition, 0);
        assert_eq!(p.fetch_offset, 200);
        assert_eq!(p.current_leader_epoch, 8);
        assert_eq!(p.last_fetched_epoch, 5);
        assert_eq!(p.log_start_offset, 180);

        let (topics_resp, _, error_code, session_id, throttle_ms) =
            decode_fetch_response(&mut &RESP[..], 12).expect("fetch v12 response");
        assert_eq!(throttle_ms, 70);
        assert_eq!(error_code, 0);
        assert_eq!(session_id, 200);
        assert_eq!(topics_resp[0].topic, "fetch-v12-flexible");
        let resp_p = &topics_resp[0].partitions[0];
        assert_eq!(resp_p.preferred_read_replica, 3);
        assert_eq!(resp_p.current_leader_id, 2);
        assert_eq!(resp_p.current_leader_epoch, 10);
        assert_eq!(resp_p.diverging_epoch, 4);
        assert_eq!(resp_p.diverging_end_offset, 195);
        assert_eq!(resp_p.snapshot_end_offset, 190);
        assert_eq!(resp_p.snapshot_epoch, 4);
        assert_eq!(resp_p.records.len(), 1);
        assert_eq!(resp_p.records[0].base_offset, 200);
        assert_eq!(resp_p.records[0].records.len(), 2);
    }

    // 6. Fetch v13: topic IDs wire transition (topicId replaces topic name on wire)
    {
        const REQ: &[u8] =
            include_bytes!("fixtures/protocol_oracles/fetch_v13_topic_ids_request.bin");
        const RESP: &[u8] =
            include_bytes!("fixtures/protocol_oracles/fetch_v13_topic_ids_response.bin");

        let expected_topic_id = [
            0x11, 0x11, 0x22, 0x22, 0x33, 0x33, 0x44, 0x44,
            0x55, 0x55, 0x66, 0x66, 0x77, 0x77, 0x88, 0x88,
        ];
        let expected_forgotten_id = [
            0xaa, 0xaa, 0xbb, 0xbb, 0xcc, 0xcc, 0xdd, 0xdd,
            0x11, 0x11, 0x22, 0x22, 0x33, 0x33, 0x44, 0x44,
        ];

        let (_, _, topics, _, _, forgotten, _, _, _, _, _) =
            decode_fetch_request(&mut &REQ[..], 13).expect("fetch v13 request");
        assert!(topics[0].topic.is_empty(), "v13 request topic name is empty");
        assert_eq!(topics[0].topic_id, expected_topic_id, "v13 request topicId");
        assert!(forgotten[0].topic.is_empty(), "v13 forgotten topic name is empty");
        assert_eq!(forgotten[0].topic_id, expected_forgotten_id, "v13 forgotten topicId");

        let (topics_resp, _, _, session_id, throttle_ms) =
            decode_fetch_response(&mut &RESP[..], 13).expect("fetch v13 response");
        assert_eq!(throttle_ms, 80);
        assert_eq!(session_id, 300);
        assert!(topics_resp[0].topic.is_empty(), "v13 response topic name is empty");
        assert_eq!(topics_resp[0].topic_id, expected_topic_id, "v13 response topicId");
        let resp_p = &topics_resp[0].partitions[0];
        assert_eq!(resp_p.aborted_transactions, vec![(55555, 290)]);
        assert_eq!(resp_p.records.len(), 1);
        assert_eq!(resp_p.records[0].base_offset, 300);
    }

    // 7. Fetch v15: replica-field transition (untagged replicaId omitted, replicaState tagged field 1 added)
    {
        const REQ: &[u8] =
            include_bytes!("fixtures/protocol_oracles/fetch_v15_replica_state_request.bin");
        const RESP: &[u8] =
            include_bytes!("fixtures/protocol_oracles/fetch_v15_replica_state_response.bin");

        let expected_topic_id = [
            0x99, 0x99, 0x88, 0x88, 0x77, 0x77, 0x66, 0x66,
            0x55, 0x55, 0x44, 0x44, 0x33, 0x33, 0x22, 0x22,
        ];

        let (_, _, topics, _, session, _, _, _, replica_id, replica_epoch, _) =
            decode_fetch_request(&mut &REQ[..], 15).expect("fetch v15 request");
        assert_eq!(replica_id, 5, "v15 replicaState replicaId");
        assert_eq!(replica_epoch, 12345, "v15 replicaState replicaEpoch");
        assert_eq!(session.session_id(), 400);
        assert_eq!(topics[0].topic_id, expected_topic_id);

        let (topics_resp, _, _, session_id, throttle_ms) =
            decode_fetch_response(&mut &RESP[..], 15).expect("fetch v15 response");
        assert_eq!(throttle_ms, 90);
        assert_eq!(session_id, 400);
        assert_eq!(topics_resp[0].topic_id, expected_topic_id);
        assert_eq!(topics_resp[0].partitions[0].current_leader_id, 1);
        assert_eq!(topics_resp[0].partitions[0].current_leader_epoch, 15);
    }

    // 8. Fetch v16: top-level nodeEndpoints tagged field 0 in response
    {
        const REQ: &[u8] =
            include_bytes!("fixtures/protocol_oracles/fetch_v16_endpoints_request.bin");
        const RESP: &[u8] =
            include_bytes!("fixtures/protocol_oracles/fetch_v16_endpoints_response.bin");

        let (_, _, _, _, _, _, _, _, replica_id, _, _) =
            decode_fetch_request(&mut &REQ[..], 16).expect("fetch v16 request");
        assert_eq!(replica_id, CONSUMER_REPLICA_ID, "v16 consumer replicaId");

        let (topics_resp, endpoints, _, session_id, throttle_ms) =
            decode_fetch_response(&mut &RESP[..], 16).expect("fetch v16 response");
        assert_eq!(throttle_ms, 110);
        assert_eq!(session_id, 500);
        assert_eq!(endpoints.len(), 2, "v16 nodeEndpoints count");
        assert_eq!(endpoints[0].node_id, 1);
        assert_eq!(endpoints[0].host, "broker1.kafka.local");
        assert_eq!(endpoints[0].port, 9092);
        assert_eq!(endpoints[0].rack.as_deref(), Some("rack-a"));
        assert_eq!(endpoints[1].node_id, 2);
        assert_eq!(endpoints[1].host, "broker2.kafka.local");
        assert_eq!(endpoints[1].port, 9092);
        assert_eq!(endpoints[1].rack.as_deref(), Some("rack-b"));
        assert_eq!(topics_resp[0].partitions[0].high_watermark, 20);
        assert_eq!(topics_resp[0].partitions[0].last_stable_offset, 18);
    }

    // 9. Fetch v17: highest valid version in Apache Kafka 3.9.1, partition replicaDirectoryId tagged field 0
    {
        const REQ: &[u8] =
            include_bytes!("fixtures/protocol_oracles/fetch_v17_max_3_9_1_request.bin");
        const RESP: &[u8] =
            include_bytes!("fixtures/protocol_oracles/fetch_v17_max_3_9_1_response.bin");

        let expected_topic_id = [
            0xfa, 0xce, 0xfe, 0xed, 0xca, 0xfe, 0xbe, 0xef,
            0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08,
        ];
        let expected_replica_dir_id = [
            0xaa, 0xaa, 0xbb, 0xbb, 0x00, 0x00, 0x11, 0x11,
            0x22, 0x22, 0x33, 0x33, 0x44, 0x44, 0x55, 0x55,
        ];

        let (_, _, topics, rack, session, _, _, _, _, _, _) =
            decode_fetch_request(&mut &REQ[..], 17).expect("fetch v17 request");
        assert_eq!(rack, "rack-v17");
        assert_eq!(session.session_id(), 600);
        assert_eq!(session.epoch(), 5);
        assert_eq!(topics[0].topic_id, expected_topic_id);
        let p = &topics[0].partitions[0];
        assert_eq!(p.current_leader_epoch, 20);
        assert_eq!(p.last_fetched_epoch, 19);
        assert_eq!(p.log_start_offset, 900);
        assert_eq!(p.replica_directory_id, expected_replica_dir_id, "v17 partition replicaDirectoryId");

        let (topics_resp, endpoints, _, session_id, throttle_ms) =
            decode_fetch_response(&mut &RESP[..], 17).expect("fetch v17 response");
        assert_eq!(throttle_ms, 120);
        assert_eq!(session_id, 600);
        assert_eq!(endpoints.len(), 1);
        assert_eq!(endpoints[0].node_id, 3);
        assert_eq!(endpoints[0].host, "broker3.kafka.local");
        assert_eq!(endpoints[0].port, 9093);
        assert_eq!(endpoints[0].rack.as_deref(), Some("rack-c"));
        let resp_p = &topics_resp[0].partitions[0];
        assert_eq!(resp_p.current_leader_id, 3);
        assert_eq!(resp_p.current_leader_epoch, 20);
        assert_eq!(resp_p.diverging_epoch, 19);
        assert_eq!(resp_p.diverging_end_offset, 995);
    }
}

/// KL01-05: Distinguish a valid short final fetch tail from malformed complete records.
///
/// Verifies that when a broker truncates a fetch response at max_bytes boundaries,
/// leaving an incomplete trailing record batch (fewer than 12 bytes or fewer than declared
/// batch_len), decoding cleanly stops and returns the complete batches before the cut.
/// In contrast, when a complete record batch has corrupted CRC, invalid magic, or malformed
/// record contents, decoding fails with an Error::protocol.
#[test]
fn fetch_short_tail_and_malformed_records_oracles() {
    let mut rec_bytes = BytesMut::new();
    let batch1 = RecordBatch::from_records(vec![Record {
        offset: 0,
        timestamp: 1_710_000_000_000,
        key: Some(Bytes::from_static(b"k1")),
        value: Some(Bytes::from_static(b"v1")),
        headers: vec![],
    }]);
    let batch2 = RecordBatch::from_records(vec![Record {
        offset: 1,
        timestamp: 1_710_000_000_001,
        key: Some(Bytes::from_static(b"k2")),
        value: Some(Bytes::from_static(b"v2")),
        headers: vec![],
    }]);
    records::encode_record_batch(&mut rec_bytes, &batch1).unwrap();
    let batch1_end = rec_bytes.len();
    records::encode_record_batch(&mut rec_bytes, &batch2).unwrap();

    // 1. Valid short final fetch tail:
    // Case A: Trailing partial header (< 12 bytes remaining)
    let mut short_tail_header = rec_bytes.clone();
    short_tail_header.extend_from_slice(&[0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02]); // 8 bytes < 12
    let mut cur = &short_tail_header[..];
    let decoded = records::decode_record_batches(&mut cur).expect("short header tail must succeed");
    assert_eq!(decoded.len(), 2, "decodes the 2 complete batches before short tail");
    assert_eq!(decoded[0].records[0].value.as_deref(), Some(b"v1".as_slice()));
    assert_eq!(decoded[1].records[0].value.as_deref(), Some(b"v2".as_slice()));

    // Case B: Declared batch_len = 100 bytes, but only 20 bytes payload provided
    let mut short_tail_body = rec_bytes.clone();
    short_tail_body.extend_from_slice(&2i64.to_be_bytes()); // base_offset
    short_tail_body.extend_from_slice(&100i32.to_be_bytes()); // batch_len = 100
    short_tail_body.extend_from_slice(&[0x00; 20]); // only 20 bytes provided instead of 100
    let mut cur = &short_tail_body[..];
    let decoded = records::decode_record_batches(&mut cur).expect("short body tail must succeed");
    assert_eq!(decoded.len(), 2, "decodes the 2 complete batches before truncated tail");

    // 2. Malformed complete records:
    // Case A: Full batch bytes present, but corrupted CRC32-C
    let mut corrupt_crc = rec_bytes.clone();
    let corrupt_idx = batch1_end + 25; // inside batch 2 payload
    corrupt_crc[corrupt_idx] ^= 0xff;
    let mut cur = &corrupt_crc[..];
    let err = records::decode_record_batches(&mut cur).unwrap_err();
    assert!(
        err.to_string().contains("corrupt"),
        "corrupted batch CRC must fail decoding: {err}"
    );

    // Case B: Full batch bytes present, but invalid magic byte (magic 3 instead of 2)
    let mut corrupt_magic = rec_bytes.clone();
    corrupt_magic[batch1_end + 16] = 3; // magic offset is 16 (8 base_offset + 4 batch_len + 4 leader_epoch)
    let mut cur = &corrupt_magic[..];
    assert!(
        records::decode_record_batches(&mut cur).is_err(),
        "invalid magic byte in complete batch must fail"
    );

    // Case C: Full batch bytes present, but corrupted record varint inside the batch
    let mut corrupt_record = rec_bytes.clone();
    let last_byte = corrupt_record.len() - 1;
    corrupt_record[last_byte] ^= 0x80; // corrupt varint in record
    // Recompute CRC so CRC passes but inner record parser fails
    let crc_start = batch1_end + 21;
    let crc = crc32c::crc32c(&corrupt_record[crc_start..]);
    corrupt_record[batch1_end + 17..batch1_end + 21].copy_from_slice(&crc.to_be_bytes());
    let mut cur = &corrupt_record[..];
    assert!(
        records::decode_record_batches(&mut cur).is_err(),
        "corrupted inner record in complete batch must fail"
    );
}

/// KL01-05: Negative and mutation tests for Fetch.
///
/// Verifies that mutating field order or required version gates fails decoding.
#[test]
fn fetch_version_gate_and_field_order_mutations_fail() {
    const V4_RESP: &[u8] =
        include_bytes!("fixtures/protocol_oracles/fetch_v4_classic_response.bin");
    const V5_RESP: &[u8] =
        include_bytes!("fixtures/protocol_oracles/fetch_v5_start_offset_response.bin");
    const V7_RESP: &[u8] =
        include_bytes!("fixtures/protocol_oracles/fetch_v7_session_response.bin");
    const V11_RESP: &[u8] =
        include_bytes!("fixtures/protocol_oracles/fetch_v11_batches_records_response.bin");
    const V12_RESP: &[u8] =
        include_bytes!("fixtures/protocol_oracles/fetch_v12_flexible_tags_response.bin");
    const V13_RESP: &[u8] =
        include_bytes!("fixtures/protocol_oracles/fetch_v13_topic_ids_response.bin");
    const V15_REQ: &[u8] =
        include_bytes!("fixtures/protocol_oracles/fetch_v15_replica_state_request.bin");

    // Mutation 1: Version gate mutation for log_start_offset (gate is v5+).
    // Decoding v4 response (no log_start_offset on wire) with v5 decoder fails with buffer underflow.
    let mut cur = V4_RESP;
    assert!(
        decode_fetch_response(&mut cur, 5).is_err(),
        "decoding v4 response as v5 must fail because log_start_offset is missing on wire"
    );

    // Decoding v5 response with v4 decoder ignores log_start_offset and causes protocol desync/error.
    let mut cur = V5_RESP;
    assert!(
        decode_fetch_response(&mut cur, 4).is_err(),
        "decoding v5 response with v4 decoder must fail due to protocol desync"
    );

    // Mutation 2: Version gate mutation for session metadata (gate is v7+).
    // Decoding v7 response with v6 decoder ignores top-level error_code and session_id, leaving unconsumed bytes.
    let mut cur = V7_RESP;
    let _ = decode_fetch_response(&mut cur, 6).expect("decodes without v7 session metadata");
    assert!(
        !cur.is_empty(),
        "decoding v7 response with v6 decoder must leave unconsumed session metadata bytes"
    );

    // Mutation 3: Flexible version gate mutation (gate is v12+).
    // Decoding v11 classic response as v12 flexible treats 0x00 as null array, leaving unconsumed bytes.
    let mut cur = V11_RESP;
    let _ = decode_fetch_response(&mut cur, 12).expect("decodes null array on format mismatch");
    assert!(
        !cur.is_empty(),
        "decoding v11 classic response as v12 flexible must leave unconsumed bytes"
    );

    // Decoding v12 flexible response as v11 classic must fail with format mismatch.
    let mut cur = V12_RESP;
    assert!(
        decode_fetch_response(&mut cur, 11).is_err(),
        "decoding v12 flexible response as v11 classic must fail"
    );

    // Mutation 4: Topic IDs version gate mutation (gate is v13+).
    // Decoding v12 response as v13 fails because v13 expects UUID (16 bytes) while v12 uses compact string.
    let mut cur = V12_RESP;
    assert!(
        decode_fetch_response(&mut cur, 13).is_err(),
        "decoding v12 response as v13 must fail due to topic name vs topic ID mismatch"
    );

    // Decoding v13 response as v12 fails because v12 expects compact string while v13 uses UUID.
    let mut cur = V13_RESP;
    assert!(
        decode_fetch_response(&mut cur, 12).is_err(),
        "decoding v13 response as v12 must fail due to topic ID vs topic name mismatch"
    );

    // Mutation 5: ReplicaId omission version gate mutation (gate is v15+).
    // Decoding v15 request with v14 decoder fails because v14 expects untagged replicaId before max_wait_ms.
    let mut cur = V15_REQ;
    assert!(
        decode_fetch_request(&mut cur, 14).is_err(),
        "decoding v15 request as v14 must fail because untagged replicaId is missing on v15"
    );

    // Mutation 6: Mutating field order in FetchResponse (e.g. omitting or moving throttle_time_ms).
    // In FetchResponse, throttle_time_ms is written first. If topic count is placed first, decode fails.
    let mut mutated_order = BytesMut::new();
    mutated_order.extend_from_slice(&1i32.to_be_bytes()); // topic count = 1
    mutated_order.extend_from_slice(&(2i16).to_be_bytes()); // string len = 2
    mutated_order.extend_from_slice(b"t1");
    mutated_order.extend_from_slice(&0i32.to_be_bytes()); // part count = 0
    let mut cur = mutated_order.as_ref();
    assert!(
        decode_fetch_response(&mut cur, 4).is_err(),
        "mutating FetchResponse field order must fail decoding"
    );
}

/// KL01-05: Decode Rust output with Apache where Java is available.
///
/// Encodes Fetch requests and responses in Rust across the advertised version boundaries,
/// then runs Apache Kafka's `FetchRequestData.read` and `FetchResponseData.read` in Java
/// to verify that Apache successfully decodes partitionline wire output.
#[test]
fn rust_fetch_output_decodes_with_apache_when_java_available() {
    let Some((java_bin, cp)) = java_conformance_classpath() else {
        println!("java toolchain / conformance jars not available; skipping live Apache decode of Rust output");
        return;
    };

    let versions: [(i16, i32, i32); 9] = [
        (4, 500, 25),
        (5, 1000, 35),
        (7, 5000, 45),
        (11, 2500, 55),
        (12, 1500, 70),
        (13, 2000, 80),
        (15, 1000, 90),
        (16, 3000, 110),
        (17, 1200, 120),
    ];

    for (version, max_wait_ms, throttle_ms) in versions {
        let topic_id = [0x22u8; 16];
        let topic_name = if version >= 13 { "" } else { "rust-fetch-topic" };
        let topic_id_bytes = if version >= 13 { topic_id } else { [0u8; 16] };

        // 1. Rust encodes FetchRequest
        let mut req_buf = BytesMut::new();
        let topic = FetchTopic {
            topic: topic_name.into(),
            topic_id: topic_id_bytes,
            partitions: vec![FetchPartition::partition_data(
                0, 100, 50, 1048576, Some(5), Some(4)
            )],
        };
        if version >= 15 {
            encode_fetch_request_with_replica_state(
                &mut req_buf, version, max_wait_ms, 1, 10485760, 0, &[topic], Some("rack-1"), 5, 12345
            ).expect("encode fetch request in Rust");
        } else if version >= 12 {
            encode_fetch_request_with_cluster_id(
                &mut req_buf, version, max_wait_ms, 1, 10485760, 0, &[topic], Some("rack-1"), -1, -1, Some("cluster-rust")
            ).expect("encode fetch request in Rust");
        } else if version >= 7 {
            encode_fetch_request_with_session(
                &mut req_buf, version, max_wait_ms, 1, 10485760, 1, &[topic], None, FetchMetadata::new(42, 1)
            ).expect("encode fetch request in Rust");
        } else if version >= 5 {
            encode_fetch_request_with_replica_id(
                &mut req_buf, version, max_wait_ms, 1, 10485760, 0, &[topic], None, 2
            ).expect("encode fetch request in Rust");
        } else {
            encode_fetch_request(
                &mut req_buf, version, max_wait_ms, 1, 10485760, 0, &[topic], None
            ).expect("encode fetch request in Rust");
        }
        let req_hex: String = req_buf.iter().map(|b| format!("{b:02x}")).collect();

        let req_out = std::process::Command::new(&java_bin)
            .args([
                "-cp",
                &cp,
                "org.apache.kafka.conformance.FixtureGenerator",
                "--decode-rust",
                "fetch-req",
                &version.to_string(),
                &req_hex,
            ])
            .output()
            .expect("execute java decode-rust fetch-req");
        assert!(
            req_out.status.success(),
            "Apache Java failed to decode Rust FetchRequest v{version}: {}",
            String::from_utf8_lossy(&req_out.stderr)
        );
        let req_stdout = String::from_utf8_lossy(&req_out.stdout);
        assert!(
            req_stdout.contains(&format!("OK: req v{version}")),
            "Apache output confirmation: {req_stdout}"
        );

        // 2. Rust encodes FetchResponse
        let mut resp_buf = BytesMut::new();
        let mut part = FetchedPartition::partition_response(0, 0);
        part.high_watermark = 200;
        part.last_stable_offset = 190;
        part.log_start_offset = 50;
        if version >= 11 {
            part.preferred_read_replica = 2;
        }
        if version >= 12 {
            part.current_leader_id = 1;
            part.current_leader_epoch = 10;
        }
        let topic_resp = FetchedTopic {
            topic: topic_name.into(),
            topic_id: topic_id_bytes,
            partitions: vec![part],
        };
        if version >= 16 {
            let ep = NodeEndpoint {
                node_id: 1,
                host: "broker1.kafka.local".into(),
                port: 9092,
                rack: Some("rack-a".into()),
            };
            encode_fetch_response_with_endpoints(
                &mut resp_buf, version, &[topic_resp], 0, 42, &[ep]
            ).expect("encode fetch response in Rust");
        } else if version >= 7 {
            encode_fetch_response_with_endpoints(
                &mut resp_buf, version, &[topic_resp], 0, 42, &[]
            ).expect("encode fetch response in Rust");
        } else {
            encode_fetch_response_with_throttle(
                &mut resp_buf, version, &[topic_resp], throttle_ms
            ).expect("encode fetch response in Rust");
        }
        let resp_hex: String = resp_buf.iter().map(|b| format!("{b:02x}")).collect();

        let resp_out = std::process::Command::new(&java_bin)
            .args([
                "-cp",
                &cp,
                "org.apache.kafka.conformance.FixtureGenerator",
                "--decode-rust",
                "fetch-resp",
                &version.to_string(),
                &resp_hex,
            ])
            .output()
            .expect("execute java decode-rust fetch-resp");
        assert!(
            resp_out.status.success(),
            "Apache Java failed to decode Rust FetchResponse v{version}: {}",
            String::from_utf8_lossy(&resp_out.stderr)
        );
        let resp_stdout = String::from_utf8_lossy(&resp_out.stdout);
        assert!(
            resp_stdout.contains(&format!("OK: resp v{version}")),
            "Apache output confirmation: {resp_stdout}"
        );
    }
}


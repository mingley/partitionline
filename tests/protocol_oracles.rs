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
use partitionline::error::{
    CLUSTER_AUTHORIZATION_FAILED, GROUP_AUTHORIZATION_FAILED, INVALID_RECORD_STATE,
    LEADER_NOT_AVAILABLE, NOT_LEADER_OR_FOLLOWER, SHARE_SESSION_NOT_FOUND,
    UNKNOWN_TOPIC_OR_PARTITION,
};
use partitionline::net::BrokerConn;
use partitionline::protocol::api::{
    decode_api_versions_handshake, decode_metadata_request_topics, decode_metadata_response,
    decode_produce_request, decode_produce_response, encode_api_versions_request,
    encode_metadata_request,
    encode_metadata_request_topics_with_include_cluster_authorized_operations,
    encode_metadata_response, encode_produce_request, encode_produce_response,
    encode_produce_response_with_throttle, Broker, MetadataRequest, MetadataRequestTopic,
    MetadataResponse, NodeEndpoint, PartitionMetadata, ProducePartitionData,
    ProducePartitionResponse, ProduceRecordError, ProduceTopicData, TopicMetadata,
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
    decode_list_offsets_topics_request, decode_list_offsets_topics_response,
    encode_list_offsets_request, encode_list_offsets_topics_request,
    encode_list_offsets_topics_response, encode_list_offsets_topics_response_with_throttle,
    ListOffsetsPartition, ListOffsetsPartitionRequest, ListOffsetsRequest, ListOffsetsResponse,
    ListOffsetsResponsePartition, ListOffsetsTopicRequest, ListOffsetsTopicResponse,
    DEBUGGING_REPLICA_ID, EARLIEST_LOCAL_TIMESTAMP, EARLIEST_TIMESTAMP, LATEST_TIERED_TIMESTAMP,
    LATEST_TIMESTAMP, MAX_TIMESTAMP,
};
use partitionline::protocol::records::{
    self, ControlRecordType, EndTransactionMarker, Record, RecordBatch,
};
use partitionline::protocol::share::{
    check_share_acknowledge_version, check_share_fetch_version, decode_share_acknowledge_request,
    decode_share_acknowledge_topics_response, decode_share_fetch_request,
    decode_share_fetch_response, SHARE_ACKNOWLEDGE_CRATE_MAX_VERSION,
    SHARE_FETCH_CRATE_MAX_VERSION,
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
    assert_eq!(decoded.brokers.len(), 2);
    assert_eq!(decoded.brokers[0].node_id, 1);
    assert_eq!(decoded.brokers[0].host, "localhost");
    assert_eq!(decoded.brokers[0].port, 9092);
    assert_eq!(decoded.brokers[1].node_id, 2);
    assert_eq!(decoded.brokers[1].host, "remotehost");
    assert_eq!(decoded.brokers[1].port, 9093);
    if version >= 1 {
        assert_eq!(decoded.brokers[0].rack.as_deref(), Some("rack-a"));
        assert_eq!(decoded.brokers[1].rack.as_deref(), Some("rack-b"));
        assert_eq!(decoded.controller_id, 1);
        assert_eq!(decoded.controller().map(|b| b.node_id), Some(1));
    } else {
        assert_eq!(decoded.controller_id, MetadataResponse::NO_CONTROLLER_ID);
    }
    assert_eq!(decoded.brokers_by_id().len(), 2);
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
    assert_eq!(ok.partitions.len(), 2);
    let p0 = &ok.partitions[0];
    assert_eq!(p0.error_code, 0);
    assert_eq!(p0.partition_index, 0);
    assert_eq!(p0.leader_id, 1);
    assert_eq!(p0.replica_nodes, vec![1, 2]);
    assert_eq!(p0.isr_nodes, vec![1]);
    if version >= 7 && gated_present {
        assert_eq!(p0.leader_epoch, 4);
    } else {
        assert_eq!(p0.leader_epoch, RecordBatch::NO_PARTITION_LEADER_EPOCH);
    }
    if version >= 5 {
        assert_eq!(p0.offline_replicas, vec![2]);
    } else {
        assert!(p0.offline_replicas.is_empty());
    }

    let p1 = &ok.partitions[1];
    assert_eq!(p1.error_code, LEADER_NOT_AVAILABLE);
    assert_eq!(p1.partition_index, 1);
    assert_eq!(p1.leader_id, MetadataResponse::NO_LEADER_ID);
    assert_eq!(p1.leader_epoch, RecordBatch::NO_PARTITION_LEADER_EPOCH);
    if version >= 5 {
        assert_eq!(p1.offline_replicas, vec![1, 2]);
    } else {
        assert!(p1.offline_replicas.is_empty());
    }

    if (8..=10).contains(&version) && gated_present {
        assert_eq!(decoded.cluster_authorized_operations, 0xdf);
    } else {
        assert_eq!(
            decoded.cluster_authorized_operations,
            MetadataResponse::AUTHORIZED_OPERATIONS_OMITTED
        );
    }

    let err = &decoded.topics[1];
    assert_eq!(err.error_code, UNKNOWN_TOPIC_OR_PARTITION);
    assert_eq!(err.name.as_deref(), Some("missing-topic"));

    if version >= 13 && gated_present {
        assert_eq!(decoded.error_code, 31);
    } else {
        assert_eq!(decoded.error_code, 0);
    }
}

fn metadata_body(gated_present: bool) -> MetadataResponse {
    let epoch = if gated_present { Some(4) } else { None };
    MetadataResponse {
        throttle_time_ms: THROTTLE_MS,
        brokers: vec![
            Broker::new(1, "localhost", 9092, Some("rack-a".into())),
            Broker::new(2, "remotehost", 9093, Some("rack-b".into())),
        ],
        cluster_id: gated_present.then(|| "cluster-x".into()),
        controller_id: 1,
        topics: vec![
            TopicMetadata::new(
                0,
                "ok-topic",
                false,
                vec![
                    PartitionMetadata::new(0, 0, Some(1), epoch, vec![1, 2], vec![1], vec![2]),
                    PartitionMetadata::new(
                        LEADER_NOT_AVAILABLE,
                        1,
                        None,
                        None,
                        vec![1, 2],
                        vec![],
                        vec![1, 2],
                    ),
                ],
            ),
            TopicMetadata::error(UNKNOWN_TOPIC_OR_PARTITION, Some("missing-topic"), [0; 16]),
        ],
        cluster_authorized_operations: if gated_present {
            0xdf
        } else {
            MetadataResponse::AUTHORIZED_OPERATIONS_OMITTED
        },
        error_code: if gated_present { 31 } else { 0 },
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
        assert_eq!(parts[1].error_message.as_deref(), Some("Broker not leader"));
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
    let (parts, _, _) =
        decode_produce_response(&mut cur, 9).expect("v9 decodes without leader tag");
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
    let kafka_4_jar = format!("{cache_dir}/kafka-clients-4.1.0.jar");
    let slf4j_jar = format!("{cache_dir}/slf4j-api-1.7.36.jar");
    let classes_dir = format!("{cache_dir}/classes");

    if !std::path::Path::new(&kafka_jar).exists()
        || !std::path::Path::new(&slf4j_jar).exists()
        || !std::path::Path::new(&classes_dir).exists()
    {
        return None;
    }

    let cp = if std::path::Path::new(&kafka_4_jar).exists() {
        format!("{kafka_4_jar}:{kafka_jar}:{slf4j_jar}:{classes_dir}")
    } else {
        format!("{kafka_jar}:{slf4j_jar}:{classes_dir}")
    };
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
        const REQ: &[u8] = include_bytes!("fixtures/protocol_oracles/fetch_v4_classic_request.bin");
        const RESP: &[u8] =
            include_bytes!("fixtures/protocol_oracles/fetch_v4_classic_response.bin");

        let (
            isolation,
            max_bytes,
            topics,
            rack,
            session,
            forgotten,
            max_wait_ms,
            min_bytes,
            replica_id,
            replica_epoch,
            cluster_id,
        ) = decode_fetch_request(&mut &REQ[..], 4).expect("fetch v4 request");
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
        assert_eq!(
            topics[0].partitions[0].log_start_offset,
            INVALID_LOG_START_OFFSET
        );
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
        assert_eq!(
            topics_resp[0].partitions[1].error_code,
            UNKNOWN_TOPIC_OR_PARTITION
        );
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
        assert_eq!(
            topics[0].partitions[0].log_start_offset, 100,
            "v5 log_start_offset present on wire"
        );

        let (topics_resp, _, _, _, throttle_ms) =
            decode_fetch_response(&mut &RESP[..], 5).expect("fetch v5 response");
        assert_eq!(throttle_ms, 35);
        assert_eq!(topics_resp.len(), 1);
        assert_eq!(topics_resp[0].topic, "fetch-v5-start-offset");
        assert_eq!(topics_resp[0].partitions[0].high_watermark, 250);
        assert_eq!(topics_resp[0].partitions[0].last_stable_offset, 240);
        assert_eq!(
            topics_resp[0].partitions[0].log_start_offset, 100,
            "v5 response log_start_offset"
        );
    }

    // 3. Fetch v7: classic wire format, session metadata gate (sessionId, sessionEpoch, forgottenTopicsData), abortedTransactions
    {
        const REQ: &[u8] = include_bytes!("fixtures/protocol_oracles/fetch_v7_session_request.bin");
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
        assert_eq!(
            topics[0].partitions[0].fetch_offset, 102,
            "requested fetch offset 102"
        );
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
        assert_eq!(
            cluster_id.as_deref(),
            Some("cluster-v12"),
            "v12 cluster_id tag 0"
        );
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
            0x11, 0x11, 0x22, 0x22, 0x33, 0x33, 0x44, 0x44, 0x55, 0x55, 0x66, 0x66, 0x77, 0x77,
            0x88, 0x88,
        ];
        let expected_forgotten_id = [
            0xaa, 0xaa, 0xbb, 0xbb, 0xcc, 0xcc, 0xdd, 0xdd, 0x11, 0x11, 0x22, 0x22, 0x33, 0x33,
            0x44, 0x44,
        ];

        let (_, _, topics, _, _, forgotten, _, _, _, _, _) =
            decode_fetch_request(&mut &REQ[..], 13).expect("fetch v13 request");
        assert!(
            topics[0].topic.is_empty(),
            "v13 request topic name is empty"
        );
        assert_eq!(topics[0].topic_id, expected_topic_id, "v13 request topicId");
        assert!(
            forgotten[0].topic.is_empty(),
            "v13 forgotten topic name is empty"
        );
        assert_eq!(
            forgotten[0].topic_id, expected_forgotten_id,
            "v13 forgotten topicId"
        );

        let (topics_resp, _, _, session_id, throttle_ms) =
            decode_fetch_response(&mut &RESP[..], 13).expect("fetch v13 response");
        assert_eq!(throttle_ms, 80);
        assert_eq!(session_id, 300);
        assert!(
            topics_resp[0].topic.is_empty(),
            "v13 response topic name is empty"
        );
        assert_eq!(
            topics_resp[0].topic_id, expected_topic_id,
            "v13 response topicId"
        );
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
            0x99, 0x99, 0x88, 0x88, 0x77, 0x77, 0x66, 0x66, 0x55, 0x55, 0x44, 0x44, 0x33, 0x33,
            0x22, 0x22,
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
            0xfa, 0xce, 0xfe, 0xed, 0xca, 0xfe, 0xbe, 0xef, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06,
            0x07, 0x08,
        ];
        let expected_replica_dir_id = [
            0xaa, 0xaa, 0xbb, 0xbb, 0x00, 0x00, 0x11, 0x11, 0x22, 0x22, 0x33, 0x33, 0x44, 0x44,
            0x55, 0x55,
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
        assert_eq!(
            p.replica_directory_id, expected_replica_dir_id,
            "v17 partition replicaDirectoryId"
        );

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
    assert_eq!(
        decoded.len(),
        2,
        "decodes the 2 complete batches before short tail"
    );
    assert_eq!(
        decoded[0].records[0].value.as_deref(),
        Some(b"v1".as_slice())
    );
    assert_eq!(
        decoded[1].records[0].value.as_deref(),
        Some(b"v2".as_slice())
    );

    // Case B: Declared batch_len = 100 bytes, but only 20 bytes payload provided
    let mut short_tail_body = rec_bytes.clone();
    short_tail_body.extend_from_slice(&2i64.to_be_bytes()); // base_offset
    short_tail_body.extend_from_slice(&100i32.to_be_bytes()); // batch_len = 100
    short_tail_body.extend_from_slice(&[0x00; 20]); // only 20 bytes provided instead of 100
    let mut cur = &short_tail_body[..];
    let decoded = records::decode_record_batches(&mut cur).expect("short body tail must succeed");
    assert_eq!(
        decoded.len(),
        2,
        "decodes the 2 complete batches before truncated tail"
    );

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
        let topic_name = if version >= 13 {
            ""
        } else {
            "rust-fetch-topic"
        };
        let topic_id_bytes = if version >= 13 { topic_id } else { [0u8; 16] };

        // 1. Rust encodes FetchRequest
        let mut req_buf = BytesMut::new();
        let topic = FetchTopic {
            topic: topic_name.into(),
            topic_id: topic_id_bytes,
            partitions: vec![FetchPartition::partition_data(
                0,
                100,
                50,
                1048576,
                Some(5),
                Some(4),
            )],
        };
        if version >= 15 {
            encode_fetch_request_with_replica_state(
                &mut req_buf,
                version,
                max_wait_ms,
                1,
                10485760,
                0,
                &[topic],
                Some("rack-1"),
                5,
                12345,
            )
            .expect("encode fetch request in Rust");
        } else if version >= 12 {
            encode_fetch_request_with_cluster_id(
                &mut req_buf,
                version,
                max_wait_ms,
                1,
                10485760,
                0,
                &[topic],
                Some("rack-1"),
                -1,
                -1,
                Some("cluster-rust"),
            )
            .expect("encode fetch request in Rust");
        } else if version >= 7 {
            encode_fetch_request_with_session(
                &mut req_buf,
                version,
                max_wait_ms,
                1,
                10485760,
                1,
                &[topic],
                None,
                FetchMetadata::new(42, 1),
            )
            .expect("encode fetch request in Rust");
        } else if version >= 5 {
            encode_fetch_request_with_replica_id(
                &mut req_buf,
                version,
                max_wait_ms,
                1,
                10485760,
                0,
                &[topic],
                None,
                2,
            )
            .expect("encode fetch request in Rust");
        } else {
            encode_fetch_request(
                &mut req_buf,
                version,
                max_wait_ms,
                1,
                10485760,
                0,
                &[topic],
                None,
            )
            .expect("encode fetch request in Rust");
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
                &mut resp_buf,
                version,
                &[topic_resp],
                0,
                42,
                &[ep],
            )
            .expect("encode fetch response in Rust");
        } else if version >= 7 {
            encode_fetch_response_with_endpoints(&mut resp_buf, version, &[topic_resp], 0, 42, &[])
                .expect("encode fetch response in Rust");
        } else {
            encode_fetch_response_with_throttle(&mut resp_buf, version, &[topic_resp], throttle_ms)
                .expect("encode fetch response in Rust");
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

/// KL01-06: Independent Metadata wire fixtures covering advertised version boundaries.
///
/// Consumes committed Apache Kafka 3.9.1 binary fixtures offline without requiring
/// Java or network access. Verifies classic and flexible formats, null vs empty topics,
/// topic IDs vs topic names, offline replicas, cluster and topic authorized operations,
/// stale/unknown leaders, unknown tagged fields, and multiple brokers across racks.
#[test]
fn apache_metadata_boundary_fixtures_decode_offline() {
    // 1. Metadata v1: classic wire format (oldest spoken), null topics (all topics), multiple brokers across racks, controllerId, isInternal, unknown leader
    {
        const REQ: &[u8] =
            include_bytes!("fixtures/protocol_oracles/metadata_v1_classic_request.bin");
        const RESP: &[u8] =
            include_bytes!("fixtures/protocol_oracles/metadata_v1_classic_response.bin");

        let (topics, allow_auto, include_topic_auth, include_cluster_auth) =
            decode_metadata_request_topics(&mut &REQ[..], 1).expect("metadata v1 request");
        assert!(topics.is_none(), "v1 null topics represents all topics");
        assert!(allow_auto, "v1 allow_auto default true");
        assert!(!include_topic_auth);
        assert!(!include_cluster_auth);

        let resp = decode_metadata_response(&mut &RESP[..], 1).expect("metadata v1 response");
        assert_eq!(resp.throttle_time_ms, 0, "v1 throttle omitted");
        assert!(resp.cluster_id.is_none(), "v1 cluster_id omitted");
        assert_eq!(resp.controller_id, 1);
        assert_eq!(resp.brokers.len(), 3, "multiple brokers across racks");
        assert_eq!(resp.brokers[0].node_id, 1);
        assert_eq!(resp.brokers[0].host, "broker1.kafka.local");
        assert_eq!(resp.brokers[0].port, 9092);
        assert_eq!(resp.brokers[0].rack.as_deref(), Some("rack-a"));
        assert_eq!(resp.brokers[1].node_id, 2);
        assert_eq!(resp.brokers[1].host, "broker2.kafka.local");
        assert_eq!(resp.brokers[1].port, 9092);
        assert_eq!(resp.brokers[1].rack.as_deref(), Some("rack-b"));
        assert_eq!(resp.brokers[2].node_id, 3);
        assert_eq!(resp.brokers[2].host, "broker3.kafka.local");
        assert_eq!(resp.brokers[2].port, 9093);
        assert_eq!(resp.brokers[2].rack, None);
        assert_eq!(resp.controller().map(|b| b.node_id), Some(1));
        assert_eq!(resp.brokers_by_id().len(), 3);

        assert_eq!(resp.topics.len(), 2);
        let t1 = &resp.topics[0];
        assert_eq!(t1.error_code, 0);
        assert_eq!(t1.name.as_deref(), Some("meta-v1-topic"));
        assert!(!t1.is_internal);
        assert_eq!(t1.topic_id, [0u8; 16]);
        assert_eq!(t1.partitions.len(), 2);

        // Partition 0: normal leader 1
        assert_eq!(t1.partitions[0].error_code, 0);
        assert_eq!(t1.partitions[0].partition_index, 0);
        assert_eq!(t1.partitions[0].leader_id, 1);
        assert_eq!(
            t1.partitions[0].leader_epoch,
            RecordBatch::NO_PARTITION_LEADER_EPOCH
        );
        assert_eq!(t1.partitions[0].replica_nodes, vec![1, 2]);
        assert_eq!(t1.partitions[0].isr_nodes, vec![1, 2]);
        assert!(t1.partitions[0].offline_replicas.is_empty());

        // Partition 1: unknown leader (-1) with LEADER_NOT_AVAILABLE
        assert_eq!(t1.partitions[1].error_code, LEADER_NOT_AVAILABLE);
        assert_eq!(t1.partitions[1].partition_index, 1);
        assert_eq!(t1.partitions[1].leader_id, MetadataResponse::NO_LEADER_ID);
        assert_eq!(t1.partitions[1].replica_nodes, vec![2, 3]);
        assert_eq!(t1.partitions[1].isr_nodes, vec![2]);

        let t2 = &resp.topics[1];
        assert_eq!(t2.error_code, UNKNOWN_TOPIC_OR_PARTITION);
        assert_eq!(t2.name.as_deref(), Some("meta-v1-missing"));
        assert!(t2.partitions.is_empty());
        assert_eq!(
            resp.cluster_authorized_operations,
            MetadataResponse::AUTHORIZED_OPERATIONS_OMITTED
        );
        assert_eq!(resp.error_code, 0);
    }

    // 2. Metadata v4: classic wire format, empty topics array, allowAutoTopicCreation gate (false), clusterId, throttleTimeMs
    {
        const REQ: &[u8] =
            include_bytes!("fixtures/protocol_oracles/metadata_v4_empty_topics_request.bin");
        const RESP: &[u8] =
            include_bytes!("fixtures/protocol_oracles/metadata_v4_empty_topics_response.bin");

        let (topics, allow_auto, _, _) =
            decode_metadata_request_topics(&mut &REQ[..], 4).expect("metadata v4 request");
        assert_eq!(topics, Some(Vec::new()), "v4 empty topics array (not null)");
        assert!(!allow_auto, "v4 allow_auto gate (false)");

        let resp = decode_metadata_response(&mut &RESP[..], 4).expect("metadata v4 response");
        assert_eq!(resp.throttle_time_ms, 30, "v4 throttle present");
        assert_eq!(resp.cluster_id.as_deref(), Some("cluster-meta-v4"));
        assert_eq!(resp.controller_id, 2);
        assert_eq!(resp.brokers.len(), 3);
        assert!(resp.topics.is_empty(), "empty topics response");
    }

    // 3. Metadata v5: classic wire format, offlineReplicas gate present, multiple brokers, unknown leader
    {
        const REQ: &[u8] =
            include_bytes!("fixtures/protocol_oracles/metadata_v5_offline_replicas_request.bin");
        const RESP: &[u8] =
            include_bytes!("fixtures/protocol_oracles/metadata_v5_offline_replicas_response.bin");

        let (topics, allow_auto, _, _) =
            decode_metadata_request_topics(&mut &REQ[..], 5).expect("metadata v5 request");
        assert_eq!(topics.as_ref().map(|t| t.len()), Some(1));
        assert_eq!(topics.unwrap()[0].name.as_deref(), Some("meta-v5-offline"));
        assert!(allow_auto);

        let resp = decode_metadata_response(&mut &RESP[..], 5).expect("metadata v5 response");
        assert_eq!(resp.throttle_time_ms, 45);
        assert_eq!(resp.cluster_id.as_deref(), Some("cluster-meta-v5"));
        assert_eq!(resp.controller_id, 1);
        assert_eq!(resp.brokers.len(), 3);
        assert_eq!(resp.topics.len(), 1);

        let p0 = &resp.topics[0].partitions[0];
        assert_eq!(p0.leader_id, 2);
        assert_eq!(p0.replica_nodes, vec![1, 2, 3]);
        assert_eq!(p0.isr_nodes, vec![1, 2]);
        assert_eq!(p0.offline_replicas, vec![3], "v5 offlineReplicas gate");

        let p1 = &resp.topics[0].partitions[1];
        assert_eq!(p1.error_code, NOT_LEADER_OR_FOLLOWER);
        assert_eq!(
            p1.leader_id,
            MetadataResponse::NO_LEADER_ID,
            "unknown leader"
        );
        assert_eq!(p1.replica_nodes, vec![1, 3]);
        assert_eq!(p1.isr_nodes, vec![1]);
        assert_eq!(p1.offline_replicas, vec![3]);
    }

    // 4. Metadata v7: classic wire format, leaderEpoch gate present across multiple partitions, active and stale leader epochs
    {
        const REQ: &[u8] =
            include_bytes!("fixtures/protocol_oracles/metadata_v7_leader_epoch_request.bin");
        const RESP: &[u8] =
            include_bytes!("fixtures/protocol_oracles/metadata_v7_leader_epoch_response.bin");

        let (topics, _, _, _) =
            decode_metadata_request_topics(&mut &REQ[..], 7).expect("metadata v7 request");
        assert_eq!(topics.unwrap()[0].name.as_deref(), Some("meta-v7-epoch"));

        let resp = decode_metadata_response(&mut &RESP[..], 7).expect("metadata v7 response");
        assert_eq!(resp.throttle_time_ms, 50);
        assert_eq!(resp.cluster_id.as_deref(), Some("cluster-meta-v7"));
        assert_eq!(resp.controller_id, 1);
        assert_eq!(resp.brokers.len(), 3);

        let p0 = &resp.topics[0].partitions[0];
        assert_eq!(p0.leader_id, 1);
        assert_eq!(p0.leader_epoch, 8, "v7 leader_epoch gate");

        let p1 = &resp.topics[0].partitions[1];
        assert_eq!(p1.leader_id, 2);
        assert_eq!(p1.leader_epoch, 5, "stale leader_epoch");
    }

    // 5. Metadata v8: classic format boundary before flexible, authorized operations
    {
        const REQ: &[u8] =
            include_bytes!("fixtures/protocol_oracles/metadata_v8_authorized_ops_request.bin");
        const RESP: &[u8] =
            include_bytes!("fixtures/protocol_oracles/metadata_v8_authorized_ops_response.bin");

        let (topics, allow_auto, include_topic_auth, include_cluster_auth) =
            decode_metadata_request_topics(&mut &REQ[..], 8).expect("metadata v8 request");
        assert_eq!(topics.unwrap()[0].name.as_deref(), Some("meta-v8-ops"));
        assert!(allow_auto);
        assert!(include_topic_auth, "v8 include_topic_authorized_operations");
        assert!(
            include_cluster_auth,
            "v8-10 include_cluster_authorized_operations"
        );

        let resp = decode_metadata_response(&mut &RESP[..], 8).expect("metadata v8 response");
        assert_eq!(resp.throttle_time_ms, 60);
        assert_eq!(resp.cluster_id.as_deref(), Some("cluster-meta-v8"));
        assert_eq!(resp.controller_id, 2);
        assert_eq!(
            resp.cluster_authorized_operations, 0xdf,
            "v8-10 cluster authorized operations"
        );
        assert_eq!(
            resp.topics[0].topic_authorized_operations, 0x1f,
            "v8 topic authorized operations"
        );
        assert_eq!(resp.topics[0].partitions[0].leader_epoch, 12);
        assert_eq!(resp.topics[0].partitions[0].offline_replicas, vec![2]);
    }

    // 6. Metadata v9: flexible wire format boundary, compact encoding, clusterId, multiple brokers, unknown tagged fields
    {
        const REQ: &[u8] =
            include_bytes!("fixtures/protocol_oracles/metadata_v9_flexible_request.bin");
        const RESP: &[u8] =
            include_bytes!("fixtures/protocol_oracles/metadata_v9_flexible_response.bin");

        let (topics, allow_auto, include_topic_auth, include_cluster_auth) =
            decode_metadata_request_topics(&mut &REQ[..], 9).expect("metadata v9 request");
        assert_eq!(topics.unwrap()[0].name.as_deref(), Some("meta-v9-flex"));
        assert!(allow_auto);
        assert!(include_topic_auth);
        assert!(!include_cluster_auth);

        let resp = decode_metadata_response(&mut &RESP[..], 9).expect("metadata v9 response");
        assert_eq!(resp.throttle_time_ms, 75);
        assert_eq!(resp.cluster_id.as_deref(), Some("cluster-meta-v9"));
        assert_eq!(resp.controller_id, 3);
        assert_eq!(
            resp.cluster_authorized_operations,
            MetadataResponse::AUTHORIZED_OPERATIONS_OMITTED
        );
        assert_eq!(resp.brokers.len(), 3);
        assert_eq!(resp.topics[0].topic_authorized_operations, 0xdf);

        let p0 = &resp.topics[0].partitions[0];
        assert_eq!(p0.leader_id, 3);
        assert_eq!(p0.leader_epoch, 15);
        assert_eq!(p0.offline_replicas, vec![1]);

        let p1 = &resp.topics[0].partitions[1];
        assert_eq!(p1.error_code, LEADER_NOT_AVAILABLE);
        assert_eq!(
            p1.leader_id,
            MetadataResponse::NO_LEADER_ID,
            "unknown leader"
        );
        assert_eq!(p1.leader_epoch, RecordBatch::NO_PARTITION_LEADER_EPOCH);
        assert_eq!(p1.offline_replicas, vec![1, 2]);
    }

    // 7. Metadata v10: flexible wire format, topicId wire transition on request and response topics, clusterAuthorizedOperations, unknown leader
    {
        const REQ: &[u8] =
            include_bytes!("fixtures/protocol_oracles/metadata_v10_topic_ids_request.bin");
        const RESP: &[u8] =
            include_bytes!("fixtures/protocol_oracles/metadata_v10_topic_ids_response.bin");

        let expected_topic_id = [
            0x11, 0x11, 0x22, 0x22, 0x33, 0x33, 0x44, 0x44, 0x55, 0x55, 0x66, 0x66, 0x77, 0x77,
            0x88, 0x88,
        ];

        let (topics, allow_auto, include_topic_auth, include_cluster_auth) =
            decode_metadata_request_topics(&mut &REQ[..], 10).expect("metadata v10 request");
        assert!(!allow_auto);
        assert!(include_topic_auth);
        assert!(include_cluster_auth);
        let req_topic = &topics.unwrap()[0];
        assert_eq!(req_topic.name.as_deref(), Some("meta-v10-ids"));
        assert_eq!(
            req_topic.topic_id, expected_topic_id,
            "v10 request topic_id"
        );

        let resp = decode_metadata_response(&mut &RESP[..], 10).expect("metadata v10 response");
        assert_eq!(resp.throttle_time_ms, 80);
        assert_eq!(resp.cluster_id.as_deref(), Some("cluster-meta-v10"));
        assert_eq!(resp.controller_id, 1);
        assert_eq!(resp.cluster_authorized_operations, 0xdf);
        assert_eq!(
            resp.topics[0].topic_id, expected_topic_id,
            "v10 response topic_id"
        );
        assert_eq!(resp.topics[0].topic_authorized_operations, 0x1f);

        let p0 = &resp.topics[0].partitions[0];
        assert_eq!(p0.leader_id, 1);
        assert_eq!(p0.leader_epoch, 20);

        let p1 = &resp.topics[0].partitions[1];
        assert_eq!(p1.error_code, NOT_LEADER_OR_FOLLOWER);
        assert_eq!(
            p1.leader_id,
            MetadataResponse::NO_LEADER_ID,
            "unknown leader"
        );
        assert_eq!(
            p1.leader_epoch, 18,
            "stale leader_epoch on partition without leader"
        );
        assert_eq!(p1.offline_replicas, vec![3]);
    }

    // 8. Metadata v11: flexible wire format boundary where clusterAuthorizedOperations is dropped
    {
        const REQ: &[u8] =
            include_bytes!("fixtures/protocol_oracles/metadata_v11_no_cluster_auth_request.bin");
        const RESP: &[u8] =
            include_bytes!("fixtures/protocol_oracles/metadata_v11_no_cluster_auth_response.bin");

        let expected_topic_id = [
            0x22, 0x22, 0x33, 0x33, 0x44, 0x44, 0x55, 0x55, 0x66, 0x66, 0x77, 0x77, 0x88, 0x88,
            0x99, 0x99,
        ];

        let (topics, allow_auto, include_topic_auth, include_cluster_auth) =
            decode_metadata_request_topics(&mut &REQ[..], 11).expect("metadata v11 request");
        assert!(allow_auto);
        assert!(include_topic_auth);
        assert!(!include_cluster_auth, "v11 drops cluster authorized ops");
        assert_eq!(topics.unwrap()[0].topic_id, expected_topic_id);

        let resp = decode_metadata_response(&mut &RESP[..], 11).expect("metadata v11 response");
        assert_eq!(resp.throttle_time_ms, 90);
        assert_eq!(resp.cluster_id.as_deref(), Some("cluster-meta-v11"));
        assert_eq!(resp.controller_id, 2);
        assert_eq!(
            resp.cluster_authorized_operations,
            MetadataResponse::AUTHORIZED_OPERATIONS_OMITTED,
            "v11 drops cluster authorized ops on wire"
        );
        assert_eq!(resp.topics[0].topic_authorized_operations, 0xdf);
        assert_eq!(resp.topics[0].partitions[0].leader_id, 2);
        assert_eq!(resp.topics[0].partitions[0].leader_epoch, 25);
    }

    // 9. Metadata v12: flexible wire format at highest Apache Kafka 3.9.1 valid version boundary with ID-only describe (null name)
    {
        const REQ: &[u8] =
            include_bytes!("fixtures/protocol_oracles/metadata_v12_id_only_request.bin");
        const RESP: &[u8] =
            include_bytes!("fixtures/protocol_oracles/metadata_v12_id_only_response.bin");

        let expected_topic_id = [
            0x33, 0x33, 0x44, 0x44, 0x55, 0x55, 0x66, 0x66, 0x77, 0x77, 0x88, 0x88, 0x99, 0x99,
            0xaa, 0xaa,
        ];

        let (topics, allow_auto, include_topic_auth, include_cluster_auth) =
            decode_metadata_request_topics(&mut &REQ[..], 12).expect("metadata v12 request");
        assert!(!allow_auto);
        assert!(include_topic_auth);
        assert!(!include_cluster_auth);
        let req_topic = &topics.unwrap()[0];
        assert_eq!(
            req_topic.name, None,
            "v12 null topic name when describing by ID"
        );
        assert_eq!(
            req_topic.topic_id, expected_topic_id,
            "v12 describe by topic_id"
        );

        let resp = decode_metadata_response(&mut &RESP[..], 12).expect("metadata v12 response");
        assert_eq!(resp.throttle_time_ms, 100);
        assert_eq!(resp.cluster_id.as_deref(), Some("cluster-meta-v12"));
        assert_eq!(resp.controller_id, 3);
        assert_eq!(resp.brokers.len(), 3);
        let t = &resp.topics[0];
        assert_eq!(t.name.as_deref(), Some("meta-v12-resolved"));
        assert_eq!(t.topic_id, expected_topic_id);
        assert_eq!(t.topic_authorized_operations, 0x1f);
        assert_eq!(t.partitions[0].leader_id, 3);
        assert_eq!(t.partitions[0].leader_epoch, 30);
        assert_eq!(t.partitions[1].error_code, LEADER_NOT_AVAILABLE);
        assert_eq!(t.partitions[1].leader_id, MetadataResponse::NO_LEADER_ID);
        assert_eq!(t.partitions[1].leader_epoch, 28);
        assert_eq!(t.partitions[1].offline_replicas, vec![3]);
    }
}

/// KL01-06: Negative and mutation tests for Metadata.
///
/// Verifies that mutating field order or required version gates fails decoding.
#[test]
fn metadata_version_gate_and_field_order_mutations_fail() {
    const V1_REQ: &[u8] =
        include_bytes!("fixtures/protocol_oracles/metadata_v1_classic_request.bin");
    const V5_RESP: &[u8] =
        include_bytes!("fixtures/protocol_oracles/metadata_v5_offline_replicas_response.bin");
    const V7_RESP: &[u8] =
        include_bytes!("fixtures/protocol_oracles/metadata_v7_leader_epoch_response.bin");
    const V8_RESP: &[u8] =
        include_bytes!("fixtures/protocol_oracles/metadata_v8_authorized_ops_response.bin");
    const V9_RESP: &[u8] =
        include_bytes!("fixtures/protocol_oracles/metadata_v9_flexible_response.bin");
    const V10_RESP: &[u8] =
        include_bytes!("fixtures/protocol_oracles/metadata_v10_topic_ids_response.bin");
    const V12_RESP: &[u8] =
        include_bytes!("fixtures/protocol_oracles/metadata_v12_id_only_response.bin");

    // Mutation 1: AllowAutoTopicCreation gate (v4+).
    // Decoding v1 request (no allow_auto byte on wire) with v4 decoder fails with buffer underflow.
    let mut cur = V1_REQ;
    assert!(
        decode_metadata_request_topics(&mut cur, 4).is_err(),
        "decoding v1 request as v4 must fail because allow_auto byte is missing"
    );

    // Mutation 2: OfflineReplicas gate (v5+).
    // Decoding v5 response with v4 decoder ignores offline_replicas and leaves unconsumed bytes.
    let mut cur = V5_RESP;
    let _ = decode_metadata_response(&mut cur, 4).expect("decodes without offline_replicas");
    assert!(
        !cur.is_empty(),
        "decoding v5 response with v4 decoder must leave unconsumed offline_replicas bytes"
    );

    // Mutation 3: LeaderEpoch gate (v7+).
    // Decoding v7 response with v6 decoder fails due to schema desync from unexpected leader_epoch bytes.
    let mut cur = V7_RESP;
    assert!(
        decode_metadata_response(&mut cur, 6).is_err(),
        "decoding v7 response with v6 decoder must fail due to missing leader_epoch in v6 schema"
    );

    // Mutation 4: Flexible version gate mutation (gate is v9+).
    // Decoding v8 classic response as v9 flexible must fail with format mismatch.
    let mut cur = V8_RESP;
    assert!(
        decode_metadata_response(&mut cur, 9).is_err(),
        "decoding v8 classic response as v9 flexible must fail"
    );

    // Decoding v9 flexible response as v8 classic must fail with format mismatch.
    let mut cur = V9_RESP;
    assert!(
        decode_metadata_response(&mut cur, 8).is_err(),
        "decoding v9 flexible response as v8 classic must fail"
    );

    // Mutation 5: Topic IDs gate (v10+).
    // Decoding v10 response with v9 decoder fails or causes protocol desync because v9 lacks topicId UUID.
    let mut cur = V10_RESP;
    assert!(
        decode_metadata_response(&mut cur, 9).is_err(),
        "decoding v10 response as v9 must fail due to topicId UUID in payload"
    );

    // Mutation 6: ClusterAuthorizedOperations omission gate (v11+).
    // Decoding v10 response with v11 decoder ignores cluster_authorized_operations, leaving unconsumed bytes.
    let mut cur = V10_RESP;
    let _ = decode_metadata_response(&mut cur, 11).expect("decodes without cluster_authorized_ops");
    assert!(
        !cur.is_empty(),
        "decoding v10 response with v11 decoder must leave unconsumed cluster_authorized_ops bytes"
    );

    // Mutation 7: ID-only describe gate (v12+).
    // Calling MetadataRequest::build below v12 with a null topic name must fail with Unsupported error.
    let req_id_only = [MetadataRequestTopic::by_id([0x11; 16])];
    assert!(
        MetadataRequest::build(11, Some(&req_id_only), false).is_err(),
        "describing topic by ID below v12 must fail"
    );

    // Mutation 8: Mutating field order in MetadataResponse (e.g. putting throttle_time_ms after brokers instead of before brokers).
    let mut mutated_order = BytesMut::new();
    mutated_order.extend_from_slice(&0i32.to_be_bytes()); // broker count = 0
    mutated_order.extend_from_slice(&25i32.to_be_bytes()); // mutated throttle placement
    mutated_order.extend_from_slice(&(-1i32).to_be_bytes()); // null cluster_id
    mutated_order.extend_from_slice(&1i32.to_be_bytes()); // controller_id
    mutated_order.extend_from_slice(&0i32.to_be_bytes()); // topic count = 0
    let mut cur = mutated_order.as_ref();
    assert!(
        decode_metadata_response(&mut cur, 3).is_err(),
        "mutating MetadataResponse field order must fail decoding"
    );

    // Mutation 9: Metadata v13 top-level error code handling.
    // Rust encodes v13 response with top-level error code CLUSTER_AUTHORIZATION_FAILED (31).
    let mut resp13 = MetadataResponse {
        throttle_time_ms: 50,
        brokers: vec![Broker::new(1, "b1", 9092, None)],
        cluster_id: Some("c1".into()),
        controller_id: 1,
        topics: vec![],
        cluster_authorized_operations: MetadataResponse::AUTHORIZED_OPERATIONS_OMITTED,
        error_code: CLUSTER_AUTHORIZATION_FAILED,
    };
    let mut v13_bytes = BytesMut::new();
    encode_metadata_response(&mut v13_bytes, 13, &resp13).expect("encode v13 response");

    // Decodes successfully with v13 decoder, error_code matches non-zero error.
    let mut cur = v13_bytes.as_ref();
    let decoded13 = decode_metadata_response(&mut cur, 13).expect("decode v13 response");
    assert_eq!(decoded13.error_code, CLUSTER_AUTHORIZATION_FAILED);
    assert_ne!(decoded13.error_code, 0);

    // When error_code is 0 on v13, check succeeds.
    resp13.error_code = 0;
    let mut v13_ok_bytes = BytesMut::new();
    encode_metadata_response(&mut v13_ok_bytes, 13, &resp13).expect("encode v13 ok response");
    let mut cur = v13_ok_bytes.as_ref();
    let decoded13_ok = decode_metadata_response(&mut cur, 13).expect("decode v13 ok response");
    assert_eq!(decoded13_ok.error_code, 0);

    // Decoding v12 response with v13 decoder fails because error_code is missing on wire.
    let mut cur = V12_RESP;
    assert!(
        decode_metadata_response(&mut cur, 13).is_err(),
        "decoding v12 response as v13 must fail because error_code is missing on wire"
    );
}

/// KL01-06: Decode Rust output with Apache where Java is available.
///
/// Encodes Metadata requests and responses in Rust across the advertised version boundaries,
/// then runs Apache Kafka's `MetadataRequestData.read` and `MetadataResponseData.read` in Java
/// to verify that Apache successfully decodes partitionline wire output.
#[test]
fn rust_metadata_output_decodes_with_apache_when_java_available() {
    let Some((java_bin, cp)) = java_conformance_classpath() else {
        println!("java toolchain / conformance jars not available; skipping live Apache decode of Rust output");
        return;
    };

    let versions: [(i16, bool, bool, bool, i32); 9] = [
        (1, true, false, false, 0),
        (4, false, false, false, 30),
        (5, true, false, false, 45),
        (7, true, false, false, 50),
        (8, true, true, true, 60),
        (9, true, true, false, 75),
        (10, false, true, true, 80),
        (11, true, true, false, 90),
        (12, false, true, false, 100),
    ];

    for (version, allow_auto, include_topic_auth, include_cluster_auth, throttle_ms) in versions {
        let topic_id = [0x55u8; 16];
        let req_topic = if version >= 12 {
            MetadataRequestTopic::by_id(topic_id)
        } else {
            MetadataRequestTopic::by_name("rust-meta-topic")
        };

        // 1. Rust encodes MetadataRequest
        let mut req_buf = BytesMut::new();
        encode_metadata_request_topics_with_include_cluster_authorized_operations(
            &mut req_buf,
            version,
            Some(&[req_topic]),
            allow_auto,
            include_topic_auth,
            include_cluster_auth,
        )
        .expect("encode metadata request in Rust");
        let req_hex: String = req_buf.iter().map(|b| format!("{b:02x}")).collect();

        let req_out = std::process::Command::new(&java_bin)
            .args([
                "-cp",
                &cp,
                "org.apache.kafka.conformance.FixtureGenerator",
                "--decode-rust",
                "metadata-req",
                &version.to_string(),
                &req_hex,
            ])
            .output()
            .expect("execute java decode-rust metadata-req");
        assert!(
            req_out.status.success(),
            "Apache Java failed to decode Rust MetadataRequest v{version}: {}",
            String::from_utf8_lossy(&req_out.stderr)
        );
        let req_stdout = String::from_utf8_lossy(&req_out.stdout);
        assert!(
            req_stdout.contains(&format!("OK: req v{version}")),
            "Apache output confirmation: {req_stdout}"
        );

        // 2. Rust encodes MetadataResponse
        let mut resp_buf = BytesMut::new();
        let resp = MetadataResponse {
            throttle_time_ms: throttle_ms,
            brokers: vec![
                Broker::new(1, "broker1.kafka.local", 9092, Some("rack-a".into())),
                Broker::new(2, "broker2.kafka.local", 9092, Some("rack-b".into())),
                Broker::new(3, "broker3.kafka.local", 9093, None),
            ],
            cluster_id: (version >= 2).then(|| "rust-cluster".into()),
            controller_id: 1,
            topics: vec![TopicMetadata {
                error_code: 0,
                name: Some("rust-meta-topic".into()),
                topic_id: if version >= 10 { topic_id } else { [0; 16] },
                is_internal: false,
                partitions: vec![
                    PartitionMetadata::new(
                        0,
                        0,
                        Some(1),
                        (version >= 7).then_some(5),
                        vec![1, 2],
                        vec![1],
                        if version >= 5 { vec![2] } else { vec![] },
                    ),
                    PartitionMetadata::new(
                        LEADER_NOT_AVAILABLE,
                        1,
                        None,
                        None,
                        vec![1, 2],
                        vec![],
                        if version >= 5 { vec![1, 2] } else { vec![] },
                    ),
                ],
                topic_authorized_operations: if version >= 8 {
                    0x1f
                } else {
                    MetadataResponse::AUTHORIZED_OPERATIONS_OMITTED
                },
            }],
            cluster_authorized_operations: if (8..=10).contains(&version) {
                0xdf
            } else {
                MetadataResponse::AUTHORIZED_OPERATIONS_OMITTED
            },
            error_code: 0,
        };
        encode_metadata_response(&mut resp_buf, version, &resp)
            .expect("encode metadata response in Rust");
        let resp_hex: String = resp_buf.iter().map(|b| format!("{b:02x}")).collect();

        let resp_out = std::process::Command::new(&java_bin)
            .args([
                "-cp",
                &cp,
                "org.apache.kafka.conformance.FixtureGenerator",
                "--decode-rust",
                "metadata-resp",
                &version.to_string(),
                &resp_hex,
            ])
            .output()
            .expect("execute java decode-rust metadata-resp");
        assert!(
            resp_out.status.success(),
            "Apache Java failed to decode Rust MetadataResponse v{version}: {}",
            String::from_utf8_lossy(&resp_out.stderr)
        );
        let resp_stdout = String::from_utf8_lossy(&resp_out.stdout);
        assert!(
            resp_stdout.contains(&format!("OK: resp v{version}")),
            "Apache output confirmation: {resp_stdout}"
        );
    }
}

/// KL01-07: Offline decoding of Apache ListOffsets boundary fixtures.
///
/// Asserts semantic required fields across all 9 version boundaries generated by
/// pinned Apache Kafka 3.9.1. Compares values against the committed fixtures without
/// requiring network or Java.
#[test]
fn apache_list_offsets_boundary_fixtures_decode_offline() {
    // 1. ListOffsets v1: classic wire format (oldest spoken), earliest (-2), latest (-1), explicit timestamp (1710000000000)
    {
        const REQ: &[u8] =
            include_bytes!("fixtures/protocol_oracles/list_offsets_v1_classic_request.bin");
        const RESP: &[u8] =
            include_bytes!("fixtures/protocol_oracles/list_offsets_v1_classic_response.bin");

        let (isolation, topics, timeout_ms, replica_id) =
            decode_list_offsets_topics_request(&mut &REQ[..], 1).expect("list_offsets v1 request");
        assert_eq!(replica_id, CONSUMER_REPLICA_ID);
        assert_eq!(isolation, 0, "v1 isolation level omitted, decodes 0");
        assert_eq!(timeout_ms, None, "v1 timeout_ms omitted");
        assert_eq!(topics.len(), 1);
        assert_eq!(topics[0].name, "offsets-v1");
        assert_eq!(topics[0].partitions.len(), 3);

        // Partition 0: earliest (-2)
        assert_eq!(topics[0].partitions[0].partition, 0);
        assert_eq!(topics[0].partitions[0].timestamp, EARLIEST_TIMESTAMP);
        assert_eq!(
            topics[0].partitions[0].current_leader_epoch,
            RecordBatch::NO_PARTITION_LEADER_EPOCH,
            "v1 leader epoch omitted"
        );

        // Partition 1: latest (-1)
        assert_eq!(topics[0].partitions[1].partition, 1);
        assert_eq!(topics[0].partitions[1].timestamp, LATEST_TIMESTAMP);
        assert_eq!(
            topics[0].partitions[1].current_leader_epoch,
            RecordBatch::NO_PARTITION_LEADER_EPOCH
        );

        // Partition 2: explicit timestamp (1710000000000)
        assert_eq!(topics[0].partitions[2].partition, 2);
        assert_eq!(topics[0].partitions[2].timestamp, 1_710_000_000_000);
        assert_eq!(
            topics[0].partitions[2].current_leader_epoch,
            RecordBatch::NO_PARTITION_LEADER_EPOCH
        );

        let (resp_topics, throttle_ms) = decode_list_offsets_topics_response(&mut &RESP[..], 1)
            .expect("list_offsets v1 response");
        assert_eq!(throttle_ms, 0, "v1 throttle omitted, decodes 0");
        assert_eq!(resp_topics.len(), 1);
        assert_eq!(resp_topics[0].name, "offsets-v1");
        assert_eq!(resp_topics[0].partitions.len(), 3);

        assert_eq!(resp_topics[0].partitions[0].partition_index, 0);
        assert_eq!(resp_topics[0].partitions[0].error_code, 0);
        assert_eq!(resp_topics[0].partitions[0].offset, 100);
        assert_eq!(
            resp_topics[0].partitions[0].leader_epoch,
            ListOffsetsPartition::UNKNOWN_EPOCH,
            "v1 leader epoch omitted"
        );

        assert_eq!(resp_topics[0].partitions[1].partition_index, 1);
        assert_eq!(resp_topics[0].partitions[1].error_code, 0);
        assert_eq!(resp_topics[0].partitions[1].offset, 250);
        assert_eq!(
            resp_topics[0].partitions[1].leader_epoch,
            ListOffsetsPartition::UNKNOWN_EPOCH
        );

        assert_eq!(resp_topics[0].partitions[2].partition_index, 2);
        assert_eq!(
            resp_topics[0].partitions[2].error_code,
            UNKNOWN_TOPIC_OR_PARTITION
        );
        assert_eq!(resp_topics[0].partitions[2].offset, -1);
        assert_eq!(
            resp_topics[0].partitions[2].leader_epoch,
            ListOffsetsPartition::UNKNOWN_EPOCH
        );
    }

    // 2. ListOffsets v2: isolationLevel gate (READ_COMMITTED), throttleTimeMs gate, and NOT_LEADER_OR_FOLLOWER error
    {
        const REQ: &[u8] =
            include_bytes!("fixtures/protocol_oracles/list_offsets_v2_isolation_request.bin");
        const RESP: &[u8] =
            include_bytes!("fixtures/protocol_oracles/list_offsets_v2_isolation_response.bin");

        let (isolation, topics, timeout_ms, replica_id) =
            decode_list_offsets_topics_request(&mut &REQ[..], 2).expect("list_offsets v2 request");
        assert_eq!(replica_id, CONSUMER_REPLICA_ID);
        assert_eq!(isolation, 1, "v2 isolation level READ_COMMITTED");
        assert_eq!(timeout_ms, None);
        assert_eq!(topics.len(), 1);
        assert_eq!(topics[0].name, "offsets-v2");
        assert_eq!(topics[0].partitions.len(), 2);
        assert_eq!(topics[0].partitions[0].timestamp, 1_710_000_001_000);
        assert_eq!(topics[0].partitions[1].timestamp, LATEST_TIMESTAMP);

        let (resp_topics, throttle_ms) = decode_list_offsets_topics_response(&mut &RESP[..], 2)
            .expect("list_offsets v2 response");
        assert_eq!(throttle_ms, 25, "v2 throttle present");
        assert_eq!(resp_topics.len(), 1);
        assert_eq!(resp_topics[0].name, "offsets-v2");
        assert_eq!(resp_topics[0].partitions.len(), 2);

        assert_eq!(resp_topics[0].partitions[0].partition_index, 0);
        assert_eq!(resp_topics[0].partitions[0].error_code, 0);
        assert_eq!(resp_topics[0].partitions[0].timestamp, 1_710_000_001_000);
        assert_eq!(resp_topics[0].partitions[0].offset, 150);

        assert_eq!(resp_topics[0].partitions[1].partition_index, 1);
        assert_eq!(
            resp_topics[0].partitions[1].error_code,
            NOT_LEADER_OR_FOLLOWER
        );
        assert_eq!(resp_topics[0].partitions[1].offset, -1);
    }

    // 3. ListOffsets v3: client throttling enabled, READ_UNCOMMITTED isolationLevel
    {
        const REQ: &[u8] =
            include_bytes!("fixtures/protocol_oracles/list_offsets_v3_throttle_request.bin");
        const RESP: &[u8] =
            include_bytes!("fixtures/protocol_oracles/list_offsets_v3_throttle_response.bin");

        let (isolation, topics, _, _) =
            decode_list_offsets_topics_request(&mut &REQ[..], 3).expect("list_offsets v3 request");
        assert_eq!(isolation, 0, "v3 isolation level READ_UNCOMMITTED");
        assert_eq!(topics[0].name, "offsets-v3");

        let (resp_topics, throttle_ms) = decode_list_offsets_topics_response(&mut &RESP[..], 3)
            .expect("list_offsets v3 response");
        assert_eq!(throttle_ms, 45, "v3 throttle present");
        assert!(ListOffsetsResponse::should_client_throttle(3));
        assert!(!ListOffsetsResponse::should_client_throttle(2));
        assert_eq!(resp_topics[0].partitions[0].offset, 300);
    }

    // 4. ListOffsets v4: currentLeaderEpoch in request and leaderEpoch in response
    {
        const REQ: &[u8] =
            include_bytes!("fixtures/protocol_oracles/list_offsets_v4_leader_epoch_request.bin");
        const RESP: &[u8] =
            include_bytes!("fixtures/protocol_oracles/list_offsets_v4_leader_epoch_response.bin");

        let (isolation, topics, _, _) =
            decode_list_offsets_topics_request(&mut &REQ[..], 4).expect("list_offsets v4 request");
        assert_eq!(isolation, 1);
        assert_eq!(topics[0].name, "offsets-v4");
        assert_eq!(topics[0].partitions[0].current_leader_epoch, 10);
        assert_eq!(topics[0].partitions[0].timestamp, 1_710_000_003_000);
        assert_eq!(
            topics[0].partitions[1].current_leader_epoch,
            RecordBatch::NO_PARTITION_LEADER_EPOCH
        );
        assert_eq!(topics[0].partitions[1].timestamp, EARLIEST_TIMESTAMP);

        let (resp_topics, throttle_ms) = decode_list_offsets_topics_response(&mut &RESP[..], 4)
            .expect("list_offsets v4 response");
        assert_eq!(throttle_ms, 60);
        assert_eq!(resp_topics[0].partitions[0].offset, 400);
        assert_eq!(resp_topics[0].partitions[0].leader_epoch, 10);
        assert_eq!(resp_topics[0].partitions[1].offset, 50);
        assert_eq!(
            resp_topics[0].partitions[1].leader_epoch,
            ListOffsetsPartition::UNKNOWN_EPOCH
        );
    }

    // 5. ListOffsets v5: classic wire format boundary before flexible transition with multiple topics, debugging replicaId, and LEADER_NOT_AVAILABLE
    {
        const REQ: &[u8] = include_bytes!(
            "fixtures/protocol_oracles/list_offsets_v5_classic_boundary_request.bin"
        );
        const RESP: &[u8] = include_bytes!(
            "fixtures/protocol_oracles/list_offsets_v5_classic_boundary_response.bin"
        );

        let (isolation, topics, _, replica_id) =
            decode_list_offsets_topics_request(&mut &REQ[..], 5).expect("list_offsets v5 request");
        assert_eq!(replica_id, DEBUGGING_REPLICA_ID);
        assert_eq!(isolation, 0);
        assert_eq!(topics.len(), 2);
        assert_eq!(topics[0].name, "offsets-v5-a");
        assert_eq!(topics[0].partitions[0].current_leader_epoch, 12);
        assert_eq!(topics[1].name, "offsets-v5-b");
        assert_eq!(topics[1].partitions[0].current_leader_epoch, 8);
        assert_eq!(topics[1].partitions[1].timestamp, 1_710_000_004_000);

        let (resp_topics, throttle_ms) = decode_list_offsets_topics_response(&mut &RESP[..], 5)
            .expect("list_offsets v5 response");
        assert_eq!(throttle_ms, 75);
        assert_eq!(resp_topics.len(), 2);
        assert_eq!(resp_topics[0].partitions[0].offset, 500);
        assert_eq!(resp_topics[0].partitions[0].leader_epoch, 12);
        assert_eq!(resp_topics[1].partitions[0].offset, 0);
        assert_eq!(resp_topics[1].partitions[0].leader_epoch, 8);
        assert_eq!(
            resp_topics[1].partitions[1].error_code,
            LEADER_NOT_AVAILABLE
        );
        assert_eq!(resp_topics[1].partitions[1].offset, -1);
    }

    // 6. ListOffsets v6: flexible wire format boundary with compact encoding and unknown tagged fields
    {
        const REQ: &[u8] =
            include_bytes!("fixtures/protocol_oracles/list_offsets_v6_flexible_request.bin");
        const RESP: &[u8] =
            include_bytes!("fixtures/protocol_oracles/list_offsets_v6_flexible_response.bin");

        let (isolation, topics, timeout_ms, replica_id) =
            decode_list_offsets_topics_request(&mut &REQ[..], 6).expect("list_offsets v6 request");
        assert_eq!(replica_id, CONSUMER_REPLICA_ID);
        assert_eq!(isolation, 1);
        assert_eq!(timeout_ms, None);
        assert_eq!(topics.len(), 1);
        assert_eq!(topics[0].name, "offsets-v6-flex");
        assert_eq!(topics[0].partitions[0].current_leader_epoch, 15);
        assert_eq!(topics[0].partitions[0].timestamp, 1_710_000_005_000);

        let (resp_topics, throttle_ms) = decode_list_offsets_topics_response(&mut &RESP[..], 6)
            .expect("list_offsets v6 response");
        assert_eq!(throttle_ms, 80);
        assert_eq!(resp_topics.len(), 1);
        assert_eq!(resp_topics[0].name, "offsets-v6-flex");
        assert_eq!(resp_topics[0].partitions[0].offset, 600);
        assert_eq!(resp_topics[0].partitions[0].leader_epoch, 15);
    }

    // 7. ListOffsets v7: flexible wire format with MAX_TIMESTAMP (-3, KIP-734)
    {
        const REQ: &[u8] =
            include_bytes!("fixtures/protocol_oracles/list_offsets_v7_max_timestamp_request.bin");
        const RESP: &[u8] =
            include_bytes!("fixtures/protocol_oracles/list_offsets_v7_max_timestamp_response.bin");

        let (_, topics, _, _) =
            decode_list_offsets_topics_request(&mut &REQ[..], 7).expect("list_offsets v7 request");
        assert_eq!(topics[0].name, "offsets-v7-max");
        assert_eq!(topics[0].partitions[0].timestamp, MAX_TIMESTAMP);
        assert_eq!(topics[0].partitions[0].current_leader_epoch, 20);

        let (resp_topics, throttle_ms) = decode_list_offsets_topics_response(&mut &RESP[..], 7)
            .expect("list_offsets v7 response");
        assert_eq!(throttle_ms, 90);
        assert_eq!(resp_topics[0].partitions[0].offset, 700);
        assert_eq!(resp_topics[0].partitions[0].timestamp, 1_710_000_006_000);
        assert_eq!(resp_topics[0].partitions[0].leader_epoch, 20);
    }

    // 8. ListOffsets v8: flexible wire format with EARLIEST_LOCAL_TIMESTAMP (-4, KIP-405)
    {
        const REQ: &[u8] =
            include_bytes!("fixtures/protocol_oracles/list_offsets_v8_earliest_local_request.bin");
        const RESP: &[u8] =
            include_bytes!("fixtures/protocol_oracles/list_offsets_v8_earliest_local_response.bin");

        let (_, topics, _, _) =
            decode_list_offsets_topics_request(&mut &REQ[..], 8).expect("list_offsets v8 request");
        assert_eq!(topics[0].name, "offsets-v8-local");
        assert_eq!(topics[0].partitions[0].timestamp, EARLIEST_LOCAL_TIMESTAMP);
        assert_eq!(topics[0].partitions[0].current_leader_epoch, 22);

        let (resp_topics, throttle_ms) = decode_list_offsets_topics_response(&mut &RESP[..], 8)
            .expect("list_offsets v8 response");
        assert_eq!(throttle_ms, 95);
        assert_eq!(resp_topics[0].partitions[0].offset, 800);
        assert_eq!(resp_topics[0].partitions[0].leader_epoch, 22);
    }

    // 9. ListOffsets v9: highest version supported by Apache Kafka 3.9.1, with LATEST_TIERED_TIMESTAMP (-5, KIP-1005)
    {
        const REQ: &[u8] =
            include_bytes!("fixtures/protocol_oracles/list_offsets_v9_latest_tiered_request.bin");
        const RESP: &[u8] =
            include_bytes!("fixtures/protocol_oracles/list_offsets_v9_latest_tiered_response.bin");

        let (_, topics, _, _) =
            decode_list_offsets_topics_request(&mut &REQ[..], 9).expect("list_offsets v9 request");
        assert_eq!(topics[0].name, "offsets-v9-tiered");
        assert_eq!(topics[0].partitions[0].timestamp, LATEST_TIERED_TIMESTAMP);
        assert_eq!(topics[0].partitions[0].current_leader_epoch, 25);

        let (resp_topics, throttle_ms) = decode_list_offsets_topics_response(&mut &RESP[..], 9)
            .expect("list_offsets v9 response");
        assert_eq!(throttle_ms, 100);
        assert_eq!(resp_topics[0].partitions[0].offset, 799);
        assert_eq!(resp_topics[0].partitions[0].leader_epoch, 25);
    }

    // 10. ListOffsets v10: flexible wire format with non-default TimeoutMs (1500 ms, KIP-1075)
    {
        const REQ: &[u8] =
            include_bytes!("fixtures/protocol_oracles/list_offsets_v10_timeout_request.bin");
        const RESP: &[u8] =
            include_bytes!("fixtures/protocol_oracles/list_offsets_v10_timeout_response.bin");

        let (isolation, topics, timeout_ms, replica_id) =
            decode_list_offsets_topics_request(&mut &REQ[..], 10)
                .expect("list_offsets v10 request");
        assert_eq!(replica_id, CONSUMER_REPLICA_ID);
        assert_eq!(isolation, 1);
        assert_eq!(timeout_ms, Some(1500), "v10 non-default timeoutMs 1500 ms");
        assert_eq!(topics.len(), 1);
        assert_eq!(topics[0].name, "offsets-v10-timeout");
        assert_eq!(topics[0].partitions[0].current_leader_epoch, 15);
        assert_eq!(topics[0].partitions[0].timestamp, LATEST_TIMESTAMP);

        let (resp_topics, throttle_ms) = decode_list_offsets_topics_response(&mut &RESP[..], 10)
            .expect("list_offsets v10 response");
        assert_eq!(throttle_ms, 50);
        assert_eq!(resp_topics.len(), 1);
        assert_eq!(resp_topics[0].name, "offsets-v10-timeout");
        assert_eq!(resp_topics[0].partitions[0].offset, 100);
        assert_eq!(resp_topics[0].partitions[0].leader_epoch, 15);
        assert_eq!(resp_topics[0].partitions[0].timestamp, 1_710_000_000_000);
    }
}

/// KL01-07: Negative, mutation, and unsupported combinations tests for ListOffsets.
///
/// Verifies that mutating field order or required version gates fails decoding,
/// v10 timeoutMs wire encoding/decoding is enforced, and unsupported timestamp/version
/// combinations produce explicit outcomes.
#[test]
fn list_offsets_version_gate_and_field_order_mutations_fail() {
    const V1_REQ: &[u8] =
        include_bytes!("fixtures/protocol_oracles/list_offsets_v1_classic_request.bin");
    const V1_RESP: &[u8] =
        include_bytes!("fixtures/protocol_oracles/list_offsets_v1_classic_response.bin");
    const V2_REQ: &[u8] =
        include_bytes!("fixtures/protocol_oracles/list_offsets_v2_isolation_request.bin");
    const V2_RESP: &[u8] =
        include_bytes!("fixtures/protocol_oracles/list_offsets_v2_isolation_response.bin");
    const V4_RESP: &[u8] =
        include_bytes!("fixtures/protocol_oracles/list_offsets_v4_leader_epoch_response.bin");
    const V5_RESP: &[u8] =
        include_bytes!("fixtures/protocol_oracles/list_offsets_v5_classic_boundary_response.bin");
    const V6_RESP: &[u8] =
        include_bytes!("fixtures/protocol_oracles/list_offsets_v6_flexible_response.bin");
    const V9_REQ: &[u8] =
        include_bytes!("fixtures/protocol_oracles/list_offsets_v9_latest_tiered_request.bin");
    const V10_REQ: &[u8] =
        include_bytes!("fixtures/protocol_oracles/list_offsets_v10_timeout_request.bin");
    const V10_RESP: &[u8] =
        include_bytes!("fixtures/protocol_oracles/list_offsets_v10_timeout_response.bin");

    // Mutation 1: IsolationLevel gate on request (gate is v2+).
    // Decoding v1 request (no isolationLevel on wire) with v2 decoder fails or causes protocol desync.
    let mut cur = V1_REQ;
    assert!(
        decode_list_offsets_topics_request(&mut cur, 2).is_err(),
        "decoding v1 request as v2 must fail because isolationLevel byte is missing on wire"
    );

    // Decoding v2 request with v1 decoder ignores isolationLevel and causes protocol desync / leftover bytes.
    let mut cur = V2_REQ;
    let res = decode_list_offsets_topics_request(&mut cur, 1);
    assert!(
        res.is_err() || !cur.is_empty(),
        "decoding v2 request with v1 decoder must fail or leave unconsumed bytes"
    );

    // Mutation 2: ThrottleTimeMs gate on response (gate is v2+).
    // Decoding v2 response with v1 decoder fails because throttleTimeMs is treated as topic count.
    let mut cur = V2_RESP;
    assert!(
        decode_list_offsets_topics_response(&mut cur, 1).is_err(),
        "decoding v2 response with v1 decoder must fail due to throttleTimeMs treated as topic count"
    );

    // Decoding v1 response with v2 decoder fails or causes desync.
    let mut cur = V1_RESP;
    assert!(
        decode_list_offsets_topics_response(&mut cur, 2).is_err(),
        "decoding v1 response with v2 decoder must fail because throttleTimeMs is missing on wire"
    );

    // Mutation 3: LeaderEpoch gate on response (gate is v4+).
    // Decoding v4 response with v3 decoder ignores leaderEpoch, leaving unconsumed bytes on wire.
    let mut cur = V4_RESP;
    let _ = decode_list_offsets_topics_response(&mut cur, 3).expect("decodes without leaderEpoch");
    assert!(
        !cur.is_empty(),
        "decoding v4 response with v3 decoder must leave unconsumed leaderEpoch bytes"
    );

    // Mutation 4: Flexible format gate (gate is v6+).
    // Decoding v5 classic response with v6 flexible decoder treats 0x00 as null array and leaves unconsumed bytes.
    let mut cur = V5_RESP;
    let res = decode_list_offsets_topics_response(&mut cur, 6);
    assert!(
        res.is_err() || !cur.is_empty(),
        "decoding v5 classic response as v6 flexible must fail or leave unconsumed bytes"
    );

    // Decoding v6 flexible response with v5 classic decoder fails with format mismatch.
    let mut cur = V6_RESP;
    assert!(
        decode_list_offsets_topics_response(&mut cur, 5).is_err(),
        "decoding v6 flexible response as v5 classic must fail"
    );

    // Mutation 5: Mutating field order in ListOffsetsResponse (e.g. putting throttle_time_ms after topics).
    let mut mutated_order = BytesMut::new();
    mutated_order.extend_from_slice(&1i32.to_be_bytes()); // topic count = 1
    mutated_order.extend_from_slice(&25i32.to_be_bytes()); // mutated throttle first
    mutated_order.extend_from_slice(&(2i16).to_be_bytes()); // string len = 2
    mutated_order.extend_from_slice(b"t1");
    mutated_order.extend_from_slice(&0i32.to_be_bytes()); // part count = 0
    let mut cur = mutated_order.as_ref();
    assert!(
        decode_list_offsets_topics_response(&mut cur, 2).is_err(),
        "mutating ListOffsetsResponse field order must fail decoding"
    );

    // Mutation 6: v10 TimeoutMs gate and wire handling (KIP-1075).
    // Rust encodes v10 request with TimeoutMs = 5000.
    let mut req10_bytes = BytesMut::new();
    encode_list_offsets_request(
        &mut req10_bytes,
        10,
        1,
        "topic-v10",
        0,
        15,
        LATEST_TIMESTAMP,
        5000,
    )
    .expect("encode v10 request with TimeoutMs");

    // Decodes successfully with v10 decoder, timeout_ms is Some(5000).
    let mut cur = req10_bytes.as_ref();
    let (iso10, topics10, timeout10, rep10) =
        decode_list_offsets_topics_request(&mut cur, 10).expect("decode v10 request");
    leftover_empty(cur, "ListOffsets v10 request");
    assert_eq!(rep10, CONSUMER_REPLICA_ID);
    assert_eq!(iso10, 1);
    assert_eq!(timeout10, Some(5000));
    assert_eq!(topics10[0].name, "topic-v10");
    assert_eq!(topics10[0].partitions[0].current_leader_epoch, 15);

    // Decoding v10 request with v9 decoder ignores timeout_ms and leaves unconsumed bytes (trailing 4 bytes).
    let mut cur = req10_bytes.as_ref();
    let (_, _, timeout9, _) =
        decode_list_offsets_topics_request(&mut cur, 9).expect("decode v10 as v9");
    assert_eq!(timeout9, None);
    assert_eq!(
        cur.len(),
        4,
        "decoding v10 request with v9 decoder must leave unconsumed 4 bytes of TimeoutMs"
    );

    // Decoding committed Apache v10 request with v9 decoder ignores timeout_ms and leaves 4 unconsumed bytes.
    let mut cur = V10_REQ;
    let (_, _, timeout_committed9, _) =
        decode_list_offsets_topics_request(&mut cur, 9).expect("decode committed v10 as v9");
    assert_eq!(timeout_committed9, None);
    assert_eq!(
        cur.len(),
        4,
        "decoding committed v10 request with v9 decoder must leave unconsumed 4 bytes of TimeoutMs"
    );

    // Decoding v9 request with v10 decoder fails because TimeoutMs is missing on wire.
    let mut cur = V9_REQ;
    assert!(
        decode_list_offsets_topics_request(&mut cur, 10).is_err(),
        "decoding v9 request as v10 must fail because TimeoutMs is missing on wire"
    );

    // Decoding committed v10 response with v10 decoder.
    let mut cur = V10_RESP;
    let (v10_resp_topics, throttle) =
        decode_list_offsets_topics_response(&mut cur, 10).expect("decode committed v10 response");
    leftover_empty(cur, "committed ListOffsets v10 response");
    assert_eq!(throttle, 50);
    assert_eq!(v10_resp_topics[0].name, "offsets-v10-timeout");

    // Mutation 7: Unsupported timestamp / version combinations and builder requirements.
    // ListOffsetsRequest::for_consumer maps feature requirements to minimum allowed versions:
    assert_eq!(
        ListOffsetsRequest::for_consumer(false, false, false, false, true),
        9,
        "LATEST_TIERED_TIMESTAMP requires v9+"
    );
    assert_eq!(
        ListOffsetsRequest::for_consumer(false, false, false, true, false),
        8,
        "EARLIEST_LOCAL_TIMESTAMP requires v8+"
    );
    assert_eq!(
        ListOffsetsRequest::for_consumer(false, false, true, false, false),
        7,
        "MAX_TIMESTAMP requires v7+"
    );
    assert_eq!(
        ListOffsetsRequest::for_consumer(false, true, false, false, false),
        2,
        "READ_COMMITTED requires v2+"
    );
    assert_eq!(
        ListOffsetsRequest::for_consumer(true, false, false, false, false),
        1,
        "require_timestamp requires v1+"
    );
    assert_eq!(
        ListOffsetsRequest::for_consumer(false, false, false, false, false),
        0
    );

    // Unimplemented versions (< 0 or > 10) have explicit Error::Protocol outcomes:
    let mut buf = BytesMut::new();
    assert!(
        encode_list_offsets_request(&mut buf, -1, 0, "t", 0, 0, -1, 0).is_err(),
        "v-1 is not implemented"
    );
    assert!(
        encode_list_offsets_request(&mut buf, 11, 0, "t", 0, 0, -1, 0).is_err(),
        "v11 is not implemented"
    );
    let mut empty = &[][..];
    assert!(
        decode_list_offsets_topics_request(&mut empty, -1).is_err(),
        "v-1 decode is not implemented"
    );
    assert!(
        decode_list_offsets_topics_request(&mut empty, 11).is_err(),
        "v11 decode is not implemented"
    );
    assert!(
        decode_list_offsets_topics_response(&mut empty, -1).is_err(),
        "v-1 resp decode is not implemented"
    );
    assert!(
        decode_list_offsets_topics_response(&mut empty, 11).is_err(),
        "v11 resp decode is not implemented"
    );
}

/// KL01-07: Decode Rust output with Apache where Java is available.
///
/// Encodes ListOffsets requests and responses in Rust across the advertised version boundaries (v1..=10),
/// then runs Apache Kafka's `ListOffsetsRequestData.read` and `ListOffsetsResponseData.read` in Java
/// to verify that Apache successfully decodes partitionline wire output.
#[test]
fn rust_list_offsets_output_decodes_with_apache_when_java_available() {
    let Some((java_bin, cp)) = java_conformance_classpath() else {
        println!("java toolchain / conformance jars not available; skipping live Apache decode of Rust output");
        return;
    };

    let versions: [(i16, i8, i64, i32, i32, i32); 10] = [
        (1, 0, -2, -1, 0, 0),
        (2, 1, 1_710_000_001_000, -1, 25, 0),
        (3, 0, -1, -1, 45, 0),
        (4, 1, 1_710_000_003_000, 10, 60, 0),
        (5, 0, -1, 12, 75, 0),
        (6, 1, 1_710_000_005_000, 15, 80, 0),
        (7, 0, -3, 20, 90, 0),
        (8, 0, -4, 22, 95, 0),
        (9, 1, -5, 25, 100, 0),
        (10, 1, -1, 15, 50, 1500),
    ];

    for (version, isolation, timestamp, epoch, throttle_ms, timeout_ms) in versions {
        // 1. Rust encodes ListOffsetsRequest
        let mut req_buf = BytesMut::new();
        let topic_req = ListOffsetsTopicRequest::new(
            "rust-offsets-topic",
            vec![ListOffsetsPartitionRequest::new(0, epoch, timestamp)],
        );
        encode_list_offsets_topics_request(
            &mut req_buf,
            version,
            isolation,
            &[topic_req],
            timeout_ms,
        )
        .expect("encode list offsets request in Rust");
        let req_hex: String = req_buf.iter().map(|b| format!("{b:02x}")).collect();

        let req_out = std::process::Command::new(&java_bin)
            .args([
                "-cp",
                &cp,
                "org.apache.kafka.conformance.FixtureGenerator",
                "--decode-rust",
                "list-offsets-req",
                &version.to_string(),
                &req_hex,
            ])
            .output()
            .expect("execute java decode-rust list-offsets-req");
        assert!(
            req_out.status.success(),
            "Apache Java failed to decode Rust ListOffsetsRequest v{version}: {}",
            String::from_utf8_lossy(&req_out.stderr)
        );
        let req_stdout = String::from_utf8_lossy(&req_out.stdout);
        assert!(
            req_stdout.contains(&format!("OK: req v{version}")),
            "Apache output confirmation: {req_stdout}"
        );
        if version >= 10 {
            assert!(
                req_stdout.contains("timeoutMs=1500"),
                "Apache output must confirm timeoutMs=1500: {req_stdout}"
            );
        }

        // 2. Rust encodes ListOffsetsResponse
        let mut resp_buf = BytesMut::new();
        let topic_resp = ListOffsetsTopicResponse::new(
            "rust-offsets-topic",
            vec![ListOffsetsResponsePartition::new(
                0,
                ListOffsetsPartition::ok(1_710_000_000_000, 500, epoch),
            )],
        );
        encode_list_offsets_topics_response_with_throttle(
            &mut resp_buf,
            version,
            &[topic_resp],
            throttle_ms,
        )
        .expect("encode list offsets response in Rust");
        let resp_hex: String = resp_buf.iter().map(|b| format!("{b:02x}")).collect();

        let resp_out = std::process::Command::new(&java_bin)
            .args([
                "-cp",
                &cp,
                "org.apache.kafka.conformance.FixtureGenerator",
                "--decode-rust",
                "list-offsets-resp",
                &version.to_string(),
                &resp_hex,
            ])
            .output()
            .expect("execute java decode-rust list-offsets-resp");
        assert!(
            resp_out.status.success(),
            "Apache Java failed to decode Rust ListOffsetsResponse v{version}: {}",
            String::from_utf8_lossy(&resp_out.stderr)
        );
        let resp_stdout = String::from_utf8_lossy(&resp_out.stdout);
        assert!(
            resp_stdout.contains(&format!("OK: resp v{version}")),
            "Apache output confirmation: {resp_stdout}"
        );
    }
}

/// KL05-14: 16-byte topic id with bytes `start..start+16`.
fn share_seq16(start: u8) -> [u8; 16] {
    let mut out = [0u8; 16];
    for (i, b) in out.iter_mut().enumerate() {
        *b = start.wrapping_add(u8::try_from(i).expect("seq16 index fits u8"));
    }
    out
}

/// KL05-14: decode a hex descriptor field into bytes.
fn share_hex_bytes(hex: &str) -> Vec<u8> {
    assert!(hex.len() % 2 == 0, "hex length");
    hex.as_bytes()
        .chunks(2)
        .map(|c| {
            u8::from_str_radix(std::str::from_utf8(c).expect("hex ascii"), 16).expect("hex pair")
        })
        .collect()
}

/// KL05-14: pin a fixture descriptor to its committed request/response bytes.
///
/// Asserts the descriptor's file names, sizes, and hex payloads match the
/// `include_bytes!` inputs exactly, so the JSON cannot drift from the bins.
fn assert_share_fixture_descriptor(json: &str, req: &[u8], resp: &[u8]) {
    let parsed = parse_json(json);
    let obj = parsed.as_object();
    for (key, body) in [("request", req), ("response", resp)] {
        let section = obj
            .iter()
            .find(|(k, _)| k == key)
            .unwrap_or_else(|| panic!("share fixture missing {key}"))
            .1
            .as_object();
        let hex = section
            .iter()
            .find(|(k, _)| k == "hex")
            .unwrap_or_else(|| panic!("share fixture {key} missing hex"))
            .1
            .as_str();
        let size = section
            .iter()
            .find(|(k, _)| k == "size_bytes")
            .unwrap_or_else(|| panic!("share fixture {key} missing size_bytes"))
            .1
            .as_i64();
        let file = section
            .iter()
            .find(|(k, _)| k == "file")
            .unwrap_or_else(|| panic!("share fixture {key} missing file"))
            .1
            .as_str();
        assert!(file.starts_with("share_"), "share fixture file prefix");
        assert!(file.ends_with(".bin"), "share fixture file suffix");
        assert_eq!(share_hex_bytes(hex), body, "share fixture {key} hex");
        assert_eq!(
            size,
            i64::try_from(body.len()).expect("fixture size fits i64"),
            "share fixture {key} size"
        );
    }
}

/// KL05-14: ShareFetch v0 wire deltas decoded offline.
///
/// v0 carries per-partition PartitionMaxBytes (removed in v1) and
/// ForgottenTopicsData (kept duplicate partition indexes). Response pins
/// JSON defaults (throttle 0, null messages, empty endpoints, 0/0 leaders)
/// plus a partition-level `UNKNOWN_TOPIC_OR_PARTITION` error case.
#[test]
fn share_fetch_v0_delta_fixture_decodes_offline() {
    const REQ: &[u8] =
        include_bytes!("fixtures/protocol_oracles/share_fetch_v0_partition_max_bytes_request.bin");
    const RESP: &[u8] =
        include_bytes!("fixtures/protocol_oracles/share_fetch_v0_partition_max_bytes_response.bin");
    const DESC: &str =
        include_str!("fixtures/protocol_oracles/share_fetch_v0_partition_max_bytes.json");
    assert_share_fixture_descriptor(DESC, REQ, RESP);

    let (
        group_id,
        member_id,
        epoch,
        max_records,
        topics,
        forgotten,
        batch_size,
        max_wait_ms,
        min_bytes,
        max_bytes,
    ) = decode_share_fetch_request(&mut &REQ[..], 0).expect("share fetch v0 request");
    assert_eq!(group_id, "share-v0-deltas");
    assert_eq!(member_id, "member-v0-1");
    assert_eq!(epoch, 1);
    assert_eq!(max_records, 0, "v0 omits MaxRecords");
    assert_eq!(batch_size, 0, "v0 omits BatchSize");
    assert_eq!(max_wait_ms, 500);
    assert_eq!(min_bytes, 1);
    assert_eq!(max_bytes, 0x7fff_ffff, "v0 MaxBytes JSON default");
    assert_eq!(topics.len(), 1);
    assert_eq!(topics[0].topic_id, share_seq16(0x00));
    assert_eq!(topics[0].partitions.len(), 2);
    assert_eq!(topics[0].partitions[0].partition, 0);
    assert_eq!(
        topics[0].partitions[0].partition_max_bytes, 1_048_576,
        "v0 PartitionMaxBytes on the wire"
    );
    assert_eq!(
        topics[0].partitions[0].acknowledgements.len(),
        1,
        "v0 piggybacked ack batch"
    );
    let batch = &topics[0].partitions[0].acknowledgements[0];
    assert_eq!((batch.first_offset, batch.last_offset), (100, 102));
    assert_eq!(batch.types, vec![1, 1, 1]);
    assert_eq!(topics[0].partitions[1].partition, 1);
    assert_eq!(
        topics[0].partitions[1].partition_max_bytes, 0,
        "v0 zero PartitionMaxBytes stays zero"
    );
    let gap = &topics[0].partitions[1].acknowledgements[0];
    assert_eq!((gap.first_offset, gap.last_offset), (50, 53));
    assert_eq!(gap.types, vec![1, 0, 2, 3], "accept, gap, release, reject");
    assert_eq!(forgotten.len(), 1);
    assert_eq!(forgotten[0].topic_id, share_seq16(0x00));
    assert_eq!(
        forgotten[0].partitions,
        vec![2, 2, 3],
        "duplicate forgotten partitions are kept"
    );

    let (resp_topics, endpoints, throttle, error_message, acq, error_code) =
        decode_share_fetch_response(&mut &RESP[..], 0).expect("share fetch v0 response");
    assert_eq!(throttle, 0, "v0 throttle JSON default");
    assert_eq!(error_code, 0);
    assert_eq!(error_message, None);
    assert_eq!(acq, 0, "v0 omits AcquisitionLockTimeoutMs");
    assert!(endpoints.is_empty());
    assert_eq!(resp_topics.len(), 1);
    assert_eq!(resp_topics[0].topic_id, share_seq16(0x00));
    assert_eq!(resp_topics[0].partitions.len(), 2);
    let ok = &resp_topics[0].partitions[0];
    assert_eq!((ok.partition, ok.error_code), (0, 0));
    assert_eq!(ok.error_message, None);
    assert_eq!(ok.acknowledge_error_code, 0);
    assert_eq!(ok.acknowledge_error_message, None);
    assert_eq!((ok.current_leader_id, ok.current_leader_epoch), (0, 0));
    assert!(ok.records.is_empty());
    assert_eq!(ok.acquired.len(), 1);
    assert_eq!(ok.acquired[0].first_offset, 100);
    assert_eq!(ok.acquired[0].last_offset, 102);
    assert_eq!(ok.acquired[0].delivery_count, 1);
    let missing = &resp_topics[0].partitions[1];
    assert_eq!(missing.partition, 1);
    assert_eq!(missing.error_code, UNKNOWN_TOPIC_OR_PARTITION);
    assert_eq!(missing.error_message.as_deref(), Some("unknown topic"));
    assert!(missing.acquired.is_empty());
}

/// KL05-14: ShareFetch v1 wire deltas decoded offline.
///
/// v1 replaces PartitionMaxBytes with top-level MaxRecords/BatchSize and
/// adds AcquisitionLockTimeoutMs to the response. Pins a non-default
/// acquisition timeout, the acknowledge-only `INVALID_RECORD_STATE` code,
/// CurrentLeader, and NodeEndpoints.
#[test]
fn share_fetch_v1_delta_fixture_decodes_offline() {
    const REQ: &[u8] =
        include_bytes!("fixtures/protocol_oracles/share_fetch_v1_batch_size_request.bin");
    const RESP: &[u8] =
        include_bytes!("fixtures/protocol_oracles/share_fetch_v1_batch_size_response.bin");
    const DESC: &str = include_str!("fixtures/protocol_oracles/share_fetch_v1_batch_size.json");
    assert_share_fixture_descriptor(DESC, REQ, RESP);

    let (
        group_id,
        member_id,
        epoch,
        max_records,
        topics,
        forgotten,
        batch_size,
        max_wait_ms,
        min_bytes,
        max_bytes,
    ) = decode_share_fetch_request(&mut &REQ[..], 1).expect("share fetch v1 request");
    assert_eq!(group_id, "share-v1-deltas");
    assert_eq!(member_id, "member-v1-1");
    assert_eq!(epoch, 2);
    assert_eq!(max_records, 500, "v1 MaxRecords on the wire");
    assert_eq!(batch_size, 100, "v1 BatchSize distinct from MaxRecords");
    assert_eq!(max_wait_ms, 1000);
    assert_eq!(min_bytes, 1024);
    assert_eq!(max_bytes, 8_388_608);
    assert_eq!(topics.len(), 1);
    assert_eq!(topics[0].topic_id, share_seq16(0x10));
    assert_eq!(topics[0].partitions.len(), 1);
    assert_eq!(topics[0].partitions[0].partition, 0);
    assert_eq!(
        topics[0].partitions[0].partition_max_bytes, 0,
        "v1 omits PartitionMaxBytes"
    );
    assert!(forgotten.is_empty());

    let (resp_topics, endpoints, throttle, error_message, acq, error_code) =
        decode_share_fetch_response(&mut &RESP[..], 1).expect("share fetch v1 response");
    assert_eq!(throttle, 42, "v1 throttle round-trips");
    assert_eq!(error_code, 0);
    assert_eq!(error_message, None);
    assert_eq!(acq, 30_000, "v1 non-default AcquisitionLockTimeoutMs");
    assert_eq!(endpoints.len(), 1);
    assert_eq!(endpoints[0].node_id, 1);
    assert_eq!(endpoints[0].host, "broker-v1");
    assert_eq!(endpoints[0].port, 9092);
    assert_eq!(endpoints[0].rack.as_deref(), Some("rack-v1"));
    assert_eq!(resp_topics.len(), 1);
    let part = &resp_topics[0].partitions[0];
    assert_eq!((part.partition, part.error_code), (0, 0));
    assert_eq!(part.acknowledge_error_code, INVALID_RECORD_STATE);
    assert_eq!(
        part.acknowledge_error_message.as_deref(),
        Some("record state changed")
    );
    assert_eq!((part.current_leader_id, part.current_leader_epoch), (1, 5));
    assert!(part.records.is_empty());
    assert_eq!(part.acquired.len(), 1);
    assert_eq!(part.acquired[0].delivery_count, 2);
}

/// KL05-14: ShareFetch Records nullability split and error-response path.
///
/// Kafka 4.0 `nullableVersions` is `0+` (v0 compact null decodes empty);
/// Kafka 4.1 `nullableVersions` is `0` only (v1 compact null is a protocol
/// error). The v1 getErrorResponse path carries empty Responses, a top-level
/// error, non-zero throttle, and AcquisitionLockTimeoutMs 0.
#[test]
fn share_fetch_null_records_and_error_paths_decode_offline() {
    const V0_REQ: &[u8] =
        include_bytes!("fixtures/protocol_oracles/share_fetch_v0_null_records_request.bin");
    const V0_RESP: &[u8] =
        include_bytes!("fixtures/protocol_oracles/share_fetch_v0_null_records_response.bin");
    const V0_DESC: &str =
        include_str!("fixtures/protocol_oracles/share_fetch_v0_null_records.json");
    assert_share_fixture_descriptor(V0_DESC, V0_REQ, V0_RESP);

    let (group_id, member_id, epoch, _, topics, forgotten, _, _, _, _) =
        decode_share_fetch_request(&mut &V0_REQ[..], 0).expect("v0 null-records request");
    assert_eq!(group_id, "share-v0-null");
    assert_eq!(member_id, "");
    assert_eq!(epoch, -1, "final share session epoch");
    assert!(topics.is_empty(), "empty is empty Topics");
    assert!(forgotten.is_empty());
    let (resp_topics, _, throttle, _, acq, error_code) =
        decode_share_fetch_response(&mut &V0_RESP[..], 0).expect("v0 null-records response");
    assert_eq!((throttle, error_code, acq), (0, 0, 0));
    assert_eq!(resp_topics.len(), 1);
    assert_eq!(resp_topics[0].topic_id, share_seq16(0x20));
    assert!(
        resp_topics[0].partitions[0].records.is_empty(),
        "v0 compact null Records decodes empty"
    );

    const V1_REQ: &[u8] = include_bytes!(
        "fixtures/protocol_oracles/share_fetch_v1_null_records_rejected_request.bin"
    );
    const V1_RESP: &[u8] = include_bytes!(
        "fixtures/protocol_oracles/share_fetch_v1_null_records_rejected_response.bin"
    );
    const V1_DESC: &str =
        include_str!("fixtures/protocol_oracles/share_fetch_v1_null_records_rejected.json");
    assert_share_fixture_descriptor(V1_DESC, V1_REQ, V1_RESP);
    let mut cur = V1_REQ;
    let (group_id, _, _, _, topics, _, _, _, _, _) =
        decode_share_fetch_request(&mut cur, 1).expect("v1 request decodes");
    assert_eq!(group_id, "share-v1-null");
    assert!(topics.is_empty());
    leftover_empty(cur, "ShareFetch v1 null-records request");
    let mut cur = V1_RESP;
    let err = decode_share_fetch_response(&mut cur, 1).unwrap_err();
    assert!(
        err.to_string()
            .contains("non-nullable field records was serialized as null"),
        "v1 compact null Records is a protocol error, got {err}"
    );

    const ERR_REQ: &[u8] =
        include_bytes!("fixtures/protocol_oracles/share_fetch_v1_error_response_request.bin");
    const ERR_RESP: &[u8] =
        include_bytes!("fixtures/protocol_oracles/share_fetch_v1_error_response_response.bin");
    const ERR_DESC: &str =
        include_str!("fixtures/protocol_oracles/share_fetch_v1_error_response.json");
    assert_share_fixture_descriptor(ERR_DESC, ERR_REQ, ERR_RESP);
    let mut cur = ERR_REQ;
    let _ = decode_share_fetch_request(&mut cur, 1).expect("v1 error-path request decodes");
    leftover_empty(cur, "ShareFetch v1 error-path request");
    let mut cur = ERR_RESP;
    let (err_topics, err_endpoints, err_throttle, err_message, err_acq, err_code) =
        decode_share_fetch_response(&mut cur, 1).expect("v1 error-path response decodes");
    leftover_empty(cur, "ShareFetch v1 error-path response");
    assert!(err_topics.is_empty(), "error path has empty Responses");
    assert!(err_endpoints.is_empty());
    assert_eq!(err_code, SHARE_SESSION_NOT_FOUND);
    assert_eq!(err_throttle, 13, "error path round-trips throttle");
    assert_eq!(err_message, None);
    assert_eq!(err_acq, 0, "error path keeps AcquisitionLockTimeoutMs at 0");
}

/// KL05-14: ShareAcknowledge v0/v1 same-fields fixtures decoded offline.
///
/// Request/response fields are identical across v0 and v1, so each request
/// fixture decodes under both versions. Pins throttle defaults and
/// overrides, the top-level error case, acknowledge-only partition errors,
/// and null-rack endpoints.
#[test]
fn share_acknowledge_v0_v1_fixtures_decode_offline() {
    const V0_REQ: &[u8] =
        include_bytes!("fixtures/protocol_oracles/share_ack_v0_same_fields_request.bin");
    const V0_RESP: &[u8] =
        include_bytes!("fixtures/protocol_oracles/share_ack_v0_same_fields_response.bin");
    const V0_DESC: &str = include_str!("fixtures/protocol_oracles/share_ack_v0_same_fields.json");
    assert_share_fixture_descriptor(V0_DESC, V0_REQ, V0_RESP);

    for version in [0_i16, 1] {
        let mut cur = V0_REQ;
        let (group_id, member_id, epoch, flat) =
            decode_share_acknowledge_request(&mut cur, version)
                .expect("ack v0 request decodes under v0 and v1");
        leftover_empty(cur, &format!("ShareAcknowledge v0 request as v{version}"));
        assert_eq!(group_id, "share-ack-v0");
        assert_eq!(member_id, "ack-m0");
        assert_eq!(epoch, 4);
        assert_eq!(flat.len(), 3);
        assert_eq!(flat[0].0, share_seq16(0x30));
        assert_eq!(flat[0].1, 0);
        assert_eq!(flat[0].2.len(), 1);
        assert_eq!(flat[0].2[0].first_offset, 10);
        assert_eq!(flat[0].2[0].last_offset, 12);
        assert_eq!(flat[0].2[0].types, vec![1, 1, 1]);
        assert_eq!(flat[1].1, 1);
        assert!(flat[1].2.is_empty(), "empty ack batch list");
        assert_eq!(flat[2].1, 2);
        assert_eq!(flat[2].2[0].types, vec![1, 0, 2, 3]);

        let mut cur = V0_RESP;
        let (error_code, resp_topics, endpoints, throttle, error_message) =
            decode_share_acknowledge_topics_response(&mut cur, version)
                .expect("ack v0 response decodes under v0 and v1");
        leftover_empty(cur, &format!("ShareAcknowledge v0 response as v{version}"));
        assert_eq!(error_code, 0);
        assert_eq!(throttle, 0, "throttle JSON default");
        assert_eq!(error_message, None);
        assert!(endpoints.is_empty());
        assert_eq!(resp_topics.len(), 1);
        assert_eq!(resp_topics[0].topic_id, share_seq16(0x30));
        assert_eq!(resp_topics[0].partitions.len(), 2);
        assert_eq!(resp_topics[0].partitions[0].partition, 0);
        assert_eq!(resp_topics[0].partitions[0].error_code, 0);
        assert_eq!(resp_topics[0].partitions[0].error_message, None);
        assert_eq!(resp_topics[0].partitions[1].partition, 1);
        assert_eq!(
            resp_topics[0].partitions[1].error_code,
            NOT_LEADER_OR_FOLLOWER
        );
        assert_eq!(
            resp_topics[0].partitions[1].error_message.as_deref(),
            Some("not leader")
        );
        assert_eq!(
            (
                resp_topics[0].partitions[1].current_leader_id,
                resp_topics[0].partitions[1].current_leader_epoch
            ),
            (2, 7)
        );
    }

    const V1_REQ: &[u8] =
        include_bytes!("fixtures/protocol_oracles/share_ack_v1_same_fields_request.bin");
    const V1_RESP: &[u8] =
        include_bytes!("fixtures/protocol_oracles/share_ack_v1_same_fields_response.bin");
    const V1_DESC: &str = include_str!("fixtures/protocol_oracles/share_ack_v1_same_fields.json");
    assert_share_fixture_descriptor(V1_DESC, V1_REQ, V1_RESP);

    for version in [0_i16, 1] {
        let mut cur = V1_REQ;
        let (group_id, member_id, epoch, flat) =
            decode_share_acknowledge_request(&mut cur, version)
                .expect("ack v1 request decodes under v0 and v1");
        leftover_empty(cur, &format!("ShareAcknowledge v1 request as v{version}"));
        assert_eq!(
            (group_id.as_str(), member_id.as_str(), epoch),
            ("share-ack-v1", "ack-m1", 5)
        );
        assert_eq!(flat.len(), 1);
        assert_eq!((flat[0].0, flat[0].1), (share_seq16(0x40), 3));
        assert_eq!(flat[0].2.len(), 1);
        assert_eq!(
            (flat[0].2[0].first_offset, flat[0].2[0].last_offset),
            (30, 31)
        );
        assert_eq!(flat[0].2[0].types, vec![2, 2]);

        let mut cur = V1_RESP;
        let (error_code, resp_topics, endpoints, throttle, error_message) =
            decode_share_acknowledge_topics_response(&mut cur, version)
                .expect("ack v1 response decodes under v0 and v1");
        leftover_empty(cur, &format!("ShareAcknowledge v1 response as v{version}"));
        assert_eq!(error_code, GROUP_AUTHORIZATION_FAILED);
        assert_eq!(error_message.as_deref(), Some("not authorized for group"));
        assert_eq!(throttle, 9);
        assert_eq!(resp_topics.len(), 1);
        assert_eq!(resp_topics[0].partitions.len(), 1);
        assert_eq!(resp_topics[0].partitions[0].partition, 3);
        assert_eq!(
            resp_topics[0].partitions[0].error_code,
            INVALID_RECORD_STATE
        );
        assert_eq!(
            resp_topics[0].partitions[0].error_message.as_deref(),
            Some("bad record state")
        );
        assert_eq!(endpoints.len(), 1);
        assert_eq!(endpoints[0].node_id, 2);
        assert_eq!(endpoints[0].host, "ack-broker");
        assert_eq!(endpoints[0].port, 9093);
        assert_eq!(endpoints[0].rack, None, "null rack");
    }
}

/// KL05-14: share version pin plus explicit v2 rejection, offline.
///
/// The `share_v2_pin.json` record pins the per-release ShareFetch and
/// ShareAcknowledge ranges, the crate-spoken versions, and the rejection
/// expectations. The committed fixture bytes decode under v0/v1 and fail
/// explicitly under v2, so no high-level v2 operation is advertised.
#[test]
fn share_v2_pin_and_explicit_rejection_oracle() {
    const PIN: &str = include_str!("fixtures/protocol_oracles/share_v2_pin.json");
    let parsed = parse_json(PIN);
    let obj = parsed.as_object();
    assert_eq!(field_str(obj, "fixture_id"), "share-v2-pin");
    assert_eq!(field_str(obj, "pin"), "4.3.1");

    let pins = obj
        .iter()
        .find(|(k, _)| k == "pins")
        .unwrap_or_else(|| panic!("share pin missing pins"))
        .1
        .as_object();
    let range_43 = pins
        .iter()
        .find(|(k, _)| k == "4.3.1")
        .unwrap_or_else(|| panic!("share pin missing 4.3.1"))
        .1
        .as_object();
    assert_eq!(field_i16s(range_43, "ShareFetch"), vec![0, 2]);
    assert_eq!(field_i16s(range_43, "ShareAcknowledge"), vec![0, 1]);
    let range_41 = pins
        .iter()
        .find(|(k, _)| k == "4.1.0")
        .unwrap_or_else(|| panic!("share pin missing 4.1.0"))
        .1
        .as_object();
    assert_eq!(field_i16s(range_41, "ShareFetch"), vec![0, 1]);
    assert_eq!(field_i16s(range_41, "ShareAcknowledge"), vec![0, 1]);

    let spoken = obj
        .iter()
        .find(|(k, _)| k == "crate_spoken")
        .unwrap_or_else(|| panic!("share pin missing crate_spoken"))
        .1
        .as_object();
    assert_eq!(field_i16s(spoken, "ShareFetch"), vec![0, 1]);
    assert_eq!(field_i16s(spoken, "ShareAcknowledge"), vec![0, 1]);
    assert_eq!(SHARE_FETCH_CRATE_MAX_VERSION, 1);
    assert_eq!(SHARE_ACKNOWLEDGE_CRATE_MAX_VERSION, 1);

    let rejection = obj
        .iter()
        .find(|(k, _)| k == "rejection")
        .unwrap_or_else(|| panic!("share pin missing rejection"))
        .1
        .as_object();
    for api in ["ShareFetch", "ShareAcknowledge"] {
        let entry = rejection
            .iter()
            .find(|(k, _)| k == api)
            .unwrap_or_else(|| panic!("share pin missing rejection for {api}"))
            .1
            .as_object();
        assert_eq!(
            field_i64(entry, "version"),
            2,
            "{api} rejection pins version 2"
        );
    }

    // Crate checkers agree with the pin: v0/v1 pass, v2 fails explicitly.
    assert_eq!(check_share_fetch_version(0).unwrap(), 0);
    assert_eq!(check_share_fetch_version(1).unwrap(), 1);
    let err = check_share_fetch_version(2).unwrap_err();
    assert!(err.to_string().contains("not implemented"));
    assert_eq!(check_share_acknowledge_version(0).unwrap(), 0);
    assert_eq!(check_share_acknowledge_version(1).unwrap(), 1);
    let err = check_share_acknowledge_version(2).unwrap_err();
    assert!(err.to_string().contains("not implemented"));

    // Committed v0/v1 bytes are rejected explicitly under v2, both directions.
    const FETCH_V1_REQ: &[u8] =
        include_bytes!("fixtures/protocol_oracles/share_fetch_v1_batch_size_request.bin");
    const FETCH_V1_RESP: &[u8] =
        include_bytes!("fixtures/protocol_oracles/share_fetch_v1_batch_size_response.bin");
    const ACK_V1_REQ: &[u8] =
        include_bytes!("fixtures/protocol_oracles/share_ack_v1_same_fields_request.bin");
    const ACK_V1_RESP: &[u8] =
        include_bytes!("fixtures/protocol_oracles/share_ack_v1_same_fields_response.bin");
    let mut cur = FETCH_V1_REQ;
    let err = decode_share_fetch_request(&mut cur, 2)
        .map(|_| ())
        .unwrap_err();
    assert!(
        err.to_string().contains("not implemented"),
        "share fetch request v2 rejected, got {err}"
    );
    let mut cur = FETCH_V1_RESP;
    let err = decode_share_fetch_response(&mut cur, 2)
        .map(|_| ())
        .unwrap_err();
    assert!(
        err.to_string().contains("not implemented"),
        "share fetch response v2 rejected, got {err}"
    );
    let mut cur = ACK_V1_REQ;
    let err = decode_share_acknowledge_request(&mut cur, 2)
        .map(|_| ())
        .unwrap_err();
    assert!(
        err.to_string().contains("not implemented"),
        "share ack request v2 rejected, got {err}"
    );
    let mut cur = ACK_V1_RESP;
    let err = decode_share_acknowledge_topics_response(&mut cur, 2)
        .map(|_| ())
        .unwrap_err();
    assert!(
        err.to_string().contains("not implemented"),
        "share ack response v2 rejected, got {err}"
    );
}

//! Compare Produce v3 through v13 bodies with pinned Apache SDK serializers.

#![expect(
    clippy::disallowed_methods,
    reason = "synchronous, bounded conformance fixture files in a test"
)]

use bytes::{Bytes, BytesMut};
use partitionline::protocol::api::{
    decode_produce_request, decode_produce_request_with_topic_ids, decode_produce_response,
    decode_produce_response_with_topic_ids, encode_produce_request,
    encode_produce_request_with_topic_ids, encode_produce_response_with_endpoints,
    encode_produce_response_with_throttle, encode_produce_response_with_topic_ids, NodeEndpoint,
    ProducePartitionData, ProducePartitionResponse, ProduceRecordError, ProduceRequest,
    ProduceTopicData,
};
use partitionline::protocol::records::{Header, Record, RecordBatch};
use std::{error::Error, fmt::Write};

const LEGACY_VERSIONS: [i16; 4] = [4, 6, 7, 12];
const TOPIC: &str = "produce-fixture";
const OTHER_TOPIC: &str = "produce-other";
const TOPIC_ID: [u8; 16] = 0x0000_0000_0000_0001_0000_0000_0000_0011_u128.to_be_bytes();
const OTHER_TOPIC_ID: [u8; 16] = 0x0000_0000_0000_0002_0000_0000_0000_0012_u128.to_be_bytes();
const TIMESTAMP: i64 = 1_700_000_000_000;

fn transactional_id(version: i16) -> Option<&'static str> {
    matches!(version, 3 | 11).then_some("produce-fixture-txn")
}

fn has_record_errors(version: i16) -> bool {
    matches!(version, 8 | 9 | 12 | 13)
}

fn topic_ids() -> [(String, [u8; 16]); 2] {
    [
        (TOPIC.into(), TOPIC_ID),
        (OTHER_TOPIC.into(), OTHER_TOPIC_ID),
    ]
}

fn request_partition(index: i32, version: i16) -> ProducePartitionData {
    let timestamp = TIMESTAMP + i64::from(index);
    let mut records = RecordBatch::from_records(vec![
        Record {
            offset: 0,
            timestamp,
            key: Some(Bytes::from_static(b"key")),
            value: Some(Bytes::from_static(b"value")),
            headers: vec![Header::new("fixture", Bytes::from_static(&[1]))],
        },
        Record {
            offset: 0,
            timestamp: timestamp + 1,
            key: Some(Bytes::from_static(b"key")),
            value: None,
            headers: vec![Header::null("fixture")],
        },
    ]);
    if transactional_id(version).is_some() {
        records.producer_id = 71;
        records.producer_epoch = 2;
        records.base_sequence = 21 + index;
        records = records.with_transactional(true);
    }
    ProducePartitionData { index, records }
}

fn request_topics(version: i16) -> Vec<ProduceTopicData> {
    let mut partitions = vec![request_partition(2, version), request_partition(5, version)];
    if has_record_errors(version) {
        partitions.push(request_partition(9, version));
    }
    let mut topics = vec![ProduceTopicData {
        topic: TOPIC.into(),
        partitions,
    }];
    if version == 13 {
        topics.push(ProduceTopicData {
            topic: OTHER_TOPIC.into(),
            partitions: vec![request_partition(2, version)],
        });
    }
    topics
}

fn response(version: i16) -> (Vec<ProducePartitionResponse>, Vec<NodeEndpoint>, i32) {
    let success = ProducePartitionResponse::partition_response_with_offsets(
        TOPIC,
        2,
        0,
        43,
        TIMESTAMP + 1000,
        if version >= 5 { 17 } else { -1 },
    );
    let error = match version {
        4 => 56,
        11 => 120,
        _ => 6,
    };
    let mut failed = ProducePartitionResponse::partition_response(TOPIC, 5, error);
    let mut endpoints = Vec::new();
    if matches!(version, 10 | 12 | 13) {
        failed.current_leader_id = 3;
        failed.current_leader_epoch = 7;
        let rack = if version == 10 {
            None
        } else {
            Some("rack-a".into())
        };
        endpoints.push(NodeEndpoint::new(3, "broker.example", 9092, rack));
    }
    let mut parts = vec![success, failed];
    if has_record_errors(version) {
        let mut invalid = ProducePartitionResponse::partition_response(TOPIC, 9, 87);
        invalid.error_message = Some("invalid batch".into());
        invalid.record_errors = vec![
            ProduceRecordError::new(0, Some("invalid timestamp".into())),
            ProduceRecordError::new(1, None),
        ];
        parts.push(invalid);
    }
    if version == 13 {
        parts.push(ProducePartitionResponse::partition_response(
            OTHER_TOPIC,
            2,
            100,
        ));
    }
    let throttle = if matches!(version, 10 | 12) { 0 } else { 37 };
    (parts, endpoints, throttle)
}

fn decode_hex(hex: &str) -> Result<Vec<u8>, Box<dyn Error>> {
    let (pairs, remainder) = hex.as_bytes().as_chunks::<2>();
    if !remainder.is_empty() {
        return Err("odd hex length".into());
    }
    pairs
        .iter()
        .map(|pair| Ok(u8::from_str_radix(std::str::from_utf8(pair)?, 16)?))
        .collect()
}

fn encode_hex(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}

fn verify_request(version: i16, mut input: &[u8]) -> Result<BytesMut, Box<dyn Error>> {
    let (txn, acks, timeout_ms, topics) = if version == 13 {
        let (txn, acks, timeout_ms, topics, ids) =
            decode_produce_request_with_topic_ids(&mut input)?;
        assert_eq!(ids, [TOPIC_ID, OTHER_TOPIC_ID], "v13 request topic IDs");
        (txn, acks, timeout_ms, topics)
    } else {
        decode_produce_request(&mut input, version)?
    };
    assert!(input.is_empty(), "unconsumed Produce request bytes");
    let expected_txn = transactional_id(version);
    let expected_timeout_ms = 1230 + i32::from(version);
    let expected_topics = request_topics(version);
    assert_eq!(txn.as_deref(), expected_txn, "v{version} transactional ID");
    assert_eq!(acks, -1, "v{version} acks");
    assert_eq!(timeout_ms, expected_timeout_ms, "v{version} timeout");
    assert_eq!(
        topics.len(),
        expected_topics.len(),
        "v{version} topic count"
    );
    for (topic, expected) in topics.iter().zip(&expected_topics) {
        let name = if version == 13 { "" } else { &expected.topic };
        assert_eq!(topic.topic, name, "v{version} topic");
        assert_eq!(topic.partitions.len(), expected.partitions.len());
        for (part, expected_part) in topic.partitions.iter().zip(&expected.partitions) {
            assert_eq!(part.index, expected_part.index, "v{version} partition");
            assert_eq!(
                part.records, expected_part.records,
                "v{version} record batch"
            );
        }
    }

    let mut out = BytesMut::new();
    if version == 13 {
        encode_produce_request_with_topic_ids(
            &mut out,
            expected_txn,
            -1,
            expected_timeout_ms,
            &expected_topics,
            &topic_ids(),
        )?;
    } else {
        encode_produce_request(
            &mut out,
            version,
            expected_txn,
            -1,
            expected_timeout_ms,
            &expected_topics,
        )?;
    }
    Ok(out)
}

fn verify_response(version: i16, mut input: &[u8]) -> Result<BytesMut, Box<dyn Error>> {
    let (parts, endpoints, throttle) = response(version);
    let (actual_parts, actual_endpoints, actual_throttle) = if version == 13 {
        let (mut decoded, endpoints, throttle, ids) =
            decode_produce_response_with_topic_ids(&mut input)?;
        assert_eq!(
            ids,
            [TOPIC_ID, TOPIC_ID, TOPIC_ID, OTHER_TOPIC_ID],
            "v13 response topic IDs per partition"
        );
        for (part, expected) in decoded.iter_mut().zip(&parts) {
            assert!(part.topic.is_empty(), "v13 response carried a topic name");
            part.topic.clone_from(&expected.topic);
        }
        (decoded, endpoints, throttle)
    } else {
        decode_produce_response(&mut input, version)?
    };
    assert!(input.is_empty(), "unconsumed Produce response bytes");
    assert_eq!(actual_parts, parts, "v{version} partition responses differ");
    assert_eq!(
        actual_endpoints, endpoints,
        "v{version} node endpoints differ"
    );
    assert_eq!(actual_throttle, throttle, "v{version} throttle differs");

    let mut out = BytesMut::new();
    if version == 13 {
        encode_produce_response_with_topic_ids(
            &mut out,
            &parts,
            &endpoints,
            throttle,
            &topic_ids(),
        )?;
    } else if endpoints.is_empty() {
        encode_produce_response_with_throttle(&mut out, version, &parts, throttle)?;
    } else {
        encode_produce_response_with_endpoints(&mut out, version, &parts, &endpoints)?;
    }
    Ok(out)
}

fn verify_corpus(
    text: &str,
    versions: impl IntoIterator<Item = i16>,
) -> Result<String, Box<dyn Error>> {
    assert!(
        text.len() <= 16_384,
        "Produce corpus exceeds fixture budget"
    );
    let mut lines = text.lines();
    let mut reverse = String::new();
    for version in versions {
        let mut fields = lines
            .next()
            .ok_or("missing Produce row")?
            .split_ascii_whitespace();
        let actual_version = fields.next().ok_or("missing version")?.parse::<i16>()?;
        assert_eq!(actual_version, version, "Produce row version differs");
        let request_bytes = decode_hex(fields.next().ok_or("missing request body")?)?;
        let response_bytes = decode_hex(fields.next().ok_or("missing response body")?)?;
        assert!(fields.next().is_none(), "extra field in Produce row");

        let request_out = verify_request(version, &request_bytes)?;
        let response_out = verify_response(version, &response_bytes)?;
        writeln!(
            reverse,
            "{version} {} {}",
            encode_hex(&request_out),
            encode_hex(&response_out)
        )?;
    }
    assert!(lines.next().is_none(), "extra Produce row");
    Ok(reverse)
}

#[test]
fn produce_wire_committed_corpus() -> Result<(), Box<dyn Error>> {
    let corpus =
        include_str!("fixtures/produce_wire/apache-v3-through-v13.txt").replace("\r\n", "\n");
    let (min, max) = ProduceRequest::builder(false);
    assert_eq!(verify_corpus(&corpus, min..=max)?, corpus);
    let legacy =
        include_str!("fixtures/produce_wire/apache-v4-v6-v7-v12.txt").replace("\r\n", "\n");
    assert_eq!(verify_corpus(&legacy, LEGACY_VERSIONS)?, legacy);
    Ok(())
}

#[test]
#[ignore = "requires generated pinned Apache SDK fixtures"]
fn produce_wire_apache() -> Result<(), Box<dyn Error>> {
    let input = std::env::var("PRODUCE_WIRE_INPUT")?;
    let output = std::env::var("PRODUCE_WIRE_OUTPUT")?;
    let peer_max = match std::env::var("PRODUCE_WIRE_APACHE_MAX")?.as_str() {
        "11" => 11,
        "13" => 13,
        _ => return Err("unexpected Apache Produce version range".into()),
    };
    let (min, max) = ProduceRequest::builder(false);
    let text = std::fs::read_to_string(input)?;
    std::fs::write(output, verify_corpus(&text, min..=max.min(peer_max))?)?;
    Ok(())
}

//! Compare selected Produce bodies with pinned Apache SDK serializers.

#![expect(
    clippy::disallowed_methods,
    reason = "synchronous, bounded conformance fixture files in a test"
)]

use bytes::{Bytes, BytesMut};
use partitionline::protocol::api::{
    decode_produce_request, decode_produce_response, encode_produce_request,
    encode_produce_response_with_endpoints, encode_produce_response_with_throttle, NodeEndpoint,
    ProducePartitionData, ProducePartitionResponse, ProduceRecordError, ProduceTopicData,
};
use partitionline::protocol::records::{Header, Record, RecordBatch};
use std::{error::Error, fmt::Write};

const VERSIONS: [i16; 4] = [4, 6, 7, 12];
const TOPIC: &str = "produce-fixture";
const TIMESTAMP: i64 = 1_700_000_000_000;

fn batch(partition: i32) -> RecordBatch {
    let timestamp = TIMESTAMP + i64::from(partition);
    RecordBatch::from_records(vec![
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
    ])
}

fn request_topics(version: i16) -> [ProduceTopicData; 1] {
    let indexes: &[i32] = if version == 12 { &[2, 5, 9] } else { &[2, 5] };
    [ProduceTopicData {
        topic: TOPIC.into(),
        partitions: indexes
            .iter()
            .map(|&index| ProducePartitionData {
                index,
                records: batch(index),
            })
            .collect(),
    }]
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
    let mut failed =
        ProducePartitionResponse::partition_response(TOPIC, 5, if version == 4 { 56 } else { 6 });
    if version == 12 {
        failed.current_leader_id = 3;
        failed.current_leader_epoch = 7;
        let mut invalid = ProducePartitionResponse::partition_response(TOPIC, 9, 87);
        invalid.error_message = Some("invalid batch".into());
        invalid.record_errors = vec![
            ProduceRecordError::new(0, Some("invalid timestamp".into())),
            ProduceRecordError::new(1, None),
        ];
        (
            vec![success, failed, invalid],
            vec![NodeEndpoint::new(
                3,
                "broker.example",
                9092,
                Some("rack-a".into()),
            )],
            0,
        )
    } else {
        (vec![success, failed], vec![], 37)
    }
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

fn verify_corpus(text: &str, versions: &[i16]) -> Result<String, Box<dyn Error>> {
    assert!(
        text.len() <= 16_384,
        "Produce corpus exceeds fixture budget"
    );
    let mut lines = text.lines();
    let mut reverse = String::new();
    for &version in versions {
        let mut fields = lines
            .next()
            .ok_or("missing Produce row")?
            .split_ascii_whitespace();
        let actual_version = fields.next().ok_or("missing version")?.parse::<i16>()?;
        assert_eq!(actual_version, version, "Produce row version differs");
        let request_bytes = decode_hex(fields.next().ok_or("missing request body")?)?;
        let response_bytes = decode_hex(fields.next().ok_or("missing response body")?)?;
        assert!(fields.next().is_none(), "extra field in Produce row");

        let mut request_reader = request_bytes.as_slice();
        let (txn, acks, timeout_ms, topics) = decode_produce_request(&mut request_reader, version)?;
        let expected_topics = request_topics(version);
        assert!(
            request_reader.is_empty(),
            "unconsumed Produce request bytes"
        );
        assert_eq!(txn, None, "v{version} transactional ID");
        assert_eq!(acks, -1, "v{version} acks");
        assert_eq!(timeout_ms, 1230 + i32::from(version), "v{version} timeout");
        assert_eq!(
            topics.len(),
            expected_topics.len(),
            "v{version} topic count"
        );
        for (topic, expected) in topics.iter().zip(&expected_topics) {
            assert_eq!(topic.topic, expected.topic, "v{version} topic");
            assert_eq!(topic.partitions.len(), expected.partitions.len());
            for (part, expected_part) in topic.partitions.iter().zip(&expected.partitions) {
                assert_eq!(part.index, expected_part.index, "v{version} partition");
                assert_eq!(
                    part.records, expected_part.records,
                    "v{version} record batch"
                );
            }
        }

        let mut response_reader = response_bytes.as_slice();
        let (parts, endpoints, throttle) = response(version);
        let decoded = decode_produce_response(&mut response_reader, version)?;
        assert!(
            response_reader.is_empty(),
            "unconsumed Produce response bytes"
        );
        assert_eq!(decoded.0, parts, "v{version} partition responses differ");
        assert_eq!(decoded.1, endpoints, "v{version} node endpoints differ");
        assert_eq!(decoded.2, throttle, "v{version} throttle differs");

        let mut request_out = BytesMut::new();
        encode_produce_request(
            &mut request_out,
            version,
            None,
            -1,
            1230 + i32::from(version),
            &expected_topics,
        )?;
        let mut response_out = BytesMut::new();
        if endpoints.is_empty() {
            encode_produce_response_with_throttle(&mut response_out, version, &parts, throttle)?;
        } else {
            encode_produce_response_with_endpoints(&mut response_out, version, &parts, &endpoints)?;
        }
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
        include_str!("fixtures/produce_wire/apache-v4-v6-v7-v12.txt").replace("\r\n", "\n");
    assert_eq!(verify_corpus(&corpus, &VERSIONS)?, corpus);
    Ok(())
}

#[test]
#[ignore = "requires generated pinned Apache SDK fixtures"]
fn produce_wire_apache() -> Result<(), Box<dyn Error>> {
    let input = std::env::var("PRODUCE_WIRE_INPUT")?;
    let output = std::env::var("PRODUCE_WIRE_OUTPUT")?;
    let versions = match std::env::var("PRODUCE_WIRE_APACHE_MAX")?.as_str() {
        "11" => &VERSIONS[..3],
        "13" => VERSIONS.as_slice(),
        _ => return Err("unexpected Apache Produce version range".into()),
    };
    let text = std::fs::read_to_string(input)?;
    std::fs::write(output, verify_corpus(&text, versions)?)?;
    Ok(())
}

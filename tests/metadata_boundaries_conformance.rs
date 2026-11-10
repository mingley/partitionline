//! Bounded Apache SDK comparison for Metadata boundary-version bodies.

#![expect(
    clippy::disallowed_methods,
    reason = "synchronous, bounded conformance fixture files in a test"
)]

use bytes::BytesMut;
use partitionline::protocol::api::{
    decode_metadata_request_topics, decode_metadata_response,
    encode_metadata_request_topics_with_include_cluster_authorized_operations,
    encode_metadata_response, Broker, MetadataRequestTopic, MetadataResponse, PartitionMetadata,
    TopicMetadata,
};
use std::error::Error;

const VERSIONS: [i16; 8] = [0, 1, 3, 8, 9, 10, 11, 12];

fn cases(version: i16) -> Vec<&'static str> {
    let mut cells = vec!["empty", "name"];
    if version >= 1 {
        cells.push("all");
    }
    if version >= 9 {
        cells.push("tagged");
    }
    if version >= 12 {
        cells.push("id");
    }
    cells
}

fn decode_hex(hex: &str) -> Result<Vec<u8>, Box<dyn Error>> {
    let (pairs, remainder) = hex.as_bytes().as_chunks::<2>();
    let bytes = pairs
        .iter()
        .map(|pair| Ok(u8::from_str_radix(std::str::from_utf8(pair)?, 16)?))
        .collect::<Result<Vec<_>, Box<dyn Error>>>()?;
    if !remainder.is_empty() {
        return Err("odd hex length".into());
    }
    Ok(bytes)
}

fn encode_hex(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}

fn topic_id() -> [u8; 16] {
    [0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 17]
}

fn expected_response(version: i16) -> MetadataResponse {
    let omitted = MetadataResponse::AUTHORIZED_OPERATIONS_OMITTED;
    MetadataResponse {
        throttle_time_ms: if version >= 3 { 37 } else { 0 },
        brokers: vec![Broker::new(
            3,
            "broker.example",
            9092,
            (version >= 1).then(|| "rack-a".into()),
        )],
        cluster_id: (version >= 2).then(|| "cluster-α".into()),
        controller_id: if version >= 1 { 3 } else { -1 },
        topics: vec![TopicMetadata::with_topic_id(
            0,
            "café",
            if version >= 10 { topic_id() } else { [0; 16] },
            version >= 1,
            vec![PartitionMetadata::new(
                0,
                0,
                Some(3),
                (version >= 7).then_some(7),
                vec![3, 4],
                vec![3],
                if version >= 5 { vec![4] } else { vec![] },
            )],
            if version >= 8 { 5 } else { omitted },
        )],
        cluster_authorized_operations: if (8..=10).contains(&version) {
            13
        } else {
            omitted
        },
        error_code: 0,
    }
}

fn verify_corpus(text: &str) -> Result<String, Box<dyn Error>> {
    assert!(text.len() <= 32_768);
    let mut lines = text.lines();
    let mut reverse = String::new();
    for version in VERSIONS {
        for cell in cases(version) {
            let mut fields = lines.next().ok_or("missing case")?.split_whitespace();
            let actual_cell = fields.next().ok_or("missing case name")?;
            let request_bytes = decode_hex(fields.next().ok_or("missing request")?)?;
            let response_bytes = decode_hex(fields.next().ok_or("missing response")?)?;
            if fields.next().is_some() {
                return Err("unexpected extra field".into());
            }
            assert_eq!(actual_cell, format!("{version}:{cell}"));
            let expected_topics = match cell {
                "all" => None,
                "empty" => Some(vec![]),
                "name" | "tagged" => Some(vec![MetadataRequestTopic::by_name("café")]),
                "id" => Some(vec![MetadataRequestTopic::by_id(topic_id())]),
                _ => return Err("unexpected case".into()),
            };
            let allow_auto = version < 4 || cell == "all";
            let topic_auth = version >= 8 && cell != "all";
            let cluster_auth = (8..=10).contains(&version) && cell == "name";

            let mut request_reader = request_bytes.as_slice();
            let (topics, actual_auto, actual_topic_auth, actual_cluster_auth) =
                decode_metadata_request_topics(&mut request_reader, version)?;
            assert!(request_reader.is_empty());
            assert_eq!(topics, expected_topics, "{version}:{cell}");
            assert_eq!(actual_auto, allow_auto, "{version}:{cell}");
            assert_eq!(actual_topic_auth, topic_auth, "{version}:{cell}");
            assert_eq!(actual_cluster_auth, cluster_auth, "{version}:{cell}");

            let response = expected_response(version);
            let mut response_reader = response_bytes.as_slice();
            assert_eq!(
                decode_metadata_response(&mut response_reader, version)?,
                response,
                "{version}:{cell}"
            );
            assert!(response_reader.is_empty());

            // Unknown tags are skipped; the Apache SDK checks known fields on re-encoding.
            let mut request_out = BytesMut::new();
            encode_metadata_request_topics_with_include_cluster_authorized_operations(
                &mut request_out,
                version,
                expected_topics.as_deref(),
                allow_auto,
                topic_auth,
                cluster_auth,
            )?;
            let mut response_out = BytesMut::new();
            encode_metadata_response(&mut response_out, version, &response)?;
            reverse.push_str(&format!(
                "{version}:{cell} {} {}\n",
                encode_hex(&request_out),
                encode_hex(&response_out)
            ));
        }
    }
    assert!(lines.next().is_none(), "extra corpus row");
    Ok(reverse)
}

#[test]
fn metadata_boundaries_committed_corpus() -> Result<(), Box<dyn Error>> {
    let input = include_str!("fixtures/metadata_wire/boundaries-apache.txt");
    let expected = include_str!("fixtures/metadata_wire/boundaries-rust.txt");
    assert_eq!(verify_corpus(input)?, expected);
    Ok(())
}

#[test]
#[ignore = "requires generated pinned Apache SDK fixtures"]
fn metadata_boundaries_apache_wire() -> Result<(), Box<dyn Error>> {
    let input = std::env::var("METADATA_BOUNDARY_INPUT")?;
    let output = std::env::var("METADATA_BOUNDARY_OUTPUT")?;
    let text = std::fs::read_to_string(input)?;
    std::fs::write(output, verify_corpus(&text)?)?;
    Ok(())
}

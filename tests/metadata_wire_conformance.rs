//! Bounded Apache SDK comparison for Metadata v13 bodies.

#![expect(
    clippy::disallowed_methods,
    reason = "synchronous, bounded conformance fixture files in a test"
)]

use bytes::BytesMut;
use partitionline::protocol::api::{
    decode_metadata_request_topics, decode_metadata_response, encode_metadata_request_topics,
    encode_metadata_response, Broker, MetadataRequestTopic, MetadataResponse, PartitionMetadata,
    TopicMetadata,
};
use std::error::Error;

const CASES: [&str; 5] = ["all", "empty", "name", "id", "tagged"];
const NESTED_CASES: [&str; 3] = ["by-name", "by-id", "known-empty"];
const INTERNAL_TOPIC_ID: [u8; 16] = ((2u128 << 64) | 23).to_be_bytes();
const MISSING_TOPIC_ID: [u8; 16] = ((3u128 << 64) | 29).to_be_bytes();

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

fn expected_response() -> MetadataResponse {
    MetadataResponse {
        throttle_time_ms: 37,
        brokers: vec![Broker::new(
            3,
            "broker.example",
            9092,
            Some("rack-a".into()),
        )],
        cluster_id: Some("cluster-α".into()),
        controller_id: 3,
        topics: vec![TopicMetadata::with_topic_id(
            0,
            "café",
            topic_id(),
            false,
            vec![PartitionMetadata::new(
                0,
                0,
                Some(3),
                Some(7),
                vec![3, 4],
                vec![3],
                vec![4],
            )],
            5,
        )],
        cluster_authorized_operations: MetadataResponse::AUTHORIZED_OPERATIONS_OMITTED,
        error_code: 31,
    }
}

fn expected_nested_response(cell: &str) -> MetadataResponse {
    let omitted = MetadataResponse::AUTHORIZED_OPERATIONS_OMITTED;
    let mut response = MetadataResponse {
        throttle_time_ms: 0,
        brokers: vec![
            Broker::new(11, "broker-a.example", 9092, Some("rack-a".into())),
            Broker::new(22, "broker-b.example", 9093, None),
        ],
        cluster_id: Some("cluster-α".into()),
        controller_id: 11,
        topics: Vec::new(),
        cluster_authorized_operations: omitted,
        error_code: 0,
    };
    if cell == "known-empty" {
        return response;
    }
    let by_name = cell == "by-name";
    let second = if by_name {
        PartitionMetadata::new(0, 1, Some(22), Some(12), vec![22, 11], vec![22], vec![])
    } else {
        PartitionMetadata::new(5, 1, None, None, vec![22, 11], vec![], vec![22])
    };
    response.topics.push(TopicMetadata::with_topic_id(
        0,
        "café",
        topic_id(),
        false,
        vec![
            PartitionMetadata::new(0, 0, Some(11), Some(11), vec![11, 22], vec![11, 22], vec![]),
            second,
        ],
        if by_name { 5 } else { omitted },
    ));
    if by_name {
        response.topics.push(TopicMetadata::with_topic_id(
            0,
            "__consumer_offsets",
            INTERNAL_TOPIC_ID,
            true,
            vec![PartitionMetadata::new(
                0,
                0,
                Some(22),
                Some(9),
                vec![22, 11],
                vec![22, 11],
                vec![],
            )],
            omitted,
        ));
    } else {
        response.topics.push(TopicMetadata {
            error_code: 100,
            name: None,
            topic_id: MISSING_TOPIC_ID,
            is_internal: false,
            partitions: Vec::new(),
            topic_authorized_operations: omitted,
        });
    }
    response
}

fn verify_corpus(text: &str, cases: &[&str]) -> Result<String, Box<dyn Error>> {
    assert!(text.len() <= 32_768);
    let lines = text.lines().collect::<Vec<_>>();
    assert_eq!(lines.len(), cases.len());
    let mut reverse = String::new();
    for (line, &cell) in lines.iter().zip(cases) {
        let mut fields = line.split_whitespace();
        let actual_cell = fields.next().ok_or("missing case")?;
        let request_bytes = decode_hex(fields.next().ok_or("missing request")?)?;
        let response_bytes = decode_hex(fields.next().ok_or("missing response")?)?;
        if fields.next().is_some() {
            return Err("unexpected extra field".into());
        }
        assert_eq!(actual_cell, cell);
        let expected_topics = match cell {
            "all" => None,
            "empty" | "known-empty" => Some(vec![]),
            "name" => Some(vec![MetadataRequestTopic::by_name("café")]),
            "id" | "tagged" => Some(vec![MetadataRequestTopic::by_id(topic_id())]),
            "by-name" => Some(vec![
                MetadataRequestTopic::by_name("café"),
                MetadataRequestTopic::by_name("__consumer_offsets"),
            ]),
            "by-id" => Some(vec![
                MetadataRequestTopic::by_id(topic_id()),
                MetadataRequestTopic::by_id(MISSING_TOPIC_ID),
            ]),
            _ => return Err("unexpected case".into()),
        };
        let allow_auto = cell == "all";
        let include_topic_auth = matches!(cell, "empty" | "name" | "id" | "tagged" | "by-name");
        let mut request_reader = request_bytes.as_slice();
        let (topics, actual_auto, actual_topic_auth, actual_cluster_auth) =
            decode_metadata_request_topics(&mut request_reader, 13)?;
        assert!(request_reader.is_empty());
        assert_eq!(topics, expected_topics);
        assert_eq!(actual_auto, allow_auto);
        assert_eq!(actual_topic_auth, include_topic_auth);
        assert!(!actual_cluster_auth);
        let mut response_reader = response_bytes.as_slice();
        let response = decode_metadata_response(&mut response_reader, 13)?;
        let expected = if NESTED_CASES.contains(&cell) {
            expected_nested_response(cell)
        } else {
            expected_response()
        };
        assert_eq!(response, expected);
        assert!(response_reader.is_empty());

        // The codec drops unknown tags; Java verifies the re-encoded known fields.
        let mut request_out = BytesMut::new();
        encode_metadata_request_topics(
            &mut request_out,
            13,
            topics.as_deref(),
            actual_auto,
            actual_topic_auth,
        )?;
        let mut response_out = BytesMut::new();
        encode_metadata_response(&mut response_out, 13, &response)?;
        reverse.push_str(&format!(
            "{cell} {} {}\n",
            encode_hex(&request_out),
            encode_hex(&response_out)
        ));
    }
    Ok(reverse)
}

#[test]
fn metadata_v13_committed_corpus() -> Result<(), Box<dyn Error>> {
    let input = include_str!("fixtures/metadata_wire/apache-pairs.txt");
    let expected = include_str!("fixtures/metadata_wire/rust-pairs.txt").replace("\r\n", "\n");
    assert_eq!(verify_corpus(input, &CASES)?, expected);
    Ok(())
}

#[test]
fn metadata_v13_nested_committed_corpus() -> Result<(), Box<dyn Error>> {
    let corpus = include_str!("fixtures/metadata_wire/v13-nested-pairs.txt").replace("\r\n", "\n");
    assert_eq!(verify_corpus(&corpus, &NESTED_CASES)?, corpus);
    Ok(())
}

fn verify_live_corpus(cases: &[&str]) -> Result<(), Box<dyn Error>> {
    let input = std::env::var("METADATA_WIRE_INPUT")?;
    let output = std::env::var("METADATA_WIRE_OUTPUT")?;
    let text = std::fs::read_to_string(input)?;
    std::fs::write(output, verify_corpus(&text, cases)?)?;
    Ok(())
}

#[test]
#[ignore = "requires generated pinned Apache SDK fixtures"]
fn metadata_v13_apache_wire() -> Result<(), Box<dyn Error>> {
    verify_live_corpus(&CASES)
}

#[test]
#[ignore = "requires generated pinned Apache SDK fixtures"]
fn metadata_v13_apache_nested_wire() -> Result<(), Box<dyn Error>> {
    verify_live_corpus(&NESTED_CASES)
}

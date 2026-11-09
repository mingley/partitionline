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

#[test]
#[ignore = "requires generated pinned Apache SDK fixtures"]
fn metadata_v13_apache_wire() -> Result<(), Box<dyn Error>> {
    let input = std::env::var("METADATA_WIRE_INPUT")?;
    let output = std::env::var("METADATA_WIRE_OUTPUT")?;
    let text = std::fs::read_to_string(input)?;
    assert!(text.len() <= 32_768);
    let lines = text.lines().collect::<Vec<_>>();
    assert_eq!(lines.len(), CASES.len());
    let mut reverse = String::new();
    for (line, cell) in lines.iter().zip(CASES) {
        let fields = line.split_whitespace().collect::<Vec<_>>();
        assert_eq!(fields.len(), 3);
        assert_eq!(fields[0], cell);
        let request_bytes = decode_hex(fields[1])?;
        let response_bytes = decode_hex(fields[2])?;
        let expected_topics = match cell {
            "all" => None,
            "empty" => Some(vec![]),
            "name" => Some(vec![MetadataRequestTopic::by_name("café")]),
            "id" | "tagged" => Some(vec![MetadataRequestTopic::by_id(topic_id())]),
            _ => return Err("unexpected case".into()),
        };
        let allow_auto = cell == "all";
        let include_topic_auth = !allow_auto;
        let mut request_reader = request_bytes.as_slice();
        let (topics, actual_auto, actual_topic_auth, actual_cluster_auth) =
            decode_metadata_request_topics(&mut request_reader, 13)?;
        assert!(request_reader.is_empty());
        assert_eq!(topics, expected_topics);
        assert_eq!(actual_auto, allow_auto);
        assert_eq!(actual_topic_auth, include_topic_auth);
        assert!(!actual_cluster_auth);
        let mut response_reader = response_bytes.as_slice();
        assert_eq!(
            decode_metadata_response(&mut response_reader, 13)?,
            expected_response()
        );
        assert!(response_reader.is_empty());

        // The codec drops unknown tags; Java verifies the re-encoded known fields.
        let mut request_out = BytesMut::new();
        encode_metadata_request_topics(
            &mut request_out,
            13,
            expected_topics.as_deref(),
            allow_auto,
            include_topic_auth,
        )?;
        let mut response_out = BytesMut::new();
        encode_metadata_response(&mut response_out, 13, &expected_response())?;
        reverse.push_str(&format!(
            "{cell} {} {}\n",
            encode_hex(&request_out),
            encode_hex(&response_out)
        ));
    }
    std::fs::write(output, reverse)?;
    Ok(())
}

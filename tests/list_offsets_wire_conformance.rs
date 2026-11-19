//! Compare selected ListOffsets bodies with pinned Apache SDK serializers.

#![expect(
    clippy::disallowed_methods,
    reason = "synchronous, bounded conformance fixture files in a test"
)]

use bytes::BytesMut;
use partitionline::protocol::offsets::{
    decode_list_offsets_topics_request, decode_list_offsets_topics_response,
    encode_list_offsets_topics_request_with_replica_id,
    encode_list_offsets_topics_response_with_throttle, ListOffsetsPartition,
    ListOffsetsPartitionRequest, ListOffsetsRequest, ListOffsetsResponsePartition,
    ListOffsetsTopicRequest, ListOffsetsTopicResponse,
};
use std::{error::Error, fmt::Write};

const MAX_VERSION: i16 = 11;
const MAX_CORPUS_BYTES: usize = 32_768;
const TIME: i64 = 1_710_000_000_000;
const OFFSET: i64 = 4_000_000_000;

fn cases(version: i16) -> Vec<&'static str> {
    let mut result = vec!["empty", "queries"];
    if version >= 6 {
        result.push("tagged");
    }
    if matches!(version, 5 | 6 | 11) {
        result.push("error");
    }
    result
}

fn request_topics(version: i16, cell: &str) -> Vec<ListOffsetsTopicRequest> {
    if cell == "empty" {
        return Vec::new();
    }
    let epoch = if version >= 4 { 17 } else { -1 };
    let partition = |index, timestamp| ListOffsetsPartitionRequest::new(index, epoch, timestamp);
    let mut recent = vec![partition(3, -2)];
    let mut archive = vec![partition(7, -1)];
    if cell != "error" {
        recent.push(partition(1, TIME));
        archive.push(partition(2, TIME + 10));
        for (minimum, index, timestamp) in [(7, 9, -3), (8, 11, -4), (9, 15, -5), (11, 17, -6)] {
            if version >= minimum {
                archive.push(partition(index, timestamp));
            }
        }
    }
    vec![
        ListOffsetsTopicRequest::new("orders.recent", recent),
        ListOffsetsTopicRequest::new("orders.archive", archive),
    ]
}

fn responses(
    version: i16,
    cell: &str,
    topics: &[ListOffsetsTopicRequest],
) -> Vec<ListOffsetsTopicResponse> {
    topics
        .iter()
        .map(|topic| {
            let partitions = topic
                .partitions
                .iter()
                .map(|partition| {
                    let index = partition.partition;
                    if cell == "error" {
                        ListOffsetsResponsePartition::error(index, 6)
                    } else if index == 2 {
                        ListOffsetsResponsePartition::error(index, 3)
                    } else {
                        let time = if partition.timestamp >= 0 {
                            partition.timestamp + 1
                        } else {
                            -1
                        };
                        let epoch = if version >= 4 { 18 } else { -1 };
                        ListOffsetsResponsePartition::new(
                            index,
                            ListOffsetsPartition::ok(time, OFFSET + i64::from(index), epoch),
                        )
                    }
                })
                .collect();
            ListOffsetsTopicResponse::new(topic.name.as_str(), partitions)
        })
        .collect()
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

fn verify_corpus(text: &str, max: i16) -> Result<String, Box<dyn Error>> {
    if text.len() > MAX_CORPUS_BYTES || !(9..=MAX_VERSION).contains(&max) {
        return Err("ListOffsets corpus exceeds fixture scope".into());
    }
    let mut lines = text.lines();
    let mut reverse = String::new();
    for version in 1..=max {
        let mut untagged = None;
        for cell in cases(version) {
            let identity = format!("{version}:{cell}");
            let mut fields = lines
                .next()
                .ok_or("missing ListOffsets row")?
                .split_ascii_whitespace();
            assert_eq!(fields.next(), Some(identity.as_str()), "ListOffsets row");
            let request_hex = fields.next().ok_or("missing request")?;
            let response_hex = fields.next().ok_or("missing response")?;
            assert!(fields.next().is_none(), "extra ListOffsets row field");

            let request_bytes = decode_hex(request_hex)?;
            let response_bytes = decode_hex(response_hex)?;
            let mut request_reader = request_bytes.as_slice();
            let mut response_reader = response_bytes.as_slice();
            let topics = request_topics(version, cell);
            let expected_response = responses(version, cell, &topics);
            let replica = if cell == "error" { -2 } else { -1 };
            let timeout = if cell == "empty" {
                0
            } else {
                1250 + i32::from(version)
            };
            let throttle = if version < 2 || cell == "empty" {
                0
            } else if cell == "error" {
                43
            } else {
                30 + i32::from(version)
            };
            let decoded_request = decode_list_offsets_topics_request(&mut request_reader, version)?;
            assert!(request_reader.is_empty(), "unconsumed request {identity}");
            assert_eq!(
                decoded_request,
                (
                    0,
                    topics.clone(),
                    (version >= 10).then_some(timeout),
                    replica
                ),
                "ListOffsets request fields differ {identity}"
            );
            let decoded_response =
                decode_list_offsets_topics_response(&mut response_reader, version)?;
            assert!(response_reader.is_empty(), "unconsumed response {identity}");
            assert_eq!(
                decoded_response,
                (expected_response.clone(), throttle),
                "ListOffsets response fields differ {identity}"
            );

            let mut request_out = BytesMut::new();
            encode_list_offsets_topics_request_with_replica_id(
                &mut request_out,
                version,
                0,
                &topics,
                timeout,
                replica,
            )?;
            let mut response_out = BytesMut::new();
            if cell == "error" {
                ListOffsetsRequest::error_response(
                    &mut response_out,
                    version,
                    &topics,
                    6,
                    throttle,
                )?;
            } else {
                encode_list_offsets_topics_response_with_throttle(
                    &mut response_out,
                    version,
                    &expected_response,
                    throttle,
                )?;
            }

            // The Rust codec intentionally discards unknown tags.
            if cell == "queries" {
                untagged = Some((request_hex, response_hex));
            }
            let (expected_request, expected_response) = if cell == "tagged" {
                untagged.ok_or("missing untagged ListOffsets reference")?
            } else {
                (request_hex, response_hex)
            };
            let request = encode_hex(&request_out);
            let response = encode_hex(&response_out);
            assert_eq!(
                request, expected_request,
                "Rust request bytes differ {identity}"
            );
            assert_eq!(
                response, expected_response,
                "Rust response bytes differ {identity}"
            );
            writeln!(reverse, "{identity} {request} {response}")?;
        }
    }
    assert!(lines.next().is_none(), "extra ListOffsets row");
    Ok(reverse)
}

#[test]
fn list_offsets_wire_committed_corpus() -> Result<(), Box<dyn Error>> {
    let raw = include_str!("fixtures/list_offsets_wire/apache-v1-v11.txt");
    let _encoded = verify_corpus(raw, MAX_VERSION)?;
    Ok(())
}

#[test]
#[ignore = "requires generated pinned Apache SDK fixtures"]
fn list_offsets_wire_apache() -> Result<(), Box<dyn Error>> {
    let input = std::env::var("LIST_OFFSETS_WIRE_INPUT")?;
    let output = std::env::var("LIST_OFFSETS_WIRE_OUTPUT")?;
    let max = std::env::var("LIST_OFFSETS_WIRE_APACHE_MAX")?.parse::<i16>()?;
    let text = std::fs::read_to_string(input)?;
    std::fs::write(output, verify_corpus(&text, max)?)?;
    Ok(())
}

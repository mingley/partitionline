//! Bounded current DescribeQuorum codecs and independent native Admin checks.
use bytes::{Buf, BufMut, Bytes, BytesMut};
use partitionline::protocol::{admin::*, buf};

#[expect(clippy::unwrap_used, reason = "finite malformed-wire test helper")]
fn raw_request(topics: usize, partitions: usize, name: &str) -> Bytes {
    let mut wire = BytesMut::new();
    buf::put_array_len(&mut wire, true, Some(topics)).unwrap();
    for _ in 0..topics {
        buf::put_string(&mut wire, true, Some(name)).unwrap();
        buf::put_array_len(&mut wire, true, Some(partitions)).unwrap();
        for index in 0..partitions {
            wire.put_i32(i32::try_from(index).unwrap());
            buf::put_empty_tagged_fields(&mut wire);
        }
        buf::put_empty_tagged_fields(&mut wire);
    }
    buf::put_empty_tagged_fields(&mut wire);
    wire.freeze()
}

#[test]
fn complete_body_truncations_and_trailing_bytes_are_checked() {
    for version in 0..=2 {
        let request = DescribeQuorumRequest::singleton("__cluster_metadata", 0);
        let mut wire = BytesMut::new();
        encode_describe_quorum_request(&mut wire, version, &request).unwrap();
        for length in 0..wire.len() {
            assert!(decode_describe_quorum_request(&mut &wire[..length], version).is_err());
        }
        assert_eq!(
            decode_describe_quorum_request(&mut wire.clone().freeze(), version).unwrap(),
            request
        );
        wire.put_u8(0);
        assert!(decode_describe_quorum_request(&mut wire.freeze(), version).is_err());
        let response = DescribeQuorumResponse::new(31, Some("denied".into()), vec![], vec![]);
        let mut wire = BytesMut::new();
        encode_describe_quorum_response(&mut wire, version, &response).unwrap();
        for length in 0..wire.len() {
            assert!(decode_describe_quorum_response(&mut &wire[..length], version).is_err());
        }
        wire.put_u8(0);
        assert!(decode_describe_quorum_response(&mut wire.freeze(), version).is_err());
    }
}

#[test]
fn nested_array_limits_apply_before_reservation_and_across_the_tree() {
    assert_eq!(
        decode_describe_quorum_request(&mut raw_request(8192, 0, ""), 2)
            .unwrap()
            .topics
            .len(),
        8192
    );
    assert!(decode_describe_quorum_request(&mut raw_request(8193, 0, ""), 2).is_err());
    let valid = decode_describe_quorum_request(&mut raw_request(4, 8191, "a"), 2).unwrap();
    assert_eq!(
        valid
            .topics
            .iter()
            .map(|topic| topic.partitions.len())
            .sum::<usize>(),
        32764
    );
    assert!(decode_describe_quorum_request(&mut raw_request(4, 8192, "a"), 2).is_err());
    let mut prefix = BytesMut::from(&b"prefix"[..]);
    let excessive = DescribeQuorumRequest {
        topics: vec![DescribeQuorumTopic::new("a", vec![0; 8192]); 4],
    };
    assert!(encode_describe_quorum_request(&mut prefix, 2, &excessive).is_err());
    assert_eq!(&prefix[..], b"prefix");
}

#[test]
fn strings_wire_bytes_and_encoder_helpers_are_bounded() {
    assert!(decode_describe_quorum_request(&mut raw_request(1, 0, &"a".repeat(32767)), 2).is_ok());
    assert!(decode_describe_quorum_request(&mut raw_request(1, 0, &"a".repeat(32768)), 2).is_err());
    assert!(decode_describe_quorum_request(&mut Bytes::from(vec![1; 1024 * 1024 + 1]), 2).is_err());
    let mut output = BytesMut::from(&b"kept"[..]);
    let oversized = "a".repeat(32768);
    assert!(encode_describe_quorum_request(
        &mut output,
        2,
        &DescribeQuorumRequest::singleton(&oversized, 0)
    )
    .is_err());
    assert_eq!(&output[..], b"kept");
    assert!(DescribeQuorumRequest::error_response(&mut output, 2, 31, Some(&oversized)).is_err());
    assert_eq!(&output[..], b"kept");
    let topics = [DescribeQuorumTopic::new("a", vec![0; 1024])];
    assert!(DescribeQuorumRequest::partition_error_response(
        &mut output,
        2,
        &topics,
        31,
        Some(&"x".repeat(1024))
    )
    .is_err());
    assert_eq!(&output[..], b"kept");
    DescribeQuorumRequest::error_response(&mut BytesMut::new(), 0, 31, Some(&oversized)).unwrap();
}

#[test]
fn nullable_error_messages_and_nonnullable_arrays_and_names_stay_distinct() {
    for version in 0..=2 {
        assert!(decode_describe_quorum_request(&mut &b"\0\0"[..], version).is_err());
        assert!(decode_describe_quorum_request(&mut &b"\x02\0\x01\0\0"[..], version).is_err());
    }
    for message in [None, Some(String::new())] {
        let expected = DescribeQuorumResponse::new(0, message, vec![], vec![]);
        let mut body = BytesMut::new();
        encode_describe_quorum_response(&mut body, 2, &expected).unwrap();
        assert_eq!(
            decode_describe_quorum_response(&mut body.freeze(), 2).unwrap(),
            expected
        );
    }
}

#[test]
fn opaque_tags_are_skipped_with_finite_order_count_and_payload_limits() {
    let original = raw_request(1, 1, "alpha");
    let mut tagged = BytesMut::from(&original[..original.len() - 1]);
    tagged.extend_from_slice(&[1, 9, 3, 1, 2, 3]);
    assert_eq!(
        decode_describe_quorum_request(&mut tagged.clone().freeze(), 2).unwrap(),
        DescribeQuorumRequest::singleton("alpha", 0)
    );
    tagged.truncate(tagged.len() - 1);
    assert!(decode_describe_quorum_request(&mut tagged.freeze(), 2).is_err());
    let mut excessive = BytesMut::from(&original[..original.len() - 1]);
    buf::put_unsigned_varint(&mut excessive, 65);
    for tag in 0..65 {
        buf::put_unsigned_varint(&mut excessive, tag);
        buf::put_unsigned_varint(&mut excessive, 0);
    }
    assert!(decode_describe_quorum_request(&mut excessive.freeze(), 2).is_err());
    let mut duplicate = BytesMut::from(&original[..original.len() - 1]);
    duplicate.extend_from_slice(&[2, 9, 0, 9, 0]);
    assert!(decode_describe_quorum_request(&mut duplicate.freeze(), 2).is_err());
}

#[test]
fn available_version_fields_preserve_signed_limits_and_unsigned_ports() {
    for version in 0..=2 {
        let replica = DescribeQuorumReplicaState {
            replica_id: i32::MIN,
            replica_directory_id: if version >= 2 { [7; 16] } else { [0; 16] },
            log_end_offset: i64::MIN,
            last_fetch_timestamp: if version >= 1 { i64::MAX } else { -1 },
            last_caught_up_timestamp: -1,
        };
        let value = DescribeQuorumResponse::new(
            -1,
            if version >= 2 {
                Some(String::new())
            } else {
                None
            },
            vec![DescribeQuorumResult {
                topic: "alpha".into(),
                partitions: vec![DescribeQuorumPartition {
                    partition_index: i32::MAX,
                    error_code: 31,
                    error_message: None,
                    leader_id: -1,
                    leader_epoch: i32::MIN,
                    high_watermark: i64::MAX,
                    current_voters: vec![replica.clone()],
                    observers: vec![replica],
                }],
            }],
            if version >= 2 {
                vec![DescribeQuorumNode {
                    node_id: 0,
                    listeners: vec![DescribeQuorumListener {
                        name: "CONTROLLER".into(),
                        host: "::1".into(),
                        port: u16::MAX,
                    }],
                }]
            } else {
                vec![]
            },
        );
        let mut body = BytesMut::new();
        encode_describe_quorum_response(&mut body, version, &value).unwrap();
        assert_eq!(
            decode_describe_quorum_response(&mut body.freeze(), version).unwrap(),
            value
        );
    }
}

#[tokio::test]
#[ignore = "requires guarded genuine Apache SDK fixtures and reverse parsing"]
async fn actual_sdk_describe_quorum_bodies() {
    let fixtures = std::path::PathBuf::from(std::env::var_os("QUORUM_FIXTURES").unwrap());
    let reverse = std::path::PathBuf::from(std::env::var_os("QUORUM_REVERSE").unwrap());
    tokio::fs::create_dir(&reverse).await.unwrap();
    let mut directory = tokio::fs::read_dir(&fixtures).await.unwrap();
    let mut count = 0;
    while let Some(entry) = directory.next_entry().await.unwrap() {
        let name = entry.file_name().into_string().unwrap();
        if !name.ends_with(".bin") {
            continue;
        }
        let version = name.split('-').nth(1).unwrap().parse::<i16>().unwrap();
        let bytes = tokio::fs::read(entry.path()).await.unwrap();
        let mut body = Bytes::from(bytes);
        let mut output = BytesMut::new();
        if name.ends_with("request.bin") {
            let value = decode_describe_quorum_request(&mut body, version).unwrap();
            assert!(!body.has_remaining());
            encode_describe_quorum_request(&mut output, version, &value).unwrap();
        } else {
            let value = decode_describe_quorum_response(&mut body, version).unwrap();
            assert!(!body.has_remaining());
            encode_describe_quorum_response(&mut output, version, &value).unwrap();
        }
        tokio::fs::write(reverse.join(name), output).await.unwrap();
        count += 1;
    }
    assert_eq!(count, 45, "complete declared current-SDK cohort");
    let large = tokio::fs::read(fixtures.join("policy-array-8193.request"))
        .await
        .unwrap();
    assert!(decode_describe_quorum_request(&mut Bytes::from(large), 2).is_err());
    let maximum = tokio::fs::read(fixtures.join("policy-string-32767.request"))
        .await
        .unwrap();
    assert_eq!(
        decode_describe_quorum_request(&mut Bytes::from(maximum), 2)
            .unwrap()
            .topics[0]
            .topic
            .len(),
        32767
    );
}

fn json_quote(value: &str) -> String {
    format!(
        "\"{}\"",
        value
            .replace('\\', "\\\\")
            .replace('"', "\\\"")
            .replace('\n', "\\n")
            .replace('\r', "\\r")
            .replace('\t', "\\t")
    )
}
fn replica_json(values: &[DescribeQuorumReplicaState]) -> String {
    let rows: Vec<_> = values
        .iter()
        .map(|value| {
            let directory: String = value
                .replica_directory_id
                .iter()
                .map(|byte| format!("{byte:02x}"))
                .collect();
            format!(
                "{{\"id\":{},\"directory\":\"{}\",\"end\":{},\"fetch\":{},\"caught\":{}}}",
                value.replica_id,
                directory,
                value.log_end_offset,
                value.last_fetch_timestamp,
                value.last_caught_up_timestamp
            )
        })
        .collect();
    format!("[{}]", rows.join(","))
}
fn info_json(mut value: partitionline::admin::QuorumInfo) -> String {
    value.nodes.sort_by_key(|node| node.node_id);
    let nodes: Vec<_> = value
        .nodes
        .iter()
        .map(|node| {
            let listeners: Vec<_> = node
                .listeners
                .iter()
                .map(|listener| {
                    format!(
                        "{{\"name\":{},\"host\":{},\"port\":{}}}",
                        json_quote(&listener.name),
                        json_quote(&listener.host),
                        listener.port
                    )
                })
                .collect();
            format!(
                "{{\"id\":{},\"listeners\":[{}]}}",
                node.node_id,
                listeners.join(",")
            )
        })
        .collect();
    format!("{{\"leader\":{},\"epoch\":{},\"watermark\":{},\"voters\":{},\"observers\":{},\"nodes\":[{}]}}",value.leader_id,value.leader_epoch,value.high_watermark,replica_json(&value.voters),replica_json(&value.observers),nodes.join(","))
}

#[tokio::test]
#[ignore = "requires an owned socket peer or native Kafka broker and SDK field checks"]
async fn public_describe_quorum_probe() {
    use std::time::{Duration, Instant};
    let address = std::env::var("QUORUM_ADDRESS").unwrap();
    let mode = std::env::var("QUORUM_MODE").unwrap();
    let output = std::path::PathBuf::from(std::env::var_os("QUORUM_OUTCOME").unwrap());
    let mut config = partitionline::AdminConfig::bootstrap([address]);
    config.request_timeout = Duration::from_millis(1500);
    config.connect_timeout = Duration::from_millis(1000);
    config.retry_backoff = Duration::from_millis(1);
    config.retry_backoff_max = Duration::from_millis(1);
    config.reconnect_backoff = Duration::from_millis(1);
    config.reconnect_backoff_max = Duration::from_millis(1);
    let mut admin = partitionline::Admin::new(config).await.unwrap();
    let started = Instant::now();
    let timeout = Duration::from_millis(
        if matches!(mode.as_str(), "deadline" | "retry-exhaustion") {
            300
        } else {
            1500
        },
    );
    let result = admin.describe_quorum_timeout(timeout).await;
    let elapsed = started.elapsed().as_nanos();
    admin.close().await.unwrap();
    let (info, error, code) = match result {
        Ok(value) => (info_json(value), "null".into(), "null".into()),
        Err(error) => {
            let category = match &error {
                partitionline::Error::Unsupported(_) => "UnsupportedVersionException",
                partitionline::Error::Timeout => "TimeoutException",
                partitionline::Error::Protocol(_) => "UnknownServerException",
                _ => match error.broker_code() {
                    Some(31) => "ClusterAuthorizationException",
                    Some(42) => "InvalidRequestException",
                    Some(6) => "NotLeaderOrFollowerException",
                    Some(41) => "NotControllerException",
                    _ => "other",
                },
            };
            (
                "null".into(),
                json_quote(category),
                error
                    .broker_code()
                    .map_or_else(|| "null".into(), |code| code.to_string()),
            )
        }
    };
    let receipt=format!("{{\"result\":{info},\"failure\":{error},\"broker_code\":{code},\"elapsed_ns\":{elapsed},\"connections_closed\":true}}\n");
    tokio::fs::write(output, receipt).await.unwrap();
}

#[tokio::test]
#[ignore = "requires an owned native Kafka broker and independent SDK parsing"]
async fn native_describe_quorum_raw_history() {
    use partitionline::net::{BrokerConn, Deadline};
    use std::time::Duration;
    let address = std::env::var("QUORUM_ADDRESS").unwrap();
    let directory = std::path::PathBuf::from(std::env::var_os("QUORUM_NATIVE_DIRECTORY").unwrap());
    tokio::fs::create_dir(&directory).await.unwrap();
    let deadline = Deadline::from_timeout(Duration::from_secs(5));
    let mut connection = deadline
        .run(BrokerConn::connect(
            &address,
            "quorum-rust-raw",
            Duration::from_secs(2),
        ))
        .await
        .unwrap();
    for version in 0..=2 {
        let value = DescribeQuorumRequest::singleton("__cluster_metadata", 0);
        let mut request = BytesMut::new();
        encode_describe_quorum_request(&mut request, version, &value).unwrap();
        tokio::fs::write(
            directory.join(format!("native-{version}-request.bin")),
            &request,
        )
        .await
        .unwrap();
        let response = connection
            .roundtrip_deadline(
                55,
                version,
                |output| encode_describe_quorum_request(output, version, &value),
                deadline,
            )
            .await
            .unwrap();
        assert!(response.len() <= 1024 * 1024);
        tokio::fs::write(
            directory.join(format!("native-{version}-response.bin")),
            &response,
        )
        .await
        .unwrap();
        let value = decode_describe_quorum_response(&mut response.clone(), version).unwrap();
        assert_eq!(value.error_code, 0);
        assert_eq!(value.topics.len(), 1);
        assert_eq!(value.topics[0].topic, "__cluster_metadata");
        assert_eq!(value.topics[0].partitions.len(), 1);
        let partition = &value.topics[0].partitions[0];
        assert_eq!(partition.partition_index, 0);
        assert_eq!(partition.error_code, 0);
        assert_eq!(partition.leader_id, 1);
        assert!(partition.high_watermark >= 0);
        assert_eq!(partition.current_voters.len(), 1);
        assert_eq!(partition.current_voters[0].replica_id, 1);
    }
    connection.close();
    drop(connection);
    tokio::fs::write(
        directory.join("receipt.json"),
        b"{\"actual_native_raw_requests\":3,\"versions\":[0,1,2],\"socket_closed\":true}\n",
    )
    .await
    .unwrap();
}

#[test]
fn nonignorable_directory_ids_and_nodes_refuse_older_versions_before_writing() {
    let replica = DescribeQuorumReplicaState {
        replica_id: 1,
        replica_directory_id: [1; 16],
        log_end_offset: 0,
        last_fetch_timestamp: 123,
        last_caught_up_timestamp: 456,
    };
    let partition = DescribeQuorumPartition {
        partition_index: 0,
        error_code: 0,
        error_message: Some("ignored".into()),
        leader_id: 1,
        leader_epoch: 0,
        high_watermark: 0,
        current_voters: vec![replica],
        observers: vec![],
    };
    let mut directory = DescribeQuorumResponse::new(
        0,
        Some("ignored".into()),
        vec![DescribeQuorumResult {
            topic: "__cluster_metadata".into(),
            partitions: vec![partition],
        }],
        vec![],
    );
    let nodes = DescribeQuorumResponse::new(
        0,
        None,
        vec![],
        vec![DescribeQuorumNode {
            node_id: 1,
            listeners: vec![],
        }],
    );
    for version in 0..2 {
        for value in [&directory, &nodes] {
            let mut output = BytesMut::from(&b"untouched"[..]);
            assert!(matches!(
                encode_describe_quorum_response(&mut output, version, value),
                Err(partitionline::Error::Unsupported(_))
            ));
            assert_eq!(&output[..], b"untouched");
        }
    }
    directory.topics[0].partitions[0].current_voters[0].replica_directory_id = [0; 16];
    let mut output = BytesMut::new();
    encode_describe_quorum_response(&mut output, 0, &directory).unwrap();
    let decoded = decode_describe_quorum_response(&mut output.freeze(), 0).unwrap();
    assert_eq!(decoded.error_message, None);
    assert_eq!(
        decoded.topics[0].partitions[0].current_voters[0].last_fetch_timestamp,
        -1
    );
}

#[tokio::test]
async fn committed_current_apache_bodies_decode_offline() {
    let root =
        std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/describe_quorum");
    let mut count = 0;
    for release in ["4.1.2", "4.2.1", "4.3.1"] {
        let mut directory = tokio::fs::read_dir(root.join(release)).await.unwrap();
        while let Some(entry) = directory.next_entry().await.unwrap() {
            let name = entry.file_name().into_string().unwrap();
            if !name.ends_with(".bin") {
                continue;
            }
            let version = name.split('-').nth(1).unwrap().parse::<i16>().unwrap();
            let mut body = Bytes::from(tokio::fs::read(entry.path()).await.unwrap());
            if name.ends_with("request.bin") {
                let _value = decode_describe_quorum_request(&mut body, version).unwrap();
            } else {
                let _value = decode_describe_quorum_response(&mut body, version).unwrap();
            }
            assert!(body.is_empty());
            count += 1;
        }
    }
    assert_eq!(count, 135, "all committed genuine SDK bodies");
}

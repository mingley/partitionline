//! API73 v0: Apache-generated nested bodies and owned controller dispatch.
use bytes::{Buf, Bytes, BytesMut};
use partitionline::protocol::admin::{
    decode_assign_replicas_to_dirs_request, decode_assign_replicas_to_dirs_response,
    encode_assign_replicas_to_dirs_request, encode_assign_replicas_to_dirs_response,
    AssignReplicasToDirsDirectory, AssignReplicasToDirsPartition, AssignReplicasToDirsRequest,
    AssignReplicasToDirsResponse, AssignReplicasToDirsTopic,
};

#[test]
fn reference_assignment_hint_and_error_factory_are_distinct_from_nested_errors() {
    assert_eq!(
        AssignReplicasToDirsRequest::MAX_ASSIGNMENTS_PER_REQUEST,
        2250
    );
    for code in [0, 31, 41, 77, -1] {
        let value = AssignReplicasToDirsRequest::error_response(code);
        assert_eq!(value, AssignReplicasToDirsResponse::new(code, Vec::new()));
        assert_eq!(
            value.error_counts(),
            std::collections::HashMap::from([(code, 1)])
        );
    }
}

#[test]
fn truncated_empty_messages_and_untrusted_array_counts_are_rejected() {
    let mut body = BytesMut::new();
    encode_assign_replicas_to_dirs_request(
        &mut body,
        &AssignReplicasToDirsRequest::new(1, -1, Vec::new()),
    )
    .unwrap();
    for length in 0..body.len() {
        assert!(decode_assign_replicas_to_dirs_request(&mut &body[..length]).is_err());
    }
    // Compact array length 8194 encodes 8193 elements, above the decoder budget.
    body.truncate(12);
    body.extend_from_slice(&[0x82, 0x40, 0]);
    assert!(decode_assign_replicas_to_dirs_request(&mut body.freeze()).is_err());
    let mut response = BytesMut::new();
    encode_assign_replicas_to_dirs_response(
        &mut response,
        &AssignReplicasToDirsResponse::new(41, Vec::new()),
    )
    .unwrap();
    for length in 0..response.len() {
        assert!(decode_assign_replicas_to_dirs_response(&mut &response[..length]).is_err());
    }
}

#[tokio::test]
#[ignore = "requires guarded authentic Apache fixtures and independent reverse parsing"]
async fn actual_sdk_assign_replicas_to_dirs_bodies() {
    let fixtures = std::path::PathBuf::from(std::env::var_os("ASSIGN_DIRS_FIXTURES").unwrap());
    let reverse = std::path::PathBuf::from(std::env::var_os("ASSIGN_DIRS_REVERSE").unwrap());
    tokio::fs::create_dir(&reverse).await.unwrap();
    let mut files = tokio::fs::read_dir(fixtures).await.unwrap();
    let mut count = 0;
    while let Some(item) = files.next_entry().await.unwrap() {
        let path = item.path();
        if path.extension().and_then(|v| v.to_str()) != Some("bin") {
            continue;
        }
        let name = path.file_name().unwrap().to_str().unwrap();
        let data = tokio::fs::read(&path).await.unwrap();
        assert!(data.len() <= 65536);
        for length in 0..data.len() {
            if name.ends_with("request.bin") {
                assert!(decode_assign_replicas_to_dirs_request(&mut &data[..length]).is_err());
            } else {
                assert!(decode_assign_replicas_to_dirs_response(&mut &data[..length]).is_err());
            }
        }
        let mut cursor = Bytes::from(data);
        let mut encoded = BytesMut::new();
        if name.ends_with("request.bin") {
            let value = decode_assign_replicas_to_dirs_request(&mut cursor).unwrap();
            encode_assign_replicas_to_dirs_request(&mut encoded, &value).unwrap();
        } else {
            let value = decode_assign_replicas_to_dirs_response(&mut cursor).unwrap();
            assert_eq!(
                value.error_counts(),
                std::collections::HashMap::from([(value.error_code, 1)])
            );
            encode_assign_replicas_to_dirs_response(&mut encoded, &value).unwrap();
        }
        assert!(!cursor.has_remaining());
        tokio::fs::write(reverse.join(name), encoded).await.unwrap();
        count += 1;
    }
    assert_eq!(count, 23);
}

fn uuid_from_environment(name: &str) -> [u8; 16] {
    let text = std::env::var(name).unwrap();
    assert_eq!(text.len(), 32);
    std::array::from_fn(|i| u8::from_str_radix(&text[2 * i..2 * i + 2], 16).unwrap())
}

#[tokio::test]
#[ignore = "requires an owned current Kafka controller, topic and independent SDK parsing"]
async fn native_assign_replicas_to_dirs_raw_history() {
    use partitionline::net::{BrokerConn, Deadline};
    use partitionline::protocol::api::{decode_api_versions_response, encode_api_versions_request};
    use std::time::Duration;
    let address = std::env::var("ASSIGN_DIRS_CONTROLLER").unwrap();
    let epoch: i64 = std::env::var("ASSIGN_DIRS_BROKER_EPOCH")
        .unwrap()
        .parse()
        .unwrap();
    let directory_id = uuid_from_environment("ASSIGN_DIRS_DIRECTORY_ID");
    let topic_id = uuid_from_environment("ASSIGN_DIRS_TOPIC_ID");
    let output = std::path::PathBuf::from(std::env::var_os("ASSIGN_DIRS_OUTPUT").unwrap());
    tokio::fs::create_dir(&output).await.unwrap();
    let deadline = Deadline::from_timeout(Duration::from_secs(5));
    let mut conn = deadline
        .run(BrokerConn::connect(
            &address,
            "assign-dirs-rust",
            Duration::from_secs(2),
        ))
        .await
        .unwrap();
    let body = conn
        .roundtrip_deadline(
            18,
            0,
            |buf| encode_api_versions_request(buf, 0, "partitionline", "0.1.0"),
            deadline,
        )
        .await
        .unwrap();
    assert!(body.len() <= 65536);
    let versions = decode_api_versions_response(&mut body.clone(), 0).unwrap();
    assert_eq!(versions.error_code, 0);
    assert!(versions
        .api_keys
        .iter()
        .any(|v| v.api_key == 73 && v.min_version <= 0 && v.max_version >= 0));
    for i in 0..9 {
        let (id, requested_epoch) = match i {
            1 => (99, 0),
            2 => (1, -1),
            _ => (1, epoch),
        };
        let directories = match i {
            3..=8 => {
                let topics = if i == 4 {
                    Vec::new()
                } else {
                    let parts = match i {
                        3 => vec![0, -1, i32::MAX],
                        5 => Vec::new(),
                        6 => vec![99],
                        8 => vec![0, 0],
                        _ => vec![0],
                    };
                    vec![AssignReplicasToDirsTopic::new(
                        if i <= 5 { [0; 16] } else { topic_id },
                        parts
                            .into_iter()
                            .map(AssignReplicasToDirsPartition::new)
                            .collect(),
                    )]
                };
                vec![AssignReplicasToDirsDirectory::new(directory_id, topics)]
            }
            _ => Vec::new(),
        };
        let request = AssignReplicasToDirsRequest::new(id, requested_epoch, directories);
        let mut body = BytesMut::new();
        encode_assign_replicas_to_dirs_request(&mut body, &request).unwrap();
        tokio::fs::write(output.join(format!("live-{i}-request.bin")), body)
            .await
            .unwrap();
        let body = conn
            .roundtrip_deadline(
                73,
                0,
                |buf| encode_assign_replicas_to_dirs_request(buf, &request),
                deadline,
            )
            .await
            .unwrap();
        assert!(body.len() <= 65536);
        tokio::fs::write(output.join(format!("live-{i}-response.bin")), &body)
            .await
            .unwrap();
        let mut cursor = body;
        let response = decode_assign_replicas_to_dirs_response(&mut cursor).unwrap();
        assert!(!cursor.has_remaining());
        assert_eq!(response.error_code, if i == 1 || i == 2 { 77 } else { 0 });
        if response.error_code == 0 {
            assert_eq!(response.directories.len(), request.directories.len());
            for (got, sent) in response.directories.iter().zip(&request.directories) {
                assert_eq!(got.id, sent.id);
                assert_eq!(got.topics.len(), sent.topics.len());
                for (got, sent) in got.topics.iter().zip(&sent.topics) {
                    assert_eq!(got.topic_id, sent.topic_id);
                    assert_eq!(got.partitions.len(), sent.partitions.len());
                    for (got, sent) in got.partitions.iter().zip(&sent.partitions) {
                        assert_eq!(got.partition_index, sent.partition_index);
                        assert_eq!(
                            got.error_code,
                            match i {
                                3 => 100,
                                6 => 3,
                                _ => 0,
                            }
                        );
                    }
                }
            }
        } else {
            assert!(response.directories.is_empty());
        }
    }
    conn.close();
    drop(conn);
    let mut config =
        partitionline::AdminConfig::bootstrap(vec![std::env::var("ASSIGN_DIRS_BROKER").unwrap()]);
    config.request_timeout = Duration::from_secs(3);
    let mut admin = partitionline::Admin::new(config).await.unwrap();
    assert!(matches!(
        admin.assign_replicas_to_dirs(1, epoch, Vec::new()).await,
        Err(partitionline::Error::Unsupported(_))
    ));
    admin.close().await.unwrap();
    tokio::fs::write(output.join("receipt.json"), b"{\"actual_controller_requests\":9,\"socket_closed\":true,\"ordinary_broker_Admin_policy\":\"Unsupported\"}\n").await.unwrap();
}

//! Bounded topic identities in offset requests and responses.
#![expect(clippy::unwrap_used, reason = "finite actual SDK fixture assertions")]
use partitionline::protocol::group::*;

#[test]
fn wrong_identity_cannot_be_encoded_as_a_different_wire_layout() {
    let mut request = OffsetCommitRequestData {
        group_id: "g".into(),
        topics: vec![OffsetCommitTopicData {
            identity: OffsetTopicIdentity::Name("topic".into()),
            ..Default::default()
        }],
        ..Default::default()
    };
    assert!(encode_offset_commit_request_data(&request, 10, OffsetLimits::default()).is_err());
    request.topics[0].identity = OffsetTopicIdentity::Id([1; 16]);
    assert!(encode_offset_commit_request_data(&request, 9, OffsetLimits::default()).is_err());
    let body = encode_offset_commit_request_data(&request, 10, OffsetLimits::default()).unwrap();
    assert_eq!(
        decode_offset_commit_request_data(&body, 10, OffsetLimits::default()).unwrap(),
        request
    );
}
#[test]
fn count_string_body_and_requested_storage_limits_precede_allocations() {
    let request = OffsetFetchRequestData {
        groups: vec![OffsetFetchGroupData {
            group_id: "group".into(),
            ..Default::default()
        }],
        ..Default::default()
    };
    for limits in [
        OffsetLimits {
            array_elements: 0,
            ..Default::default()
        },
        OffsetLimits {
            string_bytes: 2,
            ..Default::default()
        },
        OffsetLimits {
            decoded_bytes: 1,
            ..Default::default()
        },
        OffsetLimits {
            wire_bytes: 1,
            ..Default::default()
        },
    ] {
        assert!(encode_offset_fetch_request_data(&request, 10, limits).is_err());
    }
    let hostile = [0xff, 0xff, 0xff, 0xff, 0x07, 0, 0];
    assert!(decode_offset_fetch_request_data(&hostile, 10, OffsetLimits::default()).is_err());
    let raw = encode_offset_fetch_request_data(&request, 10, OffsetLimits::default()).unwrap();
    for count in 0..raw.len() {
        assert!(
            decode_offset_fetch_request_data(&raw[..count], 10, OffsetLimits::default()).is_err()
        );
    }
    let mut extra = raw;
    extra.push(0);
    assert!(decode_offset_fetch_request_data(&extra, 10, OffsetLimits::default()).is_err());
}
#[test]
fn membership_and_nullable_metadata_are_not_silently_dropped() {
    let mut request = OffsetFetchRequestData {
        groups: vec![OffsetFetchGroupData {
            group_id: "g".into(),
            member_id: Some(String::new()),
            member_epoch: 7,
            ..Default::default()
        }],
        ..Default::default()
    };
    assert!(encode_offset_fetch_request_data(&request, 8, OffsetLimits::default()).is_err());
    request.groups[0].member_id = None;
    request.groups[0].member_epoch = -1;
    let raw = encode_offset_fetch_request_data(&request, 8, OffsetLimits::default()).unwrap();
    assert_eq!(
        decode_offset_fetch_request_data(&raw, 8, OffsetLimits::default()).unwrap(),
        request
    );
}

#[test]
fn bounded_unknown_tags_are_discarded_and_malformed_tags_are_rejected() {
    let request = OffsetFetchRequestData::default();
    let mut raw = encode_offset_fetch_request_data(&request, 10, OffsetLimits::default()).unwrap();
    assert_eq!(raw.pop(), Some(0));
    let untagged = raw.clone();
    raw.extend_from_slice(&[1, 7, 2, 99, 100]);
    assert_eq!(
        decode_offset_fetch_request_data(&raw, 10, OffsetLimits::default()).unwrap(),
        request
    );
    assert!(decode_offset_fetch_request_data(
        &raw,
        10,
        OffsetLimits {
            tag_bytes: 1,
            ..Default::default()
        }
    )
    .is_err());
    for tags in [
        vec![2, 7, 0, 7, 0],
        vec![2, 7, 0, 6, 0],
        vec![1, 7, 2, 1],
        vec![255, 255, 255, 255, 7],
    ] {
        let mut body = untagged.clone();
        body.extend(tags);
        assert!(decode_offset_fetch_request_data(&body, 10, OffsetLimits::default()).is_err());
    }
}

#[tokio::test]
#[ignore = "requires generated pinned SDK fixtures and owned runner"]
async fn actual_sdk_offset_id_bodies() {
    let root = std::path::PathBuf::from(std::env::var_os("OFFSET_TOPIC_IDS_FIXTURES").unwrap());
    let proof = std::path::PathBuf::from(std::env::var_os("OFFSET_TOPIC_IDS_REVERSE").unwrap());
    tokio::fs::create_dir(&proof).await.unwrap();
    for version in [8, 9, 10] {
        for mode in ["full", "empty", "null-empty", "null-topics", "errors"] {
            let name = format!("v{version}-{mode}");

            let c = decode_offset_commit_request_data(
                &read_fixture(&root, &name, "commit-request").await,
                version,
                OffsetLimits::default(),
            )
            .unwrap();
            let cr = decode_offset_commit_response_data(
                &read_fixture(&root, &name, "commit-response").await,
                version,
                OffsetLimits::default(),
            )
            .unwrap();
            let f = decode_offset_fetch_request_data(
                &read_fixture(&root, &name, "fetch-request").await,
                version,
                OffsetLimits::default(),
            )
            .unwrap();
            let fr = decode_offset_fetch_response_data(
                &read_fixture(&root, &name, "fetch-response").await,
                version,
                OffsetLimits::default(),
            )
            .unwrap();
            assert_eq!(c.group_id, "group-κ");
            assert_eq!(c.member_id, "member-λ");
            assert_eq!(c.generation_id_or_member_epoch, 7);
            assert_eq!(cr.throttle_time_ms, 42);
            assert_eq!(fr.throttle_time_ms, 42);
            assert!(f.require_stable);
            assert_eq!(f.groups.len(), 2);
            assert_eq!(fr.groups[0].group_id, "group-κ-1");
            if mode != "empty" {
                for (index, topic) in c.topics.iter().enumerate() {
                    let number = i64::try_from(index).unwrap();
                    let expected = if version == 10 {
                        let mut id = [0; 16];
                        id[..8].copy_from_slice(&(number + 1).to_be_bytes());
                        id[8..].copy_from_slice(&(number + 17).to_be_bytes());
                        OffsetTopicIdentity::Id(id)
                    } else {
                        OffsetTopicIdentity::Name(format!("topic-κ-{index}"))
                    };
                    assert_eq!(topic.identity, expected);
                    let partition = &topic.partitions[0];
                    assert_eq!(
                        partition.committed_offset,
                        if index == 0 { i64::MAX } else { -1 }
                    );
                    assert_eq!(
                        partition.committed_leader_epoch,
                        if index == 0 { i32::MAX } else { -1 }
                    );
                    assert_eq!(
                        partition.committed_metadata,
                        if mode == "null-empty" {
                            if index == 0 {
                                None
                            } else {
                                Some(String::new())
                            }
                        } else {
                            Some("metadata-λ".into())
                        }
                    );
                }
            } else {
                assert!(c.topics.is_empty());
                assert!(cr.topics.is_empty());
            }
            if mode == "null-topics" {
                assert!(f.groups.iter().all(|g| g.topics.is_none()));
            }
            for (kind, bytes) in [
                (
                    "commit-request",
                    encode_offset_commit_request_data(&c, version, OffsetLimits::default())
                        .unwrap(),
                ),
                (
                    "commit-response",
                    encode_offset_commit_response_data(&cr, version, OffsetLimits::default())
                        .unwrap(),
                ),
                (
                    "fetch-request",
                    encode_offset_fetch_request_data(&f, version, OffsetLimits::default()).unwrap(),
                ),
                (
                    "fetch-response",
                    encode_offset_fetch_response_data(&fr, version, OffsetLimits::default())
                        .unwrap(),
                ),
            ] {
                assert_eq!(bytes, read_fixture(&root, &name, kind).await);
                tokio::fs::write(proof.join(format!("{name}.{kind}.bin")), bytes)
                    .await
                    .unwrap();
            }
        }
    }
}

async fn read_fixture(root: &std::path::Path, name: &str, kind: &str) -> Vec<u8> {
    tokio::fs::read(root.join(format!("{name}.{kind}.bin")))
        .await
        .unwrap()
}

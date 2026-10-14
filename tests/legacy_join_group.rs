//! KL05-42: JoinGroup v0/v1 classic membership and bounded fallback.
//!
//! Wire leftover-empty and Apache serializer/parser fixtures are the
//! independent protocol reference. `tests/common` Mock public
//! `ConsumerGroup::join` runs are labeled local and are not an Apache
//! oracle. A successful v0/v1 lifecycle is not inferred from a field
//! roundtrip.

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing,
    reason = "integration-test helpers; clippy.toml allow-*-in-tests covers #[test] only"
)]

mod common;

use std::path::{Path, PathBuf};
use std::time::Duration;

use bytes::{BufMut, BytesMut};
use partitionline::protocol::api_keys::{pick_version, JOIN_GROUP};
use partitionline::protocol::group::{
    decode_join_group_request, decode_join_group_request_protocols, decode_join_group_response,
    encode_join_group_request, encode_join_group_response, JoinGroupRequest, JoinMember,
};
use partitionline::{ConsumerConfig, ConsumerGroup, Error};

/// Official JSON v1/v2 request: group `g`, session/rebalance 10000, member
/// `m1`, protocol type `consumer`, one protocol `range` metadata `[1,2,3]`.
const REQ_V1: &[u8] = &[
    0x00, 0x01, 0x67, 0x00, 0x00, 0x27, 0x10, 0x00, 0x00, 0x27, 0x10, 0x00, 0x02, 0x6d, 0x31, 0x00,
    0x08, 0x63, 0x6f, 0x6e, 0x73, 0x75, 0x6d, 0x65, 0x72, 0x00, 0x00, 0x00, 0x01, 0x00, 0x05, 0x72,
    0x61, 0x6e, 0x67, 0x65, 0x00, 0x00, 0x00, 0x03, 0x01, 0x02, 0x03,
];

/// v0 omits RebalanceTimeoutMs after SessionTimeoutMs.
const REQ_V0: &[u8] = &[
    0x00, 0x01, 0x67, 0x00, 0x00, 0x27, 0x10, 0x00, 0x02, 0x6d, 0x31, 0x00, 0x08, 0x63, 0x6f, 0x6e,
    0x73, 0x75, 0x6d, 0x65, 0x72, 0x00, 0x00, 0x00, 0x01, 0x00, 0x05, 0x72, 0x61, 0x6e, 0x67, 0x65,
    0x00, 0x00, 0x00, 0x03, 0x01, 0x02, 0x03,
];

fn fixture_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/legacy-join-group")
}

fn leftover(buf: &[u8], what: &str) {
    assert!(buf.is_empty(), "{what}: leftover {} bytes", buf.len());
}

fn canonical_request() -> JoinGroupRequest<'static> {
    JoinGroupRequest {
        group_id: "g",
        session_timeout_ms: 10_000,
        rebalance_timeout_ms: 10_000,
        member_id: "m1",
        group_instance_id: Some("ignored-below-v5"),
        protocol_type: "consumer",
        protocol_name: "range",
        metadata: &[1, 2, 3],
        reason: None,
    }
}

fn canonical_members() -> [JoinMember; 1] {
    [JoinMember {
        member_id: "m1".into(),
        group_instance_id: None,
        metadata: vec![1, 2, 3],
    }]
}

#[test]
fn join_group_v0_v1_request_response_leftover_empty() {
    let req = canonical_request();
    let mut v0 = BytesMut::new();
    encode_join_group_request(&mut v0, 0, &req).unwrap();
    assert_eq!(&v0[..], REQ_V0);
    let mut cur = v0.as_ref();
    let (gid, member, instance, protocols, reason, session, rebalance, ptype) =
        decode_join_group_request_protocols(&mut cur, 0).unwrap();
    leftover(cur, "v0 request");
    assert_eq!(
        (gid.as_str(), member.as_str(), ptype.as_str()),
        ("g", "m1", "consumer")
    );
    assert!(instance.is_none());
    assert_eq!(reason, None);
    assert_eq!(session, 10_000);
    assert_eq!(rebalance, session, "v0 fills rebalance from session");
    assert_eq!(protocols[0].name, "range");
    assert_eq!(protocols[0].metadata, vec![1, 2, 3]);

    let mut v1 = BytesMut::new();
    encode_join_group_request(&mut v1, 1, &req).unwrap();
    assert_eq!(&v1[..], REQ_V1);
    let mut cur = v1.as_ref();
    let (_, _, _, _, _, session, rebalance, _) =
        decode_join_group_request_protocols(&mut cur, 1).unwrap();
    leftover(cur, "v1 request");
    assert_eq!((session, rebalance), (10_000, 10_000));

    let members = canonical_members();
    let mut resp0 = BytesMut::new();
    encode_join_group_response(&mut resp0, 0, 0, 7, "range", "l", "m1", &members).unwrap();
    let mut resp1 = BytesMut::new();
    encode_join_group_response(&mut resp1, 1, 0, 7, "range", "l", "m1", &members).unwrap();
    assert_eq!(resp0.as_ref(), resp1.as_ref(), "v0 response matches v1");
    assert_eq!(resp0[0..2], 0i16.to_be_bytes(), "v0 ErrorCode at bytes 0-1");
    for version in [0_i16, 1] {
        let mut cur = resp0.as_ref();
        let (err, gen, proto, leader, mid, skip, got, ptype, throttle) =
            decode_join_group_response(&mut cur, version).unwrap();
        leftover(cur, &format!("v{version} response"));
        assert_eq!(
            (
                err,
                gen,
                proto.as_str(),
                leader.as_str(),
                mid.as_str(),
                skip,
                throttle
            ),
            (0, 7, "range", "l", "m1", false, 0)
        );
        assert!(ptype.is_none());
        assert_eq!(got[0].metadata, vec![1, 2, 3]);
    }
}

#[test]
fn join_group_v0_v1_malformed_counts_metadata_and_truncation() {
    let mut short = BytesMut::new();
    short.put_i16(1);
    short.put_u8(b'g');
    short.put_i16(0);
    for version in [0_i16, 1] {
        let mut cur = short.as_ref();
        assert!(
            decode_join_group_request_protocols(&mut cur, version).is_err(),
            "truncated session timeout must fail on v{version}"
        );
    }

    let mut missing_meta = BytesMut::new();
    encode_join_group_request(&mut missing_meta, 0, &canonical_request()).unwrap();
    missing_meta.truncate(missing_meta.len() - 2);
    let mut cur = missing_meta.as_ref();
    assert!(
        decode_join_group_request_protocols(&mut cur, 0).is_err(),
        "truncated protocol metadata must fail on v0"
    );

    let mut count_two = BytesMut::from(REQ_V1);
    let count_at = 3 + 4 + 4 + 4 + 10;
    count_two[count_at..count_at + 4].copy_from_slice(&2i32.to_be_bytes());
    let mut cur = count_two.as_ref();
    assert!(
        decode_join_group_request_protocols(&mut cur, 1).is_err(),
        "Protocols count 2 with one body must fail on v1"
    );

    let mut resp = BytesMut::new();
    encode_join_group_response(&mut resp, 1, 0, 7, "range", "l", "m1", &canonical_members())
        .unwrap();
    resp.truncate(1);
    for version in [0_i16, 1] {
        let mut cur = resp.as_ref();
        assert!(
            decode_join_group_response(&mut cur, version).is_err(),
            "truncated ErrorCode must fail on v{version}"
        );
    }
}

#[test]
fn join_group_v0_v1_static_and_disjoint_fail_closed() {
    assert!(JoinGroupRequest::build(0, Some("static")).is_err());
    assert!(JoinGroupRequest::build(1, Some("static")).is_err());
    assert!(JoinGroupRequest::build(0, Some("")).is_err());
    assert!(JoinGroupRequest::build(0, None).is_ok());
    assert!(JoinGroupRequest::build(1, None).is_ok());
    assert_eq!(pick_version(0, 0, 0, 9), Some(0));
    assert_eq!(pick_version(1, 1, 0, 9), Some(1));
    assert_eq!(pick_version(10, 10, 0, 9), None);
    assert_eq!(pick_version(0, 1, 2, 9), None);
    assert!(
        encode_join_group_request(&mut BytesMut::new(), 10, &canonical_request()).is_err(),
        "v10+ is not spoken"
    );
}

#[test]
fn apache_join_group_v0_v1_bodies_reverse_parse_leftover_empty() {
    let root = fixture_root();
    let manifest = std::fs::read_to_string(root.join("manifest.json")).unwrap();
    assert!(
        manifest.contains("KL05-42"),
        "fixture manifest must name KL05-42"
    );
    let mut parsed = 0u32;
    let mut receipts = 0u32;
    for release in ["3.9.1", "4.1.0", "4.1.2", "4.2.1", "4.3.1"] {
        let dir = root.join(release);
        for version in [0_i16, 1] {
            let req = dir.join(format!("v{version}-request-canonical.bin"));
            let resp = dir.join(format!("v{version}-response-canonical.bin"));
            let err = dir.join(format!("v{version}-response-error.bin"));
            if req.is_file() {
                parse_apache_request(&req, version, release);
                parsed += 1;
            }
            if resp.is_file() {
                parse_apache_response(&resp, version, release, false);
                parsed += 1;
            }
            if err.is_file() {
                parse_apache_response(&err, version, release, true);
                parsed += 1;
            }
            let receipt = dir.join(format!("v{version}-unsupported.json"));
            if receipt.is_file() {
                let body = std::fs::read_to_string(&receipt).unwrap();
                assert!(body.contains("\"disposition\": \"unsupported\""));
                assert!(body.contains(&format!("\"version\": {version}")));
                receipts += 1;
            }
            let static_receipt = dir.join(format!("v{version}-static-unsupported.json"));
            if static_receipt.is_file() {
                let body = std::fs::read_to_string(&static_receipt).unwrap();
                assert!(body.contains("\"disposition\": \"unsupported\""));
                receipts += 1;
            }
        }
    }
    assert!(
        parsed >= 6,
        "need Apache v0/v1 bodies from at least one speaking pin, parsed {parsed}"
    );
    assert!(
        receipts >= 2,
        "need retained unsupported receipts, got {receipts}"
    );
}

fn parse_apache_request(path: &Path, version: i16, release: &str) {
    let bytes = std::fs::read(path).unwrap();
    let mut cur = bytes.as_slice();
    let (gid, member, instance, meta, reason) = decode_join_group_request(&mut cur, version)
        .unwrap_or_else(|e| panic!("{} v{version} request: {e}", path.display()));
    leftover(cur, &format!("{release} v{version} request"));
    assert_eq!((gid.as_str(), member.as_str()), ("g", "m1"));
    assert!(instance.is_none());
    assert_eq!(meta, vec![1, 2, 3]);
    assert_eq!(reason, None);
    if version == 0 {
        assert_eq!(bytes, REQ_V0, "{release} v0 request must match Apache layout");
    } else {
        assert_eq!(bytes, REQ_V1, "{release} v1 request must match Apache layout");
    }
}

fn parse_apache_response(path: &Path, version: i16, release: &str, error: bool) {
    let bytes = std::fs::read(path).unwrap();
    let mut cur = bytes.as_slice();
    let (err, gen, proto, leader, mid, skip, members, ptype, throttle) =
        decode_join_group_response(&mut cur, version)
            .unwrap_or_else(|e| panic!("{} v{version} response: {e}", path.display()));
    leftover(cur, &format!("{release} v{version} response"));
    assert_eq!(bytes[0..2], err.to_be_bytes(), "ErrorCode at bytes 0-1");
    assert!(!skip);
    assert!(ptype.is_none());
    assert_eq!(throttle, 0);
    if error {
        assert_ne!(err, 0);
        assert_eq!(gen, JoinGroupRequest::UNKNOWN_GENERATION_ID);
        assert!(members.is_empty());
    } else {
        assert_eq!(err, 0);
        assert_eq!(
            (gen, proto.as_str(), leader.as_str(), mid.as_str()),
            (7, "range", "l", "m1")
        );
        assert_eq!(members[0].metadata, vec![1, 2, 3]);
    }
}

fn group_cfg(addr: &str) -> ConsumerConfig {
    let mut cfg = ConsumerConfig::bootstrap([addr.to_owned()]);
    cfg.max_wait_ms = 10;
    cfg.request_timeout = Duration::from_secs(5);
    cfg.heartbeat_interval = Duration::from_millis(50);
    cfg
}

#[tokio::test]
async fn local_mock_public_join_on_join_group_v0_only() {
    let mock = common::Mock::start().await;
    mock.set_api_range(JOIN_GROUP, 0, 0);
    let mut group = ConsumerGroup::join(group_cfg(&mock.addr), "jg0", "t")
        .await
        .unwrap();
    assert_eq!(
        mock.last_join_group_version(),
        Some(0),
        "local Mock: public join must speak JoinGroup v0 on a 0-only peer"
    );
    assert_eq!(
        mock.join_group_calls(),
        1,
        "local Mock: v0 joins in one RPC (requiresKnownMemberId is false)"
    );
    assert!(!group.assignment().is_empty());
    group.leave().await.unwrap();
}

#[tokio::test]
async fn local_mock_public_join_on_join_group_v1_only() {
    let mock = common::Mock::start().await;
    mock.set_api_range(JOIN_GROUP, 1, 1);
    let mut group = ConsumerGroup::join(group_cfg(&mock.addr), "jg1", "t")
        .await
        .unwrap();
    assert_eq!(
        mock.last_join_group_version(),
        Some(1),
        "local Mock: public join must speak JoinGroup v1 on a 1-only peer"
    );
    assert_eq!(
        mock.join_group_calls(),
        1,
        "local Mock: v1 joins in one RPC (requiresKnownMemberId is false)"
    );
    assert!(!group.assignment().is_empty());
    group.leave().await.unwrap();
}

#[tokio::test]
async fn local_mock_static_membership_sends_zero_join_group_frames() {
    for (min, max) in [(0, 0), (1, 1)] {
        let mock = common::Mock::start().await;
        mock.set_api_range(JOIN_GROUP, min, max);
        let mut cfg = group_cfg(&mock.addr);
        cfg.group_instance_id = Some("static-legacy".into());
        let err = ConsumerGroup::join(cfg, "jg-static", "t")
            .await
            .err()
            .unwrap();
        assert!(
            matches!(err, Error::Unsupported(_)),
            "local Mock: static membership on v{min}-{max} must fail closed, got {err}"
        );
        assert!(
            err.to_string()
                .contains("does not support usage of config group.instance.id"),
            "got {err}"
        );
        assert_eq!(
            mock.join_group_calls(),
            0,
            "local Mock: static v{min} must send zero JoinGroup frames"
        );
    }
}

#[tokio::test]
async fn local_mock_hidden_and_disjoint_join_group_fail_closed() {
    let hidden = common::Mock::start().await;
    hidden.hide_api(JOIN_GROUP);
    let err = ConsumerGroup::join(group_cfg(&hidden.addr), "jg-hide", "t")
        .await
        .err()
        .unwrap();
    assert!(
        matches!(err, Error::Unsupported(_)),
        "local Mock: hidden JOIN_GROUP must fail closed, got {err}"
    );
    assert_eq!(
        hidden.join_group_calls(),
        0,
        "local Mock: hidden JOIN_GROUP sends zero JoinGroup frames"
    );

    let disjoint = common::Mock::start().await;
    disjoint.set_api_range(JOIN_GROUP, 10, 10);
    let err = ConsumerGroup::join(group_cfg(&disjoint.addr), "jg-disjoint", "t")
        .await
        .err()
        .unwrap();
    assert!(
        matches!(err, Error::Unsupported(_)),
        "local Mock: disjoint JOIN_GROUP 10-10 must fail closed, got {err}"
    );
    assert_eq!(
        disjoint.join_group_calls(),
        0,
        "local Mock: disjoint JOIN_GROUP sends zero JoinGroup frames"
    );
}

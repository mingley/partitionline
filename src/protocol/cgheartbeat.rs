//! ConsumerGroupHeartbeat (KIP-848, api key 68). Flexible v0–v1.

use std::collections::HashMap;

use bytes::{Buf, BufMut, BytesMut};

use super::buf;
use crate::error::{Error, Result};

/// Topic UUID plus partition indexes in a KIP-848 assignment.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TopicPartitions {
    /// Topic id (UUID).
    pub topic_id: [u8; 16],
    /// Assigned partition indexes.
    pub partitions: Vec<i32>,
}

/// ConsumerGroupHeartbeat request (join, heartbeat, or leave).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConsumerGroupHeartbeatRequest {
    /// Group id.
    pub group_id: String,
    /// Member id (`""` on v0 join; client-generated on v1, KIP-1082).
    pub member_id: String,
    /// Member epoch ([`Self::JOIN_GROUP_MEMBER_EPOCH`] join,
    /// [`Self::LEAVE_GROUP_MEMBER_EPOCH`] /
    /// [`Self::LEAVE_GROUP_STATIC_MEMBER_EPOCH`] leave, otherwise heartbeat).
    pub member_epoch: i32,
    /// Kafka `group.instance.id`.
    pub instance_id: Option<String>,
    /// Kafka `client.rack`.
    pub rack_id: Option<String>,
    /// Rebalance timeout (JSON `0+`).
    ///
    /// `-1` if unchanged since the last heartbeat (JSON default). Official
    /// Java `ConsumerGroupHeartbeatRequestData.rebalanceTimeoutMs`. Join
    /// sends `max.poll.interval.ms`.
    pub rebalance_timeout_ms: i32,
    /// Subscribed topic names (`None` means unchanged).
    pub subscribed_topic_names: Option<Vec<String>>,
    /// Subscribed topic regex (`None` means unchanged). v1+ (KIP-848).
    pub subscribed_topic_regex: Option<String>,
    /// Server-side assignor (JSON `0+`).
    ///
    /// `None` if unused or unchanged since the last heartbeat (JSON
    /// default null). Official Java
    /// `ConsumerGroupHeartbeatRequestData.serverAssignor`. Kafka
    /// `group.remote.assignor`.
    pub server_assignor: Option<String>,
    /// Owned partitions (`None` means unchanged).
    pub topic_partitions: Option<Vec<TopicPartitions>>,
}

impl ConsumerGroupHeartbeatRequest {
    /// Java `ConsumerGroupHeartbeatRequest.LEAVE_GROUP_MEMBER_EPOCH`.
    ///
    /// Dynamic members send this on leave.
    pub const LEAVE_GROUP_MEMBER_EPOCH: i32 = -1;
    /// Java `ConsumerGroupHeartbeatRequest.LEAVE_GROUP_STATIC_MEMBER_EPOCH`.
    ///
    /// Static members (`group.instance.id` present) send this on leave.
    pub const LEAVE_GROUP_STATIC_MEMBER_EPOCH: i32 = -2;
    /// Java `ConsumerGroupHeartbeatRequest.JOIN_GROUP_MEMBER_EPOCH`.
    pub const JOIN_GROUP_MEMBER_EPOCH: i32 = 0;
    /// JSON default for [`Self::rebalance_timeout_ms`]: unchanged since the
    /// last heartbeat.
    pub const UNCHANGED_REBALANCE_TIMEOUT_MS: i32 = -1;
    /// Java `ConsumerGroupHeartbeatRequest.CONSUMER_GENERATED_MEMBER_ID_REQUIRED_VERSION`.
    ///
    /// ConsumerGroupHeartbeat v1+ (KIP-1082): the client generates MemberId.
    pub const CONSUMER_GENERATED_MEMBER_ID_REQUIRED_VERSION: i16 = 1;
    /// Java `ConsumerGroupHeartbeatRequest.REGEX_RESOLUTION_NOT_SUPPORTED_MSG`.
    ///
    /// `Builder.build` rejects SubscribedTopicRegex on v0.
    pub const REGEX_RESOLUTION_NOT_SUPPORTED_MSG: &'static str = "The cluster does not support regular expressions resolution on ConsumerGroupHeartbeat API version 0. It must be upgraded to use ConsumerGroupHeartbeat API version >= 1 to allow to subscribe to a SubscriptionPattern.";

    /// Java `ConsumerGroupHeartbeatRequest.getErrorResponse`.
    ///
    /// ThrottleTimeMs is `throttle_time_ms`. ErrorCode is `error_code`.
    /// Other fields stay at JSON defaults (ErrorMessage / MemberId null,
    /// MemberEpoch `0`, HeartbeatIntervalMs `0`, Assignment null). Encode
    /// still writes the struct fields independently. This crate speaks
    /// 0–1. This is not [`ConsumerGroupHeartbeatResponse::error_counts`] /
    /// ShareGroupHeartbeat / ShareFetch / ShareAcknowledge
    /// getErrorResponse.
    #[must_use]
    pub fn error_response(
        error_code: i16,
        throttle_time_ms: i32,
    ) -> ConsumerGroupHeartbeatResponse {
        ConsumerGroupHeartbeatResponse {
            throttle_time_ms,
            error_code,
            error_message: None,
            member_id: None,
            member_epoch: 0,
            heartbeat_interval_ms: 0,
            assignment: None,
        }
    }

    /// Java `ConsumerGroupHeartbeatRequest.Builder.build`.
    ///
    /// SubscribedTopicRegex on v0 is `UnsupportedVersionException`
    /// ([`Self::REGEX_RESOLUTION_NOT_SUPPORTED_MSG`]; Java `!= null`, so
    /// empty is still present). Encode still writes independently after
    /// this helper. This crate speaks 0–1. This is not
    /// [`Self::error_response`] / [`Self::leave_group_epoch`].
    pub fn build(version: i16, subscribed_topic_regex: Option<&str>) -> Result<()> {
        if version == 0 && subscribed_topic_regex.is_some() {
            return Err(Error::Unsupported(
                Self::REGEX_RESOLUTION_NOT_SUPPORTED_MSG.into(),
            ));
        }
        Ok(())
    }

    /// Java `ConsumerMembershipManager.leaveGroupEpoch`.
    ///
    /// `Some` (including empty) is a static member (`Optional.isPresent`).
    #[must_use]
    pub const fn leave_group_epoch(group_instance_id: Option<&str>) -> i32 {
        match group_instance_id {
            Some(_) => Self::LEAVE_GROUP_STATIC_MEMBER_EPOCH,
            None => Self::LEAVE_GROUP_MEMBER_EPOCH,
        }
    }
}

/// ConsumerGroupHeartbeat response.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConsumerGroupHeartbeatResponse {
    /// ConsumerGroupHeartbeat `ThrottleTimeMs` (JSON `0+`). JSON default is `0`.
    pub throttle_time_ms: i32,
    /// Kafka error code (`0` is success).
    pub error_code: i16,
    /// Broker error message.
    pub error_message: Option<String>,
    /// Assigned member id.
    pub member_id: Option<String>,
    /// Current member epoch.
    pub member_epoch: i32,
    /// Next heartbeat interval.
    pub heartbeat_interval_ms: i32,
    /// New assignment, or `None` when unchanged.
    pub assignment: Option<Vec<TopicPartitions>>,
}

impl ConsumerGroupHeartbeatResponse {
    /// Java `ConsumerGroupHeartbeatResponse.errorCounts`.
    ///
    /// Top-level `errorCode` only, including `NONE` (Java
    /// `Collections.singletonMap`). Assignment is not counted. This is
    /// not Heartbeat / ShareGroupHeartbeat `errorCounts`.
    #[must_use]
    pub fn error_counts(&self) -> HashMap<i16, i32> {
        HashMap::from([(self.error_code, 1)])
    }
}

/// Check that ConsumerGroupHeartbeat `version` is spoken (0–1).
///
/// Flexible from v0. v1 adds SubscribedTopicRegex (KIP-848) and requires
/// the consumer to generate its own MemberId (KIP-1082). Kafka 4.0
/// `validVersions` is `0-1`. This crate speaks 0–1. v2+ is not spoken.
/// v1 response matches v0 (`INVALID_REGULAR_EXPRESSION` is v1+).
fn consumer_group_heartbeat_spoken(version: i16) -> Result<i16> {
    match version {
        0..=1 => Ok(version),
        other => Err(Error::protocol(format!(
            "ConsumerGroupHeartbeat version {other} is not implemented"
        ))),
    }
}

/// Encode a flexible v0–v1 ConsumerGroupHeartbeat request.
///
/// Java `ConsumerGroupHeartbeatRequest.Builder.build` rejects
/// SubscribedTopicRegex on v0
/// ([`ConsumerGroupHeartbeatRequest::REGEX_RESOLUTION_NOT_SUPPORTED_MSG`]).
pub fn encode_consumer_group_heartbeat_request(
    buf: &mut BytesMut,
    version: i16,
    req: &ConsumerGroupHeartbeatRequest,
) -> crate::error::Result<()> {
    let _ = consumer_group_heartbeat_spoken(version)?;
    ConsumerGroupHeartbeatRequest::build(version, req.subscribed_topic_regex.as_deref())?;
    buf::put_compact_string(buf, Some(&req.group_id))?;
    buf::put_compact_string(buf, Some(&req.member_id))?;
    buf.put_i32(req.member_epoch);
    buf::put_compact_string(buf, req.instance_id.as_deref())?;
    buf::put_compact_string(buf, req.rack_id.as_deref())?;
    buf.put_i32(req.rebalance_timeout_ms);
    match &req.subscribed_topic_names {
        None => buf::put_array_len(buf, true, None)?,
        Some(names) => {
            buf::put_array_len(buf, true, Some(names.len()))?;
            for n in names {
                buf::put_compact_string(buf, Some(n))?;
            }
        }
    }
    if version >= 1 {
        buf::put_compact_string(buf, req.subscribed_topic_regex.as_deref())?;
    }
    buf::put_compact_string(buf, req.server_assignor.as_deref())?;
    encode_topic_partitions(buf, req.topic_partitions.as_deref())?;
    buf::put_empty_tagged_fields(buf);
    Ok(())
}

/// Decode a flexible v0–v1 ConsumerGroupHeartbeat request.
pub fn decode_consumer_group_heartbeat_request<B: Buf>(
    buf: &mut B,
    version: i16,
) -> Result<ConsumerGroupHeartbeatRequest> {
    let _ = consumer_group_heartbeat_spoken(version)?;
    let group_id = buf::get_compact_string(buf)?.unwrap_or_default();
    let member_id = buf::get_compact_string(buf)?.unwrap_or_default();
    let member_epoch = buf::get_i32(buf)?;
    let instance_id = buf::get_compact_string(buf)?;
    let rack_id = buf::get_compact_string(buf)?;
    let rebalance_timeout_ms = buf::get_i32(buf)?;
    let subscribed_topic_names = {
        let n = buf::get_array_len(buf, true)?;
        match n {
            None => None,
            Some(n) => {
                let mut names = Vec::with_capacity(n);
                for _ in 0..n {
                    names.push(buf::get_compact_string(buf)?.unwrap_or_default());
                }
                Some(names)
            }
        }
    };
    let subscribed_topic_regex = if version >= 1 {
        buf::get_compact_string(buf)?
    } else {
        None
    };
    let server_assignor = buf::get_compact_string(buf)?;
    let topic_partitions = decode_topic_partitions(buf)?;
    buf::skip_tagged_fields(buf)?;
    Ok(ConsumerGroupHeartbeatRequest {
        group_id,
        member_id,
        member_epoch,
        instance_id,
        rack_id,
        rebalance_timeout_ms,
        subscribed_topic_names,
        subscribed_topic_regex,
        server_assignor,
        topic_partitions,
    })
}

/// Encode a flexible v0–v1 ConsumerGroupHeartbeat response.
///
/// ThrottleTimeMs is JSON `0+` (from [`ConsumerGroupHeartbeatResponse::throttle_time_ms`];
/// JSON default `0`).
pub fn encode_consumer_group_heartbeat_response(
    buf: &mut BytesMut,
    version: i16,
    resp: &ConsumerGroupHeartbeatResponse,
) -> crate::error::Result<()> {
    let _ = consumer_group_heartbeat_spoken(version)?;
    buf.put_i32(resp.throttle_time_ms);
    buf.put_i16(resp.error_code);
    buf::put_compact_string(buf, resp.error_message.as_deref())?;
    buf::put_compact_string(buf, resp.member_id.as_deref())?;
    buf.put_i32(resp.member_epoch);
    buf.put_i32(resp.heartbeat_interval_ms);
    match &resp.assignment {
        None => buf::put_unsigned_varint(buf, 0),
        Some(parts) => {
            buf::put_unsigned_varint(buf, 1);
            encode_topic_partitions(buf, Some(parts))?;
            buf::put_empty_tagged_fields(buf);
        }
    }
    buf::put_empty_tagged_fields(buf);
    Ok(())
}

/// Decode a flexible v0–v1 ConsumerGroupHeartbeat response.
///
/// ThrottleTimeMs is JSON `0+` (always on the wire).
pub fn decode_consumer_group_heartbeat_response<B: Buf>(
    buf: &mut B,
    version: i16,
) -> Result<ConsumerGroupHeartbeatResponse> {
    let _ = consumer_group_heartbeat_spoken(version)?;
    let throttle_time_ms = buf::get_i32(buf)?;
    let error_code = buf::get_i16(buf)?;
    let error_message = buf::get_compact_string(buf)?;
    let member_id = buf::get_compact_string(buf)?;
    let member_epoch = buf::get_i32(buf)?;
    let heartbeat_interval_ms = buf::get_i32(buf)?;
    let present = buf::get_unsigned_varint(buf)?;
    let assignment = if present == 0 {
        None
    } else {
        let parts = decode_topic_partitions(buf)?;
        buf::skip_tagged_fields(buf)?;
        parts
    };
    buf::skip_tagged_fields(buf)?;
    Ok(ConsumerGroupHeartbeatResponse {
        throttle_time_ms,
        error_code,
        error_message,
        member_id,
        member_epoch,
        heartbeat_interval_ms,
        assignment,
    })
}

fn encode_topic_partitions(
    buf: &mut BytesMut,
    parts: Option<&[TopicPartitions]>,
) -> crate::error::Result<()> {
    match parts {
        None => buf::put_array_len(buf, true, None)?,
        Some(parts) => {
            buf::put_array_len(buf, true, Some(parts.len()))?;
            for t in parts {
                buf.extend_from_slice(&t.topic_id);
                buf::put_array_len(buf, true, Some(t.partitions.len()))?;
                for p in &t.partitions {
                    buf.put_i32(*p);
                }
                buf::put_empty_tagged_fields(buf);
            }
        }
    }
    Ok(())
}

fn decode_topic_partitions<B: Buf>(buf: &mut B) -> Result<Option<Vec<TopicPartitions>>> {
    let n = buf::get_array_len(buf, true)?;
    let Some(n) = n else {
        return Ok(None);
    };
    let mut out = Vec::with_capacity(n);
    for _ in 0..n {
        let topic_id = buf::get_uuid(buf)?;
        let pn = buf::get_array_len(buf, true)?.unwrap_or(0);
        let mut partitions = Vec::with_capacity(pn);
        for _ in 0..pn {
            partitions.push(buf::get_i32(buf)?);
        }
        buf::skip_tagged_fields(buf)?;
        out.push(TopicPartitions {
            topic_id,
            partitions,
        });
    }
    Ok(Some(out))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    use bytes::Buf;

    fn join_req() -> ConsumerGroupHeartbeatRequest {
        ConsumerGroupHeartbeatRequest {
            group_id: "g".into(),
            member_id: String::new(),
            member_epoch: ConsumerGroupHeartbeatRequest::JOIN_GROUP_MEMBER_EPOCH,
            instance_id: Some("worker-1".into()),
            rack_id: Some("az1".into()),
            rebalance_timeout_ms: 45_000,
            subscribed_topic_names: Some(vec!["t".into()]),
            subscribed_topic_regex: None,
            server_assignor: None,
            topic_partitions: None,
        }
    }

    #[test]
    fn consumer_group_heartbeat_v0_roundtrip_join() {
        let req = join_req();
        let mut buf = BytesMut::new();
        encode_consumer_group_heartbeat_request(&mut buf, 0, &req).unwrap();
        assert_eq!(
            decode_consumer_group_heartbeat_request(&mut &buf[..], 0).unwrap(),
            req
        );

        let topic_id = [7u8; 16];
        let resp = ConsumerGroupHeartbeatResponse {
            throttle_time_ms: 0,
            error_code: 0,
            error_message: None,
            member_id: Some("m1".into()),
            member_epoch: 1,
            heartbeat_interval_ms: 5000,
            assignment: Some(vec![TopicPartitions {
                topic_id,
                partitions: vec![0, 1],
            }]),
        };
        buf.clear();
        encode_consumer_group_heartbeat_response(&mut buf, 0, &resp).unwrap();
        assert_eq!(
            decode_consumer_group_heartbeat_response(&mut &buf[..], 0).unwrap(),
            resp
        );
    }

    #[test]
    fn consumer_group_heartbeat_leave_has_epoch_minus_one() {
        let req = ConsumerGroupHeartbeatRequest {
            group_id: "g".into(),
            member_id: "m1".into(),
            member_epoch: ConsumerGroupHeartbeatRequest::LEAVE_GROUP_MEMBER_EPOCH,
            instance_id: None,
            rack_id: None,
            rebalance_timeout_ms: 45_000,
            subscribed_topic_names: None,
            subscribed_topic_regex: None,
            server_assignor: None,
            topic_partitions: None,
        };
        let mut buf = BytesMut::new();
        encode_consumer_group_heartbeat_request(&mut buf, 0, &req).unwrap();
        let decoded = decode_consumer_group_heartbeat_request(&mut &buf[..], 0).unwrap();
        assert_eq!(
            decoded.member_epoch,
            ConsumerGroupHeartbeatRequest::LEAVE_GROUP_MEMBER_EPOCH
        );
        assert_eq!(decoded.member_id, "m1");
    }

    #[test]
    fn consumer_group_heartbeat_static_leave_has_epoch_minus_two() {
        let req = ConsumerGroupHeartbeatRequest {
            group_id: "g".into(),
            member_id: "m1".into(),
            member_epoch: ConsumerGroupHeartbeatRequest::leave_group_epoch(Some("worker-1")),
            instance_id: Some("worker-1".into()),
            rack_id: None,
            rebalance_timeout_ms: 45_000,
            subscribed_topic_names: None,
            subscribed_topic_regex: None,
            server_assignor: None,
            topic_partitions: None,
        };
        let mut buf = BytesMut::new();
        encode_consumer_group_heartbeat_request(&mut buf, 0, &req).unwrap();
        let decoded = decode_consumer_group_heartbeat_request(&mut &buf[..], 0).unwrap();
        assert_eq!(
            decoded.member_epoch,
            ConsumerGroupHeartbeatRequest::LEAVE_GROUP_STATIC_MEMBER_EPOCH
        );
        assert_eq!(decoded.instance_id.as_deref(), Some("worker-1"));
    }

    #[test]
    fn consumer_group_heartbeat_request_matches_java() {
        assert_eq!(ConsumerGroupHeartbeatRequest::LEAVE_GROUP_MEMBER_EPOCH, -1);
        assert_eq!(
            ConsumerGroupHeartbeatRequest::LEAVE_GROUP_STATIC_MEMBER_EPOCH,
            -2
        );
        assert_eq!(ConsumerGroupHeartbeatRequest::JOIN_GROUP_MEMBER_EPOCH, 0);
        assert_eq!(
            ConsumerGroupHeartbeatRequest::CONSUMER_GENERATED_MEMBER_ID_REQUIRED_VERSION,
            1
        );
        assert_eq!(
            ConsumerGroupHeartbeatRequest::leave_group_epoch(None),
            ConsumerGroupHeartbeatRequest::LEAVE_GROUP_MEMBER_EPOCH
        );
        assert_eq!(
            ConsumerGroupHeartbeatRequest::leave_group_epoch(Some("worker-1")),
            ConsumerGroupHeartbeatRequest::LEAVE_GROUP_STATIC_MEMBER_EPOCH
        );
        assert_eq!(
            ConsumerGroupHeartbeatRequest::leave_group_epoch(Some("")),
            ConsumerGroupHeartbeatRequest::LEAVE_GROUP_STATIC_MEMBER_EPOCH,
            "Java Optional.isPresent is true for empty group.instance.id"
        );
    }

    #[test]
    fn consumer_group_heartbeat_v1_compact_layout_matches_independent_encode() {
        // group "g", empty member, epoch 0, null instance/rack, timeout
        // 45000, topics ["t"], null regex, null assignor, null partitions,
        // empty tagged. Compact string length is n+1.
        const REQ_V0: &[u8] = &[
            0x02, 0x67, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xaf, 0xc8, 0x02,
            0x02, 0x74, 0x00, 0x00, 0x00,
        ];
        const REQ_V1: &[u8] = &[
            0x02, 0x67, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xaf, 0xc8, 0x02,
            0x02, 0x74, 0x00, 0x00, 0x00, 0x00,
        ];
        const REQ_V1_REGEX: &[u8] = &[
            0x02, 0x67, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xaf, 0xc8, 0x02,
            0x02, 0x74, 0x04, 0x74, 0x2e, 0x2a, 0x00, 0x00, 0x00,
        ];
        let req = ConsumerGroupHeartbeatRequest {
            group_id: "g".into(),
            member_id: String::new(),
            member_epoch: ConsumerGroupHeartbeatRequest::JOIN_GROUP_MEMBER_EPOCH,
            instance_id: None,
            rack_id: None,
            rebalance_timeout_ms: 45_000,
            subscribed_topic_names: Some(vec!["t".into()]),
            subscribed_topic_regex: None,
            server_assignor: None,
            topic_partitions: None,
        };
        let mut buf = BytesMut::new();
        encode_consumer_group_heartbeat_request(&mut buf, 0, &req).unwrap();
        assert_eq!(&buf[..], REQ_V0);
        buf.clear();
        encode_consumer_group_heartbeat_request(&mut buf, 1, &req).unwrap();
        assert_eq!(&buf[..], REQ_V1);
        let mut with_regex = req.clone();
        with_regex.subscribed_topic_regex = Some("t.*".into());
        buf.clear();
        encode_consumer_group_heartbeat_request(&mut buf, 1, &with_regex).unwrap();
        assert_eq!(&buf[..], REQ_V1_REGEX);
        assert_eq!(
            decode_consumer_group_heartbeat_request(&mut &buf[..], 1).unwrap(),
            with_regex
        );
        let err = encode_consumer_group_heartbeat_request(&mut BytesMut::new(), 0, &with_regex)
            .unwrap_err();
        assert!(
            matches!(err, Error::Unsupported(_)),
            "regex on v0 is Java UnsupportedVersionException, got {err}"
        );
        assert!(
            err.to_string()
                .contains(ConsumerGroupHeartbeatRequest::REGEX_RESOLUTION_NOT_SUPPORTED_MSG),
            "got {err}"
        );
        assert!(
            encode_consumer_group_heartbeat_request(&mut BytesMut::new(), 2, &req).is_err(),
            "ConsumerGroupHeartbeat v2+ is not spoken"
        );
        buf.clear();
        encode_consumer_group_heartbeat_response(
            &mut buf,
            1,
            &ConsumerGroupHeartbeatResponse {
                throttle_time_ms: 0,
                error_code: 0,
                error_message: None,
                member_id: Some("m1".into()),
                member_epoch: 1,
                heartbeat_interval_ms: 5000,
                assignment: None,
            },
        )
        .unwrap();
        let mut v0 = BytesMut::new();
        encode_consumer_group_heartbeat_response(
            &mut v0,
            0,
            &ConsumerGroupHeartbeatResponse {
                throttle_time_ms: 0,
                error_code: 0,
                error_message: None,
                member_id: Some("m1".into()),
                member_epoch: 1,
                heartbeat_interval_ms: 5000,
                assignment: None,
            },
        )
        .unwrap();
        assert_eq!(&buf[..], &v0[..], "v1 response layout matches v0");
    }

    #[test]
    fn consumer_group_heartbeat_v1_roundtrip_is_leftover_empty() {
        let req = ConsumerGroupHeartbeatRequest {
            group_id: "g".into(),
            member_id: "m1".into(),
            member_epoch: 1,
            instance_id: None,
            rack_id: None,
            rebalance_timeout_ms: 45_000,
            subscribed_topic_names: None,
            subscribed_topic_regex: Some("t.*".into()),
            server_assignor: None,
            topic_partitions: None,
        };
        let mut buf = BytesMut::new();
        encode_consumer_group_heartbeat_request(&mut buf, 1, &req).unwrap();
        let mut cur = &buf[..];
        assert_eq!(
            decode_consumer_group_heartbeat_request(&mut cur, 1).unwrap(),
            req
        );
        assert!(
            !cur.has_remaining(),
            "ConsumerGroupHeartbeat v1 request must be leftover-empty"
        );

        let resp = ConsumerGroupHeartbeatResponse {
            throttle_time_ms: 0,
            error_code: 0,
            error_message: None,
            member_id: Some("m1".into()),
            member_epoch: 1,
            heartbeat_interval_ms: 5000,
            assignment: None,
        };
        buf.clear();
        encode_consumer_group_heartbeat_response(&mut buf, 1, &resp).unwrap();
        let mut cur = &buf[..];
        assert_eq!(
            decode_consumer_group_heartbeat_response(&mut cur, 1).unwrap(),
            resp
        );
        assert!(
            !cur.has_remaining(),
            "ConsumerGroupHeartbeat v1 response must be leftover-empty"
        );
    }

    #[test]
    fn consumer_group_heartbeat_request_rebalance_timeout_ms_matches_java() {
        // Kafka 4.0 ConsumerGroupHeartbeatRequest.json RebalanceTimeoutMs is
        // versions 0+ (INT32 after RackId; default -1). Official Java
        // ConsumerGroupHeartbeatRequestData.rebalanceTimeoutMs reads it.
        // Encode previously hardcoded 45000; decode discarded it. JSON
        // default -1 means unchanged since the last heartbeat. This crate
        // speaks 0–1. This is not ServerAssignor / RackId / JoinGroup
        // RebalanceTimeoutMs.
        assert_eq!(
            ConsumerGroupHeartbeatRequest::UNCHANGED_REBALANCE_TIMEOUT_MS,
            -1
        );
        let mut req = join_req();
        req.rebalance_timeout_ms = 300_000;
        for version in [0_i16, 1] {
            let mut buf = BytesMut::new();
            encode_consumer_group_heartbeat_request(&mut buf, version, &req).unwrap();
            let mut cur = buf.as_ref();
            let got = decode_consumer_group_heartbeat_request(&mut cur, version).unwrap();
            assert_eq!(got.rebalance_timeout_ms, 300_000);
            assert_eq!(got, req);
            assert!(
                cur.is_empty(),
                "ConsumerGroupHeartbeat request v{version} RebalanceTimeoutMs leftover-empty"
            );
        }

        let mut with = BytesMut::new();
        encode_consumer_group_heartbeat_request(&mut with, 0, &req).unwrap();
        let mut unchanged = join_req();
        unchanged.rebalance_timeout_ms =
            ConsumerGroupHeartbeatRequest::UNCHANGED_REBALANCE_TIMEOUT_MS;
        let mut minus_one = BytesMut::new();
        encode_consumer_group_heartbeat_request(&mut minus_one, 0, &unchanged).unwrap();
        assert_ne!(
            &with[..],
            &minus_one[..],
            "v0 RebalanceTimeoutMs is not always UNCHANGED"
        );
        let got = decode_consumer_group_heartbeat_request(&mut minus_one.as_ref(), 0).unwrap();
        assert_eq!(
            got.rebalance_timeout_ms,
            ConsumerGroupHeartbeatRequest::UNCHANGED_REBALANCE_TIMEOUT_MS
        );
    }

    #[test]
    fn consumer_group_heartbeat_request_server_assignor_matches_java() {
        // Kafka 4.0 ConsumerGroupHeartbeatRequest.json ServerAssignor is
        // versions 0+ (nullable STRING after SubscribedTopicRegex on v1 /
        // after SubscribedTopicNames on v0; default null). Official Java
        // ConsumerGroupHeartbeatRequestData.serverAssignor reads it. Encode
        // previously always wrote null; decode discarded it. This crate
        // speaks 0–1. This is not RebalanceTimeoutMs / RackId /
        // SubscribedTopicRegex / TopicPartitions.
        let mut req = join_req();
        req.server_assignor = Some("uniform".into());
        for version in [0_i16, 1] {
            let mut buf = BytesMut::new();
            encode_consumer_group_heartbeat_request(&mut buf, version, &req).unwrap();
            let mut cur = buf.as_ref();
            let got = decode_consumer_group_heartbeat_request(&mut cur, version).unwrap();
            assert_eq!(got.server_assignor.as_deref(), Some("uniform"));
            assert_eq!(got, req);
            assert!(
                cur.is_empty(),
                "ConsumerGroupHeartbeat request v{version} ServerAssignor leftover-empty"
            );
        }

        let mut with = BytesMut::new();
        encode_consumer_group_heartbeat_request(&mut with, 0, &req).unwrap();
        let none = join_req();
        let mut omitted = BytesMut::new();
        encode_consumer_group_heartbeat_request(&mut omitted, 0, &none).unwrap();
        assert_ne!(
            &with[..],
            &omitted[..],
            "v0 ServerAssignor is not always null"
        );
        let got = decode_consumer_group_heartbeat_request(&mut omitted.as_ref(), 0).unwrap();
        assert_eq!(got.server_assignor, None);
    }

    #[test]
    fn consumer_group_heartbeat_response_throttle_time_ms_matches_java() {
        // Kafka 4.0.0 ConsumerGroupHeartbeatResponse.json ThrottleTimeMs is
        // versions 0+ (INT32 on every spoken version). Official Java
        // ConsumerGroupHeartbeatRequest.getErrorResponse /
        // ConsumerGroupHeartbeatResponse.throttleTimeMs set / read it.
        // Encode writes ConsumerGroupHeartbeatResponse.throttle_time_ms
        // (JSON default 0). v0 and v1 response bodies match. This is not
        // ShareGroupHeartbeat ThrottleTimeMs.
        let zero = ConsumerGroupHeartbeatResponse {
            throttle_time_ms: 0,
            error_code: 0,
            error_message: None,
            member_id: Some("m1".into()),
            member_epoch: 1,
            heartbeat_interval_ms: 5000,
            assignment: None,
        };
        let with = ConsumerGroupHeartbeatResponse {
            throttle_time_ms: 3_600_000,
            error_code: 0,
            error_message: None,
            member_id: Some("m1".into()),
            member_epoch: 1,
            heartbeat_interval_ms: 5000,
            assignment: None,
        };
        for version in [0_i16, 1] {
            let mut buf = BytesMut::new();
            encode_consumer_group_heartbeat_response(&mut buf, version, &with).unwrap();
            let mut cur = buf.as_ref();
            let got = decode_consumer_group_heartbeat_response(&mut cur, version).unwrap();
            assert_eq!(got, with);
            assert_eq!(got.throttle_time_ms, 3_600_000);
            assert!(
                cur.is_empty(),
                "ConsumerGroupHeartbeat v{version} ThrottleTimeMs leftover-empty"
            );
        }

        let mut with_buf = BytesMut::new();
        encode_consumer_group_heartbeat_response(&mut with_buf, 0, &with).unwrap();
        let mut zero_buf = BytesMut::new();
        encode_consumer_group_heartbeat_response(&mut zero_buf, 0, &zero).unwrap();
        assert_ne!(
            &with_buf[..],
            &zero_buf[..],
            "v0 ThrottleTimeMs is not always the JSON default 0"
        );

        let mut v1_with = BytesMut::new();
        encode_consumer_group_heartbeat_response(&mut v1_with, 1, &with).unwrap();
        assert_eq!(
            &with_buf[..],
            &v1_with[..],
            "v0 and v1 both write ThrottleTimeMs (JSON 0+); ConsumerGroupHeartbeat response matches v0"
        );
    }

    #[test]
    fn consumer_group_heartbeat_request_error_response_matches_java() {
        // Java 4.0 ConsumerGroupHeartbeatRequest.getErrorResponse:
        // ThrottleTimeMs from the argument, ErrorCode from the exception,
        // other fields at JSON defaults. Official Java
        // ConsumerGroupHeartbeatRequest.getErrorResponse. Encode still
        // writes the struct fields independently. This crate speaks 0-1.
        // This is not errorCounts / ShareGroupHeartbeat /
        // ShareFetch / ShareAcknowledge getErrorResponse.
        let err = ConsumerGroupHeartbeatRequest::error_response(16, 3_600_000);
        assert_eq!(err.throttle_time_ms, 3_600_000);
        assert_eq!(err.error_code, 16);
        assert!(err.error_message.is_none());
        assert!(err.member_id.is_none());
        assert_eq!(err.member_epoch, 0);
        assert_eq!(err.heartbeat_interval_ms, 0);
        assert!(err.assignment.is_none());
        assert_eq!(
            ConsumerGroupHeartbeatRequest::error_response(0, 0),
            ConsumerGroupHeartbeatResponse {
                throttle_time_ms: 0,
                error_code: 0,
                error_message: None,
                member_id: None,
                member_epoch: 0,
                heartbeat_interval_ms: 0,
                assignment: None,
            }
        );
        leftover_consumer_group_heartbeat_error_response(0, &err);
        leftover_consumer_group_heartbeat_error_response(
            0,
            &ConsumerGroupHeartbeatRequest::error_response(0, 0),
        );
        leftover_consumer_group_heartbeat_error_response(1, &err);
        leftover_consumer_group_heartbeat_error_response(
            1,
            &ConsumerGroupHeartbeatRequest::error_response(0, 0),
        );
    }

    fn leftover_consumer_group_heartbeat_error_response(
        version: i16,
        resp: &ConsumerGroupHeartbeatResponse,
    ) {
        let mut buf = BytesMut::new();
        encode_consumer_group_heartbeat_response(&mut buf, version, resp).unwrap();
        let mut cur = buf.as_ref();
        let got = decode_consumer_group_heartbeat_response(&mut cur, version).unwrap();
        assert_eq!(got, *resp);
        let empty = if resp.error_code == 0 && resp.throttle_time_ms == 0 {
            "empty "
        } else {
            ""
        };
        assert!(
            cur.is_empty(),
            "ConsumerGroupHeartbeat v{version} getErrorResponse {empty}leftover-empty; leftover {} bytes",
            cur.len()
        );
    }

    #[test]
    fn consumer_group_heartbeat_request_build_matches_java() {
        // Java 4.0 ConsumerGroupHeartbeatRequest.Builder.build:
        // SubscribedTopicRegex on v0 is UnsupportedVersionException
        // (Java != null, so empty is still present). Official Java
        // ConsumerGroupHeartbeatRequest.Builder.build. Encode still
        // writes independently after this helper. This crate speaks
        // 0-1. This is not getErrorResponse / leaveGroupEpoch /
        // ShareGroupHeartbeat.
        assert!(ConsumerGroupHeartbeatRequest::build(0, None).is_ok());
        assert!(ConsumerGroupHeartbeatRequest::build(1, None).is_ok());
        assert!(ConsumerGroupHeartbeatRequest::build(1, Some("t.*")).is_ok());
        let v0 = ConsumerGroupHeartbeatRequest::build(0, Some("t.*")).unwrap_err();
        assert!(
            matches!(v0, Error::Unsupported(_)),
            "regex on v0 is Java UnsupportedVersionException, got {v0}"
        );
        assert!(
            v0.to_string()
                .contains(ConsumerGroupHeartbeatRequest::REGEX_RESOLUTION_NOT_SUPPORTED_MSG),
            "got {v0}"
        );
        let empty = ConsumerGroupHeartbeatRequest::build(0, Some("")).unwrap_err();
        assert!(
            matches!(empty, Error::Unsupported(_)),
            "empty regex on v0 is still present, got {empty}"
        );
        let req = ConsumerGroupHeartbeatRequest {
            group_id: "g".into(),
            member_id: String::new(),
            member_epoch: ConsumerGroupHeartbeatRequest::JOIN_GROUP_MEMBER_EPOCH,
            instance_id: None,
            rack_id: None,
            rebalance_timeout_ms: 45_000,
            subscribed_topic_names: Some(vec!["t".into()]),
            subscribed_topic_regex: None,
            server_assignor: None,
            topic_partitions: None,
        };
        leftover_consumer_group_heartbeat_build(0, &req);
        leftover_consumer_group_heartbeat_build(1, &req);
        let mut with_regex = req.clone();
        with_regex.subscribed_topic_regex = Some("t.*".into());
        leftover_consumer_group_heartbeat_build(1, &with_regex);
        assert!(
            encode_consumer_group_heartbeat_request(&mut BytesMut::new(), 0, &with_regex).is_err(),
            "encode rejects regex on v0; Builder.build rejects it too"
        );
    }

    fn leftover_consumer_group_heartbeat_build(version: i16, req: &ConsumerGroupHeartbeatRequest) {
        ConsumerGroupHeartbeatRequest::build(version, req.subscribed_topic_regex.as_deref())
            .unwrap();
        let mut buf = BytesMut::new();
        encode_consumer_group_heartbeat_request(&mut buf, version, req).unwrap();
        let mut cur = buf.as_ref();
        let got = decode_consumer_group_heartbeat_request(&mut cur, version).unwrap();
        assert_eq!(got, *req);
        let empty = if req.subscribed_topic_regex.is_none() {
            "empty "
        } else {
            ""
        };
        assert!(
            cur.is_empty(),
            "ConsumerGroupHeartbeat v{version} Builder.build {empty}leftover-empty; leftover {} bytes",
            cur.len()
        );
    }

    #[test]
    fn consumer_group_heartbeat_response_error_counts_matches_java() {
        // Java ConsumerGroupHeartbeatResponse.errorCounts:
        // Collections.singletonMap(Errors.forCode(data.errorCode()), 1),
        // including NONE. Official Java ConsumerGroupHeartbeatResponse.errorCounts.
        // Java ConsumerGroupHeartbeatResponse has no error() helper.
        // Assignment is not counted. This is not Heartbeat errorCounts /
        // ShareGroupHeartbeat errorCounts.
        let none = ConsumerGroupHeartbeatResponse {
            throttle_time_ms: 0,
            error_code: 0,
            error_message: None,
            member_id: Some("m1".into()),
            member_epoch: 1,
            heartbeat_interval_ms: 5000,
            assignment: Some(vec![TopicPartitions {
                topic_id: [7u8; 16],
                partitions: vec![0, 1],
            }]),
        };
        assert_eq!(
            none.error_counts(),
            HashMap::from([(0, 1)]),
            "NONE is a singleton 1, not an empty map"
        );
        let fenced = ConsumerGroupHeartbeatResponse {
            throttle_time_ms: 0,
            error_code: crate::error::FENCED_MEMBER_EPOCH,
            error_message: None,
            member_id: Some("m1".into()),
            member_epoch: 1,
            heartbeat_interval_ms: 5000,
            assignment: Some(vec![TopicPartitions {
                topic_id: [7u8; 16],
                partitions: vec![0, 1],
            }]),
        };
        assert_eq!(
            fenced.error_counts(),
            HashMap::from([(crate::error::FENCED_MEMBER_EPOCH, 1)])
        );
        for version in 0..=1_i16 {
            let mut resp = BytesMut::new();
            encode_consumer_group_heartbeat_response(&mut resp, version, &fenced).unwrap();
            let mut cur = &resp[..];
            let decoded = decode_consumer_group_heartbeat_response(&mut cur, version).unwrap();
            assert_eq!(
                decoded.error_counts(),
                HashMap::from([(crate::error::FENCED_MEMBER_EPOCH, 1)]),
                "ConsumerGroupHeartbeat v{version} errorCounts must count the decoded code"
            );
            assert!(
                cur.is_empty(),
                "ConsumerGroupHeartbeat v{version} errorCounts leftover-empty; leftover {} bytes",
                cur.len()
            );
        }
    }
}

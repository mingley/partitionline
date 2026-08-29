//! OffsetForLeaderEpoch (api key 23). Classic v0–v3; flexible v4.

use bytes::{Buf, BufMut, BytesMut};

use super::buf;
use crate::error::{Error, Result};

/// Check that OffsetForLeaderEpoch `version` is spoken (0–4).
///
/// v0–v1 have no ReplicaId and no CurrentLeaderEpoch. v1 response adds
/// LeaderEpoch. v2 adds CurrentLeaderEpoch and response ThrottleTimeMs.
/// v3 adds ReplicaId (`-1` for a consumer). v4 is flexible (compact
/// strings/arrays plus tagged fields; request header 2, response header
/// 1). Kafka 4.0 `validVersions` is `2-4` (v0–v1 removed). This crate
/// speaks 0–4. v5+ is not spoken.
fn offset_for_leader_epoch_spoken(version: i16) -> Result<i16> {
    match version {
        0..=4 => Ok(version),
        other => Err(Error::protocol(format!(
            "OffsetForLeaderEpoch version {other} is not implemented"
        ))),
    }
}

fn offset_for_leader_epoch_flexible(version: i16) -> Result<bool> {
    Ok(offset_for_leader_epoch_spoken(version)? >= 4)
}

/// Encode a single-topic, single-partition OffsetForLeaderEpoch request.
///
/// `replica_id` `-1` (consumer) is written on v3+. `current_leader_epoch`
/// is written on v2+.
pub fn encode_offset_for_leader_epoch_request(
    buf: &mut BytesMut,
    version: i16,
    topic: &str,
    partition: i32,
    current_leader_epoch: i32,
    leader_epoch: i32,
) -> crate::error::Result<()> {
    let flexible = offset_for_leader_epoch_flexible(version)?;
    if version >= 3 {
        buf.put_i32(-1); // replica_id (consumer)
    }
    buf::put_array_len(buf, flexible, Some(1))?;
    buf::put_string(buf, flexible, Some(topic))?;
    buf::put_array_len(buf, flexible, Some(1))?;
    buf.put_i32(partition);
    if version >= 2 {
        buf.put_i32(current_leader_epoch);
    }
    buf.put_i32(leader_epoch);
    if flexible {
        buf::put_empty_tagged_fields(buf); // partition
        buf::put_empty_tagged_fields(buf); // topic
        buf::put_empty_tagged_fields(buf); // top-level
    }
    Ok(())
}

/// Decode a single-topic, single-partition OffsetForLeaderEpoch request.
///
/// Returns `(topic, partition, current_leader_epoch, leader_epoch)`.
/// `current_leader_epoch` is `-1` below v2.
pub fn decode_offset_for_leader_epoch_request<B: Buf>(
    buf: &mut B,
    version: i16,
) -> Result<(String, i32, i32, i32)> {
    let flexible = offset_for_leader_epoch_flexible(version)?;
    if version >= 3 {
        let _replica = buf::get_i32(buf)?;
    }
    let _tn = buf::get_array_len(buf, flexible)?.unwrap_or(0);
    let topic = buf::get_string(buf, flexible)?.unwrap_or_default();
    let _pn = buf::get_array_len(buf, flexible)?.unwrap_or(0);
    let partition = buf::get_i32(buf)?;
    let current_leader_epoch = if version >= 2 { buf::get_i32(buf)? } else { -1 };
    let leader_epoch = buf::get_i32(buf)?;
    if flexible {
        buf::skip_tagged_fields(buf)?; // partition
        buf::skip_tagged_fields(buf)?; // topic
        buf::skip_tagged_fields(buf)?; // top-level
    }
    Ok((topic, partition, current_leader_epoch, leader_epoch))
}

/// Encode a single-topic, single-partition OffsetForLeaderEpoch response.
///
/// Throttle is `0` on v2+. `leader_epoch` is written on v1+.
pub fn encode_offset_for_leader_epoch_response(
    buf: &mut BytesMut,
    version: i16,
    topic: &str,
    partition: i32,
    error_code: i16,
    leader_epoch: i32,
    end_offset: i64,
) -> crate::error::Result<()> {
    let flexible = offset_for_leader_epoch_flexible(version)?;
    if version >= 2 {
        buf.put_i32(0);
    }
    buf::put_array_len(buf, flexible, Some(1))?;
    buf::put_string(buf, flexible, Some(topic))?;
    buf::put_array_len(buf, flexible, Some(1))?;
    buf.put_i16(error_code);
    buf.put_i32(partition);
    if version >= 1 {
        buf.put_i32(leader_epoch);
    }
    buf.put_i64(end_offset);
    if flexible {
        buf::put_empty_tagged_fields(buf); // partition
        buf::put_empty_tagged_fields(buf); // topic
        buf::put_empty_tagged_fields(buf); // top-level
    }
    Ok(())
}

/// Decode a single-topic, single-partition OffsetForLeaderEpoch response.
///
/// Returns `(error_code, leader_epoch, end_offset)`. `leader_epoch` is `-1`
/// below v1.
pub fn decode_offset_for_leader_epoch_response<B: Buf>(
    buf: &mut B,
    version: i16,
) -> Result<(i16, i32, i64)> {
    let flexible = offset_for_leader_epoch_flexible(version)?;
    if version >= 2 {
        let _throttle = buf::get_i32(buf)?;
    }
    let _tn = buf::get_array_len(buf, flexible)?.unwrap_or(0);
    let _topic = buf::get_string(buf, flexible)?;
    let _pn = buf::get_array_len(buf, flexible)?.unwrap_or(0);
    let error_code = buf::get_i16(buf)?;
    let _partition = buf::get_i32(buf)?;
    let leader_epoch = if version >= 1 { buf::get_i32(buf)? } else { -1 };
    let end_offset = buf::get_i64(buf)?;
    if flexible {
        buf::skip_tagged_fields(buf)?; // partition
        buf::skip_tagged_fields(buf)?; // topic
        buf::skip_tagged_fields(buf)?; // top-level
    }
    Ok((error_code, leader_epoch, end_offset))
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Buf;

    #[test]
    fn offset_for_leader_epoch_v2_roundtrip() {
        let mut req = BytesMut::new();
        encode_offset_for_leader_epoch_request(&mut req, 2, "t", 0, 3, 3).unwrap();
        let (topic, part, current, epoch) =
            decode_offset_for_leader_epoch_request(&mut &req[..], 2).unwrap();
        assert_eq!(topic, "t");
        assert_eq!(part, 0);
        assert_eq!(current, 3);
        assert_eq!(epoch, 3);

        let mut resp = BytesMut::new();
        encode_offset_for_leader_epoch_response(&mut resp, 2, "t", 0, 0, 4, 12).unwrap();
        let (err, got_epoch, end) =
            decode_offset_for_leader_epoch_response(&mut &resp[..], 2).unwrap();
        assert_eq!(err, 0);
        assert_eq!(got_epoch, 4);
        assert_eq!(end, 12);
    }

    #[test]
    fn offset_for_leader_epoch_v0_has_no_replica_or_current_epoch() {
        let mut req = BytesMut::new();
        encode_offset_for_leader_epoch_request(&mut req, 0, "t", 1, 9, 2).unwrap();
        let (topic, part, current, epoch) =
            decode_offset_for_leader_epoch_request(&mut &req[..], 0).unwrap();
        assert_eq!((topic.as_str(), part, current, epoch), ("t", 1, -1, 2));
    }

    #[test]
    fn offset_for_leader_epoch_v4_compact_layout_matches_independent_encode() {
        // ReplicaId -1, 1 topic "t", 1 partition 0, current 3, epoch 3,
        // empty tagged fields on the partition, topic, and top-level.
        const REQ_V4: &[u8] = &[
            0xff, 0xff, 0xff, 0xff, 0x02, 0x02, 0x74, 0x02, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
            0x00, 0x03, 0x00, 0x00, 0x00, 0x03, 0x00, 0x00, 0x00,
        ];
        const REQ_V3: &[u8] = &[
            0xff, 0xff, 0xff, 0xff, 0x00, 0x00, 0x00, 0x01, 0x00, 0x01, 0x74, 0x00, 0x00, 0x00,
            0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x03, 0x00, 0x00, 0x00, 0x03,
        ];
        const REQ_V2: &[u8] = &[
            0x00, 0x00, 0x00, 0x01, 0x00, 0x01, 0x74, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00,
            0x00, 0x00, 0x00, 0x00, 0x03, 0x00, 0x00, 0x00, 0x03,
        ];
        const RESP_V4: &[u8] = &[
            0x00, 0x00, 0x00, 0x00, 0x02, 0x02, 0x74, 0x02, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
            0x00, 0x00, 0x00, 0x04, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x0c, 0x00, 0x00,
            0x00,
        ];
        let mut buf = BytesMut::new();
        encode_offset_for_leader_epoch_request(&mut buf, 4, "t", 0, 3, 3).unwrap();
        assert_eq!(&buf[..], REQ_V4);
        buf.clear();
        encode_offset_for_leader_epoch_request(&mut buf, 3, "t", 0, 3, 3).unwrap();
        assert_eq!(&buf[..], REQ_V3);
        buf.clear();
        encode_offset_for_leader_epoch_request(&mut buf, 2, "t", 0, 3, 3).unwrap();
        assert_eq!(&buf[..], REQ_V2);
        assert_ne!(&buf[..], &REQ_V3[..], "v2 must not send ReplicaId (v3+)");
        buf.clear();
        encode_offset_for_leader_epoch_response(&mut buf, 4, "t", 0, 0, 4, 12).unwrap();
        assert_eq!(&buf[..], RESP_V4);
        let mut v3 = BytesMut::new();
        encode_offset_for_leader_epoch_request(&mut v3, 3, "t", 0, 3, 3).unwrap();
        assert_eq!(&v3[..], REQ_V3, "v3 ReplicaId then classic arrays");
        assert!(
            encode_offset_for_leader_epoch_request(&mut BytesMut::new(), 5, "t", 0, 3, 3).is_err(),
            "OffsetForLeaderEpoch v5+ is not spoken"
        );
    }

    #[test]
    fn offset_for_leader_epoch_v4_roundtrip_is_leftover_empty() {
        let mut buf = BytesMut::new();
        encode_offset_for_leader_epoch_request(&mut buf, 4, "t", 0, 3, 3).unwrap();
        let mut cur = &buf[..];
        assert_eq!(
            decode_offset_for_leader_epoch_request(&mut cur, 4).unwrap(),
            ("t".into(), 0, 3, 3)
        );
        assert!(
            !cur.has_remaining(),
            "OffsetForLeaderEpoch v4 request must be leftover-empty"
        );

        buf.clear();
        encode_offset_for_leader_epoch_response(&mut buf, 4, "t", 0, 0, 4, 12).unwrap();
        let mut cur = &buf[..];
        assert_eq!(
            decode_offset_for_leader_epoch_response(&mut cur, 4).unwrap(),
            (0, 4, 12)
        );
        assert!(
            !cur.has_remaining(),
            "OffsetForLeaderEpoch v4 response must be leftover-empty"
        );
    }

    #[test]
    fn offset_for_leader_epoch_v1_response_has_epoch_not_throttle() {
        // Official: v1 added LeaderEpoch on the partition; v2 added
        // ThrottleTimeMs. v1 body is topics then {error, partition,
        // leader_epoch, end_offset} with no leading throttle.
        let mut buf = BytesMut::new();
        encode_offset_for_leader_epoch_response(&mut buf, 1, "t", 0, 0, 4, 12).unwrap();
        let mut v2 = BytesMut::new();
        encode_offset_for_leader_epoch_response(&mut v2, 2, "t", 0, 0, 4, 12).unwrap();
        assert!(buf.len() < v2.len(), "v1 response must omit ThrottleTimeMs");
        let (err, epoch, end) = decode_offset_for_leader_epoch_response(&mut &buf[..], 1).unwrap();
        assert_eq!((err, epoch, end), (0, 4, 12));
    }
}

//! OffsetForLeaderEpoch (api key 23). Classic v0–v2.

use bytes::{Buf, BufMut, BytesMut};

use super::buf;
use crate::error::Result;

/// Encode a single-topic, single-partition OffsetForLeaderEpoch request.
///
/// `replica_id` is written on v1+. `current_leader_epoch` is written on v2+.
pub fn encode_offset_for_leader_epoch_request(
    buf: &mut BytesMut,
    version: i16,
    topic: &str,
    partition: i32,
    current_leader_epoch: i32,
    leader_epoch: i32,
) -> crate::error::Result<()> {
    if version >= 1 {
        buf.put_i32(-1); // replica_id
    }
    buf::put_array_len(buf, false, Some(1))?;
    buf::put_classic_nullable_string(buf, Some(topic))?;
    buf::put_array_len(buf, false, Some(1))?;
    buf.put_i32(partition);
    if version >= 2 {
        buf.put_i32(current_leader_epoch);
    }
    buf.put_i32(leader_epoch);
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
    if version >= 1 {
        let _replica = buf::get_i32(buf)?;
    }
    let _tn = buf::get_array_len(buf, false)?.unwrap_or(0);
    let topic = buf::get_classic_nullable_string(buf)?.unwrap_or_default();
    let _pn = buf::get_array_len(buf, false)?.unwrap_or(0);
    let partition = buf::get_i32(buf)?;
    let current_leader_epoch = if version >= 2 { buf::get_i32(buf)? } else { -1 };
    let leader_epoch = buf::get_i32(buf)?;
    Ok((topic, partition, current_leader_epoch, leader_epoch))
}

/// Encode a single-topic, single-partition OffsetForLeaderEpoch response.
///
/// Throttle is `0` on v1+. `leader_epoch` is written on v2+.
pub fn encode_offset_for_leader_epoch_response(
    buf: &mut BytesMut,
    version: i16,
    topic: &str,
    partition: i32,
    error_code: i16,
    leader_epoch: i32,
    end_offset: i64,
) -> crate::error::Result<()> {
    if version >= 1 {
        buf.put_i32(0);
    }
    buf::put_array_len(buf, false, Some(1))?;
    buf::put_classic_nullable_string(buf, Some(topic))?;
    buf::put_array_len(buf, false, Some(1))?;
    buf.put_i16(error_code);
    buf.put_i32(partition);
    if version >= 2 {
        buf.put_i32(leader_epoch);
    }
    buf.put_i64(end_offset);
    Ok(())
}

/// Decode a single-topic, single-partition OffsetForLeaderEpoch response.
///
/// Returns `(error_code, leader_epoch, end_offset)`. `leader_epoch` is `-1` below v2.
pub fn decode_offset_for_leader_epoch_response<B: Buf>(
    buf: &mut B,
    version: i16,
) -> Result<(i16, i32, i64)> {
    if version >= 1 {
        let _throttle = buf::get_i32(buf)?;
    }
    let _tn = buf::get_array_len(buf, false)?.unwrap_or(0);
    let _topic = buf::get_classic_nullable_string(buf)?;
    let _pn = buf::get_array_len(buf, false)?.unwrap_or(0);
    let error_code = buf::get_i16(buf)?;
    let _partition = buf::get_i32(buf)?;
    let leader_epoch = if version >= 2 { buf::get_i32(buf)? } else { -1 };
    let end_offset = buf::get_i64(buf)?;
    Ok((error_code, leader_epoch, end_offset))
}

#[cfg(test)]
mod tests {
    use super::*;

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
}

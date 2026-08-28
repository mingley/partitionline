//! ListOffsets (api key 2). Classic v1–v5.

use bytes::{Buf, BufMut, BytesMut};

use super::buf;
use crate::error::{Error, Result};

/// Log start (earliest).
pub const EARLIEST_TIMESTAMP: i64 = -2;
/// High watermark (latest).
pub const LATEST_TIMESTAMP: i64 = -1;

/// One partition in a ListOffsets response.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ListOffsetsPartition {
    /// Kafka error code (`0` is success).
    pub error_code: i16,
    /// Matched timestamp, or `-1` when unknown.
    pub timestamp: i64,
    /// Log offset, or `-1` when unknown.
    pub offset: i64,
    /// Leader epoch (v4+). `-1` when unknown or the request version is below 4.
    pub leader_epoch: i32,
}

impl ListOffsetsPartition {
    /// Successful partition body.
    #[must_use]
    pub fn ok(timestamp: i64, offset: i64, leader_epoch: i32) -> Self {
        Self {
            error_code: 0,
            timestamp,
            offset,
            leader_epoch,
        }
    }
}

/// ListOffsets v1–v5 (classic). Isolation is v2+. `current_leader_epoch` is v4+.
pub fn encode_list_offsets_request(
    buf: &mut BytesMut,
    version: i16,
    isolation_level: i8,
    topic: &str,
    partition: i32,
    current_leader_epoch: i32,
    timestamp: i64,
) -> crate::error::Result<()> {
    buf.put_i32(-1); // replica_id
    if version >= 2 {
        buf.put_i8(isolation_level);
    }
    buf::put_array_len(buf, false, Some(1))?;
    buf::put_classic_nullable_string(buf, Some(topic))?;
    buf::put_array_len(buf, false, Some(1))?;
    buf.put_i32(partition);
    if version >= 4 {
        buf.put_i32(current_leader_epoch);
    }
    buf.put_i64(timestamp);
    Ok(())
}

/// Decode a single-topic, single-partition ListOffsets request.
///
/// Returns `(isolation_level, topic, partition, current_leader_epoch, timestamp)`.
/// Isolation is `0` below v2. `current_leader_epoch` is `-1` below v4.
pub fn decode_list_offsets_request<B: Buf>(
    buf: &mut B,
    version: i16,
) -> Result<(i8, String, i32, i32, i64)> {
    let _replica = buf::get_i32(buf)?;
    let isolation = if version >= 2 { buf::get_i8(buf)? } else { 0 };
    let _tn = buf::get_array_len(buf, false)?.unwrap_or(0);
    let topic = buf::get_classic_nullable_string(buf)?.unwrap_or_default();
    let _pn = buf::get_array_len(buf, false)?.unwrap_or(0);
    let partition = buf::get_i32(buf)?;
    let current_leader_epoch = if version >= 4 { buf::get_i32(buf)? } else { -1 };
    let timestamp = buf::get_i64(buf)?;
    Ok((isolation, topic, partition, current_leader_epoch, timestamp))
}

/// Encode a single-topic, single-partition ListOffsets response.
pub fn encode_list_offsets_response(
    buf: &mut BytesMut,
    version: i16,
    topic: &str,
    partition: i32,
    result: ListOffsetsPartition,
) -> crate::error::Result<()> {
    if version >= 2 {
        buf.put_i32(0);
    }
    buf::put_array_len(buf, false, Some(1))?;
    buf::put_classic_nullable_string(buf, Some(topic))?;
    buf::put_array_len(buf, false, Some(1))?;
    buf.put_i32(partition);
    buf.put_i16(result.error_code);
    buf.put_i64(result.timestamp);
    buf.put_i64(result.offset);
    if version >= 4 {
        buf.put_i32(result.leader_epoch);
    }
    Ok(())
}

/// Decode a single-topic, single-partition ListOffsets response.
///
/// Broker `error_code != 0` is [`Error::Broker`]. [`ListOffsetsPartition::leader_epoch`]
/// is `-1` below v4.
pub fn decode_list_offsets_response<B: Buf>(
    buf: &mut B,
    version: i16,
) -> Result<ListOffsetsPartition> {
    if version >= 2 {
        let _throttle = buf::get_i32(buf)?;
    }
    let _tn = buf::get_array_len(buf, false)?.unwrap_or(0);
    let _topic = buf::get_classic_nullable_string(buf)?;
    let _pn = buf::get_array_len(buf, false)?.unwrap_or(0);
    let _p = buf::get_i32(buf)?;
    let error_code = buf::get_i16(buf)?;
    let timestamp = buf::get_i64(buf)?;
    let offset = buf::get_i64(buf)?;
    let leader_epoch = if version >= 4 { buf::get_i32(buf)? } else { -1 };
    if error_code != 0 {
        return Err(Error::broker(error_code, "ListOffsets"));
    }
    Ok(ListOffsetsPartition {
        error_code,
        timestamp,
        offset,
        leader_epoch,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn list_offsets_v2_roundtrip() {
        let mut req = BytesMut::new();
        encode_list_offsets_request(&mut req, 2, 1, "t", 3, 9, EARLIEST_TIMESTAMP).unwrap();
        let mut cur = &req[..];
        let (iso, topic, part, epoch, ts) = decode_list_offsets_request(&mut cur, 2).unwrap();
        assert_eq!((iso, topic.as_str(), part, epoch, ts), (1, "t", 3, -1, -2));
        assert!(
            cur.is_empty(),
            "v2 request has no current_leader_epoch; leftover {} bytes",
            cur.len()
        );
        let mut resp = BytesMut::new();
        encode_list_offsets_response(&mut resp, 2, "t", 3, ListOffsetsPartition::ok(-1, 7, 4))
            .unwrap();
        let mut cur = &resp[..];
        let got = decode_list_offsets_response(&mut cur, 2).unwrap();
        assert_eq!(got, ListOffsetsPartition::ok(-1, 7, -1));
        assert!(cur.is_empty(), "v2 response leftover {} bytes", cur.len());
    }

    #[test]
    fn list_offsets_v4_sends_current_leader_epoch_and_consumes_response_epoch() {
        let mut req = BytesMut::new();
        encode_list_offsets_request(&mut req, 4, 1, "t", 0, 7, LATEST_TIMESTAMP).unwrap();
        let mut cur = &req[..];
        let (iso, topic, part, epoch, ts) = decode_list_offsets_request(&mut cur, 4).unwrap();
        assert_eq!((iso, topic.as_str(), part, epoch, ts), (1, "t", 0, 7, -1));
        assert!(
            cur.is_empty(),
            "v4 request must place current_leader_epoch before timestamp; leftover {} bytes",
            cur.len()
        );

        let mut resp = BytesMut::new();
        encode_list_offsets_response(&mut resp, 4, "t", 0, ListOffsetsPartition::ok(-1, 12, 3))
            .unwrap();
        let mut cur = &resp[..];
        let got = decode_list_offsets_response(&mut cur, 4).unwrap();
        assert_eq!(got, ListOffsetsPartition::ok(-1, 12, 3));
        assert!(
            cur.is_empty(),
            "v4 decoder must consume leader_epoch after offset; leftover {} bytes",
            cur.len()
        );
    }

    #[test]
    fn list_offsets_v5_matches_v4_layout() {
        let mut req = BytesMut::new();
        encode_list_offsets_request(&mut req, 5, 0, "orders", 2, 3, 1_700_000_000_000).unwrap();
        let mut cur = &req[..];
        let (iso, topic, part, epoch, ts) = decode_list_offsets_request(&mut cur, 5).unwrap();
        assert_eq!(
            (iso, topic.as_str(), part, epoch, ts),
            (0, "orders", 2, 3, 1_700_000_000_000)
        );
        assert!(cur.is_empty());

        let mut resp = BytesMut::new();
        encode_list_offsets_response(
            &mut resp,
            5,
            "orders",
            2,
            ListOffsetsPartition::ok(1_700_000_000_000, 44, 3),
        )
        .unwrap();
        let mut cur = &resp[..];
        let got = decode_list_offsets_response(&mut cur, 5).unwrap();
        assert_eq!(got, ListOffsetsPartition::ok(1_700_000_000_000, 44, 3));
        assert!(cur.is_empty());
    }
}

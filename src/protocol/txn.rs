#![expect(
    missing_docs,
    reason = "wire types follow the Kafka spec field-for-field; public so integration tests can drive the mock broker"
)]

use bytes::{Buf, BufMut, BytesMut};

use super::buf;
use crate::error::Result;

pub const ADD_PARTITIONS_TO_TXN: i16 = 24;
pub const ADD_OFFSETS_TO_TXN: i16 = 25;
pub const END_TXN: i16 = 26;
pub const TXN_OFFSET_COMMIT: i16 = 28;

pub fn encode_add_partitions_to_txn_request(
    buf: &mut BytesMut,
    transactional_id: &str,
    producer_id: i64,
    producer_epoch: i16,
    topic: &str,
    partition: i32,
) -> crate::error::Result<()> {
    buf::put_classic_nullable_string(buf, Some(transactional_id))?;
    buf.put_i64(producer_id);
    buf.put_i16(producer_epoch);
    buf::put_array_len(buf, false, Some(1))?;
    buf::put_classic_nullable_string(buf, Some(topic))?;
    buf::put_array_len(buf, false, Some(1))?;
    buf.put_i32(partition);
    Ok(())
}

pub fn decode_add_partitions_to_txn_request<B: Buf>(
    buf: &mut B,
) -> Result<(String, i64, i16, String, i32)> {
    let tid = buf::get_classic_nullable_string(buf)?.unwrap_or_default();
    let pid = buf::get_i64(buf)?;
    let epoch = buf::get_i16(buf)?;
    let _tn = buf::get_array_len(buf, false)?.unwrap_or(0);
    let topic = buf::get_classic_nullable_string(buf)?.unwrap_or_default();
    let _pn = buf::get_array_len(buf, false)?.unwrap_or(0);
    let part = buf::get_i32(buf)?;
    Ok((tid, pid, epoch, topic, part))
}

pub fn encode_add_partitions_to_txn_response(buf: &mut BytesMut, error: i16) -> Result<()> {
    buf.put_i32(0);
    buf::put_array_len(buf, false, Some(1))?;
    buf::put_classic_nullable_string(buf, Some("t"))?;
    buf::put_array_len(buf, false, Some(1))?;
    buf.put_i32(0);
    buf.put_i16(error);
    Ok(())
}

pub fn decode_add_partitions_to_txn_response<B: Buf>(buf: &mut B) -> Result<i16> {
    let _th = buf::get_i32(buf)?;
    let _tn = buf::get_array_len(buf, false)?.unwrap_or(0);
    let _topic = buf::get_classic_nullable_string(buf)?;
    let _pn = buf::get_array_len(buf, false)?.unwrap_or(0);
    let _p = buf::get_i32(buf)?;
    buf::get_i16(buf)
}

pub fn encode_end_txn_request(
    buf: &mut BytesMut,
    transactional_id: &str,
    producer_id: i64,
    producer_epoch: i16,
    committed: bool,
) -> crate::error::Result<()> {
    buf::put_classic_nullable_string(buf, Some(transactional_id))?;
    buf.put_i64(producer_id);
    buf.put_i16(producer_epoch);
    buf.put_u8(u8::from(committed));
    Ok(())
}

pub fn decode_end_txn_request<B: Buf>(buf: &mut B) -> Result<(String, i64, i16, bool)> {
    let tid = buf::get_classic_nullable_string(buf)?.unwrap_or_default();
    let pid = buf::get_i64(buf)?;
    let epoch = buf::get_i16(buf)?;
    let committed = buf.get_u8() != 0;
    Ok((tid, pid, epoch, committed))
}

pub fn encode_end_txn_response(buf: &mut BytesMut, error: i16) -> Result<()> {
    buf.put_i32(0);
    buf.put_i16(error);
    Ok(())
}

pub fn decode_end_txn_response<B: Buf>(buf: &mut B) -> Result<i16> {
    let _th = buf::get_i32(buf)?;
    buf::get_i16(buf)
}

pub fn encode_add_offsets_to_txn_request(
    buf: &mut BytesMut,
    transactional_id: &str,
    producer_id: i64,
    producer_epoch: i16,
    group_id: &str,
) -> crate::error::Result<()> {
    buf::put_classic_nullable_string(buf, Some(transactional_id))?;
    buf.put_i64(producer_id);
    buf.put_i16(producer_epoch);
    buf::put_classic_nullable_string(buf, Some(group_id))?;
    Ok(())
}

pub fn decode_add_offsets_to_txn_request<B: Buf>(buf: &mut B) -> Result<(String, String)> {
    let tid = buf::get_classic_nullable_string(buf)?.unwrap_or_default();
    let _pid = buf::get_i64(buf)?;
    let _epoch = buf::get_i16(buf)?;
    let gid = buf::get_classic_nullable_string(buf)?.unwrap_or_default();
    Ok((tid, gid))
}

pub fn encode_add_offsets_to_txn_response(buf: &mut BytesMut, error: i16) -> Result<()> {
    buf.put_i32(0);
    buf.put_i16(error);
    Ok(())
}

pub fn decode_add_offsets_to_txn_response<B: Buf>(buf: &mut B) -> Result<i16> {
    let _th = buf::get_i32(buf)?;
    buf::get_i16(buf)
}

#[expect(
    clippy::too_many_arguments,
    reason = "TxnOffsetCommit request fields match the Kafka spec"
)]
pub fn encode_txn_offset_commit_request(
    buf: &mut BytesMut,
    transactional_id: &str,
    group_id: &str,
    producer_id: i64,
    producer_epoch: i16,
    topic: &str,
    partition: i32,
    offset: i64,
) -> crate::error::Result<()> {
    buf::put_classic_nullable_string(buf, Some(transactional_id))?;
    buf::put_classic_nullable_string(buf, Some(group_id))?;
    buf.put_i64(producer_id);
    buf.put_i16(producer_epoch);
    buf::put_array_len(buf, false, Some(1))?;
    buf::put_classic_nullable_string(buf, Some(topic))?;
    buf::put_array_len(buf, false, Some(1))?;
    buf.put_i32(partition);
    buf.put_i64(offset);
    buf.put_i32(-1);
    buf::put_classic_nullable_string(buf, None)?;
    Ok(())
}

pub fn decode_txn_offset_commit_request<B: Buf>(buf: &mut B) -> Result<(String, String, i32, i64)> {
    let tid = buf::get_classic_nullable_string(buf)?.unwrap_or_default();
    let gid = buf::get_classic_nullable_string(buf)?.unwrap_or_default();
    let _pid = buf::get_i64(buf)?;
    let _epoch = buf::get_i16(buf)?;
    let _tn = buf::get_array_len(buf, false)?.unwrap_or(0);
    let _topic = buf::get_classic_nullable_string(buf)?;
    let _pn = buf::get_array_len(buf, false)?.unwrap_or(0);
    let part = buf::get_i32(buf)?;
    let off = buf::get_i64(buf)?;
    Ok((tid, gid, part, off))
}

pub fn encode_txn_offset_commit_response(buf: &mut BytesMut, error: i16) -> Result<()> {
    buf.put_i32(0);
    buf::put_array_len(buf, false, Some(1))?;
    buf::put_classic_nullable_string(buf, Some("t"))?;
    buf::put_array_len(buf, false, Some(1))?;
    buf.put_i32(0);
    buf.put_i16(error);
    Ok(())
}

pub fn decode_txn_offset_commit_response<B: Buf>(buf: &mut B) -> Result<i16> {
    let _th = buf::get_i32(buf)?;
    let _tn = buf::get_array_len(buf, false)?.unwrap_or(0);
    let _topic = buf::get_classic_nullable_string(buf)?;
    let _pn = buf::get_array_len(buf, false)?.unwrap_or(0);
    let _p = buf::get_i32(buf)?;
    buf::get_i16(buf)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn end_txn_roundtrip() {
        let mut buf = BytesMut::new();
        encode_end_txn_request(&mut buf, "tx", 9, 1, true).unwrap();
        let (tid, pid, epoch, committed) = decode_end_txn_request(&mut &buf[..]).unwrap();
        assert_eq!((tid.as_str(), pid, epoch, committed), ("tx", 9, 1, true));
        let mut resp = BytesMut::new();
        encode_end_txn_response(&mut resp, 0).unwrap();
        assert_eq!(decode_end_txn_response(&mut &resp[..]).unwrap(), 0);
    }
}

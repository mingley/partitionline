//! InitProducerId (api key 22). Classic v0–v1.

use bytes::{Buf, BufMut, BytesMut};

use super::buf;
use crate::error::Result;

/// InitProducerId v0–v1 (classic). v2+ is flexible; we speak v1.
///
/// `transaction_timeout_ms` is Kafka `transaction.timeout.ms` (INT32 after
/// the nullable transactional id).
pub fn encode_init_producer_id_request(
    buf: &mut BytesMut,
    version: i16,
    transactional_id: Option<&str>,
    transaction_timeout_ms: i32,
) -> crate::error::Result<()> {
    buf::put_classic_nullable_string(buf, transactional_id)?;
    buf.put_i32(transaction_timeout_ms);
    if version >= 3 {
        buf.put_i64(-1);
        buf.put_i16(-1);
    }
    Ok(())
}

/// Decode InitProducerId: `(error_code, producer_id, producer_epoch)`.
pub fn decode_init_producer_id_response<B: Buf>(
    buf: &mut B,
    version: i16,
) -> Result<(i16, i64, i16)> {
    let _throttle = if version >= 1 { buf::get_i32(buf)? } else { 0 };
    let error_code = buf::get_i16(buf)?;
    let producer_id = buf::get_i64(buf)?;
    let producer_epoch = buf::get_i16(buf)?;
    Ok((error_code, producer_id, producer_epoch))
}

/// Encode InitProducerId. Throttle is `0` on v1+.
pub fn encode_init_producer_id_response(
    buf: &mut BytesMut,
    version: i16,
    error_code: i16,
    producer_id: i64,
    producer_epoch: i16,
) -> crate::error::Result<()> {
    if version >= 1 {
        buf.put_i32(0);
    }
    buf.put_i16(error_code);
    buf.put_i64(producer_id);
    buf.put_i16(producer_epoch);
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::Error;

    #[test]
    fn init_producer_id_v1_roundtrip() {
        let mut req = BytesMut::new();
        encode_init_producer_id_request(&mut req, 1, Some("tid"), 45_000).unwrap();
        let mut cur = &req[..];
        assert_eq!(
            buf::get_classic_nullable_string(&mut cur)
                .unwrap()
                .as_deref(),
            Some("tid")
        );
        assert_eq!(buf::get_i32(&mut cur).unwrap(), 45_000);
        assert!(cur.is_empty());

        let mut resp = BytesMut::new();
        encode_init_producer_id_response(&mut resp, 1, 0, 1234, 7).unwrap();
        let (err, pid, epoch) = decode_init_producer_id_response(&mut &resp[..], 1).unwrap();
        assert_eq!(err, 0);
        assert_eq!(pid, 1234);
        assert_eq!(epoch, 7);
    }

    #[test]
    fn init_producer_id_error_is_visible() {
        let mut resp = BytesMut::new();
        encode_init_producer_id_response(&mut resp, 1, 58, -1, -1).unwrap();
        let (err, pid, _) = decode_init_producer_id_response(&mut &resp[..], 1).unwrap();
        assert_eq!(err, 58);
        assert_eq!(pid, -1);
        assert!(Error::broker(err, "InitProducerId")
            .to_string()
            .contains("58"));
    }
}

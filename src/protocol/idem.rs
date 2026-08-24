use bytes::{Buf, BufMut, BytesMut};

use super::buf;
use crate::error::Result;

/// InitProducerId v0–v1 (classic). v2+ is flexible; we speak v1.
pub fn encode_init_producer_id_request(buf: &mut BytesMut, version: i16) {
    buf::put_classic_nullable_string(buf, None);
    buf.put_i32(60_000);
    if version >= 3 {
        buf.put_i64(-1);
        buf.put_i16(-1);
    }
}

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

pub fn encode_init_producer_id_response(
    buf: &mut BytesMut,
    version: i16,
    error_code: i16,
    producer_id: i64,
    producer_epoch: i16,
) {
    if version >= 1 {
        buf.put_i32(0);
    }
    buf.put_i16(error_code);
    buf.put_i64(producer_id);
    buf.put_i16(producer_epoch);
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::Error;

    #[test]
    fn init_producer_id_v1_roundtrip() {
        let mut req = BytesMut::new();
        encode_init_producer_id_request(&mut req, 1);
        let mut cur = &req[..];
        assert_eq!(buf::get_classic_nullable_string(&mut cur).unwrap(), None);
        assert_eq!(buf::get_i32(&mut cur).unwrap(), 60_000);
        assert!(cur.is_empty());

        let mut resp = BytesMut::new();
        encode_init_producer_id_response(&mut resp, 1, 0, 1234, 7);
        let (err, pid, epoch) = decode_init_producer_id_response(&mut &resp[..], 1).unwrap();
        assert_eq!(err, 0);
        assert_eq!(pid, 1234);
        assert_eq!(epoch, 7);
    }

    #[test]
    fn init_producer_id_error_is_visible() {
        let mut resp = BytesMut::new();
        encode_init_producer_id_response(&mut resp, 1, 58, -1, -1);
        let (err, pid, _) = decode_init_producer_id_response(&mut &resp[..], 1).unwrap();
        assert_eq!(err, 58);
        assert_eq!(pid, -1);
        assert!(Error::broker(err, "InitProducerId")
            .to_string()
            .contains("58"));
    }
}

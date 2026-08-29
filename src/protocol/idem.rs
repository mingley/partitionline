//! InitProducerId (api key 22). v0–v1 classic; v2 flexible.

use bytes::{Buf, BufMut, BytesMut};

use super::buf;
use crate::error::{Error, Result};

/// `true` when InitProducerId `version` is flexible (v2).
///
/// v0–v1 are classic. v2 is compact strings plus tagged fields
/// (Apache JSON `flexibleVersions: "2+"`). v3+ (ProducerId / ProducerEpoch
/// on the request, KIP-360) is not spoken.
fn init_producer_id_flexible(version: i16) -> Result<bool> {
    match version {
        0..=1 => Ok(false),
        2 => Ok(true),
        other => Err(Error::protocol(format!(
            "InitProducerId version {other} is not implemented"
        ))),
    }
}

/// InitProducerId v0–v1 (classic) or v2 (flexible).
///
/// `transaction_timeout_ms` is Kafka `transaction.timeout.ms` (INT32 after
/// the nullable transactional id).
pub fn encode_init_producer_id_request(
    buf: &mut BytesMut,
    version: i16,
    transactional_id: Option<&str>,
    transaction_timeout_ms: i32,
) -> crate::error::Result<()> {
    let flexible = init_producer_id_flexible(version)?;
    buf::put_string(buf, flexible, transactional_id)?;
    buf.put_i32(transaction_timeout_ms);
    if flexible {
        buf::put_empty_tagged_fields(buf);
    }
    Ok(())
}

/// Decode InitProducerId request: `(transactional_id, transaction_timeout_ms)`.
pub fn decode_init_producer_id_request<B: Buf>(
    buf: &mut B,
    version: i16,
) -> Result<(Option<String>, i32)> {
    let flexible = init_producer_id_flexible(version)?;
    let transactional_id = buf::get_string(buf, flexible)?;
    let transaction_timeout_ms = buf::get_i32(buf)?;
    if flexible {
        buf::skip_tagged_fields(buf)?;
    }
    Ok((transactional_id, transaction_timeout_ms))
}

/// Decode InitProducerId: `(error_code, producer_id, producer_epoch)`.
pub fn decode_init_producer_id_response<B: Buf>(
    buf: &mut B,
    version: i16,
) -> Result<(i16, i64, i16)> {
    let flexible = init_producer_id_flexible(version)?;
    let _throttle = if version >= 1 { buf::get_i32(buf)? } else { 0 };
    let error_code = buf::get_i16(buf)?;
    let producer_id = buf::get_i64(buf)?;
    let producer_epoch = buf::get_i16(buf)?;
    if flexible {
        buf::skip_tagged_fields(buf)?;
    }
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
    let flexible = init_producer_id_flexible(version)?;
    if version >= 1 {
        buf.put_i32(0);
    }
    buf.put_i16(error_code);
    buf.put_i64(producer_id);
    buf.put_i16(producer_epoch);
    if flexible {
        buf::put_empty_tagged_fields(buf);
    }
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
        let (tid, timeout) = decode_init_producer_id_request(&mut cur, 1).unwrap();
        assert_eq!(tid.as_deref(), Some("tid"));
        assert_eq!(timeout, 45_000);
        assert!(cur.is_empty());

        let mut resp = BytesMut::new();
        encode_init_producer_id_response(&mut resp, 1, 0, 1234, 7).unwrap();
        let mut cur = &resp[..];
        let (err, pid, epoch) = decode_init_producer_id_response(&mut cur, 1).unwrap();
        assert_eq!(err, 0);
        assert_eq!(pid, 1234);
        assert_eq!(epoch, 7);
        assert!(cur.is_empty());
    }

    #[test]
    fn init_producer_id_v2_roundtrip_is_leftover_empty() {
        let mut req = BytesMut::new();
        encode_init_producer_id_request(&mut req, 2, Some("tid"), 45_000).unwrap();
        let mut cur = &req[..];
        let (tid, timeout) = decode_init_producer_id_request(&mut cur, 2).unwrap();
        assert_eq!(tid.as_deref(), Some("tid"));
        assert_eq!(timeout, 45_000);
        assert!(
            cur.is_empty(),
            "InitProducerId v2 request must consume compact tagged fields"
        );

        let mut resp = BytesMut::new();
        encode_init_producer_id_response(&mut resp, 2, 0, 1234, 7).unwrap();
        let mut cur = &resp[..];
        let (err, pid, epoch) = decode_init_producer_id_response(&mut cur, 2).unwrap();
        assert_eq!(err, 0);
        assert_eq!(pid, 1234);
        assert_eq!(epoch, 7);
        assert!(
            cur.is_empty(),
            "InitProducerId v2 response must consume compact tagged fields"
        );
        req.clear();
        assert!(
            encode_init_producer_id_request(&mut req, 3, Some("tid"), 45_000).is_err(),
            "InitProducerId v3+ (KIP-360 ProducerId) is not spoken"
        );
    }

    #[test]
    fn init_producer_id_v2_request_matches_compact_layout() {
        // Compact nullable "tid" (n+1 = 4), timeout 45000, tagged.
        const REQ: &[u8] = &[0x04, 0x74, 0x69, 0x64, 0x00, 0x00, 0xaf, 0xc8, 0x00];
        let mut buf = BytesMut::new();
        encode_init_producer_id_request(&mut buf, 2, Some("tid"), 45_000).unwrap();
        assert_eq!(&buf[..], REQ);
    }

    #[test]
    fn init_producer_id_v2_response_matches_compact_layout() {
        // Throttle 0, error 0, producer_id 1234, epoch 7, tagged.
        const RESP: &[u8] = &[
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x04, 0xd2,
            0x00, 0x07, 0x00,
        ];
        let mut buf = BytesMut::new();
        encode_init_producer_id_response(&mut buf, 2, 0, 1234, 7).unwrap();
        assert_eq!(&buf[..], RESP);
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

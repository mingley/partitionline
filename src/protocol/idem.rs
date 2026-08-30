//! InitProducerId (api key 22). v0–v1 classic; v2–v5 flexible.

use bytes::{Buf, BufMut, BytesMut};

use super::buf;
use super::records::RecordBatch;
use crate::error::{Error, Result};

/// `true` when InitProducerId `version` is flexible (v2+).
///
/// v0–v1 are classic. v2 is compact strings plus tagged fields
/// (Apache JSON `flexibleVersions: "2+"`). v3+ adds ProducerId /
/// ProducerEpoch on the request (KIP-360). v4 is PRODUCER_FENCED; v5
/// is TRANSACTION_ABORTABLE (KIP-890). Kafka 4.0 `validVersions` is
/// `0-5`. v6+ (KIP-939 2PC Enable2Pc / KeepPreparedTxn) is not spoken.
fn init_producer_id_flexible(version: i16) -> Result<bool> {
    match version {
        0..=1 => Ok(false),
        2..=5 => Ok(true),
        other => Err(Error::protocol(format!(
            "InitProducerId version {other} is not implemented"
        ))),
    }
}

/// Java `InitProducerIdResponse` helpers.
pub struct InitProducerIdResponse;

impl InitProducerIdResponse {
    /// Java `InitProducerIdResponse.shouldClientThrottle`.
    #[must_use]
    pub const fn should_client_throttle(version: i16) -> bool {
        version >= 1
    }
}

/// InitProducerId v0–v1 (classic) or v2–v5 (flexible).
///
/// `transaction_timeout_ms` is Kafka `transaction.timeout.ms` (INT32 after
/// the nullable transactional id). `producer_id` / `producer_epoch` are
/// written at v3+ (KIP-360); first init sends [`RecordBatch::NO_PRODUCER_ID`] /
/// [`RecordBatch::NO_PRODUCER_EPOCH`]. Epoch-bump resume sends the last
/// producer id and epoch. Ignored on v0–v2. Java
/// `InitProducerIdRequest.getErrorResponse` writes those same sentinels.
/// Java `InitProducerIdRequest.Builder.build` rejects a non-positive
/// timeout and an empty (non-null) transactional id.
pub fn encode_init_producer_id_request(
    buf: &mut BytesMut,
    version: i16,
    transactional_id: Option<&str>,
    transaction_timeout_ms: i32,
    producer_id: i64,
    producer_epoch: i16,
) -> crate::error::Result<()> {
    let flexible = init_producer_id_flexible(version)?;
    if transaction_timeout_ms <= 0 {
        return Err(Error::protocol(format!(
            "transaction timeout value is not positive: {transaction_timeout_ms}"
        )));
    }
    if transactional_id.is_some_and(str::is_empty) {
        return Err(Error::protocol(
            "Must set either a null or a non-empty transactional id.",
        ));
    }
    buf::put_string(buf, flexible, transactional_id)?;
    buf.put_i32(transaction_timeout_ms);
    if version >= 3 {
        buf.put_i64(producer_id);
        buf.put_i16(producer_epoch);
    }
    if flexible {
        buf::put_empty_tagged_fields(buf);
    }
    Ok(())
}

/// Decode InitProducerId request: `(transactional_id, timeout, producer_id, producer_epoch)`.
///
/// `producer_id` / `producer_epoch` are [`RecordBatch::NO_PRODUCER_ID`] /
/// [`RecordBatch::NO_PRODUCER_EPOCH`] when the version is below 3.
pub fn decode_init_producer_id_request<B: Buf>(
    buf: &mut B,
    version: i16,
) -> Result<(Option<String>, i32, i64, i16)> {
    let flexible = init_producer_id_flexible(version)?;
    let transactional_id = buf::get_string(buf, flexible)?;
    let transaction_timeout_ms = buf::get_i32(buf)?;
    let (producer_id, producer_epoch) = if version >= 3 {
        (buf::get_i64(buf)?, buf::get_i16(buf)?)
    } else {
        (RecordBatch::NO_PRODUCER_ID, RecordBatch::NO_PRODUCER_EPOCH)
    };
    if flexible {
        buf::skip_tagged_fields(buf)?;
    }
    Ok((
        transactional_id,
        transaction_timeout_ms,
        producer_id,
        producer_epoch,
    ))
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
///
/// Java `InitProducerIdRequest.getErrorResponse` writes
/// [`RecordBatch::NO_PRODUCER_ID`] / [`RecordBatch::NO_PRODUCER_EPOCH`].
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
        encode_init_producer_id_request(
            &mut req,
            1,
            Some("tid"),
            45_000,
            RecordBatch::NO_PRODUCER_ID,
            RecordBatch::NO_PRODUCER_EPOCH,
        )
        .unwrap();
        let mut cur = &req[..];
        let (tid, timeout, pid, epoch) = decode_init_producer_id_request(&mut cur, 1).unwrap();
        assert_eq!(tid.as_deref(), Some("tid"));
        assert_eq!(timeout, 45_000);
        assert_eq!(pid, RecordBatch::NO_PRODUCER_ID);
        assert_eq!(epoch, RecordBatch::NO_PRODUCER_EPOCH);
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
        encode_init_producer_id_request(
            &mut req,
            2,
            Some("tid"),
            45_000,
            RecordBatch::NO_PRODUCER_ID,
            RecordBatch::NO_PRODUCER_EPOCH,
        )
        .unwrap();
        let mut cur = &req[..];
        let (tid, timeout, pid, epoch) = decode_init_producer_id_request(&mut cur, 2).unwrap();
        assert_eq!(tid.as_deref(), Some("tid"));
        assert_eq!(timeout, 45_000);
        assert_eq!(pid, RecordBatch::NO_PRODUCER_ID);
        assert_eq!(epoch, RecordBatch::NO_PRODUCER_EPOCH);
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
    }

    #[test]
    fn init_producer_id_v5_roundtrip_is_leftover_empty() {
        let mut req = BytesMut::new();
        encode_init_producer_id_request(&mut req, 5, Some("tid"), 45_000, 1234, 7).unwrap();
        let mut cur = &req[..];
        let (tid, timeout, pid, epoch) = decode_init_producer_id_request(&mut cur, 5).unwrap();
        assert_eq!(tid.as_deref(), Some("tid"));
        assert_eq!(timeout, 45_000);
        assert_eq!(pid, 1234);
        assert_eq!(epoch, 7);
        assert!(
            cur.is_empty(),
            "InitProducerId v5 request must consume ProducerId, ProducerEpoch, and tagged fields"
        );

        let mut resp = BytesMut::new();
        encode_init_producer_id_response(&mut resp, 5, 0, 1234, 7).unwrap();
        let mut cur = &resp[..];
        let (err, pid, epoch) = decode_init_producer_id_response(&mut cur, 5).unwrap();
        assert_eq!(err, 0);
        assert_eq!(pid, 1234);
        assert_eq!(epoch, 7);
        assert!(
            cur.is_empty(),
            "InitProducerId v5 response must consume compact tagged fields"
        );
        req.clear();
        assert!(
            encode_init_producer_id_request(
                &mut req,
                6,
                Some("tid"),
                45_000,
                RecordBatch::NO_PRODUCER_ID,
                RecordBatch::NO_PRODUCER_EPOCH,
            )
            .is_err(),
            "InitProducerId v6+ (KIP-939 2PC) is not spoken"
        );
    }

    #[test]
    fn init_producer_id_v2_request_matches_compact_layout() {
        // Compact nullable "tid" (n+1 = 4), timeout 45000, tagged.
        const REQ: &[u8] = &[0x04, 0x74, 0x69, 0x64, 0x00, 0x00, 0xaf, 0xc8, 0x00];
        let mut buf = BytesMut::new();
        encode_init_producer_id_request(
            &mut buf,
            2,
            Some("tid"),
            45_000,
            RecordBatch::NO_PRODUCER_ID,
            RecordBatch::NO_PRODUCER_EPOCH,
        )
        .unwrap();
        assert_eq!(&buf[..], REQ);
    }

    #[test]
    fn init_producer_id_v3_request_matches_compact_layout() {
        // v2 body plus ProducerId 1234, ProducerEpoch 7, tagged.
        const REQ: &[u8] = &[
            0x04, 0x74, 0x69, 0x64, 0x00, 0x00, 0xaf, 0xc8, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
            0x04, 0xd2, 0x00, 0x07, 0x00,
        ];
        let mut buf = BytesMut::new();
        encode_init_producer_id_request(&mut buf, 3, Some("tid"), 45_000, 1234, 7).unwrap();
        assert_eq!(&buf[..], REQ);
        buf.clear();
        encode_init_producer_id_request(&mut buf, 5, Some("tid"), 45_000, 1234, 7).unwrap();
        assert_eq!(&buf[..], REQ, "v4/v5 request body matches v3");
    }

    #[test]
    fn init_producer_id_v3_first_init_sends_no_producer_id() {
        // Compact "tid", timeout 45000, NO_PRODUCER_ID / NO_PRODUCER_EPOCH, tagged.
        const REQ: &[u8] = &[
            0x04, 0x74, 0x69, 0x64, 0x00, 0x00, 0xaf, 0xc8, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff,
            0xff, 0xff, 0xff, 0xff, 0x00,
        ];
        let mut buf = BytesMut::new();
        encode_init_producer_id_request(
            &mut buf,
            3,
            Some("tid"),
            45_000,
            RecordBatch::NO_PRODUCER_ID,
            RecordBatch::NO_PRODUCER_EPOCH,
        )
        .unwrap();
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
        buf.clear();
        encode_init_producer_id_response(&mut buf, 5, 0, 1234, 7).unwrap();
        assert_eq!(&buf[..], RESP, "v3–v5 response body matches v2");
    }

    #[test]
    fn init_producer_id_builder_matches_java() {
        assert!(!InitProducerIdResponse::should_client_throttle(0));
        assert!(InitProducerIdResponse::should_client_throttle(1));
        encode_init_producer_id_request(
            &mut BytesMut::new(),
            1,
            None,
            45_000,
            RecordBatch::NO_PRODUCER_ID,
            RecordBatch::NO_PRODUCER_EPOCH,
        )
        .unwrap();
        let timeout = encode_init_producer_id_request(
            &mut BytesMut::new(),
            1,
            Some("tid"),
            0,
            RecordBatch::NO_PRODUCER_ID,
            RecordBatch::NO_PRODUCER_EPOCH,
        )
        .unwrap_err();
        assert!(
            matches!(timeout, Error::Protocol(_)),
            "non-positive timeout is Java IllegalArgumentException, got {timeout}"
        );
        assert!(
            timeout.to_string().contains("not positive"),
            "got {timeout}"
        );
        let empty = encode_init_producer_id_request(
            &mut BytesMut::new(),
            1,
            Some(""),
            45_000,
            RecordBatch::NO_PRODUCER_ID,
            RecordBatch::NO_PRODUCER_EPOCH,
        )
        .unwrap_err();
        assert!(
            matches!(empty, Error::Protocol(_)),
            "empty transactional id is Java IllegalArgumentException, got {empty}"
        );
        assert!(
            empty.to_string().contains("non-empty transactional id"),
            "got {empty}"
        );
    }

    #[test]
    fn init_producer_id_error_is_visible() {
        let mut resp = BytesMut::new();
        encode_init_producer_id_response(
            &mut resp,
            1,
            58,
            RecordBatch::NO_PRODUCER_ID,
            RecordBatch::NO_PRODUCER_EPOCH,
        )
        .unwrap();
        let (err, pid, epoch) = decode_init_producer_id_response(&mut &resp[..], 1).unwrap();
        assert_eq!(err, 58);
        assert_eq!(pid, RecordBatch::NO_PRODUCER_ID);
        assert_eq!(epoch, RecordBatch::NO_PRODUCER_EPOCH);
        assert!(Error::broker(err, "InitProducerId")
            .to_string()
            .contains("58"));
    }
}

//! InitProducerId (api key 22). v0–v1 classic; v2–v5 flexible.

use std::collections::HashMap;

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

/// Java `InitProducerIdRequest` helpers.
pub struct InitProducerIdRequest;

impl InitProducerIdRequest {
    /// Java `InitProducerIdRequest.Builder.build`.
    ///
    /// Rejects a non-positive `transactionTimeoutMs`
    /// (`IllegalArgumentException`) and an empty (non-null) transactional
    /// id. Null transactional id is idempotent produce. Encode still
    /// writes independently after this helper. This crate speaks 0–5.
    /// This is not [`Self::error_response`].
    pub fn build(transaction_timeout_ms: i32, transactional_id: Option<&str>) -> Result<()> {
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
        Ok(())
    }

    /// Java `InitProducerIdRequest.getErrorResponse`.
    ///
    /// Producer id / epoch are [`RecordBatch::NO_PRODUCER_ID`] /
    /// [`RecordBatch::NO_PRODUCER_EPOCH`]. ThrottleTimeMs stays the JSON
    /// default (`0`); official Java `getErrorResponse` sets
    /// `throttleTimeMs` to `0` even when the argument is non-zero. Crate
    /// convenience encode still writes `0`.
    pub fn error_response(
        buf: &mut BytesMut,
        version: i16,
        error_code: i16,
    ) -> crate::error::Result<()> {
        encode_init_producer_id_response(
            buf,
            version,
            error_code,
            RecordBatch::NO_PRODUCER_ID,
            RecordBatch::NO_PRODUCER_EPOCH,
        )
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

    /// Java `InitProducerIdResponse.errorCounts`.
    ///
    /// Top-level `errorCode` only, including `NONE` (Java
    /// `Collections.singletonMap`). This is not EndTxn / AddOffsetsToTxn /
    /// Heartbeat `errorCounts`.
    #[must_use]
    pub fn error_counts(error_code: i16) -> HashMap<i16, i32> {
        HashMap::from([(error_code, 1)])
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
/// [`InitProducerIdRequest::build`] is Java
/// `InitProducerIdRequest.Builder.build` (rejects a non-positive timeout
/// and an empty (non-null) transactional id). Encode still writes
/// independently after that helper.
pub fn encode_init_producer_id_request(
    buf: &mut BytesMut,
    version: i16,
    transactional_id: Option<&str>,
    transaction_timeout_ms: i32,
    producer_id: i64,
    producer_epoch: i16,
) -> crate::error::Result<()> {
    let flexible = init_producer_id_flexible(version)?;
    InitProducerIdRequest::build(transaction_timeout_ms, transactional_id)?;
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

/// Decode InitProducerId: `(error_code, producer_id, producer_epoch, throttle_time_ms)`.
///
/// ThrottleTimeMs is JSON `0+` (always on the wire). Top-level ErrorCode
/// is at bytes 4–5.
pub fn decode_init_producer_id_response<B: Buf>(
    buf: &mut B,
    version: i16,
) -> Result<(i16, i64, i16, i32)> {
    let flexible = init_producer_id_flexible(version)?;
    let throttle_time_ms = buf::get_i32(buf)?;
    let error_code = buf::get_i16(buf)?;
    let producer_id = buf::get_i64(buf)?;
    let producer_epoch = buf::get_i16(buf)?;
    if flexible {
        buf::skip_tagged_fields(buf)?;
    }
    Ok((error_code, producer_id, producer_epoch, throttle_time_ms))
}

/// Encode InitProducerId. Throttle is the JSON default (`0`).
///
/// ThrottleTimeMs is JSON `0+` on every spoken version. Java
/// `InitProducerIdRequest.getErrorResponse` writes
/// [`RecordBatch::NO_PRODUCER_ID`] / [`RecordBatch::NO_PRODUCER_EPOCH`]
/// and throttle `0`.
pub fn encode_init_producer_id_response(
    buf: &mut BytesMut,
    version: i16,
    error_code: i16,
    producer_id: i64,
    producer_epoch: i16,
) -> crate::error::Result<()> {
    encode_init_producer_id_response_with_throttle(
        buf,
        version,
        error_code,
        producer_id,
        producer_epoch,
        0,
    )
}

/// Encode InitProducerId v0–v5 with ThrottleTimeMs.
///
/// ThrottleTimeMs is JSON `0+`: written on every spoken version.
/// v0–v1 are classic. v2–v5 are flexible. v3 and v4 match v2 (KIP-360
/// ProducerId is on the request; v4 is PRODUCER_FENCED). v5 is
/// TRANSACTION_ABORTABLE (KIP-890; same layout as v2). Kafka 4.0
/// `validVersions` is `0-5`. This crate speaks 0–5. v6+ is not spoken.
/// Official Java `InitProducerIdResponse.throttleTimeMs` /
/// `InitProducerIdResponseData.throttleTimeMs`. Java
/// `getErrorResponse` sets `throttleTimeMs` to `0` even when the
/// argument is non-zero ([`encode_init_producer_id_response`] still
/// writes `0`). KIP-219 only changes `shouldClientThrottle` (v1+).
/// Top-level ErrorCode is at bytes 4–5. This is not EndTxn /
/// AddOffsetsToTxn / Produce ThrottleTimeMs.
pub fn encode_init_producer_id_response_with_throttle(
    buf: &mut BytesMut,
    version: i16,
    error_code: i16,
    producer_id: i64,
    producer_epoch: i16,
    throttle_time_ms: i32,
) -> crate::error::Result<()> {
    let flexible = init_producer_id_flexible(version)?;
    buf.put_i32(throttle_time_ms);
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
    use std::collections::HashMap;

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
        let (err, pid, epoch, ..) = decode_init_producer_id_response(&mut cur, 1).unwrap();
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
        let (err, pid, epoch, ..) = decode_init_producer_id_response(&mut cur, 2).unwrap();
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
        let (err, pid, epoch, ..) = decode_init_producer_id_response(&mut cur, 5).unwrap();
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
    fn init_producer_id_response_throttle_time_ms_matches_java() {
        // Kafka 4.0.0 InitProducerIdResponse.json ThrottleTimeMs is
        // versions 0+ (INT32 on spoken v0–v5; first field). Encode
        // previously always wrote 0 on v1+ and omitted the field on v0;
        // decode discarded it. Official Java
        // InitProducerIdResponse.throttleTimeMs /
        // InitProducerIdResponseData.throttleTimeMs. Java
        // getErrorResponse sets throttleTimeMs to 0 even when the
        // argument is non-zero. encode_init_producer_id_response still
        // writes the JSON default 0. KIP-219 only changes
        // shouldClientThrottle (v1+). Empty-error v0 == v1 (classic);
        // v2, v3, v4, and v5 bodies match (flexible). Top-level
        // ErrorCode is at bytes 4–5. Kafka 4.0 validVersions is 0-5.
        // This crate speaks 0–5. This is not EndTxn ThrottleTimeMs /
        // AddOffsetsToTxn ThrottleTimeMs / Produce ThrottleTimeMs.
        for version in [0_i16, 1, 2, 3, 4, 5] {
            let mut buf = BytesMut::new();
            encode_init_producer_id_response_with_throttle(
                &mut buf, version, 0, 1234, 7, 3_600_000,
            )
            .unwrap();
            let mut cur = buf.as_ref();
            let (err, pid, epoch, throttle) =
                decode_init_producer_id_response(&mut cur, version).unwrap();
            assert_eq!(err, 0);
            assert_eq!(pid, 1234);
            assert_eq!(epoch, 7);
            assert_eq!(throttle, 3_600_000);
            assert!(
                cur.is_empty(),
                "InitProducerId v{version} ThrottleTimeMs leftover-empty"
            );
        }

        let mut with = BytesMut::new();
        encode_init_producer_id_response_with_throttle(&mut with, 0, 0, 1234, 7, 3_600_000)
            .unwrap();
        let mut zero = BytesMut::new();
        encode_init_producer_id_response_with_throttle(&mut zero, 0, 0, 1234, 7, 0).unwrap();
        assert_ne!(
            &with[..],
            &zero[..],
            "v0 ThrottleTimeMs is not always the JSON default 0"
        );
        assert_eq!(
            with.get(0..4),
            Some(3_600_000i32.to_be_bytes().as_slice()),
            "classic ThrottleTimeMs is the first INT32"
        );
        assert_eq!(
            zero.get(0..4),
            Some([0, 0, 0, 0].as_slice()),
            "encode_init_producer_id_response_with_throttle 0 is four zero bytes"
        );
        let mut conv = BytesMut::new();
        encode_init_producer_id_response(&mut conv, 0, 0, 1234, 7).unwrap();
        assert_eq!(
            &conv[..],
            &zero[..],
            "encode_init_producer_id_response still writes ThrottleTimeMs 0"
        );

        let mut v1_with = BytesMut::new();
        encode_init_producer_id_response_with_throttle(&mut v1_with, 1, 0, 1234, 7, 3_600_000)
            .unwrap();
        assert_eq!(
            &with[..],
            &v1_with[..],
            "empty-error ThrottleTimeMs bodies: v0 == v1"
        );
        let mut v2_with = BytesMut::new();
        encode_init_producer_id_response_with_throttle(&mut v2_with, 2, 0, 1234, 7, 3_600_000)
            .unwrap();
        assert_ne!(&v1_with[..], &v2_with[..], "v2 adds compact tagged fields");
        let mut v3_with = BytesMut::new();
        encode_init_producer_id_response_with_throttle(&mut v3_with, 3, 0, 1234, 7, 3_600_000)
            .unwrap();
        let mut v4_with = BytesMut::new();
        encode_init_producer_id_response_with_throttle(&mut v4_with, 4, 0, 1234, 7, 3_600_000)
            .unwrap();
        let mut v5_with = BytesMut::new();
        encode_init_producer_id_response_with_throttle(&mut v5_with, 5, 0, 1234, 7, 3_600_000)
            .unwrap();
        assert_eq!(
            &v2_with[..],
            &v3_with[..],
            "empty-error ThrottleTimeMs bodies: v2 == v3"
        );
        assert_eq!(
            &v3_with[..],
            &v4_with[..],
            "empty-error ThrottleTimeMs bodies: v3 == v4"
        );
        assert_eq!(
            &v4_with[..],
            &v5_with[..],
            "empty-error ThrottleTimeMs bodies: v4 == v5"
        );
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
    fn init_producer_id_request_build_matches_java() {
        // Java 4.0 InitProducerIdRequest.Builder.build: rejects
        // transactionTimeoutMs <= 0 and an empty (non-null) transactional
        // id (IllegalArgumentException). Null transactional id is
        // idempotent produce. Official Java
        // InitProducerIdRequest.Builder.build. Encode still writes
        // independently after this helper. This crate speaks 0-5. This
        // is not getErrorResponse / errorCounts.
        InitProducerIdRequest::build(45_000, Some("tid")).unwrap();
        InitProducerIdRequest::build(45_000, None).unwrap();
        InitProducerIdRequest::build(1, Some("tid")).unwrap();
        let timeout = InitProducerIdRequest::build(0, Some("tid")).unwrap_err();
        assert!(
            matches!(timeout, Error::Protocol(_)),
            "non-positive timeout is Java IllegalArgumentException, got {timeout}"
        );
        assert!(
            timeout.to_string().contains("not positive"),
            "got {timeout}"
        );
        let negative = InitProducerIdRequest::build(-1, None).unwrap_err();
        assert!(
            matches!(negative, Error::Protocol(_)),
            "negative timeout is Java IllegalArgumentException, got {negative}"
        );
        let empty = InitProducerIdRequest::build(45_000, Some("")).unwrap_err();
        assert!(
            matches!(empty, Error::Protocol(_)),
            "empty transactional id is Java IllegalArgumentException, got {empty}"
        );
        assert!(
            empty.to_string().contains("non-empty transactional id"),
            "got {empty}"
        );
        leftover_init_producer_id_build(0, Some("tid"), 45_000);
        leftover_init_producer_id_build(0, None, 45_000);
        leftover_init_producer_id_build(1, Some("tid"), 45_000);
        leftover_init_producer_id_build(1, None, 45_000);
        leftover_init_producer_id_build(2, Some("tid"), 45_000);
        leftover_init_producer_id_build(2, None, 45_000);
        leftover_init_producer_id_build(5, Some("tid"), 45_000);
        leftover_init_producer_id_build(5, None, 45_000);
    }

    fn leftover_init_producer_id_build(
        version: i16,
        transactional_id: Option<&str>,
        transaction_timeout_ms: i32,
    ) {
        InitProducerIdRequest::build(transaction_timeout_ms, transactional_id).unwrap();
        let mut buf = BytesMut::new();
        encode_init_producer_id_request(
            &mut buf,
            version,
            transactional_id,
            transaction_timeout_ms,
            RecordBatch::NO_PRODUCER_ID,
            RecordBatch::NO_PRODUCER_EPOCH,
        )
        .unwrap();
        let mut cur = buf.as_ref();
        let (tid, timeout, pid, epoch) =
            decode_init_producer_id_request(&mut cur, version).unwrap();
        assert_eq!(tid.as_deref(), transactional_id);
        assert_eq!(timeout, transaction_timeout_ms);
        assert_eq!(pid, RecordBatch::NO_PRODUCER_ID);
        assert_eq!(epoch, RecordBatch::NO_PRODUCER_EPOCH);
        let empty = if transactional_id.is_none() {
            "null "
        } else {
            ""
        };
        assert!(
            cur.is_empty(),
            "InitProducerId v{version} Builder.build {empty}leftover-empty; leftover {} bytes",
            cur.len()
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
        let (err, pid, epoch, ..) = decode_init_producer_id_response(&mut &resp[..], 1).unwrap();
        assert_eq!(err, 58);
        assert_eq!(pid, RecordBatch::NO_PRODUCER_ID);
        assert_eq!(epoch, RecordBatch::NO_PRODUCER_EPOCH);
        assert!(Error::broker(err, "InitProducerId")
            .to_string()
            .contains("58"));
    }

    #[test]
    fn init_producer_id_error_response_matches_java() {
        // Java InitProducerIdRequest.getErrorResponse: NO_PRODUCER_ID /
        // NO_PRODUCER_EPOCH, throttle always 0 (ignores throttleTimeMs).
        for version in [0_i16, 1, 2, 5] {
            let mut expected = BytesMut::new();
            encode_init_producer_id_response(
                &mut expected,
                version,
                16,
                RecordBatch::NO_PRODUCER_ID,
                RecordBatch::NO_PRODUCER_EPOCH,
            )
            .unwrap();
            let mut got = BytesMut::new();
            InitProducerIdRequest::error_response(&mut got, version, 16).unwrap();
            assert_eq!(
                &got[..],
                &expected[..],
                "InitProducerId v{version} getErrorResponse must match sentinel encode"
            );
            let mut cur = &got[..];
            let (err, pid, epoch, throttle) =
                decode_init_producer_id_response(&mut cur, version).unwrap();
            assert_eq!(err, 16);
            assert_eq!(pid, RecordBatch::NO_PRODUCER_ID);
            assert_eq!(epoch, RecordBatch::NO_PRODUCER_EPOCH);
            assert_eq!(throttle, 0);
            assert!(
                cur.is_empty(),
                "InitProducerId v{version} getErrorResponse leftover-empty; leftover {} bytes",
                cur.len()
            );
        }
        let mut v0 = BytesMut::new();
        InitProducerIdRequest::error_response(&mut v0, 0, 16).unwrap();
        let mut v1 = BytesMut::new();
        InitProducerIdRequest::error_response(&mut v1, 1, 16).unwrap();
        assert_eq!(
            &v0[..],
            &v1[..],
            "empty-error ThrottleTimeMs bodies: v0 == v1"
        );
        let mut v2 = BytesMut::new();
        InitProducerIdRequest::error_response(&mut v2, 2, 16).unwrap();
        assert_ne!(&v1[..], &v2[..], "v2+ getErrorResponse is flexible");
    }

    #[test]
    fn init_producer_id_response_error_counts_matches_java() {
        // Java InitProducerIdResponse.errorCounts:
        // Collections.singletonMap(Errors.forCode(data.errorCode()), 1),
        // including NONE. Official Java InitProducerIdResponse.errorCounts.
        // This is not InitProducerIdResponse.error / EndTxn errorCounts /
        // AddOffsetsToTxn errorCounts / Heartbeat errorCounts.
        assert_eq!(
            InitProducerIdResponse::error_counts(0),
            HashMap::from([(0, 1)]),
            "NONE is a singleton 1, not an empty map"
        );
        assert_eq!(
            InitProducerIdResponse::error_counts(crate::error::NOT_COORDINATOR),
            HashMap::from([(crate::error::NOT_COORDINATOR, 1)])
        );
        for version in 0..=5_i16 {
            let mut resp = BytesMut::new();
            encode_init_producer_id_response(
                &mut resp,
                version,
                crate::error::NOT_COORDINATOR,
                RecordBatch::NO_PRODUCER_ID,
                RecordBatch::NO_PRODUCER_EPOCH,
            )
            .unwrap();
            let mut cur = &resp[..];
            let (err, ..) = decode_init_producer_id_response(&mut cur, version).unwrap();
            assert_eq!(
                InitProducerIdResponse::error_counts(err),
                HashMap::from([(crate::error::NOT_COORDINATOR, 1)]),
                "InitProducerId v{version} errorCounts must count the decoded code"
            );
            assert!(
                cur.is_empty(),
                "InitProducerId v{version} errorCounts leftover-empty; leftover {} bytes",
                cur.len()
            );
        }
    }
}

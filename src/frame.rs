//! TCP size-prefix framing only. Request/response **bodies** are
//! `kafka_protocol` `Encodable` / `Decodable` types.

use bytes::{Buf, BufMut, Bytes, BytesMut};
use kafka_protocol::messages::{ApiKey, RequestHeader, ResponseHeader};
use kafka_protocol::protocol::{encode_request_header_into_buffer, Decodable, Encodable};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

use crate::error::{Error, Result};

/// Kafka max request we will accept from a broker (64 MiB).
pub const MAX_FRAME: i32 = 64 * 1024 * 1024;

/// Encode `header` + `body` (already versioned) into a size-prefixed frame.
///
/// One buffer: length placeholder, then header+body, then patch the prefix.
/// A second `BytesMut` + `extend_from_slice` copied every Produce body
/// (~1 MiB at Lab A `batch.size`) on the actor before `write_frame`.
pub fn encode_request<R: Encodable>(
    header: &RequestHeader,
    body: &R,
    api_version: i16,
) -> Result<Bytes> {
    let mut frame = BytesMut::new();
    frame.put_i32(0);
    encode_request_header_into_buffer(&mut frame, header).map_err(Error::protocol)?;
    body.encode(&mut frame, api_version)
        .map_err(Error::protocol)?;
    let inner_len = frame.len().saturating_sub(4);
    let inner_len = i32::try_from(inner_len).map_err(|_| Error::FrameTooLarge(i32::MAX))?;
    if inner_len < 0 || inner_len > MAX_FRAME {
        return Err(Error::FrameTooLarge(inner_len));
    }
    frame[0..4].copy_from_slice(&inner_len.to_be_bytes());
    Ok(frame.freeze())
}

/// Decode a response header at the version implied by `api_key` + `api_version`.
pub fn decode_response_header(
    buf: &mut Bytes,
    api_key: ApiKey,
    api_version: i16,
) -> Result<ResponseHeader> {
    let hv = api_key.response_header_version(api_version);
    ResponseHeader::decode(buf, hv).map_err(Error::protocol)
}

/// Decode a response body after the header has been consumed.
pub fn decode_response_body<R: Decodable>(buf: &mut Bytes, api_version: i16) -> Result<R> {
    R::decode(buf, api_version).map_err(Error::protocol)
}

/// Write one size-prefixed frame.
pub async fn write_frame<W: AsyncWrite + Unpin>(w: &mut W, frame: &[u8]) -> Result<()> {
    w.write_all(frame).await?;
    Ok(())
}

/// Read one size-prefixed frame (length not included in the returned bytes).
pub async fn read_frame<R: AsyncRead + Unpin>(r: &mut R) -> Result<Bytes> {
    let mut len_buf = [0u8; 4];
    r.read_exact(&mut len_buf).await?;
    let len = i32::from_be_bytes(len_buf);
    if len < 0 || len > MAX_FRAME {
        return Err(Error::FrameTooLarge(len));
    }
    let mut body = vec![0u8; len as usize];
    r.read_exact(&mut body).await?;
    Ok(Bytes::from(body))
}

/// Helper: peek correlation id (first i32 of a response body, all header versions).
pub fn correlation_id(response_frame: &[u8]) -> Result<i32> {
    if response_frame.len() < 4 {
        return Err(Error::protocol("response shorter than correlation id"));
    }
    Ok((&response_frame[..4]).get_i32())
}

#[cfg(test)]
mod tests {
    use super::*;
    use kafka_protocol::messages::produce_request::{PartitionProduceData, TopicProduceData};
    use kafka_protocol::messages::{ApiKey, MetadataRequest, ProduceRequest, TopicName};
    use kafka_protocol::protocol::StrBytes;

    fn prefix_len(frame: &[u8]) -> i32 {
        i32::from_be_bytes(frame[0..4].try_into().unwrap())
    }

    #[test]
    fn encode_request_length_prefix_matches_body() {
        let header = RequestHeader::default()
            .with_request_api_key(ApiKey::Metadata as i16)
            .with_request_api_version(12)
            .with_correlation_id(7)
            .with_client_id(Some(StrBytes::from_static_str("partitionline")));
        let frame = encode_request(&header, &MetadataRequest::default(), 12).unwrap();
        assert!(frame.len() > 4);
        assert_eq!(prefix_len(&frame) as usize, frame.len() - 4);
    }

    #[test]
    fn encode_request_keeps_record_bytes() {
        let rec = Bytes::from(vec![0xab; 10_000]);
        let body = ProduceRequest::default()
            .with_acks(-1)
            .with_timeout_ms(1000)
            .with_topic_data(vec![TopicProduceData::default()
                .with_name(TopicName(StrBytes::from_static_str("bench")))
                .with_partition_data(vec![PartitionProduceData::default()
                    .with_index(0)
                    .with_records(Some(rec.clone()))])]);
        let header = RequestHeader::default()
            .with_request_api_key(ApiKey::Produce as i16)
            .with_request_api_version(9)
            .with_correlation_id(1)
            .with_client_id(Some(StrBytes::from_static_str("partitionline")));
        let frame = encode_request(&header, &body, 9).unwrap();
        assert_eq!(prefix_len(&frame) as usize, frame.len() - 4);
        assert!(
            frame.windows(rec.len()).any(|w| w == rec.as_ref()),
            "single-buffer encode must keep the records payload"
        );
    }
}

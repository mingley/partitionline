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
pub fn encode_request<R: Encodable>(
    header: &RequestHeader,
    body: &R,
    api_version: i16,
) -> Result<Bytes> {
    let mut inner = BytesMut::new();
    encode_request_header_into_buffer(&mut inner, header).map_err(Error::protocol)?;
    body.encode(&mut inner, api_version)
        .map_err(Error::protocol)?;
    let mut frame = BytesMut::with_capacity(4 + inner.len());
    frame.put_i32(inner.len() as i32);
    frame.extend_from_slice(&inner);
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

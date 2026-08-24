use std::time::Duration;

use bytes::{BufMut, Bytes, BytesMut};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::time::timeout;

use crate::error::{Error, Result};
use crate::protocol::header::{decode_response_header, encode_request_header, RequestHeader};

pub const MAX_FRAME: i32 = 100 * 1024 * 1024;

pub struct BrokerConn {
    stream: TcpStream,
    read_buf: BytesMut,
    next_correlation: i32,
    client_id: String,
    addr: String,
}

impl BrokerConn {
    pub async fn connect(addr: &str, client_id: &str, connect_timeout: Duration) -> Result<Self> {
        let stream = timeout(connect_timeout, TcpStream::connect(addr))
            .await
            .map_err(|_| Error::Timeout)??;
        stream.set_nodelay(true)?;
        Ok(Self {
            stream,
            read_buf: BytesMut::with_capacity(8 * 1024),
            next_correlation: 1,
            client_id: client_id.to_string(),
            addr: addr.to_string(),
        })
    }

    pub fn addr(&self) -> &str {
        &self.addr
    }

    pub async fn send(
        &mut self,
        api_key: i16,
        api_version: i16,
        encode_body: impl FnOnce(&mut BytesMut),
        request_timeout: Duration,
    ) -> Result<i32> {
        let correlation = self.next_correlation;
        self.next_correlation = self.next_correlation.wrapping_add(1);
        let mut buf = BytesMut::new();
        buf.put_i32(0);
        encode_request_header(
            &mut buf,
            &RequestHeader {
                api_key,
                api_version,
                correlation_id: correlation,
                client_id: Some(self.client_id.clone()),
            },
        );
        encode_body(&mut buf);
        let size = (buf.len() - 4) as i32;
        buf[0..4].copy_from_slice(&size.to_be_bytes());
        timeout(request_timeout, async {
            self.stream.write_all(&buf).await?;
            self.stream.flush().await
        })
        .await
        .map_err(|_| Error::Timeout)??;
        Ok(correlation)
    }

    pub async fn roundtrip(
        &mut self,
        api_key: i16,
        api_version: i16,
        encode_body: impl FnOnce(&mut BytesMut),
        request_timeout: Duration,
    ) -> Result<Bytes> {
        let correlation = self
            .send(api_key, api_version, encode_body, request_timeout)
            .await?;
        let frame = timeout(request_timeout, self.read_frame())
            .await
            .map_err(|_| Error::Timeout)??;
        let mut cur = frame;
        let header = decode_response_header(&mut cur, api_key, api_version)?;
        if header.correlation_id != correlation {
            return Err(Error::protocol(format!(
                "correlation mismatch: sent {correlation}, got {}",
                header.correlation_id
            )));
        }
        Ok(cur)
    }

    async fn read_frame(&mut self) -> Result<Bytes> {
        loop {
            if self.read_buf.len() >= 4 {
                let size = i32::from_be_bytes(self.read_buf[0..4].try_into().unwrap());
                if !(0..=MAX_FRAME).contains(&size) {
                    return Err(Error::protocol(format!("invalid frame size {size}")));
                }
                let total = 4 + size as usize;
                if self.read_buf.len() >= total {
                    let mut frame = self.read_buf.split_to(total);
                    let _ = frame.split_to(4);
                    return Ok(frame.freeze());
                }
            }
            let n = self.stream.read_buf(&mut self.read_buf).await?;
            if n == 0 {
                return Err(Error::Io(std::io::Error::new(
                    std::io::ErrorKind::UnexpectedEof,
                    "broker closed connection",
                )));
            }
        }
    }
}

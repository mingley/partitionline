//! One TCP connection to a Kafka broker. Bodies are kafka-protocol types.
//!
//! Writes are pipelined: several requests may be in flight on one socket.
//! The reader task demuxes responses by the first i32 (correlation id).
//! `connect` always runs ApiVersions on that socket before returning.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicI32, Ordering};
use std::sync::Arc;

use kafka_protocol::messages::{ApiKey, ApiVersionsResponse, RequestHeader};
use kafka_protocol::protocol::{Decodable, Encodable, StrBytes};
use tokio::io::{AsyncWriteExt, BufReader, BufWriter};
use tokio::net::{
    tcp::{OwnedReadHalf, OwnedWriteHalf},
    TcpStream,
};
use tokio::sync::{oneshot, Mutex};

use crate::error::{Error, Result};
use crate::frame::{
    correlation_id, decode_response_body, decode_response_header, encode_request, read_frame,
    write_frame,
};
use crate::{CLIENT_NAME, VERSION};

type PendingTx = oneshot::Sender<Result<bytes::Bytes>>;

/// Shared with the reader task. Not held by the write half, so dropping the
/// last [`Broker`] closes the socket and the reader exits on EOF.
struct Shared {
    pending: std::sync::Mutex<HashMap<i32, PendingTx>>,
    next_corr: AtomicI32,
    client_id: StrBytes,
    api_versions: std::sync::Mutex<Option<ApiVersionsResponse>>,
}

struct BrokerInner {
    writer: Mutex<BufWriter<OwnedWriteHalf>>,
    shared: Arc<Shared>,
}

/// Pipelined request/response connection. Clone shares the same socket.
#[derive(Clone)]
pub struct Broker {
    inner: Arc<BrokerInner>,
    /// Peer, for logs and metadata matching.
    pub addr: SocketAddr,
}

impl Broker {
    /// Connect to `host:port` and run ApiVersions on this connection.
    pub async fn connect(host: &str, port: u16) -> Result<Self> {
        let stream = TcpStream::connect((host, port)).await?;
        stream.set_nodelay(true)?;
        let broker = Self::from_stream(stream)?;
        broker.handshake().await?;
        Ok(broker)
    }

    fn from_stream(stream: TcpStream) -> Result<Self> {
        let addr = stream.peer_addr()?;
        let (r, w) = stream.into_split();
        let shared = Arc::new(Shared {
            pending: std::sync::Mutex::new(HashMap::new()),
            next_corr: AtomicI32::new(1),
            client_id: StrBytes::from_static_str(CLIENT_NAME),
            api_versions: std::sync::Mutex::new(None),
        });
        let inner = Arc::new(BrokerInner {
            writer: Mutex::new(BufWriter::new(w)),
            shared: Arc::clone(&shared),
        });
        tokio::spawn(reader_loop(BufReader::new(r), shared));
        Ok(Self { inner, addr })
    }

    async fn handshake(&self) -> Result<ApiVersionsResponse> {
        let resp = self.api_versions().await?;
        Error::check(resp.error_code)?;
        *self.inner.shared.api_versions.lock().unwrap() = Some(resp.clone());
        Ok(resp)
    }

    /// ApiVersions body from the handshake on **this** socket.
    pub fn last_api_versions(&self) -> Option<ApiVersionsResponse> {
        self.inner.shared.api_versions.lock().unwrap().clone()
    }

    fn alloc_corr(&self) -> i32 {
        loop {
            let c = self.inner.shared.next_corr.fetch_add(1, Ordering::Relaxed);
            if c > 0 {
                return c;
            }
            self.inner.shared.next_corr.store(1, Ordering::Relaxed);
        }
    }

    /// Encode `body` as `api_key`/`api_version`, write, wait for the matching
    /// correlation id. Safe to call concurrently on clones of the same connection.
    pub async fn call<Req, Resp>(
        &self,
        api_key: ApiKey,
        api_version: i16,
        body: &Req,
    ) -> Result<Resp>
    where
        Req: Encodable,
        Resp: Decodable,
    {
        let corr = self.alloc_corr();
        let header = RequestHeader::default()
            .with_request_api_key(api_key as i16)
            .with_request_api_version(api_version)
            .with_correlation_id(corr)
            .with_client_id(Some(self.inner.shared.client_id.clone()));

        let frame = encode_request(&header, body, api_version)?;
        let (tx, rx) = oneshot::channel();
        {
            let mut pending = self.inner.shared.pending.lock().unwrap();
            pending.insert(corr, tx);
        }

        let write_res = {
            let mut w = self.inner.writer.lock().await;
            match write_frame(&mut *w, &frame).await {
                Ok(()) => AsyncWriteExt::flush(&mut *w).await.map_err(Error::from),
                Err(e) => Err(e),
            }
        };
        if let Err(e) = write_res {
            self.inner.shared.pending.lock().unwrap().remove(&corr);
            return Err(e);
        }

        let mut resp = rx
            .await
            .map_err(|_| Error::protocol("broker connection closed"))??;
        let header = decode_response_header(&mut resp, api_key, api_version)?;
        if header.correlation_id != corr {
            return Err(Error::Correlation {
                expected: corr,
                got: header.correlation_id,
            });
        }
        decode_response_body(&mut resp, api_version)
    }

    /// ApiVersions v3 handshake (flexible body; header is the special case).
    pub async fn api_versions(&self) -> Result<ApiVersionsResponse> {
        let req = kafka_protocol::messages::ApiVersionsRequest::default()
            .with_client_software_name(StrBytes::from_static_str(CLIENT_NAME))
            .with_client_software_version(StrBytes::from_string(VERSION.to_string()));
        self.call(ApiKey::ApiVersions, 3, &req).await
    }
}

async fn reader_loop(mut reader: BufReader<OwnedReadHalf>, shared: Arc<Shared>) {
    loop {
        match read_frame(&mut reader).await {
            Ok(frame) => {
                let got = match correlation_id(&frame) {
                    Ok(c) => c,
                    Err(e) => {
                        fail_all(&shared, e);
                        break;
                    }
                };
                let waiter = shared.pending.lock().unwrap().remove(&got);
                match waiter {
                    Some(tx) => {
                        let _ = tx.send(Ok(frame));
                    }
                    None => {}
                }
            }
            Err(e) => {
                fail_all(&shared, e);
                break;
            }
        }
    }
}

fn fail_all(shared: &Shared, err: Error) {
    let msg = err.to_string();
    let mut pending = shared.pending.lock().unwrap();
    for (_, tx) in pending.drain() {
        let _ = tx.send(Err(Error::protocol(msg.clone())));
    }
}

/// Parse `"host:port"` bootstrap entries.
pub fn parse_bootstrap(s: &str) -> Result<(String, u16)> {
    let (host, port) = s.rsplit_once(':').ok_or(Error::NoBootstrap)?;
    let port: u16 = port.parse().map_err(|_| Error::NoBootstrap)?;
    if host.is_empty() {
        return Err(Error::NoBootstrap);
    }
    Ok((host.trim_matches(['[', ']']).to_string(), port))
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::{BufMut, BytesMut};
    use kafka_protocol::messages::MetadataRequest;
    use kafka_protocol::protocol::buf::ByteBuf;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;

    struct EmptyResp;

    impl Decodable for EmptyResp {
        fn decode<B: ByteBuf>(_buf: &mut B, _version: i16) -> anyhow::Result<Self> {
            Ok(EmptyResp)
        }
    }

    fn empty_response_frame(corr: i32) -> bytes::Bytes {
        // Flexible response header: correlation id + empty tagged fields.
        let mut inner = BytesMut::new();
        inner.put_i32(corr);
        inner.put_u8(0);
        let mut frame = BytesMut::new();
        frame.put_i32(inner.len() as i32);
        frame.extend_from_slice(&inner);
        frame.freeze()
    }

    #[tokio::test]
    async fn pipeline_correlates_out_of_order() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut sock, _) = listener.accept().await.unwrap();
            let mut frames = Vec::new();
            for _ in 0..2 {
                let mut len_buf = [0u8; 4];
                sock.read_exact(&mut len_buf).await.unwrap();
                let len = i32::from_be_bytes(len_buf) as usize;
                let mut body = vec![0u8; len];
                sock.read_exact(&mut body).await.unwrap();
                // RequestHeader: api_key i16, api_version i16, correlation_id i32.
                let corr = i32::from_be_bytes(body[4..8].try_into().unwrap());
                frames.push(corr);
            }
            // Reply newest first so correlation (not arrival order) must be used.
            for corr in frames.into_iter().rev() {
                sock.write_all(&empty_response_frame(corr)).await.unwrap();
            }
            sock.flush().await.unwrap();
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        });

        let stream = TcpStream::connect(addr).await.unwrap();
        let broker = Broker::from_stream(stream).unwrap();
        let other = broker.clone();
        let req = MetadataRequest::default();
        let a = tokio::spawn({
            let broker = broker.clone();
            let req = req.clone();
            async move {
                broker
                    .call::<_, EmptyResp>(ApiKey::Metadata, 12, &req)
                    .await
            }
        });
        let b =
            tokio::spawn(
                async move { other.call::<_, EmptyResp>(ApiKey::Metadata, 12, &req).await },
            );
        a.await.unwrap().unwrap();
        b.await.unwrap().unwrap();
        server.await.unwrap();
    }
}

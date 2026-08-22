//! One TCP connection to a Kafka broker. Bodies are kafka-protocol types.

use std::net::SocketAddr;

use kafka_protocol::messages::{ApiKey, RequestHeader};
use kafka_protocol::protocol::{Decodable, Encodable, StrBytes};
use tokio::io::{BufReader, BufWriter};
use tokio::net::{
    tcp::{OwnedReadHalf, OwnedWriteHalf},
    TcpStream,
};

use crate::error::{Error, Result};
use crate::frame::{
    correlation_id, decode_response_body, decode_response_header, encode_request, read_frame,
    write_frame,
};
use crate::{CLIENT_NAME, VERSION};

/// Sequential request/response connection (pipeline comes later).
pub struct Broker {
    reader: BufReader<OwnedReadHalf>,
    writer: BufWriter<OwnedWriteHalf>,
    next_corr: i32,
    client_id: StrBytes,
    /// Peer, for logs and metadata matching.
    pub addr: SocketAddr,
}

impl Broker {
    /// Connect to `host:port`.
    pub async fn connect(host: &str, port: u16) -> Result<Self> {
        let stream = TcpStream::connect((host, port)).await?;
        stream.set_nodelay(true)?;
        let addr = stream.peer_addr()?;
        let (r, w) = stream.into_split();
        Ok(Self {
            reader: BufReader::new(r),
            writer: BufWriter::new(w),
            next_corr: 1,
            client_id: StrBytes::from_static_str(CLIENT_NAME),
            addr,
        })
    }

    /// Encode `body` as `api_key`/`api_version`, write, read, decode `Resp`.
    pub async fn call<Req, Resp>(
        &mut self,
        api_key: ApiKey,
        api_version: i16,
        body: &Req,
    ) -> Result<Resp>
    where
        Req: Encodable,
        Resp: Decodable,
    {
        let corr = self.next_corr;
        self.next_corr = self.next_corr.wrapping_add(1);
        if self.next_corr <= 0 {
            self.next_corr = 1;
        }

        let header = RequestHeader::default()
            .with_request_api_key(api_key as i16)
            .with_request_api_version(api_version)
            .with_correlation_id(corr)
            .with_client_id(Some(self.client_id.clone()));

        let frame = encode_request(&header, body, api_version)?;
        write_frame(&mut self.writer, &frame).await?;
        tokio::io::AsyncWriteExt::flush(&mut self.writer).await?;

        let mut resp = read_frame(&mut self.reader).await?;
        let got = correlation_id(&resp)?;
        if got != corr {
            return Err(Error::Correlation {
                expected: corr,
                got,
            });
        }
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
    pub async fn api_versions(&mut self) -> Result<kafka_protocol::messages::ApiVersionsResponse> {
        let req = kafka_protocol::messages::ApiVersionsRequest::default()
            .with_client_software_name(StrBytes::from_static_str(CLIENT_NAME))
            .with_client_software_version(StrBytes::from_string(VERSION.to_string()));
        self.call(ApiKey::ApiVersions, 3, &req).await
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

//! ApiVersions is exchanged before SASL, so its response must not trust counts.
use std::{io, time::Duration};

use bytes::{Buf, BufMut, BytesMut};
use partitionline::net::BrokerConn;
use partitionline::protocol::api::{
    encode_api_versions_response, negotiate_api_versions, ApiVersion, ApiVersionsResponse,
};
use partitionline::protocol::buf;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

async fn serve_one(
    body: BytesMut,
) -> io::Result<(String, tokio::task::JoinHandle<io::Result<()>>)> {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let address = listener.local_addr()?.to_string();
    let task = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await?;
        let mut size = [0u8; 4];
        let _ = socket.read_exact(&mut size).await?;
        let length = usize::try_from(i32::from_be_bytes(size))
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "negative frame size"))?;
        if !(8..=4096).contains(&length) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "unexpected frame size",
            ));
        }
        let mut request = vec![0; length];
        let _ = socket.read_exact(&mut request).await?;
        let mut request = request.as_slice();
        let api = request.get_i16();
        let version = request.get_i16();
        let correlation = request.get_i32();
        if api != 18 || version != 4 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "unexpected API version request",
            ));
        }
        let frame_len = body
            .len()
            .checked_add(4)
            .and_then(|n| i32::try_from(n).ok())
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "response too large"))?;
        socket.write_all(&frame_len.to_be_bytes()).await?;
        socket.write_all(&correlation.to_be_bytes()).await?;
        socket.write_all(&body).await?;
        // Keep the peer open until the client has consumed the response.
        let mut tail = [0u8; 1];
        let _closed = tokio::time::timeout(Duration::from_secs(2), socket.read(&mut tail)).await;
        Ok(())
    });
    Ok((address, task))
}

#[tokio::test]
async fn preauth_rejects_padded_array_count_from_real_socket() {
    let count = 100_000usize;
    let mut body = BytesMut::new();
    body.put_i16(0);
    buf::put_array_len(&mut body, true, Some(count)).unwrap();
    body.resize(body.len() + count, 0);
    let (address, peer) = serve_one(body).await.unwrap();
    let mut connection = BrokerConn::connect(&address, "budget-test", Duration::from_secs(2))
        .await
        .unwrap();
    let error = negotiate_api_versions(&mut connection, Duration::from_secs(2))
        .await
        .unwrap_err();
    assert!(
        matches!(error, partitionline::Error::Protocol(_)),
        "{error:?}"
    );
    drop(connection);
    peer.await.unwrap().unwrap();
}

#[tokio::test]
async fn preauth_accepts_large_valid_api_version_list() {
    let count = 100_000usize;
    let response = ApiVersionsResponse {
        api_keys: (0..count)
            .map(|index| ApiVersion {
                api_key: i16::try_from(index % 1000).unwrap(),
                min_version: 0,
                max_version: 4,
            })
            .collect(),
        ..ApiVersionsResponse::default()
    };
    let mut body = BytesMut::new();
    encode_api_versions_response(&mut body, 4, &response).unwrap();
    let (address, peer) = serve_one(body).await.unwrap();
    let mut connection = BrokerConn::connect(&address, "budget-test", Duration::from_secs(2))
        .await
        .unwrap();
    let decoded = negotiate_api_versions(&mut connection, Duration::from_secs(2))
        .await
        .unwrap();
    assert_eq!(decoded.api_keys, response.api_keys);
    drop(connection);
    peer.await.unwrap().unwrap();
}

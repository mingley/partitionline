#![cfg(feature = "registry")]

use partitionline_schema::registry::{RegistryClient, RegistryClientConfig, RegistryError};
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

async fn client_for(response: Vec<u8>) -> RegistryClient {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        let mut request = Vec::new();
        let mut buf = [0; 1024];
        loop {
            let n = socket.read(&mut buf).await.unwrap();
            assert_ne!(n, 0);
            request.extend_from_slice(&buf[..n]);
            if request.windows(4).any(|w| w == b"\r\n\r\n") {
                break;
            }
            assert!(request.len() < 8192);
        }
        let _result = socket.write_all(&response).await;
        let _closed = socket.shutdown().await;
    });
    RegistryClient::new(
        RegistryClientConfig::new(format!("http://{addr}"))
            .max_attempts(1)
            .request_timeout(Duration::from_secs(3)),
    )
    .unwrap()
}

#[tokio::test]
async fn informational_response_precedes_a_successful_schema_lookup() {
    let body = br#"{"schema":"ok"}"#;
    for framing in 0..3 {
        let mut wire = b"HTTP/1.1 103 Early Hints\r\nLink: </schema>\r\n\r\n".to_vec();
        match framing {
            0 => {
                wire.extend_from_slice(
                    format!("HTTP/1.1 200 OK\r\nContent-Length: {}\r\n\r\n", body.len()).as_bytes(),
                );
                wire.extend_from_slice(body);
            }
            1 => {
                wire.extend_from_slice(b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n");
                wire.extend_from_slice(format!("{:x}\r\n", body.len()).as_bytes());
                wire.extend_from_slice(body);
                wire.extend_from_slice(b"\r\n0\r\n\r\n");
            }
            _ => {
                wire.extend_from_slice(b"HTTP/1.1 200 OK\r\nConnection: close\r\n\r\n");
                wire.extend_from_slice(body);
            }
        }
        let schema = client_for(wire).await.get_schema_by_id(7).await.unwrap();
        assert_eq!(schema.id, 7);
        assert_eq!(schema.schema, "ok");
    }
}

#[tokio::test]
async fn unsupported_transfer_coding_is_not_silently_accepted() {
    let body = br#"{"schema":"ok"}"#;
    let mut wire = b"HTTP/1.1 200 OK\r\nTransfer-Encoding: gzip, chunked\r\n\r\n".to_vec();
    wire.extend_from_slice(format!("{:x}\r\n", body.len()).as_bytes());
    wire.extend_from_slice(body);
    wire.extend_from_slice(b"\r\n0\r\n\r\n");
    let result = client_for(wire).await.get_schema_by_id(7).await;
    assert!(matches!(result, Err(RegistryError::Malformed { .. })), "{result:?}");
}

#[tokio::test]
async fn switching_protocols_returns_unsupported_status() {
    let wire = b"HTTP/1.1 101 Switching Protocols\r\nUpgrade: example\r\nConnection: Upgrade\r\n\r\nHTTP/1.1 200 OK\r\nContent-Length: 15\r\n\r\n{\"schema\":\"ok\"}".to_vec();
    let result = client_for(wire).await.get_schema_by_id(7).await;
    assert!(matches!(
        result,
        Err(RegistryError::UnexpectedStatus { status: 101 })
    ));
}

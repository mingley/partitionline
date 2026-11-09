#![cfg(feature = "registry")]

use partitionline_schema::registry::{RegistryClient, RegistryClientConfig, RegistryError};
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

async fn read_request(socket: &mut tokio::net::TcpStream) {
    let mut request = Vec::new();
    let mut chunk = [0; 1024];
    loop {
        let count = socket.read(&mut chunk).await.unwrap();
        assert_ne!(count, 0);
        request.extend_from_slice(&chunk[..count]);
        if request.windows(4).any(|bytes| bytes == b"\r\n\r\n") {
            return;
        }
        assert!(request.len() < 8192);
    }
}

async fn serve(response: Vec<u8>) -> String {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        read_request(&mut socket).await;
        let _written = socket.write_all(&response).await;
        let _closed = socket.shutdown().await;
    });
    format!("http://{addr}")
}

fn head() -> Vec<u8> {
    b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\nConnection: close\r\n\r\n".to_vec()
}

fn chunked(body: &[u8], chunk_size: usize) -> Vec<u8> {
    let mut response = head();
    for part in body.chunks(chunk_size) {
        response.extend_from_slice(format!("{:x}\r\n", part.len()).as_bytes());
        response.extend_from_slice(part);
        response.extend_from_slice(b"\r\n");
    }
    response.extend_from_slice(b"0\r\n\r\n");
    response
}

fn body_of_size(size: usize) -> String {
    let prefix = r#"{"schema":""#;
    let suffix = r#""}"#;
    format!("{prefix}{}{suffix}", "x".repeat(size - prefix.len() - suffix.len()))
}

fn client(base: String, limit: usize) -> RegistryClient {
    RegistryClient::new(
        RegistryClientConfig::new(base)
            .max_body_bytes(limit)
            .request_timeout(Duration::from_secs(10))
            .max_attempts(1),
    )
    .unwrap()
}

#[tokio::test]
async fn many_tiny_chunks_count_decoded_bytes_not_framing() {
    const LIMIT: usize = 32 * 1024;
    let body = body_of_size(30_000);
    let response = chunked(body.as_bytes(), 1);
    assert!(response.len() > LIMIT + 64 * 1024);
    let result = client(serve(response).await, LIMIT).get_schema_by_id(5).await;
    let schema = result.expect("valid tiny chunks within the decoded body limit");
    assert_eq!(schema.id, 5);
    assert_eq!(schema.schema.len(), 30_000 - r#"{"schema":""#.len() - r#""}"#.len());
}

#[tokio::test]
async fn chunked_body_boundary_is_enforced_on_decoded_data() {
    const LIMIT: usize = 1024;
    for (size, accepted) in [(LIMIT, true), (LIMIT + 1, false)] {
        let body = body_of_size(size);
        let response = chunked(body.as_bytes(), 1);
        let result = client(serve(response).await, LIMIT).get_schema_by_id(5).await;
        if accepted {
            assert!(result.is_ok(), "{result:?}");
        } else {
            assert!(matches!(result, Err(RegistryError::TooLarge { limit: LIMIT })), "{result:?}");
        }
    }
}

#[tokio::test]
async fn malformed_and_truncated_chunks_are_rejected() {
    for raw in [
        &b"z\r\n"[..],
        &b"2\r\nx"[..],
        &b"1\r\nxNO0\r\n\r\n"[..],
        &b"1\r\nx\r\n"[..],
    ] {
        let mut response = head();
        response.extend_from_slice(raw);
        let result = client(serve(response).await, 1024).get_schema_by_id(5).await;
        assert!(matches!(result, Err(RegistryError::Malformed { .. })), "{result:?}");
    }
}

#[tokio::test]
async fn canceling_a_partial_chunk_closes_the_socket() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let (ready_tx, ready_rx) = tokio::sync::oneshot::channel();
    let (closed_tx, closed_rx) = tokio::sync::oneshot::channel();
    tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        read_request(&mut socket).await;
        let mut response = head();
        response.extend_from_slice(b"100\r\nx");
        socket.write_all(&response).await.unwrap();
        let _ready = ready_tx.send(());
        let mut buf = [0; 32];
        let observed = match socket.read(&mut buf).await {
            Ok(count) => Ok(buf[..count].to_vec()),
            Err(error) => Err((error.kind(), error.raw_os_error(), error.to_string())),
        };
        let _sent = closed_tx.send(observed);
    });
    let client = client(format!("http://{addr}"), 1024);
    let mut call = Box::pin(client.get_schema_by_id(5));
    tokio::select! {
        result = &mut call => panic!("partial response unexpectedly finished: {result:?}"),
        result = ready_rx => result.unwrap(),
    }
    drop(call);
    let observed = tokio::time::timeout(Duration::from_secs(3), closed_rx)
        .await.unwrap().unwrap();
    println!("PL_SERVER_OBSERVATION {observed:?}");
    assert!(
        matches!(&observed, Ok(bytes) if bytes.is_empty())
            || matches!(&observed, Err((std::io::ErrorKind::ConnectionReset, _, _))),
        "server observed {observed:?}"
    );
}

#[tokio::test]
async fn stalled_partial_chunk_obeys_request_deadline() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let (ready_tx, ready_rx) = tokio::sync::oneshot::channel();
    let (closed_tx, closed_rx) = tokio::sync::oneshot::channel();
    tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        read_request(&mut socket).await;
        let mut response = head();
        response.extend_from_slice(b"100\r\nx");
        socket.write_all(&response).await.unwrap();
        let _ready = ready_tx.send(());
        let mut buf = [0; 32];
        let observed = match socket.read(&mut buf).await {
            Ok(count) => Ok(buf[..count].to_vec()),
            Err(error) => Err((error.kind(), error.raw_os_error(), error.to_string())),
        };
        let _sent = closed_tx.send(observed);
    });
    let client = RegistryClient::new(
        RegistryClientConfig::new(format!("http://{addr}"))
            .max_body_bytes(1024)
            .request_timeout(Duration::from_millis(300))
            .max_attempts(1),
    ).unwrap();
    let mut call = Box::pin(client.get_schema_by_id(5));
    tokio::select! {
        result = &mut call => panic!("partial response unexpectedly finished: {result:?}"),
        result = ready_rx => result.unwrap(),
    }
    let result = call.await;
    assert!(matches!(result, Err(RegistryError::Timeout)), "{result:?}");
    let observed = tokio::time::timeout(Duration::from_secs(3), closed_rx)
        .await.unwrap().unwrap();
    println!("PL_SERVER_OBSERVATION {observed:?}");
    assert!(matches!(&observed, Ok(bytes) if bytes.is_empty()), "server observed {observed:?}");
}

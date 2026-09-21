//! KL02-07: Network deadline and connection safety tests.
//!
//! Validates:
//! - Combined write and read delay cannot consume two independent full RPC budgets.
//! - A timeout or cancellation never returns a reusable desynchronized connection.
//! - Partial reads, cancellation, and early responses preserve framing and correlation safety.

use std::time::{Duration, Instant};

use bytes::BytesMut;
use partitionline::net::{BrokerConn, Deadline};
use partitionline::protocol::api_keys::API_VERSIONS;
use partitionline::Error;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

fn make_response_frame(correlation_id: i32, body: &[u8]) -> Vec<u8> {
    let frame_len = 4 + body.len();
    let mut frame = Vec::with_capacity(4 + frame_len);
    frame.extend_from_slice(&(frame_len as i32).to_be_bytes());
    frame.extend_from_slice(&correlation_id.to_be_bytes());
    frame.extend_from_slice(body);
    frame
}

#[tokio::test]
async fn combined_delay_cannot_consume_2x_budget() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap().to_string();

    let server_task = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();

        // Delay 60ms before reading the request.
        tokio::time::sleep(Duration::from_millis(60)).await;
        let mut size_buf = [0u8; 4];
        let _ = socket.read_exact(&mut size_buf).await.unwrap();
        let size = i32::from_be_bytes(size_buf) as usize;
        let mut req_buf = vec![0u8; size];
        let _ = socket.read_exact(&mut req_buf).await.unwrap();
        let correlation_id = i32::from_be_bytes(req_buf[4..8].try_into().unwrap());

        // Delay another 60ms before sending the response.
        tokio::time::sleep(Duration::from_millis(60)).await;
        let resp = make_response_frame(correlation_id, &[]);
        let _ = socket.write_all(&resp).await;
    });

    let budget = Duration::from_millis(100);
    let mut conn = BrokerConn::connect(&addr, "test-client", Duration::from_secs(1))
        .await
        .unwrap();

    let started = Instant::now();
    let result = conn.roundtrip(API_VERSIONS, 0, |_| Ok(()), budget).await;
    let elapsed = started.elapsed();

    assert!(
        matches!(result, Err(Error::Timeout)),
        "expected Timeout, got {result:?}"
    );
    // If write and read each got a fresh full budget (2x = 200ms),
    // this roundtrip would have succeeded at 120ms or timed out at 200ms.
    // Under the single shared deadline, it must fail around 100ms.
    assert!(
        elapsed < Duration::from_millis(160),
        "combined delay consumed too much time: {elapsed:?} (should be well below 2x budget of 200ms)"
    );
    assert!(
        elapsed >= Duration::from_millis(85),
        "elapsed {elapsed:?} should be close to 1x budget of 100ms"
    );
    assert!(conn.is_closed(), "timed out connection must be closed");

    let _ = server_task.await;
}

#[tokio::test]
async fn timeout_does_not_leave_reusable_desynchronized_connection() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap().to_string();

    let server_task = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        let mut size_buf = [0u8; 4];
        let _ = socket.read_exact(&mut size_buf).await.unwrap();
        let size = i32::from_be_bytes(size_buf) as usize;
        let mut req_buf = vec![0u8; size];
        let _ = socket.read_exact(&mut req_buf).await.unwrap();
        let correlation_id = i32::from_be_bytes(req_buf[4..8].try_into().unwrap());

        // Peer delays sending response past client's timeout.
        tokio::time::sleep(Duration::from_millis(100)).await;
        let resp = make_response_frame(correlation_id, &[]);
        let _ = socket.write_all(&resp).await;
    });

    let budget = Duration::from_millis(40);
    let mut conn = BrokerConn::connect(&addr, "test-client", Duration::from_secs(1))
        .await
        .unwrap();

    let result = conn.roundtrip(API_VERSIONS, 0, |_| Ok(()), budget).await;
    assert!(matches!(result, Err(Error::Timeout)));

    // Must be marked closed/unusable immediately.
    assert!(conn.is_closed());
    assert!(conn.idle_expired(Duration::from_secs(3600)));

    // Subsequent operations must fail immediately as Closed without touching wire.
    let reuse_roundtrip = conn
        .roundtrip(API_VERSIONS, 0, |_| Ok(()), Duration::from_millis(50))
        .await;
    assert!(
        matches!(reuse_roundtrip, Err(Error::Closed)),
        "expected Closed on reused timed-out connection, got {reuse_roundtrip:?}"
    );

    let reuse_send = conn
        .send(API_VERSIONS, 0, |_| Ok(()), Duration::from_millis(50))
        .await;
    assert!(matches!(reuse_send, Err(Error::Closed)));

    let reuse_read = conn
        .read_response(API_VERSIONS, 0, 1, Duration::from_millis(50))
        .await;
    assert!(matches!(reuse_read, Err(Error::Closed)));

    let reuse_write = conn
        .write_all_timeout(&[0u8; 4], Duration::from_millis(50))
        .await;
    assert!(matches!(reuse_write, Err(Error::Closed)));

    let _ = server_task.await;
}

#[tokio::test]
async fn partial_read_marks_connection_closed_and_not_reusable() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap().to_string();

    let server_task = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        let mut size_buf = [0u8; 4];
        let _ = socket.read_exact(&mut size_buf).await.unwrap();
        let size = i32::from_be_bytes(size_buf) as usize;
        let mut req_buf = vec![0u8; size];
        let _ = socket.read_exact(&mut req_buf).await.unwrap();

        // Send only 2 bytes of the 4-byte frame length header, then stall.
        socket.write_all(&[0, 0]).await.unwrap();
        tokio::time::sleep(Duration::from_millis(150)).await;
    });

    let mut conn = BrokerConn::connect(&addr, "test-client", Duration::from_secs(1))
        .await
        .unwrap();

    let budget = Duration::from_millis(40);
    let result = conn.roundtrip(API_VERSIONS, 0, |_| Ok(()), budget).await;
    assert!(matches!(result, Err(Error::Timeout)));

    assert!(
        conn.is_closed(),
        "partial read timeout must close connection"
    );
    let next = conn
        .roundtrip(API_VERSIONS, 0, |_| Ok(()), Duration::from_millis(50))
        .await;
    assert!(matches!(next, Err(Error::Closed)));

    let _ = server_task.await;
}

#[tokio::test]
async fn cancellation_mid_flight_marks_connection_closed_and_not_reusable() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap().to_string();

    let server_task = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        let mut size_buf = [0u8; 4];
        let _ = socket.read_exact(&mut size_buf).await.unwrap();
        let size = i32::from_be_bytes(size_buf) as usize;
        let mut req_buf = vec![0u8; size];
        let _ = socket.read_exact(&mut req_buf).await.unwrap();
        // Server hangs indefinitely.
        tokio::time::sleep(Duration::from_secs(5)).await;
    });

    let mut conn = BrokerConn::connect(&addr, "test-client", Duration::from_secs(1))
        .await
        .unwrap();

    // Cancel by dropping future via tokio::select! before deadline.
    tokio::select! {
        _ = conn.roundtrip(API_VERSIONS, 0, |_| Ok(()), Duration::from_secs(5)) => {
            panic!("should not resolve");
        }
        _ = tokio::time::sleep(Duration::from_millis(30)) => {}
    }

    assert!(
        conn.is_closed(),
        "cancelled future must mark connection closed"
    );
    let next = conn
        .roundtrip(API_VERSIONS, 0, |_| Ok(()), Duration::from_millis(50))
        .await;
    assert!(matches!(next, Err(Error::Closed)));

    server_task.abort();
}

#[tokio::test]
async fn correlation_mismatch_marks_connection_closed_and_not_reusable() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap().to_string();

    let server_task = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        let mut size_buf = [0u8; 4];
        let _ = socket.read_exact(&mut size_buf).await.unwrap();
        let size = i32::from_be_bytes(size_buf) as usize;
        let mut req_buf = vec![0u8; size];
        let _ = socket.read_exact(&mut req_buf).await.unwrap();

        // Send a response with mismatched correlation ID (9999).
        let resp = make_response_frame(9999, &[]);
        socket.write_all(&resp).await.unwrap();
    });

    let mut conn = BrokerConn::connect(&addr, "test-client", Duration::from_secs(1))
        .await
        .unwrap();

    let result = conn
        .roundtrip(API_VERSIONS, 0, |_| Ok(()), Duration::from_millis(100))
        .await;
    assert!(
        matches!(result, Err(Error::Protocol(_))),
        "expected Protocol error on correlation mismatch, got {result:?}"
    );

    assert!(
        conn.is_closed(),
        "correlation mismatch must close connection"
    );
    let next = conn
        .roundtrip(API_VERSIONS, 0, |_| Ok(()), Duration::from_millis(50))
        .await;
    assert!(matches!(next, Err(Error::Closed)));

    let _ = server_task.await;
}

#[tokio::test]
async fn healthy_connection_allows_sequential_roundtrips() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap().to_string();

    let server_task = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        for _ in 0..3 {
            let mut size_buf = [0u8; 4];
            let _ = socket.read_exact(&mut size_buf).await.unwrap();
            let size = i32::from_be_bytes(size_buf) as usize;
            let mut req_buf = vec![0u8; size];
            let _ = socket.read_exact(&mut req_buf).await.unwrap();
            let correlation_id = i32::from_be_bytes(req_buf[4..8].try_into().unwrap());

            let resp = make_response_frame(correlation_id, &[]);
            socket.write_all(&resp).await.unwrap();
        }
    });

    let mut conn = BrokerConn::connect(&addr, "test-client", Duration::from_secs(1))
        .await
        .unwrap();

    for _ in 0..3 {
        let res = conn
            .roundtrip(API_VERSIONS, 0, |_| Ok(()), Duration::from_millis(100))
            .await;
        assert!(res.is_ok(), "healthy roundtrip should succeed: {res:?}");
        assert!(!conn.is_closed());
    }

    let _ = server_task.await;
}

#[tokio::test]
async fn early_response_during_write_preserves_framing_and_correlation() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap().to_string();

    let server_task = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        // Read client request.
        let mut size_buf = [0u8; 4];
        let _ = socket.read_exact(&mut size_buf).await.unwrap();
        let size = i32::from_be_bytes(size_buf) as usize;
        let mut req_buf = vec![0u8; size];
        let _ = socket.read_exact(&mut req_buf).await.unwrap();
        let correlation_id = i32::from_be_bytes(req_buf[4..8].try_into().unwrap());

        // Immediately write response.
        let resp = make_response_frame(correlation_id, &[1, 2, 3, 4]);
        socket.write_all(&resp).await.unwrap();
    });

    let mut conn = BrokerConn::connect(&addr, "test-client", Duration::from_secs(1))
        .await
        .unwrap();

    let body = conn
        .roundtrip(
            API_VERSIONS,
            0,
            |buf: &mut BytesMut| {
                buf.extend_from_slice(&[0u8; 64]);
                Ok(())
            },
            Duration::from_millis(100),
        )
        .await
        .unwrap();

    assert_eq!(&body[..], &[1, 2, 3, 4]);
    assert!(!conn.is_closed());

    let _ = server_task.await;
}

#[tokio::test]
async fn explicit_deadline_contract_bounds_roundtrip() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap().to_string();

    let server_task = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        let mut size_buf = [0u8; 4];
        let _ = socket.read_exact(&mut size_buf).await.unwrap();
        let size = i32::from_be_bytes(size_buf) as usize;
        let mut req_buf = vec![0u8; size];
        let _ = socket.read_exact(&mut req_buf).await.unwrap();
        let correlation_id = i32::from_be_bytes(req_buf[4..8].try_into().unwrap());

        tokio::time::sleep(Duration::from_millis(100)).await;
        let resp = make_response_frame(correlation_id, &[]);
        let _ = socket.write_all(&resp).await;
    });

    let mut conn = BrokerConn::connect(&addr, "test-client", Duration::from_secs(1))
        .await
        .unwrap();

    let deadline = Deadline::from_timeout(Duration::from_millis(40));
    assert!(!deadline.is_expired());
    assert!(deadline.remaining().unwrap() <= Duration::from_millis(40));

    let res = conn
        .roundtrip_deadline(API_VERSIONS, 0, |_| Ok(()), deadline)
        .await;
    assert!(matches!(res, Err(Error::Timeout)));
    assert!(conn.is_closed());

    let _ = server_task.await;
}

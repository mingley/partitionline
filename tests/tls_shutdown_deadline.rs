//! TLS shutdown must not hold a timed-out request when the peer stops reading.
mod common;

use partitionline::net::{BrokerConn, TlsConfig};
use partitionline::Error;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio_rustls::server::TlsStream;

async fn peer_observes_full_close(mut stream: TlsStream<TcpStream>) -> bool {
    tokio::time::timeout(Duration::from_secs(2), async {
        let mut bytes = [0u8; 8192];
        loop {
            match stream.read(&mut bytes).await {
                Ok(0) | Err(_) => break,
                Ok(_) => {}
            }
        }
        // A FIN alone closes only the client's write half. Raw writes must
        // also eventually fail while the client object is still retained.
        loop {
            if stream.get_mut().0.write_all(&bytes).await.is_err() {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .is_ok()
}

#[tokio::test(flavor = "current_thread")]
async fn stalled_tls_peer_cannot_hold_a_write_timeout_during_shutdown() {
    partitionline::net::install_crypto_provider();
    let (server, ca_pem) = common::tls_server_identity();
    let acceptor = tokio_rustls::TlsAcceptor::from(Arc::new(server));
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap().to_string();
    let (handshake_tx, handshake_rx) = tokio::sync::oneshot::channel();
    let (release_tx, release_rx) = tokio::sync::oneshot::channel();
    let peer = tokio::spawn(async move {
        let (socket, _) = listener.accept().await.unwrap();
        let stream = acceptor.accept(socket).await.unwrap();
        let _ = handshake_tx.send(());
        let _ = release_rx.await;
        peer_observes_full_close(stream).await
    });
    let tls = TlsConfig::default().ca_pem(ca_pem).server_name("localhost");
    let mut conn =
        BrokerConn::connect_tls(&addr, "stalled-tls", Duration::from_secs(2), Some(&tls))
            .await
            .unwrap();
    tokio::time::timeout(Duration::from_secs(2), handshake_rx)
        .await
        .unwrap()
        .unwrap();
    let payload = vec![0x5a; 32 * 1024 * 1024];
    let started = Instant::now();
    let outcome = tokio::time::timeout(
        Duration::from_secs(2),
        conn.write_all_timeout(&payload, Duration::from_millis(80)),
    )
    .await;
    let elapsed = started.elapsed();
    let closed = conn.is_closed();
    let _ = release_tx.send(());
    let peer_closed = peer.await.unwrap();
    assert!(
        peer_closed,
        "timed-out TLS connection retained its peer socket"
    );
    assert!(
        matches!(outcome, Ok(Err(Error::Timeout))),
        "write timeout did not return while TLS peer stopped reading: {outcome:?}"
    );
    assert!(closed, "timed-out TLS connection must be closed");
    assert!(elapsed < Duration::from_secs(1), "timeout took {elapsed:?}");
}

#[tokio::test(flavor = "current_thread")]
async fn cancelled_tls_write_releases_peer_while_client_is_retained() {
    partitionline::net::install_crypto_provider();
    let (server, ca_pem) = common::tls_server_identity();
    let acceptor = tokio_rustls::TlsAcceptor::from(Arc::new(server));
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap().to_string();
    let (handshake_tx, handshake_rx) = tokio::sync::oneshot::channel();
    let (release_tx, release_rx) = tokio::sync::oneshot::channel();
    let peer = tokio::spawn(async move {
        let (socket, _) = listener.accept().await.unwrap();
        let stream = acceptor.accept(socket).await.unwrap();
        let _ = handshake_tx.send(());
        let _ = release_rx.await;
        peer_observes_full_close(stream).await
    });
    let tls = TlsConfig::default().ca_pem(ca_pem).server_name("localhost");
    let mut conn =
        BrokerConn::connect_tls(&addr, "cancelled-tls", Duration::from_secs(2), Some(&tls))
            .await
            .unwrap();
    tokio::time::timeout(Duration::from_secs(2), handshake_rx)
        .await
        .unwrap()
        .unwrap();
    let payload = vec![0x5a; 32 * 1024 * 1024];
    let mut write = Box::pin(conn.write_all_timeout(&payload, Duration::from_secs(30)));
    tokio::select! {
        result = &mut write => panic!("TLS write finished before cancellation: {result:?}"),
        () = tokio::time::sleep(Duration::from_millis(80)) => {}
    }
    drop(write);
    assert!(
        conn.is_closed(),
        "cancelled write must poison its connection"
    );
    let _ = release_tx.send(());
    assert!(
        peer.await.unwrap(),
        "cancelled TLS connection retained its peer socket"
    );
    assert!(
        conn.is_closed(),
        "connection became reusable after cancellation"
    );
}

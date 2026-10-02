//! Bootstrap TCP deadlines, bounded racing, and cancellation safety.

#[path = "common/stalled_dial.rs"]
#[cfg(target_os = "linux")]
#[expect(
    unreachable_pub,
    reason = "This fixture is also publicly exported by the standalone benchmark crate."
)]
mod stalled_dial;

use std::time::Duration;
#[cfg(target_os = "linux")]
use std::time::Instant;

use partitionline::net::BrokerConn;
#[cfg(target_os = "linux")]
use stalled_dial::StalledDial;
use tokio::net::TcpListener;

#[cfg(target_os = "linux")]
async fn pending_tcp_dials(addr: std::net::SocketAddr) -> std::io::Result<usize> {
    let remote = format!("0100007F:{:04X}", addr.port());
    Ok(tokio::fs::read_to_string("/proc/net/tcp")
        .await?
        .lines()
        .skip(1)
        .filter(|line| {
            let mut fields = line.split_whitespace();
            fields.nth(2) == Some(remote.as_str()) && fields.next() == Some("02")
        })
        .count())
}

#[cfg(target_os = "linux")]
#[tokio::test]
async fn stalled_first_bootstrap_does_not_delay_reachable_host() {
    let stalled = StalledDial::new().await.unwrap();
    assert!(stalled.verified_stall() >= Duration::from_millis(45));
    let live = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let live_addr = live.local_addr().unwrap().to_string();
    let addrs = vec![stalled.addr().to_string(), live_addr.clone()];
    let started = Instant::now();
    let conn = BrokerConn::connect_tls_any(&addrs, "test", Duration::from_millis(200), None)
        .await
        .unwrap();
    assert_eq!(conn.addr(), live_addr);
    assert!(
        started.elapsed() < Duration::from_millis(100),
        "reachable bootstrap waited for the stalled TCP connect timeout"
    );
    assert_eq!(
        pending_tcp_dials(stalled.addr()).await.unwrap(),
        0,
        "losing TCP dial was retained"
    );
}

#[cfg(target_os = "linux")]
#[tokio::test]
async fn at_most_four_bootstrap_dials_run_at_once() {
    let mut stalled = Vec::new();
    for _ in 0..4 {
        stalled.push(StalledDial::new().await.unwrap());
    }
    let live = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let live_addr = live.local_addr().unwrap().to_string();
    let mut addrs: Vec<String> = stalled.iter().map(|s| s.addr().to_string()).collect();
    addrs.push(live_addr.clone());
    let started = Instant::now();
    let conn = BrokerConn::connect_tls_any(&addrs, "test", Duration::from_millis(200), None)
        .await
        .unwrap();
    let elapsed = started.elapsed();
    assert_eq!(conn.addr(), live_addr);
    assert!(
        elapsed >= Duration::from_millis(160),
        "fifth dial exceeded the four-dial bound"
    );
    assert!(
        elapsed < Duration::from_millis(350),
        "first four dials did not overlap"
    );
    for fixture in stalled {
        assert_eq!(pending_tcp_dials(fixture.addr()).await.unwrap(), 0);
    }
}

#[cfg(target_os = "linux")]
#[tokio::test]
async fn cancelling_bootstrap_race_closes_every_pending_tcp_dial() {
    let first = StalledDial::new().await.unwrap();
    let second = StalledDial::new().await.unwrap();
    let addrs = vec![first.addr().to_string(), second.addr().to_string()];
    {
        let dialing = BrokerConn::connect_tls_any(&addrs, "test", Duration::from_secs(1), None);
        tokio::pin!(dialing);
        tokio::select! {
            _ = &mut dialing => panic!("stalled TCP dials should remain pending"),
            _ = tokio::time::sleep(Duration::from_millis(20)) => {
                assert_eq!(pending_tcp_dials(first.addr()).await.unwrap(), 1);
                assert_eq!(pending_tcp_dials(second.addr()).await.unwrap(), 1);
            }
        }
    }
    assert_eq!(pending_tcp_dials(first.addr()).await.unwrap(), 0);
    assert_eq!(pending_tcp_dials(second.addr()).await.unwrap(), 0);
}

#[cfg(target_os = "linux")]
#[tokio::test]
async fn all_failures_preserve_last_configured_address_error() {
    let stalled = StalledDial::new().await.unwrap();
    let refused = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let refused_addr = refused.local_addr().unwrap().to_string();
    drop(refused);
    for (addrs, timeout_last) in [
        (vec![refused_addr.clone(), stalled.addr().to_string()], true),
        (vec![stalled.addr().to_string(), refused_addr], false),
    ] {
        let result =
            BrokerConn::connect_tls_any(&addrs, "test", Duration::from_millis(100), None).await;
        if timeout_last {
            assert!(matches!(result, Err(partitionline::Error::Timeout)));
        } else {
            assert!(
                matches!(result, Err(partitionline::Error::Io(ref e)) if e.kind() == std::io::ErrorKind::ConnectionRefused)
            );
        }
    }
}

#[tokio::test]
async fn refused_first_bootstrap_still_connects_to_live_host() {
    let refused = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let refused_addr = refused.local_addr().unwrap().to_string();
    drop(refused);
    let live = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let live_addr = live.local_addr().unwrap().to_string();
    let addrs = vec![refused_addr, live_addr.clone()];
    let conn = BrokerConn::connect_tls_any(&addrs, "test", Duration::from_millis(100), None)
        .await
        .unwrap();
    assert_eq!(conn.addr(), live_addr);
}

#[tokio::test]
async fn cancelling_bootstrap_race_closes_pending_tls_handshakes() {
    use tokio::io::AsyncReadExt;
    let mut addrs = Vec::new();
    let mut ready = Vec::new();
    let mut servers = Vec::new();
    for _ in 0..2 {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        addrs.push(listener.local_addr().unwrap().to_string());
        let (sent, received) = tokio::sync::oneshot::channel();
        ready.push(received);
        servers.push(tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut byte = [0u8; 1];
            assert_eq!(
                socket.read(&mut byte).await.unwrap(),
                1,
                "ClientHello was not sent"
            );
            sent.send(()).unwrap();
            let result = tokio::time::timeout(
                Duration::from_millis(200),
                socket.read_to_end(&mut Vec::new()),
            )
            .await;
            assert!(result.is_ok(), "cancelled TLS dial retained its TCP socket");
            if let Ok(Err(error)) = result {
                assert_eq!(error.kind(), std::io::ErrorKind::ConnectionReset);
            }
        }));
    }
    let tls = partitionline::TlsConfig::default();
    {
        let dialing =
            BrokerConn::connect_tls_any(&addrs, "test", Duration::from_secs(1), Some(&tls));
        tokio::pin!(dialing);
        tokio::select! {
            _ = &mut dialing => panic!("TLS peers do not respond to ClientHello"),
            _ = async { for receiver in ready { receiver.await.unwrap(); } } => {}
        }
    }
    for server in servers {
        server.await.unwrap();
    }
}

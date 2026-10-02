//! Verified Rustls and independent OpenSSL peers, handshake bounds and rotation.
#![cfg(feature = "tls")]

use partitionline_broker::{
    security::tls::{Acceptor, ClientAuth, Limits},
    transport::{Config, Handler, Peer, Transport},
};
use rustls::{
    pki_types::{CertificateDer, PrivateKeyDer, ServerName},
    ClientConfig, RootCertStore,
};
use std::{
    future::{poll_fn, Future},
    io,
    net::{Ipv4Addr, SocketAddr},
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    task::Poll,
    time::Duration,
};
use tokio::{
    io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt},
    net::TcpStream,
    sync::Mutex,
    time::{sleep, timeout},
};
use tokio_rustls::{client::TlsStream, TlsConnector};

type Result<T = ()> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;
macro_rules! cert {
    ($name:literal) => {
        include_bytes!(concat!("fixtures/tls/", $name, ".cert.der")).to_vec()
    };
}
macro_rules! key {
    ($name:literal) => {
        include_bytes!(concat!("fixtures/tls/", $name, ".key.der")).to_vec()
    };
}

fn acceptor(auth: ClientAuth, handshake_ms: u64) -> Result<Acceptor> {
    Ok(Acceptor::new(
        vec![cert!("server1")],
        key!("server1"),
        auth,
        Limits {
            handshake_timeout: Duration::from_millis(handshake_ms),
            ..Limits::default()
        },
    )?)
}

fn echo(request: Vec<u8>) -> impl Future<Output = io::Result<Option<Vec<u8>>>> + Send {
    std::future::ready(Ok(Some(request)))
}

fn config(connections: usize, read_ms: u64) -> Result<Config> {
    Ok(Config::new(
        connections,
        connections,
        4096,
        4096,
        Duration::from_millis(read_ms),
        Duration::from_secs(2),
        Duration::from_secs(2),
    )?)
}

async fn start<H: Handler + 'static>(
    handler: Arc<H>,
    acceptor: Acceptor,
    connections: usize,
    read_ms: u64,
) -> Result<Transport> {
    Ok(Transport::bind_tls(
        SocketAddr::from((Ipv4Addr::LOCALHOST, 0)),
        config(connections, read_ms)?,
        handler,
        acceptor,
    )
    .await?)
}

fn client(
    roots: Vec<Vec<u8>>,
    identity: Option<(Vec<Vec<u8>>, Vec<u8>)>,
    versions: &[&'static rustls::SupportedProtocolVersion],
) -> Result<Arc<ClientConfig>> {
    let mut store = RootCertStore::empty();
    for root in roots {
        store.add(CertificateDer::from(root))?;
    }
    let builder =
        ClientConfig::builder_with_provider(Arc::new(rustls::crypto::ring::default_provider()))
            .with_protocol_versions(versions)?
            .with_root_certificates(store);
    let config = match identity {
        None => builder.with_no_client_auth(),
        Some((chain, key)) => builder.with_client_auth_cert(
            chain.into_iter().map(CertificateDer::from).collect(),
            PrivateKeyDer::try_from(key).map_err(io::Error::other)?,
        )?,
    };
    Ok(Arc::new(config))
}

fn anonymous() -> Result<Arc<ClientConfig>> {
    client(vec![cert!("ca1")], None, rustls::DEFAULT_VERSIONS)
}
fn authenticated1() -> Result<Arc<ClientConfig>> {
    client(
        vec![cert!("ca1")],
        Some((vec![cert!("client1")], key!("client1"))),
        rustls::DEFAULT_VERSIONS,
    )
}
fn authenticated2() -> Result<Arc<ClientConfig>> {
    client(
        vec![cert!("ca2")],
        Some((vec![cert!("client2")], key!("client2"))),
        rustls::DEFAULT_VERSIONS,
    )
}

async fn upgrade(
    socket: TcpStream,
    config: Arc<ClientConfig>,
    name: &str,
) -> Result<TlsStream<TcpStream>> {
    Ok(timeout(
        Duration::from_secs(2),
        TlsConnector::from(config).connect(ServerName::try_from(name.to_owned())?, socket),
    )
    .await??)
}

async fn connect(server: &Transport, config: Arc<ClientConfig>) -> Result<TlsStream<TcpStream>> {
    upgrade(
        TcpStream::connect(server.local_addr()).await?,
        config,
        "localhost",
    )
    .await
}

fn framed(payload: &[u8]) -> Result<Vec<u8>> {
    Ok([&i32::try_from(payload.len())?.to_be_bytes()[..], payload].concat())
}

async fn roundtrip<S: AsyncRead + AsyncWrite + Unpin>(
    socket: &mut S,
    payload: &[u8],
) -> Result<Vec<u8>> {
    socket.write_all(&framed(payload)?).await?;
    let mut prefix = [0; 4];
    let _ = timeout(Duration::from_secs(2), socket.read_exact(&mut prefix)).await??;
    let length = usize::try_from(i32::from_be_bytes(prefix))?;
    assert!(length <= 4096);
    let mut result = vec![0; length];
    let _ = timeout(Duration::from_secs(2), socket.read_exact(&mut result)).await??;
    Ok(result)
}

async fn closed(socket: &mut TcpStream) -> Result<bool> {
    Ok(timeout(Duration::from_secs(2), async {
        // Rejected TLS peers may receive a fatal alert before EOF. Drain at
        // most 256 bytes under one deadline; dispatch is checked separately.
        let mut bytes = [0; 64];
        let mut received = 0;
        loop {
            match socket.read(&mut bytes).await {
                Ok(0) => return Ok::<_, io::Error>(true),
                Ok(count) => {
                    received += count;
                    if received > 256 {
                        return Ok(false);
                    }
                }
                Err(error) if error.kind() == io::ErrorKind::ConnectionReset => return Ok(true),
                Err(error) => return Err(error),
            }
        }
    })
    .await??)
}

async fn rejected(server: &Transport, config: Arc<ClientConfig>) -> Result<bool> {
    match connect(server, config).await {
        Err(_) => Ok(true),
        Ok(mut stream) => Ok(roundtrip(&mut stream, b"must-not-dispatch").await.is_err()),
    }
}

#[derive(Default)]
struct Observed {
    peers: Mutex<Vec<Peer>>,
}

impl Handler for Observed {
    type Error = io::Error;
    fn handle(&self, _: Vec<u8>) -> impl Future<Output = io::Result<Option<Vec<u8>>>> + Send {
        std::future::ready(Err(io::Error::other("metadata dispatch required")))
    }
    async fn handle_with_peer(&self, peer: &Peer, request: Vec<u8>) -> io::Result<Option<Vec<u8>>> {
        self.peers.lock().await.push(peer.clone());
        if request == b"no-reply" {
            Ok(None)
        } else {
            Ok(Some(request))
        }
    }
}

#[test]
fn rejects_invalid_limits_der_keys_trust_and_totals_eagerly() -> Result {
    let valid = Limits::default();
    for limits in [
        Limits {
            max_certificates: 0,
            ..valid
        },
        Limits {
            max_certificates: 33,
            ..valid
        },
        Limits {
            max_certificate_bytes: 0,
            ..valid
        },
        Limits {
            max_certificate_bytes: 65_537,
            ..valid
        },
        Limits {
            max_key_bytes: 0,
            ..valid
        },
        Limits {
            max_key_bytes: 65_537,
            ..valid
        },
        Limits {
            max_roots: 0,
            ..valid
        },
        Limits {
            max_roots: 257,
            ..valid
        },
        Limits {
            max_total_der_bytes: 0,
            ..valid
        },
        Limits {
            max_total_der_bytes: 4 * 1024 * 1024 + 1,
            ..valid
        },
        Limits {
            handshake_timeout: Duration::ZERO,
            ..valid
        },
        Limits {
            handshake_timeout: Duration::MAX,
            ..valid
        },
        Limits {
            max_certificate_bytes: 1,
            ..valid
        },
        Limits {
            max_key_bytes: 1,
            ..valid
        },
        Limits {
            max_total_der_bytes: 1,
            ..valid
        },
    ] {
        assert!(Acceptor::new(
            vec![cert!("server1")],
            key!("server1"),
            ClientAuth::ServerOnly,
            limits
        )
        .is_err());
    }
    for (chain, key) in [
        (vec![], key!("server1")),
        (vec![vec![]], key!("server1")),
        (vec![vec![0; 5]], key!("server1")),
        (vec![cert!("server1"), vec![0; 5]], key!("server1")),
        (vec![cert!("server1")], vec![]),
        (vec![cert!("server1")], vec![0; 5]),
        (vec![cert!("server1")], key!("server2")),
    ] {
        assert!(Acceptor::new(chain, key, ClientAuth::ServerOnly, valid).is_err());
    }
    for roots in [vec![], vec![vec![0; 5]], vec![cert!("ca1"); 65]] {
        assert!(Acceptor::new(
            vec![cert!("server1")],
            key!("server1"),
            ClientAuth::Required(roots),
            valid
        )
        .is_err());
    }
    assert_eq!(acceptor(ClientAuth::ServerOnly, 100)?.generation(), 0);
    Ok(())
}

#[tokio::test]
async fn verified_tls12_and_tls13_frame_roundtrip() -> Result {
    let mut server = start(
        Arc::new(echo),
        acceptor(ClientAuth::ServerOnly, 2000)?,
        2,
        2000,
    )
    .await?;
    for version in [&rustls::version::TLS12, &rustls::version::TLS13] {
        let config = client(vec![cert!("ca1")], None, &[version])?;
        let mut socket = connect(&server, config).await?;
        assert_eq!(socket.get_ref().1.protocol_version(), Some(version.version));
        assert_eq!(roundtrip(&mut socket, b"verified").await?, b"verified");
        assert_eq!(roundtrip(&mut socket, b"").await?, b"");
    }
    let report = server.shutdown().await?;
    assert_eq!(report.accepted_connections, 2);
    assert_eq!(report.joined_connections, 2);
    assert_eq!(report.tls_handshake_errors, 0);
    Ok(())
}

#[tokio::test]
async fn required_mtls_verifies_intermediate_chain_and_preserves_peer_metadata() -> Result {
    let observed = Arc::new(Observed::default());
    let mut server = start(
        observed.clone(),
        acceptor(ClientAuth::Required(vec![cert!("ca1")]), 2000)?,
        1,
        2000,
    )
    .await?;
    let config = client(
        vec![cert!("ca1")],
        Some((
            vec![cert!("client-chain"), cert!("intermediate")],
            key!("client-chain"),
        )),
        rustls::DEFAULT_VERSIONS,
    )?;
    let mut socket = connect(&server, config).await?;
    socket
        .write_all(&[framed(b"no-reply")?, framed(b"reply")?].concat())
        .await?;
    let mut bytes = [0; 9];
    let _ = timeout(Duration::from_secs(2), socket.read_exact(&mut bytes)).await??;
    assert_eq!(bytes, [0, 0, 0, 5, b'r', b'e', b'p', b'l', b'y']);
    let peers = observed.peers.lock().await;
    assert_eq!(peers.len(), 2);
    for peer in &*peers {
        assert!(peer.address().ip().is_loopback());
        let tls = peer.tls().ok_or("missing TLS metadata")?;
        assert_eq!(tls.generation(), 0);
        assert_eq!(
            tls.client_certificates_der(),
            Some(&[cert!("client-chain"), cert!("intermediate")][..])
        );
    }
    drop(peers);
    assert_eq!(server.shutdown().await?.joined_connections, 1);
    Ok(())
}

#[tokio::test]
async fn server_only_tls_has_no_authenticated_client_identity() -> Result {
    let observed = Arc::new(Observed::default());
    let mut server = start(
        observed.clone(),
        acceptor(ClientAuth::ServerOnly, 2000)?,
        1,
        2000,
    )
    .await?;
    let mut socket = connect(&server, anonymous()?).await?;
    assert_eq!(
        roundtrip(&mut socket, b"server-only").await?,
        b"server-only"
    );
    let peers = observed.peers.lock().await;
    assert_eq!(peers.len(), 1);
    assert!(peers[0]
        .tls()
        .ok_or("missing TLS")?
        .client_certificates_der()
        .is_none());
    drop(peers);
    let _ = server.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn required_mtls_rejects_missing_untrusted_expired_and_wrong_purpose_clients() -> Result {
    let calls = Arc::new(AtomicUsize::new(0));
    let h = calls.clone();
    let mut server = start(
        Arc::new(move |request| {
            let _ = h.fetch_add(1, Ordering::SeqCst);
            std::future::ready(Ok::<_, io::Error>(Some(request)))
        }),
        acceptor(ClientAuth::Required(vec![cert!("ca1")]), 2000)?,
        4,
        2000,
    )
    .await?;
    for config in [
        anonymous()?,
        client(
            vec![cert!("ca1")],
            Some((vec![cert!("client2")], key!("client2"))),
            rustls::DEFAULT_VERSIONS,
        )?,
        client(
            vec![cert!("ca1")],
            Some((vec![cert!("client-expired")], key!("client-expired"))),
            rustls::DEFAULT_VERSIONS,
        )?,
        client(
            vec![cert!("ca1")],
            Some((
                vec![cert!("client-wrong-purpose")],
                key!("client-wrong-purpose"),
            )),
            rustls::DEFAULT_VERSIONS,
        )?,
    ] {
        assert!(rejected(&server, config).await?);
    }
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    let mut valid = connect(&server, authenticated1()?).await?;
    assert_eq!(roundtrip(&mut valid, b"healthy").await?, b"healthy");
    let report = server.shutdown().await?;
    assert_eq!(report.tls_handshake_errors, 4);
    assert_eq!(report.accepted_connections, report.joined_connections);
    Ok(())
}

#[tokio::test]
async fn verifying_clients_reject_wrong_server_name_expiry_and_trust() -> Result {
    for (certificate, private_key, roots) in [
        (
            cert!("server-wrong-name"),
            key!("server-wrong-name"),
            vec![cert!("ca1")],
        ),
        (
            cert!("server-expired"),
            key!("server-expired"),
            vec![cert!("ca1")],
        ),
        (cert!("server1"), key!("server1"), vec![cert!("ca2")]),
    ] {
        let tls = Acceptor::new(
            vec![certificate],
            private_key,
            ClientAuth::ServerOnly,
            Limits::default(),
        )?;
        let mut server = start(Arc::new(echo), tls, 1, 2000).await?;
        assert!(rejected(&server, client(roots, None, rustls::DEFAULT_VERSIONS)?).await?);
        let report = server.shutdown().await?;
        assert_eq!(report.accepted_connections, report.joined_connections);
    }
    Ok(())
}

#[tokio::test]
async fn plaintext_never_falls_back_on_tls_listener() -> Result {
    let observed = Arc::new(Observed::default());
    let mut server = start(
        observed.clone(),
        acceptor(ClientAuth::ServerOnly, 2000)?,
        2,
        2000,
    )
    .await?;
    let mut plain = TcpStream::connect(server.local_addr()).await?;
    plain.write_all(&framed(b"plaintext")?).await?;
    assert!(closed(&mut plain).await?);
    assert!(observed.peers.lock().await.is_empty());
    let mut verified = connect(&server, anonymous()?).await?;
    assert_eq!(roundtrip(&mut verified, b"healthy").await?, b"healthy");
    assert_eq!(server.shutdown().await?.tls_handshake_errors, 1);
    Ok(())
}

#[tokio::test]
async fn idle_and_fragmented_tls_progress_share_absolute_handshake_deadline() -> Result {
    let mut server = start(
        Arc::new(echo),
        acceptor(ClientAuth::ServerOnly, 100)?,
        2,
        2000,
    )
    .await?;
    let mut idle = TcpStream::connect(server.local_addr()).await?;
    assert!(closed(&mut idle).await?);
    let mut slow = TcpStream::connect(server.local_addr()).await?;
    slow.write_all(&[22, 3]).await?;
    sleep(Duration::from_millis(60)).await;
    slow.write_all(&[3, 0, 10, 1]).await?;
    sleep(Duration::from_millis(60)).await;
    assert!(closed(&mut slow).await?);
    let report = server.shutdown().await?;
    assert_eq!(report.tls_handshake_deadlines, 2);
    assert_eq!(report.joined_connections, 2);
    Ok(())
}

#[tokio::test]
async fn oversized_tls_record_and_handshake_lengths_fail_closed() -> Result {
    let observed = Arc::new(Observed::default());
    let mut server = start(
        observed.clone(),
        acceptor(ClientAuth::ServerOnly, 2000)?,
        2,
        2000,
    )
    .await?;
    for prefix in [
        &[22, 3, 3, 255, 255][..],
        &[22, 3, 3, 0, 4, 1, 255, 255, 255][..],
    ] {
        let mut socket = TcpStream::connect(server.local_addr()).await?;
        socket.write_all(prefix).await?;
        assert!(closed(&mut socket).await?);
    }
    assert!(observed.peers.lock().await.is_empty());
    assert_eq!(server.shutdown().await?.tls_handshake_errors, 2);
    Ok(())
}

#[tokio::test]
async fn handshakes_and_established_sessions_share_cap_and_joined_shutdown() -> Result {
    let mut server = start(
        Arc::new(echo),
        acceptor(ClientAuth::ServerOnly, 20_000)?,
        2,
        20_000,
    )
    .await?;
    let mut established = connect(&server, anonymous()?).await?;
    assert_eq!(roundtrip(&mut established, b"active").await?, b"active");
    let mut pending = TcpStream::connect(server.local_addr()).await?;
    pending.write_all(&[22]).await?;
    let mut excess = TcpStream::connect(server.local_addr()).await?;
    assert!(closed(&mut excess).await?);
    let report = timeout(Duration::from_secs(1), server.shutdown()).await??;
    assert_eq!(report.overload_rejections, 1);
    assert_eq!(report.peak_connections, 2);
    assert_eq!(report.accepted_connections, 2);
    assert_eq!(report.joined_connections, 2);
    assert_eq!(report.shutdown_connections, 2);
    assert!(closed(&mut pending).await?);
    assert!(roundtrip(&mut established, b"closed").await.is_err());
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn pending_handshake_shutdown_can_be_cancelled_retried_and_drop_cleaned() -> Result {
    let mut server = start(
        Arc::new(echo),
        acceptor(ClientAuth::ServerOnly, 20_000)?,
        1,
        20_000,
    )
    .await?;
    let mut pending = TcpStream::connect(server.local_addr()).await?;
    let mut excess = TcpStream::connect(server.local_addr()).await?;
    assert!(closed(&mut excess).await?); // proves first socket was admitted
    let mut shutdown = Box::pin(server.shutdown());
    poll_fn(|context| {
        assert!(shutdown.as_mut().poll(context).is_pending());
        Poll::Ready(())
    })
    .await;
    drop(shutdown);
    let report = timeout(Duration::from_secs(1), server.shutdown()).await??;
    assert_eq!(report.joined_connections, 1);
    assert_eq!(report.shutdown_connections, 1);
    assert!(closed(&mut pending).await?);
    let server = start(
        Arc::new(echo),
        acceptor(ClientAuth::ServerOnly, 20_000)?,
        1,
        20_000,
    )
    .await?;
    let address = server.local_addr();
    let mut pending = TcpStream::connect(address).await?;
    let mut excess = TcpStream::connect(address).await?;
    assert!(closed(&mut excess).await?);
    drop(server);
    assert!(closed(&mut pending).await?);
    assert!(TcpStream::connect(address).await.is_err());
    Ok(())
}

#[tokio::test]
async fn rotation_updates_new_server_identity_and_client_trust_preserving_old_session() -> Result {
    let observed = Arc::new(Observed::default());
    let tls = acceptor(ClientAuth::Required(vec![cert!("ca1")]), 2000)?;
    let mut server = start(observed.clone(), tls.clone(), 4, 2000).await?;
    let old_config = client(
        vec![cert!("ca1"), cert!("ca2")],
        Some((vec![cert!("client1")], key!("client1"))),
        rustls::DEFAULT_VERSIONS,
    )?;
    let mut old = connect(&server, old_config.clone()).await?;
    assert_eq!(roundtrip(&mut old, b"before").await?, b"before");
    assert_eq!(
        old.get_ref().1.handshake_kind(),
        Some(rustls::HandshakeKind::Full)
    );
    assert_eq!(
        tls.rotate(
            vec![cert!("server2")],
            key!("server2"),
            ClientAuth::Required(vec![cert!("ca2")])
        )?,
        1
    );
    assert_eq!(roundtrip(&mut old, b"existing").await?, b"existing");
    assert!(rejected(&server, authenticated1()?).await?); // old server trust rejects identity2
    assert!(rejected(&server, old_config).await?); // old client trust rejected even with both server roots
    let mut new = connect(&server, authenticated2()?).await?;
    assert_eq!(roundtrip(&mut new, b"new").await?, b"new");
    assert_eq!(
        new.get_ref().1.handshake_kind(),
        Some(rustls::HandshakeKind::Full)
    );
    assert_eq!(
        new.get_ref()
            .1
            .peer_certificates()
            .ok_or("missing server cert")?[0]
            .as_ref(),
        cert!("server2")
    );
    assert!(tls
        .rotate(
            vec![cert!("server2")],
            key!("server1"),
            ClientAuth::Required(vec![cert!("ca2")])
        )
        .is_err());
    assert!(tls
        .rotate(
            vec![cert!("server1")],
            key!("server1"),
            ClientAuth::Required(vec![vec![0]])
        )
        .is_err());
    assert_eq!(tls.generation(), 1);
    assert_eq!(
        roundtrip(&mut new, b"after-invalid-update").await?,
        b"after-invalid-update"
    );
    let mut after_invalid = connect(&server, authenticated2()?).await?;
    assert_eq!(
        roundtrip(&mut after_invalid, b"fresh-after-invalid").await?,
        b"fresh-after-invalid"
    );
    assert_eq!(
        after_invalid
            .get_ref()
            .1
            .peer_certificates()
            .ok_or("missing server cert")?[0]
            .as_ref(),
        cert!("server2")
    );
    let peers = observed.peers.lock().await;
    assert_eq!(peers.len(), 5);
    assert_eq!(peers[0].tls().ok_or("missing TLS")?.generation(), 0);
    assert_eq!(peers[1].tls().ok_or("missing TLS")?.generation(), 0);
    assert_eq!(peers[2].tls().ok_or("missing TLS")?.generation(), 1);
    assert_eq!(peers[3].tls().ok_or("missing TLS")?.generation(), 1);
    assert_eq!(peers[4].tls().ok_or("missing TLS")?.generation(), 1);
    drop(peers);
    let report = server.shutdown().await?;
    assert_eq!(report.accepted_connections, report.joined_connections);
    Ok(())
}

#[tokio::test]
async fn admitted_pending_handshake_retains_pre_rotation_generation() -> Result {
    let observed = Arc::new(Observed::default());
    let tls = acceptor(ClientAuth::Required(vec![cert!("ca1")]), 2000)?;
    let mut server = start(observed.clone(), tls.clone(), 1, 2000).await?;
    let pending = TcpStream::connect(server.local_addr()).await?;
    let mut excess = TcpStream::connect(server.local_addr()).await?;
    assert!(closed(&mut excess).await?); // synchronizes admission before rotation
    let _ = tls.rotate(
        vec![cert!("server2")],
        key!("server2"),
        ClientAuth::Required(vec![cert!("ca2")]),
    )?;
    let mut old = upgrade(pending, authenticated1()?, "localhost").await?;
    assert_eq!(roundtrip(&mut old, b"captured").await?, b"captured");
    let peers = observed.peers.lock().await;
    assert_eq!(peers[0].tls().ok_or("missing TLS")?.generation(), 0);
    assert_eq!(tls.generation(), 1);
    drop(peers);
    let report = server.shutdown().await?;
    assert_eq!(report.overload_rejections, 1);
    assert_eq!(report.joined_connections, 1);
    Ok(())
}

#[tokio::test]
async fn verified_peer_chain_cap_closes_before_handler_dispatch() -> Result {
    let observed = Arc::new(Observed::default());
    let tls = Acceptor::new(
        vec![cert!("server1")],
        key!("server1"),
        ClientAuth::Required(vec![cert!("ca1")]),
        Limits {
            max_certificates: 1,
            ..Limits::default()
        },
    )?;
    let mut server = start(observed.clone(), tls, 1, 2000).await?;
    let config = client(
        vec![cert!("ca1")],
        Some((
            vec![cert!("client-chain"), cert!("intermediate")],
            key!("client-chain"),
        )),
        rustls::DEFAULT_VERSIONS,
    )?;
    assert!(rejected(&server, config).await?);
    assert!(observed.peers.lock().await.is_empty());
    assert_eq!(server.shutdown().await?.tls_handshake_errors, 1);
    Ok(())
}

async fn openssl_peer(
    address: SocketAddr,
    identity: Option<&'static str>,
    version: &'static str,
) -> Result<Vec<u8>> {
    tokio::task::spawn_blocking(move || -> Result<Vec<u8>> {
        use std::{
            io::Write,
            process::{Child, Command, Stdio},
            time::Instant,
        };
        struct OwnedChild(Option<Child>);
        impl Drop for OwnedChild {
            fn drop(&mut self) {
                if let Some(mut child) = self.0.take() {
                    let _ = child.kill();
                    let _ = child.wait();
                }
            }
        }
        let fixtures = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/tls");
        let mut command = Command::new("openssl");
        command.args([
            "s_client",
            "-quiet",
            "-ign_eof",
            "-verify_return_error",
            "-verify_hostname",
            "localhost",
            "-connect",
            &address.to_string(),
            version,
        ]);
        command.arg("-CAfile").arg(fixtures.join("ca1.cert.pem"));
        if let Some(name) = identity {
            command
                .arg("-cert")
                .arg(fixtures.join(format!("{name}.cert.pem")))
                .arg("-key")
                .arg(fixtures.join(format!("{name}.key.pem")));
        }
        let child = command
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()?;
        let mut owned = OwnedChild(Some(child));
        let child = owned.0.as_mut().ok_or("missing OpenSSL child")?;
        if let Some(mut input) = child.stdin.take() {
            input.write_all(&framed(b"openssl-peer")?)?;
        }
        let deadline = Instant::now() + Duration::from_secs(4);
        loop {
            if child.try_wait()?.is_some() {
                break;
            }
            if Instant::now() >= deadline {
                let _ = child.kill();
                let _ = child.wait();
                return Err("OpenSSL peer exceeded process deadline".into());
            }
            std::thread::park_timeout(Duration::from_millis(5));
        }
        // The transport closes without TLS close_notify after its next-frame
        // deadline. A peer may report EOF as a nonzero exit; exact response bytes
        // plus verify_return_error prove authenticated application exchange.
        let output = owned
            .0
            .take()
            .ok_or("missing OpenSSL child")?
            .wait_with_output()?;
        Ok(output.stdout)
    })
    .await?
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn independent_openssl_peer_verifies_tls_versions_and_mtls_rejections() -> Result {
    let observed = Arc::new(Observed::default());
    let mut server = start(
        observed.clone(),
        acceptor(ClientAuth::Required(vec![cert!("ca1")]), 2000)?,
        2,
        100,
    )
    .await?;
    for version in ["-tls1_2", "-tls1_3"] {
        assert_eq!(
            openssl_peer(server.local_addr(), Some("client1"), version).await?,
            framed(b"openssl-peer")?
        );
    }
    for identity in [
        None,
        Some("client2"),
        Some("client-expired"),
        Some("client-wrong-purpose"),
    ] {
        assert!(openssl_peer(server.local_addr(), identity, "-tls1_3")
            .await?
            .is_empty());
    }
    assert_eq!(observed.peers.lock().await.len(), 2);
    let report = server.shutdown().await?;
    assert_eq!(report.tls_handshake_errors, 4);
    assert_eq!(report.joined_connections, 6);
    Ok(())
}

//! Actual TLS OAuth sockets, typed authority, failure ACK, expiry and cancellation.
#![cfg(feature = "oidc")]

mod oidc_support;
use oidc_support::{at_least, Harness, Result};
use partitionline_broker::{
    security::{
        credentials::{Limits as CredentialLimits, Store},
        oidc::Service,
        sasl::Authority,
        session::{
            Limits, OAuthAdministrator, Profile, OIDC_API_VERSIONS, OIDC_METADATA_API_VERSIONS,
            SASL_METADATA_API_VERSIONS,
        },
        tls::{Acceptor, ClientAuth, Limits as TlsLimits},
    },
    transport::{Config, Handler, Peer, Transport},
};
use std::{
    io,
    net::{Ipv4Addr, SocketAddr},
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    time::Duration,
};
use tokio::{
    io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt},
    net::TcpStream,
    sync::{Mutex, Semaphore},
    time::timeout,
};
use zeroize::Zeroizing;

struct Probe {
    calls: AtomicUsize,
    cancelled: AtomicUsize,
    identities: Mutex<Vec<(String, String, u64)>>,
    entered: Semaphore,
    block: AtomicUsize,
}
impl Default for Probe {
    fn default() -> Self {
        Self {
            calls: AtomicUsize::new(0),
            cancelled: AtomicUsize::new(0),
            identities: Mutex::new(Vec::new()),
            entered: Semaphore::new(0),
            block: AtomicUsize::new(0),
        }
    }
}
struct Active<'a>(&'a AtomicUsize);
impl Drop for Active<'_> {
    fn drop(&mut self) {
        self.0.fetch_add(1, Ordering::AcqRel);
    }
}
impl Handler for Probe {
    type Error = io::Error;
    async fn handle(&self, _: Vec<u8>) -> io::Result<Option<Vec<u8>>> {
        Err(io::Error::other("missing typed identity"))
    }
    async fn handle_with_peer(&self, peer: &Peer, request: Vec<u8>) -> io::Result<Option<Vec<u8>>> {
        let identity = peer
            .identity()
            .ok_or_else(|| io::Error::other("unverified dispatch"))?;
        let issuer = match identity.authority() {
            Authority::Oidc { issuer } => issuer.clone(),
            _ => return Err(io::Error::other("wrong authority family")),
        };
        assert!(!format!("{peer:?}").contains(identity.name()));
        assert!(!format!("{:?}", identity.authority()).contains(&issuer));
        self.identities.lock().await.push((
            issuer,
            identity.name().to_owned(),
            identity.generation(),
        ));
        self.calls.fetch_add(1, Ordering::AcqRel);
        let active = Active(&self.cancelled);
        self.entered.add_permits(1);
        if self.block.load(Ordering::Acquire) == 1 {
            std::future::pending::<()>().await;
        }
        std::mem::forget(active);
        if self.block.load(Ordering::Acquire) == 2 {
            return Ok(Some(vec![0; 32 * 1024 * 1024]));
        }
        Ok(Some(request))
    }
}
fn config(handlers: usize) -> Result<Config> {
    Ok(Config::new(
        16,
        handlers,
        65536,
        65536,
        Duration::from_secs(5),
        Duration::from_secs(5),
        Duration::from_secs(5),
    )?)
}
fn acceptor() -> Result<Acceptor> {
    Ok(Acceptor::new(
        vec![include_bytes!("fixtures/tls/server1.cert.der").to_vec()],
        include_bytes!("fixtures/tls/server1.key.der").to_vec(),
        ClientAuth::ServerOnly,
        TlsLimits::default(),
    )?)
}
async fn listener(profile: Profile, probe: Arc<Probe>, handlers: usize) -> Result<Transport> {
    Ok(Transport::bind_tls_sasl(
        SocketAddr::from((Ipv4Addr::LOCALHOST, 0)),
        config(handlers)?,
        probe,
        acceptor()?,
        profile,
    )
    .await?)
}
async fn service(harness: &Harness) -> Result<Service> {
    let mut config = harness.config();
    config.runtime.active_leases = 16;
    Ok(Service::start(config, harness.trust()).await?)
}
fn initial(token: Vec<u8>, authz: &str) -> Zeroizing<Vec<u8>> {
    let token = Zeroizing::new(token);
    let mut bytes = Zeroizing::new(format!("n,{authz},\x01auth=Bearer ").into_bytes());
    bytes.extend_from_slice(&token);
    bytes.extend_from_slice(b"\x01\x01");
    bytes
}
fn header(key: i16, version: i16, correlation: i32, flexible: bool) -> Vec<u8> {
    let mut out = Vec::new();
    out.extend_from_slice(&key.to_be_bytes());
    out.extend_from_slice(&version.to_be_bytes());
    out.extend_from_slice(&correlation.to_be_bytes());
    out.extend_from_slice(&(-1i16).to_be_bytes());
    if flexible {
        out.push(0);
    }
    out
}
fn varint(out: &mut Vec<u8>, mut value: usize) {
    loop {
        let byte = (value & 127) as u8;
        value >>= 7;
        out.push(byte | if value > 0 { 128 } else { 0 });
        if value == 0 {
            break;
        }
    }
}
fn read_varint(bytes: &[u8], position: &mut usize) -> Result<usize> {
    let mut result = 0usize;
    for shift in [0, 7, 14, 21, 28] {
        let byte = *bytes.get(*position).ok_or("truncated peer varint")?;
        *position += 1;
        result |= usize::from(byte & 127) << shift;
        if byte & 128 == 0 {
            return Ok(result);
        }
    }
    Err("invalid peer varint".into())
}
fn handshake(version: i16, correlation: i32, mechanism: &str) -> Vec<u8> {
    let mut out = header(17, version, correlation, false);
    out.extend_from_slice(&(mechanism.len() as i16).to_be_bytes());
    out.extend_from_slice(mechanism.as_bytes());
    out
}
fn authenticate(version: i16, correlation: i32, message: &[u8]) -> Vec<u8> {
    let mut out = header(36, version, correlation, version == 2);
    if version == 2 {
        varint(&mut out, message.len() + 1);
    } else {
        out.extend_from_slice(&(message.len() as i32).to_be_bytes());
    }
    out.extend_from_slice(message);
    if version == 2 {
        out.push(0);
    }
    out
}
async fn send<S: AsyncWrite + Unpin>(socket: &mut S, request: &[u8]) -> Result {
    socket
        .write_all(&(request.len() as i32).to_be_bytes())
        .await?;
    socket.write_all(request).await?;
    Ok(())
}
async fn receive<S: AsyncRead + Unpin>(socket: &mut S) -> Result<Vec<u8>> {
    let mut prefix = [0; 4];
    timeout(Duration::from_secs(3), socket.read_exact(&mut prefix)).await??;
    let length = usize::try_from(i32::from_be_bytes(prefix))?;
    assert!(length <= 65536);
    let mut payload = vec![0; length];
    timeout(Duration::from_secs(3), socket.read_exact(&mut payload)).await??;
    Ok(payload)
}
async fn closed<S: AsyncRead + Unpin>(socket: &mut S) -> Result<bool> {
    let mut byte = [0];
    match timeout(Duration::from_secs(3), socket.read(&mut byte)).await? {
        Ok(0) => Ok(true),
        Ok(_) => Ok(false),
        Err(e)
            if matches!(
                e.kind(),
                io::ErrorKind::UnexpectedEof
                    | io::ErrorKind::ConnectionReset
                    | io::ErrorKind::BrokenPipe
            ) =>
        {
            Ok(true)
        }
        Err(e) => Err(e.into()),
    }
}
fn auth_response(bytes: &[u8], version: i16, correlation: i32) -> Result<(i16, Vec<u8>)> {
    assert_eq!(bytes.get(..4), Some(&correlation.to_be_bytes()[..]));
    let mut position = if version == 2 {
        assert_eq!(bytes[4], 0);
        5
    } else {
        4
    };
    let error = i16::from_be_bytes(bytes[position..position + 2].try_into()?);
    position += 2;
    let length = if version == 2 {
        assert!(read_varint(bytes, &mut position)? <= 1);
        read_varint(bytes, &mut position)?
            .checked_sub(1)
            .ok_or("null auth bytes")?
    } else {
        assert!(matches!(
            i16::from_be_bytes(bytes[position..position + 2].try_into()?),
            -1 | 0
        ));
        position += 2;
        let length = usize::try_from(i32::from_be_bytes(
            bytes[position..position + 4].try_into()?,
        ))?;
        position += 4;
        length
    };
    let message = bytes
        .get(position..position + length)
        .ok_or("truncated auth response")?
        .to_vec();
    position += length;
    if version >= 1 {
        assert_eq!(&bytes[position..position + 8], &0i64.to_be_bytes());
        position += 8;
    }
    if version == 2 {
        assert_eq!(bytes[position], 0);
        position += 1;
    }
    assert_eq!(position, bytes.len());
    Ok((error, message))
}
async fn tls_client(server: &Transport) -> Result<tokio_rustls::client::TlsStream<TcpStream>> {
    use rustls::{
        pki_types::{CertificateDer, ServerName},
        ClientConfig, RootCertStore,
    };
    let mut roots = RootCertStore::empty();
    roots.add(CertificateDer::from(
        include_bytes!("fixtures/tls/ca1.cert.der").to_vec(),
    ))?;
    let client =
        ClientConfig::builder_with_provider(Arc::new(rustls::crypto::ring::default_provider()))
            .with_safe_default_protocol_versions()?
            .with_root_certificates(roots)
            .with_no_client_auth();
    Ok(tokio_rustls::TlsConnector::from(Arc::new(client))
        .connect(
            ServerName::try_from("localhost")?,
            TcpStream::connect(server.local_addr()).await?,
        )
        .await?)
}

async fn oauth<S: AsyncRead + AsyncWrite + Unpin>(
    socket: &mut S,
    token: Vec<u8>,
    version: i16,
    legacy: bool,
    authz: &str,
) -> Result {
    send(
        socket,
        &handshake(if legacy { 0 } else { 1 }, 11, "OAUTHBEARER"),
    )
    .await?;
    let response = receive(socket).await?;
    assert_eq!(&response[4..6], &0i16.to_be_bytes());
    assert!(response.windows(11).any(|b| b == b"OAUTHBEARER"));
    let first = initial(token, authz);
    if legacy {
        send(socket, &first).await?;
        assert!(receive(socket).await?.is_empty());
    } else {
        send(socket, &authenticate(version, 12, &first)).await?;
        assert_eq!(
            auth_response(&receive(socket).await?, version, 12)?,
            (0, Vec::new())
        );
    }
    Ok(())
}
fn advertised(bytes: &[u8]) -> Result<Vec<(i16, i16, i16)>> {
    assert_eq!(&bytes[4..6], &0i16.to_be_bytes());
    let count = usize::try_from(i32::from_be_bytes(bytes[6..10].try_into()?))?;
    let mut result = Vec::new();
    let mut pos = 10;
    for _ in 0..count {
        result.push((
            i16::from_be_bytes(bytes[pos..pos + 2].try_into()?),
            i16::from_be_bytes(bytes[pos + 2..pos + 4].try_into()?),
            i16::from_be_bytes(bytes[pos + 4..pos + 6].try_into()?),
        ));
        pos += 6;
    }
    assert_eq!(pos, bytes.len());
    Ok(result)
}

#[tokio::test]
async fn only_installed_oauth_apis_and_mechanisms_are_advertised() -> Result {
    let harness = Harness::start().await?;
    let service = service(&harness).await?;
    let probe = Arc::new(Probe::default());
    assert!(Profile::oidc_tls(service.clone(), Limits::default())?
        .with_advertised(&SASL_METADATA_API_VERSIONS)
        .is_err());
    for (apis, expected) in [
        (&OIDC_API_VERSIONS[..], vec![17, 18, 36]),
        (&OIDC_METADATA_API_VERSIONS[..], vec![3, 17, 18, 19, 20, 36]),
    ] {
        let profile =
            Profile::oidc_tls(service.clone(), Limits::default())?.with_advertised(apis)?;
        assert!(Transport::bind_sasl(
            SocketAddr::from((Ipv4Addr::LOCALHOST, 0)),
            config(1)?,
            probe.clone(),
            profile.clone()
        )
        .await
        .is_err());
        let mut server = listener(profile, probe.clone(), 1).await?;
        let mut socket = tls_client(&server).await?;
        send(&mut socket, &header(18, 0, 1, false)).await?;
        assert_eq!(
            advertised(&receive(&mut socket).await?)?
                .iter()
                .map(|a| a.0)
                .collect::<Vec<_>>(),
            expected
        );
        send(&mut socket, &handshake(1, 2, "PLAIN")).await?;
        assert_eq!(&receive(&mut socket).await?[4..6], &33i16.to_be_bytes());
        assert!(closed(&mut socket).await?);
        let mut socket = tls_client(&server).await?;
        send(&mut socket, &header(3, 0, 3, false)).await?;
        assert!(closed(&mut socket).await?);
        server.shutdown().await?;
    }
    assert_eq!(probe.calls.load(Ordering::Acquire), 0);
    service.shutdown().await?;
    harness.shutdown().await
}

#[tokio::test]
async fn all_auth_versions_and_legacy_bind_exact_issuer_and_escaped_subject() -> Result {
    let harness = Harness::start().await?;
    let service = service(&harness).await?;
    let probe = Arc::new(Probe::default());
    let profile = Profile::oidc_tls(service.clone(), Limits::default())?
        .with_advertised(&OIDC_METADATA_API_VERSIONS)?;
    let mut server = listener(profile, probe.clone(), 2).await?;
    for (version, legacy) in [(0, false), (1, false), (2, false), (0, true)] {
        let mut socket = tls_client(&server).await?;
        oauth(
            &mut socket,
            harness.token_bytes(0, "shared-key", 30, "user,name=ok")?,
            version,
            legacy,
            "a=user=2Cname=3Dok",
        )
        .await?;
        let app = header(3, 0, 21 + version as i32, false);
        send(&mut socket, &app).await?;
        assert_eq!(receive(&mut socket).await?, app);
        // Authentication success does not authorize reauthentication until KL11-71.
        if !legacy {
            send(&mut socket, &authenticate(version, 30, b"\x01")).await?;
            assert_eq!(
                auth_response(&receive(&mut socket).await?, version, 30)?.0,
                34
            );
            assert!(closed(&mut socket).await?);
        }
    }
    let identities = probe.identities.lock().await;
    assert_eq!(identities.len(), 4);
    assert!(identities.iter().all(
        |(issuer, subject, _)| issuer == &harness.authority.issuer && subject == "user,name=ok"
    ));
    drop(identities);
    server.shutdown().await?;
    service.shutdown().await?;
    harness.shutdown().await
}

#[tokio::test]
async fn invalid_token_challenge_requires_one_exact_terminal_ack() -> Result {
    let harness = Harness::start().await?;
    let service = service(&harness).await?;
    let probe = Arc::new(Probe::default());
    let mut server = listener(
        Profile::oidc_tls(service.clone(), Limits::default())?,
        probe.clone(),
        2,
    )
    .await?;
    for (version, legacy) in [(0, false), (1, false), (2, false), (0, true)] {
        let mut socket = tls_client(&server).await?;
        send(
            &mut socket,
            &handshake(if legacy { 0 } else { 1 }, 1, "OAUTHBEARER"),
        )
        .await?;
        receive(&mut socket).await?;
        let first = initial(b"opaque-invalid-token".to_vec(), "");
        send(
            &mut socket,
            &if legacy {
                first.to_vec()
            } else {
                authenticate(version, 2, &first)
            },
        )
        .await?;
        let response = receive(&mut socket).await?;
        let message = if legacy {
            response
        } else {
            let (error, message) = auth_response(&response, version, 2)?;
            assert_eq!(error, 0);
            message
        };
        assert_eq!(message, b"{\"status\":\"invalid_token\"}");
        send(
            &mut socket,
            &if legacy {
                vec![1]
            } else {
                authenticate(version, 3, &[1])
            },
        )
        .await?;
        if !legacy {
            assert_eq!(
                auth_response(&receive(&mut socket).await?, version, 3)?.0,
                58
            );
        }
        assert!(closed(&mut socket).await?);
    }
    for retry in [authenticate(2, 5, b"retry"), header(18, 0, 5, false)] {
        let mut socket = tls_client(&server).await?;
        send(&mut socket, &handshake(1, 1, "OAUTHBEARER")).await?;
        receive(&mut socket).await?;
        let first = initial(
            harness.token_bytes(0, "shared-key", 30, "test-user")?,
            "a=other",
        );
        send(&mut socket, &authenticate(2, 2, &first)).await?;
        assert_eq!(
            auth_response(&receive(&mut socket).await?, 2, 2)?.1,
            b"{\"status\":\"invalid_token\"}"
        );
        send(&mut socket, &retry).await?;
        assert!(closed(&mut socket).await?);
    }
    assert_eq!(probe.calls.load(Ordering::Acquire), 0);
    server.shutdown().await?;
    service.shutdown().await?;
    harness.shutdown().await
}

#[tokio::test]
async fn hard_expiry_closes_idle_and_partial_reads_then_fresh_reconnects() -> Result {
    let harness = Harness::start().await?;
    let service = service(&harness).await?;
    let probe = Arc::new(Probe::default());
    let mut server = listener(
        Profile::oidc_tls(service.clone(), Limits::default())?
            .with_advertised(&OIDC_METADATA_API_VERSIONS)?,
        probe.clone(),
        2,
    )
    .await?;
    for partial in [false, true] {
        let mut socket = tls_client(&server).await?;
        oauth(
            &mut socket,
            harness.token_bytes(0, "shared-key", 2, "test-user")?,
            2,
            false,
            "",
        )
        .await?;
        if partial {
            socket.write_all(&[0, 0]).await?;
        }
        assert!(closed(&mut socket).await?);
    }
    assert_eq!(probe.calls.load(Ordering::Acquire), 0);
    let mut socket = tls_client(&server).await?;
    oauth(
        &mut socket,
        harness.token_bytes(0, "shared-key", 30, "test-user")?,
        2,
        false,
        "",
    )
    .await?;
    let app = header(3, 0, 4, false);
    send(&mut socket, &app).await?;
    assert_eq!(receive(&mut socket).await?, app);
    server.shutdown().await?;
    service.shutdown().await?;
    harness.shutdown().await
}

#[tokio::test]
async fn revocation_cancels_active_handler_and_waiting_handler_admission() -> Result {
    let harness = Harness::start().await?;
    let service = service(&harness).await?;
    let probe = Arc::new(Probe::default());
    let mut server = listener(
        Profile::oidc_tls(service.clone(), Limits::default())?
            .with_advertised(&OIDC_METADATA_API_VERSIONS)?,
        probe.clone(),
        1,
    )
    .await?;
    let mut first = tls_client(&server).await?;
    let mut waiting = tls_client(&server).await?;
    oauth(
        &mut first,
        harness.token_bytes(0, "shared-key", 30, "test-user")?,
        2,
        false,
        "",
    )
    .await?;
    oauth(
        &mut waiting,
        harness.token_bytes(0, "shared-key", 30, "test-user")?,
        2,
        false,
        "",
    )
    .await?;
    probe.block.store(1, Ordering::Release);
    send(&mut first, &header(3, 0, 4, false)).await?;
    timeout(Duration::from_secs(1), probe.entered.acquire())
        .await??
        .forget();
    send(&mut waiting, &header(3, 0, 5, false)).await?;
    harness
        .authority
        .introspection_mode
        .store(5, Ordering::Release);
    assert!(closed(&mut first).await?);
    assert!(closed(&mut waiting).await?);
    assert_eq!(probe.calls.load(Ordering::Acquire), 1);
    assert_eq!(probe.cancelled.load(Ordering::Acquire), 1);
    harness
        .authority
        .introspection_mode
        .store(0, Ordering::Release);
    probe.block.store(0, Ordering::Release);
    let mut fresh = tls_client(&server).await?;
    oauth(
        &mut fresh,
        harness.token_bytes(0, "shared-key", 30, "test-user")?,
        2,
        false,
        "",
    )
    .await?;
    let app = header(3, 0, 6, false);
    send(&mut fresh, &app).await?;
    assert_eq!(receive(&mut fresh).await?, app);
    server.shutdown().await?;
    service.shutdown().await?;
    harness.shutdown().await
}

#[tokio::test]
async fn changed_key_closes_existing_socket_and_new_generation_authenticates() -> Result {
    let harness = Harness::start().await?;
    let service = service(&harness).await?;
    let probe = Arc::new(Probe::default());
    let mut server = listener(
        Profile::oidc_tls(service.clone(), Limits::default())?
            .with_advertised(&OIDC_METADATA_API_VERSIONS)?,
        probe.clone(),
        2,
    )
    .await?;
    let mut old = tls_client(&server).await?;
    oauth(
        &mut old,
        harness.token_bytes(0, "shared-key", 30, "test-user")?,
        2,
        false,
        "",
    )
    .await?;
    let app = header(3, 0, 4, false);
    send(&mut old, &app).await?;
    receive(&mut old).await?;
    let before = harness.authority.key_requests.load(Ordering::Acquire);
    harness.authority.generation.store(1, Ordering::Release);
    assert!(closed(&mut old).await?);
    at_least(&harness.authority.key_requests, before + 2).await?;
    let mut fresh = tls_client(&server).await?;
    oauth(
        &mut fresh,
        harness.token_bytes(1, "shared-key", 30, "test-user")?,
        2,
        false,
        "",
    )
    .await?;
    send(&mut fresh, &app).await?;
    receive(&mut fresh).await?;
    let ids = probe.identities.lock().await;
    assert_eq!(ids.len(), 2);
    assert!(ids[1].2 > ids[0].2);
    drop(ids);
    server.shutdown().await?;
    service.shutdown().await?;
    harness.shutdown().await
}

#[tokio::test]
async fn oauth_subject_never_inherits_credential_administrator_authority() -> Result {
    static NEXT: AtomicUsize = AtomicUsize::new(0);
    let path = std::env::temp_dir().join(format!(
        "partitionline-oidc-admin-{}-{}",
        std::process::id(),
        NEXT.fetch_add(1, Ordering::Relaxed)
    ));
    let (store, _) = Store::open(&path, CredentialLimits::default()).await?;
    let harness = Harness::start().await?;
    let service = service(&harness).await?;
    let probe = Arc::new(Probe::default());
    for (admins, expected) in [
        (vec![], 31),
        (
            vec![OAuthAdministrator {
                issuer: "https://different.example/issuer".into(),
                subject: "admin".into(),
            }],
            31,
        ),
        (
            vec![OAuthAdministrator {
                issuer: harness.authority.issuer.clone(),
                subject: "admin".into(),
            }],
            0,
        ),
    ] {
        let profile = Profile::tls(store.clone(), vec!["admin".into()], Limits::default())?
            .with_oidc(service.clone(), admins)?;
        let mut server = listener(profile, probe.clone(), 2).await?;
        let mut socket = tls_client(&server).await?;
        oauth(
            &mut socket,
            harness.token_bytes(0, "shared-key", 30, "admin")?,
            2,
            false,
            "",
        )
        .await?;
        let mut describe = header(50, 0, 8, true);
        describe.extend_from_slice(&[0, 0]);
        send(&mut socket, &describe).await?;
        assert_eq!(
            &receive(&mut socket).await?[9..11],
            &(expected as i16).to_be_bytes()
        );
        assert!(store.describe(None).await?.is_empty());
        server.shutdown().await?;
    }
    assert!(
        Profile::scram_plaintext(store.clone(), vec![], Limits::default())?
            .with_oidc(service.clone(), vec![])
            .is_err()
    );
    let mut server = listener(
        Profile::oidc_tls(service.clone(), Limits::default())?,
        probe.clone(),
        2,
    )
    .await?;
    let mut socket = tls_client(&server).await?;
    oauth(
        &mut socket,
        harness.token_bytes(0, "shared-key", 30, "admin")?,
        2,
        false,
        "",
    )
    .await?;
    let mut describe = header(50, 0, 9, true);
    describe.extend_from_slice(&[0, 0]);
    send(&mut socket, &describe).await?;
    assert!(closed(&mut socket).await?);
    server.shutdown().await?;
    assert_eq!(probe.calls.load(Ordering::Acquire), 0);
    service.shutdown().await?;
    harness.shutdown().await?;
    store.shutdown().await?;
    std::fs::remove_file(path)?;
    Ok(())
}

#[tokio::test]
async fn revocation_interrupts_a_blocked_write_without_delivering_the_complete_response() -> Result
{
    let harness = Harness::start().await?;
    let service = service(&harness).await?;
    let probe = Arc::new(Probe::default());
    let transport = Config::new(
        4,
        1,
        65536,
        32 * 1024 * 1024,
        Duration::from_secs(5),
        Duration::from_secs(5),
        Duration::from_secs(5),
    )?;
    let profile = Profile::oidc_tls(service.clone(), Limits::default())?
        .with_advertised(&OIDC_METADATA_API_VERSIONS)?;
    let mut server = Transport::bind_tls_sasl(
        SocketAddr::from((Ipv4Addr::LOCALHOST, 0)),
        transport,
        probe.clone(),
        acceptor()?,
        profile,
    )
    .await?;
    let mut socket = tls_client(&server).await?;
    oauth(
        &mut socket,
        harness.token_bytes(0, "shared-key", 30, "test-user")?,
        2,
        false,
        "",
    )
    .await?;
    probe.block.store(2, Ordering::Release);
    send(&mut socket, &header(3, 0, 50, false)).await?;
    let mut prefix = [0; 4];
    timeout(Duration::from_secs(2), socket.read_exact(&mut prefix)).await??;
    let expected = usize::try_from(i32::from_be_bytes(prefix))?;
    assert_eq!(expected, 32 * 1024 * 1024);
    // Reading only the prefix leaves a response much larger than socket/TLS
    // buffering blocked in write_all; provider revocation must cancel that I/O.
    let checks = harness.authority.checks.load(Ordering::Acquire);
    harness
        .authority
        .introspection_mode
        .store(5, Ordering::Release);
    at_least(&harness.authority.checks, checks + 1).await?;
    // Leave backpressure in place while the observed authority response is
    // consumed; draining the socket immediately would race a complete write.
    tokio::time::sleep(Duration::from_millis(100)).await;
    let received = timeout(Duration::from_secs(3), async {
        let mut count = 0;
        let mut chunk = [0; 65536];
        loop {
            match socket.read(&mut chunk).await {
                Ok(0) => break,
                Ok(n) => {
                    count += n;
                    assert!(count <= expected);
                }
                Err(error)
                    if matches!(
                        error.kind(),
                        io::ErrorKind::UnexpectedEof
                            | io::ErrorKind::ConnectionReset
                            | io::ErrorKind::BrokenPipe
                    ) =>
                {
                    break
                }
                Err(error) => return Err(error),
            }
        }
        Ok::<_, io::Error>(count)
    })
    .await??;
    assert!(
        received < expected,
        "expired write delivered a complete response"
    );
    assert_eq!(probe.calls.load(Ordering::Acquire), 1);
    let report = server.shutdown().await?;
    assert!(report.handler_errors >= 1);
    service.shutdown().await?;
    harness.shutdown().await
}

#[tokio::test]
async fn stalled_initial_authority_obeys_absolute_socket_deadline_and_releases_validation() -> Result
{
    let harness = Harness::start().await?;
    let mut trust = harness.trust();
    trust.limits.timeout = Duration::from_secs(2);
    let service = Service::start(harness.config(), trust).await?;
    let probe = Arc::new(Probe::default());
    let profile = Profile::oidc_tls(
        service.clone(),
        Limits {
            preauth_timeout: Duration::from_millis(200),
            ..Limits::default()
        },
    )?;
    let mut server = listener(profile, probe.clone(), 1).await?;
    let mut socket = tls_client(&server).await?;
    send(&mut socket, &handshake(1, 1, "OAUTHBEARER")).await?;
    receive(&mut socket).await?;
    harness
        .authority
        .introspection_mode
        .store(10, Ordering::Release);
    let first = initial(harness.token_bytes(0, "shared-key", 30, "test-user")?, "");
    send(&mut socket, &authenticate(2, 2, &first)).await?;
    timeout(Duration::from_secs(1), harness.authority.entered.acquire())
        .await??
        .forget();
    assert!(timeout(Duration::from_secs(1), closed(&mut socket)).await??);
    harness
        .authority
        .introspection_mode
        .store(0, Ordering::Release);
    harness.authority.release.add_permits(1);
    let mut fresh = tls_client(&server).await?;
    oauth(
        &mut fresh,
        harness.token_bytes(0, "shared-key", 30, "test-user")?,
        2,
        false,
        "",
    )
    .await?;
    assert_eq!(probe.calls.load(Ordering::Acquire), 0);
    let report = server.shutdown().await?;
    assert!(report.handler_deadlines >= 1);
    service.shutdown().await?;
    harness.shutdown().await
}

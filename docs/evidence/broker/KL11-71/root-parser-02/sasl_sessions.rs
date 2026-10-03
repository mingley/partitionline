//! Actual-socket SASL gating, typed identity, bounds and durable admin policy.
#![cfg(feature = "sasl")]
use base64::{engine::general_purpose::STANDARD, Engine};
use hmac::{Hmac, KeyInit, Mac};
use partitionline_broker::{
    security::{
        credentials::{Change, Limits as CredentialLimits, Store},
        sasl::{Algorithm, Secret},
        session::{Limits, Profile, SASL_METADATA_API_VERSIONS},
    },
    transport::{Config, Handler, Peer, Transport},
};
use pbkdf2::pbkdf2_hmac;
use sha2::{Digest, Sha256, Sha512};
use std::{
    io,
    net::{Ipv4Addr, SocketAddr},
    path::PathBuf,
    sync::{
        atomic::{AtomicU64, AtomicUsize, Ordering},
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
type Result<T = ()> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;
static NEXT: AtomicU64 = AtomicU64::new(0);
struct Path(PathBuf);
impl Path {
    fn new() -> Self {
        Self(std::env::temp_dir().join(format!(
            "partitionline-sasl-socket-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        )))
    }
}
impl Drop for Path {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.0);
    }
}
fn hex(value: &str) -> Result<Vec<u8>> {
    if value.len() % 2 != 0 {
        return Err("invalid fixture hex".into());
    }
    (0..value.len())
        .step_by(2)
        .map(|i| Ok(u8::from_str_radix(&value[i..i + 2], 16)?))
        .collect()
}
async fn store(path: &Path) -> Result<Store> {
    store_with_limits(path, CredentialLimits::default()).await
}
async fn store_with_limits(path: &Path, limits: CredentialLimits) -> Result<Store> {
    let (store, _) = Store::open(&path.0, limits).await?;
    for line in include_str!("fixtures/sasl-wire/apache-bootstrap.tsv")
        .lines()
        .skip(1)
    {
        let c: Vec<_> = line.split('\t').collect();
        assert_eq!(c.len(), 5);
        let algorithm = match c[1] {
            "SCRAM-SHA-256" => Algorithm::Sha256,
            "SCRAM-SHA-512" => Algorithm::Sha512,
            _ => return Err("invalid bootstrap algorithm".into()),
        };
        store
            .mutate(
                c[0].into(),
                vec![Change::Upsert {
                    algorithm,
                    iterations: c[2].parse()?,
                    salt: hex(c[3])?,
                    salted_password: Secret::new(hex(c[4])?),
                }],
            )
            .await?;
    }
    Ok(store)
}
#[derive(Default)]
struct Probe {
    calls: AtomicUsize,
    identities: Mutex<Vec<(String, u64)>>,
}
impl Handler for Probe {
    type Error = io::Error;
    async fn handle(&self, _request: Vec<u8>) -> io::Result<Option<Vec<u8>>> {
        Err(io::Error::new(
            io::ErrorKind::PermissionDenied,
            "missing typed peer",
        ))
    }
    async fn handle_with_peer(&self, peer: &Peer, request: Vec<u8>) -> io::Result<Option<Vec<u8>>> {
        let identity = peer.identity().ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::PermissionDenied,
                "unverified application dispatch",
            )
        })?;
        assert!(!format!("{peer:?}").contains(identity.name()));
        self.identities
            .lock()
            .await
            .push((identity.name().into(), identity.generation()));
        self.calls.fetch_add(1, Ordering::SeqCst);
        Ok(Some(request))
    }
}
fn config() -> Result<Config> {
    Ok(Config::new(
        16,
        8,
        65536,
        65536,
        Duration::from_secs(2),
        Duration::from_secs(2),
        Duration::from_secs(2),
    )?)
}
async fn plaintext(store: Store, probe: Arc<Probe>, limits: Limits) -> Result<Transport> {
    let profile = Profile::scram_plaintext(store, vec!["admin".into()], limits)?
        .with_advertised(&SASL_METADATA_API_VERSIONS)?;
    Ok(Transport::bind_sasl(
        SocketAddr::from((Ipv4Addr::LOCALHOST, 0)),
        config()?,
        probe,
        profile,
    )
    .await?)
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
    let (error, message, lifetime) = auth_response_lifetime(bytes, version, correlation)?;
    assert_eq!(lifetime, 0);
    Ok((error, message))
}
fn auth_response_lifetime(
    bytes: &[u8],
    version: i16,
    correlation: i32,
) -> Result<(i16, Vec<u8>, i64)> {
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
    let lifetime = if version >= 1 {
        let lifetime = i64::from_be_bytes(bytes[position..position + 8].try_into()?);
        position += 8;
        lifetime
    } else {
        0
    };
    if version == 2 {
        assert_eq!(bytes[position], 0);
        position += 1;
    }
    assert_eq!(position, bytes.len());
    Ok((error, message, lifetime))
}
async fn renewable_plaintext<H: Handler + 'static>(
    store: Store,
    probe: Arc<H>,
    lifetime: Duration,
    limits: Limits,
) -> Result<Transport> {
    let profile = Profile::scram_plaintext(store, vec!["admin".into()], limits)?
        .with_advertised(&SASL_METADATA_API_VERSIONS)?
        .with_reauthentication(lifetime)?;
    Ok(Transport::bind_sasl(
        SocketAddr::from((Ipv4Addr::LOCALHOST, 0)),
        config()?,
        probe,
        profile,
    )
    .await?)
}
async fn rotate_test_password(store: &Store, algorithm: Algorithm, password: &str) -> Result {
    let salt = b"public-renewal-salt-01".to_vec();
    let mut salted = vec![
        0;
        if algorithm == Algorithm::Sha256 {
            32
        } else {
            64
        }
    ];
    match algorithm {
        Algorithm::Sha256 => pbkdf2_hmac::<Sha256>(password.as_bytes(), &salt, 4096, &mut salted),
        Algorithm::Sha512 => pbkdf2_hmac::<Sha512>(password.as_bytes(), &salt, 4096, &mut salted),
    }
    store
        .mutate(
            "user".into(),
            vec![Change::Upsert {
                algorithm,
                iterations: 4096,
                salt,
                salted_password: Secret::new(salted),
            }],
        )
        .await?;
    Ok(())
}
fn mac(algorithm: Algorithm, key: &[u8], message: &[u8]) -> Result<Vec<u8>> {
    Ok(match algorithm {
        Algorithm::Sha256 => {
            let mut m = Hmac::<Sha256>::new_from_slice(key)?;
            m.update(message);
            m.finalize().into_bytes().to_vec()
        }
        Algorithm::Sha512 => {
            let mut m = Hmac::<Sha512>::new_from_slice(key)?;
            m.update(message);
            m.finalize().into_bytes().to_vec()
        }
    })
}
fn final_message(
    algorithm: Algorithm,
    password: &str,
    first: &str,
    server: &[u8],
) -> Result<(Vec<u8>, Vec<u8>)> {
    let server = std::str::from_utf8(server)?;
    let attr = |key: &str| {
        server
            .split(',')
            .find_map(|p| p.strip_prefix(key))
            .ok_or("missing challenge field")
    };
    let salt = STANDARD.decode(attr("s=")?)?;
    let iterations = attr("i=")?.parse()?;
    let without = format!("c=biws,r={}", attr("r=")?);
    let bare = first.strip_prefix("n,,").ok_or("missing GS2")?;
    let auth = format!("{bare},{server},{without}");
    let mut salted = vec![
        0;
        if algorithm == Algorithm::Sha256 {
            32
        } else {
            64
        }
    ];
    match algorithm {
        Algorithm::Sha256 => {
            pbkdf2_hmac::<Sha256>(password.as_bytes(), &salt, iterations, &mut salted)
        }
        Algorithm::Sha512 => {
            pbkdf2_hmac::<Sha512>(password.as_bytes(), &salt, iterations, &mut salted)
        }
    };
    let client = mac(algorithm, &salted, b"Client Key")?;
    let stored = match algorithm {
        Algorithm::Sha256 => Sha256::digest(&client).to_vec(),
        Algorithm::Sha512 => Sha512::digest(&client).to_vec(),
    };
    let signature = mac(algorithm, &stored, auth.as_bytes())?;
    let proof: Vec<_> = client.iter().zip(signature).map(|(a, b)| a ^ b).collect();
    let expected = format!(
        "v={}",
        STANDARD.encode(mac(
            algorithm,
            &mac(algorithm, &salted, b"Server Key")?,
            auth.as_bytes()
        )?)
    )
    .into_bytes();
    Ok((
        format!("{without},p={}", STANDARD.encode(proof)).into_bytes(),
        expected,
    ))
}
async fn scram<S: AsyncRead + AsyncWrite + Unpin>(
    socket: &mut S,
    algorithm: Algorithm,
    user: &str,
    password: &str,
    version: i16,
    legacy: bool,
) -> Result<i16> {
    let (error, lifetime) =
        scram_lifetime(socket, algorithm, user, password, version, legacy).await?;
    assert_eq!(lifetime, 0);
    Ok(error)
}
async fn scram_lifetime<S: AsyncRead + AsyncWrite + Unpin>(
    socket: &mut S,
    algorithm: Algorithm,
    user: &str,
    password: &str,
    version: i16,
    legacy: bool,
) -> Result<(i16, i64)> {
    send(
        socket,
        &handshake(if legacy { 0 } else { 1 }, 77, algorithm.name()),
    )
    .await?;
    let response = receive(socket).await?;
    assert_eq!(
        &response[..6],
        &[&77i32.to_be_bytes()[..], &0i16.to_be_bytes()[..]].concat()
    );
    scram_proof(socket, algorithm, user, password, version, legacy).await
}
async fn scram_proof<S: AsyncRead + AsyncWrite + Unpin>(
    socket: &mut S,
    algorithm: Algorithm,
    user: &str,
    password: &str,
    version: i16,
    legacy: bool,
) -> Result<(i16, i64)> {
    let first = format!("n,,n={user},r=public-socket-client-nonce");
    send(
        socket,
        &if legacy {
            first.as_bytes().to_vec()
        } else {
            authenticate(version, 78, first.as_bytes())
        },
    )
    .await?;
    let response = receive(socket).await?;
    let challenge = if legacy {
        response
    } else {
        let (error, challenge, lifetime) = auth_response_lifetime(&response, version, 78)?;
        assert_eq!(error, 0);
        assert_eq!(lifetime, 0);
        challenge
    };
    let (final_message, expected) = final_message(algorithm, password, &first, &challenge)?;
    send(
        socket,
        &if legacy {
            final_message
        } else {
            authenticate(version, 79, &final_message)
        },
    )
    .await?;
    let response = receive(socket).await?;
    if legacy {
        assert_eq!(response, expected);
        Ok((0, 0))
    } else {
        let (error, signature, lifetime) = auth_response_lifetime(&response, version, 79)?;
        if error == 0 {
            assert_eq!(signature, expected);
        }
        Ok((error, lifetime))
    }
}
#[tokio::test]
async fn actual_socket_scram_all_versions_legacy_and_typed_identity_gate() -> Result {
    let path = Path::new();
    let store = store(&path).await?;
    let probe = Arc::new(Probe::default());
    let mut server = plaintext(store.clone(), probe.clone(), Limits::default()).await?;
    for algorithm in [Algorithm::Sha256, Algorithm::Sha512] {
        for version in 0..=2 {
            let mut socket = TcpStream::connect(server.local_addr()).await?;
            assert_eq!(
                scram(&mut socket, algorithm, "user", "pencil", version, false).await?,
                0
            );
            let mut app = header(3, 0, 80, false);
            app.extend_from_slice(&(-1i32).to_be_bytes());
            send(&mut socket, &app).await?;
            assert_eq!(receive(&mut socket).await?, app);
        }
        let mut socket = TcpStream::connect(server.local_addr()).await?;
        scram(&mut socket, algorithm, "user", "pencil", 0, true).await?;
    }
    assert_eq!(probe.calls.load(Ordering::SeqCst), 6);
    assert!(probe
        .identities
        .lock()
        .await
        .iter()
        .all(|(user, _)| user == "user"));
    server.shutdown().await?;
    store.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn renewable_scram_versions_rotation_and_same_principal_preserve_socket_identity() -> Result {
    for algorithm in [Algorithm::Sha256, Algorithm::Sha512] {
        for version in [1, 2] {
            let path = Path::new();
            let store = store(&path).await?;
            let probe = Arc::new(Probe::default());
            let mut server = renewable_plaintext(
                store.clone(),
                probe.clone(),
                Duration::from_secs(2),
                Limits::default(),
            )
            .await?;
            let mut socket = TcpStream::connect(server.local_addr()).await?;
            assert_eq!(
                scram_lifetime(&mut socket, algorithm, "user", "pencil", version, false).await?,
                (0, 2000)
            );
            send(&mut socket, &header(3, 0, 101, false)).await?;
            receive(&mut socket).await?;
            let first = probe
                .identities
                .lock()
                .await
                .last()
                .cloned()
                .ok_or("initial identity")?;
            rotate_test_password(&store, algorithm, "public-rotated-pencil").await?;
            assert_eq!(
                scram_lifetime(
                    &mut socket,
                    algorithm,
                    "user",
                    "public-rotated-pencil",
                    version,
                    false
                )
                .await?,
                (0, 2000)
            );
            send(&mut socket, &header(3, 0, 102, false)).await?;
            receive(&mut socket).await?;
            let last = probe
                .identities
                .lock()
                .await
                .last()
                .cloned()
                .ok_or("renewed identity")?;
            assert_eq!(first.0, last.0);
            assert!(last.1 > first.1);
            assert_eq!(probe.calls.load(Ordering::SeqCst), 2);
            let report = server.shutdown().await?;
            assert_eq!(report.accepted_connections, report.joined_connections);
            store.shutdown().await?;
        }
    }
    Ok(())
}

#[tokio::test]
async fn renewable_profile_auth0_remains_zero_and_cannot_start_renewal() -> Result {
    let path = Path::new();
    let store = store(&path).await?;
    let probe = Arc::new(Probe::default());
    let mut server = renewable_plaintext(
        store.clone(),
        probe.clone(),
        Duration::from_millis(100),
        Limits::default(),
    )
    .await?;
    let mut socket = TcpStream::connect(server.local_addr()).await?;
    assert_eq!(
        scram_lifetime(&mut socket, Algorithm::Sha256, "user", "pencil", 0, false).await?,
        (0, 0)
    );
    tokio::time::sleep(Duration::from_millis(180)).await;
    send(&mut socket, &header(3, 0, 111, false)).await?;
    receive(&mut socket).await?;
    send(&mut socket, &handshake(1, 112, "SCRAM-SHA-256")).await?;
    let response = receive(&mut socket).await?;
    assert_eq!(&response[4..6], &34i16.to_be_bytes());
    assert!(closed(&mut socket).await?);
    assert_eq!(probe.calls.load(Ordering::SeqCst), 1);
    server.shutdown().await?;
    store.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn renewed_principal_change_valid_proof_is_terminal_and_cannot_dispatch() -> Result {
    let path = Path::new();
    let store = store(&path).await?;
    let probe = Arc::new(Probe::default());
    let mut server = renewable_plaintext(
        store.clone(),
        probe.clone(),
        Duration::from_secs(2),
        Limits::default(),
    )
    .await?;
    let mut socket = TcpStream::connect(server.local_addr()).await?;
    assert_eq!(
        scram_lifetime(&mut socket, Algorithm::Sha256, "user", "pencil", 2, false).await?,
        (0, 2000)
    );
    assert_eq!(
        scram_lifetime(&mut socket, Algorithm::Sha256, "admin", "pencil", 2, false).await?,
        (58, 0)
    );
    assert!(closed(&mut socket).await?);
    assert_eq!(probe.calls.load(Ordering::SeqCst), 0);
    server.shutdown().await?;
    store.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn renewed_mechanism_unknown_and_authenticate_without_handshake_are_terminal() -> Result {
    let path = Path::new();
    let store = store(&path).await?;
    let probe = Arc::new(Probe::default());
    let mut server = renewable_plaintext(
        store.clone(),
        probe.clone(),
        Duration::from_secs(2),
        Limits::default(),
    )
    .await?;
    for (request, expected) in [
        (handshake(1, 121, "SCRAM-SHA-512"), 58),
        (handshake(1, 122, "UNKNOWN"), 33),
        (handshake(0, 123, "SCRAM-SHA-256"), 34),
    ] {
        let mut socket = TcpStream::connect(server.local_addr()).await?;
        scram_lifetime(&mut socket, Algorithm::Sha256, "user", "pencil", 2, false).await?;
        send(&mut socket, &request).await?;
        let response = receive(&mut socket).await?;
        assert_eq!(&response[4..6], &(expected as i16).to_be_bytes());
        assert!(closed(&mut socket).await?);
    }
    let mut socket = TcpStream::connect(server.local_addr()).await?;
    scram_lifetime(&mut socket, Algorithm::Sha256, "user", "pencil", 2, false).await?;
    send(
        &mut socket,
        &authenticate(2, 124, b"not a renewed handshake"),
    )
    .await?;
    assert_eq!(
        auth_response_lifetime(&receive(&mut socket).await?, 2, 124)?.0,
        34
    );
    assert!(closed(&mut socket).await?);
    assert_eq!(probe.calls.load(Ordering::SeqCst), 0);
    server.shutdown().await?;
    store.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn renewable_idle_expiry_and_incomplete_renewal_cannot_extend_old_deadline() -> Result {
    let path = Path::new();
    let store = store(&path).await?;
    let probe = Arc::new(Probe::default());
    let mut server = renewable_plaintext(
        store.clone(),
        probe.clone(),
        Duration::from_millis(500),
        Limits::default(),
    )
    .await?;
    let mut idle = TcpStream::connect(server.local_addr()).await?;
    assert_eq!(
        scram_lifetime(&mut idle, Algorithm::Sha256, "user", "pencil", 2, false).await?,
        (0, 500)
    );
    assert!(timeout(Duration::from_millis(1400), closed(&mut idle)).await??);
    let mut renewing = TcpStream::connect(server.local_addr()).await?;
    scram_lifetime(&mut renewing, Algorithm::Sha256, "user", "pencil", 2, false).await?;
    send(&mut renewing, &handshake(1, 131, "SCRAM-SHA-256")).await?;
    receive(&mut renewing).await?;
    assert!(timeout(Duration::from_millis(1400), closed(&mut renewing)).await??);
    assert_eq!(probe.calls.load(Ordering::SeqCst), 0);
    server.shutdown().await?;
    store.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn renewal_application_and_remaining_byte_budget_never_bypass_authentication() -> Result {
    let path = Path::new();
    let store = store(&path).await?;
    let probe = Arc::new(Probe::default());
    let limits = Limits {
        preauth_bytes: 1024,
        frame_bytes: 512,
        ..Limits::default()
    };
    let mut server =
        renewable_plaintext(store.clone(), probe.clone(), Duration::from_secs(2), limits).await?;
    for byte_budget in [false, true] {
        let mut socket = TcpStream::connect(server.local_addr()).await?;
        scram_lifetime(&mut socket, Algorithm::Sha256, "user", "pencil", 2, false).await?;
        send(&mut socket, &handshake(1, 141, "SCRAM-SHA-256")).await?;
        receive(&mut socket).await?;
        if byte_budget {
            socket.write_all(&513i32.to_be_bytes()).await?;
        } else {
            send(&mut socket, &header(3, 0, 142, false)).await?;
        }
        assert!(closed(&mut socket).await?);
    }
    assert_eq!(probe.calls.load(Ordering::SeqCst), 0);
    server.shutdown().await?;
    store.shutdown().await?;
    Ok(())
}
struct DrainingProbe {
    probe: Probe,
    entered: Semaphore,
    release: Semaphore,
    cancelled: AtomicUsize,
}
impl Default for DrainingProbe {
    fn default() -> Self {
        Self {
            probe: Probe::default(),
            entered: Semaphore::new(0),
            release: Semaphore::new(0),
            cancelled: AtomicUsize::new(0),
        }
    }
}
struct CancelledHandler<'a> {
    counter: &'a AtomicUsize,
    completed: bool,
}
impl Drop for CancelledHandler<'_> {
    fn drop(&mut self) {
        if !self.completed {
            self.counter.fetch_add(1, Ordering::SeqCst);
        }
    }
}
impl Handler for DrainingProbe {
    type Error = io::Error;
    async fn handle(&self, request: Vec<u8>) -> io::Result<Option<Vec<u8>>> {
        self.probe.handle(request).await
    }
    async fn handle_with_peer(&self, peer: &Peer, request: Vec<u8>) -> io::Result<Option<Vec<u8>>> {
        let response = self.probe.handle_with_peer(peer, request).await?;
        if self.probe.calls.load(Ordering::SeqCst) == 1 {
            let mut active = CancelledHandler {
                counter: &self.cancelled,
                completed: false,
            };
            self.entered.add_permits(1);
            self.release
                .acquire()
                .await
                .map_err(|_| io::Error::other("test release stopped"))?
                .forget();
            active.completed = true;
        }
        Ok(response)
    }
}
#[tokio::test]
async fn renewal_handshake_waits_for_prior_application_reply_and_resumes_after_proof() -> Result {
    let path = Path::new();
    let store = store(&path).await?;
    let probe = Arc::new(DrainingProbe::default());
    let mut server = renewable_plaintext(
        store.clone(),
        probe.clone(),
        Duration::from_secs(2),
        Limits::default(),
    )
    .await?;
    let mut socket = TcpStream::connect(server.local_addr()).await?;
    scram_lifetime(&mut socket, Algorithm::Sha256, "user", "pencil", 2, false).await?;
    let app = header(3, 0, 151, false);
    send(&mut socket, &app).await?;
    timeout(Duration::from_secs(1), probe.entered.acquire())
        .await??
        .forget();
    send(&mut socket, &handshake(1, 152, "SCRAM-SHA-256")).await?;
    let mut byte = [0];
    assert!(timeout(Duration::from_millis(50), socket.read(&mut byte))
        .await
        .is_err());
    assert_eq!(probe.probe.calls.load(Ordering::SeqCst), 1);
    probe.release.add_permits(1);
    assert_eq!(receive(&mut socket).await?, app);
    let response = receive(&mut socket).await?;
    assert_eq!(
        &response[..6],
        &[&152i32.to_be_bytes()[..], &0i16.to_be_bytes()[..]].concat()
    );
    assert_eq!(
        scram_proof(&mut socket, Algorithm::Sha256, "user", "pencil", 2, false).await?,
        (0, 2000)
    );
    let app = header(3, 0, 153, false);
    send(&mut socket, &app).await?;
    assert_eq!(receive(&mut socket).await?, app);
    assert_eq!(probe.probe.calls.load(Ordering::SeqCst), 2);
    assert_eq!(probe.cancelled.load(Ordering::SeqCst), 0);
    let report = server.shutdown().await?;
    assert_eq!(report.accepted_connections, report.joined_connections);
    store.shutdown().await?;
    Ok(())
}
#[tokio::test]
async fn renewal_expiry_cancels_application_handler_and_joins_owned_connection() -> Result {
    let path = Path::new();
    let store = store(&path).await?;
    let probe = Arc::new(DrainingProbe::default());
    let mut server = renewable_plaintext(
        store.clone(),
        probe.clone(),
        Duration::from_millis(500),
        Limits::default(),
    )
    .await?;
    let mut socket = TcpStream::connect(server.local_addr()).await?;
    scram_lifetime(&mut socket, Algorithm::Sha256, "user", "pencil", 2, false).await?;
    send(&mut socket, &header(3, 0, 161, false)).await?;
    timeout(Duration::from_secs(1), probe.entered.acquire())
        .await??
        .forget();
    send(&mut socket, &handshake(1, 162, "SCRAM-SHA-256")).await?;
    assert!(timeout(Duration::from_millis(1400), closed(&mut socket)).await??);
    assert_eq!(probe.cancelled.load(Ordering::SeqCst), 1);
    assert_eq!(probe.probe.calls.load(Ordering::SeqCst), 1);
    let report = server.shutdown().await?;
    assert_eq!(report.accepted_connections, report.joined_connections);
    store.shutdown().await?;
    Ok(())
}
#[tokio::test]
async fn renewal_handler_admission_wait_consumes_authentication_deadline() -> Result {
    let path = Path::new();
    let store = store(&path).await?;
    let probe = Arc::new(DrainingProbe::default());
    let limits = Limits {
        preauth_timeout: Duration::from_millis(300),
        ..Limits::default()
    };
    let profile = Profile::scram_plaintext(store.clone(), vec![], limits)?
        .with_advertised(&SASL_METADATA_API_VERSIONS)?
        .with_reauthentication(Duration::from_secs(2))?;
    let transport_config = Config::new(
        4,
        1,
        65536,
        65536,
        Duration::from_secs(2),
        Duration::from_secs(2),
        Duration::from_secs(2),
    )?;
    let mut server = Transport::bind_sasl(
        SocketAddr::from((Ipv4Addr::LOCALHOST, 0)),
        transport_config,
        probe.clone(),
        profile,
    )
    .await?;
    let mut first = TcpStream::connect(server.local_addr()).await?;
    let mut renewing = TcpStream::connect(server.local_addr()).await?;
    scram_lifetime(&mut first, Algorithm::Sha256, "user", "pencil", 2, false).await?;
    scram_lifetime(&mut renewing, Algorithm::Sha256, "user", "pencil", 2, false).await?;
    let app = header(3, 0, 181, false);
    send(&mut first, &app).await?;
    timeout(Duration::from_secs(1), probe.entered.acquire())
        .await??
        .forget();
    send(&mut renewing, &handshake(1, 182, "SCRAM-SHA-256")).await?;
    assert!(timeout(Duration::from_secs(1), closed(&mut renewing)).await??);
    assert_eq!(probe.probe.calls.load(Ordering::SeqCst), 1);
    probe.release.add_permits(1);
    assert_eq!(receive(&mut first).await?, app);
    let report = server.shutdown().await?;
    assert_eq!(report.accepted_connections, report.joined_connections);
    store.shutdown().await?;
    Ok(())
}
#[tokio::test]
async fn unsupported_plain_early_application_wrong_proof_and_repeated_handshake_are_terminal(
) -> Result {
    let path = Path::new();
    let store = store(&path).await?;
    let probe = Arc::new(Probe::default());
    let mut server = plaintext(store.clone(), probe.clone(), Limits::default()).await?;
    let mut socket = TcpStream::connect(server.local_addr()).await?;
    send(&mut socket, &handshake(1, 1, "PLAIN")).await?;
    assert_eq!(&receive(&mut socket).await?[4..6], &33i16.to_be_bytes());
    assert!(closed(&mut socket).await?);
    let mut socket = TcpStream::connect(server.local_addr()).await?;
    send(&mut socket, &header(3, 0, 2, false)).await?;
    assert!(closed(&mut socket).await?);
    let mut socket = TcpStream::connect(server.local_addr()).await?;
    send(&mut socket, &authenticate(2, 3, b"\0user\0pencil")).await?;
    assert_eq!(auth_response(&receive(&mut socket).await?, 2, 3)?.0, 34);
    assert!(closed(&mut socket).await?);
    let mut socket = TcpStream::connect(server.local_addr()).await?;
    assert_eq!(
        scram(
            &mut socket,
            Algorithm::Sha256,
            "user",
            "incorrect",
            2,
            false
        )
        .await?,
        58
    );
    assert!(closed(&mut socket).await?);
    let mut socket = TcpStream::connect(server.local_addr()).await?;
    scram(&mut socket, Algorithm::Sha256, "user", "pencil", 2, false).await?;
    send(&mut socket, &handshake(1, 4, "SCRAM-SHA-256")).await?;
    assert_eq!(&receive(&mut socket).await?[4..6], &34i16.to_be_bytes());
    assert!(closed(&mut socket).await?);
    assert_eq!(probe.calls.load(Ordering::SeqCst), 0);
    server.shutdown().await?;
    store.shutdown().await?;
    Ok(())
}
#[tokio::test]
async fn preauth_deadline_rounds_bytes_and_shutdown_are_bound_to_each_socket() -> Result {
    let path = Path::new();
    let store = store(&path).await?;
    let probe = Arc::new(Probe::default());
    let limits = Limits {
        preauth_timeout: Duration::from_millis(150),
        frame_bytes: 256,
        preauth_bytes: 1024,
        control_rounds: 2,
        ..Limits::default()
    };
    let mut server = plaintext(store.clone(), probe.clone(), limits).await?;
    let mut idle = TcpStream::connect(server.local_addr()).await?;
    assert!(closed(&mut idle).await?);
    let mut socket = TcpStream::connect(server.local_addr()).await?;
    for correlation in [1, 2] {
        send(&mut socket, &header(18, 0, correlation, false)).await?;
        let bytes = receive(&mut socket).await?;
        assert_eq!(&bytes[..4], &correlation.to_be_bytes());
    }
    send(&mut socket, &header(18, 0, 3, false)).await?;
    assert!(closed(&mut socket).await?);
    let mut oversized = TcpStream::connect(server.local_addr()).await?;
    oversized.write_all(&257i32.to_be_bytes()).await?;
    assert!(closed(&mut oversized).await?);
    let mut waiting = TcpStream::connect(server.local_addr()).await?;
    send(&mut waiting, &handshake(1, 5, "SCRAM-SHA-256")).await?;
    receive(&mut waiting).await?;
    server.shutdown().await?;
    assert!(closed(&mut waiting).await?);
    assert_eq!(probe.calls.load(Ordering::SeqCst), 0);
    store.shutdown().await?;
    Ok(())
}
#[tokio::test]
async fn admin_authority_is_explicit_and_unauthorized_alter_never_reaches_storage() -> Result {
    let path = Path::new();
    let store = store(&path).await?;
    let probe = Arc::new(Probe::default());
    let mut server = plaintext(store.clone(), probe, Limits::default()).await?;
    let before = store.describe(None).await?.len();
    let mut socket = TcpStream::connect(server.local_addr()).await?;
    scram(&mut socket, Algorithm::Sha256, "user", "pencil", 2, false).await?;
    let mut describe = header(50, 0, 90, true);
    describe.extend_from_slice(&[0, 0]);
    send(&mut socket, &describe).await?;
    let response = receive(&mut socket).await?;
    assert_eq!(&response[9..11], &31i16.to_be_bytes());
    let line = include_str!("fixtures/sasl-wire/apache-wire.tsv")
        .lines()
        .skip(1)
        .find(|l| l.starts_with("request\talter-delete\t"))
        .ok_or("missing independent alter fixture")?;
    let fields: Vec<_> = line.split('\t').collect();
    send(&mut socket, &hex(fields[8])?).await?;
    let response = receive(&mut socket).await?;
    assert!(response.windows(2).any(|p| p == 31i16.to_be_bytes()));
    assert_eq!(store.describe(None).await?.len(), before);
    server.shutdown().await?;
    store.shutdown().await?;
    Ok(())
}
#[cfg(feature = "tls")]
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
#[cfg(feature = "tls")]
#[tokio::test]
async fn tls_plain_renewal_validates_fresh_password_and_preserves_principal() -> Result {
    use partitionline_broker::security::tls::{Acceptor, ClientAuth};
    let path = Path::new();
    let store = store(&path).await?;
    let probe = Arc::new(Probe::default());
    let acceptor = Acceptor::new(
        vec![include_bytes!("fixtures/tls/server1.cert.der").to_vec()],
        include_bytes!("fixtures/tls/server1.key.der").to_vec(),
        ClientAuth::ServerOnly,
        Default::default(),
    )?;
    let profile = Profile::tls(store.clone(), vec!["admin".into()], Limits::default())?
        .with_advertised(&SASL_METADATA_API_VERSIONS)?
        .with_reauthentication(Duration::from_secs(2))?;
    let mut server = Transport::bind_tls_sasl(
        SocketAddr::from((Ipv4Addr::LOCALHOST, 0)),
        config()?,
        probe.clone(),
        acceptor,
        profile,
    )
    .await?;
    for version in [1, 2] {
        let mut socket = tls_client(&server).await?;
        for round in 0..2 {
            send(&mut socket, &handshake(1, 171, "PLAIN")).await?;
            assert_eq!(&receive(&mut socket).await?[4..6], &0i16.to_be_bytes());
            send(&mut socket, &authenticate(version, 172, b"\0user\0pencil")).await?;
            assert_eq!(
                auth_response_lifetime(&receive(&mut socket).await?, version, 172)?,
                (0, Vec::new(), 2000)
            );
            let app = header(3, 0, 173 + round, false);
            send(&mut socket, &app).await?;
            assert_eq!(receive(&mut socket).await?, app);
            if round == 0 {
                // Replacement of the actor verifier is observed on the renewal
                // handshake even when the public test password is unchanged.
                rotate_test_password(&store, Algorithm::Sha256, "pencil").await?;
            }
        }
    }
    let identities = probe.identities.lock().await;
    assert_eq!(identities.len(), 4);
    assert!(identities.iter().all(|(name, _)| name == "user"));
    assert!(identities[1].1 > identities[0].1);
    assert!(identities[3].1 > identities[2].1);
    drop(identities);
    let report = server.shutdown().await?;
    assert_eq!(report.accepted_connections, report.joined_connections);
    store.shutdown().await?;
    Ok(())
}
#[cfg(feature = "tls")]
#[tokio::test]
async fn tls_plain_all_versions_legacy_unicode_and_poison_shutdown_dispatch_gate() -> Result {
    use partitionline_broker::security::tls::{Acceptor, ClientAuth};
    let path = Path::new();
    let store = store(&path).await?;
    let probe = Arc::new(Probe::default());
    let acceptor = Acceptor::new(
        vec![include_bytes!("fixtures/tls/server1.cert.der").to_vec()],
        include_bytes!("fixtures/tls/server1.key.der").to_vec(),
        ClientAuth::ServerOnly,
        Default::default(),
    )?;
    let profile = Profile::tls(store.clone(), vec!["admin".into()], Limits::default())?
        .with_advertised(&SASL_METADATA_API_VERSIONS)?;
    let mut server = Transport::bind_tls_sasl(
        SocketAddr::from((Ipv4Addr::LOCALHOST, 0)),
        config()?,
        probe.clone(),
        acceptor,
        profile,
    )
    .await?;
    for version in 0..=2 {
        let mut socket = tls_client(&server).await?;
        send(&mut socket, &handshake(1, 1, "PLAIN")).await?;
        receive(&mut socket).await?;
        send(&mut socket, &authenticate(version, 2, b"\0user\0pencil")).await?;
        assert_eq!(
            auth_response(&receive(&mut socket).await?, version, 2)?.0,
            0
        );
        send(&mut socket, &header(3, 0, 3, false)).await?;
        receive(&mut socket).await?;
    }
    let mut socket = tls_client(&server).await?;
    send(&mut socket, &handshake(0, 4, "PLAIN")).await?;
    receive(&mut socket).await?;
    send(&mut socket, b"\0user\0pencil").await?;
    assert!(receive(&mut socket).await?.is_empty());
    send(&mut socket, &header(3, 0, 5, false)).await?;
    receive(&mut socket).await?;
    let mut unicode = tls_client(&server).await?;
    send(&mut unicode, &handshake(1, 7, "PLAIN")).await?;
    receive(&mut unicode).await?;
    send(
        &mut unicode,
        &authenticate(2, 8, "\0unicode\0péncil-🔑".as_bytes()),
    )
    .await?;
    assert_eq!(auth_response(&receive(&mut unicode).await?, 2, 8)?.0, 0);
    send(&mut unicode, &header(3, 0, 9, false)).await?;
    receive(&mut unicode).await?;
    let row = include_str!("fixtures/sasl-wire/apache-bootstrap.tsv")
        .lines()
        .skip(1)
        .find(|line| line.starts_with("unicode\tSCRAM-SHA-256\t"))
        .ok_or("missing Unicode password oracle")?;
    let fields: Vec<_> = row.split('\t').collect();
    store
        .mutate(
            "用户".into(),
            vec![Change::Upsert {
                algorithm: Algorithm::Sha256,
                iterations: fields[2].parse()?,
                salt: hex(fields[3])?,
                salted_password: Secret::new(hex(fields[4])?),
            }],
        )
        .await?;
    let mut unicode_name = tls_client(&server).await?;
    send(&mut unicode_name, &handshake(1, 10, "PLAIN")).await?;
    receive(&mut unicode_name).await?;
    send(
        &mut unicode_name,
        &authenticate(2, 11, "\0用户\0péncil-🔑".as_bytes()),
    )
    .await?;
    assert_eq!(
        auth_response(&receive(&mut unicode_name).await?, 2, 11)?.0,
        0
    );
    send(&mut unicode_name, &header(3, 0, 12, false)).await?;
    receive(&mut unicode_name).await?;
    assert!(probe
        .identities
        .lock()
        .await
        .iter()
        .any(|(name, _)| name == "用户"));
    let mut scram_name = tls_client(&server).await?;
    send(&mut scram_name, &handshake(1, 13, "SCRAM-SHA-256")).await?;
    receive(&mut scram_name).await?;
    send(
        &mut scram_name,
        &authenticate(2, 14, "n,,n=用户,r=public-nonce".as_bytes()),
    )
    .await?;
    assert_eq!(
        auth_response(&receive(&mut scram_name).await?, 2, 14)?.0,
        58
    );
    assert!(closed(&mut scram_name).await?);
    assert_eq!(probe.calls.load(Ordering::SeqCst), 6);
    store.shutdown().await?;
    send(&mut socket, &header(3, 0, 6, false)).await?;
    assert!(closed(&mut socket).await?);
    assert_eq!(probe.calls.load(Ordering::SeqCst), 6);
    server.shutdown().await?;
    Ok(())
}

fn skip_nullable_compact(bytes: &[u8], position: &mut usize) -> Result {
    let length = read_varint(bytes, position)?.checked_sub(1);
    if let Some(length) = length {
        *position = position.checked_add(length).ok_or("peer field overflow")?;
        if *position > bytes.len() {
            return Err("peer field truncation".into());
        }
    }
    Ok(())
}
fn compact_peer_string(bytes: &[u8], position: &mut usize) -> Result<String> {
    let length = read_varint(bytes, position)?
        .checked_sub(1)
        .ok_or("null peer string")?;
    let value = std::str::from_utf8(
        bytes
            .get(*position..*position + length)
            .ok_or("peer string truncated")?,
    )?
    .to_owned();
    *position += length;
    Ok(value)
}
fn alter_results(bytes: &[u8]) -> Result<Vec<(String, i16)>> {
    assert_eq!(bytes[4], 0);
    assert_eq!(&bytes[5..9], &0i32.to_be_bytes());
    let mut position = 9;
    let count = read_varint(bytes, &mut position)?
        .checked_sub(1)
        .ok_or("null peer results")?;
    let mut results = Vec::new();
    for _ in 0..count {
        let user = compact_peer_string(bytes, &mut position)?;
        let error = i16::from_be_bytes(bytes[position..position + 2].try_into()?);
        position += 2;
        skip_nullable_compact(bytes, &mut position)?;
        assert_eq!(read_varint(bytes, &mut position)?, 0);
        results.push((user, error));
    }
    assert_eq!(read_varint(bytes, &mut position)?, 0);
    assert_eq!(position, bytes.len());
    Ok(results)
}
fn upsert_packet(user: &str, algorithm: Algorithm, password: &str, correlation: i32) -> Vec<u8> {
    let mut out = header(51, 0, correlation, true);
    out.push(1);
    out.push(2);
    varint(&mut out, user.len() + 1);
    out.extend_from_slice(user.as_bytes());
    out.push(if algorithm == Algorithm::Sha256 { 1 } else { 2 });
    out.extend_from_slice(&4096i32.to_be_bytes());
    let salt = b"public-wire-rotation-salt";
    let mut salted = vec![
        0;
        if algorithm == Algorithm::Sha256 {
            32
        } else {
            64
        }
    ];
    match algorithm {
        Algorithm::Sha256 => pbkdf2_hmac::<Sha256>(password.as_bytes(), salt, 4096, &mut salted),
        Algorithm::Sha512 => pbkdf2_hmac::<Sha512>(password.as_bytes(), salt, 4096, &mut salted),
    }
    varint(&mut out, salt.len() + 1);
    out.extend_from_slice(salt);
    varint(&mut out, salted.len() + 1);
    out.extend_from_slice(&salted);
    out.extend_from_slice(&[0, 0]);
    out
}
#[tokio::test]
async fn authorized_wire_rotation_restart_duplicate_policy_invalid_credentials_and_delete() -> Result
{
    let path = Path::new();
    let store = store(&path).await?;
    let probe = Arc::new(Probe::default());
    let mut server = plaintext(store.clone(), probe, Limits::default()).await?;
    let mut admin = TcpStream::connect(server.local_addr()).await?;
    scram(&mut admin, Algorithm::Sha512, "admin", "pencil", 2, false).await?;
    let baseline_generation = store
        .plain(Secret::new(b"\0user\0pencil".to_vec()))
        .await?
        .generation();
    for name in [
        "alter-duplicate",
        "alter-both",
        "alter-invalid-iterations",
        "alter-empty-salt",
        "alter-short-salted",
    ] {
        let row = include_str!("fixtures/sasl-wire/apache-wire.tsv")
            .lines()
            .skip(1)
            .find(|l| l.starts_with(&format!("request\t{name}\t")))
            .ok_or("missing independent admin request")?;
        let c: Vec<_> = row.split('\t').collect();
        send(&mut admin, &hex(c[8])?).await?;
        let results = alter_results(&receive(&mut admin).await?)?;
        assert_eq!(
            results,
            vec![(
                "user".into(),
                if matches!(name, "alter-duplicate" | "alter-both") {
                    92
                } else {
                    93
                }
            )]
        );
        assert_eq!(
            store
                .plain(Secret::new(b"\0user\0pencil".to_vec()))
                .await?
                .generation(),
            baseline_generation
        );
    }
    for algorithm in [Algorithm::Sha256, Algorithm::Sha512] {
        send(
            &mut admin,
            &upsert_packet("user", algorithm, "rotated", 101),
        )
        .await?;
        assert_eq!(
            alter_results(&receive(&mut admin).await?)?,
            vec![("user".into(), 0)]
        );
        let mut user = TcpStream::connect(server.local_addr()).await?;
        assert_eq!(
            scram(&mut user, algorithm, "user", "rotated", 2, false).await?,
            0
        );
    }
    server.shutdown().await?;
    store.shutdown().await?;
    let (store, recovery) = Store::open(&path.0, CredentialLimits::default()).await?;
    assert_eq!(recovery.recovered_entries, 8);
    let mut server =
        plaintext(store.clone(), Arc::new(Probe::default()), Limits::default()).await?;
    let mut user = TcpStream::connect(server.local_addr()).await?;
    assert_eq!(
        scram(&mut user, Algorithm::Sha256, "user", "rotated", 2, false).await?,
        0
    );
    let mut admin = TcpStream::connect(server.local_addr()).await?;
    scram(&mut admin, Algorithm::Sha256, "admin", "pencil", 1, false).await?;
    let mut delete = header(51, 0, 102, true);
    delete.push(2);
    delete.push(5);
    delete.extend_from_slice(b"user");
    delete.extend_from_slice(&[1, 0, 1, 0]);
    send(&mut admin, &delete).await?;
    assert_eq!(
        alter_results(&receive(&mut admin).await?)?,
        vec![("user".into(), 0)]
    );
    send(&mut admin, &delete).await?;
    assert_eq!(
        alter_results(&receive(&mut admin).await?)?,
        vec![("user".into(), 91)]
    );
    server.shutdown().await?;
    store.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn wire_budget_failure_never_acknowledges_success_or_publishes_new_credentials() -> Result {
    let path = Path::new();
    let store = store_with_limits(
        &path,
        CredentialLimits {
            operations: 6,
            ..CredentialLimits::default()
        },
    )
    .await?;
    let generation = store
        .plain(Secret::new(b"\0user\0pencil".to_vec()))
        .await?
        .generation();
    let mut server =
        plaintext(store.clone(), Arc::new(Probe::default()), Limits::default()).await?;
    let mut admin = TcpStream::connect(server.local_addr()).await?;
    scram(&mut admin, Algorithm::Sha256, "admin", "pencil", 2, false).await?;
    send(
        &mut admin,
        &upsert_packet("user", Algorithm::Sha256, "uncommitted", 201),
    )
    .await?;
    assert_eq!(
        alter_results(&receive(&mut admin).await?)?,
        vec![("user".into(), 89)]
    );
    assert!(store.is_healthy());
    assert_eq!(
        store
            .plain(Secret::new(b"\0user\0pencil".to_vec()))
            .await?
            .generation(),
        generation
    );
    server.shutdown().await?;
    store.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn cumulative_preauth_bytes_include_prefixes_and_cannot_be_reset_by_valid_negotiation(
) -> Result {
    let path = Path::new();
    let store = store(&path).await?;
    let mut server = plaintext(
        store.clone(),
        Arc::new(Probe::default()),
        Limits {
            frame_bytes: 256,
            preauth_bytes: 1024,
            control_rounds: 8,
            ..Limits::default()
        },
    )
    .await?;
    let mut socket = TcpStream::connect(server.local_addr()).await?;
    for correlation in 0..4 {
        let mut request = header(18, 0, correlation, false);
        request.truncate(8);
        request.extend_from_slice(&240i16.to_be_bytes());
        request.extend_from_slice(&[b'x'; 240]);
        send(&mut socket, &request).await?;
        let response = receive(&mut socket).await?;
        assert_eq!(&response[..4], &correlation.to_be_bytes());
    }
    let mut request = header(18, 0, 4, false);
    request.truncate(8);
    request.extend_from_slice(&240i16.to_be_bytes());
    request.extend_from_slice(&[b'x'; 240]);
    send(&mut socket, &request).await?;
    assert!(closed(&mut socket).await?);
    let report = server.shutdown().await?;
    assert_eq!(report.invalid_lengths, 1);
    store.shutdown().await?;
    Ok(())
}
#[cfg(feature = "tls")]
#[tokio::test]
async fn absolute_sasl_admission_deadline_also_caps_unfinished_tls_handshake() -> Result {
    use partitionline_broker::security::tls::{Acceptor, ClientAuth};
    let path = Path::new();
    let store = store(&path).await?;
    let acceptor = Acceptor::new(
        vec![include_bytes!("fixtures/tls/server1.cert.der").to_vec()],
        include_bytes!("fixtures/tls/server1.key.der").to_vec(),
        ClientAuth::ServerOnly,
        Default::default(),
    )?;
    let profile = Profile::tls(
        store.clone(),
        vec![],
        Limits {
            preauth_timeout: Duration::from_millis(150),
            ..Limits::default()
        },
    )?;
    let mut server = Transport::bind_tls_sasl(
        SocketAddr::from((Ipv4Addr::LOCALHOST, 0)),
        config()?,
        Arc::new(Probe::default()),
        acceptor,
        profile,
    )
    .await?;
    let mut raw = TcpStream::connect(server.local_addr()).await?;
    assert!(closed(&mut raw).await?);
    let report = server.shutdown().await?;
    assert_eq!(report.tls_handshake_deadlines, 1);
    store.shutdown().await?;
    Ok(())
}

async fn authenticated_control_case(key: i16) -> Result {
    let path = Path::new();
    let store = store(&path).await?;
    let generation = store
        .plain(Secret::new(b"\0user\0pencil".to_vec()))
        .await?
        .generation();
    let probe = Arc::new(Probe::default());
    let mut server = plaintext(
        store.clone(),
        probe.clone(),
        Limits {
            frame_bytes: 256,
            ..Limits::default()
        },
    )
    .await?;
    let request = match key {
        17 => handshake(1, 301, "SCRAM-SHA-256"),
        36 => authenticate(2, 302, b"\0user\0pencil"),
        50 => {
            let mut request = header(50, 0, 303, true);
            request.extend_from_slice(&[0, 0]);
            request
        }
        51 => upsert_packet("user", Algorithm::Sha256, "forbidden-by-control-cap", 304),
        _ => return Err("unexpected test control API".into()),
    };
    let mut socket = TcpStream::connect(server.local_addr()).await?;
    scram(&mut socket, Algorithm::Sha256, "admin", "pencil", 2, false).await?;
    // Classic nullable ClientId also occurs in flexible request headers. The
    // frame and schema remain valid under the wider transport allocation cap.
    let mut padded = request[..8].to_vec();
    padded.extend_from_slice(&300i16.to_be_bytes());
    padded.extend_from_slice(&[b'x'; 300]);
    padded.extend_from_slice(&request[10..]);
    assert!(padded.len() > 256);
    send(&mut socket, &padded).await?;
    assert!(
        closed(&mut socket).await?,
        "control API{key} bypassed its configured ceiling"
    );
    assert_eq!(
        store
            .plain(Secret::new(b"\0user\0pencil".to_vec()))
            .await?
            .generation(),
        generation
    );
    assert_eq!(probe.calls.load(Ordering::SeqCst), 0);
    server.shutdown().await?;
    store.shutdown().await?;
    Ok(())
}
#[tokio::test]
async fn authenticated_handshake_control_ceiling_precedes_header_parse() -> Result {
    authenticated_control_case(17).await
}
#[tokio::test]
async fn authenticated_authenticate_control_ceiling_precedes_header_parse() -> Result {
    authenticated_control_case(36).await
}
#[tokio::test]
async fn authenticated_describe_control_ceiling_precedes_header_parse() -> Result {
    authenticated_control_case(50).await
}
#[tokio::test]
async fn authenticated_alter_control_ceiling_precedes_header_parse_and_storage() -> Result {
    authenticated_control_case(51).await
}

#[tokio::test]
async fn canonical_and_native_redundant_empty_alter_tag_commit_once_and_survive_restart() -> Result
{
    let path = Path::new();
    let store = store(&path).await?;
    let mut server =
        plaintext(store.clone(), Arc::new(Probe::default()), Limits::default()).await?;
    let mut admin = TcpStream::connect(server.local_addr()).await?;
    scram(&mut admin, Algorithm::Sha256, "admin", "pencil", 2, false).await?;
    let mut generation = store
        .plain(Secret::new(b"\0admin\0pencil".to_vec()))
        .await?
        .generation();
    let mut users = Vec::new();
    for (index, algorithm) in [Algorithm::Sha256, Algorithm::Sha512]
        .into_iter()
        .enumerate()
    {
        for native_tail in [false, true] {
            let user = format!("compat-{index}-{native_tail}");
            let mut packet = upsert_packet(&user, algorithm, "public-compatible-password", 401);
            if native_tail {
                // Actual librdkafka2.15 writes canonical body tags, then its
                // FLEXVER finalizer appends one further empty top-level block.
                packet.push(0);
            }
            send(&mut admin, &packet).await?;
            assert_eq!(
                alter_results(&receive(&mut admin).await?)?,
                vec![(user.clone(), 0)]
            );
            generation += 1;
            assert_eq!(
                store
                    .plain(Secret::new(b"\0admin\0pencil".to_vec()))
                    .await?
                    .generation(),
                generation
            );
            let mut socket = TcpStream::connect(server.local_addr()).await?;
            assert_eq!(
                scram(
                    &mut socket,
                    algorithm,
                    &user,
                    "public-compatible-password",
                    2,
                    false
                )
                .await?,
                0
            );
            users.push((user, algorithm));
        }
    }
    server.shutdown().await?;
    store.shutdown().await?;
    let (store, recovery) = Store::open(&path.0, CredentialLimits::default()).await?;
    assert_eq!(recovery.recovered_entries, 10);
    let mut server =
        plaintext(store.clone(), Arc::new(Probe::default()), Limits::default()).await?;
    for (user, algorithm) in users {
        let mut socket = TcpStream::connect(server.local_addr()).await?;
        assert_eq!(
            scram(
                &mut socket,
                algorithm,
                &user,
                "public-compatible-password",
                1,
                false
            )
            .await?,
            0
        );
    }
    server.shutdown().await?;
    store.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn malformed_alter_tails_and_other_api_empty_tails_never_mutate_or_dispatch() -> Result {
    let path = Path::new();
    let store = store(&path).await?;
    let probe = Arc::new(Probe::default());
    let mut server = plaintext(store.clone(), probe.clone(), Limits::default()).await?;
    let generation = store
        .plain(Secret::new(b"\0admin\0pencil".to_vec()))
        .await?
        .generation();
    for tail in [&[1][..], &[0, 0], &[0, 1], &[1, 0], &[0, 0, 0], &[128]] {
        let mut socket = TcpStream::connect(server.local_addr()).await?;
        scram(&mut socket, Algorithm::Sha256, "admin", "pencil", 2, false).await?;
        let mut packet = upsert_packet("forbidden-tail", Algorithm::Sha256, "not-committed", 402);
        packet.extend_from_slice(tail);
        send(&mut socket, &packet).await?;
        assert!(closed(&mut socket).await?);
        assert_eq!(
            store
                .plain(Secret::new(b"\0admin\0pencil".to_vec()))
                .await?
                .generation(),
            generation
        );
        assert!(store
            .describe(Some(vec!["forbidden-tail".into()]))
            .await?
            .is_empty());
    }
    for key in [17, 36, 50] {
        let mut socket = TcpStream::connect(server.local_addr()).await?;
        scram(&mut socket, Algorithm::Sha256, "admin", "pencil", 2, false).await?;
        let mut packet = match key {
            17 => handshake(1, 403, "SCRAM-SHA-256"),
            36 => authenticate(2, 403, b"invalid-in-authenticated-state"),
            50 => {
                let mut packet = header(50, 0, 403, true);
                packet.extend_from_slice(&[0, 0]);
                packet
            }
            _ => unreachable!(),
        };
        packet.push(0);
        send(&mut socket, &packet).await?;
        assert!(closed(&mut socket).await?);
    }
    assert_eq!(probe.calls.load(Ordering::SeqCst), 0);
    server.shutdown().await?;
    store.shutdown().await?;
    let (store, recovery) = Store::open(&path.0, CredentialLimits::default()).await?;
    assert_eq!(recovery.recovered_entries, 6);
    assert!(store
        .describe(Some(vec!["forbidden-tail".into()]))
        .await?
        .is_empty());
    store.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn authentic_native_106_and_apache_canonical_105_frames_commit_without_rewriting() -> Result {
    let canonical =
        hex(include_str!("fixtures/sasl-wire/native-alter-canonical.frame.hex").trim())?;
    let native =
        hex(include_str!("fixtures/sasl-wire/native-alter-redundant-empty-tag.frame.hex").trim())?;
    assert_eq!((canonical.len(), native.len()), (105, 106));
    assert_eq!(&canonical[4..], &native[4..native.len() - 1]);
    assert_eq!(native.last(), Some(&0));
    let path = Path::new();
    let store = store(&path).await?;
    let mut server =
        plaintext(store.clone(), Arc::new(Probe::default()), Limits::default()).await?;
    let mut admin = TcpStream::connect(server.local_addr()).await?;
    scram(&mut admin, Algorithm::Sha256, "admin", "pencil", 2, false).await?;
    let mut generation = store
        .plain(Secret::new(b"\0admin\0pencil".to_vec()))
        .await?
        .generation();
    for frame in [&canonical, &native] {
        assert_eq!(
            i32::from_be_bytes(frame[..4].try_into()?) as usize,
            frame.len() - 4
        );
        // Forward the independently captured/generated full length-prefixed
        // frame byte for byte, including its original ClientId/correlation7.
        admin.write_all(frame).await?;
        admin.flush().await?;
        let response = receive(&mut admin).await?;
        assert_eq!(i32::from_be_bytes(response[..4].try_into()?), 7);
        assert_eq!(
            alter_results(&response)?,
            vec![("native-created".into(), 0)]
        );
        generation += 1;
        assert_eq!(
            store
                .plain(Secret::new(b"\0native-created\0pencil".to_vec()))
                .await?
                .generation(),
            generation
        );
    }
    let info = store.describe(Some(vec!["native-created".into()])).await?;
    assert_eq!(info.len(), 1);
    assert_eq!(info[0].algorithm(), Algorithm::Sha256);
    assert_eq!(info[0].iterations(), 4096);
    server.shutdown().await?;
    store.shutdown().await?;
    let (store, recovery) = Store::open(&path.0, CredentialLimits::default()).await?;
    assert_eq!(recovery.recovered_entries, 8);
    let mut server =
        plaintext(store.clone(), Arc::new(Probe::default()), Limits::default()).await?;
    let mut user = TcpStream::connect(server.local_addr()).await?;
    assert_eq!(
        scram(
            &mut user,
            Algorithm::Sha256,
            "native-created",
            "pencil",
            2,
            false
        )
        .await?,
        0
    );
    server.shutdown().await?;
    store.shutdown().await?;
    Ok(())
}

#[derive(Default)]
struct ReadyAfterExpiryProbe {
    calls: AtomicUsize,
}
impl Handler for ReadyAfterExpiryProbe {
    type Error = io::Error;
    async fn handle(&self, _: Vec<u8>) -> io::Result<Option<Vec<u8>>> {
        Err(io::Error::other("missing authenticated peer"))
    }
    #[expect(
        clippy::disallowed_methods,
        reason = "deliberate synchronous work exercises timeout_at Ready completion after expiry"
    )]
    async fn handle_with_peer(&self, peer: &Peer, request: Vec<u8>) -> io::Result<Option<Vec<u8>>> {
        assert!(peer.identity().is_some());
        self.calls.fetch_add(1, Ordering::SeqCst);
        // Synchronous work cannot be preempted by timeout_at. The expired
        // result must still be discarded before the actual Transport writes it.
        std::thread::sleep(Duration::from_millis(300));
        Ok(Some(request))
    }
}
#[tokio::test(flavor = "current_thread")]
async fn ready_handler_after_expiry_does_not_emit_expired_reply() -> Result {
    let path = Path::new();
    let store = store(&path).await?;
    let probe = Arc::new(ReadyAfterExpiryProbe::default());
    let mut server = renewable_plaintext(
        store.clone(),
        probe.clone(),
        Duration::from_millis(100),
        Limits::default(),
    )
    .await?;
    let mut socket = TcpStream::connect(server.local_addr()).await?;
    assert_eq!(
        scram_lifetime(&mut socket, Algorithm::Sha256, "user", "pencil", 2, false).await?,
        (0, 100)
    );
    send(&mut socket, &header(3, 0, 201, false)).await?;
    let result = receive(&mut socket).await;
    let report = server.shutdown().await?;
    store.shutdown().await?;
    assert_eq!(report.accepted_connections, report.joined_connections);
    assert_eq!(probe.calls.load(Ordering::SeqCst), 1);
    assert!(
        result.is_err(),
        "complete expired application reply was delivered"
    );
    Ok(())
}

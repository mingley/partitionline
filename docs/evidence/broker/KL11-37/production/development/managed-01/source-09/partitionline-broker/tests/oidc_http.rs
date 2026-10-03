//! Actual TLS authority, refresh, revocation and ownership regressions.
#![cfg(feature = "oidc")]

use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine as _};
use partitionline_broker::security::{
    oidc::{
        AccessToken, Algorithm, Config, Error, HttpLimits, HttpsTrust, Introspection, KeySource,
        Limits, Policy, RuntimeLimits, Service,
    },
    sasl::Secret,
};
use ring::{
    rand::SystemRandom,
    signature::{EcdsaKeyPair, KeyPair, ECDSA_P256_SHA256_FIXED_SIGNING},
};
use rustls::{
    pki_types::{CertificateDer, PrivateKeyDer, PrivatePkcs8KeyDer},
    ServerConfig,
};
use serde_json::{json, Value};
use std::{
    error::Error as StdError,
    io,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    time::{Duration, SystemTime},
};
use tokio::{
    io::{AsyncReadExt as _, AsyncWriteExt as _},
    net::{TcpListener, TcpStream},
    sync::{watch, Semaphore},
    task::{JoinHandle, JoinSet},
    time::timeout,
};
use tokio_rustls::{server::TlsStream, TlsAcceptor};

type Result<T = ()> = std::result::Result<T, Box<dyn StdError + Send + Sync>>;
struct Authority {
    issuer: String,
    keys: [EcdsaKeyPair; 2],
    generation: AtomicUsize,
    discovery_mode: AtomicUsize,
    key_mode: AtomicUsize,
    introspection_mode: AtomicUsize,
    discoveries: AtomicUsize,
    key_requests: AtomicUsize,
    checks: AtomicUsize,
    entered: Semaphore,
    release: Semaphore,
}
struct Harness {
    authority: Arc<Authority>,
    stop: watch::Sender<bool>,
    join: JoinHandle<Result>,
}
impl Harness {
    async fn start() -> Result<Self> {
        let random = SystemRandom::new();
        let make_key = || -> Result<EcdsaKeyPair> {
            let bytes = EcdsaKeyPair::generate_pkcs8(&ECDSA_P256_SHA256_FIXED_SIGNING, &random)
                .map_err(|_| io::Error::other("test key generation failed"))?;
            EcdsaKeyPair::from_pkcs8(&ECDSA_P256_SHA256_FIXED_SIGNING, bytes.as_ref(), &random)
                .map_err(|_| io::Error::other("test key parsing failed").into())
        };
        let listener = TcpListener::bind((std::net::Ipv4Addr::LOCALHOST, 0)).await?;
        let issuer = format!("https://localhost:{}/issuer", listener.local_addr()?.port());
        let authority = Arc::new(Authority {
            issuer,
            keys: [make_key()?, make_key()?],
            generation: AtomicUsize::new(0),
            discovery_mode: AtomicUsize::new(0),
            key_mode: AtomicUsize::new(0),
            introspection_mode: AtomicUsize::new(0),
            discoveries: AtomicUsize::new(0),
            key_requests: AtomicUsize::new(0),
            checks: AtomicUsize::new(0),
            entered: Semaphore::new(0),
            release: Semaphore::new(0),
        });
        let server =
            ServerConfig::builder_with_provider(Arc::new(rustls::crypto::ring::default_provider()))
                .with_safe_default_protocol_versions()?
                .with_no_client_auth()
                .with_single_cert(
                    vec![CertificateDer::from(
                        include_bytes!("fixtures/tls/server1.cert.der").to_vec(),
                    )],
                    PrivateKeyDer::Pkcs8(PrivatePkcs8KeyDer::from(
                        include_bytes!("fixtures/tls/server1.key.der").to_vec(),
                    )),
                )?;
        let acceptor = TlsAcceptor::from(Arc::new(server));
        let (stop, mut stopped) = watch::channel(false);
        let state = authority.clone();
        let join = tokio::spawn(async move {
            let mut workers = JoinSet::new();
            loop {
                tokio::select! {
                    biased;
                    _=stopped.changed()=>break,
                    Some(result)=workers.join_next(), if !workers.is_empty()=>{result??;},
                    incoming=listener.accept()=>{
                        let (socket, _)=incoming?;
                        let acceptor=acceptor.clone();
                        let state=state.clone();
                        let mut stop=stopped.clone();
                        workers.spawn(async move {
                            tokio::select! {
                                biased;
                                _=stop.changed()=>Ok(()),
                                r=serve(acceptor,socket,state)=>r,
                            }
                        });
                    }
                }
            }
            while let Some(result) = workers.join_next().await {
                result??;
            }
            Ok(())
        });
        Ok(Self {
            authority,
            stop,
            join,
        })
    }
    fn config(&self) -> Config {
        Config {
            policy: Policy {
                issuer: self.authority.issuer.clone(),
                audiences: vec!["partitionline".to_owned()],
                algorithms: vec![Algorithm::Es256],
                access_token: AccessToken::AtJwt,
                limits: Limits {
                    authority_freshness: Duration::from_secs(2),
                    ..Limits::default()
                },
            },
            keys: KeySource::Discovery {
                allowed_jwks_origins: vec![self
                    .authority
                    .issuer
                    .trim_end_matches("/issuer")
                    .to_owned()],
            },
            introspection: Introspection {
                endpoint: format!("{}/introspect", self.authority.issuer),
                client_id: "client".to_owned(),
                client_secret: Secret::new(b"synthetic-secret".to_vec()),
            },
            runtime: RuntimeLimits {
                active_leases: 4,
                validations: 8,
                refresh_interval: Duration::from_millis(100),
                unknown_kid_cooldown: Duration::from_secs(1),
                revocation_lease: Duration::from_millis(800),
                revocation_refresh: Duration::from_millis(100),
            },
        }
    }
    fn trust(&self) -> HttpsTrust {
        HttpsTrust {
            roots: vec![include_bytes!("fixtures/tls/ca1.cert.der").to_vec()],
            limits: HttpLimits {
                timeout: Duration::from_millis(300),
                ..HttpLimits::default()
            },
        }
    }
    fn token(&self, key: usize, kid: &str, lifetime: u64) -> Result<Secret> {
        let now = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)?
            .as_secs();
        let header = URL_SAFE_NO_PAD.encode(serde_json::to_vec(
            &json!({"alg":"ES256","kid":kid,"typ":"at+jwt"}),
        )?);
        let payload = URL_SAFE_NO_PAD.encode(serde_json::to_vec(&json!({"iss":self.authority.issuer,"sub":"test-user","aud":"partitionline","iat":now,"exp":now+lifetime,"jti":"synthetic-token-id"}))?);
        let signing = format!("{header}.{payload}");
        let proof = self.authority.keys[key]
            .sign(&SystemRandom::new(), signing.as_bytes())
            .map_err(|_| io::Error::other("test signing failed"))?;
        Ok(Secret::new(
            format!("{signing}.{}", URL_SAFE_NO_PAD.encode(proof.as_ref())).into_bytes(),
        ))
    }
    async fn shutdown(self) -> Result {
        self.stop.send_replace(true);
        self.join.await??;
        Ok(())
    }
}
async fn serve(acceptor: TlsAcceptor, socket: TcpStream, authority: Arc<Authority>) -> Result {
    // Invalid-CA and cancellation cells deliberately disconnect during TLS/read.
    let mut stream = match acceptor.accept(socket).await {
        Ok(stream) => stream,
        Err(_) => return Ok(()),
    };
    let mut bytes = Vec::new();
    let end = loop {
        if let Some(end) = bytes.windows(4).position(|p| p == b"\r\n\r\n") {
            break end + 4;
        }
        if bytes.len() >= 32768 {
            return Err("test request ceiling exceeded".into());
        }
        let mut chunk = [0u8; 1024];
        let count = stream.read(&mut chunk).await?;
        if count == 0 {
            return Ok(());
        }
        bytes.extend_from_slice(&chunk[..count]);
    };
    let head = std::str::from_utf8(&bytes[..end])?.to_owned();
    let length = head
        .lines()
        .find_map(|line| {
            let (name, value) = line.split_once(':')?;
            name.eq_ignore_ascii_case("content-length")
                .then_some(value.trim())
        })
        .map(str::parse::<usize>)
        .transpose()?
        .unwrap_or(0);
    if length > 16384 {
        return Err("test body ceiling exceeded".into());
    }
    while bytes.len() < end + length {
        let mut chunk = [0u8; 1024];
        let count = stream.read(&mut chunk).await?;
        if count == 0 {
            return Ok(());
        }
        bytes.extend_from_slice(&chunk[..count]);
    }
    let path = head
        .split_whitespace()
        .nth(1)
        .ok_or("missing request target")?;
    let (mode, value) = match path {
        "/issuer/.well-known/openid-configuration" => {
            authority.discoveries.fetch_add(1, Ordering::AcqRel);
            let mode = authority.discovery_mode.load(Ordering::Acquire);
            let issuer = if mode == 3 {
                "https://wrong.example"
            } else {
                &authority.issuer
            };
            let keys = if mode == 4 {
                "https://unapproved.example/keys".to_owned()
            } else {
                format!("{}/keys", authority.issuer)
            };
            (mode, json!({"issuer":issuer,"jwks_uri":keys}))
        }
        "/issuer/keys" => {
            authority.key_requests.fetch_add(1, Ordering::AcqRel);
            let generation = authority.generation.load(Ordering::Acquire);
            let public = authority.keys[generation].public_key().as_ref();
            (
                authority.key_mode.load(Ordering::Acquire),
                json!({"keys":[{"kty":"EC","crv":"P-256","kid":"shared-key","x":URL_SAFE_NO_PAD.encode(&public[1..33]),"y":URL_SAFE_NO_PAD.encode(&public[33..65])}]}),
            )
        }
        "/issuer/introspect" => {
            authority.checks.fetch_add(1, Ordering::AcqRel);
            if !head.lines().any(|line| {
                line.eq_ignore_ascii_case("authorization: Basic Y2xpZW50OnN5bnRoZXRpYy1zZWNyZXQ=")
            }) {
                return reply(&mut stream, 401, b"{}", "application/json", "").await;
            }
            let form = std::str::from_utf8(&bytes[end..end + length])?;
            // Compact JWS uses only unescaped base64url and '.', so this test
            // issuer needs no permissive form-decoding shortcut.
            let token = form
                .strip_prefix("token=")
                .and_then(|s| s.strip_suffix("&token_type_hint=access_token"))
                .ok_or("bad introspection form")?;
            let payload = token.split('.').nth(1).ok_or("missing token payload")?;
            let mut value: Value = serde_json::from_slice(&URL_SAFE_NO_PAD.decode(payload)?)?;
            let mode = authority.introspection_mode.load(Ordering::Acquire);
            value["active"] = json!(mode != 5);
            match mode {
                6 => value["sub"] = json!("different-user"),
                7 => value["iss"] = json!("https://wrong.example"),
                8 => value["aud"] = json!("other-service"),
                9 => value["exp"] = json!("not-an-integer"),
                14 => {
                    value["exp"] = json!(value["exp"].as_u64().ok_or("bad test expiration")? + 100)
                }
                15 => value["active"] = json!("true"),
                16 => value["aud"] = json!(["partitionline", "partitionline"]),
                17 => value["jti"] = json!("different-token-id"),
                _ => {}
            }
            (mode, value)
        }
        _ => return Err("unexpected test request target".into()),
    };
    if mode == 10 {
        authority.entered.add_permits(1);
        authority.release.acquire().await?.forget();
    }
    let body = serde_json::to_vec(&value)?;
    match mode {
        1 => reply(&mut stream, 503, b"{}", "application/json", "").await,
        2 => {
            reply(
                &mut stream,
                302,
                b"{}",
                "application/json",
                "Location: https://unapproved.example/\r\n",
            )
            .await
        }
        11 => {
            reply(
                &mut stream,
                200,
                b"{\"duplicate\":1,\"duplicate\":2}",
                "application/json",
                "",
            )
            .await
        }
        12 => reply(&mut stream, 200, &vec![b'x'; 65537], "application/json", "").await,
        13 => reply(&mut stream, 200, &body, "text/html", "").await,
        _ => reply(&mut stream, 200, &body, "application/json", "").await,
    }
}
async fn reply(
    stream: &mut TlsStream<TcpStream>,
    status: u16,
    body: &[u8],
    content_type: &str,
    extra: &str,
) -> Result {
    let head=format!("HTTP/1.1 {status} Test\r\nContent-Type: {content_type}\r\nContent-Length: {}\r\nConnection: close\r\n{extra}\r\n",body.len());
    // A bounded client rejection may close before the complete oversized body.
    if stream.write_all(head.as_bytes()).await.is_err() {
        return Ok(());
    }
    if stream.write_all(body).await.is_err() {
        return Ok(());
    }
    let _ = stream.shutdown().await;
    Ok(())
}
async fn at_least(counter: &AtomicUsize, value: usize) -> Result {
    timeout(Duration::from_secs(3), async {
        while counter.load(Ordering::Acquire) < value {
            tokio::task::yield_now().await;
        }
    })
    .await?;
    Ok(())
}

#[tokio::test]
async fn actual_https_discovery_trust_whole_json_and_size_policy() -> Result {
    let harness = Harness::start().await?;
    let mut untrusted = harness.trust();
    untrusted.roots = vec![include_bytes!("fixtures/tls/ca2.cert.der").to_vec()];
    assert_eq!(
        Service::start(harness.config(), untrusted)
            .await
            .unwrap_err(),
        Error::Unavailable
    );
    for mode in [1, 2, 3, 4, 11, 12, 13] {
        harness
            .authority
            .discovery_mode
            .store(mode, Ordering::Release);
        assert!(
            Service::start(harness.config(), harness.trust())
                .await
                .is_err(),
            "bad discovery accepted"
        );
    }
    harness.authority.discovery_mode.store(0, Ordering::Release);
    let service = Service::start(harness.config(), harness.trust()).await?;
    let lease = service
        .validate(harness.token(0, "shared-key", 30)?)
        .await?;
    assert_eq!(lease.subject(), "test-user");
    assert_eq!(lease.issuer(), harness.authority.issuer);
    assert!(lease.failure().is_none());
    service.shutdown().await?;
    assert_eq!(lease.invalidated().await, Error::Unavailable);
    harness.shutdown().await
}

#[tokio::test]
async fn revocation_binding_and_outage_never_extend_a_lease() -> Result {
    let harness = Harness::start().await?;
    let mut config = harness.config();
    // Both audiences are locally allowed, but the provider's active response
    // must still bind the exact audience carried by this verified token.
    config.policy.audiences.push("other-service".to_owned());
    let service = Service::start(config, harness.trust()).await?;
    for mode in [5, 6, 7, 8, 9, 11, 14, 15, 16, 17] {
        harness
            .authority
            .introspection_mode
            .store(mode, Ordering::Release);
        assert!(
            service
                .validate(harness.token(0, "shared-key", 30)?)
                .await
                .is_err(),
            "bad introspection accepted"
        );
    }
    harness
        .authority
        .introspection_mode
        .store(0, Ordering::Release);
    let lease = service
        .validate(harness.token(0, "shared-key", 30)?)
        .await?;
    let original = lease.deadline();
    let checks = harness.authority.checks.load(Ordering::Acquire);
    harness
        .authority
        .introspection_mode
        .store(1, Ordering::Release);
    at_least(&harness.authority.checks, checks + 2).await?;
    assert_eq!(lease.deadline(), original);
    assert_eq!(
        timeout(Duration::from_secs(2), lease.invalidated()).await?,
        Error::Expired
    );
    harness
        .authority
        .introspection_mode
        .store(0, Ordering::Release);
    assert_eq!(lease.failure(), Some(Error::Expired));
    let fresh = service
        .validate(harness.token(0, "shared-key", 30)?)
        .await?;
    harness
        .authority
        .introspection_mode
        .store(5, Ordering::Release);
    assert_eq!(
        timeout(Duration::from_secs(2), fresh.invalidated()).await?,
        Error::Revoked
    );
    service.shutdown().await?;
    harness.shutdown().await
}

#[tokio::test]
async fn removed_or_changed_key_interrupts_stalled_introspection() -> Result {
    let harness = Harness::start().await?;
    let service = Service::start(harness.config(), harness.trust()).await?;
    let lease = service
        .validate(harness.token(0, "shared-key", 30)?)
        .await?;
    harness
        .authority
        .introspection_mode
        .store(10, Ordering::Release);
    timeout(Duration::from_secs(2), harness.authority.entered.acquire())
        .await??
        .forget();
    // Same kid, independently generated replacement public material: a kid-only
    // cache comparison would incorrectly leave this lease authenticated.
    harness.authority.generation.store(1, Ordering::Release);
    assert_eq!(
        timeout(Duration::from_secs(2), lease.invalidated()).await?,
        Error::Revoked
    );
    harness
        .authority
        .introspection_mode
        .store(0, Ordering::Release);
    harness.authority.release.add_permits(1);
    let replacement = service
        .validate(harness.token(1, "shared-key", 30)?)
        .await?;
    assert!(replacement.generation() > lease.generation());
    service.shutdown().await?;
    harness.shutdown().await
}

#[tokio::test]
async fn key_rotation_during_initial_introspection_cannot_publish_old_authentication() -> Result {
    let harness = Harness::start().await?;
    let mut trust = harness.trust();
    trust.limits.timeout = Duration::from_secs(2);
    let service = Service::start(harness.config(), trust).await?;
    harness
        .authority
        .introspection_mode
        .store(10, Ordering::Release);
    let token = harness.token(0, "shared-key", 30)?;
    let other = service.clone();
    let validating = tokio::spawn(async move { other.validate(token).await });
    timeout(Duration::from_secs(2), harness.authority.entered.acquire())
        .await??
        .forget();
    let before = harness.authority.key_requests.load(Ordering::Acquire);
    harness.authority.generation.store(1, Ordering::Release);
    // Two manager fetches prove that its preceding generation was published,
    // before allowing the old proof's introspection to complete.
    at_least(&harness.authority.key_requests, before + 2).await?;
    harness.authority.release.add_permits(1);
    assert_eq!(validating.await?.unwrap_err(), Error::Revoked);
    service.shutdown().await?;
    harness.shutdown().await
}

#[tokio::test]
async fn authority_outage_expiry_admission_and_joined_shutdown() -> Result {
    let harness = Harness::start().await?;
    let mut config = harness.config();
    config.runtime.active_leases = 1;
    let service = Service::start(config, harness.trust()).await?;
    let lease = service
        .validate(harness.token(0, "shared-key", 30)?)
        .await?;
    assert_eq!(
        service
            .validate(harness.token(0, "shared-key", 30)?)
            .await
            .unwrap_err(),
        Error::Busy
    );
    harness.authority.key_mode.store(1, Ordering::Release);
    assert_eq!(
        timeout(Duration::from_secs(3), lease.invalidated()).await?,
        Error::Expired
    );
    // Failure publication wakes the socket before the task's final permit drop.
    // Wait for that bounded cleanup, while preserving Busy as a valid outcome.
    timeout(Duration::from_secs(1), async {
        loop {
            let error = service
                .validate(harness.token(0, "shared-key", 30)?)
                .await
                .unwrap_err();
            if error != Error::Busy {
                assert_eq!(error, Error::Expired);
                break;
            }
            tokio::task::yield_now().await;
        }
        Result::Ok(())
    })
    .await??;
    service.shutdown().await?;
    harness.authority.key_mode.store(0, Ordering::Release);
    let service = Service::start(harness.config(), harness.trust()).await?;
    let lease = service.validate(harness.token(0, "shared-key", 2)?).await?;
    assert_eq!(
        timeout(Duration::from_secs(3), lease.invalidated()).await?,
        Error::Expired
    );
    let lease = service
        .validate(harness.token(0, "shared-key", 30)?)
        .await?;
    harness
        .authority
        .introspection_mode
        .store(10, Ordering::Release);
    timeout(Duration::from_secs(2), harness.authority.entered.acquire())
        .await??
        .forget();
    timeout(Duration::from_secs(1), service.shutdown()).await??;
    assert_eq!(lease.failure(), Some(Error::Unavailable));
    harness.shutdown().await
}

#[tokio::test]
async fn unknown_kid_requests_are_single_flight_and_globally_bounded() -> Result {
    let harness = Harness::start().await?;
    let mut config = harness.config();
    config.runtime.refresh_interval = Duration::from_secs(1);
    let service = Service::start(config, harness.trust()).await?;
    let before = harness.authority.key_requests.load(Ordering::Acquire);
    let mut requests = JoinSet::new();
    for index in 0..8 {
        let token = harness.token(1, &format!("unknown-{index}"), 30)?;
        let other = service.clone();
        requests.spawn(async move { other.validate(token).await });
    }
    while let Some(request) = requests.join_next().await {
        assert!(matches!(
            request?.unwrap_err(),
            Error::Authentication | Error::Busy
        ));
    }
    assert_eq!(
        harness.authority.key_requests.load(Ordering::Acquire),
        before + 1
    );
    assert_eq!(harness.authority.checks.load(Ordering::Acquire), 0);
    service.shutdown().await?;
    harness.shutdown().await
}

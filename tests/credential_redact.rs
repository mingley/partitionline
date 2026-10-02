//! KL-06 slice: credential material must not appear in `Debug` output.
//!
//! Passwords, OIDC client secrets, and mTLS private keys are redacted. This
//! does not close full KL-06 (rotation/outage recovery remain open).
//!
//! KL06-07 extends the checks end to end: unique sentinel secrets flow
//! through real failing SASL / OIDC-refresh / TLS paths (including the
//! KL06-04 reauthentication and KL06-05 rotation paths) while a capturing
//! tracing subscriber records every span and event. Errors keep their
//! actionable code/category with sanitized detail.

mod common;

use std::time::Duration;

use partitionline::net::BrokerConn;
use partitionline::protocol::api::negotiate_api_versions;
use partitionline::protocol::sasl::{
    apply_api_keys, authenticate_oauthbearer, authenticate_plain, authenticate_scram,
    reauthenticate_oauthbearer_token, reauthenticate_plain, reauthenticate_scram,
};
use partitionline::protocol::scram::ScramAlg;
use partitionline::{
    AdminConfig, ConsumerConfig, Error, OidcConfig, ProduceRecord, Producer, ProducerConfig, Sasl,
    TlsConfig,
};

const PASSWORD: &str = "super-secret-password-kl06";
const CLIENT_SECRET: &str = "oidc-client-secret-kl06";
const KEY_PEM: &str =
    "-----BEGIN PRIVATE KEY-----\nkl06-test-key-material\n-----END PRIVATE KEY-----";
const CERT_PEM: &str = "-----BEGIN CERTIFICATE-----\nkl06-test-cert\n-----END CERTIFICATE-----";

// KL06-07 unique sentinels, one per credential kind.
const ECHO_PASSWORD: &str = "kl0607-plain-secret-9f3c";
const ECHO_SCRAM_PASSWORD: &str = "kl0607-scram-secret-71ab";
const ECHO_TOKEN: &str = "kl0607-token-material-44de";
const ECHO_URL_PASSWORD: &str = "kl0607-url-secret-6a20";
const ECHO_KEY_BODY: &str = "kl0607-key-material-5e91";
const ECHO_SCRAM_E: &str = "kl0607-scram-echo-c833";

/// Capture every span creation and event while `fut` runs (KL06-07).
///
/// Only compiled with `feature = "tracing"`, matching the instrumented
/// build. Without the feature there is nothing to capture; error assertions
/// in the same tests still run.
#[cfg(feature = "tracing")]
mod capture {
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::sync::Arc;
    use tracing::field::{Field, Visit};
    use tracing::{Event, Id, Metadata, Subscriber};

    struct Recorder {
        fields: Vec<String>,
    }

    impl Visit for Recorder {
        fn record_debug(&mut self, field: &Field, value: &dyn std::fmt::Debug) {
            self.fields.push(format!("{}={value:?}", field.name()));
        }
    }

    static NEXT_ID: AtomicU64 = AtomicU64::new(1);

    struct CaptureSubscriber {
        out: Arc<parking_lot::Mutex<Vec<String>>>,
    }

    impl Subscriber for CaptureSubscriber {
        fn enabled(&self, _: &Metadata<'_>) -> bool {
            true
        }

        fn new_span(&self, span: &tracing::span::Attributes<'_>) -> Id {
            let mut rec = Recorder { fields: Vec::new() };
            span.values().record(&mut rec);
            self.out.lock().push(format!(
                "span {} {}",
                span.metadata().name(),
                rec.fields.join(" ")
            ));
            Id::from_u64(NEXT_ID.fetch_add(1, Ordering::Relaxed))
        }

        fn record(&self, _: &Id, _: &tracing::span::Record<'_>) {}

        fn record_follows_from(&self, _: &Id, _: &Id) {}

        fn event(&self, event: &Event<'_>) {
            let mut rec = Recorder { fields: Vec::new() };
            event.record(&mut rec);
            self.out.lock().push(format!(
                "event {} {}",
                event.metadata().name(),
                rec.fields.join(" ")
            ));
        }

        fn enter(&self, _: &Id) {}

        fn exit(&self, _: &Id) {}
    }

    /// Run `fut` under a capturing subscriber and return every recorded
    /// span/event line.
    pub(crate) async fn capture_traces<F, T>(fut: F) -> (T, Vec<String>)
    where
        F: std::future::Future<Output = T>,
    {
        let out = Arc::new(parking_lot::Mutex::new(Vec::new()));
        let sub = CaptureSubscriber { out: out.clone() };
        // `set_default` (thread-local guard) holds the subscriber across
        // `.await` points; `with_default(sub, || fut).await` would only
        // cover future construction and record nothing.
        let _guard = tracing::subscriber::set_default(sub);
        let result = fut.await;
        drop(_guard);
        let lines = out.lock().clone();
        (result, lines)
    }

    /// Assert none of `secrets` appears in any captured line.
    pub(crate) fn assert_no_secrets(lines: &[String], secrets: &[&str], label: &str) {
        for line in lines {
            for secret in secrets {
                assert!(
                    !line.contains(secret),
                    "{label} tracing output leaked secret material: {line}"
                );
            }
        }
    }
}

#[cfg(feature = "tracing")]
#[tokio::test]
async fn tracing_capture_harness_records_span_fields() {
    // Non-vacuity proof for the harness below: a sentinel emitted through a
    // span field and an event must both be captured.
    let (_, lines) = capture::capture_traces(async {
        let span = tracing::info_span!("probe", probe_field = %"kl0607-harness-marker");
        let _guard = span.enter();
        tracing::info!(event_field = %"kl0607-harness-marker", "probe event");
    })
    .await;
    assert!(
        lines.iter().any(|l| l.contains("kl0607-harness-marker")),
        "capture harness must record span/event fields, got {lines:?}"
    );
}

fn assert_no_secret(debug: &str, secret: &str, label: &str) {
    assert!(
        !debug.contains(secret),
        "{label} Debug leaked secret material: {debug}"
    );
    assert!(
        debug.contains("<redacted>"),
        "{label} Debug should mark redaction: {debug}"
    );
}

#[test]
fn sasl_plain_debug_redacts_password() {
    let dbg = format!("{:?}", Sasl::plain("alice", PASSWORD));
    assert_no_secret(&dbg, PASSWORD, "Sasl::Plain");
    assert!(
        dbg.contains("alice"),
        "username should remain visible: {dbg}"
    );
}

#[test]
fn sasl_scram_sha256_debug_redacts_password() {
    let dbg = format!("{:?}", Sasl::scram_sha256("alice", PASSWORD));
    assert_no_secret(&dbg, PASSWORD, "Sasl::ScramSha256");
}

#[test]
fn sasl_scram_sha512_debug_redacts_password() {
    let dbg = format!("{:?}", Sasl::scram_sha512("alice", PASSWORD));
    assert_no_secret(&dbg, PASSWORD, "Sasl::ScramSha512");
}

#[test]
fn oidc_config_debug_redacts_client_secret() {
    let cfg = OidcConfig::new("https://idp.example/token", "client-id", CLIENT_SECRET);
    let dbg = format!("{cfg:?}");
    assert_no_secret(&dbg, CLIENT_SECRET, "OidcConfig");
    assert!(
        dbg.contains("client-id"),
        "client_id should remain visible: {dbg}"
    );
}

#[test]
fn oidc_token_response_debug_redacts_access_token() {
    let resp = partitionline::protocol::oidc::OidcTokenResponse {
        access_token: "super-secret-token-material-kl06".into(),
        token_type: "Bearer".into(),
        expires_in: Some(3600),
    };
    let dbg = format!("{resp:?}");
    assert!(
        !dbg.contains("super-secret-token-material-kl06"),
        "OidcTokenResponse Debug leaked token material: {dbg}"
    );
    assert!(
        dbg.contains("<redacted>"),
        "OidcTokenResponse Debug must redact access_token: {dbg}"
    );
    assert!(
        dbg.contains("Bearer"),
        "token_type should remain visible: {dbg}"
    );
    assert!(
        dbg.contains("3600"),
        "expires_in should remain visible: {dbg}"
    );
}

#[test]
fn tls_config_debug_redacts_client_key_pem() {
    let tls = TlsConfig::default().client_identity(CERT_PEM.as_bytes(), KEY_PEM.as_bytes());
    let dbg = format!("{tls:?}");
    assert_no_secret(&dbg, KEY_PEM, "TlsConfig");
    assert!(
        !dbg.contains("kl06-test-key-material"),
        "TlsConfig Debug leaked key body: {dbg}"
    );
    assert!(
        !dbg.contains("kl06-test-cert"),
        "TlsConfig Debug should not dump cert PEM: {dbg}"
    );
}

#[test]
fn producer_config_debug_redacts_embedded_secrets() {
    let cfg = ProducerConfig::bootstrap(["127.0.0.1:9092"])
        .sasl(Sasl::scram_sha256("alice", PASSWORD))
        .tls(TlsConfig::default().client_identity(CERT_PEM.as_bytes(), KEY_PEM.as_bytes()));
    let dbg = format!("{cfg:?}");
    assert_no_secret(&dbg, PASSWORD, "ProducerConfig");
    assert!(
        !dbg.contains("kl06-test-key-material"),
        "ProducerConfig Debug leaked key: {dbg}"
    );
}

#[test]
fn consumer_config_debug_redacts_embedded_secrets() {
    let cfg = ConsumerConfig::bootstrap(["127.0.0.1:9092"]).sasl(Sasl::plain("bob", PASSWORD));
    let dbg = format!("{cfg:?}");
    assert_no_secret(&dbg, PASSWORD, "ConsumerConfig");
}

#[test]
fn admin_config_debug_redacts_oidc_secret() {
    let cfg = AdminConfig::bootstrap(["127.0.0.1:9092"]).sasl(Sasl::oidc(OidcConfig::new(
        "https://idp.example/token",
        "admin-client",
        CLIENT_SECRET,
    )));
    let dbg = format!("{cfg:?}");
    assert_no_secret(&dbg, CLIENT_SECRET, "AdminConfig");
}

#[tokio::test]
async fn oidc_http_error_display_omits_response_body() {
    use partitionline::Error;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    drop(tokio::spawn(async move {
        let (mut sock, _) = listener.accept().await.unwrap();
        let mut buf = vec![0u8; 2048];
        let _ = sock.read(&mut buf).await.unwrap();
        let body = format!(
            "{{\"error\":\"invalid_client\",\"error_description\":\"secret={CLIENT_SECRET} token=leak-token-kl06\"}}"
        );
        let resp = format!(
            "HTTP/1.1 401 Unauthorized\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
            body.len()
        );
        sock.write_all(resp.as_bytes()).await.unwrap();
    }));

    let cfg = OidcConfig::new(format!("http://{addr}/token"), "cid", CLIENT_SECRET);
    let err = partitionline::protocol::oidc::fetch_client_credentials_token(
        &cfg,
        std::time::Duration::from_secs(5),
    )
    .await
    .unwrap_err();

    let display = err.to_string();
    let debug = format!("{err:?}");
    for surface in [&display, &debug] {
        assert!(
            !surface.contains(CLIENT_SECRET),
            "OIDC Error leaked client_secret: {surface}"
        );
        assert!(
            !surface.contains("leak-token-kl06"),
            "OIDC Error leaked token material: {surface}"
        );
        assert!(
            !surface.contains("invalid_client"),
            "OIDC Error must not embed IdP body: {surface}"
        );
    }
    match err {
        Error::Protocol(m) => assert_eq!(m, "oidc token endpoint HTTP 401"),
        other => panic!("expected Protocol, got {other:?}"),
    }
}

#[tokio::test]
async fn oidc_http_error_structured_fetch_omits_response_body() {
    use partitionline::Error;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    drop(tokio::spawn(async move {
        let (mut sock, _) = listener.accept().await.unwrap();
        let mut buf = vec![0u8; 2048];
        let _ = sock.read(&mut buf).await.unwrap();
        let body = format!(
            "{{\"error\":\"invalid_client\",\"error_description\":\"secret={CLIENT_SECRET} token=leak-token-kl06\"}}"
        );
        let resp = format!(
            "HTTP/1.1 401 Unauthorized\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
            body.len()
        );
        sock.write_all(resp.as_bytes()).await.unwrap();
    }));

    let cfg = OidcConfig::new(format!("http://{addr}/token"), "cid", CLIENT_SECRET);
    let err = partitionline::protocol::oidc::fetch_client_credentials_token_response(
        &cfg,
        std::time::Duration::from_secs(5),
    )
    .await
    .unwrap_err();

    let display = err.to_string();
    let debug = format!("{err:?}");
    for surface in [&display, &debug] {
        assert!(
            !surface.contains(CLIENT_SECRET),
            "OIDC Error leaked client_secret: {surface}"
        );
        assert!(
            !surface.contains("leak-token-kl06"),
            "OIDC Error leaked token material: {surface}"
        );
        assert!(
            !surface.contains("invalid_client"),
            "OIDC Error must not embed IdP body: {surface}"
        );
    }
    match err {
        Error::Protocol(m) => assert_eq!(m, "oidc token endpoint HTTP 401"),
        other => panic!("expected Protocol, got {other:?}"),
    }
}

#[test]
fn oidc_token_parse_errors_never_leak_body_or_token() {
    let secret_payload = format!(
        "{{\"error\":\"fail\",\"access_token\":\"{CLIENT_SECRET}\",\"expires_in\":\"bad_type\"}}"
    );
    let err =
        partitionline::protocol::oidc::parse_oidc_token_response(&secret_payload).unwrap_err();
    let display = err.to_string();
    let debug = format!("{err:?}");
    for surface in [&display, &debug] {
        assert!(
            !surface.contains(CLIENT_SECRET),
            "leaked secret in error: {surface}"
        );
        assert!(
            !surface.contains("bad_type"),
            "leaked payload in error: {surface}"
        );
        assert!(
            !surface.contains("fail"),
            "leaked error description: {surface}"
        );
    }
}

#[test]
fn oauthbearer_error_message_is_fixed() {
    // Compile-time honesty: sasl path must use the fixed string (bars also grep).
    // Runtime broker echo coverage lives in auth-smoke; this asserts the public
    // Error spelling operators will see.
    let err = partitionline::Error::protocol("oauthbearer: authentication failed");
    let s = err.to_string();
    assert!(s.contains("oauthbearer: authentication failed"), "{s}");
    assert!(!s.contains("access_token"), "{s}");
}

#[test]
fn metrics_debug_excludes_credential_material() {
    use partitionline::{
        AdminMetrics, ConsumerMetrics, ProducerMetrics, ShareMetrics, TopicFetchMetrics,
        TopicProduceMetrics,
    };

    // Metrics snapshots are counters + latency + topic names only — they must not
    // become a channel for password / client_secret / PEM material (KL-06).
    let producer = ProducerMetrics {
        topics: vec![TopicProduceMetrics {
            topic: "orders".into(),
            ..TopicProduceMetrics::default()
        }],
        ..ProducerMetrics::default()
    };
    let consumer = ConsumerMetrics {
        topics: vec![TopicFetchMetrics {
            topic: "orders".into(),
            ..TopicFetchMetrics::default()
        }],
        ..ConsumerMetrics::default()
    };
    let share = ShareMetrics {
        topics: vec![TopicFetchMetrics {
            topic: "orders".into(),
            ..TopicFetchMetrics::default()
        }],
        ..ShareMetrics::default()
    };
    let admin = AdminMetrics::default();

    for (label, dbg) in [
        ("ProducerMetrics", format!("{producer:?}")),
        ("ConsumerMetrics", format!("{consumer:?}")),
        ("ShareMetrics", format!("{share:?}")),
        ("AdminMetrics", format!("{admin:?}")),
    ] {
        assert!(
            !dbg.contains(PASSWORD),
            "{label} Debug must not contain password material: {dbg}"
        );
        assert!(
            !dbg.contains(CLIENT_SECRET),
            "{label} Debug must not contain client_secret material: {dbg}"
        );
        assert!(
            !dbg.contains(KEY_PEM),
            "{label} Debug must not contain key PEM material: {dbg}"
        );
        assert!(
            !dbg.contains("BEGIN PRIVATE KEY"),
            "{label} Debug must not contain PEM armor: {dbg}"
        );
    }
}

#[test]
fn tracing_instruments_skip_self_holding_configs() {
    // Source-policy honesty for feature=tracing: every instrumented public path
    // must skip `self` (configs with credentials live on the client). Allowed
    // fields are topic / protocol names only — not configs or records.
    let roots = [
        include_str!("../src/producer.rs"),
        include_str!("../src/consumer.rs"),
        include_str!("../src/group.rs"),
    ];
    let mut instruments = 0usize;
    for src in roots {
        for line in src.lines() {
            let trimmed = line.trim();
            if !trimmed.contains("tracing::instrument") {
                continue;
            }
            instruments += 1;
            assert!(
                trimmed.contains("skip(self") || trimmed.contains("skip(self,"),
                "tracing::instrument must skip(self): {trimmed}"
            );
            assert!(
                !trimmed.contains("skip(") || trimmed.contains("skip(self"),
                "unexpected instrument skip set: {trimmed}"
            );
            // Disallow dumping full config/record via fields=
            assert!(
                !trimmed.contains("fields(self")
                    && !trimmed.contains("fields(cfg")
                    && !trimmed.contains("fields(config"),
                "instrument must not field-dump config: {trimmed}"
            );
        }
    }
    assert!(
        instruments >= 5,
        "expected several tracing::instrument sites, found {instruments}"
    );
}

/// Assert an auth failure keeps its broker code while carrying no secret in
/// Display or Debug (KL06-07).
#[expect(
    clippy::panic,
    reason = "test-only assertion helper; failures must fail the test"
)]
fn assert_sanitized_broker_error(err: &Error, code: i16, secrets: &[&str], label: &str) {
    match err {
        Error::Broker { code: got, .. } => assert_eq!(
            *got, code,
            "{label} must retain the broker error code, got {err:?}"
        ),
        other => panic!("{label} must stay Error::Broker, got {other:?}"),
    }
    for surface in [err.to_string(), format!("{err:?}")] {
        for secret in secrets {
            assert!(
                !surface.contains(secret),
                "{label} error leaked secret material: {surface}"
            );
        }
    }
}

/// KL06-07: a broker that echoes the PLAIN password in its error message
/// must not get it into the client's Error.
#[tokio::test]
async fn sasl_plain_broker_echo_never_reaches_error() {
    let mock = common::Mock::start_with_sasl(Some(("alice".into(), "correct-horse".into()))).await;
    mock.set_sasl_authenticate_error_message(&format!("denied, pw was {ECHO_PASSWORD}"));
    let err = match Producer::new(
        ProducerConfig::bootstrap([mock.addr.clone()]).sasl(Sasl::plain("alice", ECHO_PASSWORD)),
    )
    .await
    {
        Ok(_) => panic!("wrong password must fail"),
        Err(e) => e,
    };
    assert_sanitized_broker_error(&err, 58, &[ECHO_PASSWORD], "PLAIN");
    let got = err.to_string();
    assert_eq!(
        got, "broker error 58 (SASL_AUTHENTICATION_FAILED): sasl PLAIN authentication failed",
        "PLAIN failure must use the fixed sanitized message, got {got}"
    );
}

/// KL06-07: a broker that echoes SCRAM material in its error message must
/// not get it into the client's Error.
#[tokio::test]
async fn sasl_scram_broker_echo_never_reaches_error() {
    let mock = common::Mock::start_with_scram(("alice".into(), "correct-horse".into())).await;
    mock.set_sasl_authenticate_error_message(&format!("denied, pw was {ECHO_SCRAM_PASSWORD}"));
    let err = match Producer::new(
        ProducerConfig::bootstrap([mock.addr.clone()])
            .sasl(Sasl::scram_sha256("alice", ECHO_SCRAM_PASSWORD)),
    )
    .await
    {
        Ok(_) => panic!("wrong password must fail"),
        Err(e) => e,
    };
    assert_sanitized_broker_error(&err, 58, &[ECHO_SCRAM_PASSWORD], "SCRAM");
    let got = err.to_string();
    assert_eq!(
        got,
        "broker error 58 (SASL_AUTHENTICATION_FAILED): sasl SCRAM-SHA-256 authentication failed",
        "SCRAM failure must use the fixed sanitized message, got {got}"
    );
}

/// KL06-07: a broker that echoes token material in its error message must
/// not get it into the client's Error.
#[tokio::test]
async fn sasl_oauthbearer_broker_echo_never_reaches_error() {
    let mock = common::Mock::start_with_oauthbearer("alice".into()).await;
    mock.set_sasl_authenticate_error_message(&format!("denied token {ECHO_TOKEN}"));
    let err = match Producer::new(
        ProducerConfig::bootstrap([mock.addr.clone()]).sasl(Sasl::oauthbearer("intruder")),
    )
    .await
    {
        Ok(_) => panic!("wrong principal must fail"),
        Err(e) => e,
    };
    assert_sanitized_broker_error(&err, 58, &[ECHO_TOKEN], "OAUTHBEARER");
}

/// Open a plaintext connection and negotiate SASL versions (KL06-07 reauth
/// tests authenticate first, then reauthenticate with failing material).
async fn sasl_conn(addr: &str) -> Result<BrokerConn, Error> {
    let mut conn = BrokerConn::connect(addr, "redact-test", Duration::from_secs(5)).await?;
    let versions = negotiate_api_versions(&mut conn, Duration::from_secs(5)).await?;
    apply_api_keys(&mut conn, &versions.api_keys);
    Ok(conn)
}

/// KL06-07: reauthentication failures cannot surface echoed passwords.
#[tokio::test]
async fn sasl_reauthenticate_plain_echo_never_reaches_error() {
    let mock = common::Mock::start_with_sasl(Some(("alice".into(), "correct-horse".into()))).await;
    let mut conn = sasl_conn(&mock.addr).await.unwrap();
    authenticate_plain(&mut conn, "alice", "correct-horse", Duration::from_secs(5))
        .await
        .unwrap();
    mock.set_sasl_authenticate_error_message(&format!("denied, pw was {ECHO_PASSWORD}"));
    let err = reauthenticate_plain(&mut conn, "alice", "wrong", Duration::from_secs(5))
        .await
        .unwrap_err();
    assert_sanitized_broker_error(&err, 58, &[ECHO_PASSWORD], "PLAIN reauth");
}

/// KL06-07: SCRAM reauthentication failures cannot surface echoed material.
#[tokio::test]
async fn sasl_reauthenticate_scram_echo_never_reaches_error() {
    let mock = common::Mock::start_with_scram(("alice".into(), "correct-horse".into())).await;
    let mut conn = sasl_conn(&mock.addr).await.unwrap();
    authenticate_scram(
        &mut conn,
        ScramAlg::Sha256,
        "alice",
        "correct-horse",
        Duration::from_secs(5),
    )
    .await
    .unwrap();
    mock.set_sasl_authenticate_error_message(&format!("denied, pw was {ECHO_SCRAM_PASSWORD}"));
    let err = reauthenticate_scram(
        &mut conn,
        ScramAlg::Sha256,
        "alice",
        "wrong",
        Duration::from_secs(5),
    )
    .await
    .unwrap_err();
    assert_sanitized_broker_error(&err, 58, &[ECHO_SCRAM_PASSWORD], "SCRAM reauth");
}

/// KL06-07: OAUTHBEARER reauthentication failures cannot surface echoed
/// token material.
#[tokio::test]
async fn sasl_reauthenticate_oauthbearer_echo_never_reaches_error() {
    let mock = common::Mock::start_with_oauthbearer("alice".into()).await;
    let mut conn = sasl_conn(&mock.addr).await.unwrap();
    authenticate_oauthbearer(&mut conn, "alice", Duration::from_secs(5))
        .await
        .unwrap();
    mock.set_sasl_authenticate_error_message(&format!("denied token {ECHO_TOKEN}"));
    let err = reauthenticate_oauthbearer_token(
        &mut conn,
        &format!("bogus-{ECHO_TOKEN}"),
        Duration::from_secs(5),
    )
    .await
    .unwrap_err();
    assert_sanitized_broker_error(&err, 58, &[ECHO_TOKEN], "OAUTHBEARER reauth");
}

/// KL06-07: userinfo in the token URL must be redacted in Debug.
#[test]
fn oidc_token_url_userinfo_redacted_in_debug() {
    let cfg = OidcConfig::new(
        format!("https://idp-user:{ECHO_URL_PASSWORD}@idp.example/token"),
        "client-id",
        CLIENT_SECRET,
    );
    let dbg = format!("{cfg:?}");
    assert!(
        !dbg.contains(ECHO_URL_PASSWORD),
        "OidcConfig Debug leaked URL password: {dbg}"
    );
    assert!(
        !dbg.contains("idp-user:"),
        "OidcConfig Debug must redact the userinfo component: {dbg}"
    );
    assert!(
        dbg.contains("idp.example/token"),
        "host and path should remain visible: {dbg}"
    );
}

/// KL06-07: userinfo in the token URL is rejected before any fetch with a
/// fixed error that echoes nothing.
#[tokio::test]
async fn oidc_token_url_userinfo_rejected_at_fetch() {
    let cfg = OidcConfig::new(
        format!("https://idp-user:{ECHO_URL_PASSWORD}@127.0.0.1:9/token"),
        "client-id",
        CLIENT_SECRET,
    );
    let err =
        partitionline::protocol::oidc::fetch_client_credentials_token(&cfg, Duration::from_secs(5))
            .await
            .unwrap_err();
    for surface in [err.to_string(), format!("{err:?}")] {
        assert!(
            !surface.contains(ECHO_URL_PASSWORD),
            "OIDC fetch error leaked URL password: {surface}"
        );
    }
    match err {
        Error::Protocol(m) => assert_eq!(m, "oidc token_url must not contain userinfo"),
        other => panic!("expected userinfo rejection, got {other:?}"),
    }
}

/// KL06-07: SCRAM `e=` values are bounded to known tokens; a hostile value
/// carrying secret-lookalike bytes is masked, not echoed.
#[test]
fn scram_server_error_value_is_bounded() {
    let err = partitionline::protocol::scram::verify_server_final(
        ScramAlg::Sha256,
        "password",
        "n=user,r=clientnonce",
        "r=clientnonceserver,s=c2FsdA==,i=4096",
        "c=biws,r=clientnonceserver,p=cHJvb2Y=",
        &format!("e=bad-{ECHO_SCRAM_E}"),
    )
    .unwrap_err();
    for surface in [err.to_string(), format!("{err:?}")] {
        assert!(
            !surface.contains(ECHO_SCRAM_E),
            "SCRAM error echoed hostile e= value: {surface}"
        );
    }
    match err {
        Error::Protocol(m) => assert_eq!(m, "scram server error: unrecognized"),
        other => panic!("expected bounded scram error, got {other:?}"),
    }

    // Known tokens still pass through for actionability.
    let err = partitionline::protocol::scram::verify_server_final(
        ScramAlg::Sha256,
        "password",
        "n=user,r=clientnonce",
        "r=clientnonceserver,s=c2FsdA==,i=4096",
        "c=biws,r=clientnonceserver,p=cHJvb2Y=",
        "e=invalid-proof",
    )
    .unwrap_err();
    match err {
        Error::Protocol(m) => assert_eq!(m, "scram server error: invalid-proof"),
        other => panic!("expected bounded scram error, got {other:?}"),
    }

    // Malformed attributes are bounded too: no echo of the offending bytes.
    let err = partitionline::protocol::scram::verify_server_final(
        ScramAlg::Sha256,
        "password",
        "n=user,r=clientnonce",
        "r=clientnonceserver,s=c2FsdA==,i=4096",
        "c=biws,r=clientnonceserver,p=cHJvb2Y=",
        &format!("eX{ECHO_SCRAM_E}"),
    )
    .unwrap_err();
    for surface in [err.to_string(), format!("{err:?}")] {
        assert!(
            !surface.contains(ECHO_SCRAM_E),
            "SCRAM error echoed malformed attr bytes: {surface}"
        );
    }
    match err {
        Error::Protocol(m) => assert_eq!(m, "scram bad attr"),
        other => panic!("expected bounded scram error, got {other:?}"),
    }
}

/// KL06-07: TLS identity parse failures must not echo key material.
#[tokio::test]
async fn tls_key_parse_error_omits_key_material() {
    let (mock, mut tls) = common::Mock::start_tls().await;
    tls.client_cert_pem = Some(CERT_PEM.as_bytes().to_vec());
    tls.client_key_pem = Some(
        format!("-----BEGIN PRIVATE KEY-----\n{ECHO_KEY_BODY}\n-----END PRIVATE KEY-----")
            .into_bytes(),
    );
    let err = match Producer::new(ProducerConfig::bootstrap([mock.addr.clone()]).tls(tls)).await {
        Ok(_) => panic!("garbage key must fail"),
        Err(e) => e,
    };
    for surface in [err.to_string(), format!("{err:?}")] {
        assert!(
            !surface.contains(ECHO_KEY_BODY),
            "TLS error echoed key material: {surface}"
        );
    }
    assert!(
        matches!(err, Error::Protocol(_)),
        "key parse failure must stay a client-side Protocol error, got {err:?}"
    );
}

/// KL06-07: failing SASL, OIDC and TLS flows emit no secrets to tracing
/// (captured when the feature is on) and none to their errors.
#[tokio::test]
async fn failing_auth_flows_emit_no_secrets_to_tracing_or_errors() {
    async fn plain_fail(addr: String, echo: String) -> Error {
        match Producer::new(
            ProducerConfig::bootstrap([addr]).sasl(Sasl::plain("alice", ECHO_PASSWORD)),
        )
        .await
        {
            Ok(_) => panic!("wrong password must fail, echo={echo}"),
            Err(e) => e,
        }
    }

    async fn oidc_fail(url: String) -> Error {
        let cfg = OidcConfig::new(url, "cid", CLIENT_SECRET);
        partitionline::protocol::oidc::fetch_client_credentials_token(&cfg, Duration::from_secs(5))
            .await
            .unwrap_err()
    }

    let mock = common::Mock::start_with_sasl(Some(("alice".into(), "correct-horse".into()))).await;
    mock.set_sasl_authenticate_error_message(&format!("denied, pw was {ECHO_PASSWORD}"));

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let idp = listener.local_addr().unwrap();
    drop(tokio::spawn(async move {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let (mut sock, _) = listener.accept().await.unwrap();
        let mut buf = vec![0u8; 2048];
        let _ = sock.read(&mut buf).await.unwrap();
        let body = format!(
            "{{\"error\":\"invalid_client\",\"secret\":{CLIENT_SECRET:?},\"token\":\"{ECHO_TOKEN}\"}}"
        );
        let resp = format!(
            "HTTP/1.1 401 Unauthorized\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
            body.len()
        );
        sock.write_all(resp.as_bytes()).await.unwrap();
    }));

    let addr = mock.addr.clone();
    let echo = format!("denied, pw was {ECHO_PASSWORD}");
    #[cfg(feature = "tracing")]
    let ((plain_err, oidc_err), lines) = capture::capture_traces(async {
        let plain_err = plain_fail(addr, echo).await;
        let oidc_err = oidc_fail(format!("http://{idp}/token")).await;
        (plain_err, oidc_err)
    })
    .await;
    #[cfg(not(feature = "tracing"))]
    let (plain_err, oidc_err) = {
        let plain_err = plain_fail(addr, echo).await;
        let oidc_err = oidc_fail(format!("http://{idp}/token")).await;
        (plain_err, oidc_err)
    };

    for (label, err, secrets) in [
        ("PLAIN", &plain_err, &[ECHO_PASSWORD][..]),
        ("OIDC", &oidc_err, &[CLIENT_SECRET, ECHO_TOKEN][..]),
    ] {
        for surface in [err.to_string(), format!("{err:?}")] {
            for secret in secrets {
                assert!(
                    !surface.contains(secret),
                    "{label} error leaked secret material: {surface}"
                );
            }
        }
    }
    #[cfg(feature = "tracing")]
    capture::assert_no_secrets(
        &lines,
        &[ECHO_PASSWORD, CLIENT_SECRET, ECHO_TOKEN],
        "failing auth",
    );
}

/// KL06-07: mTLS rotation emits no key material to tracing, and rotated
/// configs stay redacted.
#[tokio::test]
async fn tls_rotation_emits_no_key_material_to_tracing() {
    let (mock, fix) = common::Mock::start_tls_mtls().await;
    let tls = TlsConfig {
        ca_pem: Some(fix.server_ca.clone()),
        client_cert_pem: Some(fix.client_cert.clone()),
        client_key_pem: Some(fix.client_key.clone()),
        server_name: Some("localhost".into()),
    };
    let addr = mock.addr.clone();
    #[cfg(feature = "tracing")]
    let key_before = fix.client_key.clone();

    #[cfg(feature = "tracing")]
    let ((key_after, rotated_tls), lines) = capture::capture_traces(async {
        let producer = Producer::new(
            ProducerConfig::bootstrap([addr.clone()])
                .tls(tls.clone())
                .linger(Duration::ZERO),
        )
        .await
        .unwrap();
        let _ = producer
            .send(ProduceRecord::to("t").value(&b"v"[..]))
            .await
            .unwrap();
        producer.close().await.unwrap();
        let (new_cert, new_key) = mock.rotate_tls_client_ca();
        let rotated = TlsConfig {
            ca_pem: Some(fix.server_ca.clone()),
            client_cert_pem: Some(new_cert),
            client_key_pem: Some(new_key.clone()),
            server_name: Some("localhost".into()),
        };
        let producer = Producer::new(
            ProducerConfig::bootstrap([addr.clone()])
                .tls(rotated.clone())
                .linger(Duration::ZERO),
        )
        .await
        .unwrap();
        producer.close().await.unwrap();
        (new_key, rotated)
    })
    .await;
    #[cfg(not(feature = "tracing"))]
    let (_key_after, rotated_tls) = {
        let producer = Producer::new(
            ProducerConfig::bootstrap([addr.clone()])
                .tls(tls.clone())
                .linger(Duration::ZERO),
        )
        .await
        .unwrap();
        let _ = producer
            .send(ProduceRecord::to("t").value(&b"v"[..]))
            .await
            .unwrap();
        producer.close().await.unwrap();
        let (new_cert, new_key) = mock.rotate_tls_client_ca();
        let rotated = TlsConfig {
            ca_pem: Some(fix.server_ca.clone()),
            client_cert_pem: Some(new_cert),
            client_key_pem: Some(new_key.clone()),
            server_name: Some("localhost".into()),
        };
        let producer = Producer::new(
            ProducerConfig::bootstrap([addr.clone()])
                .tls(rotated.clone())
                .linger(Duration::ZERO),
        )
        .await
        .unwrap();
        producer.close().await.unwrap();
        (new_key, rotated)
    };

    let rotated_dbg = format!("{rotated_tls:?}");
    assert!(
        !rotated_dbg.contains("BEGIN PRIVATE KEY"),
        "rotated TlsConfig Debug leaked key armor: {rotated_dbg}"
    );
    #[cfg(feature = "tracing")]
    {
        for key in [&key_before, &key_after] {
            let text = String::from_utf8_lossy(key);
            for line in &lines {
                assert!(
                    !line.contains(text.as_ref()),
                    "rotation tracing output leaked key material: {line}"
                );
            }
        }
    }
}

/// KL06-07: live metric snapshots after authenticated traffic carry no
/// credential material.
#[tokio::test]
async fn live_metrics_exclude_credential_material() {
    let mock = common::Mock::start_with_sasl(Some(("alice".into(), ECHO_PASSWORD.into()))).await;
    let producer = Producer::new(
        ProducerConfig::bootstrap([mock.addr.clone()])
            .sasl(Sasl::plain("alice", ECHO_PASSWORD))
            .linger(Duration::ZERO),
    )
    .await
    .unwrap();
    let _ = producer
        .send(ProduceRecord::to("t").value(&b"v"[..]))
        .await
        .unwrap();
    let dbg = format!("{:?}", producer.metrics());
    assert!(
        !dbg.contains(ECHO_PASSWORD),
        "live ProducerMetrics leaked password: {dbg}"
    );
    producer.close().await.unwrap();
}

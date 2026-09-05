//! KL-06 slice: credential material must not appear in `Debug` output.
//!
//! Passwords, OIDC client secrets, and mTLS private keys are redacted. This
//! does not close full KL-06 (rotation/outage recovery remain open).

use partitionline::{AdminConfig, ConsumerConfig, OidcConfig, ProducerConfig, Sasl, TlsConfig};

const PASSWORD: &str = "super-secret-password-kl06";
const CLIENT_SECRET: &str = "oidc-client-secret-kl06";
const KEY_PEM: &str =
    "-----BEGIN PRIVATE KEY-----\nkl06-test-key-material\n-----END PRIVATE KEY-----";
const CERT_PEM: &str = "-----BEGIN CERTIFICATE-----\nkl06-test-cert\n-----END CERTIFICATE-----";

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

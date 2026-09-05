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

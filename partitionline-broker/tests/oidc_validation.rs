//! Public bounded signed-JWT foundation policy and lifecycle checks.
//! Independent signed fixtures are verified with a controlled epoch by the
//! private JWT unit gate; these public checks do not claim independent crypto.
#![cfg(feature = "oidc")]

use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine as _};
use partitionline_broker::security::{
    oidc::{AccessToken, Algorithm, Error, Limits, PinnedVerifier, Policy},
    sasl::Secret,
};
use ring::{
    rand::SystemRandom,
    signature::{self, KeyPair as _},
};
use serde_json::json;
use std::time::Duration;

fn policy() -> Policy {
    Policy {
        issuer: "https://issuer.example/realm".to_owned(),
        audiences: vec!["partitionline".to_owned()],
        algorithms: vec![Algorithm::Es256],
        access_token: AccessToken::AtJwt,
        limits: Limits::default(),
    }
}
fn jwks() -> Result<Vec<u8>, Box<dyn std::error::Error>> {
    let rng = SystemRandom::new();
    let der =
        signature::EcdsaKeyPair::generate_pkcs8(&signature::ECDSA_P256_SHA256_FIXED_SIGNING, &rng)
            .map_err(|_| std::io::Error::other("synthetic signing-key generation"))?;
    let key = signature::EcdsaKeyPair::from_pkcs8(
        &signature::ECDSA_P256_SHA256_FIXED_SIGNING,
        der.as_ref(),
        &rng,
    )
    .map_err(|_| std::io::Error::other("synthetic signing-key decoding"))?;
    let public = key.public_key().as_ref();
    Ok(serde_json::to_vec(
        &json!({"keys":[{"kid":"test-key","kty":"EC","crv":"P-256","x":URL_SAFE_NO_PAD.encode(&public[1..33]),"y":URL_SAFE_NO_PAD.encode(&public[33..])}]}),
    )?)
}

#[test]
fn configured_issuer_and_access_discriminator_are_required() {
    let jwks = jwks().unwrap();
    for issuer in [
        "http://issuer.example",
        "https://user@issuer.example",
        "https://issuer.example?override=1",
        "https://issuer.example/#fragment",
        "https://issuer.example:bad",
        "https://issuer.example:65536",
        "https://issuer.example:0",
    ] {
        let mut p = policy();
        p.issuer = issuer.to_owned();
        assert!(matches!(
            PinnedVerifier::new(p, &jwks, Duration::from_secs(60)),
            Err(Error::InvalidConfiguration)
        ));
    }
    let mut p = policy();
    p.access_token = AccessToken::Claim {
        name: "sub".to_owned(),
        value: "access".to_owned(),
    };
    assert!(matches!(
        PinnedVerifier::new(p, &jwks, Duration::from_secs(60)),
        Err(Error::InvalidConfiguration)
    ));
}
#[test]
fn static_authority_never_has_an_infinite_freshness_lease() {
    let jwks = jwks().unwrap();
    for valid_for in [Duration::ZERO, Duration::from_secs(301)] {
        assert!(matches!(
            PinnedVerifier::new(policy(), &jwks, valid_for),
            Err(Error::InvalidConfiguration)
        ));
    }
}
#[tokio::test]
async fn monotonic_authority_expiry_is_enforced() {
    let verifier =
        PinnedVerifier::new(policy(), &jwks().unwrap(), Duration::from_millis(1)).unwrap();
    tokio::time::sleep(Duration::from_millis(3)).await;
    assert!(matches!(
        verifier
            .verify(Secret::new(b"never-a-token".to_vec()))
            .await,
        Err(Error::Expired)
    ));
    verifier.shutdown().await.unwrap();
}
#[tokio::test]
async fn shutdown_is_shared_and_retryable() {
    let verifier =
        PinnedVerifier::new(policy(), &jwks().unwrap(), Duration::from_secs(60)).unwrap();
    let captured = verifier.clone();
    verifier.shutdown().await.unwrap();
    assert!(matches!(
        captured
            .verify(Secret::new(b"never-a-token".to_vec()))
            .await,
        Err(Error::Unavailable)
    ));
    captured.shutdown().await.unwrap();
}
#[tokio::test]
async fn malformed_and_oversized_bearers_fail_without_echo() {
    let verifier =
        PinnedVerifier::new(policy(), &jwks().unwrap(), Duration::from_secs(60)).unwrap();
    for input in [Vec::new(), b"a.b.c.d".to_vec(), vec![b'x'; 8193]] {
        let result = verifier.verify(Secret::new(input)).await;
        assert!(result.is_err());
        assert!(!format!("{result:?}").contains("a.b.c.d"));
    }
    verifier.shutdown().await.unwrap();
}
#[test]
fn public_diagnostics_redact_tokens_and_authorities() {
    let p = policy();
    let verifier =
        PinnedVerifier::new(p.clone(), &jwks().unwrap(), Duration::from_secs(60)).unwrap();
    assert!(!format!("{p:?}").contains("issuer.example"));
    assert!(!format!("{verifier:?}").contains("issuer.example"));
    let secret = Secret::new(b"public-synthetic-bearer".to_vec());
    assert_eq!(format!("{secret:?}"), "Secret { [REDACTED] }");
    assert!(!Error::Authentication.to_string().contains("bearer"));
}

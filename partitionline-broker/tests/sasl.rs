//! Bounded optional mechanism state; transport integration is a separate contract.
#![cfg(feature = "sasl")]

use base64::{engine::general_purpose::STANDARD, Engine};
use hmac::{Hmac, KeyInit, Mac};
use partitionline_broker::security::sasl::{Algorithm, Credential, Error, Limits, Secret, Service};
use pbkdf2::pbkdf2_hmac;
use sha2::{Digest, Sha256, Sha512};
use std::collections::HashSet;

type Result<T = ()> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;
fn secret(value: &str) -> Secret {
    Secret::new(value.as_bytes().to_vec())
}
async fn service(algorithm: Algorithm, name: &str, password: &str) -> Result<Service> {
    let service = Service::new(Limits::default())?;
    let credential = service.derive(algorithm, secret(password), 4096).await?;
    service.replace(vec![(name.to_owned(), credential)])?;
    Ok(service)
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
fn hash(algorithm: Algorithm, bytes: &[u8]) -> Vec<u8> {
    match algorithm {
        Algorithm::Sha256 => Sha256::digest(bytes).to_vec(),
        Algorithm::Sha512 => Sha512::digest(bytes).to_vec(),
    }
}
// A small client-side transcript builder, independent of the production helpers.
// Apache/RFC fixed proofs are additionally checked by the private fixture suite.
fn final_message(
    algorithm: Algorithm,
    password: &str,
    first: &str,
    server: &[u8],
) -> Result<(Vec<u8>, Vec<u8>)> {
    final_with_extension(algorithm, password, first, server, "")
}

fn final_with_extension(
    algorithm: Algorithm,
    password: &str,
    first: &str,
    server: &[u8],
    extension: &str,
) -> Result<(Vec<u8>, Vec<u8>)> {
    let server = std::str::from_utf8(server)?;
    let get = |key: &str| {
        server
            .split(',')
            .find_map(|p| p.strip_prefix(key))
            .ok_or("missing attribute")
    };
    let nonce = get("r=")?;
    let salt = STANDARD.decode(get("s=")?)?;
    let iterations = get("i=")?.parse()?;
    let bare = first.splitn(3, ',').nth(2).ok_or("missing bare message")?;
    let header = &first[..first.len() - bare.len()];
    let mut without = format!("c={},r={nonce}", STANDARD.encode(header));
    if !extension.is_empty() {
        without.push(',');
        without.push_str(extension);
    }
    let auth = format!("{bare},{server},{without}");
    let mut salted = vec![
        0;
        match algorithm {
            Algorithm::Sha256 => 32,
            Algorithm::Sha512 => 64,
        }
    ];
    match algorithm {
        Algorithm::Sha256 => {
            pbkdf2_hmac::<Sha256>(password.as_bytes(), &salt, iterations, &mut salted)
        }
        Algorithm::Sha512 => {
            pbkdf2_hmac::<Sha512>(password.as_bytes(), &salt, iterations, &mut salted)
        }
    }
    let client = mac(algorithm, &salted, b"Client Key")?;
    let signature = mac(algorithm, &hash(algorithm, &client), auth.as_bytes())?;
    let proof: Vec<_> = client.iter().zip(signature).map(|(a, b)| a ^ b).collect();
    let server_key = mac(algorithm, &salted, b"Server Key")?;
    let expected = format!(
        "v={}",
        STANDARD.encode(mac(algorithm, &server_key, auth.as_bytes())?)
    )
    .into_bytes();
    Ok((
        format!("{without},p={}", STANDARD.encode(proof)).into_bytes(),
        expected,
    ))
}

#[tokio::test]
async fn optional_extensions_are_ignored_but_bound_to_the_exact_proof() -> Result {
    for algorithm in [Algorithm::Sha256, Algorithm::Sha512] {
        let service = service(algorithm, "user", "pencil").await?;
        let first = "n,,n=user,r=client-random,x=optional";
        let mut session = service.scram(algorithm)?;
        let challenge = session.challenge(first.as_bytes())?;
        let (proof, expected) =
            final_with_extension(algorithm, "pencil", first, &challenge, "x=optional")?;
        let result = session.finish(Secret::new(proof)).await?;
        assert_eq!(result.message, expected);
        assert_eq!(result.identity.name(), "user");
        let mut session = service.scram(algorithm)?;
        let challenge = session.challenge(first.as_bytes())?;
        let (proof, _) =
            final_with_extension(algorithm, "pencil", first, &challenge, "x=optional")?;
        let proof = String::from_utf8(proof)?.replace("x=optional", "x=changed");
        assert!(matches!(
            session.finish(Secret::new(proof.into_bytes())).await,
            Err(Error::AuthenticationFailed)
        ));
    }
    Ok(())
}

#[tokio::test]
async fn kafka_raw_utf8_passwords_and_identities_are_preserved() -> Result {
    for algorithm in [Algorithm::Sha256, Algorithm::Sha512] {
        let service = service(algorithm, "user", "päss💫").await?;
        let first = "n,,n=user,r=client-random";
        let mut session = service.scram(algorithm)?;
        let challenge = session.challenge(first.as_bytes())?;
        let (proof, expected) = final_message(algorithm, "päss💫", first, &challenge)?;
        let result = session.finish(Secret::new(proof)).await?;
        assert_eq!(result.message, expected);
        assert_eq!(result.identity.name(), "user");
        let credential = service.derive(algorithm, secret("päss💫"), 4096).await?;
        service.replace(vec![("用户".into(), credential)])?;
        let mut session = service.scram(algorithm)?;
        assert!(matches!(
            session.challenge("n,,n=用户,r=client-random".as_bytes()),
            Err(Error::InvalidMessage)
        ));
        drop(session);
        assert_eq!(
            service
                .plain(Secret::new("\0用户\0päss💫".as_bytes().to_vec()))
                .await?
                .name(),
            "用户"
        );
        assert!(matches!(
            service
                .plain(Secret::new("\0用户\0pass💫".as_bytes().to_vec()))
                .await,
            Err(Error::AuthenticationFailed)
        ));
    }
    Ok(())
}

#[tokio::test]
async fn valid_scram_both_hashes_and_plain_from_verifiers() -> Result {
    for algorithm in [Algorithm::Sha256, Algorithm::Sha512] {
        let service = service(algorithm, "user", "pencil").await?;
        let first = "n,,n=user,r=client-random";
        let mut session = service.scram(algorithm)?;
        let challenge = session.challenge(first.as_bytes())?;
        let (final_message, expected) = final_message(algorithm, "pencil", first, &challenge)?;
        let result = session.finish(Secret::new(final_message)).await?;
        assert_eq!(result.message, expected);
        assert_eq!(result.identity.name(), "user");
        assert_eq!(result.identity.generation(), 1);
        let identity = service
            .plain(Secret::new(b"\0user\0pencil".to_vec()))
            .await?;
        assert_eq!(identity.name(), "user");
        assert_eq!(service.work_counts().admissions, 0);
        assert_eq!(service.work_counts().workers, 0);
    }
    Ok(())
}

#[tokio::test]
async fn plain_wrong_password_unknown_identity_authz_and_malformed() -> Result {
    let service = service(Algorithm::Sha256, "user", "pencil").await?;
    for input in [
        b"\0user\0wrong".as_slice(),
        b"\0unknown\0pencil",
        b"other\0user\0pencil",
    ] {
        assert!(matches!(
            service.plain(Secret::new(input.to_vec())).await,
            Err(Error::AuthenticationFailed)
        ));
    }
    for input in [
        b"user\0pencil".as_slice(),
        b"\0user\0pencil\0extra",
        b"\0\0pencil",
        b"\0user\0",
        b"\0\xff\0pencil",
        b"\0user\0\xff",
    ] {
        assert!(matches!(
            service.plain(Secret::new(input.to_vec())).await,
            Err(Error::InvalidMessage)
        ));
    }
    assert_eq!(
        service
            .plain(Secret::new(b"user\0user\0pencil".to_vec()))
            .await?
            .name(),
        "user"
    );
    Ok(())
}

#[tokio::test]
async fn scram_escaped_name_self_authz_and_unrelated_authz() -> Result {
    let algorithm = Algorithm::Sha512;
    let service = service(algorithm, "escape,user=ok", "pencil").await?;
    let first = "n,a=escape=2Cuser=3Dok,n=escape=2Cuser=3Dok,r=client-nonce";
    let mut session = service.scram(algorithm)?;
    let challenge = session.challenge(first.as_bytes())?;
    let (final_message, _) = final_message(algorithm, "pencil", first, &challenge)?;
    assert_eq!(
        session
            .finish(Secret::new(final_message))
            .await?
            .identity
            .name(),
        "escape,user=ok"
    );
    let mut session = service.scram(algorithm)?;
    assert!(matches!(
        session.challenge(b"n,a=other,n=escape=2Cuser=3Dok,r=nonce"),
        Err(Error::AuthenticationFailed)
    ));
    Ok(())
}

#[tokio::test]
async fn scram_wrong_password_nonce_channel_binding_proof_and_replay() -> Result {
    let algorithm = Algorithm::Sha256;
    let service = service(algorithm, "user", "pencil").await?;
    let first = "n,,n=user,r=client-nonce";
    let mut session = service.scram(algorithm)?;
    let challenge = session.challenge(first.as_bytes())?;
    let (proof, _) = final_message(algorithm, "wrong", first, &challenge)?;
    assert!(matches!(
        session.finish(Secret::new(proof)).await,
        Err(Error::AuthenticationFailed)
    ));
    for alteration in [
        "nonce",
        "binding",
        "proof",
        "duplicate",
        "short",
        "notbase64",
    ] {
        let mut session = service.scram(algorithm)?;
        let challenge = session.challenge(first.as_bytes())?;
        let (proof, _) = final_message(algorithm, "pencil", first, &challenge)?;
        let mut proof = String::from_utf8(proof)?;
        match alteration {
            "nonce" => proof = proof.replace("r=client-nonce", "r=wrong-nonce"),
            "binding" => proof = proof.replace("c=biws", "c=eSws"),
            "proof" => {
                let (prefix, _) = proof.rsplit_once(",p=").ok_or("no proof")?;
                proof = format!("{prefix},p={}", STANDARD.encode([0; 32]));
            }
            "duplicate" => proof.push_str(",r=other"),
            "short" => {
                let (prefix, _) = proof.rsplit_once(",p=").ok_or("no proof")?;
                proof = format!("{prefix},p=AA==");
            }
            _ => {
                let (prefix, _) = proof.rsplit_once(",p=").ok_or("no proof")?;
                proof = format!("{prefix},p=???");
            }
        }
        assert!(session
            .finish(Secret::new(proof.into_bytes()))
            .await
            .is_err());
    }
    let mut session = service.scram(algorithm)?;
    let challenge = session.challenge(first.as_bytes())?;
    let (proof, _) = final_message(algorithm, "pencil", first, &challenge)?;
    session.finish(Secret::new(proof.clone())).await?;
    let mut replay = service.scram(algorithm)?;
    replay.challenge(first.as_bytes())?;
    assert!(matches!(
        replay.finish(Secret::new(proof)).await,
        Err(Error::AuthenticationFailed)
    ));
    Ok(())
}

#[tokio::test]
async fn malformed_first_is_terminal_and_limits_are_checked() -> Result {
    let service = service(Algorithm::Sha256, "user", "pencil").await?;
    for input in [
        "",
        "n,,n=user",
        "n,,n=user,r=",
        "n,,r=nonce,n=user",
        "n,,n=user,r=x,r=y",
        "n,,m=required,n=user,r=x",
        "y,,n=user,r=x",
        "p=tls-unique,,n=user,r=x",
        "n,,n=bad=2c,r=x",
        "n,,n=user,r=x\0",
    ] {
        let mut session = service.scram(Algorithm::Sha256)?;
        assert!(session.challenge(input.as_bytes()).is_err(), "{input}");
        assert!(session.challenge(b"n,,n=user,r=nonce").is_err());
        assert!(session
            .finish(secret("c=biws,r=nonce,p=AA=="))
            .await
            .is_err());
    }
    let mut session = service.scram(Algorithm::Sha256)?;
    assert!(session.challenge(&vec![b'a'; 8193]).is_err());
    let mut session = service.scram(Algorithm::Sha256)?;
    assert!(session
        .challenge(format!("n,,n=user,r={}", "x".repeat(214)).as_bytes())
        .is_err());
    assert!(service
        .derive(Algorithm::Sha256, secret("pencil"), 4095)
        .await
        .is_err());
    assert!(service
        .derive(Algorithm::Sha256, secret("pencil"), 16385)
        .await
        .is_err());
    assert!(service
        .derive(Algorithm::Sha256, Secret::new(vec![b'x'; 4097]), 4096)
        .await
        .is_err());
    assert!(service
        .derive(Algorithm::Sha256, Secret::new(vec![255]), 4096)
        .await
        .is_err());
    Ok(())
}

#[tokio::test]
async fn strong_fresh_nonces_salts_and_redacted_debug_errors() -> Result {
    let service = service(Algorithm::Sha256, "sensitive-user", "sensitive-password").await?;
    let a = service
        .derive(Algorithm::Sha256, secret("sensitive-password"), 4096)
        .await?;
    let b = service
        .derive(Algorithm::Sha256, secret("sensitive-password"), 4096)
        .await?;
    assert_eq!(a.form().salt.len(), 32);
    assert_ne!(a.form().salt, b.form().salt);
    let mut nonces = HashSet::new();
    for _ in 0..20 {
        let mut session = service.scram(Algorithm::Sha256)?;
        let challenge =
            String::from_utf8(session.challenge(b"n,,n=sensitive-user,r=visible-client-nonce")?)?;
        let nonce = challenge.split(',').next().ok_or("no nonce")?.to_owned();
        assert!(nonce.starts_with("r=visible-client-nonce"));
        assert_eq!(nonce.len(), 2 + 20 + 43);
        assert!(nonces.insert(nonce));
        assert!(!format!("{session:?}").contains("visible-client-nonce"));
    }
    for output in [
        format!("{service:?}"),
        format!("{a:?}"),
        format!("{:?}", a.form()),
        format!("{:?}", secret("sensitive-password")),
        format!("{}", Error::AuthenticationFailed),
        format!("{:?}", Error::AuthenticationFailed),
    ] {
        assert!(!output.contains("sensitive-password"));
        assert!(!output.contains("sensitive-user"));
        assert!(!output.contains(&STANDARD.encode(a.form().stored_key)));
    }
    Ok(())
}

#[tokio::test]
async fn credential_generation_rotation_and_snapshot_admission_bound() -> Result {
    let algorithm = Algorithm::Sha256;
    let service = Service::new(Limits {
        admissions: 1,
        workers: 1,
        credentials: 1,
        ..Limits::default()
    })?;
    let old = service.derive(algorithm, secret("old"), 4096).await?;
    let new = service.derive(algorithm, secret("new"), 4096).await?;
    service.replace(vec![("user".into(), old)])?;
    let first = "n,,n=user,r=client";
    let mut session = service.scram(algorithm)?;
    assert!(matches!(service.scram(algorithm), Err(Error::Busy)));
    let challenge = session.challenge(first.as_bytes())?;
    assert_eq!(service.replace(vec![("user".into(), new.clone())])?, 2);
    let (proof, _) = final_message(algorithm, "old", first, &challenge)?;
    assert_eq!(
        session
            .finish(Secret::new(proof))
            .await?
            .identity
            .generation(),
        1
    );
    let mut session = service.scram(algorithm)?;
    let challenge = session.challenge(first.as_bytes())?;
    let (proof, _) = final_message(algorithm, "old", first, &challenge)?;
    assert!(session.finish(Secret::new(proof)).await.is_err());
    let mut session = service.scram(algorithm)?;
    let challenge = session.challenge(first.as_bytes())?;
    let (proof, _) = final_message(algorithm, "new", first, &challenge)?;
    assert_eq!(
        session
            .finish(Secret::new(proof))
            .await?
            .identity
            .generation(),
        2
    );
    assert!(service
        .replace(vec![("user".into(), new.clone()), ("other".into(), new)])
        .is_err());
    Ok(())
}

#[tokio::test]
async fn imported_forms_and_all_hard_bounds_reject_invalid_values() -> Result {
    let service = service(Algorithm::Sha256, "user", "pencil").await?;
    let credential = service
        .derive(Algorithm::Sha256, secret("pencil"), 4096)
        .await?;
    for field in ["salt", "stored", "server", "iterations"] {
        let mut form = credential.form();
        match field {
            "salt" => form.salt.clear(),
            "stored" => form.stored_key.truncate(31),
            "server" => form.server_key.clear(),
            _ => form.iterations = 0,
        }
        assert!(Credential::from_form(form, Limits::default()).is_err());
    }
    for limits in [
        Limits {
            message_bytes: 0,
            ..Limits::default()
        },
        Limits {
            identity_bytes: 1025,
            ..Limits::default()
        },
        Limits {
            password_bytes: 16385,
            ..Limits::default()
        },
        Limits {
            salt_bytes: 1025,
            ..Limits::default()
        },
        Limits {
            nonce_bytes: 4097,
            ..Limits::default()
        },
        Limits {
            iterations: 1_000_001,
            ..Limits::default()
        },
        Limits {
            credentials: 4097,
            ..Limits::default()
        },
        Limits {
            admissions: 257,
            ..Limits::default()
        },
        Limits {
            workers: 65,
            ..Limits::default()
        },
    ] {
        assert!(Service::new(limits).is_err());
    }
    let service = Service::new(Limits::default())?;
    assert!(service
        .replace(vec![
            ("user".into(), credential.clone()),
            ("user".into(), credential.clone())
        ])
        .is_err());
    assert!(service
        .replace(vec![("".into(), credential.clone())])
        .is_err());
    assert!(service
        .replace(vec![("x".repeat(257), credential)])
        .is_err());
    Ok(())
}

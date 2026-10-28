//! Verified TLS/mTLS configurations with bounded DER input and atomic rotation.
//!
//! Each admitted socket captures one immutable configuration generation. Rotation
//! affects sockets admitted afterwards; pending handshakes and established
//! sessions retain the generation they captured. Resumption and early data are
//! disabled so a new socket always performs fresh certificate verification.
//! There is no custom verifier, optional client authentication or plaintext fallback.
//! Without required mTLS, TLS authenticates the server to a verifying client, not
//! the client to this listener. Certificate revocation and authorization are not
//! implemented here. Server certificate validity and name are checked by clients.

use rustls::{
    pki_types::{CertificateDer, PrivateKeyDer},
    server::{NoServerSessionStorage, WebPkiClientVerifier},
    RootCertStore, ServerConfig,
};
use std::{sync::Arc, time::Duration};
use tokio::sync::watch;

/// Positive bounds checked before DER parsing and configuration allocation.
#[derive(Debug, Clone, Copy)]
pub struct Limits {
    /// Maximum identity or presented client certificate count (1–32).
    pub max_certificates: usize,
    /// Maximum bytes of one identity, trust or presented certificate (1–64 KiB).
    pub max_certificate_bytes: usize,
    /// Maximum private-key DER bytes (1–64 KiB).
    pub max_key_bytes: usize,
    /// Maximum required-client trust anchors (1–256).
    pub max_roots: usize,
    /// Maximum combined identity/key/trust DER, and presented chain bytes (1–4 MiB).
    pub max_total_der_bytes: usize,
    /// Absolute handshake deadline from socket admission, positive through 24h.
    pub handshake_timeout: Duration,
}

impl Default for Limits {
    fn default() -> Self {
        Self {
            max_certificates: 8,
            max_certificate_bytes: 16 * 1024,
            max_key_bytes: 16 * 1024,
            max_roots: 64,
            max_total_der_bytes: 1024 * 1024,
            handshake_timeout: Duration::from_secs(10),
        }
    }
}

impl Limits {
    fn validate(self) -> Result<(), Error> {
        for (field, valid) in [
            (
                "max_certificates",
                (1..=32).contains(&self.max_certificates),
            ),
            (
                "max_certificate_bytes",
                (1..=64 * 1024).contains(&self.max_certificate_bytes),
            ),
            (
                "max_key_bytes",
                (1..=64 * 1024).contains(&self.max_key_bytes),
            ),
            ("max_roots", (1..=256).contains(&self.max_roots)),
            (
                "max_total_der_bytes",
                (1..=4 * 1024 * 1024).contains(&self.max_total_der_bytes),
            ),
            (
                "handshake_timeout",
                !self.handshake_timeout.is_zero()
                    && self.handshake_timeout <= Duration::from_secs(24 * 60 * 60),
            ),
        ] {
            if !valid {
                return Err(Error::Invalid(field));
            }
        }
        Ok(())
    }

    fn certificates(self, certificates: &[Vec<u8>], maximum: usize) -> Result<usize, Error> {
        if certificates.is_empty() || certificates.len() > maximum {
            return Err(Error::Invalid("certificate count"));
        }
        let mut total = 0usize;
        for certificate in certificates {
            if certificate.is_empty() || certificate.len() > self.max_certificate_bytes {
                return Err(Error::Invalid("certificate bytes"));
            }
            total = total
                .checked_add(certificate.len())
                .ok_or(Error::Invalid("DER total"))?;
        }
        Ok(total)
    }
}

/// Explicit client authentication policy; required mode never accepts an anonymous peer.
pub enum ClientAuth {
    /// Server TLS only: no client certificate is requested or authenticated.
    ServerOnly,
    /// Require a chain valid for client authentication to these DER trust anchors.
    Required(Vec<Vec<u8>>),
}

/// Invalid bounds, malformed DER/key mismatch, verifier construction or rotation failure.
#[derive(Debug)]
pub enum Error {
    /// Named material or configuration bound is invalid.
    Invalid(&'static str),
    /// Rustls rejected certificate/key material or supported TLS versions.
    Rustls(rustls::Error),
    /// Required-client verifier could not be constructed.
    Verifier(rustls::server::VerifierBuilderError),
    /// The configuration generation would overflow.
    GenerationOverflow,
}

impl std::fmt::Display for Error {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Invalid(field) => write!(formatter, "invalid TLS material or limit: {field}"),
            Self::Rustls(error) => write!(formatter, "TLS configuration: {error}"),
            Self::Verifier(error) => write!(formatter, "TLS client verifier: {error}"),
            Self::GenerationOverflow => formatter.write_str("TLS generation overflow"),
        }
    }
}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Rustls(error) => Some(error),
            Self::Verifier(error) => Some(error),
            _ => None,
        }
    }
}

fn build(
    chain: Vec<Vec<u8>>,
    key: Vec<u8>,
    auth: ClientAuth,
    limits: Limits,
) -> Result<Arc<ServerConfig>, Error> {
    limits.validate()?;
    let mut total = limits.certificates(&chain, limits.max_certificates)?;
    if key.is_empty() || key.len() > limits.max_key_bytes {
        return Err(Error::Invalid("key bytes"));
    }
    total = total
        .checked_add(key.len())
        .ok_or(Error::Invalid("DER total"))?;
    let roots = match auth {
        ClientAuth::ServerOnly => None,
        ClientAuth::Required(roots) => {
            total = total
                .checked_add(limits.certificates(&roots, limits.max_roots)?)
                .ok_or(Error::Invalid("DER total"))?;
            Some(roots)
        }
    };
    if total > limits.max_total_der_bytes {
        return Err(Error::Invalid("DER total"));
    }
    let chain: Vec<_> = chain.into_iter().map(CertificateDer::from).collect();
    // Parse every identity certificate, including intermediates, before publication.
    // This checks DER structure, not the client's trust/name/time policy.
    for certificate in &chain {
        RootCertStore::empty()
            .add(certificate.clone())
            .map_err(Error::Rustls)?;
    }
    let key = PrivateKeyDer::try_from(key).map_err(|_| Error::Invalid("private key DER"))?;
    let provider = Arc::new(rustls::crypto::ring::default_provider());
    let builder = ServerConfig::builder_with_provider(provider.clone())
        .with_safe_default_protocol_versions()
        .map_err(Error::Rustls)?;
    let builder = match roots {
        None => builder.with_no_client_auth(),
        Some(roots) => {
            let mut store = RootCertStore::empty();
            for root in roots {
                store
                    .add(CertificateDer::from(root))
                    .map_err(Error::Rustls)?;
            }
            let verifier = WebPkiClientVerifier::builder_with_provider(Arc::new(store), provider)
                .build()
                .map_err(Error::Verifier)?;
            builder.with_client_cert_verifier(verifier)
        }
    };
    let mut config = builder
        .with_single_cert(chain, key)
        .map_err(Error::Rustls)?;
    config.session_storage = Arc::new(NoServerSessionStorage {});
    config.send_tls13_tickets = 0;
    config.max_tls13_tickets = 0;
    config.max_early_data_size = 0;
    Ok(Arc::new(config))
}

pub(crate) struct Snapshot {
    pub(crate) server: Arc<ServerConfig>,
    pub(crate) generation: u64,
    pub(crate) limits: Limits,
}

/// Shared, atomically replaceable validated server identity and client trust.
#[derive(Clone)]
pub struct Acceptor {
    current: watch::Sender<Arc<Snapshot>>,
    limits: Limits,
}

impl Acceptor {
    /// Validate bounded owned DER/key material before starting any listener I/O.
    ///
    /// Identity/key matching and DER structure are checked eagerly. Clients must
    /// validate server trust, validity and names; required mTLS uses WebPKI's
    /// chain, signature, validity and client-EKU checks during each handshake.
    pub fn new(
        chain: Vec<Vec<u8>>,
        key: Vec<u8>,
        auth: ClientAuth,
        limits: Limits,
    ) -> Result<Self, Error> {
        let server = build(chain, key, auth, limits)?;
        let (current, _) = watch::channel(Arc::new(Snapshot {
            server,
            generation: 0,
            limits,
        }));
        Ok(Self { current, limits })
    }

    /// Validate an update fully, then atomically publish it for later admissions.
    ///
    /// Invalid updates leave the previous generation intact. Pending handshakes
    /// and established sessions keep their captured identity/trust. This is not
    /// forced disconnection or retroactive revocation of existing sessions.
    pub fn rotate(
        &self,
        chain: Vec<Vec<u8>>,
        key: Vec<u8>,
        auth: ClientAuth,
    ) -> Result<u64, Error> {
        let server = build(chain, key, auth, self.limits)?;
        let mut generation = None;
        self.current.send_if_modified(|current| {
            let Some(next) = current.generation.checked_add(1) else {
                return false;
            };
            *current = Arc::new(Snapshot {
                server,
                generation: next,
                limits: self.limits,
            });
            generation = Some(next);
            true
        });
        generation.ok_or(Error::GenerationOverflow)
    }

    /// Current configuration generation, beginning at zero.
    pub fn generation(&self) -> u64 {
        self.snapshot().generation
    }

    pub(crate) fn snapshot(&self) -> Arc<Snapshot> {
        self.current.borrow().clone()
    }
}

/// Security metadata obtained after a successfully verified TLS handshake.
#[derive(Debug, Clone)]
pub struct VerifiedPeer {
    generation: u64,
    certificates: Option<Arc<[Vec<u8>]>>,
}

impl VerifiedPeer {
    /// Configuration generation captured at socket admission.
    pub fn generation(&self) -> u64 {
        self.generation
    }

    /// Authenticated client leaf and presented chain, leaf first; absent for server-only TLS.
    ///
    /// WebPKI verified a path from the leaf to configured trust. Additional
    /// presented certificates are not separate authenticated identities or a
    /// guaranteed minimal verified path. This is not an authorized ACL principal;
    /// no subject/SAN parsing or authorization decision is implied.
    pub fn client_certificates_der(&self) -> Option<&[Vec<u8>]> {
        self.certificates.as_deref()
    }

    pub(crate) fn new(
        snapshot: &Snapshot,
        certificates: Option<&[CertificateDer<'_>]>,
    ) -> Result<Self, Error> {
        let certificates = if let Some(chain) = certificates {
            if chain.len() > snapshot.limits.max_certificates {
                return Err(Error::Invalid("peer certificate count"));
            }
            let mut total = 0usize;
            for certificate in chain {
                if certificate.len() > snapshot.limits.max_certificate_bytes {
                    return Err(Error::Invalid("peer certificate bytes"));
                }
                total = total
                    .checked_add(certificate.len())
                    .ok_or(Error::Invalid("peer DER total"))?;
            }
            if total > snapshot.limits.max_total_der_bytes {
                return Err(Error::Invalid("peer DER total"));
            }
            Some(
                chain
                    .iter()
                    .map(|certificate| certificate.as_ref().to_vec())
                    .collect(),
            )
        } else {
            None
        };
        Ok(Self {
            generation: snapshot.generation,
            certificates,
        })
    }
}

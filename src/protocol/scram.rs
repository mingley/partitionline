//! SASL SCRAM-SHA-256 and SCRAM-SHA-512 (RFC 5802 / RFC 7677) as used by Kafka.
//! Password hashing is PBKDF2-HMAC of the selected hash. No C SASL library.
//!
//! [`sasl_name`] / [`username`] / [`xor`] are Java `ScramFormatter.saslName` /
//! `username` / `xor` (`=` then `,`; leftover `=` after decoding `=3D` is
//! [`Error::protocol`]). [`ScramAlg::hash_algorithm`] /
//! [`ScramAlg::mac_algorithm`] / [`ScramAlg::min_iterations`] /
//! [`ScramAlg::max_iterations`] / [`ScramAlg::from_mechanism_name`] /
//! [`ScramAlg::mechanism_names`] / [`ScramAlg::is_scram`] are Java internals
//! `ScramMechanism` (unknown name is `None`; this is not admin
//! `ScramMechanism.fromMechanismName`, which returns `UNKNOWN`).

use hmac::{Hmac, Mac};
use pbkdf2::pbkdf2_hmac;
use sha2::{Digest, Sha256, Sha512};

use crate::error::{Error, Result};

const CLIENT_KEY: &[u8] = b"Client Key";
const SERVER_KEY: &[u8] = b"Server Key";
const GS2: &str = "n,,";

/// SCRAM hash algorithm used by Kafka SASL.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ScramAlg {
    /// `SCRAM-SHA-256`.
    Sha256,
    /// `SCRAM-SHA-512`.
    Sha512,
}

impl ScramAlg {
    /// Kafka SASL mechanism name.
    ///
    /// Java internals `ScramMechanism.mechanismName` (`SCRAM-SHA-256`).
    #[must_use]
    pub fn name(self) -> &'static str {
        match self {
            Self::Sha256 => "SCRAM-SHA-256",
            Self::Sha512 => "SCRAM-SHA-512",
        }
    }

    /// Java internals `ScramMechanism.hashAlgorithm`.
    #[must_use]
    pub const fn hash_algorithm(self) -> &'static str {
        match self {
            Self::Sha256 => "SHA-256",
            Self::Sha512 => "SHA-512",
        }
    }

    /// Java internals `ScramMechanism.macAlgorithm`.
    #[must_use]
    pub const fn mac_algorithm(self) -> &'static str {
        match self {
            Self::Sha256 => "HmacSHA256",
            Self::Sha512 => "HmacSHA512",
        }
    }

    /// Java internals `ScramMechanism.minIterations` (`4096`).
    #[must_use]
    pub const fn min_iterations(self) -> i32 {
        match self {
            Self::Sha256 | Self::Sha512 => 4096,
        }
    }

    /// Java internals `ScramMechanism.maxIterations` (`16384`).
    #[must_use]
    pub const fn max_iterations(self) -> i32 {
        match self {
            Self::Sha256 | Self::Sha512 => 16384,
        }
    }

    /// Java internals `ScramMechanism.forMechanismName` (unknown is `None`).
    #[must_use]
    pub fn from_mechanism_name(name: &str) -> Option<Self> {
        match name {
            "SCRAM-SHA-256" => Some(Self::Sha256),
            "SCRAM-SHA-512" => Some(Self::Sha512),
            _ => None,
        }
    }

    /// Java internals `ScramMechanism.mechanismNames` (declaration order).
    #[must_use]
    pub const fn mechanism_names() -> &'static [&'static str] {
        &["SCRAM-SHA-256", "SCRAM-SHA-512"]
    }

    /// Java internals `ScramMechanism.isScram`.
    #[must_use]
    pub fn is_scram(mechanism_name: &str) -> bool {
        Self::from_mechanism_name(mechanism_name).is_some()
    }

    fn output_len(self) -> usize {
        match self {
            Self::Sha256 => 32,
            Self::Sha512 => 64,
        }
    }

    fn hmac(self, key: &[u8], data: &[u8]) -> Vec<u8> {
        match self {
            Self::Sha256 => {
                let Ok(mut m) = Hmac::<Sha256>::new_from_slice(key) else {
                    return Vec::new();
                };
                m.update(data);
                m.finalize().into_bytes().to_vec()
            }
            Self::Sha512 => {
                let Ok(mut m) = Hmac::<Sha512>::new_from_slice(key) else {
                    return Vec::new();
                };
                m.update(data);
                m.finalize().into_bytes().to_vec()
            }
        }
    }

    fn hash(self, data: &[u8]) -> Vec<u8> {
        match self {
            Self::Sha256 => Sha256::digest(data).to_vec(),
            Self::Sha512 => Sha512::digest(data).to_vec(),
        }
    }

    fn hi(self, password: &[u8], salt: &[u8], iterations: u32) -> Vec<u8> {
        let mut out = vec![0u8; self.output_len()];
        match self {
            Self::Sha256 => pbkdf2_hmac::<Sha256>(password, salt, iterations, &mut out),
            Self::Sha512 => pbkdf2_hmac::<Sha512>(password, salt, iterations, &mut out),
        }
        out
    }
}

/// Java `ScramFormatter.saslName` (`=` then `,`).
#[must_use]
pub fn sasl_name(username: &str) -> String {
    username.replace('=', "=3D").replace(',', "=2C")
}

/// Java `ScramFormatter.username`. Leftover `=` is [`Error::protocol`]
/// (`Invalid username: …`).
pub fn username(sasl_name: &str) -> Result<String> {
    let with_commas = sasl_name.replace("=2C", ",");
    if with_commas.replace("=3D", "").contains('=') {
        return Err(Error::protocol(format!("Invalid username: {sasl_name}")));
    }
    Ok(with_commas.replace("=3D", "="))
}

/// Java `ScramFormatter.xor`. Length mismatch is [`Error::protocol`]
/// (`Argument arrays must be of the same length`).
pub fn xor(first: &[u8], second: &[u8]) -> Result<Vec<u8>> {
    if first.len() != second.len() {
        return Err(Error::protocol(
            "Argument arrays must be of the same length",
        ));
    }
    Ok(first.iter().zip(second).map(|(x, y)| x ^ y).collect())
}

fn b64(bytes: &[u8]) -> String {
    base64::Engine::encode(&base64::engine::general_purpose::STANDARD, bytes)
}

fn b64d(s: &str) -> Result<Vec<u8>> {
    base64::Engine::decode(&base64::engine::general_purpose::STANDARD, s)
        .map_err(|e| Error::protocol(format!("scram base64: {e}")))
}

fn attr_map(msg: &str) -> Result<std::collections::HashMap<char, String>> {
    let mut m = std::collections::HashMap::new();
    for part in msg.split(',') {
        let mut c = part.chars();
        let k = c
            .next()
            .ok_or_else(|| Error::protocol("scram empty attr"))?;
        if c.next() != Some('=') {
            return Err(Error::protocol(format!("scram bad attr {part}")));
        }
        drop(m.insert(k, c.as_str().to_string()));
    }
    Ok(m)
}

/// Random client nonce (`r=`), printable ASCII.
pub fn client_nonce() -> String {
    let mut raw = [0u8; 18];
    if getrandom::getrandom(&mut raw).is_err() {
        raw = [1; 18];
    }
    const A: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";
    raw.iter()
        .map(|b| {
            let idx = usize::from(*b) % A.len();
            char::from(*A.get(idx).unwrap_or(&b'A'))
        })
        .collect()
}

/// GS2 header plus `n=,r=` client-first; returns `(full, client_first_bare)`.
pub fn client_first(user: &str, nonce: &str) -> (String, String) {
    let bare = format!("n={},r={}", sasl_name(user), nonce);
    (format!("{GS2}{bare}"), bare)
}

/// Client-final message (`c=,r=,p=`) after the server-first challenge.
pub fn client_final(
    alg: ScramAlg,
    password: &str,
    client_first_bare: &str,
    server_first: &str,
) -> Result<String> {
    let attrs = attr_map(server_first)?;
    let nonce = attrs
        .get(&'r')
        .ok_or_else(|| Error::protocol("scram server missing r"))?;
    let salt = b64d(
        attrs
            .get(&'s')
            .ok_or_else(|| Error::protocol("scram server missing s"))?,
    )?;
    let iter: u32 = attrs
        .get(&'i')
        .ok_or_else(|| Error::protocol("scram server missing i"))?
        .parse()
        .map_err(|_| Error::protocol("scram bad iteration"))?;
    if iter == 0 {
        return Err(Error::protocol("scram iteration 0"));
    }
    let without = format!("c={},r={}", b64(GS2.as_bytes()), nonce);
    let auth = format!("{client_first_bare},{server_first},{without}");
    let salted = alg.hi(password.as_bytes(), &salt, iter);
    let client_key = alg.hmac(&salted, CLIENT_KEY);
    let stored = alg.hash(&client_key);
    let sig = alg.hmac(&stored, auth.as_bytes());
    let proof = xor(&client_key, &sig)?;
    Ok(format!("{without},p={}", b64(&proof)))
}

/// Verify the server-final `v=` signature (or surface a server `e=` error).
pub fn verify_server_final(
    alg: ScramAlg,
    password: &str,
    client_first_bare: &str,
    server_first: &str,
    client_final: &str,
    server_final: &str,
) -> Result<()> {
    let attrs = attr_map(server_final)?;
    if let Some(e) = attrs.get(&'e') {
        return Err(Error::protocol(format!("scram server error: {e}")));
    }
    let v = attrs
        .get(&'v')
        .ok_or_else(|| Error::protocol("scram server missing v"))?;
    let sattrs = attr_map(server_first)?;
    let salt = b64d(
        sattrs
            .get(&'s')
            .ok_or_else(|| Error::protocol("scram server missing s"))?,
    )?;
    let iter: u32 = sattrs
        .get(&'i')
        .ok_or_else(|| Error::protocol("scram server missing i"))?
        .parse()
        .map_err(|_| Error::protocol("scram bad iteration"))?;
    let without = client_final
        .rsplit_once(",p=")
        .map(|(w, _)| w)
        .ok_or_else(|| Error::protocol("scram client-final missing p"))?;
    let auth = format!("{client_first_bare},{server_first},{without}");
    let salted = alg.hi(password.as_bytes(), &salt, iter);
    let server_key = alg.hmac(&salted, SERVER_KEY);
    let sig = alg.hmac(&server_key, auth.as_bytes());
    let got = b64d(v)?;
    if got.as_slice() != sig {
        return Err(Error::protocol("scram server signature mismatch"));
    }
    Ok(())
}

/// Server-side first message (mock broker).
pub fn server_first(
    client_first: &str,
    snonce: &str,
    salt: &[u8],
    iterations: u32,
) -> Result<(String, String)> {
    let bare = client_first
        .strip_prefix(GS2)
        .ok_or_else(|| Error::protocol("scram gs2"))?;
    let attrs = attr_map(bare)?;
    let cnonce = attrs
        .get(&'r')
        .ok_or_else(|| Error::protocol("scram client missing r"))?;
    let nonce = format!("{cnonce}{snonce}");
    let msg = format!("r={nonce},s={},i={iterations}", b64(salt));
    Ok((msg, bare.to_string()))
}

/// Verify client-final and build server-final (mock broker).
pub fn server_final(
    alg: ScramAlg,
    password: &str,
    client_first_bare: &str,
    server_first: &str,
    client_final_msg: &str,
) -> Result<String> {
    let (without, p) = client_final_msg
        .rsplit_once(",p=")
        .ok_or_else(|| Error::protocol("scram client-final missing p"))?;
    let expected = client_final(alg, password, client_first_bare, server_first)?;
    let expected_p = expected
        .rsplit_once(",p=")
        .map(|(_, p)| p)
        .ok_or_else(|| Error::protocol("scram"))?;
    if p != expected_p {
        return Err(Error::protocol("scram client proof mismatch"));
    }
    let sattrs = attr_map(server_first)?;
    let salt = b64d(sattrs.get(&'s').ok_or_else(|| Error::protocol("scram"))?)?;
    let iter: u32 = sattrs
        .get(&'i')
        .ok_or_else(|| Error::protocol("scram"))?
        .parse()
        .map_err(|_| Error::protocol("scram bad iteration"))?;
    let auth = format!("{client_first_bare},{server_first},{without}");
    let salted = alg.hi(password.as_bytes(), &salt, iter);
    let server_key = alg.hmac(&salted, SERVER_KEY);
    let sig = alg.hmac(&server_key, auth.as_bytes());
    Ok(format!("v={}", b64(&sig)))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// RFC 7677 test vector.
    #[test]
    fn rfc7677_scram_sha256() {
        let user = "user";
        let pass = "pencil";
        let cnonce = "rOprNGfwEbeRWgbNEkqO";
        let (first, bare) = client_first(user, cnonce);
        assert_eq!(first, "n,,n=user,r=rOprNGfwEbeRWgbNEkqO");
        let server_first =
            "r=rOprNGfwEbeRWgbNEkqO%hvYDpWUa2RaTCAfuxFIlj)hNlF$k0,s=W22ZaJ0SNY7soEsUEjb6gQ==,i=4096";
        let fin = client_final(ScramAlg::Sha256, pass, &bare, server_first).unwrap();
        assert_eq!(
            fin,
            "c=biws,r=rOprNGfwEbeRWgbNEkqO%hvYDpWUa2RaTCAfuxFIlj)hNlF$k0,p=dHzbZapWIk4jUhN+Ute9ytag9zjfMHgsqmmiz7AndVQ="
        );
        verify_server_final(
            ScramAlg::Sha256,
            pass,
            &bare,
            server_first,
            &fin,
            "v=6rriTRBi23WpRR/wtup+mMhUZUn/dB5nLTJRsjl95G4=",
        )
        .unwrap();
    }

    /// Same RFC 7677 transcript with SHA-512. Expected proof/v computed with
    /// Python `hashlib.pbkdf2_hmac('sha512', ...)` independently of this crate.
    #[test]
    fn rfc7677_transcript_scram_sha512() {
        let (first, bare) = client_first("user", "rOprNGfwEbeRWgbNEkqO");
        assert_eq!(first, "n,,n=user,r=rOprNGfwEbeRWgbNEkqO");
        let server_first =
            "r=rOprNGfwEbeRWgbNEkqO%hvYDpWUa2RaTCAfuxFIlj)hNlF$k0,s=W22ZaJ0SNY7soEsUEjb6gQ==,i=4096";
        let fin = client_final(ScramAlg::Sha512, "pencil", &bare, server_first).unwrap();
        assert_eq!(
            fin,
            "c=biws,r=rOprNGfwEbeRWgbNEkqO%hvYDpWUa2RaTCAfuxFIlj)hNlF$k0,p=gMGXRcevScNtxZ6/8lQYpGtnsNAc3mGcmNomv+xnoOMw+3R2xNJdMNnzMlTN8PPC6wdp6dybEmDYXYTxwnYPJQ=="
        );
        verify_server_final(
            ScramAlg::Sha512,
            "pencil",
            &bare,
            server_first,
            &fin,
            "v=ZQnYEgWQMFmmsM8aQMF0nDDCy/AgCzkwk8CmMZYcMg0vSVlKDanekLtifDSeVGT4+5ZxXnJq199RVG2rR7N7Zw==",
        )
        .unwrap();
    }

    #[test]
    fn mock_server_roundtrip_sha256() {
        let (first, _) = client_first("alice", "abcNonce");
        let salt = b"saltsaltsalt1234";
        let (sf, bare) = server_first(&first, "SrvNoncE", salt, 4096).unwrap();
        let cf = client_final(ScramAlg::Sha256, "secret", &bare, &sf).unwrap();
        let fin = server_final(ScramAlg::Sha256, "secret", &bare, &sf, &cf).unwrap();
        verify_server_final(ScramAlg::Sha256, "secret", &bare, &sf, &cf, &fin).unwrap();
    }

    #[test]
    fn mock_server_roundtrip_sha512() {
        let (first, _) = client_first("alice", "abcNonce");
        let salt = b"saltsaltsalt1234";
        let (sf, bare) = server_first(&first, "SrvNoncE", salt, 4096).unwrap();
        let cf = client_final(ScramAlg::Sha512, "secret", &bare, &sf).unwrap();
        let fin = server_final(ScramAlg::Sha512, "secret", &bare, &sf, &cf).unwrap();
        verify_server_final(ScramAlg::Sha512, "secret", &bare, &sf, &cf, &fin).unwrap();
    }

    #[test]
    fn scram_formatter_matches_java() {
        assert_eq!(sasl_name("user"), "user");
        assert_eq!(sasl_name("user=name"), "user=3Dname");
        assert_eq!(sasl_name("user,name"), "user=2Cname");
        assert_eq!(sasl_name("a=,b"), "a=3D=2Cb");
        assert_eq!(username("user").unwrap(), "user");
        assert_eq!(username("user=3Dname").unwrap(), "user=name");
        assert_eq!(username("user=2Cname").unwrap(), "user,name");
        assert_eq!(username("a=3D=2Cb").unwrap(), "a=,b");
        let invalid = username("user=name").unwrap_err().to_string();
        assert!(invalid.contains("Invalid username: user=name"), "{invalid}");
        assert_eq!(xor(&[1, 2], &[1, 3]).unwrap(), vec![0, 1]);
        assert_eq!(xor(&[], &[]).unwrap(), Vec::<u8>::new());
        let mismatch = xor(&[1], &[1, 2]).unwrap_err().to_string();
        assert!(
            mismatch.contains("Argument arrays must be of the same length"),
            "{mismatch}"
        );
        let (first, _) = client_first("user=name,x", "n1");
        assert_eq!(first, "n,,n=user=3Dname=2Cx,r=n1");
    }

    #[test]
    fn scram_mechanism_internals_match_java() {
        assert_eq!(ScramAlg::Sha256.hash_algorithm(), "SHA-256");
        assert_eq!(ScramAlg::Sha512.hash_algorithm(), "SHA-512");
        assert_eq!(ScramAlg::Sha256.mac_algorithm(), "HmacSHA256");
        assert_eq!(ScramAlg::Sha512.mac_algorithm(), "HmacSHA512");
        assert_eq!(ScramAlg::Sha256.min_iterations(), 4096);
        assert_eq!(ScramAlg::Sha512.min_iterations(), 4096);
        assert_eq!(ScramAlg::Sha256.max_iterations(), 16384);
        assert_eq!(ScramAlg::Sha512.max_iterations(), 16384);
        assert_eq!(
            ScramAlg::from_mechanism_name("SCRAM-SHA-256"),
            Some(ScramAlg::Sha256)
        );
        assert_eq!(
            ScramAlg::from_mechanism_name("SCRAM-SHA-512"),
            Some(ScramAlg::Sha512)
        );
        assert_eq!(ScramAlg::from_mechanism_name("PLAIN"), None);
        assert_eq!(ScramAlg::from_mechanism_name("UNKNOWN"), None);
        assert_eq!(ScramAlg::from_mechanism_name("SCRAM_SHA_256"), None);
        assert!(ScramAlg::is_scram("SCRAM-SHA-256"));
        assert!(ScramAlg::is_scram("SCRAM-SHA-512"));
        assert!(!ScramAlg::is_scram("PLAIN"));
        assert!(!ScramAlg::is_scram("UNKNOWN"));
        assert_eq!(
            ScramAlg::mechanism_names(),
            ["SCRAM-SHA-256", "SCRAM-SHA-512"]
        );
    }
}

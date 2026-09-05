//! SASL SCRAM-SHA-256 and SCRAM-SHA-512 (RFC 5802 / RFC 7677) as used by Kafka.
//! Password hashing is PBKDF2-HMAC of the selected hash. No C SASL library.
//!
//! Helpers `sasl_name` / `username` / `xor` / `auth_message` / `to_bytes` /
//! `normalize` match Java `ScramFormatter` (`=` then `,`; leftover `=` after
//! decoding `=3D` is `Error::protocol`). `ScramAlg` methods (`hmac`, `hash`,
//! `hi`, `salted_password`, `client_key`, `stored_key`, `stored_key_from_proof`,
//! `server_key`, `client_signature`, `client_proof`, `server_signature`) match
//! Java `ScramFormatter` helpers. `from_mechanism_name` / `is_scram` match
//! Java `ScramMechanism` name lookup (unknown → `None`; not admin
//! `ScramMechanism.fromMechanismName`, which returns `UNKNOWN`).

use hmac::{Hmac, KeyInit, Mac};
use pbkdf2::pbkdf2_hmac;
use sha2::{Digest, Sha256, Sha512};

use crate::error::{Error, Result};

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

    /// Java `ScramFormatter.hmac`.
    #[must_use]
    pub fn hmac(self, key: &[u8], data: &[u8]) -> Vec<u8> {
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

    /// Java `ScramFormatter.hash`.
    #[must_use]
    pub fn hash(self, data: &[u8]) -> Vec<u8> {
        match self {
            Self::Sha256 => Sha256::digest(data).to_vec(),
            Self::Sha512 => Sha512::digest(data).to_vec(),
        }
    }

    /// Java `ScramFormatter.hi` (PBKDF2-HMAC of [`Self::hash_algorithm`]).
    #[must_use]
    pub fn hi(self, password: &[u8], salt: &[u8], iterations: u32) -> Vec<u8> {
        let mut out = vec![0u8; self.output_len()];
        match self {
            Self::Sha256 => pbkdf2_hmac::<Sha256>(password, salt, iterations, &mut out),
            Self::Sha512 => pbkdf2_hmac::<Sha512>(password, salt, iterations, &mut out),
        }
        out
    }

    /// Java `ScramFormatter.saltedPassword` (`hi` of `normalize(password)`).
    #[must_use]
    pub fn salted_password(self, password: &str, salt: &[u8], iterations: u32) -> Vec<u8> {
        self.hi(&normalize(password), salt, iterations)
    }

    /// Java `ScramFormatter.clientKey`.
    #[must_use]
    pub fn client_key(self, salted_password: &[u8]) -> Vec<u8> {
        self.hmac(salted_password, &to_bytes("Client Key"))
    }

    /// Java `ScramFormatter.storedKey` of the client key (`hash`).
    #[must_use]
    pub fn stored_key(self, client_key: &[u8]) -> Vec<u8> {
        self.hash(client_key)
    }

    /// Java `ScramFormatter.storedKey` of signature and proof (`hash(xor)`).
    pub fn stored_key_from_proof(
        self,
        client_signature: &[u8],
        client_proof: &[u8],
    ) -> Result<Vec<u8>> {
        Ok(self.hash(&xor(client_signature, client_proof)?))
    }

    /// Java `ScramFormatter.serverKey`.
    #[must_use]
    pub fn server_key(self, salted_password: &[u8]) -> Vec<u8> {
        self.hmac(salted_password, &to_bytes("Server Key"))
    }

    /// Java `ScramFormatter.clientSignature`.
    ///
    /// HMAC of the UTF-8 [`auth_message`] with `storedKey`.
    #[must_use]
    pub fn client_signature(
        self,
        stored_key: &[u8],
        client_first_message_bare: &str,
        server_first_message: &str,
        client_final_message_without_proof: &str,
    ) -> Vec<u8> {
        self.hmac(
            stored_key,
            &auth_message_bytes(
                client_first_message_bare,
                server_first_message,
                client_final_message_without_proof,
            ),
        )
    }

    /// Java `ScramFormatter.clientProof`.
    ///
    /// `xor` of `clientKey` and [`Self::client_signature`]. Length mismatch is
    /// [`crate::Error::protocol`].
    pub fn client_proof(
        self,
        salted_password: &[u8],
        client_first_message_bare: &str,
        server_first_message: &str,
        client_final_message_without_proof: &str,
    ) -> Result<Vec<u8>> {
        let client_key = self.client_key(salted_password);
        let stored_key = self.stored_key(&client_key);
        let client_signature = self.client_signature(
            &stored_key,
            client_first_message_bare,
            server_first_message,
            client_final_message_without_proof,
        );
        xor(&client_key, &client_signature)
    }

    /// Java `ScramFormatter.serverSignature`.
    ///
    /// HMAC of the UTF-8 [`auth_message`] with `serverKey`.
    #[must_use]
    pub fn server_signature(
        self,
        server_key: &[u8],
        client_first_message_bare: &str,
        server_first_message: &str,
        client_final_message_without_proof: &str,
    ) -> Vec<u8> {
        self.hmac(
            server_key,
            &auth_message_bytes(
                client_first_message_bare,
                server_first_message,
                client_final_message_without_proof,
            ),
        )
    }
}

/// Java `ScramFormatter.saslName` (`=` then `,`).
#[must_use]
pub fn sasl_name(username: &str) -> String {
    username.replace('=', "=3D").replace(',', "=2C")
}

/// Java `ScramFormatter.username`. Leftover `=` is [`crate::Error::protocol`]
/// (`Invalid username: …`).
pub fn username(sasl_name: &str) -> Result<String> {
    let with_commas = sasl_name.replace("=2C", ",");
    if with_commas.replace("=3D", "").contains('=') {
        return Err(Error::protocol(format!("Invalid username: {sasl_name}")));
    }
    Ok(with_commas.replace("=3D", "="))
}

/// Java `ScramFormatter.xor`. Length mismatch is [`crate::Error::protocol`]
/// (`Argument arrays must be of the same length`).
pub fn xor(first: &[u8], second: &[u8]) -> Result<Vec<u8>> {
    if first.len() != second.len() {
        return Err(Error::protocol(
            "Argument arrays must be of the same length",
        ));
    }
    Ok(first.iter().zip(second).map(|(x, y)| x ^ y).collect())
}

/// Java `ScramFormatter.authMessage` (`a,b,c`).
#[must_use]
pub fn auth_message(
    client_first_message_bare: &str,
    server_first_message: &str,
    client_final_message_without_proof: &str,
) -> String {
    format!(
        "{client_first_message_bare},{server_first_message},{client_final_message_without_proof}"
    )
}

fn auth_message_bytes(
    client_first_message_bare: &str,
    server_first_message: &str,
    client_final_message_without_proof: &str,
) -> Vec<u8> {
    to_bytes(&auth_message(
        client_first_message_bare,
        server_first_message,
        client_final_message_without_proof,
    ))
}

/// Java `ScramFormatter.toBytes` (UTF-8).
#[must_use]
pub fn to_bytes(s: &str) -> Vec<u8> {
    s.as_bytes().to_vec()
}

/// Java `ScramFormatter.normalize` (`toBytes`).
#[must_use]
pub fn normalize(s: &str) -> Vec<u8> {
    to_bytes(s)
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
    let salted = alg.salted_password(password, &salt, iter);
    let proof = alg.client_proof(&salted, client_first_bare, server_first, &without)?;
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
    let salted = alg.salted_password(password, &salt, iter);
    let server_key = alg.server_key(&salted);
    let sig = alg.server_signature(&server_key, client_first_bare, server_first, without);
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
    let salted = alg.salted_password(password, &salt, iter);
    let server_key = alg.server_key(&salted);
    let sig = alg.server_signature(&server_key, client_first_bare, server_first, without);
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
        assert_eq!(auth_message("a", "b", "c"), "a,b,c");
        assert_eq!(auth_message("", "", ""), ",,");
        assert_eq!(to_bytes("Client Key"), b"Client Key");
        assert_eq!(normalize("pencil"), b"pencil");
        assert_eq!(normalize(""), to_bytes(""));
        let salt = base64::Engine::decode(
            &base64::engine::general_purpose::STANDARD,
            "W22ZaJ0SNY7soEsUEjb6gQ==",
        )
        .unwrap();
        let salted = ScramAlg::Sha256.salted_password("pencil", &salt, 4096);
        let client_key = ScramAlg::Sha256.client_key(&salted);
        let stored = ScramAlg::Sha256.stored_key(&client_key);
        let sig = ScramAlg::Sha256.hmac(&stored, b"auth");
        let proof = xor(&client_key, &sig).unwrap();
        assert_eq!(
            ScramAlg::Sha256
                .stored_key_from_proof(&sig, &proof)
                .unwrap(),
            stored
        );
        assert_eq!(
            ScramAlg::Sha256.server_key(&salted),
            ScramAlg::Sha256.hmac(&salted, &to_bytes("Server Key"))
        );
        assert_eq!(
            ScramAlg::Sha256.hi(&normalize("pencil"), &salt, 4096),
            salted
        );
        assert_eq!(ScramAlg::Sha256.hash(&client_key), stored);
        let client_sig = ScramAlg::Sha256.client_signature(&stored, "a", "b", "c");
        assert_eq!(
            client_sig,
            ScramAlg::Sha256.hmac(&stored, &to_bytes(&auth_message("a", "b", "c")))
        );
        assert_eq!(
            ScramAlg::Sha256
                .client_proof(&salted, "a", "b", "c")
                .unwrap(),
            xor(&client_key, &client_sig).unwrap()
        );
        let server_key = ScramAlg::Sha256.server_key(&salted);
        assert_eq!(
            ScramAlg::Sha256.server_signature(&server_key, "a", "b", "c"),
            ScramAlg::Sha256.hmac(&server_key, &to_bytes(&auth_message("a", "b", "c")))
        );
        let mismatch = ScramAlg::Sha256
            .stored_key_from_proof(&[1], &[1, 2])
            .unwrap_err()
            .to_string();
        assert!(
            mismatch.contains("Argument arrays must be of the same length"),
            "{mismatch}"
        );
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

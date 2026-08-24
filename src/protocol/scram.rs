//! SASL SCRAM-SHA-256 (RFC 5802 / RFC 7677) as used by Kafka.
//! Password hashing is PBKDF2-HMAC-SHA-256. No C SASL library.

use hmac::{Hmac, Mac};
use pbkdf2::pbkdf2_hmac;
use sha2::{Digest, Sha256};

use crate::error::{Error, Result};

type HmacSha256 = Hmac<Sha256>;

const CLIENT_KEY: &[u8] = b"Client Key";
const SERVER_KEY: &[u8] = b"Server Key";
const GS2: &str = "n,,";

fn hmac(key: &[u8], data: &[u8]) -> [u8; 32] {
    let mut m = HmacSha256::new_from_slice(key).expect("HMAC-SHA256 key");
    m.update(data);
    m.finalize().into_bytes().into()
}

fn sha256(data: &[u8]) -> [u8; 32] {
    Sha256::digest(data).into()
}

fn hi(password: &[u8], salt: &[u8], iterations: u32) -> [u8; 32] {
    let mut out = [0u8; 32];
    pbkdf2_hmac::<Sha256>(password, salt, iterations, &mut out);
    out
}

fn xor32(a: &[u8; 32], b: &[u8; 32]) -> [u8; 32] {
    let mut o = [0u8; 32];
    for i in 0..32 {
        o[i] = a[i] ^ b[i];
    }
    o
}

fn b64(bytes: &[u8]) -> String {
    base64::Engine::encode(&base64::engine::general_purpose::STANDARD, bytes)
}

fn b64d(s: &str) -> Result<Vec<u8>> {
    base64::Engine::decode(&base64::engine::general_purpose::STANDARD, s)
        .map_err(|e| Error::protocol(format!("scram base64: {e}")))
}

fn escape_user(user: &str) -> String {
    user.replace('=', "=3D").replace(',', "=2C")
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
        m.insert(k, c.as_str().to_string());
    }
    Ok(m)
}

pub fn client_nonce() -> String {
    let mut raw = [0u8; 18];
    getrandom::getrandom(&mut raw).expect("getrandom");
    const A: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";
    raw.iter()
        .map(|b| A[(*b as usize) % A.len()] as char)
        .collect()
}

pub fn client_first(user: &str, nonce: &str) -> (String, String) {
    let bare = format!("n={},r={}", escape_user(user), nonce);
    (format!("{GS2}{bare}"), bare)
}

pub fn client_final(password: &str, client_first_bare: &str, server_first: &str) -> Result<String> {
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
    let salted = hi(password.as_bytes(), &salt, iter);
    let client_key = hmac(&salted, CLIENT_KEY);
    let stored = sha256(&client_key);
    let sig = hmac(&stored, auth.as_bytes());
    let proof = xor32(&client_key, &sig);
    Ok(format!("{without},p={}", b64(&proof)))
}

pub fn verify_server_final(
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
    let salted = hi(password.as_bytes(), &salt, iter);
    let server_key = hmac(&salted, SERVER_KEY);
    let sig = hmac(&server_key, auth.as_bytes());
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
    password: &str,
    client_first_bare: &str,
    server_first: &str,
    client_final: &str,
) -> Result<String> {
    let without = client_final
        .rsplit_once(",p=")
        .map(|(w, _)| w)
        .ok_or_else(|| Error::protocol("scram client-final missing p"))?;
    let p = client_final
        .rsplit_once(",p=")
        .map(|(_, p)| p)
        .ok_or_else(|| Error::protocol("scram client-final missing p"))?;
    let expected = client_final_from_parts(password, client_first_bare, server_first)?;
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
    let salted = hi(password.as_bytes(), &salt, iter);
    let server_key = hmac(&salted, SERVER_KEY);
    let sig = hmac(&server_key, auth.as_bytes());
    Ok(format!("v={}", b64(&sig)))
}

fn client_final_from_parts(
    password: &str,
    client_first_bare: &str,
    server_first: &str,
) -> Result<String> {
    client_final(password, client_first_bare, server_first)
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
        let fin = client_final(pass, &bare, server_first).unwrap();
        assert_eq!(
            fin,
            "c=biws,r=rOprNGfwEbeRWgbNEkqO%hvYDpWUa2RaTCAfuxFIlj)hNlF$k0,p=dHzbZapWIk4jUhN+Ute9ytag9zjfMHgsqmmiz7AndVQ="
        );
        verify_server_final(
            pass,
            &bare,
            server_first,
            &fin,
            "v=6rriTRBi23WpRR/wtup+mMhUZUn/dB5nLTJRsjl95G4=",
        )
        .unwrap();
    }

    #[test]
    fn mock_server_roundtrip() {
        let (first, _) = client_first("alice", "abcNonce");
        let salt = b"saltsaltsalt1234";
        let (sf, bare) = server_first(&first, "SrvNoncE", salt, 4096).unwrap();
        let cf = client_final("secret", &bare, &sf).unwrap();
        let fin = server_final("secret", &bare, &sf, &cf).unwrap();
        verify_server_final("secret", &bare, &sf, &cf, &fin).unwrap();
    }
}

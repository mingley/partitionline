//! SASL OAUTHBEARER (RFC 7628) with Kafka's unsecured JWT (`alg=none`).
//! Matches librdkafka's builtin `enable.sasl.oauthbearer.unsecure.jwt` token.

use std::time::{SystemTime, UNIX_EPOCH};

use crate::error::{Error, Result};

const HEADER_B64: &str = "eyJhbGciOiJub25lIn0"; // {"alg":"none"}
const KVSEP: u8 = 0x01;
const LIFE_SECONDS: f64 = 3600.0;

fn b64url(bytes: &[u8]) -> String {
    base64::Engine::encode(&base64::engine::general_purpose::URL_SAFE_NO_PAD, bytes)
}

fn b64url_decode(s: &str) -> Result<Vec<u8>> {
    base64::Engine::decode(&base64::engine::general_purpose::URL_SAFE_NO_PAD, s)
        .map_err(|e| Error::protocol(format!("oauth base64: {e}")))
}

fn json_escape(s: &str) -> String {
    s.replace('\\', "\\\\").replace('"', "\\\"")
}

/// Unsecured JWS compact serialization. `iat`/`exp` use three decimal places,
/// matching librdkafka `%.3f` NumericDate.
pub fn unsecured_jwt(principal: &str, iat: f64, life_seconds: f64) -> String {
    let exp = iat + life_seconds;
    let claims = format!(
        "{{\"sub\":\"{}\",\"iat\":{:.3},\"exp\":{:.3}}}",
        json_escape(principal),
        iat,
        exp
    );
    format!("{HEADER_B64}.{}.", b64url(claims.as_bytes()))
}

/// [`unsecured_jwt`] with `iat` truncated to whole Unix seconds and a 1h lifetime.
pub fn unsecured_jwt_now(principal: &str) -> String {
    // Whole seconds, truncated. `{:.3f}` rounding of as_secs_f64() can put
    // `iat` 1ms in the future; Kafka's unsecured validator has 0ms skew.
    let iat = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs() as f64)
        .unwrap_or(0.0);
    unsecured_jwt(principal, iat, LIFE_SECONDS)
}

/// RFC 7628 client-first: `n,,` SOH `auth=Bearer <token>` SOH SOH
pub fn client_initial(token: &str) -> Vec<u8> {
    let mut out = Vec::with_capacity(16 + token.len());
    out.extend_from_slice(b"n,,");
    out.push(KVSEP);
    out.extend_from_slice(b"auth=Bearer ");
    out.extend_from_slice(token.as_bytes());
    out.push(KVSEP);
    out.push(KVSEP);
    out
}

/// Extract the Bearer token from an RFC 7628 client-first message.
pub fn token_from_initial(bytes: &[u8]) -> Result<String> {
    let s = std::str::from_utf8(bytes).map_err(|_| Error::protocol("oauth initial not utf8"))?;
    let rest = s
        .strip_prefix("n,,")
        .ok_or_else(|| Error::protocol("oauth gs2"))?;
    if !rest.starts_with('\u{1}') {
        return Err(Error::protocol("oauth missing kvsep"));
    }
    let body = &rest[1..];
    for part in body.split('\u{1}') {
        if let Some(token) = part.strip_prefix("auth=Bearer ") {
            if token.is_empty() {
                return Err(Error::protocol("oauth empty token"));
            }
            return Ok(token.to_string());
        }
    }
    Err(Error::protocol("oauth missing auth=Bearer"))
}

/// Read `sub` from an unsecured (`alg=none`) compact JWS. Signature must be empty.
pub fn principal_from_jwt(token: &str) -> Result<String> {
    let mut parts = token.split('.');
    let header = parts
        .next()
        .ok_or_else(|| Error::protocol("oauth jwt header"))?;
    let payload = parts
        .next()
        .ok_or_else(|| Error::protocol("oauth jwt payload"))?;
    let sig = parts
        .next()
        .ok_or_else(|| Error::protocol("oauth jwt signature"))?;
    if parts.next().is_some() {
        return Err(Error::protocol("oauth jwt extra"));
    }
    if !sig.is_empty() {
        return Err(Error::protocol("oauth jwt must have empty signature"));
    }
    let header_json = String::from_utf8(b64url_decode(header)?)
        .map_err(|_| Error::protocol("oauth header not utf8"))?;
    if !header_json.contains("\"alg\":\"none\"") {
        return Err(Error::protocol("oauth alg is not none"));
    }
    let claims = String::from_utf8(b64url_decode(payload)?)
        .map_err(|_| Error::protocol("oauth claims not utf8"))?;
    json_string_field(&claims, "sub")
}

fn json_string_field(json: &str, key: &str) -> Result<String> {
    let needle = format!("\"{key}\":\"");
    let rest = json
        .split_once(&needle)
        .map(|(_, r)| r)
        .ok_or_else(|| Error::protocol(format!("oauth missing {key}")))?;
    let val = rest
        .split('"')
        .next()
        .ok_or_else(|| Error::protocol(format!("oauth truncated {key}")))?;
    Ok(val.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// librdkafka `do_unittest_config_defaults` token for principal=fubar at t=1s.
    #[test]
    fn librdkafka_unsecured_jwt_vector() {
        let token = unsecured_jwt("fubar", 1.0, 3600.0);
        assert_eq!(
            token,
            "eyJhbGciOiJub25lIn0.eyJzdWIiOiJmdWJhciIsImlhdCI6MS4wMDAsImV4cCI6MzYwMS4wMDB9."
        );
        assert_eq!(principal_from_jwt(&token).unwrap(), "fubar");
    }

    #[test]
    fn rfc7628_initial_roundtrip() {
        let token = unsecured_jwt("alice", 10.0, 3600.0);
        let init = client_initial(&token);
        assert_eq!(token_from_initial(&init).unwrap(), token);
        assert_eq!(principal_from_jwt(&token).unwrap(), "alice");
        assert!(init.starts_with(b"n,,\x01auth=Bearer "));
        assert!(init.ends_with(b"\x01\x01"));
    }
}

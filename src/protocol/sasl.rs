//! SaslHandshake (api 17, v0–v1) and SaslAuthenticate (api 36, v0–v2),
//! plus PLAIN / SCRAM / OAUTHBEARER helpers.

use std::time::Duration;

use bytes::{Buf, BufMut, BytesMut};

use super::buf;
use crate::error::{self, Error, Result};
use crate::net::BrokerConn;
use crate::protocol::api::ApiVersion;
use crate::protocol::api_keys::{pick_version, SASL_AUTHENTICATE, SASL_HANDSHAKE};

/// `true` when SaslHandshake `version` is flexible.
///
/// Official Kafka 4.0 JSON: `validVersions: "0-1"`, `flexibleVersions: "none"`.
/// v0 and v1 have the same fields. v1 exists so the client can then use
/// SaslAuthenticate. Version cannot be easily bumped (KAFKA-9577).
fn sasl_handshake_flexible(version: i16) -> Result<bool> {
    match version {
        0..=1 => Ok(false),
        other => Err(Error::protocol(format!(
            "SaslHandshake version {other} is not implemented"
        ))),
    }
}

/// `true` when SaslAuthenticate `version` is flexible (v2).
///
/// Official Kafka 4.0 JSON: `validVersions: "0-2"`, `flexibleVersions: "2+"`.
/// v0 and v1 request match (AuthBytes). v1+ SessionLifetimeMs.
fn sasl_authenticate_flexible(version: i16) -> Result<bool> {
    match version {
        0..=1 => Ok(false),
        2 => Ok(true),
        other => Err(Error::protocol(format!(
            "SaslAuthenticate version {other} is not implemented"
        ))),
    }
}

/// Pick SaslHandshake (0–1) and SaslAuthenticate (0–2) from ApiVersions.
///
/// `-1` on [`BrokerConn`] means unset because `0` is a spoken version.
pub fn apply_api_keys(conn: &mut BrokerConn, keys: &[ApiVersion]) {
    conn.sasl_handshake_version = keys
        .iter()
        .find(|k| k.api_key == SASL_HANDSHAKE)
        .and_then(|v| pick_version(v.min_version, v.max_version, 0, 1))
        .unwrap_or(-1);
    conn.sasl_authenticate_version = keys
        .iter()
        .find(|k| k.api_key == SASL_AUTHENTICATE)
        .and_then(|v| pick_version(v.min_version, v.max_version, 0, 2))
        .unwrap_or(-1);
}

fn spoken_sasl_versions(conn: &BrokerConn) -> Result<(i16, i16)> {
    let handshake = match conn.sasl_handshake_version {
        0..=1 => conn.sasl_handshake_version,
        _ => {
            return Err(Error::Unsupported(
                "broker does not support SaslHandshake v0-1".into(),
            ))
        }
    };
    let authenticate = match conn.sasl_authenticate_version {
        0..=2 => conn.sasl_authenticate_version,
        _ => {
            return Err(Error::Unsupported(
                "broker does not support SaslAuthenticate v0-2".into(),
            ))
        }
    };
    Ok((handshake, authenticate))
}

/// Encode SaslHandshake v0–v1 with the requested mechanism name.
///
/// Kafka 4.0 JSON: `validVersions: "0-1"`, `flexibleVersions: "none"`.
/// This crate speaks 0–1. v2+ is not spoken.
pub fn encode_sasl_handshake_request(
    buf: &mut BytesMut,
    version: i16,
    mechanism: &str,
) -> crate::error::Result<()> {
    let flexible = sasl_handshake_flexible(version)?;
    buf::put_string(buf, flexible, Some(mechanism))?;
    Ok(())
}

/// Decode SaslHandshake v0–v1: mechanism name.
pub fn decode_sasl_handshake_request<B: Buf>(buf: &mut B, version: i16) -> Result<String> {
    let flexible = sasl_handshake_flexible(version)?;
    Ok(buf::get_string(buf, flexible)?.unwrap_or_default())
}

/// Encode SaslHandshake v0–v1: error code plus enabled mechanism names.
pub fn encode_sasl_handshake_response(
    buf: &mut BytesMut,
    version: i16,
    error_code: i16,
    mechanisms: &[&str],
) -> crate::error::Result<()> {
    let flexible = sasl_handshake_flexible(version)?;
    buf.put_i16(error_code);
    buf::put_array_len(buf, flexible, Some(mechanisms.len()))?;
    for m in mechanisms {
        buf::put_string(buf, flexible, Some(m))?;
    }
    Ok(())
}

/// Decode SaslHandshake v0–v1: `(error_code, mechanisms)`.
pub fn decode_sasl_handshake_response<B: Buf>(
    buf: &mut B,
    version: i16,
) -> Result<(i16, Vec<String>)> {
    let flexible = sasl_handshake_flexible(version)?;
    let error_code = buf::get_i16(buf)?;
    let n = buf::get_array_len(buf, flexible)?.unwrap_or(0);
    let mut mechs = Vec::with_capacity(n);
    for _ in 0..n {
        mechs.push(buf::get_string(buf, flexible)?.unwrap_or_default());
    }
    Ok((error_code, mechs))
}

/// Java `SaslHandshakeRequest` helpers.
pub struct SaslHandshakeRequest;

impl SaslHandshakeRequest {
    /// Java `SaslHandshakeRequest.getErrorResponse`.
    ///
    /// Mechanisms stay empty (the requested mechanism is not copied). v0
    /// and v1 bodies match (`flexibleVersions: "none"`).
    pub fn error_response(
        buf: &mut BytesMut,
        version: i16,
        error_code: i16,
    ) -> crate::error::Result<()> {
        encode_sasl_handshake_response(buf, version, error_code, &[])
    }
}

/// Encode SaslAuthenticate v0–v2 with the SASL client bytes.
///
/// Kafka 4.0 JSON: `validVersions: "0-2"`, `flexibleVersions: "2+"`.
/// This crate speaks 0–2. v3+ is not spoken.
pub fn encode_sasl_authenticate_request(
    buf: &mut BytesMut,
    version: i16,
    auth_bytes: &[u8],
) -> crate::error::Result<()> {
    let flexible = sasl_authenticate_flexible(version)?;
    buf::put_bytes(buf, flexible, Some(auth_bytes))?;
    if flexible {
        buf::put_empty_tagged_fields(buf);
    }
    Ok(())
}

/// Decode SaslAuthenticate v0–v2: client/server SASL bytes.
pub fn decode_sasl_authenticate_request<B: Buf>(buf: &mut B, version: i16) -> Result<Vec<u8>> {
    let flexible = sasl_authenticate_flexible(version)?;
    let bytes = buf::get_bytes(buf, flexible)?.unwrap_or_default();
    if flexible {
        buf::skip_tagged_fields(buf)?;
    }
    Ok(bytes)
}

/// Encode SaslAuthenticate v0–v2: error, optional message, SASL bytes.
/// SessionLifetimeMs is `0` on v1+. v2 is flexible.
pub fn encode_sasl_authenticate_response(
    buf: &mut BytesMut,
    version: i16,
    error_code: i16,
    message: Option<&str>,
    auth_bytes: &[u8],
) -> crate::error::Result<()> {
    let flexible = sasl_authenticate_flexible(version)?;
    buf.put_i16(error_code);
    buf::put_string(buf, flexible, message)?;
    buf::put_bytes(buf, flexible, Some(auth_bytes))?;
    if version >= 1 {
        buf.put_i64(0);
    }
    if flexible {
        buf::put_empty_tagged_fields(buf);
    }
    Ok(())
}

/// Decode SaslAuthenticate v0–v2: `(error_code, error_message, auth_bytes)`.
/// SessionLifetimeMs is read on v1+ and discarded.
pub fn decode_sasl_authenticate_response<B: Buf>(
    buf: &mut B,
    version: i16,
) -> Result<(i16, Option<String>, Vec<u8>)> {
    let flexible = sasl_authenticate_flexible(version)?;
    let error_code = buf::get_i16(buf)?;
    let message = buf::get_string(buf, flexible)?;
    let bytes = buf::get_bytes(buf, flexible)?.unwrap_or_default();
    if version >= 1 {
        let _lifetime = buf::get_i64(buf)?;
    }
    if flexible {
        buf::skip_tagged_fields(buf)?;
    }
    Ok((error_code, message, bytes))
}

/// RFC 4616 PLAIN: `NUL authcid NUL passwd`.
pub fn plain_auth_bytes(user: &str, pass: &str) -> Vec<u8> {
    let mut out = Vec::with_capacity(user.len() + pass.len() + 2);
    out.push(0);
    out.extend_from_slice(user.as_bytes());
    out.push(0);
    out.extend_from_slice(pass.as_bytes());
    out
}

/// Parse RFC 4616 PLAIN client bytes into `(authcid, passwd)`.
pub fn parse_plain_auth_bytes(bytes: &[u8]) -> Option<(String, String)> {
    // RFC 4616: [authzid] NUL authcid NUL passwd. Clients send NUL user NUL pass.
    let mut parts = bytes.split(|b| *b == 0);
    let _authzid = parts.next()?;
    let user = std::str::from_utf8(parts.next()?).ok()?;
    let pass = std::str::from_utf8(parts.next()?).ok()?;
    Some((user.to_string(), pass.to_string()))
}

/// SaslHandshake + SaslAuthenticate for PLAIN.
pub async fn authenticate_plain(
    conn: &mut BrokerConn,
    user: &str,
    pass: &str,
    timeout: Duration,
) -> Result<()> {
    let (hs_version, auth_version) = spoken_sasl_versions(conn)?;
    let hs = conn
        .roundtrip_sasl(
            SASL_HANDSHAKE,
            hs_version,
            |buf| encode_sasl_handshake_request(buf, hs_version, "PLAIN"),
            timeout,
        )
        .await?;
    let (code, mechs) = decode_sasl_handshake_response(&mut hs.clone(), hs_version)?;
    if code != 0 {
        return Err(Error::broker(code, "SaslHandshake"));
    }
    if !mechs.iter().any(|m| m == "PLAIN") {
        return Err(Error::Unsupported(format!(
            "PLAIN not in mechanisms {mechs:?}"
        )));
    }
    let auth = plain_auth_bytes(user, pass);
    let body = conn
        .roundtrip_sasl(
            SASL_AUTHENTICATE,
            auth_version,
            |buf| encode_sasl_authenticate_request(buf, auth_version, &auth),
            timeout,
        )
        .await?;
    let (code, msg, _) = decode_sasl_authenticate_response(&mut body.clone(), auth_version)?;
    if code != 0 {
        return Err(Error::broker(
            if code == 0 {
                error::SASL_AUTHENTICATION_FAILED
            } else {
                code
            },
            msg.unwrap_or_else(|| "SaslAuthenticate".into()),
        ));
    }
    Ok(())
}

/// SaslHandshake + RFC 5802 client/server messages for SCRAM-SHA-256 or SHA-512.
pub async fn authenticate_scram(
    conn: &mut BrokerConn,
    alg: super::scram::ScramAlg,
    user: &str,
    pass: &str,
    timeout: Duration,
) -> Result<()> {
    let (hs_version, auth_version) = spoken_sasl_versions(conn)?;
    let name = alg.name();
    let hs = conn
        .roundtrip_sasl(
            SASL_HANDSHAKE,
            hs_version,
            |buf| encode_sasl_handshake_request(buf, hs_version, name),
            timeout,
        )
        .await?;
    let (code, mechs) = decode_sasl_handshake_response(&mut hs.clone(), hs_version)?;
    if code != 0 {
        return Err(Error::broker(code, "SaslHandshake"));
    }
    if !mechs.iter().any(|m| m == name) {
        return Err(Error::Unsupported(format!(
            "{name} not in mechanisms {mechs:?}"
        )));
    }
    let nonce = super::scram::client_nonce();
    let (first, bare) = super::scram::client_first(user, &nonce);
    let body = conn
        .roundtrip_sasl(
            SASL_AUTHENTICATE,
            auth_version,
            |buf| encode_sasl_authenticate_request(buf, auth_version, first.as_bytes()),
            timeout,
        )
        .await?;
    let (code, msg, bytes) = decode_sasl_authenticate_response(&mut body.clone(), auth_version)?;
    if code != 0 {
        return Err(Error::broker(
            code,
            msg.unwrap_or_else(|| "SaslAuthenticate".into()),
        ));
    }
    let server_first =
        String::from_utf8(bytes).map_err(|_| Error::protocol("scram server-first not utf8"))?;
    let client_final = super::scram::client_final(alg, pass, &bare, &server_first)?;
    let body = conn
        .roundtrip_sasl(
            SASL_AUTHENTICATE,
            auth_version,
            |buf| encode_sasl_authenticate_request(buf, auth_version, client_final.as_bytes()),
            timeout,
        )
        .await?;
    let (code, msg, bytes) = decode_sasl_authenticate_response(&mut body.clone(), auth_version)?;
    if code != 0 {
        return Err(Error::broker(
            code,
            msg.unwrap_or_else(|| "SaslAuthenticate".into()),
        ));
    }
    let server_final =
        String::from_utf8(bytes).map_err(|_| Error::protocol("scram server-final not utf8"))?;
    super::scram::verify_server_final(
        alg,
        pass,
        &bare,
        &server_first,
        &client_final,
        &server_final,
    )
}

/// [`authenticate_scram`] with [`super::scram::ScramAlg::Sha256`].
pub async fn authenticate_scram_sha256(
    conn: &mut BrokerConn,
    user: &str,
    pass: &str,
    timeout: Duration,
) -> Result<()> {
    authenticate_scram(conn, super::scram::ScramAlg::Sha256, user, pass, timeout).await
}

/// OAUTHBEARER with an unsecured JWT for `principal` (librdkafka unsecure jwt).
pub async fn authenticate_oauthbearer(
    conn: &mut BrokerConn,
    principal: &str,
    timeout: Duration,
) -> Result<()> {
    let token = super::oauth::unsecured_jwt_now(principal);
    authenticate_oauthbearer_token(conn, &token, timeout).await
}

/// OAUTHBEARER with a caller-supplied access token (OIDC or unsecured JWT).
pub async fn authenticate_oauthbearer_token(
    conn: &mut BrokerConn,
    token: &str,
    timeout: Duration,
) -> Result<()> {
    let (hs_version, auth_version) = spoken_sasl_versions(conn)?;
    let hs = conn
        .roundtrip_sasl(
            SASL_HANDSHAKE,
            hs_version,
            |buf| encode_sasl_handshake_request(buf, hs_version, "OAUTHBEARER"),
            timeout,
        )
        .await?;
    let (code, mechs) = decode_sasl_handshake_response(&mut hs.clone(), hs_version)?;
    if code != 0 {
        return Err(Error::broker(code, "SaslHandshake"));
    }
    if !mechs.iter().any(|m| m == "OAUTHBEARER") {
        return Err(Error::Unsupported(format!(
            "OAUTHBEARER not in mechanisms {mechs:?}"
        )));
    }
    let auth = super::oauth::client_initial(token);
    let body = conn
        .roundtrip_sasl(
            SASL_AUTHENTICATE,
            auth_version,
            |buf| encode_sasl_authenticate_request(buf, auth_version, &auth),
            timeout,
        )
        .await?;
    let (code, msg, bytes) = decode_sasl_authenticate_response(&mut body.clone(), auth_version)?;
    if code != 0 {
        return Err(Error::broker(
            code,
            msg.unwrap_or_else(|| "SaslAuthenticate".into()),
        ));
    }
    // RFC 7628 / librdkafka: empty server-first = success. Non-empty is an
    // error JSON; send a final SOH then fail.
    if !bytes.is_empty() {
        let err = String::from_utf8_lossy(&bytes).into_owned();
        drop(
            conn.roundtrip_sasl(
                SASL_AUTHENTICATE,
                auth_version,
                |buf| encode_sasl_authenticate_request(buf, auth_version, &[0x01]),
                timeout,
            )
            .await,
        );
        return Err(Error::protocol(format!("oauthbearer: {err}")));
    }
    Ok(())
}

/// Run the one configured SASL mechanism, or return immediately when none is set.
pub async fn authenticate(
    conn: &mut BrokerConn,
    sasl_plain: Option<&(String, String)>,
    sasl_scram: Option<&(String, String)>,
    sasl_scram_sha512: Option<&(String, String)>,
    sasl_oauthbearer: Option<&str>,
    sasl_oidc: Option<&super::oidc::OidcConfig>,
    timeout: Duration,
) -> Result<()> {
    let n = [
        sasl_plain.is_some(),
        sasl_scram.is_some(),
        sasl_scram_sha512.is_some(),
        sasl_oauthbearer.is_some(),
        sasl_oidc.is_some(),
    ]
    .into_iter()
    .filter(|x| *x)
    .count();
    if n > 1 {
        return Err(Error::protocol(
            "set only one of sasl_plain, sasl_scram, sasl_scram_sha512, sasl_oauthbearer, sasl_oauthbearer_oidc",
        ));
    }
    if let Some((u, p)) = sasl_plain {
        return authenticate_plain(conn, u, p, timeout).await;
    }
    if let Some((u, p)) = sasl_scram {
        return authenticate_scram(conn, super::scram::ScramAlg::Sha256, u, p, timeout).await;
    }
    if let Some((u, p)) = sasl_scram_sha512 {
        return authenticate_scram(conn, super::scram::ScramAlg::Sha512, u, p, timeout).await;
    }
    if let Some(oidc) = sasl_oidc {
        let token = super::oidc::fetch_client_credentials_token(oidc, timeout).await?;
        return authenticate_oauthbearer_token(conn, &token, timeout).await;
    }
    if let Some(principal) = sasl_oauthbearer {
        return authenticate_oauthbearer(conn, principal, timeout).await;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Buf;

    #[test]
    fn plain_bytes_roundtrip() {
        let b = plain_auth_bytes("alice", "secret");
        assert_eq!(
            parse_plain_auth_bytes(&b),
            Some(("alice".into(), "secret".into()))
        );
    }

    #[test]
    fn handshake_roundtrip() {
        let mut buf = BytesMut::new();
        encode_sasl_handshake_request(&mut buf, 1, "PLAIN").unwrap();
        assert_eq!(
            decode_sasl_handshake_request(&mut &buf[..], 1).unwrap(),
            "PLAIN"
        );
        let mut resp = BytesMut::new();
        encode_sasl_handshake_response(&mut resp, 1, 0, &["PLAIN"]).unwrap();
        let (c, m) = decode_sasl_handshake_response(&mut &resp[..], 1).unwrap();
        assert_eq!(c, 0);
        assert_eq!(m, vec!["PLAIN".to_string()]);
    }

    #[test]
    fn sasl_handshake_error_response_matches_java() {
        // Java SaslHandshakeRequest.getErrorResponse: ErrorCode only.
        // Mechanisms stay empty (the requested mechanism is not copied).
        // v0 and v1 response bodies match (never flexible).
        for version in [0_i16, 1] {
            let mut expected = BytesMut::new();
            encode_sasl_handshake_response(&mut expected, version, 16, &[]).unwrap();
            let mut got = BytesMut::new();
            SaslHandshakeRequest::error_response(&mut got, version, 16).unwrap();
            assert_eq!(
                &got[..],
                &expected[..],
                "SaslHandshake v{version} getErrorResponse must match empty-Mechanisms encode"
            );
            let mut cur = &got[..];
            let (err, mechs) = decode_sasl_handshake_response(&mut cur, version).unwrap();
            assert_eq!(err, 16);
            assert!(mechs.is_empty(), "v{version} Mechanisms must be empty");
            assert!(
                cur.is_empty(),
                "SaslHandshake v{version} getErrorResponse leftover-empty; leftover {} bytes",
                cur.len()
            );
        }
        let mut v0 = BytesMut::new();
        SaslHandshakeRequest::error_response(&mut v0, 0, 16).unwrap();
        let mut v1 = BytesMut::new();
        SaslHandshakeRequest::error_response(&mut v1, 1, 16).unwrap();
        assert_eq!(&v0[..], &v1[..], "v0 and v1 getErrorResponse bodies match");
        let mut with_plain = BytesMut::new();
        encode_sasl_handshake_response(&mut with_plain, 1, 16, &["PLAIN"]).unwrap();
        assert_ne!(
            &v1[..],
            &with_plain[..],
            "getErrorResponse must not copy the requested mechanism"
        );
    }

    #[test]
    fn sasl_handshake_v0_matches_v1_and_does_not_speak_v2() {
        // Official Kafka 4.0 JSON: validVersions 0-1, flexibleVersions none.
        // "Version 1 is the same as version 0" plus SaslAuthenticate support.
        // This crate speaks 0–1. v2+ is not spoken.
        let mut v0 = BytesMut::new();
        encode_sasl_handshake_request(&mut v0, 0, "PLAIN").unwrap();
        let mut v1 = BytesMut::new();
        encode_sasl_handshake_request(&mut v1, 1, "PLAIN").unwrap();
        assert_eq!(v0.as_ref(), v1.as_ref(), "v0 and v1 request bodies match");
        let mut cur = v0.as_ref();
        assert_eq!(decode_sasl_handshake_request(&mut cur, 0).unwrap(), "PLAIN");
        assert!(!cur.has_remaining(), "v0 request leftover-empty");
        let mut cur = v1.as_ref();
        assert_eq!(decode_sasl_handshake_request(&mut cur, 1).unwrap(), "PLAIN");
        assert!(!cur.has_remaining(), "v1 request leftover-empty");
        let err = encode_sasl_handshake_request(&mut BytesMut::new(), 2, "PLAIN").unwrap_err();
        assert!(
            err.to_string().contains("not implemented"),
            "v2 is not spoken, got {err}"
        );
        let mut empty: &[u8] = &[];
        let err = decode_sasl_handshake_request(&mut empty, 2).unwrap_err();
        assert!(
            err.to_string().contains("not implemented"),
            "v2 decode is not spoken, got {err}"
        );
        assert_eq!(crate::protocol::api_keys::pick_version(0, 0, 0, 1), Some(0));
        assert_eq!(crate::protocol::api_keys::pick_version(0, 1, 0, 1), Some(1));
        assert_eq!(crate::protocol::api_keys::pick_version(2, 2, 0, 1), None);

        v0.clear();
        encode_sasl_handshake_response(&mut v0, 0, 0, &["PLAIN"]).unwrap();
        v1.clear();
        encode_sasl_handshake_response(&mut v1, 1, 0, &["PLAIN"]).unwrap();
        assert_eq!(v0.as_ref(), v1.as_ref(), "v0 and v1 response bodies match");
        let mut cur = v0.as_ref();
        let (c, m) = decode_sasl_handshake_response(&mut cur, 0).unwrap();
        assert_eq!(c, 0);
        assert_eq!(m, vec!["PLAIN".to_string()]);
        assert!(!cur.has_remaining(), "v0 response leftover-empty");
        v0.clear();
        let err = encode_sasl_handshake_response(&mut v0, 2, 0, &["PLAIN"]).unwrap_err();
        assert!(
            err.to_string().contains("not implemented"),
            "v2 response is not spoken, got {err}"
        );
    }

    #[test]
    fn sasl_authenticate_v0_v1_v2_and_does_not_speak_v3() {
        // Official Kafka 4.0 JSON: validVersions 0-2, flexibleVersions 2+.
        // v0 and v1 request match (AuthBytes). v1+ SessionLifetimeMs.
        // v2 is compact bytes plus tagged fields. This crate speaks 0–2.
        let auth = b"token";
        let mut v0 = BytesMut::new();
        encode_sasl_authenticate_request(&mut v0, 0, auth).unwrap();
        let mut v1 = BytesMut::new();
        encode_sasl_authenticate_request(&mut v1, 1, auth).unwrap();
        let mut v2 = BytesMut::new();
        encode_sasl_authenticate_request(&mut v2, 2, auth).unwrap();
        assert_eq!(v0.as_ref(), v1.as_ref(), "v0 and v1 request bodies match");
        assert_ne!(
            v1.as_ref(),
            v2.as_ref(),
            "v2 request uses compact AuthBytes"
        );
        let mut cur = v0.as_ref();
        assert_eq!(decode_sasl_authenticate_request(&mut cur, 0).unwrap(), auth);
        assert!(!cur.has_remaining(), "v0 request leftover-empty");
        let mut cur = v1.as_ref();
        assert_eq!(decode_sasl_authenticate_request(&mut cur, 1).unwrap(), auth);
        assert!(!cur.has_remaining(), "v1 request leftover-empty");
        let mut cur = v2.as_ref();
        assert_eq!(decode_sasl_authenticate_request(&mut cur, 2).unwrap(), auth);
        assert!(!cur.has_remaining(), "v2 request leftover-empty");
        let err = encode_sasl_authenticate_request(&mut BytesMut::new(), 3, auth).unwrap_err();
        assert!(
            err.to_string().contains("not implemented"),
            "v3 is not spoken, got {err}"
        );
        let mut empty: &[u8] = &[];
        let err = decode_sasl_authenticate_request(&mut empty, 3).unwrap_err();
        assert!(
            err.to_string().contains("not implemented"),
            "v3 decode is not spoken, got {err}"
        );
        assert_eq!(crate::protocol::api_keys::pick_version(0, 0, 0, 2), Some(0));
        assert_eq!(crate::protocol::api_keys::pick_version(0, 1, 0, 2), Some(1));
        assert_eq!(crate::protocol::api_keys::pick_version(0, 2, 0, 2), Some(2));
        assert_eq!(crate::protocol::api_keys::pick_version(3, 3, 0, 2), None);

        v0.clear();
        encode_sasl_authenticate_response(&mut v0, 0, 0, None, auth).unwrap();
        v1.clear();
        encode_sasl_authenticate_response(&mut v1, 1, 0, None, auth).unwrap();
        v2.clear();
        encode_sasl_authenticate_response(&mut v2, 2, 0, None, auth).unwrap();
        assert_ne!(
            v0.as_ref(),
            v1.as_ref(),
            "v1 response adds SessionLifetimeMs"
        );
        assert_ne!(
            v1.as_ref(),
            v2.as_ref(),
            "v2 response uses compact strings/bytes"
        );
        let mut cur = v0.as_ref();
        let (c, msg, bytes) = decode_sasl_authenticate_response(&mut cur, 0).unwrap();
        assert_eq!(c, 0);
        assert_eq!(msg, None);
        assert_eq!(bytes, auth);
        assert!(!cur.has_remaining(), "v0 response leftover-empty");
        let mut cur = v1.as_ref();
        let (c, _, bytes) = decode_sasl_authenticate_response(&mut cur, 1).unwrap();
        assert_eq!(c, 0);
        assert_eq!(bytes, auth);
        assert!(!cur.has_remaining(), "v1 response leftover-empty");
        let mut cur = v2.as_ref();
        let (c, _, bytes) = decode_sasl_authenticate_response(&mut cur, 2).unwrap();
        assert_eq!(c, 0);
        assert_eq!(bytes, auth);
        assert!(!cur.has_remaining(), "v2 response leftover-empty");
        v0.clear();
        let err = encode_sasl_authenticate_response(&mut v0, 3, 0, None, auth).unwrap_err();
        assert!(
            err.to_string().contains("not implemented"),
            "v3 response is not spoken, got {err}"
        );
    }
}

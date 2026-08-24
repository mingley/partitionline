use std::time::Duration;

use bytes::{Buf, BufMut, BytesMut};

use super::buf;
use crate::error::{self, Error, Result};
use crate::net::BrokerConn;
use crate::protocol::api_keys::{SASL_AUTHENTICATE, SASL_HANDSHAKE};

pub fn encode_sasl_handshake_request(buf: &mut BytesMut, mechanism: &str) {
    buf::put_classic_nullable_string(buf, Some(mechanism));
}

pub fn decode_sasl_handshake_request<B: Buf>(buf: &mut B) -> Result<String> {
    Ok(buf::get_classic_nullable_string(buf)?.unwrap_or_default())
}

pub fn encode_sasl_handshake_response(buf: &mut BytesMut, error_code: i16, mechanisms: &[&str]) {
    buf.put_i16(error_code);
    buf::put_array_len(buf, false, Some(mechanisms.len()));
    for m in mechanisms {
        buf::put_classic_nullable_string(buf, Some(m));
    }
}

pub fn decode_sasl_handshake_response<B: Buf>(buf: &mut B) -> Result<(i16, Vec<String>)> {
    let error_code = buf::get_i16(buf)?;
    let n = buf::get_array_len(buf, false)?.unwrap_or(0);
    let mut mechs = Vec::with_capacity(n);
    for _ in 0..n {
        mechs.push(buf::get_classic_nullable_string(buf)?.unwrap_or_default());
    }
    Ok((error_code, mechs))
}

pub fn encode_sasl_authenticate_request(buf: &mut BytesMut, auth_bytes: &[u8]) {
    buf::put_classic_bytes(buf, Some(auth_bytes));
}

pub fn decode_sasl_authenticate_request<B: Buf>(buf: &mut B) -> Result<Vec<u8>> {
    Ok(buf::get_classic_bytes(buf)?.unwrap_or_default())
}

pub fn encode_sasl_authenticate_response(
    buf: &mut BytesMut,
    error_code: i16,
    message: Option<&str>,
    auth_bytes: &[u8],
) {
    buf.put_i16(error_code);
    buf::put_classic_nullable_string(buf, message);
    buf::put_classic_bytes(buf, Some(auth_bytes));
    buf.put_i64(0);
}

pub fn decode_sasl_authenticate_response<B: Buf>(
    buf: &mut B,
) -> Result<(i16, Option<String>, Vec<u8>)> {
    let error_code = buf::get_i16(buf)?;
    let message = buf::get_classic_nullable_string(buf)?;
    let bytes = buf::get_classic_bytes(buf)?.unwrap_or_default();
    if buf.remaining() >= 8 {
        let _lifetime = buf::get_i64(buf)?;
    }
    Ok((error_code, message, bytes.to_vec()))
}

pub fn plain_auth_bytes(user: &str, pass: &str) -> Vec<u8> {
    let mut out = Vec::with_capacity(user.len() + pass.len() + 2);
    out.push(0);
    out.extend_from_slice(user.as_bytes());
    out.push(0);
    out.extend_from_slice(pass.as_bytes());
    out
}

pub fn parse_plain_auth_bytes(bytes: &[u8]) -> Option<(String, String)> {
    // RFC 4616: [authzid] NUL authcid NUL passwd. Clients send NUL user NUL pass.
    let mut parts = bytes.split(|b| *b == 0);
    let _authzid = parts.next()?;
    let user = std::str::from_utf8(parts.next()?).ok()?;
    let pass = std::str::from_utf8(parts.next()?).ok()?;
    Some((user.to_string(), pass.to_string()))
}

pub async fn authenticate_plain(
    conn: &mut BrokerConn,
    user: &str,
    pass: &str,
    timeout: Duration,
) -> Result<()> {
    let hs = conn
        .roundtrip(
            SASL_HANDSHAKE,
            1,
            |buf| encode_sasl_handshake_request(buf, "PLAIN"),
            timeout,
        )
        .await?;
    let (code, mechs) = decode_sasl_handshake_response(&mut hs.clone())?;
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
        .roundtrip(
            SASL_AUTHENTICATE,
            1,
            |buf| encode_sasl_authenticate_request(buf, &auth),
            timeout,
        )
        .await?;
    let (code, msg, _) = decode_sasl_authenticate_response(&mut body.clone())?;
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

pub async fn authenticate_scram(
    conn: &mut BrokerConn,
    alg: super::scram::ScramAlg,
    user: &str,
    pass: &str,
    timeout: Duration,
) -> Result<()> {
    let name = alg.name();
    let hs = conn
        .roundtrip(
            SASL_HANDSHAKE,
            1,
            |buf| encode_sasl_handshake_request(buf, name),
            timeout,
        )
        .await?;
    let (code, mechs) = decode_sasl_handshake_response(&mut hs.clone())?;
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
        .roundtrip(
            SASL_AUTHENTICATE,
            1,
            |buf| encode_sasl_authenticate_request(buf, first.as_bytes()),
            timeout,
        )
        .await?;
    let (code, msg, bytes) = decode_sasl_authenticate_response(&mut body.clone())?;
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
        .roundtrip(
            SASL_AUTHENTICATE,
            1,
            |buf| encode_sasl_authenticate_request(buf, client_final.as_bytes()),
            timeout,
        )
        .await?;
    let (code, msg, bytes) = decode_sasl_authenticate_response(&mut body.clone())?;
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

pub async fn authenticate_scram_sha256(
    conn: &mut BrokerConn,
    user: &str,
    pass: &str,
    timeout: Duration,
) -> Result<()> {
    authenticate_scram(conn, super::scram::ScramAlg::Sha256, user, pass, timeout).await
}

pub async fn authenticate(
    conn: &mut BrokerConn,
    sasl_plain: Option<&(String, String)>,
    sasl_scram: Option<&(String, String)>,
    sasl_scram_sha512: Option<&(String, String)>,
    timeout: Duration,
) -> Result<()> {
    match (sasl_plain, sasl_scram, sasl_scram_sha512) {
        (Some(_), Some(_), _) | (Some(_), _, Some(_)) | (_, Some(_), Some(_)) => Err(
            Error::protocol("set only one of sasl_plain, sasl_scram, sasl_scram_sha512"),
        ),
        (Some((u, p)), None, None) => authenticate_plain(conn, u, p, timeout).await,
        (None, Some((u, p)), None) => {
            authenticate_scram(conn, super::scram::ScramAlg::Sha256, u, p, timeout).await
        }
        (None, None, Some((u, p))) => {
            authenticate_scram(conn, super::scram::ScramAlg::Sha512, u, p, timeout).await
        }
        (None, None, None) => Ok(()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
        encode_sasl_handshake_request(&mut buf, "PLAIN");
        assert_eq!(
            decode_sasl_handshake_request(&mut &buf[..]).unwrap(),
            "PLAIN"
        );
        let mut resp = BytesMut::new();
        encode_sasl_handshake_response(&mut resp, 0, &["PLAIN"]);
        let (c, m) = decode_sasl_handshake_response(&mut &resp[..]).unwrap();
        assert_eq!(c, 0);
        assert_eq!(m, vec!["PLAIN".to_string()]);
    }
}

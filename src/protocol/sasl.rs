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
) {
    buf.put_i16(error_code);
    buf::put_classic_nullable_string(buf, message);
    buf::put_classic_bytes(buf, Some(&[]));
    buf.put_i64(0);
}

pub fn decode_sasl_authenticate_response<B: Buf>(buf: &mut B) -> Result<(i16, Option<String>)> {
    let error_code = buf::get_i16(buf)?;
    let message = buf::get_classic_nullable_string(buf)?;
    let _bytes = buf::get_classic_bytes(buf)?;
    if buf.remaining() >= 8 {
        let _lifetime = buf::get_i64(buf)?;
    }
    Ok((error_code, message))
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
    let (code, msg) = decode_sasl_authenticate_response(&mut body.clone())?;
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

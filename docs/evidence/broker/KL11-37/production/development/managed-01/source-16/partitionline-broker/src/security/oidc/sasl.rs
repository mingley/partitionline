//! Bounded RFC 7628 client-first parsing; token bytes never select authority.

use super::{text, Error};
use crate::security::sasl::Secret;
use std::collections::HashSet;

pub(crate) struct Initial {
    pub(crate) token: Secret,
    pub(crate) authorization: Option<String>,
}
pub(crate) fn initial(message: &[u8]) -> Result<Initial, Error> {
    let value = std::str::from_utf8(message).map_err(|_| Error::Malformed)?;
    let (flag, remainder) = value.split_once(',').ok_or(Error::Malformed)?;
    if !matches!(flag, "n" | "y") {
        return Err(Error::Malformed);
    }
    let (authorization, fields) = remainder.split_once(',').ok_or(Error::Malformed)?;
    let authorization = if authorization.is_empty() {
        None
    } else {
        let encoded = authorization.strip_prefix("a=").ok_or(Error::Malformed)?;
        if encoded.len() > 3072 {
            return Err(Error::Malformed);
        }
        let mut decoded = String::with_capacity(encoded.len());
        let mut remaining = encoded;
        while let Some(index) = remaining.find('=') {
            decoded.push_str(&remaining[..index]);
            let escape = remaining.get(index..index + 3).ok_or(Error::Malformed)?;
            decoded.push(match escape {
                "=2C" => ',',
                "=3D" => '=',
                _ => return Err(Error::Malformed),
            });
            remaining = &remaining[index + 3..];
        }
        decoded.push_str(remaining);
        if !text(&decoded, 1024) {
            return Err(Error::Malformed);
        }
        Some(decoded)
    };
    let fields = fields.strip_prefix('\x01').ok_or(Error::Malformed)?;
    let fields = fields.strip_suffix("\x01\x01").ok_or(Error::Malformed)?;
    let mut names = HashSet::new();
    let mut token = None;
    let mut extension_bytes = 0usize;
    for field in fields.split('\x01') {
        let (name, value) = field.split_once('=').ok_or(Error::Malformed)?;
        if names.len() >= 16
            || name.is_empty()
            || name.len() > 64
            || !name.bytes().all(|c| c.is_ascii_alphabetic())
            || !names.insert(name)
            || !text(value, 32768)
        {
            return Err(Error::Malformed);
        }
        if name == "auth" {
            let value = value.strip_prefix("Bearer ").ok_or(Error::Malformed)?;
            if value.is_empty() || !value.bytes().all(|c| c.is_ascii_graphic()) {
                return Err(Error::Malformed);
            }
            token = Some(Secret::new(value.as_bytes().to_vec()));
        } else {
            extension_bytes = extension_bytes
                .checked_add(name.len() + value.len())
                .ok_or(Error::Malformed)?;
            if value.len() > 1024 || extension_bytes > 4096 {
                return Err(Error::Malformed);
            }
        }
    }
    Ok(Initial {
        token: token.ok_or(Error::Malformed)?,
        authorization,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn whole_bounded_rfc_initial_and_escaped_authorization() {
        for valid in [b"n,,\x01auth=Bearer synthetic.jwt.proof\x01\x01".as_slice(),
            b"y,a=user=2Cname=3Dok,\x01host=localhost\x01port=9093\x01auth=Bearer synthetic.jwt.proof\x01\x01"] {
            assert!(initial(valid).is_ok());
        }
        assert_eq!(
            initial(b"n,a=user=2Cname=3Dok,\x01auth=Bearer x\x01\x01")
                .unwrap()
                .authorization
                .as_deref(),
            Some("user,name=ok")
        );
        for invalid in [
            b"p=tls-exporter,,\x01auth=Bearer x\x01\x01".as_slice(),
            b"n,,auth=Bearer x\x01\x01",
            b"n,,\x01auth=Bearer x\x01",
            b"n,,\x01auth=Bearer x\x01\x01tail",
            b"n,,\x01auth=Bearer x\x01auth=Bearer y\x01\x01",
            b"n,a=user=xx,\x01auth=Bearer x\x01\x01",
            b"n,,\x01auth=Basic x\x01\x01",
            b"n,,\x01auth=Bearer x y\x01\x01",
            b"n,,\x01port=9093\x01\x01",
        ] {
            assert!(initial(invalid).is_err());
        }
    }
}

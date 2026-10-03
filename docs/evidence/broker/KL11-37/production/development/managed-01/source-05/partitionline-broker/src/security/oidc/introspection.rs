use super::{
    http::{Client, Endpoint},
    jwt, text, Error, Policy, Verified,
};
use base64::{engine::general_purpose::STANDARD, Engine as _};
use serde_json::Value;
use std::{
    fmt,
    time::{Duration, SystemTime},
};
use tokio::time::Instant;
use zeroize::Zeroizing;

/// Explicit RFC7662 revocation authority, authenticated using client_secret_basic.
/// Credentials and bearer tokens are ephemeral zeroizing source buffers.
pub struct Introspection {
    /// Exact configured HTTPS endpoint; discovery/token claims cannot override it.
    pub endpoint: String,
    /// Bounded UTF-8 client identity, at most 1024 bytes.
    pub client_id: String,
    /// Bounded ephemeral client secret, 1 through 4096 bytes, never persisted.
    pub client_secret: super::super::sasl::Secret,
}
impl fmt::Debug for Introspection {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("Introspection { [REDACTED] }")
    }
}
pub(super) struct Authority {
    endpoint: Endpoint,
    authorization: Zeroizing<String>,
}
impl Authority {
    pub(super) fn new(config: Introspection) -> Result<Self, Error> {
        if !text(&config.client_id, 1024) || !(1..=4096).contains(&config.client_secret.len()) {
            return Err(Error::InvalidConfiguration);
        }
        let endpoint = Endpoint::parse(&config.endpoint)?;
        let mut credentials = encode(config.client_id.as_bytes());
        credentials.push(':');
        credentials.push_str(&encode(config.client_secret.as_bytes()));
        let mut authorization = Zeroizing::new(String::from("Basic "));
        let encoded = Zeroizing::new(STANDARD.encode(credentials.as_bytes()));
        authorization.push_str(&encoded);
        if authorization.len() > 8192 {
            return Err(Error::InvalidConfiguration);
        }
        // The encoded authorization value itself is secret and remains zeroizing
        // for the authority lifetime. HTTP/rustls immutable wire copies do not
        // promise zeroization; no credentials are written to disk or diagnostics.
        Ok(Self {
            endpoint,
            authorization,
        })
    }
    pub(super) async fn check(
        &self,
        client: &Client,
        policy: &Policy,
        token: &[u8],
        verified: &Verified,
        maximum_lease: Duration,
    ) -> Result<Instant, Error> {
        let started = Instant::now();
        let mut body = encode(token);
        body.insert_str(0, "token=");
        body.push_str("&token_type_hint=access_token");
        let body = Zeroizing::new(body.as_bytes().to_vec());
        let authorization = Zeroizing::new(self.authorization.to_string());
        let bytes = client.post(&self.endpoint, body, authorization).await?;
        let value = jwt::parse_json(
            &bytes,
            policy.limits.document_bytes,
            policy.limits.json_depth,
        )?;
        let object = value.as_object().ok_or(Error::Malformed)?;
        match object.get("active").and_then(Value::as_bool) {
            Some(false) => return Err(Error::Revoked),
            Some(true) => {}
            None => return Err(Error::Malformed),
        }
        if object.get("iss").and_then(Value::as_str) != Some(verified.issuer())
            || object.get("sub").and_then(Value::as_str) != Some(verified.subject())
        {
            return Err(Error::Authentication);
        }
        let audience = match object.get("aud") {
            Some(Value::String(value)) => vec![value.as_str()],
            Some(Value::Array(values))
                if !values.is_empty() && values.len() <= policy.limits.audiences =>
            {
                values
                    .iter()
                    .map(|v| v.as_str().ok_or(Error::Malformed))
                    .collect::<Result<Vec<_>, _>>()?
            }
            _ => return Err(Error::Malformed),
        };
        if audience.iter().any(|a| !text(a, 2048))
            || !audience
                .iter()
                .any(|a| policy.audiences.iter().any(|b| a == b))
        {
            return Err(Error::Authentication);
        }
        let exp = object
            .get("exp")
            .and_then(Value::as_u64)
            .ok_or(Error::Malformed)?;
        let expires = SystemTime::UNIX_EPOCH
            .checked_add(Duration::from_secs(exp))
            .ok_or(Error::Malformed)?;
        if expires > verified.expires_at() {
            return Err(Error::Authentication);
        }
        let remaining = expires
            .duration_since(SystemTime::now())
            .map_err(|_| Error::Expired)?;
        let now = Instant::now();
        let until = now
            .checked_add(remaining)
            .ok_or(Error::Malformed)?
            .min(started.checked_add(maximum_lease).ok_or(Error::Malformed)?);
        if now >= until {
            return Err(Error::Expired);
        }
        Ok(until)
    }
}
fn encode(bytes: &[u8]) -> Zeroizing<String> {
    const HEX: &[u8; 16] = b"0123456789ABCDEF";
    let mut result = Zeroizing::new(String::new());
    for byte in bytes {
        match byte {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'.' | b'_' | b'*' => {
                result.push(char::from(*byte))
            }
            b' ' => result.push('+'),
            _ => {
                result.push('%');
                result.push(char::from(HEX[usize::from(*byte >> 4)]));
                result.push(char::from(HEX[usize::from(*byte & 15)]));
            }
        }
    }
    result
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn form_and_basic_credential_encoding_is_literal() {
        assert_eq!(encode(b"a b:+/%\n").as_str(), "a+b%3A%2B%2F%25%0A");
        assert_eq!(
            encode("päss💫".as_bytes()).as_str(),
            "p%C3%A4ss%F0%9F%92%AB"
        );
        let authority = Authority::new(Introspection {
            endpoint: "https://issuer.example/introspect".to_owned(),
            client_id: "client:id".to_owned(),
            client_secret: super::super::super::sasl::Secret::new(b"s e:c".to_vec()),
        })
        .unwrap();
        assert_eq!(
            authority.authorization.as_str(),
            format!("Basic {}", STANDARD.encode("client%3Aid:s+e%3Ac"))
        );
    }
}

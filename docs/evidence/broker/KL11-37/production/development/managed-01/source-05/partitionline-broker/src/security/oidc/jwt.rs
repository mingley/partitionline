use super::{text, AccessToken, Algorithm, Error, Policy, Verified};
use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine as _};
use ring::signature;
use serde::de::{self, DeserializeSeed, MapAccess, SeqAccess, Visitor};
use serde_json::{Map, Number, Value};
use std::{collections::HashMap, fmt, time::SystemTime};
use tokio::time::Instant;
use zeroize::Zeroizing;

enum Key {
    Rsa { modulus: Vec<u8>, exponent: Vec<u8> },
    Ec(Vec<u8>),
}
impl Key {
    fn algorithm(&self) -> Algorithm {
        match self {
            Self::Rsa { .. } => Algorithm::Rs256,
            Self::Ec(_) => Algorithm::Es256,
        }
    }
    fn verify(&self, message: &[u8], proof: &[u8]) -> Result<(), Error> {
        match self {
            Self::Rsa { modulus, exponent } => signature::RsaPublicKeyComponents {
                n: modulus,
                e: exponent,
            }
            .verify(&signature::RSA_PKCS1_2048_8192_SHA256, message, proof),
            Self::Ec(point) => {
                if proof.len() != 64 {
                    return Err(Error::Authentication);
                }
                signature::UnparsedPublicKey::new(&signature::ECDSA_P256_SHA256_FIXED, point)
                    .verify(message, proof)
            }
        }
        .map_err(|_| Error::Authentication)
    }
}

pub(super) struct KeySet(HashMap<String, Key>);
impl KeySet {
    pub(super) fn contains(&self, kid: &str) -> bool {
        self.0.contains_key(kid)
    }
    pub(super) fn fingerprint(&self, kid: &str) -> Option<[u8; 32]> {
        let key = self.0.get(kid)?;
        let mut hash = ring::digest::Context::new(&ring::digest::SHA256);
        hash.update(key.algorithm().name().as_bytes());
        match key {
            Key::Ec(point) => hash.update(point),
            Key::Rsa { modulus, exponent } => {
                hash.update(&(modulus.len() as u64).to_be_bytes());
                hash.update(modulus);
                hash.update(exponent);
            }
        }
        hash.finish().as_ref().try_into().ok()
    }
    pub(super) fn parse(bytes: &[u8], policy: &Policy) -> Result<Self, Error> {
        let root = parse_json(
            bytes,
            policy.limits.document_bytes,
            policy.limits.json_depth,
        )
        .map_err(|_| Error::InvalidConfiguration)?;
        let keys = root
            .as_object()
            .and_then(|o| o.get("keys"))
            .and_then(Value::as_array)
            .ok_or(Error::InvalidConfiguration)?;
        if keys.is_empty() || keys.len() > policy.limits.keys {
            return Err(Error::InvalidConfiguration);
        }
        let mut public = HashMap::new();
        let mut identifiers = std::collections::HashSet::new();
        for value in keys {
            let object = value.as_object().ok_or(Error::InvalidConfiguration)?;
            let kid = string(object, "kid").map_err(|_| Error::InvalidConfiguration)?;
            if !text(kid, policy.limits.kid_bytes) || !identifiers.insert(kid) {
                return Err(Error::InvalidConfiguration);
            }
            if ["d", "p", "q", "dp", "dq", "qi", "oth", "k"]
                .iter()
                .any(|name| object.contains_key(*name))
            {
                return Err(Error::InvalidConfiguration);
            }
            if object.get("use").is_some_and(|v| v.as_str() != Some("sig")) {
                continue;
            }
            if let Some(operations) = object.get("key_ops") {
                if operations
                    .as_array()
                    .is_none_or(|v| v.len() != 1 || v[0].as_str() != Some("verify"))
                {
                    return Err(Error::InvalidConfiguration);
                }
            }
            let algorithm = match string(object, "kty").map_err(|_| Error::InvalidConfiguration)? {
                "RSA" => Algorithm::Rs256,
                "EC" if object.get("crv").and_then(Value::as_str) == Some("P-256") => {
                    Algorithm::Es256
                }
                // Other public signing algorithms may coexist in an issuer JWKS,
                // but cannot be selected by this policy or a token.
                _ => continue,
            };
            if !policy.algorithms.contains(&algorithm) {
                continue;
            }
            if object
                .get("alg")
                .is_some_and(|v| v.as_str() != Some(algorithm.name()))
            {
                continue;
            }
            let key = match algorithm {
                Algorithm::Rs256 => {
                    let modulus = field_bytes(object, "n", 512)?;
                    let exponent = field_bytes(object, "e", 4)?;
                    if modulus.first().is_none_or(|b| *b == 0)
                        || exponent.first().is_none_or(|b| *b == 0)
                        || modulus.last().is_none_or(|b| b % 2 == 0)
                    {
                        return Err(Error::InvalidConfiguration);
                    }
                    let bits = modulus.len() * 8 - modulus[0].leading_zeros() as usize;
                    let exp = exponent.iter().fold(0u64, |n, b| (n << 8) | u64::from(*b));
                    if !(2048..=4096).contains(&bits) || exp < 3 || exp % 2 == 0 {
                        return Err(Error::InvalidConfiguration);
                    }
                    Key::Rsa { modulus, exponent }
                }
                Algorithm::Es256 => {
                    let x = field_bytes(object, "x", 32)?;
                    let y = field_bytes(object, "y", 32)?;
                    if x.len() != 32 || y.len() != 32 {
                        return Err(Error::InvalidConfiguration);
                    }
                    let mut point = Vec::with_capacity(65);
                    point.push(4);
                    point.extend_from_slice(&x);
                    point.extend_from_slice(&y);
                    Key::Ec(point)
                }
            };
            public.insert(kid.to_owned(), key);
        }
        if public.is_empty() {
            return Err(Error::InvalidConfiguration);
        }
        Ok(Self(public))
    }
}
fn field_bytes(object: &Map<String, Value>, name: &str, maximum: usize) -> Result<Vec<u8>, Error> {
    let value = string(object, name).map_err(|_| Error::InvalidConfiguration)?;
    decode(value, maximum).map_err(|_| Error::InvalidConfiguration)
}
fn decode(value: &str, maximum: usize) -> Result<Vec<u8>, Error> {
    if value.is_empty()
        || value.len() > maximum.saturating_mul(4).div_ceil(3)
        || value
            .bytes()
            .any(|b| !b.is_ascii_alphanumeric() && b != b'-' && b != b'_')
    {
        return Err(Error::Malformed);
    }
    let decoded = URL_SAFE_NO_PAD
        .decode(value)
        .map_err(|_| Error::Malformed)?;
    if decoded.len() > maximum || URL_SAFE_NO_PAD.encode(&decoded) != value {
        return Err(Error::Malformed);
    }
    Ok(decoded)
}
fn string<'a>(object: &'a Map<String, Value>, name: &str) -> Result<&'a str, Error> {
    object
        .get(name)
        .and_then(Value::as_str)
        .ok_or(Error::Malformed)
}
fn integer(object: &Map<String, Value>, name: &str) -> Result<u64, Error> {
    object
        .get(name)
        .and_then(Value::as_u64)
        .ok_or(Error::Authentication)
}

pub(super) fn peek_kid(token: &[u8], policy: &Policy) -> Result<String, Error> {
    if token.is_empty() || token.len() > policy.limits.token_bytes {
        return Err(Error::Malformed);
    }
    let text = std::str::from_utf8(token).map_err(|_| Error::Malformed)?;
    let parts: Vec<_> = text.split('.').collect();
    if parts.len() != 3 {
        return Err(Error::Malformed);
    }
    let header = Zeroizing::new(decode(parts[0], policy.limits.header_bytes)?);
    let value = parse_json(
        &header,
        policy.limits.header_bytes,
        policy.limits.json_depth,
    )?;
    let object = value.as_object().ok_or(Error::Malformed)?;
    let alg = match string(object, "alg")? {
        "RS256" => Algorithm::Rs256,
        "ES256" => Algorithm::Es256,
        _ => return Err(Error::Authentication),
    };
    if !policy.algorithms.contains(&alg) {
        return Err(Error::Authentication);
    }
    let kid = string(object, "kid")?;
    if !text_value(kid, policy.limits.kid_bytes) {
        return Err(Error::Malformed);
    };
    Ok(kid.to_owned())
}
fn text_value(value: &str, maximum: usize) -> bool {
    text(value, maximum)
}
pub(super) fn verify(
    policy: &Policy,
    keys: &KeySet,
    token: &[u8],
    now: SystemTime,
    freshness: Instant,
) -> Result<Verified, Error> {
    let admitted = Instant::now();
    if admitted >= freshness {
        return Err(Error::Expired);
    }
    if token.is_empty() || token.len() > policy.limits.token_bytes {
        return Err(Error::Malformed);
    }
    let token = std::str::from_utf8(token).map_err(|_| Error::Malformed)?;
    let mut segments = token.split('.');
    let header = segments.next().ok_or(Error::Malformed)?;
    let payload = segments.next().ok_or(Error::Malformed)?;
    let proof = segments.next().ok_or(Error::Malformed)?;
    if segments.next().is_some() {
        return Err(Error::Malformed);
    }
    let header_bytes = Zeroizing::new(decode(header, policy.limits.header_bytes)?);
    let header = parse_json(
        &header_bytes,
        policy.limits.header_bytes,
        policy.limits.json_depth,
    )?;
    let header = header.as_object().ok_or(Error::Malformed)?;
    // No token-selected authorities, embedded key, unencoded payload or critical
    // extensions. Ignoring these while treating them as trust would be unsafe.
    if ["crit", "jku", "jwk", "x5u", "b64"]
        .iter()
        .any(|name| header.contains_key(*name))
    {
        return Err(Error::Malformed);
    }
    let algorithm = match string(header, "alg")? {
        "RS256" => Algorithm::Rs256,
        "ES256" => Algorithm::Es256,
        _ => return Err(Error::Authentication),
    };
    if !policy.algorithms.contains(&algorithm) {
        return Err(Error::Authentication);
    }
    let kid = string(header, "kid")?;
    if !text(kid, policy.limits.kid_bytes) {
        return Err(Error::Malformed);
    }
    let key = keys.0.get(kid).ok_or(Error::Authentication)?;
    if key.algorithm() != algorithm {
        return Err(Error::Authentication);
    }
    let proof = Zeroizing::new(decode(proof, 512)?);
    let signed_length = token.rfind('.').ok_or(Error::Malformed)?;
    key.verify(&token.as_bytes()[..signed_length], &proof)?;
    // Parse identity-bearing claims only after the signature has succeeded.
    let payload = Zeroizing::new(decode(payload, policy.limits.token_bytes)?);
    let claims = parse_json(
        &payload,
        policy.limits.document_bytes,
        policy.limits.json_depth,
    )?;
    let claims = claims.as_object().ok_or(Error::Malformed)?;
    match &policy.access_token {
        AccessToken::AtJwt if string(header, "typ")? != "at+jwt" => {
            return Err(Error::Authentication)
        }
        AccessToken::Claim { name, value } => {
            if string(claims, name)? != value
                || header
                    .get("typ")
                    .is_some_and(|v| !matches!(v.as_str(), Some("JWT" | "at+jwt")))
            {
                return Err(Error::Authentication);
            }
        }
        AccessToken::AtJwt => {}
    }
    if string(claims, "iss")? != policy.issuer {
        return Err(Error::Authentication);
    }
    let subject = string(claims, "sub")?;
    if !text(subject, policy.limits.identity_bytes) {
        return Err(Error::Authentication);
    }
    let audiences: Vec<&str> = match claims.get("aud") {
        Some(Value::String(a)) => vec![a.as_str()],
        Some(Value::Array(a)) if !a.is_empty() && a.len() <= policy.limits.audiences => a
            .iter()
            .map(|v| v.as_str().ok_or(Error::Authentication))
            .collect::<Result<_, _>>()?,
        _ => return Err(Error::Authentication),
    };
    if audiences.iter().any(|a| !text(a, 2048))
        || !audiences
            .iter()
            .any(|a| policy.audiences.iter().any(|b| a == b))
    {
        return Err(Error::Authentication);
    }
    let seconds = now
        .duration_since(SystemTime::UNIX_EPOCH)
        .map_err(|_| Error::Unavailable)?;
    let exp = integer(claims, "exp")?;
    let iat = integer(claims, "iat")?;
    let future = seconds
        .as_secs()
        .checked_add(policy.limits.clock_skew.as_secs())
        .ok_or(Error::Unavailable)?;
    if exp <= seconds.as_secs() {
        return Err(Error::Expired);
    }
    if iat > future
        || iat >= exp
        || exp
            .checked_sub(iat)
            .is_none_or(|ttl| ttl > policy.limits.token_lifetime.as_secs())
    {
        return Err(Error::Authentication);
    }
    if claims.contains_key("nbf") {
        let nbf = integer(claims, "nbf")?;
        if nbf > future || nbf >= exp {
            return Err(Error::Authentication);
        }
    }
    let token_id = if claims.contains_key("jti") {
        let id = string(claims, "jti")?;
        if !text(id, policy.limits.identity_bytes) {
            return Err(Error::Authentication);
        }
        Some(id.to_owned())
    } else {
        None
    };
    let expires_at = SystemTime::UNIX_EPOCH
        .checked_add(std::time::Duration::from_secs(exp))
        .ok_or(Error::Authentication)?;
    let remaining = expires_at.duration_since(now).map_err(|_| Error::Expired)?;
    let token_deadline = admitted
        .checked_add(remaining)
        .ok_or(Error::Authentication)?;
    let deadline = token_deadline.min(freshness);
    if Instant::now() >= deadline {
        return Err(Error::Expired);
    }
    Ok(Verified {
        issuer: policy.issuer.clone(),
        subject: subject.to_owned(),
        token_id,
        key_id: kid.to_owned(),
        expires_at,
        token_deadline,
        deadline,
    })
}

struct JsonSeed {
    depth: usize,
    maximum: usize,
}
impl<'de> DeserializeSeed<'de> for JsonSeed {
    type Value = Value;
    fn deserialize<D: de::Deserializer<'de>>(self, deserializer: D) -> Result<Value, D::Error> {
        deserializer.deserialize_any(self)
    }
}
impl<'de> Visitor<'de> for JsonSeed {
    type Value = Value;
    fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("bounded unique JSON")
    }
    fn visit_bool<E: de::Error>(self, value: bool) -> Result<Value, E> {
        Ok(Value::Bool(value))
    }
    fn visit_i64<E: de::Error>(self, value: i64) -> Result<Value, E> {
        Ok(Value::Number(value.into()))
    }
    fn visit_u64<E: de::Error>(self, value: u64) -> Result<Value, E> {
        Ok(Value::Number(value.into()))
    }
    fn visit_f64<E: de::Error>(self, value: f64) -> Result<Value, E> {
        Number::from_f64(value)
            .map(Value::Number)
            .ok_or_else(|| E::custom("nonfinite number"))
    }
    fn visit_str<E: de::Error>(self, value: &str) -> Result<Value, E> {
        Ok(Value::String(value.to_owned()))
    }
    fn visit_string<E: de::Error>(self, value: String) -> Result<Value, E> {
        Ok(Value::String(value))
    }
    fn visit_none<E: de::Error>(self) -> Result<Value, E> {
        Ok(Value::Null)
    }
    fn visit_unit<E: de::Error>(self) -> Result<Value, E> {
        Ok(Value::Null)
    }
    fn visit_seq<A: SeqAccess<'de>>(self, mut access: A) -> Result<Value, A::Error> {
        if self.depth >= self.maximum {
            return Err(de::Error::custom("JSON depth"));
        }
        let mut values = Vec::new();
        while let Some(value) = access.next_element_seed(JsonSeed {
            depth: self.depth + 1,
            maximum: self.maximum,
        })? {
            values.push(value);
        }
        Ok(Value::Array(values))
    }
    fn visit_map<A: MapAccess<'de>>(self, mut access: A) -> Result<Value, A::Error> {
        if self.depth >= self.maximum {
            return Err(de::Error::custom("JSON depth"));
        }
        let mut values = Map::new();
        while let Some(key) = access.next_key::<String>()? {
            if values.contains_key(&key) {
                return Err(de::Error::custom("duplicate JSON field"));
            }
            let value = access.next_value_seed(JsonSeed {
                depth: self.depth + 1,
                maximum: self.maximum,
            })?;
            values.insert(key, value);
        }
        Ok(Value::Object(values))
    }
}
pub(super) fn parse_json(bytes: &[u8], maximum: usize, depth: usize) -> Result<Value, Error> {
    if bytes.is_empty() || bytes.len() > maximum {
        return Err(Error::Malformed);
    }
    let mut deserializer = serde_json::Deserializer::from_slice(bytes);
    let value = JsonSeed {
        depth: 0,
        maximum: depth,
    }
    .deserialize(&mut deserializer)
    .map_err(|_| Error::Malformed)?;
    deserializer.end().map_err(|_| Error::Malformed)?;
    Ok(value)
}

#[cfg(test)]
mod tests {
    use super::*;
    use ring::{rand::SystemRandom, signature::KeyPair as _};
    use serde_json::json;
    use std::time::Duration;
    fn fixture(filename: &str) -> Vec<u8> {
        use std::io::Read as _;
        assert!(!filename.contains('/') && !filename.contains('\\') && !filename.contains(".."));
        let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("tests/fixtures/oidc")
            .join(filename);
        let mut file = std::fs::File::open(path).unwrap();
        let mut bytes = Vec::new();
        file.read_to_end(&mut bytes).unwrap();
        bytes
    }
    fn sha(bytes: &[u8]) -> String {
        ring::digest::digest(&ring::digest::SHA256, bytes)
            .as_ref()
            .iter()
            .map(|b| format!("{b:02x}"))
            .collect()
    }
    #[test]
    fn independent_openssl_controlled_epoch_matrix() {
        let manifest: Value = serde_json::from_slice(&fixture("fixtures.json")).unwrap();
        assert_eq!(manifest["schema_version"], 1);
        assert_eq!(manifest["epoch_anchor"], 1800000000u64);
        let jwks = fixture(manifest["jwks_file"].as_str().unwrap());
        assert_eq!(sha(&jwks), manifest["jwks_sha256"].as_str().unwrap());
        let mut policy = policy();
        policy.issuer = manifest["issuer"].as_str().unwrap().to_owned();
        policy.audiences = manifest["audiences"]
            .as_array()
            .unwrap()
            .iter()
            .map(|a| a.as_str().unwrap().to_owned())
            .collect();
        policy.validate().unwrap();
        let keys = KeySet::parse(&jwks, &policy).unwrap();
        let rows = manifest["cases"].as_array().unwrap();
        let mut ids = std::collections::HashSet::new();
        let mut mismatches = Vec::new();
        let (mut accepted, mut rejected, mut signature_valid_metadata) = (0, 0, 0);
        for row in rows {
            let id = row["id"].as_str().unwrap();
            assert!(ids.insert(id), "duplicate fixture id");
            let token = fixture(row["compact_jws_file"].as_str().unwrap());
            assert_eq!(
                sha(&token),
                row["compact_jws_sha256"].as_str().unwrap(),
                "{id} token hash"
            );
            for part in ["header", "payload"] {
                let bytes = fixture(row[format!("{part}_file")].as_str().unwrap());
                assert_eq!(
                    bytes,
                    row[format!("{part}_utf8")].as_str().unwrap().as_bytes(),
                    "{id} {part} bytes"
                );
                assert_eq!(
                    sha(&bytes),
                    row[format!("{part}_sha256")].as_str().unwrap(),
                    "{id} {part} hash"
                );
            }
            let text = std::str::from_utf8(&token).unwrap();
            // Fixture provenance binds the original first two signed segments
            // even for the deliberately malformed fourth-segment case.
            let mut parts = text.split('.');
            let signed = format!("{}.{}", parts.next().unwrap(), parts.next().unwrap());
            assert_eq!(
                sha(signed.as_bytes()),
                row["signing_input_sha256"].as_str().unwrap(),
                "{id} signed bytes"
            );
            let public = fixture(&format!("{}.spki.der", row["signer_kid"].as_str().unwrap()));
            assert_eq!(
                sha(&public),
                row["public_key_sha256"].as_str().unwrap(),
                "{id} independent public key hash"
            );
            let mut selected = policy.clone();
            match row["alternate_policy"].as_str() {
                None => {}
                Some("token_use=access") => {
                    selected.access_token = AccessToken::Claim {
                        name: "token_use".to_owned(),
                        value: "access".to_owned(),
                    }
                }
                Some(_) => panic!("unknown frozen fixture policy"),
            }
            let epoch = row["validation_epoch"].as_u64().unwrap();
            let result = verify(
                &selected,
                &keys,
                &token,
                SystemTime::UNIX_EPOCH + Duration::from_secs(epoch),
                Instant::now() + Duration::from_secs(300),
            );
            let expected = match row["expected_policy_decision"].as_str().unwrap() {
                "accept" => true,
                "reject" => false,
                _ => panic!("unknown frozen fixture decision"),
            };
            if result.is_ok() != expected {
                mismatches.push(format!("{id}: expected {expected}, result {result:?}"));
            }
            if let Ok(verified) = &result {
                let payload: Value =
                    serde_json::from_slice(&fixture(row["payload_file"].as_str().unwrap()))
                        .unwrap();
                assert_eq!(verified.issuer(), selected.issuer);
                assert_eq!(verified.subject(), payload["sub"].as_str().unwrap());
                assert_eq!(verified.key_id(), row["kid"].as_str().unwrap());
                accepted += 1;
            } else {
                rejected += 1;
            }
            // These are independent OpenSSL proof decisions, not this verifier's
            // syntax/algorithm policy. DER/padded/unsupported-header rows retain
            // valid mathematical proof with an intentionally rejected JWS format.
            if row["expected_signature_valid"].as_bool().unwrap() {
                signature_valid_metadata += 1;
            }
            println!("OIDC fixture {id}: actual_accept={} expected_accept={expected} independent_signature_valid={}",result.is_ok(),row["expected_signature_valid"]);
        }
        assert_eq!(rows.len(), 70);
        assert_eq!(accepted, 12);
        assert_eq!(rejected, 58);
        assert_eq!(signature_valid_metadata, 65);
        assert!(mismatches.is_empty(), "{}", mismatches.join("; "));
        println!("OIDC independent matrix:70 cases;12 accepted;58 rejected;65 independently valid signatures;all exact histories/hashes verified");
    }

    struct Signer(signature::EcdsaKeyPair);
    impl Signer {
        fn new() -> Self {
            let rng = SystemRandom::new();
            let key = signature::EcdsaKeyPair::generate_pkcs8(
                &signature::ECDSA_P256_SHA256_FIXED_SIGNING,
                &rng,
            )
            .unwrap();
            Self(
                signature::EcdsaKeyPair::from_pkcs8(
                    &signature::ECDSA_P256_SHA256_FIXED_SIGNING,
                    key.as_ref(),
                    &rng,
                )
                .unwrap(),
            )
        }
        fn jwk(&self) -> Value {
            let public = self.0.public_key().as_ref();
            json!({"kid":"es-test","kty":"EC","crv":"P-256","alg":"ES256","use":"sig","key_ops":["verify"],"x":URL_SAFE_NO_PAD.encode(&public[1..33]),"y":URL_SAFE_NO_PAD.encode(&public[33..])})
        }
        fn sign_raw(&self, header: &[u8], payload: &[u8]) -> Vec<u8> {
            let signed = format!(
                "{}.{}",
                URL_SAFE_NO_PAD.encode(header),
                URL_SAFE_NO_PAD.encode(payload)
            );
            let proof = self
                .0
                .sign(&SystemRandom::new(), signed.as_bytes())
                .unwrap();
            format!("{}.{}", signed, URL_SAFE_NO_PAD.encode(proof.as_ref())).into_bytes()
        }
        fn sign(&self, header: &Value, payload: &Value) -> Vec<u8> {
            self.sign_raw(
                &serde_json::to_vec(header).unwrap(),
                &serde_json::to_vec(payload).unwrap(),
            )
        }
        fn keys(&self, policy: &Policy) -> KeySet {
            KeySet::parse(
                &serde_json::to_vec(&json!({"keys":[self.jwk()]})).unwrap(),
                policy,
            )
            .unwrap()
        }
    }
    fn policy() -> Policy {
        Policy {
            issuer: "https://issuer.example/realm".to_owned(),
            audiences: vec!["partitionline".to_owned()],
            algorithms: vec![Algorithm::Es256, Algorithm::Rs256],
            access_token: AccessToken::AtJwt,
            limits: super::super::Limits::default(),
        }
    }
    fn header() -> Value {
        json!({"alg":"ES256","kid":"es-test","typ":"at+jwt"})
    }
    fn claims() -> Value {
        json!({"iss":"https://issuer.example/realm","aud":"partitionline","sub":"user","iat":1000,"exp":1100,"jti":"id-1"})
    }
    fn check(policy: &Policy, keys: &KeySet, token: &[u8]) -> Result<Verified, Error> {
        verify(
            policy,
            keys,
            token,
            SystemTime::UNIX_EPOCH + Duration::from_secs(1050),
            Instant::now() + Duration::from_secs(100),
        )
    }
    #[test]
    fn es256_fixed_signature_and_typed_principal() {
        let signer = Signer::new();
        let policy = policy();
        let keys = signer.keys(&policy);
        let verified = check(&policy, &keys, &signer.sign(&header(), &claims())).unwrap();
        assert_eq!(verified.issuer(), policy.issuer);
        assert_eq!(verified.subject(), "user");
        assert_eq!(verified.token_id(), Some("id-1"));
        assert_eq!(verified.key_id(), "es-test");
        assert!(verified.deadline() > Instant::now());
        assert!(format!("{verified:?}").contains("REDACTED"));
        assert!(!format!("{verified:?}").contains("user"));
    }
    #[test]
    fn authentic_signature_is_required_before_identity() {
        let signer = Signer::new();
        let policy = policy();
        let keys = signer.keys(&policy);
        let mut token = signer.sign(&header(), &claims());
        let last = token.len() - 1;
        token[last] = if token[last] == b'A' { b'Q' } else { b'A' };
        assert!(check(&policy, &keys, &token).is_err());
        let mut invalid_header = header();
        for algorithm in ["none", "HS256", "RS256", "ES384"] {
            invalid_header["alg"] = json!(algorithm);
            assert!(check(&policy, &keys, &signer.sign(&invalid_header, &claims())).is_err());
        }
    }
    #[test]
    fn signed_wrong_claims_fail() {
        let signer = Signer::new();
        let policy = policy();
        let keys = signer.keys(&policy);
        for (name, value) in [
            ("iss", json!("https://other.example")),
            ("aud", json!("other")),
            ("aud", json!([])),
            ("aud", json!(["other", 3])),
            ("sub", json!("")),
            ("sub", json!("user\n")),
            ("exp", json!(1050)),
            ("exp", json!(1050.5)),
            ("iat", json!(1056)),
            ("iat", json!(1100)),
            ("iat", json!(-1)),
            ("nbf", json!(1056)),
            ("jti", json!("")),
        ] {
            let mut value_claims = claims();
            value_claims[name] = value;
            assert!(
                check(&policy, &keys, &signer.sign(&header(), &value_claims)).is_err(),
                "{name}"
            );
        }
        let mut value_claims = claims();
        value_claims["iat"] = json!(0);
        value_claims["exp"] = json!(10000);
        assert!(check(&policy, &keys, &signer.sign(&header(), &value_claims)).is_err());
        for missing in ["iss", "sub", "aud", "exp", "iat"] {
            let mut value_claims = claims();
            value_claims.as_object_mut().unwrap().remove(missing);
            assert!(check(&policy, &keys, &signer.sign(&header(), &value_claims)).is_err());
        }
    }
    #[test]
    fn provider_access_discriminator_is_explicit() {
        let signer = Signer::new();
        let mut policy = policy();
        let keys = signer.keys(&policy);
        let mut h = header();
        h["typ"] = json!("JWT");
        assert!(check(&policy, &keys, &signer.sign(&h, &claims())).is_err());
        policy.access_token = AccessToken::Claim {
            name: "token_use".to_owned(),
            value: "access".to_owned(),
        };
        let mut payload = claims();
        payload["token_use"] = json!("id");
        assert!(check(&policy, &keys, &signer.sign(&h, &payload)).is_err());
        payload["token_use"] = json!("access");
        assert!(check(&policy, &keys, &signer.sign(&h, &payload)).is_ok());
        payload["aud"] = json!(["other", "partitionline"]);
        assert!(check(&policy, &keys, &signer.sign(&h, &payload)).is_ok());
    }
    #[test]
    fn duplicate_json_and_whole_input_are_enforced() {
        let signer = Signer::new();
        let policy = policy();
        let keys = signer.keys(&policy);
        let token = signer.sign_raw(
            br#"{"alg":"ES256","alg":"ES256","kid":"es-test","typ":"at+jwt"}"#,
            &serde_json::to_vec(&claims()).unwrap(),
        );
        assert!(matches!(
            check(&policy, &keys, &token),
            Err(Error::Malformed)
        ));
        let duplicate = br#"{"iss":"https://issuer.example/realm","\u0069ss":"https://issuer.example/realm","aud":"partitionline","sub":"user","iat":1000,"exp":1100}"#;
        assert!(matches!(
            check(
                &policy,
                &keys,
                &signer.sign_raw(&serde_json::to_vec(&header()).unwrap(), duplicate)
            ),
            Err(Error::Malformed)
        ));
        for bytes in [
            b"{\"a\":1,\"a\":2}".as_slice(),
            b"{\"x\":{\"a\":1,\"a\":2}}",
            b"{}{}",
            b"[1]x",
        ] {
            assert_eq!(parse_json(bytes, 1024, 4).unwrap_err(), Error::Malformed);
        }
    }
    #[test]
    fn token_authorities_critical_fields_and_segments_are_rejected() {
        let signer = Signer::new();
        let policy = policy();
        let keys = signer.keys(&policy);
        for name in ["jku", "jwk", "x5u", "crit", "b64"] {
            let mut h = header();
            h[name] = json!("attacker");
            assert!(matches!(
                check(&policy, &keys, &signer.sign(&h, &claims())),
                Err(Error::Malformed)
            ));
        }
        let token = signer.sign(&header(), &claims());
        let mut extra = token.clone();
        extra.extend_from_slice(b".extra");
        assert!(matches!(
            check(&policy, &keys, &extra),
            Err(Error::Malformed)
        ));
        let mut padded = token;
        padded.push(b'=');
        assert!(check(&policy, &keys, &padded).is_err());
        assert!(decode("AB", 32).is_err()); // non-canonical unused bits
        assert!(decode("AA==", 32).is_err());
        assert!(decode("AA\n", 32).is_err());
    }
    #[test]
    fn bounds_are_checked_before_unbounded_structures() {
        let signer = Signer::new();
        let mut policy = policy();
        let keys = signer.keys(&policy);
        let mut payload = claims();
        payload["sub"] = json!("a".repeat(policy.limits.identity_bytes + 1));
        assert!(check(&policy, &keys, &signer.sign(&header(), &payload)).is_err());
        let mut h = header();
        h["kid"] = json!("a".repeat(policy.limits.kid_bytes + 1));
        assert!(check(&policy, &keys, &signer.sign(&h, &claims())).is_err());
        policy.limits.token_bytes = 512;
        assert!(check(&policy, &keys, &vec![b'a'; 513]).is_err());
        assert!(parse_json(b"[[[[]]]]", 1024, 3).is_err());
        assert!(parse_json(&vec![b' '; 1025], 1024, 3).is_err());
    }
    #[test]
    fn jwks_duplicates_private_forms_and_weak_rsa_fail() {
        let signer = Signer::new();
        let policy = policy();
        let key = signer.jwk();
        assert!(KeySet::parse(
            &serde_json::to_vec(&json!({"keys":[key.clone(),key.clone()]})).unwrap(),
            &policy
        )
        .is_err());
        for name in ["d", "p", "q", "dp", "dq", "qi", "oth", "k"] {
            let mut private = key.clone();
            private[name] = json!("AA");
            assert!(KeySet::parse(
                &serde_json::to_vec(&json!({"keys":[private]})).unwrap(),
                &policy
            )
            .is_err());
        }
        let weak =
            json!({"kid":"rsa","kty":"RSA","n":URL_SAFE_NO_PAD.encode([0x80u8;128]),"e":"AQAB"});
        assert!(KeySet::parse(
            &serde_json::to_vec(&json!({"keys":[weak]})).unwrap(),
            &policy
        )
        .is_err());
        let mut bad = key;
        bad["key_ops"] = json!(["sign", "verify"]);
        assert!(KeySet::parse(
            &serde_json::to_vec(&json!({"keys":[bad]})).unwrap(),
            &policy
        )
        .is_err());
    }
    #[test]
    fn expiration_has_no_skew_or_clock_rollback_authority_extension() {
        let signer = Signer::new();
        let policy = policy();
        let keys = signer.keys(&policy);
        let token = signer.sign(&header(), &claims());
        assert!(matches!(
            verify(
                &policy,
                &keys,
                &token,
                SystemTime::UNIX_EPOCH + Duration::from_secs(1100),
                Instant::now() + Duration::from_secs(100)
            ),
            Err(Error::Expired)
        ));
        assert!(matches!(
            verify(
                &policy,
                &keys,
                &token,
                SystemTime::UNIX_EPOCH + Duration::from_secs(1000),
                Instant::now() - Duration::from_millis(1)
            ),
            Err(Error::Expired)
        ));
        let now = SystemTime::UNIX_EPOCH + Duration::from_millis(1098500);
        let verified = verify(
            &policy,
            &keys,
            &token,
            now,
            Instant::now() + Duration::from_secs(100),
        )
        .unwrap();
        assert!(verified.deadline() <= Instant::now() + Duration::from_millis(1500));
    }
}

//! Delegation-token issue, renew, expire, and authentication.
//!
//! Creating a token requires cluster `Create` authorization. The owner and
//! listed renewers may extend expiry up to the token's maximum timestamp.
//! Only the owner, or a principal allowed to `Delete` the cluster, may revoke
//! it. Expired and revoked tokens fail authentication. Describe output and
//! `Debug` never include the secret.
//!
//! This is not the CreateDelegationToken, RenewDelegationToken, or
//! ExpireDelegationToken RPC, and it is not wired into SASL.

use super::acl::{Authorizer, Operation, Resource};
use crate::journal::{self, Journal};
use std::collections::BTreeSet;
use std::fmt;
use std::path::Path;

const RECORD_MAGIC: &[u8; 8] = b"PLTKN001";
const KIND_ISSUE: u8 = 1;
const KIND_RENEW: u8 = 2;
const KIND_REVOKE: u8 = 3;

/// Kafka `DELEGATION_TOKEN_NOT_FOUND`.
pub const DELEGATION_TOKEN_NOT_FOUND: i16 = 62;
/// Kafka `DELEGATION_TOKEN_OWNER_MISMATCH`.
pub const DELEGATION_TOKEN_OWNER_MISMATCH: i16 = 63;
/// Kafka `DELEGATION_TOKEN_AUTHORIZATION_FAILED`.
pub const DELEGATION_TOKEN_AUTHORIZATION_FAILED: i16 = 65;
/// Kafka `DELEGATION_TOKEN_EXPIRED`.
pub const DELEGATION_TOKEN_EXPIRED: i16 = 66;

/// HMAC-style secret returned once at issue time.
pub struct Secret([u8; 32]);

impl Secret {
    /// Borrow the secret for the caller that just created the token or is authenticating.
    pub fn expose(&self) -> &[u8; 32] {
        &self.0
    }
}

impl fmt::Debug for Secret {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("[redacted]")
    }
}

impl Clone for Secret {
    fn clone(&self) -> Self {
        Self(self.0)
    }
}

/// A token as stored for describe. The secret is absent.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TokenView {
    /// Stable token id.
    pub token_id: String,
    /// Owner principal.
    pub owner: String,
    /// Principals that may renew.
    pub renewers: Vec<String>,
    /// Issue time, in milliseconds.
    pub issued_at_ms: i64,
    /// Current expiry, in milliseconds.
    pub expiry_ms: i64,
    /// Latest expiry a renew may set, in milliseconds.
    pub max_timestamp_ms: i64,
}

/// The one response that carries the secret.
#[derive(Debug)]
pub struct Issued {
    /// Describe view.
    pub view: TokenView,
    /// Secret for later authentication.
    pub secret: Secret,
}

/// Fields for `issue`.
#[derive(Debug, Clone, Copy)]
pub struct IssueRequest<'a> {
    /// Authenticated requester.
    pub requester: &'a str,
    /// Requester host.
    pub host: &'a str,
    /// Token owner.
    pub owner: &'a str,
    /// Extra renewers. The owner is not required to be listed.
    pub renewers: &'a [&'a str],
    /// Current time, in milliseconds.
    pub now_ms: i64,
    /// Initial lifetime. Must be positive and no greater than the maximum.
    pub lifetime_ms: u64,
    /// Maximum lifetime from `now_ms`.
    pub max_lifetime_ms: u64,
}

/// Fields for `renew`.
#[derive(Debug, Clone, Copy)]
pub struct RenewRequest<'a> {
    /// Authenticated requester.
    pub requester: &'a str,
    /// Requester host.
    pub host: &'a str,
    /// Token id.
    pub token_id: &'a str,
    /// Current time, in milliseconds.
    pub now_ms: i64,
    /// Requested extension from `now_ms`.
    pub renew_ms: u64,
}

/// Why a token operation was refused.
#[derive(Debug)]
pub enum Error {
    /// The authorizer rejected the requester.
    Unauthorized(i16),
    /// The token does not exist or was revoked.
    NotFound,
    /// The requester is not the owner or a renewer.
    OwnerMismatch,
    /// The token's expiry is not after `now`.
    Expired,
    /// The presented secret does not match.
    AuthenticationFailed,
    /// The owner, lifetime, or renewer list is not usable.
    InvalidRequest,
    /// Randomness for a new secret was not available.
    Entropy,
    /// A failed append left this store unable to confirm further writes.
    Poisoned,
    /// The limits cannot store another token or journal record.
    InvalidLimits,
    /// The journal rejected the record or could not be recovered.
    Storage(journal::Error),
}

impl Error {
    /// Kafka code for a caller-visible refusal. Storage failures have none.
    pub fn code(&self) -> Option<i16> {
        match self {
            Self::Unauthorized(code) => Some(*code),
            Self::NotFound => Some(DELEGATION_TOKEN_NOT_FOUND),
            Self::OwnerMismatch => Some(DELEGATION_TOKEN_OWNER_MISMATCH),
            Self::Expired => Some(DELEGATION_TOKEN_EXPIRED),
            Self::AuthenticationFailed => Some(DELEGATION_TOKEN_AUTHORIZATION_FAILED),
            Self::InvalidRequest
            | Self::Entropy
            | Self::Poisoned
            | Self::InvalidLimits
            | Self::Storage(_) => None,
        }
    }
}

impl From<journal::Error> for Error {
    fn from(error: journal::Error) -> Self {
        Self::Storage(error)
    }
}

/// Token store bounds.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Limits {
    max_tokens: usize,
    max_records: usize,
    journal: journal::Limits,
}

impl Limits {
    /// Configure positive token and journal-record caps.
    pub fn new(
        max_tokens: usize,
        max_records: usize,
        journal: journal::Limits,
    ) -> Result<Self, Error> {
        if !(1..=100_000).contains(&max_tokens)
            || !(1..=1_000_000).contains(&max_records)
            || max_records < max_tokens
            || journal.max_entry_bytes() < 1024
            || journal.max_index_entries() < max_records
        {
            return Err(Error::InvalidLimits);
        }
        Ok(Self {
            max_tokens,
            max_records,
            journal,
        })
    }
}

struct Token {
    view: TokenView,
    secret: Secret,
}

/// Durable delegation tokens.
pub struct Tokens {
    limits: Limits,
    tokens: std::collections::BTreeMap<String, Token>,
    next_id: u64,
    journal: Journal,
    stored: usize,
    poisoned: bool,
}

impl Tokens {
    /// Open or recover the token store.
    pub fn open(path: impl AsRef<Path>, limits: Limits) -> Result<Self, Error> {
        let (journal, _) = Journal::open(path, 0, limits.journal)?;
        let mut tokens = Self {
            limits,
            tokens: std::collections::BTreeMap::new(),
            next_id: 1,
            journal,
            stored: 0,
            poisoned: false,
        };
        let mut cursor = tokens.journal.base_offset();
        loop {
            let batch =
                tokens
                    .journal
                    .fetch(cursor, 32, tokens.limits.journal.max_fetch_bytes())?;
            if batch.is_empty() {
                break;
            }
            for entry in batch {
                if entry.record_count != 1 {
                    return Err(Error::InvalidRequest);
                }
                tokens.apply(&entry.payload)?;
                tokens.stored = tokens.stored.saturating_add(1);
                cursor = entry
                    .first_offset
                    .checked_add(1)
                    .ok_or(Error::InvalidRequest)?;
            }
        }
        Ok(tokens)
    }

    /// Issue a token when the requester may create cluster resources.
    pub fn issue(&mut self, auth: &Authorizer, request: IssueRequest<'_>) -> Result<Issued, Error> {
        self.ready()?;
        self.allow(auth, request.requester, request.host, Operation::Create)?;
        if self.tokens.len() >= self.limits.max_tokens {
            return Err(Error::InvalidLimits);
        }
        let view = validate_issue(request, self.next_id)?;
        let secret = random_secret()?;
        let token_id = view.token_id.clone();
        self.persist(&encode_issue(&view, secret.expose())?)?;
        self.tokens.insert(
            token_id,
            Token {
                view: view.clone(),
                secret: secret.clone(),
            },
        );
        self.next_id = self.next_id.saturating_add(1);
        Ok(Issued { view, secret })
    }

    /// Extend expiry. The owner, a renewer, or a cluster `Alter` principal may renew.
    pub fn renew(&mut self, auth: &Authorizer, request: RenewRequest<'_>) -> Result<i64, Error> {
        self.ready()?;
        if request.now_ms < 0 || request.renew_ms == 0 {
            return Err(Error::InvalidRequest);
        }
        let (owner, renewers, expiry_ms, max_timestamp_ms) = {
            let token = self.tokens.get(request.token_id).ok_or(Error::NotFound)?;
            (
                token.view.owner.clone(),
                token.view.renewers.clone(),
                token.view.expiry_ms,
                token.view.max_timestamp_ms,
            )
        };
        if request.now_ms >= expiry_ms {
            return Err(Error::Expired);
        }
        if !self.can_renew(auth, request.requester, request.host, &owner, &renewers)? {
            return Err(Error::OwnerMismatch);
        }
        let extension = i64::try_from(request.renew_ms).map_err(|_| Error::InvalidRequest)?;
        let requested = request
            .now_ms
            .checked_add(extension)
            .ok_or(Error::InvalidRequest)?;
        let expiry = requested.min(max_timestamp_ms);
        if expiry == expiry_ms {
            return Ok(expiry);
        }
        let token_id = request.token_id.to_owned();
        self.persist(&encode_renew(&token_id, expiry)?)?;
        let token = self.tokens.get_mut(&token_id).ok_or(Error::NotFound)?;
        token.view.expiry_ms = expiry;
        Ok(expiry)
    }

    /// Revoke a token. A renewer who is not the owner cannot expire it.
    pub fn expire(
        &mut self,
        auth: &Authorizer,
        requester: &str,
        host: &str,
        token_id: &str,
    ) -> Result<(), Error> {
        self.ready()?;
        let owner = self
            .tokens
            .get(token_id)
            .ok_or(Error::NotFound)?
            .view
            .owner
            .clone();
        if owner != requester {
            self.allow(auth, requester, host, Operation::Delete)?;
        }
        let token_id = token_id.to_owned();
        self.persist(&encode_revoke(&token_id)?)?;
        self.tokens.remove(&token_id);
        Ok(())
    }

    /// Describe one token. The owner, or a principal allowed to `Describe`, may call it.
    pub fn view(
        &self,
        auth: &Authorizer,
        requester: &str,
        host: &str,
        token_id: &str,
    ) -> Result<TokenView, Error> {
        let token = self.tokens.get(token_id).ok_or(Error::NotFound)?;
        if token.view.owner != requester {
            self.allow(auth, requester, host, Operation::Describe)?;
        }
        Ok(token.view.clone())
    }

    /// Accept a token id and secret that are still unexpired.
    pub fn authenticate(&self, token_id: &str, secret: &[u8], now_ms: i64) -> Result<&str, Error> {
        let token = self.tokens.get(token_id).ok_or(Error::NotFound)?;
        if now_ms >= token.view.expiry_ms {
            return Err(Error::Expired);
        }
        if !secrets_match(token.secret.expose(), secret) {
            return Err(Error::AuthenticationFailed);
        }
        Ok(token.view.owner.as_str())
    }

    fn can_renew(
        &self,
        auth: &Authorizer,
        requester: &str,
        host: &str,
        owner: &str,
        renewers: &[String],
    ) -> Result<bool, Error> {
        if owner == requester || renewers.iter().any(|name| name == requester) {
            return Ok(true);
        }
        match self.allow(auth, requester, host, Operation::Alter) {
            Ok(()) => Ok(true),
            Err(Error::Unauthorized(_)) => Ok(false),
            Err(error) => Err(error),
        }
    }

    fn allow(
        &self,
        auth: &Authorizer,
        principal: &str,
        host: &str,
        operation: Operation,
    ) -> Result<(), Error> {
        auth.authorize(
            principal,
            host,
            Resource::Cluster,
            "kafka-cluster",
            operation,
        )
        .map_err(|error| {
            Error::Unauthorized(
                error
                    .code()
                    .unwrap_or(DELEGATION_TOKEN_AUTHORIZATION_FAILED),
            )
        })
    }

    fn apply(&mut self, payload: &[u8]) -> Result<(), Error> {
        match decode(payload)? {
            Record::Issue { view, secret } => {
                let id_number = view
                    .token_id
                    .strip_prefix("t-")
                    .and_then(|rest| rest.parse::<u64>().ok())
                    .ok_or(Error::InvalidRequest)?;
                self.next_id = self.next_id.max(id_number.saturating_add(1));
                self.tokens
                    .insert(view.token_id.clone(), Token { view, secret });
            }
            Record::Renew {
                token_id,
                expiry_ms,
            } => {
                let token = self
                    .tokens
                    .get_mut(&token_id)
                    .ok_or(Error::InvalidRequest)?;
                token.view.expiry_ms = expiry_ms;
            }
            Record::Revoke { token_id } => {
                self.tokens.remove(&token_id);
            }
        }
        Ok(())
    }

    fn ready(&self) -> Result<(), Error> {
        if self.poisoned || self.journal.is_poisoned() {
            Err(Error::Poisoned)
        } else {
            Ok(())
        }
    }

    fn persist(&mut self, payload: &[u8]) -> Result<(), Error> {
        if self.stored >= self.limits.max_records {
            return Err(Error::InvalidLimits);
        }
        match self.journal.append(1, payload) {
            Ok(_) => {
                self.stored = self.stored.saturating_add(1);
                Ok(())
            }
            Err(error) => {
                self.poisoned = true;
                Err(error.into())
            }
        }
    }
}

enum Record {
    Issue { view: TokenView, secret: Secret },
    Renew { token_id: String, expiry_ms: i64 },
    Revoke { token_id: String },
}

fn validate_issue(request: IssueRequest<'_>, next_id: u64) -> Result<TokenView, Error> {
    if request.now_ms < 0
        || request.lifetime_ms == 0
        || request.max_lifetime_ms < request.lifetime_ms
        || request.renewers.len() > 16
        || !valid_principal(request.owner)
    {
        return Err(Error::InvalidRequest);
    }
    let mut renewers = BTreeSet::new();
    for renewer in request.renewers {
        if !valid_principal(renewer) || renewer == &request.owner {
            return Err(Error::InvalidRequest);
        }
        renewers.insert((*renewer).to_owned());
    }
    let lifetime = i64::try_from(request.lifetime_ms).map_err(|_| Error::InvalidRequest)?;
    let max_lifetime = i64::try_from(request.max_lifetime_ms).map_err(|_| Error::InvalidRequest)?;
    let expiry_ms = request
        .now_ms
        .checked_add(lifetime)
        .ok_or(Error::InvalidRequest)?;
    let max_timestamp_ms = request
        .now_ms
        .checked_add(max_lifetime)
        .ok_or(Error::InvalidRequest)?;
    Ok(TokenView {
        token_id: format!("t-{next_id}"),
        owner: request.owner.to_owned(),
        renewers: renewers.into_iter().collect(),
        issued_at_ms: request.now_ms,
        expiry_ms,
        max_timestamp_ms,
    })
}

fn valid_principal(text: &str) -> bool {
    !text.is_empty() && text.len() <= 249 && !text.chars().any(char::is_control)
}

fn random_secret() -> Result<Secret, Error> {
    let mut bytes = [0u8; 32];
    getrandom::fill(&mut bytes).map_err(|_| Error::Entropy)?;
    Ok(Secret(bytes))
}

fn secrets_match(stored: &[u8; 32], presented: &[u8]) -> bool {
    let mut diff = 0u8;
    let mut presented_bytes = [0u8; 32];
    if presented.len() == 32 {
        presented_bytes.copy_from_slice(presented);
    } else {
        diff = 1;
    }
    for (left, right) in stored.iter().zip(presented_bytes.iter()) {
        diff |= left ^ right;
    }
    diff == 0
}

fn encode_issue(view: &TokenView, secret: &[u8; 32]) -> Result<Vec<u8>, Error> {
    let mut bytes = Vec::new();
    bytes.extend_from_slice(RECORD_MAGIC);
    bytes.push(KIND_ISSUE);
    put_view(&mut bytes, view)?;
    bytes.extend_from_slice(secret);
    Ok(bytes)
}

fn encode_renew(token_id: &str, expiry_ms: i64) -> Result<Vec<u8>, Error> {
    let mut bytes = Vec::new();
    bytes.extend_from_slice(RECORD_MAGIC);
    bytes.push(KIND_RENEW);
    put_str(&mut bytes, token_id)?;
    bytes.extend_from_slice(&expiry_ms.to_be_bytes());
    Ok(bytes)
}

fn encode_revoke(token_id: &str) -> Result<Vec<u8>, Error> {
    let mut bytes = Vec::new();
    bytes.extend_from_slice(RECORD_MAGIC);
    bytes.push(KIND_REVOKE);
    put_str(&mut bytes, token_id)?;
    Ok(bytes)
}

fn put_view(bytes: &mut Vec<u8>, view: &TokenView) -> Result<(), Error> {
    put_str(bytes, &view.token_id)?;
    put_str(bytes, &view.owner)?;
    let count = u16::try_from(view.renewers.len()).map_err(|_| Error::InvalidRequest)?;
    bytes.extend_from_slice(&count.to_be_bytes());
    for renewer in &view.renewers {
        put_str(bytes, renewer)?;
    }
    bytes.extend_from_slice(&view.issued_at_ms.to_be_bytes());
    bytes.extend_from_slice(&view.expiry_ms.to_be_bytes());
    bytes.extend_from_slice(&view.max_timestamp_ms.to_be_bytes());
    Ok(())
}

fn decode(mut bytes: &[u8]) -> Result<Record, Error> {
    if bytes.len() < RECORD_MAGIC.len() + 1 || &bytes[..RECORD_MAGIC.len()] != RECORD_MAGIC {
        return Err(Error::InvalidRequest);
    }
    let kind = bytes[RECORD_MAGIC.len()];
    bytes = &bytes[RECORD_MAGIC.len() + 1..];
    let record = match kind {
        KIND_ISSUE => {
            let view = take_view(&mut bytes)?;
            let mut secret = [0u8; 32];
            secret.copy_from_slice(take(&mut bytes, 32)?);
            Record::Issue {
                view,
                secret: Secret(secret),
            }
        }
        KIND_RENEW => Record::Renew {
            token_id: take_str(&mut bytes)?,
            expiry_ms: take_i64(&mut bytes)?,
        },
        KIND_REVOKE => Record::Revoke {
            token_id: take_str(&mut bytes)?,
        },
        _ => return Err(Error::InvalidRequest),
    };
    if !bytes.is_empty() {
        return Err(Error::InvalidRequest);
    }
    Ok(record)
}

fn take_view(bytes: &mut &[u8]) -> Result<TokenView, Error> {
    let token_id = take_str(bytes)?;
    let owner = take_str(bytes)?;
    let count = u16::from_be_bytes(
        take(bytes, 2)?
            .try_into()
            .map_err(|_| Error::InvalidRequest)?,
    );
    let mut renewers = Vec::new();
    for _ in 0..count {
        renewers.push(take_str(bytes)?);
    }
    Ok(TokenView {
        token_id,
        owner,
        renewers,
        issued_at_ms: take_i64(bytes)?,
        expiry_ms: take_i64(bytes)?,
        max_timestamp_ms: take_i64(bytes)?,
    })
}

fn put_str(bytes: &mut Vec<u8>, value: &str) -> Result<(), Error> {
    let len = u16::try_from(value.len()).map_err(|_| Error::InvalidRequest)?;
    bytes.extend_from_slice(&len.to_be_bytes());
    bytes.extend_from_slice(value.as_bytes());
    Ok(())
}

fn take<'a>(input: &mut &'a [u8], len: usize) -> Result<&'a [u8], Error> {
    if input.len() < len {
        return Err(Error::InvalidRequest);
    }
    let (head, tail) = input.split_at(len);
    *input = tail;
    Ok(head)
}

fn take_str(input: &mut &[u8]) -> Result<String, Error> {
    let len = usize::from(u16::from_be_bytes(
        take(input, 2)?
            .try_into()
            .map_err(|_| Error::InvalidRequest)?,
    ));
    let bytes = take(input, len)?;
    std::str::from_utf8(bytes)
        .map(|text| text.to_owned())
        .map_err(|_| Error::InvalidRequest)
}

fn take_i64(input: &mut &[u8]) -> Result<i64, Error> {
    let mut value = [0u8; 8];
    value.copy_from_slice(take(input, 8)?);
    Ok(i64::from_be_bytes(value))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::security::acl::{Entry, Pattern, Permission};
    use std::time::{SystemTime, UNIX_EPOCH};

    fn journal_limits() -> journal::Limits {
        journal::Limits::new(1024, 1 << 20, 100, 1 << 20)
            .unwrap_or_else(|_| panic!("journal limits"))
    }

    fn token_limits() -> Limits {
        Limits::new(8, 32, journal_limits()).unwrap_or_else(|_| panic!("token limits"))
    }

    fn acl_limits() -> crate::security::acl::Limits {
        crate::security::acl::Limits::new(32, journal_limits())
            .unwrap_or_else(|_| panic!("acl limits"))
    }

    fn path(name: &str) -> std::path::PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();
        std::env::temp_dir().join(format!("pl-tkn-{name}-{nanos}"))
    }

    fn allow(auth: &mut Authorizer, principal: &str, operation: Operation) {
        auth.grant(Entry {
            resource: Resource::Cluster,
            pattern: Pattern::Literal,
            name: "kafka-cluster".to_owned(),
            operation,
            principal: principal.to_owned(),
            host: "*".to_owned(),
            permission: Permission::Allow,
        })
        .unwrap_or_else(|error| panic!("{error:?}"));
    }

    #[test]
    fn owner_renewer_expiry_and_redacted_secret_survive_restart() {
        let acl_path = path("acl");
        let mut auth =
            Authorizer::open(&acl_path, acl_limits()).unwrap_or_else(|error| panic!("{error:?}"));
        allow(&mut auth, "User:alice", Operation::Create);
        let token_path = path("tokens");
        let mut tokens =
            Tokens::open(&token_path, token_limits()).unwrap_or_else(|error| panic!("{error:?}"));
        assert!(matches!(
            tokens.issue(
                &auth,
                IssueRequest {
                    requester: "User:bob",
                    host: "*",
                    owner: "User:bob",
                    renewers: &[],
                    now_ms: 0,
                    lifetime_ms: 1_000,
                    max_lifetime_ms: 5_000,
                },
            ),
            Err(Error::Unauthorized(_))
        ));
        assert!(tokens.tokens.is_empty());
        let issued = tokens
            .issue(
                &auth,
                IssueRequest {
                    requester: "User:alice",
                    host: "*",
                    owner: "User:alice",
                    renewers: &["User:carol"],
                    now_ms: 0,
                    lifetime_ms: 1_000,
                    max_lifetime_ms: 5_000,
                },
            )
            .unwrap_or_else(|error| panic!("{error:?}"));
        let secret_hex: String = issued
            .secret
            .expose()
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect();
        assert!(!format!("{issued:?}").contains(&secret_hex));
        let described = tokens
            .view(&auth, "User:alice", "*", &issued.view.token_id)
            .unwrap_or_else(|error| panic!("{error:?}"));
        assert!(!format!("{described:?}").contains(&secret_hex));
        assert!(matches!(
            tokens.view(&auth, "User:bob", "*", &issued.view.token_id),
            Err(Error::Unauthorized(_))
        ));
        assert_eq!(
            tokens
                .authenticate(&issued.view.token_id, issued.secret.expose(), 500)
                .unwrap_or_else(|error| panic!("{error:?}")),
            "User:alice"
        );
        assert!(matches!(
            tokens.authenticate(&issued.view.token_id, &[0; 32], 500),
            Err(Error::AuthenticationFailed)
        ));
        let expiry = tokens
            .renew(
                &auth,
                RenewRequest {
                    requester: "User:carol",
                    host: "*",
                    token_id: &issued.view.token_id,
                    now_ms: 500,
                    renew_ms: 2_000,
                },
            )
            .unwrap_or_else(|error| panic!("{error:?}"));
        assert_eq!(expiry, 2_500);
        assert!(matches!(
            tokens.renew(
                &auth,
                RenewRequest {
                    requester: "User:bob",
                    host: "*",
                    token_id: &issued.view.token_id,
                    now_ms: 500,
                    renew_ms: 1_000,
                },
            ),
            Err(Error::OwnerMismatch)
        ));
        assert!(matches!(
            tokens.expire(&auth, "User:carol", "*", &issued.view.token_id),
            Err(Error::Unauthorized(_))
        ));
        assert!(matches!(
            tokens.authenticate(&issued.view.token_id, issued.secret.expose(), 3_000),
            Err(Error::Expired)
        ));
        tokens
            .expire(&auth, "User:alice", "*", &issued.view.token_id)
            .unwrap_or_else(|error| panic!("{error:?}"));
        assert!(matches!(
            tokens.authenticate(&issued.view.token_id, issued.secret.expose(), 600),
            Err(Error::NotFound)
        ));
        let kept = tokens
            .issue(
                &auth,
                IssueRequest {
                    requester: "User:alice",
                    host: "*",
                    owner: "User:alice",
                    renewers: &["User:carol"],
                    now_ms: 0,
                    lifetime_ms: 1_000,
                    max_lifetime_ms: 5_000,
                },
            )
            .unwrap_or_else(|error| panic!("{error:?}"));
        let kept_secret = kept.secret.expose().to_vec();
        let kept_hex: String = kept_secret
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect();
        let kept_id = kept.view.token_id.clone();
        drop(tokens);
        let restored =
            Tokens::open(&token_path, token_limits()).unwrap_or_else(|error| panic!("{error:?}"));
        let restored_view = restored
            .view(&auth, "User:alice", "*", &kept_id)
            .unwrap_or_else(|error| panic!("{error:?}"));
        assert!(!format!("{restored_view:?}").contains(&kept_hex));
        assert_eq!(
            restored
                .authenticate(&kept_id, &kept_secret, 100)
                .unwrap_or_else(|error| panic!("{error:?}")),
            "User:alice"
        );
        assert!(matches!(
            restored.authenticate("t-1", &kept_secret, 100),
            Err(Error::NotFound)
        ));
    }
}

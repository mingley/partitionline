//! Persistent principal, resource, and operation authorization.
//!
//! No matching allow denies the operation. A matching deny beats a matching
//! allow. A superuser is allowed before ACL evaluation. Literal, prefixed, and
//! wildcard patterns are stored in a journal and restored on reopen.
//!
//! This authorizer is not consulted by the listener, produce, or fetch path.

use crate::journal::{self, Journal};
use std::collections::BTreeSet;
use std::path::Path;

const RECORD_MAGIC: &[u8; 8] = b"PLACL001";
const KIND_GRANT: u8 = 1;
const KIND_REVOKE: u8 = 2;
const KIND_SUPER: u8 = 3;
const KIND_UNSUPER: u8 = 4;

/// Kafka `TOPIC_AUTHORIZATION_FAILED`.
pub const TOPIC_AUTHORIZATION_FAILED: i16 = 29;
/// Kafka `GROUP_AUTHORIZATION_FAILED`.
pub const GROUP_AUTHORIZATION_FAILED: i16 = 30;
/// Kafka `CLUSTER_AUTHORIZATION_FAILED`.
pub const CLUSTER_AUTHORIZATION_FAILED: i16 = 31;
/// Kafka `TRANSACTIONAL_ID_AUTHORIZATION_FAILED`.
pub const TRANSACTIONAL_ID_AUTHORIZATION_FAILED: i16 = 53;

/// A resource class that can be authorized.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum Resource {
    /// The cluster resource.
    Cluster,
    /// A topic.
    Topic,
    /// A consumer group.
    Group,
    /// A transactional id.
    TransactionalId,
}

/// How an ACL name is compared.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum Pattern {
    /// Exact name.
    Literal,
    /// Name prefix.
    Prefixed,
    /// Every name of this resource class.
    Any,
}

/// An operation that can be allowed or denied.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum Operation {
    /// Read data or offsets.
    Read,
    /// Produce or commit.
    Write,
    /// Create a resource.
    Create,
    /// Delete a resource.
    Delete,
    /// Mutate resource metadata.
    Alter,
    /// Describe a resource.
    Describe,
    /// Controller and cluster mutation.
    ClusterAction,
    /// Idempotent produce.
    IdempotentWrite,
    /// Every operation.
    All,
}

/// Allow or deny.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Permission {
    /// The match permits the operation.
    Allow,
    /// The match refuses the operation.
    Deny,
}

/// One stored ACL binding.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Entry {
    /// Resource class.
    pub resource: Resource,
    /// Name pattern.
    pub pattern: Pattern,
    /// Literal or prefix. Empty when the pattern is [`Pattern::Any`].
    pub name: String,
    /// Operation, or [`Operation::All`].
    pub operation: Operation,
    /// Principal, or `User:*`.
    pub principal: String,
    /// Host, or `*`.
    pub host: String,
    /// Allow or deny.
    pub permission: Permission,
}

/// Why an authorization change or check was refused.
#[derive(Debug)]
pub enum Error {
    /// The principal, host, or name is not usable.
    InvalidRequest,
    /// No allow matched, or a deny matched.
    Denied(Resource),
    /// The binding is not stored.
    NotFound,
    /// A failed append left this authorizer unable to confirm further writes.
    Poisoned,
    /// The limits cannot store the bindings.
    InvalidLimits,
    /// The journal rejected the record or could not be recovered.
    Storage(journal::Error),
}

impl Error {
    /// Kafka code for a denial. Other failures have none.
    pub fn code(&self) -> Option<i16> {
        match self {
            Self::Denied(Resource::Topic) => Some(TOPIC_AUTHORIZATION_FAILED),
            Self::Denied(Resource::Group) => Some(GROUP_AUTHORIZATION_FAILED),
            Self::Denied(Resource::Cluster) => Some(CLUSTER_AUTHORIZATION_FAILED),
            Self::Denied(Resource::TransactionalId) => Some(TRANSACTIONAL_ID_AUTHORIZATION_FAILED),
            Self::InvalidRequest
            | Self::NotFound
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

/// Authorizer bounds.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Limits {
    max_entries: usize,
    journal: journal::Limits,
}

impl Limits {
    /// Configure a positive binding cap.
    pub fn new(max_entries: usize, journal: journal::Limits) -> Result<Self, Error> {
        if !(1..=100_000).contains(&max_entries)
            || journal.max_entry_bytes() < 1024
            || journal.max_index_entries() < max_entries
        {
            return Err(Error::InvalidLimits);
        }
        Ok(Self {
            max_entries,
            journal,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct Key {
    resource: Resource,
    pattern: Pattern,
    name: String,
    operation: Operation,
    principal: String,
    host: String,
}

/// Durable allow and deny bindings.
pub struct Authorizer {
    limits: Limits,
    entries: std::collections::BTreeMap<Key, Permission>,
    superusers: BTreeSet<String>,
    journal: Journal,
    stored: usize,
    poisoned: bool,
}

impl Authorizer {
    /// Open or recover the authorizer.
    pub fn open(path: impl AsRef<Path>, limits: Limits) -> Result<Self, Error> {
        let (journal, _) = Journal::open(path, 0, limits.journal)?;
        let mut authorizer = Self {
            limits,
            entries: std::collections::BTreeMap::new(),
            superusers: BTreeSet::new(),
            journal,
            stored: 0,
            poisoned: false,
        };
        let mut cursor = authorizer.journal.base_offset();
        loop {
            let batch = authorizer.journal.fetch(
                cursor,
                32,
                authorizer.limits.journal.max_fetch_bytes(),
            )?;
            if batch.is_empty() {
                break;
            }
            for entry in batch {
                if entry.record_count != 1 {
                    return Err(Error::InvalidRequest);
                }
                authorizer.apply(&entry.payload)?;
                authorizer.stored = authorizer.stored.saturating_add(1);
                cursor = entry
                    .first_offset
                    .checked_add(1)
                    .ok_or(Error::InvalidRequest)?;
            }
        }
        Ok(authorizer)
    }

    /// Store or replace a binding.
    pub fn grant(&mut self, entry: Entry) -> Result<(), Error> {
        self.ready()?;
        validate_entry(&entry)?;
        let key = key_from(&entry);
        if !self.entries.contains_key(&key) && self.entries.len() >= self.limits.max_entries {
            return Err(Error::InvalidLimits);
        }
        self.persist(&encode_grant(&entry)?)?;
        self.entries.insert(key, entry.permission);
        Ok(())
    }

    /// Remove a binding. The permission field is ignored.
    pub fn revoke(&mut self, entry: &Entry) -> Result<(), Error> {
        self.ready()?;
        validate_entry(entry)?;
        let key = key_from(entry);
        if !self.entries.contains_key(&key) {
            return Err(Error::NotFound);
        }
        self.persist(&encode_revoke(entry)?)?;
        self.entries.remove(&key);
        Ok(())
    }

    /// Allow this principal before ACL evaluation.
    pub fn add_superuser(&mut self, principal: &str) -> Result<(), Error> {
        self.ready()?;
        validate_principal(principal)?;
        if self.superusers.contains(principal) {
            return Ok(());
        }
        if self.superusers.len() >= self.limits.max_entries {
            return Err(Error::InvalidLimits);
        }
        self.persist(&encode_principal(KIND_SUPER, principal)?)?;
        self.superusers.insert(principal.to_owned());
        Ok(())
    }

    /// Remove a superuser. Existing ACL bindings stay.
    pub fn remove_superuser(&mut self, principal: &str) -> Result<(), Error> {
        self.ready()?;
        validate_principal(principal)?;
        if !self.superusers.contains(principal) {
            return Err(Error::NotFound);
        }
        self.persist(&encode_principal(KIND_UNSUPER, principal)?)?;
        self.superusers.remove(principal);
        Ok(())
    }

    /// Allow a superuser or a matching allow that is not denied.
    pub fn authorize(
        &self,
        principal: &str,
        host: &str,
        resource: Resource,
        name: &str,
        operation: Operation,
    ) -> Result<(), Error> {
        if self.superusers.contains(principal) {
            return Ok(());
        }
        let mut allowed = false;
        for (key, permission) in &self.entries {
            if !matches_binding(key, principal, host, resource, name, operation) {
                continue;
            }
            if *permission == Permission::Deny {
                return Err(Error::Denied(resource));
            }
            allowed = true;
        }
        if allowed {
            Ok(())
        } else {
            Err(Error::Denied(resource))
        }
    }

    fn apply(&mut self, payload: &[u8]) -> Result<(), Error> {
        match decode(payload)? {
            Record::Grant(entry) => {
                self.entries.insert(key_from(&entry), entry.permission);
            }
            Record::Revoke(entry) => {
                self.entries.remove(&key_from(&entry));
            }
            Record::Super(principal) => {
                self.superusers.insert(principal);
            }
            Record::Unsuper(principal) => {
                self.superusers.remove(&principal);
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
        if self.stored >= self.limits.max_entries.saturating_mul(4) {
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
    Grant(Entry),
    Revoke(Entry),
    Super(String),
    Unsuper(String),
}

fn matches_binding(
    key: &Key,
    principal: &str,
    host: &str,
    resource: Resource,
    name: &str,
    operation: Operation,
) -> bool {
    if key.resource != resource {
        return false;
    }
    let name_ok = match key.pattern {
        Pattern::Literal => key.name == name,
        Pattern::Prefixed => name.starts_with(&key.name) && !key.name.is_empty(),
        Pattern::Any => true,
    };
    let operation_ok = key.operation == Operation::All || key.operation == operation;
    let principal_ok = key.principal == "User:*" || key.principal == principal;
    let host_ok = key.host == "*" || key.host == host;
    name_ok && operation_ok && principal_ok && host_ok
}

fn key_from(entry: &Entry) -> Key {
    Key {
        resource: entry.resource,
        pattern: entry.pattern,
        name: entry.name.clone(),
        operation: entry.operation,
        principal: entry.principal.clone(),
        host: entry.host.clone(),
    }
}

fn validate_entry(entry: &Entry) -> Result<(), Error> {
    validate_principal(&entry.principal)?;
    validate_text(&entry.host)?;
    if entry.pattern == Pattern::Any {
        if entry.name.is_empty() {
            Ok(())
        } else {
            Err(Error::InvalidRequest)
        }
    } else if valid_text(&entry.name) {
        Ok(())
    } else {
        Err(Error::InvalidRequest)
    }
}

fn validate_principal(principal: &str) -> Result<(), Error> {
    if principal == "User:*" || valid_text(principal) {
        Ok(())
    } else {
        Err(Error::InvalidRequest)
    }
}

fn validate_text(text: &str) -> Result<(), Error> {
    if text == "*" || valid_text(text) {
        Ok(())
    } else {
        Err(Error::InvalidRequest)
    }
}

fn valid_text(text: &str) -> bool {
    !text.is_empty() && text.len() <= 249 && !text.chars().any(char::is_control)
}

fn encode_grant(entry: &Entry) -> Result<Vec<u8>, Error> {
    let mut bytes = Vec::new();
    bytes.extend_from_slice(RECORD_MAGIC);
    bytes.push(KIND_GRANT);
    put_entry(&mut bytes, entry)?;
    Ok(bytes)
}

fn encode_revoke(entry: &Entry) -> Result<Vec<u8>, Error> {
    let mut bytes = Vec::new();
    bytes.extend_from_slice(RECORD_MAGIC);
    bytes.push(KIND_REVOKE);
    put_entry(&mut bytes, entry)?;
    Ok(bytes)
}

fn encode_principal(kind: u8, principal: &str) -> Result<Vec<u8>, Error> {
    let mut bytes = Vec::new();
    bytes.extend_from_slice(RECORD_MAGIC);
    bytes.push(kind);
    put_str(&mut bytes, principal)?;
    Ok(bytes)
}

fn put_entry(bytes: &mut Vec<u8>, entry: &Entry) -> Result<(), Error> {
    bytes.push(resource_byte(entry.resource));
    bytes.push(pattern_byte(entry.pattern));
    put_str(bytes, &entry.name)?;
    bytes.push(operation_byte(entry.operation));
    put_str(bytes, &entry.principal)?;
    put_str(bytes, &entry.host)?;
    bytes.push(match entry.permission {
        Permission::Allow => 1,
        Permission::Deny => 0,
    });
    Ok(())
}

fn resource_byte(resource: Resource) -> u8 {
    match resource {
        Resource::Cluster => 1,
        Resource::Topic => 2,
        Resource::Group => 3,
        Resource::TransactionalId => 4,
    }
}

fn pattern_byte(pattern: Pattern) -> u8 {
    match pattern {
        Pattern::Literal => 1,
        Pattern::Prefixed => 2,
        Pattern::Any => 3,
    }
}

fn operation_byte(operation: Operation) -> u8 {
    match operation {
        Operation::Read => 1,
        Operation::Write => 2,
        Operation::Create => 3,
        Operation::Delete => 4,
        Operation::Alter => 5,
        Operation::Describe => 6,
        Operation::ClusterAction => 7,
        Operation::IdempotentWrite => 8,
        Operation::All => 9,
    }
}

fn decode(mut bytes: &[u8]) -> Result<Record, Error> {
    if bytes.len() < RECORD_MAGIC.len() + 1 || &bytes[..RECORD_MAGIC.len()] != RECORD_MAGIC {
        return Err(Error::InvalidRequest);
    }
    let kind = bytes[RECORD_MAGIC.len()];
    bytes = &bytes[RECORD_MAGIC.len() + 1..];
    let record = match kind {
        KIND_GRANT => Record::Grant(take_entry(&mut bytes)?),
        KIND_REVOKE => Record::Revoke(take_entry(&mut bytes)?),
        KIND_SUPER => Record::Super(take_str(&mut bytes)?),
        KIND_UNSUPER => Record::Unsuper(take_str(&mut bytes)?),
        _ => return Err(Error::InvalidRequest),
    };
    if !bytes.is_empty() {
        return Err(Error::InvalidRequest);
    }
    Ok(record)
}

fn take_entry(bytes: &mut &[u8]) -> Result<Entry, Error> {
    Ok(Entry {
        resource: resource_from(take_u8(bytes)?)?,
        pattern: pattern_from(take_u8(bytes)?)?,
        name: take_str(bytes)?,
        operation: operation_from(take_u8(bytes)?)?,
        principal: take_str(bytes)?,
        host: take_str(bytes)?,
        permission: match take_u8(bytes)? {
            1 => Permission::Allow,
            0 => Permission::Deny,
            _ => return Err(Error::InvalidRequest),
        },
    })
}

fn resource_from(byte: u8) -> Result<Resource, Error> {
    match byte {
        1 => Ok(Resource::Cluster),
        2 => Ok(Resource::Topic),
        3 => Ok(Resource::Group),
        4 => Ok(Resource::TransactionalId),
        _ => Err(Error::InvalidRequest),
    }
}

fn pattern_from(byte: u8) -> Result<Pattern, Error> {
    match byte {
        1 => Ok(Pattern::Literal),
        2 => Ok(Pattern::Prefixed),
        3 => Ok(Pattern::Any),
        _ => Err(Error::InvalidRequest),
    }
}

fn operation_from(byte: u8) -> Result<Operation, Error> {
    match byte {
        1 => Ok(Operation::Read),
        2 => Ok(Operation::Write),
        3 => Ok(Operation::Create),
        4 => Ok(Operation::Delete),
        5 => Ok(Operation::Alter),
        6 => Ok(Operation::Describe),
        7 => Ok(Operation::ClusterAction),
        8 => Ok(Operation::IdempotentWrite),
        9 => Ok(Operation::All),
        _ => Err(Error::InvalidRequest),
    }
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

fn take_u8(input: &mut &[u8]) -> Result<u8, Error> {
    Ok(take(input, 1)?[0])
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn limits() -> Limits {
        let journal = journal::Limits::new(1024, 1 << 20, 100, 1 << 20)
            .unwrap_or_else(|_| panic!("journal limits"));
        Limits::new(32, journal).unwrap_or_else(|_| panic!("acl limits"))
    }

    fn open(name: &str) -> (Authorizer, std::path::PathBuf) {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();
        let path = std::env::temp_dir().join(format!("pl-acl-{name}-{nanos}"));
        let authorizer =
            Authorizer::open(&path, limits()).unwrap_or_else(|error| panic!("{error:?}"));
        (authorizer, path)
    }

    fn entry(
        resource: Resource,
        pattern: Pattern,
        name: &str,
        operation: Operation,
        principal: &str,
        host: &str,
        permission: Permission,
    ) -> Entry {
        Entry {
            resource,
            pattern,
            name: name.to_owned(),
            operation,
            principal: principal.to_owned(),
            host: host.to_owned(),
            permission,
        }
    }

    #[test]
    fn deny_beats_allow_superuser_bypasses_and_reopen_restores() {
        let (mut authorizer, path) = open("authz");
        assert!(matches!(
            authorizer.authorize(
                "User:alice",
                "*",
                Resource::Topic,
                "orders",
                Operation::Describe
            ),
            Err(Error::Denied(Resource::Topic))
        ));
        assert_eq!(
            authorizer
                .authorize(
                    "User:alice",
                    "*",
                    Resource::Cluster,
                    "kafka-cluster",
                    Operation::Alter
                )
                .unwrap_err()
                .code(),
            Some(CLUSTER_AUTHORIZATION_FAILED)
        );
        authorizer
            .grant(entry(
                Resource::Topic,
                Pattern::Prefixed,
                "ord",
                Operation::Read,
                "User:*",
                "*",
                Permission::Allow,
            ))
            .unwrap_or_else(|error| panic!("{error:?}"));
        authorizer
            .authorize(
                "User:bob",
                "10.0.0.8",
                Resource::Topic,
                "orders",
                Operation::Read,
            )
            .unwrap_or_else(|error| panic!("{error:?}"));
        assert!(matches!(
            authorizer.authorize("User:bob", "*", Resource::Topic, "orders", Operation::Write),
            Err(Error::Denied(Resource::Topic))
        ));
        authorizer
            .grant(entry(
                Resource::Topic,
                Pattern::Literal,
                "orders",
                Operation::Read,
                "User:bob",
                "*",
                Permission::Deny,
            ))
            .unwrap_or_else(|error| panic!("{error:?}"));
        assert!(matches!(
            authorizer.authorize("User:bob", "*", Resource::Topic, "orders", Operation::Read),
            Err(Error::Denied(Resource::Topic))
        ));
        authorizer
            .add_superuser("User:carol")
            .unwrap_or_else(|error| panic!("{error:?}"));
        authorizer
            .authorize(
                "User:carol",
                "*",
                Resource::Group,
                "shippers",
                Operation::Delete,
            )
            .unwrap_or_else(|error| panic!("{error:?}"));
        authorizer
            .authorize(
                "User:carol",
                "*",
                Resource::TransactionalId,
                "tx",
                Operation::Write,
            )
            .unwrap_or_else(|error| panic!("{error:?}"));
        drop(authorizer);
        let restored =
            Authorizer::open(&path, limits()).unwrap_or_else(|error| panic!("{error:?}"));
        assert!(matches!(
            restored.authorize("User:bob", "*", Resource::Topic, "orders", Operation::Read),
            Err(Error::Denied(Resource::Topic))
        ));
        restored
            .authorize(
                "User:carol",
                "*",
                Resource::Cluster,
                "kafka-cluster",
                Operation::ClusterAction,
            )
            .unwrap_or_else(|error| panic!("{error:?}"));
        restored
            .authorize(
                "User:alice",
                "*",
                Resource::Topic,
                "orders",
                Operation::Read,
            )
            .unwrap_or_else(|error| panic!("{error:?}"));
    }
}

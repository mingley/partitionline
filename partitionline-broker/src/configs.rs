//! Validated dynamic broker and topic configuration.
//!
//! Describe, alter, and incremental alter apply a fixed schema. Read-only and
//! sensitive keys are rejected. Validate-only checks every operation and
//! writes nothing. A successful alter journals the whole resource once, so a
//! failed record leaves the previous values in place. In-flight task and
//! budget counters are not part of that record and are not cleared by an alter.
//!
//! This is not the Kafka DescribeConfigs or AlterConfigs RPC.

use crate::catalog;
use crate::journal::{self, Journal};
use std::collections::BTreeMap;
use std::path::Path;

const RECORD_MAGIC: &[u8; 8] = b"PLCONF01";

/// Kafka `INVALID_CONFIG`.
pub const INVALID_CONFIG: i16 = 40;
/// Kafka `INVALID_REQUEST`.
pub const INVALID_REQUEST: i16 = 42;

/// Kafka `ConfigResource` type.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum ResourceKind {
    /// A topic name.
    Topic = 2,
    /// A broker id.
    Broker = 4,
}

/// Incremental alter operation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Op {
    /// Replace the dynamic value.
    Set = 0,
    /// Remove the dynamic value and restore the default.
    Delete = 1,
    /// Add one list entry.
    Append = 2,
    /// Remove one list entry.
    Subtract = 3,
}

/// One config key to change.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Alter {
    /// Config name.
    pub name: String,
    /// Operation.
    pub op: Op,
    /// Value for set, append, and subtract. Ignored for delete.
    pub value: Option<String>,
}

/// One described config entry.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Entry {
    /// Config name.
    pub name: String,
    /// Effective value. Sensitive keys are `None`.
    pub value: Option<String>,
    /// True when a dynamic override is stored.
    pub dynamic: bool,
    /// True when the value is not returned.
    pub sensitive: bool,
    /// True when alter cannot change the key.
    pub read_only: bool,
}

/// Why a config change was refused.
#[derive(Debug)]
pub enum Error {
    /// The name, value, or operation is not valid for this key.
    InvalidConfig,
    /// The resource type or name is not usable.
    InvalidRequest,
    /// A failed append left this store unable to confirm further changes.
    Poisoned,
    /// The limits cannot store the resource.
    InvalidLimits,
    /// The journal rejected the record or could not be recovered.
    Storage(journal::Error),
}

impl Error {
    /// Kafka code for a caller-visible refusal. Storage failures have none.
    pub fn code(&self) -> Option<i16> {
        Some(match self {
            Self::InvalidConfig => INVALID_CONFIG,
            Self::InvalidRequest => INVALID_REQUEST,
            Self::Poisoned | Self::InvalidLimits | Self::Storage(_) => return None,
        })
    }
}

impl From<journal::Error> for Error {
    fn from(error: journal::Error) -> Self {
        Self::Storage(error)
    }
}

/// Resource and journal bounds.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Limits {
    max_resources: usize,
    max_records: usize,
    journal: journal::Limits,
}

impl Limits {
    /// Configure positive resource and record caps.
    pub fn new(
        max_resources: usize,
        max_records: usize,
        journal: journal::Limits,
    ) -> Result<Self, Error> {
        if !(1..=100_000).contains(&max_resources)
            || !(1..=1_000_000).contains(&max_records)
            || journal.max_entry_bytes() < 4096
            || journal.max_index_entries() < max_records
        {
            return Err(Error::InvalidLimits);
        }
        Ok(Self {
            max_resources,
            max_records,
            journal,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct Resource {
    values: BTreeMap<String, String>,
}

/// Durable dynamic configuration for broker and topic resources.
pub struct ConfigStore {
    limits: Limits,
    resources: BTreeMap<(ResourceKind, String), Resource>,
    inflight_tasks: u64,
    budget_bytes: u64,
    journal: Journal,
    stored: usize,
    poisoned: bool,
}

impl ConfigStore {
    /// Open or recover the config journal.
    pub fn open(path: impl AsRef<Path>, limits: Limits) -> Result<Self, Error> {
        let (journal, _) = Journal::open(path, 0, limits.journal)?;
        let mut store = Self {
            limits,
            resources: BTreeMap::new(),
            inflight_tasks: 0,
            budget_bytes: 0,
            journal,
            stored: 0,
            poisoned: false,
        };
        let mut offset = store.journal.base_offset();
        loop {
            let batch = store
                .journal
                .fetch(offset, 32, store.limits.journal.max_fetch_bytes())?;
            if batch.is_empty() {
                break;
            }
            for entry in batch {
                if entry.record_count != 1 {
                    return Err(Error::InvalidConfig);
                }
                store.apply(&entry.payload)?;
                store.stored = store.stored.saturating_add(1);
                offset = entry
                    .first_offset
                    .checked_add(1)
                    .ok_or(Error::InvalidConfig)?;
            }
        }
        Ok(store)
    }

    /// Tasks currently in flight. Alters do not change this counter.
    pub fn inflight_tasks(&self) -> u64 {
        self.inflight_tasks
    }

    /// Remaining byte budget. Alters do not change this counter.
    pub fn budget_bytes(&self) -> u64 {
        self.budget_bytes
    }

    /// Record in-flight work. This is not a config key.
    pub fn set_runtime_budget(&mut self, tasks: u64, bytes: u64) {
        self.inflight_tasks = tasks;
        self.budget_bytes = bytes;
    }

    /// Effective configs for one resource. Sensitive values are omitted.
    pub fn describe(&self, kind: ResourceKind, name: &str) -> Result<Vec<Entry>, Error> {
        validate_name(kind, name)?;
        let dynamic = self
            .resources
            .get(&(kind, name.to_owned()))
            .map(|resource| &resource.values);
        let mut entries = Vec::new();
        for spec in schema(kind) {
            let stored = dynamic.and_then(|values| values.get(spec.name));
            entries.push(Entry {
                name: spec.name.to_owned(),
                value: if spec.sensitive {
                    None
                } else {
                    Some(stored.cloned().unwrap_or_else(|| spec.default.to_owned()))
                },
                dynamic: stored.is_some(),
                sensitive: spec.sensitive,
                read_only: spec.read_only,
            });
        }
        Ok(entries)
    }

    /// Apply `ops` to one resource.
    ///
    /// When `validate_only` is set, every operation is checked and the stored
    /// values, journal, tasks, and budget stay unchanged.
    pub fn alter(
        &mut self,
        kind: ResourceKind,
        name: &str,
        ops: &[Alter],
        validate_only: bool,
    ) -> Result<(), Error> {
        self.ready()?;
        validate_name(kind, name)?;
        if ops.is_empty() || ops.len() > 64 {
            return Err(Error::InvalidRequest);
        }
        let key = (kind, name.to_owned());
        let mut next = self.resources.get(&key).cloned().unwrap_or(Resource {
            values: BTreeMap::new(),
        });
        for op in ops {
            apply_op(kind, &mut next.values, op)?;
        }
        if validate_only {
            return Ok(());
        }
        if self.resources.len() >= self.limits.max_resources && !self.resources.contains_key(&key) {
            return Err(Error::InvalidLimits);
        }
        let tasks = self.inflight_tasks;
        let budget = self.budget_bytes;
        self.persist(&encode_resource(kind, name, &next)?)?;
        self.resources.insert(key, next);
        self.inflight_tasks = tasks;
        self.budget_bytes = budget;
        Ok(())
    }

    fn ready(&self) -> Result<(), Error> {
        if self.poisoned || self.journal.is_poisoned() {
            Err(Error::Poisoned)
        } else {
            Ok(())
        }
    }

    fn apply(&mut self, bytes: &[u8]) -> Result<(), Error> {
        let (kind, name, resource) = decode_resource(bytes)?;
        self.resources.insert((kind, name), resource);
        Ok(())
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

struct Spec {
    name: &'static str,
    default: &'static str,
    read_only: bool,
    sensitive: bool,
    kind: ValueKind,
}

enum ValueKind {
    Int { min: i64, max: i64 },
    List(&'static [&'static str]),
    Text,
}

fn schema(kind: ResourceKind) -> &'static [Spec] {
    match kind {
        ResourceKind::Topic => &TOPIC,
        ResourceKind::Broker => &BROKER,
    }
}

const TOPIC: [Spec; 5] = [
    Spec {
        name: "retention.ms",
        default: "604800000",
        read_only: false,
        sensitive: false,
        kind: ValueKind::Int {
            min: -1,
            max: i64::MAX,
        },
    },
    Spec {
        name: "segment.bytes",
        default: "1073741824",
        read_only: false,
        sensitive: false,
        kind: ValueKind::Int {
            min: 14,
            max: i64::MAX / 4,
        },
    },
    Spec {
        name: "max.message.bytes",
        default: "1048588",
        read_only: false,
        sensitive: false,
        kind: ValueKind::Int {
            min: 0,
            max: i64::MAX / 4,
        },
    },
    Spec {
        name: "min.insync.replicas",
        default: "1",
        read_only: false,
        sensitive: false,
        kind: ValueKind::Int { min: 1, max: 128 },
    },
    Spec {
        name: "cleanup.policy",
        default: "delete",
        read_only: false,
        sensitive: false,
        kind: ValueKind::List(&["delete", "compact"]),
    },
];

const BROKER: [Spec; 6] = [
    Spec {
        name: "broker.id",
        default: "",
        read_only: true,
        sensitive: false,
        kind: ValueKind::Text,
    },
    Spec {
        name: "listeners",
        default: "",
        read_only: true,
        sensitive: false,
        kind: ValueKind::Text,
    },
    Spec {
        name: "log.dirs",
        default: "",
        read_only: true,
        sensitive: false,
        kind: ValueKind::Text,
    },
    Spec {
        name: "log.retention.ms",
        default: "604800000",
        read_only: false,
        sensitive: false,
        kind: ValueKind::Int {
            min: -1,
            max: i64::MAX,
        },
    },
    Spec {
        name: "ssl.keystore.password",
        default: "",
        read_only: true,
        sensitive: true,
        kind: ValueKind::Text,
    },
    Spec {
        name: "sasl.jaas.config",
        default: "",
        read_only: true,
        sensitive: true,
        kind: ValueKind::Text,
    },
];

fn apply_op(
    kind: ResourceKind,
    values: &mut BTreeMap<String, String>,
    op: &Alter,
) -> Result<(), Error> {
    let Some(spec) = schema(kind).iter().find(|spec| spec.name == op.name) else {
        return Err(Error::InvalidConfig);
    };
    if spec.read_only || spec.sensitive {
        return Err(Error::InvalidConfig);
    }
    match op.op {
        Op::Delete => {
            values.remove(&op.name);
            Ok(())
        }
        Op::Set => {
            let value = required_value(op)?;
            check_value(spec, value)?;
            values.insert(op.name.clone(), value.to_owned());
            Ok(())
        }
        Op::Append | Op::Subtract => {
            let ValueKind::List(allowed) = spec.kind else {
                return Err(Error::InvalidConfig);
            };
            let token = required_value(op)?;
            if !allowed.contains(&token) {
                return Err(Error::InvalidConfig);
            }
            let current = values
                .get(&op.name)
                .map(String::as_str)
                .unwrap_or(spec.default);
            let mut parts: Vec<&str> = current.split(',').filter(|part| !part.is_empty()).collect();
            match op.op {
                Op::Append if !parts.contains(&token) => parts.push(token),
                Op::Subtract => parts.retain(|part| *part != token),
                _ => {}
            }
            if parts.is_empty() {
                return Err(Error::InvalidConfig);
            }
            values.insert(op.name.clone(), parts.join(","));
            Ok(())
        }
    }
}

fn required_value(op: &Alter) -> Result<&str, Error> {
    op.value
        .as_deref()
        .filter(|value| !value.is_empty())
        .ok_or(Error::InvalidConfig)
}

fn check_value(spec: &Spec, value: &str) -> Result<(), Error> {
    match spec.kind {
        ValueKind::Int { min, max } => {
            let parsed: i64 = value.parse().map_err(|_| Error::InvalidConfig)?;
            if parsed < min || parsed > max || (min < 0 && parsed == 0) {
                return Err(Error::InvalidConfig);
            }
        }
        ValueKind::List(allowed) => {
            let mut seen = BTreeMap::<&str, ()>::new();
            if value.is_empty() {
                return Err(Error::InvalidConfig);
            }
            for part in value.split(',') {
                if !allowed.contains(&part) || seen.insert(part, ()).is_some() {
                    return Err(Error::InvalidConfig);
                }
            }
        }
        ValueKind::Text => {
            if value.len() > 1024 || value.chars().any(|c| c.is_control()) {
                return Err(Error::InvalidConfig);
            }
        }
    }
    Ok(())
}

fn validate_name(kind: ResourceKind, name: &str) -> Result<(), Error> {
    match kind {
        ResourceKind::Topic => {
            catalog::validate_topic_name(name).map_err(|_| Error::InvalidRequest)
        }
        ResourceKind::Broker => {
            if name.is_empty() || name.len() > 10 || !name.bytes().all(|byte| byte.is_ascii_digit())
            {
                Err(Error::InvalidRequest)
            } else {
                Ok(())
            }
        }
    }
}

fn encode_resource(kind: ResourceKind, name: &str, resource: &Resource) -> Result<Vec<u8>, Error> {
    let mut bytes = Vec::new();
    bytes.extend_from_slice(RECORD_MAGIC);
    bytes.push(kind as u8);
    put_str(&mut bytes, name)?;
    let count = u16::try_from(resource.values.len()).map_err(|_| Error::InvalidConfig)?;
    bytes.extend_from_slice(&count.to_be_bytes());
    for (key, value) in &resource.values {
        put_str(&mut bytes, key)?;
        put_str(&mut bytes, value)?;
    }
    Ok(bytes)
}

fn decode_resource(mut bytes: &[u8]) -> Result<(ResourceKind, String, Resource), Error> {
    let magic = take(&mut bytes, 8)?;
    if magic != RECORD_MAGIC {
        return Err(Error::InvalidConfig);
    }
    let kind = match take(&mut bytes, 1)?[0] {
        2 => ResourceKind::Topic,
        4 => ResourceKind::Broker,
        _ => return Err(Error::InvalidConfig),
    };
    let name = take_str(&mut bytes)?;
    let count = usize::from(u16::from_be_bytes(
        take(&mut bytes, 2)?
            .try_into()
            .map_err(|_| Error::InvalidConfig)?,
    ));
    let mut values = BTreeMap::new();
    for _ in 0..count {
        values.insert(take_str(&mut bytes)?, take_str(&mut bytes)?);
    }
    if !bytes.is_empty() {
        return Err(Error::InvalidConfig);
    }
    Ok((kind, name, Resource { values }))
}

fn put_str(bytes: &mut Vec<u8>, value: &str) -> Result<(), Error> {
    let len = u16::try_from(value.len()).map_err(|_| Error::InvalidConfig)?;
    bytes.extend_from_slice(&len.to_be_bytes());
    bytes.extend_from_slice(value.as_bytes());
    Ok(())
}

fn take<'a>(input: &mut &'a [u8], len: usize) -> Result<&'a [u8], Error> {
    if input.len() < len {
        return Err(Error::InvalidConfig);
    }
    let (head, tail) = input.split_at(len);
    *input = tail;
    Ok(head)
}

fn take_str(input: &mut &[u8]) -> Result<String, Error> {
    let len = usize::from(u16::from_be_bytes(
        take(input, 2)?
            .try_into()
            .map_err(|_| Error::InvalidConfig)?,
    ));
    let bytes = take(input, len)?;
    std::str::from_utf8(bytes)
        .map(|text| text.to_owned())
        .map_err(|_| Error::InvalidConfig)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn limits() -> Limits {
        let journal = journal::Limits::new(4096, 1 << 20, 100, 1 << 20)
            .unwrap_or_else(|_| panic!("journal limits"));
        Limits::new(16, 32, journal).unwrap_or_else(|_| panic!("config limits"))
    }

    fn store(name: &str) -> (ConfigStore, std::path::PathBuf) {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();
        let path = std::env::temp_dir().join(format!("pl-cfg-{name}-{nanos}"));
        let store = ConfigStore::open(&path, limits()).unwrap_or_else(|error| panic!("{error:?}"));
        (store, path)
    }

    #[test]
    fn validate_only_and_read_only_do_not_mutate() {
        let (mut store, _) = store("reject");
        store.set_runtime_budget(3, 4096);
        let invalid = Alter {
            name: "retention.ms".into(),
            op: Op::Set,
            value: Some("0".into()),
        };
        assert!(matches!(
            store.alter(ResourceKind::Topic, "orders", &[invalid], true),
            Err(Error::InvalidConfig)
        ));
        let readonly = Alter {
            name: "broker.id".into(),
            op: Op::Set,
            value: Some("1".into()),
        };
        assert!(matches!(
            store.alter(ResourceKind::Broker, "1", &[readonly], false),
            Err(Error::InvalidConfig)
        ));
        let secret = Alter {
            name: "ssl.keystore.password".into(),
            op: Op::Set,
            value: Some("secret".into()),
        };
        assert!(matches!(
            store.alter(ResourceKind::Broker, "1", &[secret], false),
            Err(Error::InvalidConfig)
        ));
        let described = store
            .describe(ResourceKind::Broker, "1")
            .unwrap_or_else(|error| panic!("{error:?}"));
        let password = described
            .iter()
            .find(|entry| entry.name == "ssl.keystore.password")
            .unwrap_or_else(|| panic!("password entry"));
        assert!(password.sensitive && password.value.is_none());
        assert_eq!(store.inflight_tasks(), 3);
        assert_eq!(store.budget_bytes(), 4096);
        assert_eq!(
            store
                .describe(ResourceKind::Topic, "orders")
                .unwrap_or_else(|error| panic!("{error:?}"))
                .iter()
                .find(|entry| entry.name == "retention.ms")
                .map(|entry| entry.value.clone()),
            Some(Some("604800000".into()))
        );
    }

    #[test]
    fn incremental_alter_persists_without_clearing_the_budget() {
        let (mut store, path) = store("persist");
        store.set_runtime_budget(2, 100);
        store
            .alter(
                ResourceKind::Topic,
                "orders",
                &[
                    Alter {
                        name: "min.insync.replicas".into(),
                        op: Op::Set,
                        value: Some("2".into()),
                    },
                    Alter {
                        name: "cleanup.policy".into(),
                        op: Op::Append,
                        value: Some("compact".into()),
                    },
                ],
                false,
            )
            .unwrap_or_else(|error| panic!("{error:?}"));
        store
            .alter(
                ResourceKind::Topic,
                "orders",
                &[Alter {
                    name: "cleanup.policy".into(),
                    op: Op::Subtract,
                    value: Some("delete".into()),
                }],
                false,
            )
            .unwrap_or_else(|error| panic!("{error:?}"));
        assert_eq!(store.inflight_tasks(), 2);
        assert_eq!(store.budget_bytes(), 100);
        drop(store);
        let restored =
            ConfigStore::open(&path, limits()).unwrap_or_else(|error| panic!("{error:?}"));
        let described = restored
            .describe(ResourceKind::Topic, "orders")
            .unwrap_or_else(|error| panic!("{error:?}"));
        assert_eq!(
            described
                .iter()
                .find(|entry| entry.name == "min.insync.replicas")
                .and_then(|entry| entry.value.clone()),
            Some("2".into())
        );
        assert_eq!(
            described
                .iter()
                .find(|entry| entry.name == "cleanup.policy")
                .and_then(|entry| entry.value.clone()),
            Some("compact".into())
        );
        assert_eq!(restored.inflight_tasks(), 0);
    }
}

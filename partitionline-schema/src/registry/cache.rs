//! Bounded completed results and caller-driven request coalescing; no tasks spawned.
use super::{RegisteredSchema, RegistryError, SchemaReference, SchemaVersion, VersionedSchema};
use std::collections::HashMap;
use std::mem::size_of;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{watch, Mutex, Notify};
use tokio::time::Instant;

/// Completed-cache limits and freshness, shared by clones of one client.
///
/// Defaults: 256 entries, 8 MiB charged retained bytes, 16 active distinct-key
/// requests; IDs/pinned versions fresh for five minutes, latest for five seconds,
/// 404s for one second. Other failures are never cached. Expiry is lazy.
/// Least recently used entries are evicted to meet both completed limits;
/// oversized successful results are returned without being cached.
#[derive(Debug, Clone)]
pub struct RegistryCacheConfig {
    entries: usize,
    bytes: usize,
    in_flight: usize,
    subject_bytes: usize,
    schema_ttl: Duration,
    latest_ttl: Duration,
    negative_ttl: Duration,
}
impl Default for RegistryCacheConfig {
    fn default() -> Self {
        Self {
            entries: 256,
            bytes: 8 * 1024 * 1024,
            in_flight: 16,
            subject_bytes: 1024,
            schema_ttl: Duration::from_secs(300),
            latest_ttl: Duration::from_secs(5),
            negative_ttl: Duration::from_secs(1),
        }
    }
}
impl RegistryCacheConfig {
    /// Disable completed caching while retaining bounded request coalescing.
    #[must_use]
    pub fn disabled() -> Self {
        Self {
            entries: 0,
            bytes: 0,
            ..Self::default()
        }
    }
    /// Entry/charged-byte and active distinct-key limits. Entry/byte limits must
    /// both be zero, or 1..=4096 entries and 1..=64 MiB. Active requests: 1..=128.
    #[must_use]
    pub fn limits(mut self, entries: usize, bytes: usize, in_flight: usize) -> Self {
        self.entries = entries;
        self.bytes = bytes;
        self.in_flight = in_flight;
        self
    }
    /// IDs/pinned versions, latest and 404 freshness, each zero through 24 hours.
    /// Zero disables completed caching for that class. Latest is a separate key.
    #[must_use]
    pub fn freshness(mut self, schema: Duration, latest: Duration, negative: Duration) -> Self {
        self.schema_ttl = schema;
        self.latest_ttl = latest;
        self.negative_ttl = negative;
        self
    }
    /// Maximum subject UTF-8 bytes, 1..=4096 (default 1024). Checked before I/O
    /// or allocating an encoded path; subject text is never an error label.
    #[must_use]
    pub fn subject_limit(mut self, bytes: usize) -> Self {
        self.subject_bytes = bytes;
        self
    }
}

/// Snapshot of retained results and active distinct-key requests.
/// Hash buckets/allocator overhead and caller-owned returned clones/futures are
/// separate from charged retained bytes. Entry and active-key counts bound the
/// cache's structures; applications must bound their own callers and results.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RegistryCacheStats {
    /// Completed success or negative entries after lazy expiry.
    pub entries: usize,
    /// Charged retained payload/key capacities and inline entry sizes.
    pub retained_bytes: usize,
    /// Distinct keys with a request actively owned by a caller.
    pub in_flight: usize,
    /// Caller-owned same-key waiters; no background tasks are created.
    pub coalesced_waiters: usize,
}

#[derive(Clone, PartialEq, Eq, Hash)]
pub(super) enum Key {
    Id(u32),
    Subject(String, SchemaVersion),
}
pub(super) enum Value {
    Id(RegisteredSchema),
    Subject(VersionedSchema),
}
type Outcome = Result<Arc<Value>, RegistryError>;
#[derive(Clone)]
pub(super) enum Flight {
    Pending,
    Finished(Outcome),
    Cancelled,
}
struct Entry {
    result: Outcome,
    expires: Instant,
    used: u64,
    weight: usize,
}
#[derive(Default)]
struct State {
    entries: HashMap<Key, Entry>,
    flights: HashMap<Key, watch::Sender<Flight>>,
    bytes: usize,
    tick: u64,
}
pub(super) struct Cache {
    config: RegistryCacheConfig,
    state: Mutex<State>,
    pub(super) changed: Arc<Notify>,
}
pub(super) enum Role {
    Hit(Outcome),
    Follower(watch::Receiver<Flight>),
    Leader(Leader),
    Capacity,
}
pub(super) struct Leader {
    tx: watch::Sender<Flight>,
    changed: Arc<Notify>,
    finished: bool,
}
impl Drop for Leader {
    fn drop(&mut self) {
        if !self.finished {
            self.tx.send_replace(Flight::Cancelled);
            self.changed.notify_waiters();
        }
    }
}
impl State {
    fn prune(&mut self) {
        let now = Instant::now();
        self.entries.retain(|_, entry| entry.expires > now);
        self.bytes = self.entries.values().map(|entry| entry.weight).sum();
        self.flights
            .retain(|_, tx| matches!(*tx.borrow(), Flight::Pending));
    }
}
impl Cache {
    pub(super) fn new(config: RegistryCacheConfig) -> Result<Self, RegistryError> {
        let disabled = config.entries == 0 && config.bytes == 0;
        if (!disabled
            && (config.entries == 0
                || config.entries > 4096
                || config.bytes == 0
                || config.bytes > 64 * 1024 * 1024))
            || config.in_flight == 0
            || config.in_flight > 128
            || config.subject_bytes == 0
            || config.subject_bytes > 4096
            || [config.schema_ttl, config.latest_ttl, config.negative_ttl]
                .iter()
                .any(|ttl| *ttl > Duration::from_secs(86_400))
        {
            return Err(RegistryError::invalid_config(
                "invalid cache entry/byte/active-request/subject/freshness limits",
            ));
        }
        Ok(Self {
            config,
            state: Mutex::default(),
            changed: Arc::new(Notify::new()),
        })
    }
    pub(super) fn config(&self) -> &RegistryCacheConfig {
        &self.config
    }
    pub(super) fn validate_subject(&self, subject: &str) -> Result<(), RegistryError> {
        if subject.len() > self.config.subject_bytes {
            return Err(RegistryError::invalid_config(
                "subject exceeds configured byte limit",
            ));
        }
        Ok(())
    }
    pub(super) async fn stats(&self) -> RegistryCacheStats {
        let mut state = self.state.lock().await;
        state.prune();
        RegistryCacheStats {
            entries: state.entries.len(),
            retained_bytes: state.bytes,
            in_flight: state.flights.len(),
            coalesced_waiters: state
                .flights
                .values()
                .map(watch::Sender::receiver_count)
                .sum(),
        }
    }
    pub(super) async fn role(&self, key: &Key) -> Role {
        let mut state = self.state.lock().await;
        state.prune();
        state.tick = state.tick.saturating_add(1);
        let tick = state.tick;
        if let Some(entry) = state.entries.get_mut(key) {
            entry.used = tick;
            return Role::Hit(entry.result.clone());
        }
        if let Some(tx) = state.flights.get(key) {
            return Role::Follower(tx.subscribe());
        }
        if state.flights.len() >= self.config.in_flight {
            return Role::Capacity;
        }
        let (tx, _rx) = watch::channel(Flight::Pending);
        state.flights.insert(key.clone(), tx.clone());
        Role::Leader(Leader {
            tx,
            changed: self.changed.clone(),
            finished: false,
        })
    }
    pub(super) async fn complete(&self, key: &Key, mut leader: Leader, result: Outcome) -> Outcome {
        let mut state = self.state.lock().await;
        state.prune();
        let ttl = match &result {
            Ok(_) => match key {
                Key::Subject(_, SchemaVersion::Latest) => self.config.latest_ttl,
                _ => self.config.schema_ttl,
            },
            Err(RegistryError::NotFound { .. }) => self.config.negative_ttl,
            Err(_) => Duration::ZERO,
        };
        let weight = weight(key, &result);
        if self.config.entries > 0 && !ttl.is_zero() && weight <= self.config.bytes {
            if let Some(expires) = Instant::now().checked_add(ttl) {
                while state.entries.len() >= self.config.entries
                    || state.bytes.saturating_add(weight) > self.config.bytes
                {
                    let oldest = state
                        .entries
                        .iter()
                        .min_by_key(|(_, entry)| entry.used)
                        .map(|(key, _)| key.clone());
                    let Some(oldest) = oldest else {
                        break;
                    };
                    if let Some(entry) = state.entries.remove(&oldest) {
                        state.bytes = state.bytes.saturating_sub(entry.weight);
                    }
                }
                state.tick = state.tick.saturating_add(1);
                let used = state.tick;
                state.entries.insert(
                    key.clone(),
                    Entry {
                        result: result.clone(),
                        expires,
                        used,
                        weight,
                    },
                );
                state.bytes = state.bytes.saturating_add(weight);
            }
        }
        state.flights.remove(key);
        leader.tx.send_replace(Flight::Finished(result.clone()));
        leader.finished = true;
        self.changed.notify_waiters();
        result
    }
}
fn references_weight(references: &Vec<SchemaReference>) -> usize {
    references.iter().fold(
        references
            .capacity()
            .saturating_mul(size_of::<SchemaReference>()),
        |bytes, reference| {
            bytes
                .saturating_add(reference.name.capacity())
                .saturating_add(reference.subject.capacity())
        },
    )
}
fn weight(key: &Key, result: &Outcome) -> usize {
    let key_bytes = match key {
        Key::Id(_) => 0,
        Key::Subject(subject, _) => subject.capacity(),
    };
    let payload = match result {
        Ok(value) => match value.as_ref() {
            Value::Id(schema) => schema
                .schema
                .capacity()
                .saturating_add(schema.schema_type.as_ref().map_or(0, String::capacity))
                .saturating_add(references_weight(&schema.references)),
            Value::Subject(schema) => schema
                .subject
                .capacity()
                .saturating_add(schema.schema.capacity())
                .saturating_add(schema.schema_type.as_ref().map_or(0, String::capacity))
                .saturating_add(references_weight(&schema.references)),
        },
        Err(RegistryError::NotFound { lookup, .. }) => lookup.capacity(),
        Err(_) => 0,
    };
    payload
        .saturating_add(key_bytes)
        .saturating_add(size_of::<Key>())
        .saturating_add(size_of::<Entry>())
        .saturating_add(size_of::<Value>())
        .saturating_add(2 * size_of::<usize>())
}

//! Client, connection, and replication request quotas.
//!
//! Each client has its own produce and fetch token bucket. Exhausting one
//! client does not spend another client's tokens. Replication uses a separate
//! bucket. A hard connection cap and an in-flight byte cap reject work that
//! would exceed them. Lowering a quota does not cancel work that was already
//! accepted, and disconnect releases only that client's connection permits.
//!
//! This is not a Kafka throttle response on the wire.

use std::collections::BTreeMap;

/// Kafka `THROTTLING_QUOTA_EXCEEDED`.
pub const THROTTLING_QUOTA_EXCEEDED: i16 = 89;

/// Which byte quota a request spends.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Kind {
    /// Client produce bandwidth.
    Produce,
    /// Client fetch bandwidth.
    Fetch,
    /// Follower replication bandwidth, not charged to a client.
    Replication,
}

/// Configured rates and hard caps.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Limits {
    /// Produce bytes refilled each second for one client.
    pub produce_bytes_per_sec: u64,
    /// Fetch bytes refilled each second for one client.
    pub fetch_bytes_per_sec: u64,
    /// Replication bytes refilled each second for the broker.
    pub replication_bytes_per_sec: u64,
    /// Connections one client may hold.
    pub max_connections_per_client: usize,
    /// Connections the broker may hold.
    pub max_connections: usize,
    /// Accepted but unfinished request bytes.
    pub max_inflight_bytes: u64,
    /// Longest throttle this broker will ask a client to wait.
    pub max_throttle_ms: u64,
}

impl Limits {
    /// Configure positive rates and caps.
    pub fn new(
        produce_bytes_per_sec: u64,
        fetch_bytes_per_sec: u64,
        replication_bytes_per_sec: u64,
        max_connections_per_client: usize,
        max_connections: usize,
        max_inflight_bytes: u64,
        max_throttle_ms: u64,
    ) -> Result<Self, Error> {
        if produce_bytes_per_sec == 0
            || fetch_bytes_per_sec == 0
            || replication_bytes_per_sec == 0
            || max_connections_per_client == 0
            || max_connections < max_connections_per_client
            || max_inflight_bytes == 0
            || max_throttle_ms == 0
            || max_throttle_ms > 60_000
        {
            return Err(Error::InvalidLimits);
        }
        Ok(Self {
            produce_bytes_per_sec,
            fetch_bytes_per_sec,
            replication_bytes_per_sec,
            max_connections_per_client,
            max_connections,
            max_inflight_bytes,
            max_throttle_ms,
        })
    }
}

/// A request that was accepted and still owns its byte permit.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Admission {
    /// Caller-visible work id.
    pub work_id: u64,
    /// Delay before the response should be sent. Zero means the bucket had tokens.
    pub throttle_ms: u64,
}

/// Why a quota decision was refused.
#[derive(Debug)]
pub enum Error {
    /// The request would exceed the throttle, connection, or in-flight cap.
    Throttled,
    /// The client id or byte count is not usable.
    InvalidRequest,
    /// The work id is unknown or was already finished.
    UnknownWork,
    /// The limits are not usable.
    InvalidLimits,
}

impl Error {
    /// Kafka code for a throttle refusal. Other failures have none.
    pub fn code(&self) -> Option<i16> {
        match self {
            Self::Throttled => Some(THROTTLING_QUOTA_EXCEEDED),
            Self::InvalidRequest | Self::UnknownWork | Self::InvalidLimits => None,
        }
    }
}

#[derive(Debug, Clone)]
struct Client {
    connections: usize,
    produce_tokens: u64,
    fetch_tokens: u64,
    updated_ms: u64,
}

#[derive(Debug, Clone)]
struct Work {
    bytes: u64,
}

/// In-memory quota window.
pub struct Window {
    limits: Limits,
    clients: BTreeMap<String, Client>,
    replication_tokens: u64,
    replication_updated_ms: u64,
    connections: usize,
    inflight_bytes: u64,
    work: BTreeMap<u64, Work>,
    next_work: u64,
}

impl Window {
    /// Start with full buckets at `now_ms`.
    pub fn new(limits: Limits, now_ms: u64) -> Self {
        Self {
            replication_tokens: limits.replication_bytes_per_sec,
            limits,
            clients: BTreeMap::new(),
            replication_updated_ms: now_ms,
            connections: 0,
            inflight_bytes: 0,
            work: BTreeMap::new(),
            next_work: 1,
        }
    }

    /// Open connections.
    pub fn connections(&self) -> usize {
        self.connections
    }

    /// Bytes of accepted work that has not finished.
    pub fn inflight_bytes(&self) -> u64 {
        self.inflight_bytes
    }

    /// Accepted work still waiting for [`Self::complete`].
    pub fn accepted(&self, work_id: u64) -> bool {
        self.work.contains_key(&work_id)
    }

    /// Replace future rates. Accepted work is kept.
    pub fn set_rates(
        &mut self,
        produce_bytes_per_sec: u64,
        fetch_bytes_per_sec: u64,
        replication_bytes_per_sec: u64,
    ) -> Result<(), Error> {
        if produce_bytes_per_sec == 0 || fetch_bytes_per_sec == 0 || replication_bytes_per_sec == 0
        {
            return Err(Error::InvalidLimits);
        }
        self.limits.produce_bytes_per_sec = produce_bytes_per_sec;
        self.limits.fetch_bytes_per_sec = fetch_bytes_per_sec;
        self.limits.replication_bytes_per_sec = replication_bytes_per_sec;
        Ok(())
    }

    /// Take one connection permit for `client`.
    pub fn connect(&mut self, client: &str) -> Result<(), Error> {
        self.named(client)?;
        if self.connections >= self.limits.max_connections {
            return Err(Error::Throttled);
        }
        let per_client = self.limits.max_connections_per_client;
        let entry = self.client_mut(client, 0);
        if entry.connections >= per_client {
            return Err(Error::Throttled);
        }
        entry.connections = entry.connections.saturating_add(1);
        self.connections = self.connections.saturating_add(1);
        Ok(())
    }

    /// Return one connection permit. Accepted work for this client stays.
    pub fn disconnect(&mut self, client: &str) -> Result<(), Error> {
        self.named(client)?;
        let Some(entry) = self.clients.get_mut(client) else {
            return Err(Error::UnknownWork);
        };
        if entry.connections == 0 {
            return Err(Error::UnknownWork);
        }
        entry.connections -= 1;
        self.connections = self.connections.saturating_sub(1);
        Ok(())
    }

    /// Accept a byte charge, or refuse it without spending tokens.
    pub fn admit(
        &mut self,
        client: Option<&str>,
        kind: Kind,
        bytes: u64,
        now_ms: u64,
    ) -> Result<Admission, Error> {
        if bytes == 0 || self.inflight_bytes.saturating_add(bytes) > self.limits.max_inflight_bytes
        {
            return Err(if bytes == 0 {
                Error::InvalidRequest
            } else {
                Error::Throttled
            });
        }
        let throttle_ms = match kind {
            Kind::Replication => self.charge_replication(bytes, now_ms)?,
            Kind::Produce | Kind::Fetch => {
                let name = client.ok_or(Error::InvalidRequest)?;
                self.named(name)?;
                self.charge_client(name, kind, bytes, now_ms)?
            }
        };
        let work_id = self.next_work;
        self.next_work = self.next_work.saturating_add(1);
        self.inflight_bytes = self.inflight_bytes.saturating_add(bytes);
        self.work.insert(work_id, Work { bytes });
        Ok(Admission {
            work_id,
            throttle_ms,
        })
    }

    /// Finish accepted work and return its byte permit.
    pub fn complete(&mut self, work_id: u64) -> Result<u64, Error> {
        let work = self.work.remove(&work_id).ok_or(Error::UnknownWork)?;
        self.inflight_bytes = self.inflight_bytes.saturating_sub(work.bytes);
        Ok(work.bytes)
    }

    fn charge_client(
        &mut self,
        client: &str,
        kind: Kind,
        bytes: u64,
        now_ms: u64,
    ) -> Result<u64, Error> {
        let rate = match kind {
            Kind::Produce => self.limits.produce_bytes_per_sec,
            Kind::Fetch => self.limits.fetch_bytes_per_sec,
            Kind::Replication => return Err(Error::InvalidRequest),
        };
        let max_throttle_ms = self.limits.max_throttle_ms;
        let entry = self.client_mut(client, now_ms);
        refill(entry, kind, rate, now_ms);
        let tokens = match kind {
            Kind::Produce => &mut entry.produce_tokens,
            Kind::Fetch => &mut entry.fetch_tokens,
            Kind::Replication => return Err(Error::InvalidRequest),
        };
        if *tokens >= bytes {
            *tokens -= bytes;
            return Ok(0);
        }
        let deficit = bytes - *tokens;
        let throttle_ms = throttle_for(deficit, rate);
        if throttle_ms > max_throttle_ms {
            return Err(Error::Throttled);
        }
        *tokens = 0;
        Ok(throttle_ms)
    }

    fn charge_replication(&mut self, bytes: u64, now_ms: u64) -> Result<u64, Error> {
        if now_ms >= self.replication_updated_ms {
            let elapsed = now_ms.saturating_sub(self.replication_updated_ms);
            let refill = elapsed.saturating_mul(self.limits.replication_bytes_per_sec) / 1_000;
            self.replication_tokens = self
                .replication_tokens
                .saturating_add(refill)
                .min(self.limits.replication_bytes_per_sec);
            self.replication_updated_ms = now_ms;
        }
        if self.replication_tokens >= bytes {
            self.replication_tokens -= bytes;
            return Ok(0);
        }
        let deficit = bytes - self.replication_tokens;
        let throttle_ms = throttle_for(deficit, self.limits.replication_bytes_per_sec);
        if throttle_ms > self.limits.max_throttle_ms {
            return Err(Error::Throttled);
        }
        self.replication_tokens = 0;
        Ok(throttle_ms)
    }

    fn client_mut(&mut self, client: &str, now_ms: u64) -> &mut Client {
        let produce = self.limits.produce_bytes_per_sec;
        let fetch = self.limits.fetch_bytes_per_sec;
        self.clients.entry(client.to_owned()).or_insert(Client {
            connections: 0,
            produce_tokens: produce,
            fetch_tokens: fetch,
            updated_ms: now_ms,
        })
    }

    fn named(&self, client: &str) -> Result<(), Error> {
        if client.is_empty() || client.len() > 249 || client.chars().any(|c| c.is_control()) {
            Err(Error::InvalidRequest)
        } else {
            Ok(())
        }
    }
}

fn refill(client: &mut Client, kind: Kind, rate: u64, now_ms: u64) {
    if now_ms < client.updated_ms {
        return;
    }
    let elapsed = now_ms.saturating_sub(client.updated_ms);
    let added = elapsed.saturating_mul(rate) / 1_000;
    match kind {
        Kind::Produce => {
            client.produce_tokens = client.produce_tokens.saturating_add(added).min(rate)
        }
        Kind::Fetch => client.fetch_tokens = client.fetch_tokens.saturating_add(added).min(rate),
        Kind::Replication => {}
    }
    client.updated_ms = now_ms;
}

fn throttle_for(deficit: u64, rate: u64) -> u64 {
    deficit.saturating_mul(1_000).div_ceil(rate.max(1))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn limits() -> Limits {
        Limits::new(100, 100, 50, 2, 4, 1_000, 1_000).unwrap_or_else(|_| panic!("quota limits"))
    }

    #[test]
    fn one_client_cannot_spend_another_clients_produce_quota() {
        let mut window = Window::new(limits(), 0);
        let first = window
            .admit(Some("a"), Kind::Produce, 100, 0)
            .unwrap_or_else(|error| panic!("{error:?}"));
        assert_eq!(first.throttle_ms, 0);
        assert!(matches!(
            window.admit(Some("a"), Kind::Produce, 101, 0),
            Err(Error::Throttled)
        ));
        let second = window
            .admit(Some("b"), Kind::Produce, 100, 0)
            .unwrap_or_else(|error| panic!("{error:?}"));
        assert_eq!(second.throttle_ms, 0);
        assert_eq!(window.inflight_bytes(), 200);
    }

    #[test]
    fn disconnect_and_quota_changes_keep_accepted_work() {
        let mut window = Window::new(limits(), 0);
        window
            .connect("a")
            .unwrap_or_else(|error| panic!("{error:?}"));
        window
            .connect("a")
            .unwrap_or_else(|error| panic!("{error:?}"));
        assert!(matches!(window.connect("a"), Err(Error::Throttled)));
        let admitted = window
            .admit(Some("a"), Kind::Fetch, 40, 0)
            .unwrap_or_else(|error| panic!("{error:?}"));
        let replication = window
            .admit(None, Kind::Replication, 50, 0)
            .unwrap_or_else(|error| panic!("{error:?}"));
        assert_eq!(replication.throttle_ms, 0);
        window
            .set_rates(1, 1, 1)
            .unwrap_or_else(|error| panic!("{error:?}"));
        window
            .disconnect("a")
            .unwrap_or_else(|error| panic!("{error:?}"));
        window
            .disconnect("a")
            .unwrap_or_else(|error| panic!("{error:?}"));
        assert_eq!(window.connections(), 0);
        assert!(window.accepted(admitted.work_id));
        assert!(window.accepted(replication.work_id));
        assert_eq!(
            window
                .complete(admitted.work_id)
                .unwrap_or_else(|error| panic!("{error:?}")),
            40
        );
        assert_eq!(
            window
                .complete(replication.work_id)
                .unwrap_or_else(|error| panic!("{error:?}")),
            50
        );
        assert_eq!(window.inflight_bytes(), 0);
        assert!(matches!(
            window.complete(admitted.work_id),
            Err(Error::UnknownWork)
        ));
        let delayed = window
            .admit(Some("a"), Kind::Fetch, 2, 0)
            .unwrap_or_else(|error| panic!("{error:?}"));
        assert_eq!(delayed.throttle_ms, 1_000);
    }

    #[test]
    fn an_older_clock_does_not_refill_a_spent_quota() {
        let mut window = Window::new(limits(), 0);
        let spent = window
            .admit(Some("a"), Kind::Produce, 100, 1_000)
            .unwrap_or_else(|error| panic!("{error:?}"));
        assert_eq!(spent.throttle_ms, 0);
        let older = window
            .admit(Some("a"), Kind::Produce, 50, 0)
            .unwrap_or_else(|error| panic!("{error:?}"));
        assert!(older.throttle_ms > 0);
        let same_time = window
            .admit(Some("a"), Kind::Produce, 50, 1_000)
            .unwrap_or_else(|error| panic!("{error:?}"));
        assert!(same_time.throttle_ms > 0);
        let replication = window
            .admit(None, Kind::Replication, 50, 1_000)
            .unwrap_or_else(|error| panic!("{error:?}"));
        assert_eq!(replication.throttle_ms, 0);
        let older_replication = window
            .admit(None, Kind::Replication, 50, 0)
            .unwrap_or_else(|error| panic!("{error:?}"));
        assert!(older_replication.throttle_ms > 0);
    }
}

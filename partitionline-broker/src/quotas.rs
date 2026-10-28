//! Client, connection, and replication request quotas.
//!
//! Each client has its own produce and fetch token bucket. Exhausting one
//! client does not spend another client's tokens. Replication uses a separate
//! bucket. A hard connection cap and an in-flight byte cap reject work that
//! would exceed them. Lowering a quota does not cancel work that was already
//! accepted, and disconnect releases only that client's connection permits.
//! Client identity state is bounded by the configured connection cap; idle
//! identities are reclaimed only after both buckets refill and all debt clears.
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
    produce: Bucket,
    fetch: Bucket,
}

// A byte is represented by 1,000 units, so millisecond refills retain their
// fractional bytes. Debt records accepted delayed requests until time pays them.
#[derive(Debug, Clone)]
struct Bucket {
    credit: u128,
    debt: u128,
    updated_ms: u64,
}

impl Bucket {
    fn new(rate: u64, now_ms: u64) -> Self {
        Self {
            credit: u128::from(rate) * 1_000,
            debt: 0,
            updated_ms: now_ms,
        }
    }

    fn refill(&mut self, rate: u64, now_ms: u64) {
        let cap = u128::from(rate) * 1_000;
        self.credit = self.credit.min(cap);
        if now_ms < self.updated_ms {
            return;
        }
        let added = u128::from(now_ms - self.updated_ms) * u128::from(rate);
        self.updated_ms = now_ms;
        let paid = added.min(self.debt);
        self.debt -= paid;
        self.credit = self.credit.saturating_add(added - paid).min(cap);
    }

    fn charge(
        &mut self,
        rate: u64,
        bytes: u64,
        now_ms: u64,
        max_throttle_ms: u64,
    ) -> Result<u64, Error> {
        // Limits has public fields, so reject an invalid directly constructed
        // configuration rather than divide by zero.
        if rate == 0 {
            return Err(Error::InvalidLimits);
        }
        self.refill(rate, now_ms);
        let amount = u128::from(bytes) * 1_000;
        let deficit = amount.saturating_sub(self.credit);
        let debt = self.debt.checked_add(deficit).ok_or(Error::Throttled)?;
        let throttle = debt.div_ceil(u128::from(rate));
        if throttle > u128::from(max_throttle_ms) {
            return Err(Error::Throttled);
        }
        self.credit = self.credit.saturating_sub(amount);
        self.debt = debt;
        Ok(throttle as u64)
    }

    fn full(&self, rate: u64) -> bool {
        self.debt == 0 && self.credit == u128::from(rate) * 1_000
    }
}

#[derive(Debug, Clone)]
struct Work {
    bytes: u64,
}

/// In-memory quota window.
pub struct Window {
    limits: Limits,
    clients: BTreeMap<String, Client>,
    replication: Bucket,
    connections: usize,
    inflight_bytes: u64,
    work: BTreeMap<u64, Work>,
    next_work: u64,
    observed_ms: u64,
}

impl Window {
    /// Start with full buckets at `now_ms`.
    pub fn new(limits: Limits, now_ms: u64) -> Self {
        Self {
            replication: Bucket::new(limits.replication_bytes_per_sec, now_ms),
            limits,
            clients: BTreeMap::new(),
            connections: 0,
            inflight_bytes: 0,
            work: BTreeMap::new(),
            next_work: 1,
            observed_ms: now_ms,
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
    ///
    /// The next refill applies these rates to the entire interval since each
    /// bucket's last observation. Use [`Self::set_rates_at`] to account for
    /// elapsed time at the old rates before changing them.
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

    /// Account for elapsed time at the current rates before replacing them.
    /// Accepted work and its accumulated debt remain in the window.
    pub fn set_rates_at(
        &mut self,
        produce_bytes_per_sec: u64,
        fetch_bytes_per_sec: u64,
        replication_bytes_per_sec: u64,
        now_ms: u64,
    ) -> Result<(), Error> {
        if produce_bytes_per_sec == 0 || fetch_bytes_per_sec == 0 || replication_bytes_per_sec == 0
        {
            return Err(Error::InvalidLimits);
        }
        self.observed_ms = self.observed_ms.max(now_ms);
        for client in self.clients.values_mut() {
            client
                .produce
                .refill(self.limits.produce_bytes_per_sec, now_ms);
            client.fetch.refill(self.limits.fetch_bytes_per_sec, now_ms);
        }
        self.replication
            .refill(self.limits.replication_bytes_per_sec, now_ms);
        self.set_rates(
            produce_bytes_per_sec,
            fetch_bytes_per_sec,
            replication_bytes_per_sec,
        )
    }

    /// Take one connection permit for `client` at the latest observed time.
    ///
    /// This method cannot advance time by itself. An otherwise idle caller
    /// must use [`Self::connect_at`] to reclaim quota state as time passes.
    pub fn connect(&mut self, client: &str) -> Result<(), Error> {
        self.connect_at(client, self.observed_ms)
    }

    /// Take one connection permit and reclaim fully refilled idle identities
    /// when needed. Times must use the same millisecond clock as [`Self::admit`].
    pub fn connect_at(&mut self, client: &str, now_ms: u64) -> Result<(), Error> {
        self.named(client)?;
        self.observed_ms = self.observed_ms.max(now_ms);
        if self.connections >= self.limits.max_connections {
            return Err(Error::Throttled);
        }
        let per_client = self.limits.max_connections_per_client;
        let entry = self.client_mut(client, now_ms)?;
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
        let next_inflight = self.inflight_bytes.checked_add(bytes);
        if bytes == 0 || next_inflight.is_none_or(|total| total > self.limits.max_inflight_bytes) {
            return Err(if bytes == 0 {
                Error::InvalidRequest
            } else {
                Error::Throttled
            });
        }
        self.observed_ms = self.observed_ms.max(now_ms);
        // Reserve a distinct work ID before spending tokens; overflow must never
        // overwrite a live permit or leave charged work without a receipt.
        let work_id = self.next_work;
        let next_work = work_id.checked_add(1).ok_or(Error::Throttled)?;
        let throttle_ms = match kind {
            Kind::Replication => self.charge_replication(bytes, now_ms)?,
            Kind::Produce | Kind::Fetch => {
                let name = client.ok_or(Error::InvalidRequest)?;
                self.named(name)?;
                self.charge_client(name, kind, bytes, now_ms)?
            }
        };
        self.next_work = next_work;
        self.inflight_bytes = next_inflight.ok_or(Error::Throttled)?;
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
        let entry = self.client_mut(client, now_ms)?;
        match kind {
            Kind::Produce => entry.produce.charge(rate, bytes, now_ms, max_throttle_ms),
            Kind::Fetch => entry.fetch.charge(rate, bytes, now_ms, max_throttle_ms),
            Kind::Replication => Err(Error::InvalidRequest),
        }
    }

    fn charge_replication(&mut self, bytes: u64, now_ms: u64) -> Result<u64, Error> {
        self.replication.charge(
            self.limits.replication_bytes_per_sec,
            bytes,
            now_ms,
            self.limits.max_throttle_ms,
        )
    }

    fn client_mut(&mut self, client: &str, now_ms: u64) -> Result<&mut Client, Error> {
        if !self.clients.contains_key(client) {
            if self.clients.len() >= self.limits.max_connections {
                let produce = self.limits.produce_bytes_per_sec;
                let fetch = self.limits.fetch_bytes_per_sec;
                self.clients.retain(|_, state| {
                    if state.connections != 0 {
                        return true;
                    }
                    state.produce.refill(produce, now_ms);
                    state.fetch.refill(fetch, now_ms);
                    !(state.produce.full(produce) && state.fetch.full(fetch))
                });
            }
            if self.clients.len() >= self.limits.max_connections {
                return Err(Error::Throttled);
            }
            self.clients.insert(
                client.to_owned(),
                Client {
                    connections: 0,
                    produce: Bucket::new(self.limits.produce_bytes_per_sec, now_ms),
                    fetch: Bucket::new(self.limits.fetch_bytes_per_sec, now_ms),
                },
            );
        }
        // Present: either it was already stored, or the insert above added it.
        #[expect(
            clippy::unwrap_used,
            reason = "the client is present after the insert above"
        )]
        Ok(self.clients.get_mut(client).unwrap())
    }

    fn named(&self, client: &str) -> Result<(), Error> {
        if client.is_empty() || client.len() > 249 || client.chars().any(|c| c.is_control()) {
            Err(Error::InvalidRequest)
        } else {
            Ok(())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn limits() -> Limits {
        Limits::new(100, 100, 50, 2, 4, 1_000, 1_000).unwrap_or_else(|_| panic!("quota limits"))
    }

    #[test]
    fn independent_buckets_and_fractional_refill_survive_interleaved_requests() {
        let limits =
            Limits::new(1, 1, 1, 1, 4, 100, 2_000).unwrap_or_else(|error| panic!("{error:?}"));
        let mut window = Window::new(limits, 0);
        window
            .admit(Some("a"), Kind::Produce, 1, 0)
            .unwrap_or_else(|error| panic!("{error:?}"));
        window
            .admit(Some("a"), Kind::Fetch, 1, 0)
            .unwrap_or_else(|error| panic!("{error:?}"));
        // Produce refills must not move the fetch clock, and frequent calls
        // must accumulate sub-byte credit instead of rounding it away.
        for time in [250, 500, 750] {
            assert!(window.admit(Some("a"), Kind::Produce, 3, time).is_err());
        }
        assert_eq!(
            window
                .admit(Some("a"), Kind::Produce, 1, 1_000)
                .unwrap_or_else(|error| panic!("{error:?}"))
                .throttle_ms,
            0
        );
        assert_eq!(
            window
                .admit(Some("a"), Kind::Fetch, 1, 1_000)
                .unwrap_or_else(|error| panic!("{error:?}"))
                .throttle_ms,
            0
        );
    }

    #[test]
    fn simultaneous_delayed_work_accumulates_debt_and_rejections_do_not_spend() {
        let mut window = Window::new(limits(), 0);
        assert_eq!(
            window
                .admit(Some("a"), Kind::Produce, 100, 0)
                .unwrap_or_else(|error| panic!("{error:?}"))
                .throttle_ms,
            0
        );
        assert_eq!(
            window
                .admit(Some("a"), Kind::Produce, 50, 0)
                .unwrap_or_else(|error| panic!("{error:?}"))
                .throttle_ms,
            500
        );
        assert!(matches!(
            window.admit(Some("a"), Kind::Produce, 51, 0),
            Err(Error::Throttled)
        ));
        assert_eq!(
            window
                .admit(Some("a"), Kind::Produce, 50, 0)
                .unwrap_or_else(|error| panic!("{error:?}"))
                .throttle_ms,
            1_000
        );
        assert_eq!(
            window
                .admit(Some("a"), Kind::Produce, 25, 500)
                .unwrap_or_else(|error| panic!("{error:?}"))
                .throttle_ms,
            750
        );
        assert_eq!(
            window
                .admit(None, Kind::Replication, 50, 0)
                .unwrap_or_else(|error| panic!("{error:?}"))
                .throttle_ms,
            0
        );
        assert_eq!(
            window
                .admit(None, Kind::Replication, 25, 0)
                .unwrap_or_else(|error| panic!("{error:?}"))
                .throttle_ms,
            500
        );
        assert_eq!(
            window
                .admit(None, Kind::Replication, 25, 0)
                .unwrap_or_else(|error| panic!("{error:?}"))
                .throttle_ms,
            1_000
        );
    }

    #[test]
    fn identity_churn_is_bounded_without_erasing_live_quota_debt() {
        let limits = Limits::new(100, 100, 100, 1, 2, 1_000, 1_000)
            .unwrap_or_else(|error| panic!("{error:?}"));
        let mut window = Window::new(limits, 0);
        window
            .connect_at("a", 0)
            .unwrap_or_else(|error| panic!("{error:?}"));
        window
            .admit(Some("a"), Kind::Produce, 150, 0)
            .unwrap_or_else(|error| panic!("{error:?}"));
        window
            .disconnect("a")
            .unwrap_or_else(|error| panic!("{error:?}"));
        window
            .connect_at("b", 0)
            .unwrap_or_else(|error| panic!("{error:?}"));
        assert!(matches!(window.connect_at("c", 0), Err(Error::Throttled)));
        assert_eq!(window.clients.len(), 2);
        assert_eq!(
            window
                .admit(Some("a"), Kind::Produce, 50, 0)
                .unwrap_or_else(|error| panic!("{error:?}"))
                .throttle_ms,
            1_000
        );
        // a's debt is paid after one second, but it needs another second to
        // refill its burst allowance; only then can a new identity replace it.
        assert!(matches!(
            window.connect_at("c", 1_000),
            Err(Error::Throttled)
        ));
        window
            .connect_at("c", 2_000)
            .unwrap_or_else(|error| panic!("{error:?}"));
        assert_eq!(window.clients.len(), 2);
        assert!(window.clients.contains_key("b"));
        assert!(window.clients.contains_key("c"));
    }

    #[test]
    fn legacy_connect_reuses_latest_time_without_regressing_on_older_calls() {
        let limits = Limits::new(100, 100, 100, 1, 2, 1_000, 1_000)
            .unwrap_or_else(|error| panic!("{error:?}"));
        let mut window = Window::new(limits, 0);
        window
            .connect("a")
            .unwrap_or_else(|error| panic!("{error:?}"));
        window
            .admit(Some("a"), Kind::Produce, 100, 0)
            .unwrap_or_else(|error| panic!("{error:?}"));
        window
            .disconnect("a")
            .unwrap_or_else(|error| panic!("{error:?}"));
        window
            .connect("b")
            .unwrap_or_else(|error| panic!("{error:?}"));
        assert!(matches!(window.connect("c"), Err(Error::Throttled)));
        window
            .admit(None, Kind::Replication, 1, 1_000)
            .unwrap_or_else(|error| panic!("{error:?}"));
        assert!(matches!(window.connect_at("b", 0), Err(Error::Throttled)));
        window
            .connect("c")
            .unwrap_or_else(|error| panic!("{error:?}"));
        assert!(window.clients.contains_key("b"));
        assert!(window.clients.contains_key("c"));
    }

    #[test]
    fn arithmetic_and_work_ids_fail_closed_without_losing_permits() {
        let limits = Limits::new(u64::MAX, u64::MAX, u64::MAX, 1, 1, u64::MAX, 1_000)
            .unwrap_or_else(|error| panic!("{error:?}"));
        let mut window = Window::new(limits, 0);
        let first = window
            .admit(None, Kind::Replication, u64::MAX, 0)
            .unwrap_or_else(|error| panic!("{error:?}"));
        assert!(matches!(
            window.admit(None, Kind::Replication, 1, 0),
            Err(Error::Throttled)
        ));
        assert_eq!(
            window
                .complete(first.work_id)
                .unwrap_or_else(|error| panic!("{error:?}")),
            u64::MAX
        );
        window.next_work = u64::MAX;
        assert!(matches!(
            window.admit(None, Kind::Replication, 1, u64::MAX),
            Err(Error::Throttled)
        ));
        assert_eq!(window.inflight_bytes(), 0);
        assert!(window.work.is_empty());
    }

    #[test]
    fn timed_rate_changes_refill_at_the_old_rate_and_keep_debt() {
        let limits =
            Limits::new(1, 1, 1, 1, 1, 1_000, 2_000).unwrap_or_else(|error| panic!("{error:?}"));
        let mut window = Window::new(limits, 0);
        window
            .admit(Some("a"), Kind::Produce, 1, 0)
            .unwrap_or_else(|error| panic!("{error:?}"));
        window
            .set_rates_at(100, 100, 100, 1_000)
            .unwrap_or_else(|error| panic!("{error:?}"));
        assert_eq!(
            window
                .admit(Some("a"), Kind::Produce, 2, 1_000)
                .unwrap_or_else(|error| panic!("{error:?}"))
                .throttle_ms,
            10
        );
        window
            .set_rates_at(1, 1, 1, 1_000)
            .unwrap_or_else(|error| panic!("{error:?}"));
        assert_eq!(
            window
                .admit(Some("a"), Kind::Produce, 1, 1_000)
                .unwrap_or_else(|error| panic!("{error:?}"))
                .throttle_ms,
            2_000
        );
        assert!(matches!(
            window.set_rates_at(0, 1, 1, 2_000),
            Err(Error::InvalidLimits)
        ));
        assert!(matches!(
            window.admit(Some("a"), Kind::Produce, 1, 1_000),
            Err(Error::Throttled)
        ));
    }

    #[test]
    fn malformed_direct_limits_and_extreme_debt_do_not_panic() {
        let mut limits = limits();
        limits.replication_bytes_per_sec = 0;
        let mut window = Window::new(limits, 0);
        assert!(matches!(
            window.admit(None, Kind::Replication, 1, 0),
            Err(Error::InvalidLimits)
        ));
        window.replication.debt = u128::MAX;
        window.replication.credit = 0;
        window.limits.replication_bytes_per_sec = 1;
        assert!(matches!(
            window.admit(None, Kind::Replication, 1, 0),
            Err(Error::Throttled)
        ));
        assert_eq!(window.inflight_bytes(), 0);
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
        window
            .set_rates(10, 10, 10)
            .unwrap_or_else(|error| panic!("{error:?}"));
        assert!(matches!(
            window.admit(Some("a"), Kind::Produce, 50, 0),
            Err(Error::Throttled)
        ));
        let mut replication_window = Window::new(limits(), 1_000);
        replication_window
            .set_rates(10, 10, 10)
            .unwrap_or_else(|error| panic!("{error:?}"));
        assert!(matches!(
            replication_window.admit(None, Kind::Replication, 40, 0),
            Err(Error::Throttled)
        ));
        let mut produce = Window::new(limits(), 1_000);
        let kept = produce
            .admit(Some("a"), Kind::Produce, 40, 1_000)
            .unwrap_or_else(|error| panic!("{error:?}"));
        assert_eq!(kept.throttle_ms, 0);
        produce
            .set_rates(10, 10, 10)
            .unwrap_or_else(|error| panic!("{error:?}"));
        assert!(matches!(
            produce.admit(Some("a"), Kind::Produce, 40, 0),
            Err(Error::Throttled)
        ));
    }
}

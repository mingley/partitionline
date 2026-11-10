//! Router-wide Produce bandwidth policy used by the opt-in quota router.
use std::{
    fmt,
    time::{Duration, Instant},
};

const BYTE_NANOS: u128 = 1_000_000_000;
const MAX_THROTTLE_MILLIS: u32 = 60_000;

/// Validated Produce byte rate and burst.
///
/// The shared policy uses one in-memory bucket for one router. The optional
/// client-label policy uses a bounded set of in-memory buckets. Neither is
/// Kafka's sampled per-user or per-client quota. Restarting the router resets
/// its debt. The charge is the request header and body, excluding the
/// four-byte frame prefix.
///
/// Each connection's mute is capped while debt is shared. This does not enforce
/// an instantaneous bandwidth limit or fairness among concurrent connections.
///
/// The actor samples time after processing the request (or its Produce batch)
/// and sets an absolute resume deadline. Time spent delivering the reply and
/// writing the response consumes that interval; the remaining pause after a
/// response may be shorter than the advertised throttle or already elapsed.
///
/// The transport writes Produce responses before waiting for the pause for
/// every supported version. Apache clients apply response throttling only from
/// Produce version 6. Timing compatibility, especially with older clients, has
/// not been verified against a running Kafka broker.
#[derive(Debug, Clone, Copy)]
pub struct ProduceQuota {
    rate: u64,
    burst: u64,
    maximum_millis: u32,
}

/// A bounded set of per-client Produce buckets and one fallback bucket.
///
/// Configured client IDs are exact, case-sensitive Kafka request-header labels,
/// not authenticated identities. A client can choose its own label. Unknown,
/// null, empty and malformed IDs use the shared fallback bucket. This policy is
/// not Kafka's sampled per-user or per-client quota and is not an authorization
/// or tenant-isolation boundary. Bucket debt is reset when the router restarts.
#[derive(Debug, Clone)]
pub struct ClientProduceQuotas {
    fallback: ProduceQuota,
    clients: Vec<(Box<str>, ProduceQuota)>,
}

impl ClientProduceQuotas {
    /// Configure at most 256 distinct nonempty client IDs of at most 256 UTF-8
    /// bytes each. Retained names are compacted and sorted at configuration
    /// time; requests do not create buckets or allocate names.
    pub fn new(
        fallback: ProduceQuota,
        clients: Vec<(String, ProduceQuota)>,
    ) -> Result<Self, QuotaConfigError> {
        if clients.len() > 256
            || clients
                .iter()
                .any(|(name, _)| name.is_empty() || name.len() > 256)
        {
            return Err(QuotaConfigError);
        }
        let mut clients: Vec<(Box<str>, ProduceQuota)> = clients
            .into_iter()
            .map(|(name, quota)| (name.into_boxed_str(), quota))
            .collect();
        clients.sort_unstable_by(|left, right| left.0.cmp(&right.0));
        if clients.windows(2).any(|pair| pair[0].0 == pair[1].0) {
            return Err(QuotaConfigError);
        }
        Ok(Self { fallback, clients })
    }
}

#[derive(Debug, Clone)]
pub(super) enum QuotaPolicy {
    Shared(ProduceQuota),
    Clients(ClientProduceQuotas),
}

/// Invalid Produce quota configuration.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct QuotaConfigError;

impl fmt::Display for QuotaConfigError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("invalid Produce quota")
    }
}
impl std::error::Error for QuotaConfigError {}

impl ProduceQuota {
    /// Set positive bytes per second, burst bytes and maximum per-response mute.
    ///
    /// The maximum is 1..=60,000 ms; excess debt is retained for later requests.
    pub fn new(rate: u64, burst: u64, maximum_millis: u32) -> Result<Self, QuotaConfigError> {
        if rate == 0 || burst == 0 || !(1..=MAX_THROTTLE_MILLIS).contains(&maximum_millis) {
            return Err(QuotaConfigError);
        }
        Ok(Self {
            rate,
            burst,
            maximum_millis,
        })
    }
}

/// Actor-owned meters. The lookup table never grows from request data.
pub(super) struct QuotaMeters {
    fallback: QuotaMeter,
    clients: Vec<(Box<str>, QuotaMeter)>,
}

impl QuotaMeters {
    pub(super) fn new(policy: QuotaPolicy, now: Instant) -> Self {
        match policy {
            QuotaPolicy::Shared(policy) => Self {
                fallback: QuotaMeter::new(policy, now),
                clients: Vec::new(),
            },
            QuotaPolicy::Clients(policy) => Self {
                fallback: QuotaMeter::new(policy.fallback, now),
                clients: policy
                    .clients
                    .into_iter()
                    .map(|(name, quota)| (name, QuotaMeter::new(quota, now)))
                    .collect(),
            },
        }
    }

    /// Select before processing mutates the request. Parse failures and unknown
    /// names use the shared fallback, and the unconfigured common path does not
    /// parse a header a second time.
    pub(super) fn select(&self, request: &[u8], limits: crate::protocol::Limits) -> Option<usize> {
        if self.clients.is_empty() {
            return None;
        }
        let version = request
            .get(2..4)
            .and_then(|bytes| <[u8; 2]>::try_from(bytes).ok())
            .map(i16::from_be_bytes)?;
        if !(3..=13).contains(&version) {
            return None;
        }
        let (header, _) = crate::protocol::RequestHeader::parse(
            request,
            if version >= 9 { 2 } else { 1 },
            limits,
        )
        .ok()?;
        if header.api_key != 0 {
            return None;
        }
        let name = header.client_id?;
        self.clients
            .binary_search_by(|(entry, _)| entry.as_ref().cmp(name))
            .ok()
    }

    pub(super) fn meter(&mut self, selection: Option<usize>) -> Option<&mut QuotaMeter> {
        match selection {
            None => Some(&mut self.fallback),
            Some(index) => self.clients.get_mut(index).map(|(_, meter)| meter),
        }
    }

    pub(super) fn fail_all(&mut self) {
        self.fallback.fail();
        for (_, meter) in &mut self.clients {
            meter.fail();
        }
    }
}

#[derive(Debug, Clone, Copy)]
enum Balance {
    Credit(u128),
    Debt(u128),
    Failed,
}

#[derive(Debug, Clone, Copy)]
pub(super) enum QuotaMark {
    Ready { until: Option<Instant>, millis: i32 },
    Failed,
}

pub(super) struct QuotaMeter {
    policy: ProduceQuota,
    balance: Balance,
    last: Instant,
}

impl QuotaMeter {
    pub(super) fn new(policy: ProduceQuota, now: Instant) -> Self {
        Self {
            policy,
            balance: Balance::Credit(u128::from(policy.burst) * BYTE_NANOS),
            last: now,
        }
    }

    pub(super) fn fail(&mut self) {
        self.balance = Balance::Failed;
    }

    pub(super) fn charge(&mut self, bytes: usize, now: Instant) -> QuotaMark {
        self.charge_with(bytes, now, |instant, duration| {
            instant.checked_add(duration)
        })
    }

    fn charge_with(
        &mut self,
        bytes: usize,
        now: Instant,
        deadline: impl FnOnce(Instant, Duration) -> Option<Instant>,
    ) -> QuotaMark {
        if matches!(self.balance, Balance::Failed) {
            return QuotaMark::Failed;
        }
        let elapsed = now.saturating_duration_since(self.last);
        self.last = self.last.max(now);
        let refill = elapsed
            .as_nanos()
            .saturating_mul(u128::from(self.policy.rate));
        let cap = u128::from(self.policy.burst) * BYTE_NANOS;
        self.balance = match self.balance {
            Balance::Credit(credit) => Balance::Credit(credit.saturating_add(refill).min(cap)),
            Balance::Debt(debt) if refill >= debt => Balance::Credit((refill - debt).min(cap)),
            Balance::Debt(debt) => Balance::Debt(debt - refill),
            Balance::Failed => return QuotaMark::Failed,
        };
        let Some(cost) = (bytes as u128).checked_mul(BYTE_NANOS) else {
            self.fail();
            return QuotaMark::Failed;
        };
        self.balance = match self.balance {
            Balance::Credit(credit) if credit >= cost => Balance::Credit(credit - cost),
            Balance::Credit(credit) => Balance::Debt(cost - credit),
            Balance::Debt(debt) => match debt.checked_add(cost) {
                Some(debt) => Balance::Debt(debt),
                None => {
                    self.fail();
                    return QuotaMark::Failed;
                }
            },
            Balance::Failed => return QuotaMark::Failed,
        };
        let debt = match self.balance {
            Balance::Credit(_) => {
                return QuotaMark::Ready {
                    until: None,
                    millis: 0,
                }
            }
            Balance::Debt(debt) => debt,
            Balance::Failed => return QuotaMark::Failed,
        };
        let denominator = u128::from(self.policy.rate) * 1_000_000;
        let rounded = debt / denominator + u128::from(debt % denominator != 0);
        let millis = rounded.min(u128::from(self.policy.maximum_millis)) as u32;
        match deadline(self.last, Duration::from_millis(u64::from(millis))) {
            Some(until) => QuotaMark::Ready {
                until: Some(until),
                millis: millis as i32,
            },
            None => {
                self.fail();
                QuotaMark::Failed
            }
        }
    }
}

#[cfg(test)]
#[allow(clippy::expect_used, clippy::panic, clippy::unwrap_used)]
mod tests {
    use super::*;

    fn ms(mark: QuotaMark) -> i32 {
        match mark {
            QuotaMark::Ready { millis, .. } => millis,
            QuotaMark::Failed => panic!("meter failed"),
        }
    }

    fn header(version: i16, client: Option<&str>) -> Vec<u8> {
        let mut bytes = Vec::new();
        bytes.extend_from_slice(&0i16.to_be_bytes());
        bytes.extend_from_slice(&version.to_be_bytes());
        bytes.extend_from_slice(&7i32.to_be_bytes());
        match client {
            Some(client) => {
                bytes.extend_from_slice(&(client.len() as i16).to_be_bytes());
                bytes.extend_from_slice(client.as_bytes());
            }
            None => bytes.extend_from_slice(&(-1i16).to_be_bytes()),
        }
        if version >= 9 {
            bytes.push(0); // flexible request header tags
        }
        bytes
    }

    #[test]
    fn named_client_buckets_are_independent_and_fallback_is_shared() {
        let now = Instant::now();
        let policy = ProduceQuota::new(1_000, 100, 10_000).unwrap();
        let config = ClientProduceQuotas::new(
            policy,
            vec![("bravo".into(), policy), ("alpha".into(), policy)],
        )
        .unwrap();
        let mut meters = QuotaMeters::new(QuotaPolicy::Clients(config), now);
        let limits = crate::protocol::Limits::default();
        let a = meters.select(&header(3, Some("alpha")), limits);
        let b = meters.select(&header(9, Some("bravo")), limits);
        assert!(a.is_some() && b.is_some() && a != b);
        assert_eq!(meters.select(&header(13, Some("alpha")), limits), a);
        assert_eq!(ms(meters.meter(a).unwrap().charge(200, now)), 100);
        assert_eq!(ms(meters.meter(b).unwrap().charge(50, now)), 0);
        assert_eq!(ms(meters.meter(a).unwrap().charge(100, now)), 200);
        assert_eq!(meters.select(&header(9, Some("other")), limits), None);
        assert_eq!(meters.select(&header(9, None), limits), None);
        assert_eq!(meters.select(&header(9, Some("")), limits), None);
        assert_eq!(meters.select(&[0, 0, 0, 9], limits), None);
        assert_eq!(ms(meters.meter(None).unwrap().charge(150, now)), 50);
        assert_eq!(ms(meters.meter(None).unwrap().charge(50, now)), 100);

        // The original shared policy deliberately aggregates different labels.
        let mut shared = QuotaMeters::new(QuotaPolicy::Shared(policy), now);
        assert_eq!(shared.select(&header(3, Some("alpha")), limits), None);
        assert_eq!(shared.select(&header(9, Some("bravo")), limits), None);
        assert_eq!(ms(shared.meter(None).unwrap().charge(200, now)), 100);
        assert_eq!(ms(shared.meter(None).unwrap().charge(50, now)), 150);
    }

    #[test]
    fn client_configuration_is_bounded_and_rejects_duplicates() {
        let q = ProduceQuota::new(100, 100, 1_000).unwrap();
        assert!(ClientProduceQuotas::new(q, vec![("".into(), q)]).is_err());
        assert!(ClientProduceQuotas::new(q, vec![("x".repeat(257), q)]).is_err());
        assert!(ClientProduceQuotas::new(q, vec![("a".into(), q), ("a".into(), q)]).is_err());
        let many = (0..257).map(|i| (i.to_string(), q)).collect();
        assert!(ClientProduceQuotas::new(q, many).is_err());
        assert!(ClientProduceQuotas::new(q, vec![("é".repeat(128), q)]).is_ok());
    }

    #[test]
    fn failed_named_meter_does_not_erase_or_poison_other_debts() {
        let now = Instant::now();
        let q = ProduceQuota::new(100, 100, 10_000).unwrap();
        let config =
            ClientProduceQuotas::new(q, vec![("alpha".into(), q), ("bravo".into(), q)]).unwrap();
        let mut meters = QuotaMeters::new(QuotaPolicy::Clients(config), now);
        let limits = crate::protocol::Limits::default();
        let a = meters.select(&header(3, Some("alpha")), limits);
        let b = meters.select(&header(3, Some("bravo")), limits);
        meters.meter(a).unwrap().balance = Balance::Debt(u128::MAX);
        assert!(matches!(
            meters.meter(a).unwrap().charge(1, now),
            QuotaMark::Failed
        ));
        assert_eq!(ms(meters.meter(b).unwrap().charge(150, now)), 500);
        assert!(matches!(
            meters.meter(a).unwrap().charge(1, now),
            QuotaMark::Failed
        ));
        assert_eq!(ms(meters.meter(b).unwrap().charge(1, now)), 510);
    }

    #[test]
    fn partial_refill_and_round_up_preserve_debt() {
        let now = Instant::now();
        let mut meter = QuotaMeter::new(ProduceQuota::new(1000, 100, 10_000).unwrap(), now);
        assert_eq!(ms(meter.charge(50, now)), 0);
        assert_eq!(ms(meter.charge(100, now)), 50);
        assert_eq!(ms(meter.charge(10, now + Duration::from_millis(20))), 40);
        assert_eq!(ms(meter.charge(1, now + Duration::from_millis(60))), 1);
    }

    #[test]
    fn capped_delay_does_not_erase_debt_and_old_time_does_not_refill() {
        let now = Instant::now();
        let mut meter = QuotaMeter::new(ProduceQuota::new(1, 1, 1000).unwrap(), now);
        assert_eq!(ms(meter.charge(100, now)), 1000);
        assert_eq!(ms(meter.charge(1, now + Duration::from_secs(1))), 1000);
        assert_eq!(ms(meter.charge(1, now)), 1000);
        assert!(matches!(meter.balance, Balance::Debt(d) if d == 100 * BYTE_NANOS));
    }

    #[test]
    fn overflow_fails_closed_and_config_is_validated() {
        assert!(ProduceQuota::new(0, 1, 1).is_err());
        assert!(ProduceQuota::new(1, 0, 1).is_err());
        assert!(ProduceQuota::new(1, 1, 0).is_err());
        assert!(ProduceQuota::new(1, 1, 60_001).is_err());
        let now = Instant::now();
        let mut meter = QuotaMeter::new(ProduceQuota::new(1, 1, 1).unwrap(), now);
        meter.balance = Balance::Debt(u128::MAX);
        assert!(matches!(meter.charge(1, now), QuotaMark::Failed));
        assert!(matches!(
            meter.charge(1, now + Duration::from_secs(1)),
            QuotaMark::Failed
        ));

        let mut meter = QuotaMeter::new(ProduceQuota::new(1, 1, 1).unwrap(), now);
        assert!(matches!(
            meter.charge_with(2, now, |_, _| None),
            QuotaMark::Failed
        ));
        assert!(matches!(
            meter.charge(1, now + Duration::from_secs(1)),
            QuotaMark::Failed
        ));
    }
}

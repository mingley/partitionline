//! Router-wide Produce bandwidth policy used by the opt-in quota router.
use std::{
    fmt,
    time::{Duration, Instant},
};

const BYTE_NANOS: u128 = 1_000_000_000;
const MAX_THROTTLE_MILLIS: u32 = 60_000;

/// Validated router-wide Produce byte rate and burst.
///
/// The policy uses one in-memory bucket shared by all connections for one
/// router, not Kafka's per-user or per-client quota. Restarting the router
/// resets its debt. The charge is the request header and body, excluding the
/// four-byte frame prefix.
///
/// The actor samples time after processing the request (or its Produce batch)
/// and sets an absolute resume deadline. Time spent delivering the reply and
/// writing the response consumes that interval; the remaining pause after a
/// response may be shorter than the advertised throttle or already elapsed.
#[derive(Debug, Clone, Copy)]
pub struct ProduceQuota {
    rate: u64,
    burst: u64,
    maximum_millis: u32,
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

//! KL04-06 fixed-rate open-loop arrivals: a pause never moves later appointments.

use std::time::Duration;

/// Absolute arrival offsets `floor(id * 1e9 / rate)` nanoseconds from origin.
#[derive(Clone, Copy, Debug)]
pub struct FixedRateSchedule {
    rate: u64,
}

impl FixedRateSchedule {
    /// `rate` is records per second in `1..=1_000_000_000`.
    pub fn new(rate: u64) -> Result<Self, String> {
        if rate == 0 || rate > 1_000_000_000 {
            return Err("RATE_PER_SECOND must be in 1..=1000000000".into());
        }
        Ok(Self { rate })
    }

    /// Intended arrival of record `id` relative to the measurement origin.
    pub fn arrival(&self, id: u64) -> Result<Duration, String> {
        let ns = u128::from(id)
            .checked_mul(1_000_000_000)
            .and_then(|n| n.checked_div(u128::from(self.rate)))
            .ok_or_else(|| "arrival schedule overflow".to_string())?;
        Ok(Duration::from_nanos(
            u64::try_from(ns).map_err(|_| "arrival schedule overflow".to_string())?,
        ))
    }

    /// Records per second.
    #[must_use]
    pub fn rate(self) -> u64 {
        self.rate
    }
}

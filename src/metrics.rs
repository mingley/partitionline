//! Client counters. Snapshots, not histograms.

/// Produce counters since this [`crate::Producer`] connected.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct ProducerMetrics {
    /// Records successfully queued (`send` / `send_all` / `try_send`).
    pub records_queued: u64,
    /// Records the broker acknowledged (including `acks=0` local complete).
    pub records_acked: u64,
    /// Records that failed (broker error, timeout, closed).
    pub produce_errors: u64,
    /// Key plus value bytes of queued records.
    pub bytes_queued: u64,
}

/// Fetch counters since this [`crate::Consumer`] connected.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct ConsumerMetrics {
    /// Successful [`crate::Consumer::fetch`] / [`crate::ConsumerGroup::poll`] calls.
    pub fetch_rounds: u64,
    /// Records returned to the caller.
    pub records_fetched: u64,
    /// Key plus value bytes of returned records.
    pub bytes_fetched: u64,
    /// Failed fetch rounds.
    pub fetch_errors: u64,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn metrics_default_zero() {
        assert_eq!(ProducerMetrics::default().records_queued, 0);
        assert_eq!(ConsumerMetrics::default().records_fetched, 0);
    }
}

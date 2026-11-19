use super::{Bucket, Error};
use crate::{produce::DATA_API_VERSIONS, protocol};
use std::time::Duration;
use tokio::{sync::Mutex, time::Instant};

const MAX_CLIENTS: usize = 256;
const MAX_CLIENT_BYTES: usize = 249;
const MAX_DELAY_MS: u32 = 60_000;

/// In-memory bandwidth policy for a plaintext listener bound with
/// [`Transport::bind_with_produce_quota`](crate::transport::Transport::bind_with_produce_quota).
///
/// Configured client IDs share a bucket across connections with the same ID.
/// Unlisted and null IDs share one fallback bucket. Every bucket has the same
/// rate and burst. Kafka client IDs are caller-supplied labels. Treat them as
/// traffic classes among cooperating clients.
///
/// A supported Produce frame is charged when the transport acquires a handler
/// slot. With configured IDs, this first checks the request header. A charge
/// remains after a later handler failure or connection close.
/// The byte count includes the request header and excludes the four-byte frame
/// length. Time spent in the handler and writing the response consumes the pause.
/// Concurrent connections can exceed the configured rate. The cap bounds each
/// pause and the bucket retains the debt. Restarting the listener clears debt.
pub struct ProduceQuota {
    rate: u64,
    burst: u64,
    max_delay_ms: u32,
    start: Instant,
    clients: Vec<(Box<str>, Mutex<Bucket>)>,
    fallback: Mutex<Bucket>,
}

impl ProduceQuota {
    /// Require positive byte rate and burst, a pause cap of 1–60,000 ms,
    /// and at most 256 unique client IDs of 1–249 bytes without control characters.
    pub fn new(
        bytes_per_second: u64,
        burst_bytes: u64,
        max_delay_ms: u32,
        client_ids: &[&str],
    ) -> Result<Self, Error> {
        if bytes_per_second == 0
            || burst_bytes == 0
            || !(1..=MAX_DELAY_MS).contains(&max_delay_ms)
            || client_ids.len() > MAX_CLIENTS
            || client_ids.iter().any(|id| {
                id.is_empty() || id.len() > MAX_CLIENT_BYTES || id.chars().any(char::is_control)
            })
        {
            return Err(Error::InvalidLimits);
        }
        let mut clients = client_ids.to_vec();
        clients.sort_unstable();
        if clients.windows(2).any(|ids| ids[0] == ids[1]) {
            return Err(Error::InvalidLimits);
        }
        Ok(Self {
            rate: bytes_per_second,
            burst: burst_bytes,
            max_delay_ms,
            start: Instant::now(),
            clients: clients
                .into_iter()
                .map(|id| (Box::from(id), Mutex::new(Bucket::new(burst_bytes, 0))))
                .collect(),
            fallback: Mutex::new(Bucket::new(burst_bytes, 0)),
        })
    }

    pub(crate) async fn charge(
        &self,
        request: &[u8],
        limits: protocol::Limits,
    ) -> Result<Option<Throttle>, Error> {
        let Some([0, 0, high, low]) = request.get(..4) else {
            return Ok(None);
        };
        let version = i16::from_be_bytes([*high, *low]);
        let produce = DATA_API_VERSIONS[0];
        if !(produce.min_version..=produce.max_version).contains(&version) {
            return Ok(None);
        }
        let bucket = if self.clients.is_empty() {
            &self.fallback
        } else {
            let (header, _) =
                protocol::RequestHeader::parse(request, if version >= 9 { 2 } else { 1 }, limits)
                    .map_err(|_| Error::InvalidRequest)?;
            self.bucket(header.client_id)
        };
        let mut meter = bucket.lock().await;
        let now = Instant::now();
        let now_ms = u64::try_from(now.saturating_duration_since(self.start).as_millis())
            .map_err(|_| Error::Throttled)?;
        let bytes = u64::try_from(request.len()).map_err(|_| Error::InvalidRequest)?;
        let millis = self.debit(&mut meter, bytes, now_ms)?;
        if millis == 0 {
            return Ok(None);
        }
        let until = now
            .checked_add(Duration::from_millis(millis))
            .ok_or(Error::Throttled)?;
        Ok(Some(Throttle { version, until }))
    }

    fn bucket(&self, client_id: Option<&str>) -> &Mutex<Bucket> {
        client_id
            .and_then(|id| {
                self.clients
                    .binary_search_by(|(name, _)| name.as_ref().cmp(id))
                    .ok()
            })
            .map_or(&self.fallback, |index| &self.clients[index].1)
    }

    fn debit(&self, meter: &mut Bucket, bytes: u64, now_ms: u64) -> Result<u64, Error> {
        meter.refill_to(self.rate, self.burst, now_ms);
        let (credit, debt) = meter.charged(bytes)?;
        meter.credit = credit;
        meter.debt = debt;
        Ok(debt
            .div_ceil(u128::from(self.rate))
            .min(u128::from(self.max_delay_ms)) as u64)
    }
}

#[derive(Clone, Copy)]
pub(crate) struct Throttle {
    pub(crate) version: i16,
    pub(crate) until: Instant,
}

impl Throttle {
    pub(crate) fn remaining_ms(self) -> i32 {
        self.until
            .saturating_duration_since(Instant::now())
            .as_nanos()
            .div_ceil(1_000_000) as i32
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn burst_refill_retains_fractional_bytes() -> Result<(), Error> {
        let policy = ProduceQuota::new(10, 20, 500, &[])?;
        let mut meter = Bucket::new(20, 0);
        assert_eq!(policy.debit(&mut meter, 20, 0)?, 0);
        assert_eq!(policy.debit(&mut meter, 1, 30)?, 70);
        assert_eq!(policy.debit(&mut meter, 1, 80)?, 120);
        assert_eq!(policy.debit(&mut meter, 20, 10_000)?, 0);
        Ok(())
    }

    #[test]
    fn capped_delay_retains_excess_debt() -> Result<(), Error> {
        let policy = ProduceQuota::new(100, 10, 200, &[])?;
        let mut meter = Bucket::new(10, 0);
        assert_eq!(policy.debit(&mut meter, 100, 0)?, 200);
        assert_eq!(policy.debit(&mut meter, 10, 800)?, 200);
        assert_eq!(policy.debit(&mut meter, 1, 900)?, 110);
        assert_eq!(policy.debit(&mut meter, 1, 1_200)?, 0);
        Ok(())
    }

    #[test]
    fn configured_clients_have_independent_buckets() -> Result<(), Error> {
        let policy = ProduceQuota::new(100, 10, 200, &["bravo", "alpha"])?;
        assert!(std::ptr::eq(
            policy.bucket(Some("missing")),
            policy.bucket(None),
        ));
        assert!(!std::ptr::eq(
            policy.bucket(Some("alpha")),
            policy.bucket(None),
        ));
        assert!(!std::ptr::eq(
            policy.bucket(Some("alpha")),
            policy.bucket(Some("bravo")),
        ));
        Ok(())
    }

    #[test]
    fn invalid_configuration_is_rejected() {
        assert!(ProduceQuota::new(0, 10, 100, &[]).is_err());
        assert!(ProduceQuota::new(10, 0, 100, &[]).is_err());
        assert!(ProduceQuota::new(10, 10, 60_001, &[]).is_err());
        assert!(ProduceQuota::new(10, 10, 100, &["alpha", "alpha"]).is_err());
        assert!(ProduceQuota::new(10, 10, 100, &[""]).is_err());
        assert!(ProduceQuota::new(10, 10, 100, &["alpha\n"]).is_err());
        assert!(ProduceQuota::new(10, 10, 100, &[&"a".repeat(250)]).is_err());
        assert!(ProduceQuota::new(10, 10, 100, &["alpha"; 257]).is_err());
    }
}

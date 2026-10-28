//! Fetch benchmark limits and measurement phases.
use super::history;
use partitionline::{ConsumerConfig, Error, Result};
use std::time::Duration;

pub(crate) struct Settings {
    pub(crate) count: u64,
    pub(crate) warmup: u64,
    pub(crate) mode: String,
    pub(crate) group: String,
    pub(crate) timeout: Duration,
    pub(crate) config: ConsumerConfig,
}
impl Settings {
    pub(crate) fn from_env(bootstrap: String) -> Result<Self> {
        let count = history::setting("COUNT", 8_000_000u64)?;
        history::positive("COUNT", count)?;
        let warmup = history::setting("WARMUP", 0u64)?;
        if count == 0 || warmup >= count {
            return Err(Error::protocol("COUNT must exceed WARMUP and be positive"));
        }
        let mode = std::env::var("FETCH_MODE").unwrap_or_else(|_| "manual".into());
        if mode != "manual" && mode != "group" {
            return Err(Error::protocol("FETCH_MODE must be manual or group"));
        }
        let group = std::env::var("GROUP_ID").unwrap_or_else(|_| "plbench-fetch".into());
        if group.trim().is_empty() {
            return Err(Error::protocol("GROUP_ID must be nonempty"));
        }
        let mut config = ConsumerConfig::bootstrap([bootstrap]);
        config.max_wait_ms = history::setting("MAX_WAIT_MS", 100i32)?;
        config.max_bytes = history::setting("MAX_BYTES", 16_777_216i32)?;
        config.max_partition_fetch_bytes = history::setting("MAX_PARTITION_BYTES", 1_048_576i32)?;
        config.min_bytes = history::setting("MIN_BYTES", 1i32)?;
        config.buffer_memory = history::setting("FETCH_BUFFER_MEMORY", 32 * 1024 * 1024usize)?;
        config.max_poll_records = Some(history::setting("MAX_POLL_RECORDS", 1000usize)?);
        let timeout = history::setting("RUN_TIMEOUT_MS", 120_000u64)?;
        if config.max_wait_ms < 0
            || config.max_wait_ms > 60_000
            || config.max_bytes <= 0
            || config.max_bytes > 64 * 1024 * 1024
            || config.max_partition_fetch_bytes <= 0
            || config.max_partition_fetch_bytes > 64 * 1024 * 1024
            || config.min_bytes <= 0
            || config.min_bytes > config.max_bytes
            || config.buffer_memory == 0
            || config.buffer_memory > 1024 * 1024 * 1024
            || config.max_poll_records == Some(0)
            || config.max_poll_records > Some(1_000_000)
            || !(1..=3_600_000).contains(&timeout)
        {
            return Err(Error::protocol("invalid fetch benchmark limits"));
        }
        config.enable_auto_commit = false;
        Ok(Self {
            count,
            warmup,
            mode,
            group,
            timeout: Duration::from_millis(timeout),
            config,
        })
    }
    pub(crate) fn effective_json(
        &self,
        isolation: &str,
        verification: &str,
        seed: u64,
        payload: usize,
        headers: usize,
    ) -> String {
        let c = &self.config;
        format!("{{\"mode\":{},\"group_id\":{},\"count\":{},\"warmup_records\":{},\"max_wait_ms\":{},\"max_bytes\":{},\"max_partition_bytes\":{},\"min_bytes\":{},\"max_poll_records\":{},\"buffer_memory_bytes\":{},\"run_timeout_ms\":{},\"isolation\":{},\"verification\":{},\"seed\":{},\"payload_bytes\":{},\"verify_headers\":{},\"auto_commit\":false}}",
            history::quote(&self.mode),history::quote(&self.group),self.count,self.warmup,c.max_wait_ms,c.max_bytes,
            c.max_partition_fetch_bytes,c.min_bytes,c.max_poll_records.unwrap_or(0),c.buffer_memory,self.timeout.as_millis(),
            history::quote(isolation),history::quote(verification),seed,payload,headers)
    }
}

pub(crate) struct Measurement {
    warmup: u64,
    pub(crate) consumed: u64,
    pub(crate) measured: u64,
    pub(crate) measured_value_bytes: u64,
    started: Option<std::time::Instant>,
}
impl Measurement {
    pub(crate) fn new(warmup: u64) -> Self {
        Self {
            warmup,
            consumed: 0,
            measured: 0,
            measured_value_bytes: 0,
            started: (warmup == 0).then(std::time::Instant::now),
        }
    }
    pub(crate) fn record(&mut self, value_bytes: usize) {
        self.consumed += 1;
        if self.consumed <= self.warmup {
            if self.consumed == self.warmup {
                self.started = Some(std::time::Instant::now());
            }
        } else {
            self.measured += 1;
            self.measured_value_bytes += u64::try_from(value_bytes).unwrap_or(u64::MAX);
        }
    }
    pub(crate) fn elapsed(&self) -> Duration {
        self.started
            .map(|start| start.elapsed())
            .unwrap_or_default()
    }
}

#[cfg(test)]
mod tests {
    use super::Measurement;

    #[test]
    fn warmup_boundary_inside_a_poll_excludes_warmup_records_and_bytes() {
        let mut measurement = Measurement::new(2);
        measurement.record(100);
        assert!(measurement.started.is_none());
        measurement.record(200);
        assert!(measurement.started.is_some());
        assert_eq!(
            (measurement.measured, measurement.measured_value_bytes),
            (0, 0)
        );
        measurement.record(17);
        measurement.record(0);
        measurement.record(3);
        assert_eq!(measurement.consumed, 5);
        assert_eq!(
            (measurement.measured, measurement.measured_value_bytes),
            (3, 20)
        );
    }

    #[test]
    fn zero_warmup_counts_the_first_record_and_its_actual_value_length() {
        let mut measurement = Measurement::new(0);
        assert!(measurement.started.is_some());
        measurement.record(13);
        assert_eq!(
            (
                measurement.consumed,
                measurement.measured,
                measurement.measured_value_bytes
            ),
            (1, 1, 13)
        );
    }
}

//! Sequential smoke and scheduled open-loop produce-ack latency example.
//!
//! Sequential `Producer::send` (call to Produce ack) and non-empty
//! `Consumer::fetch` (already-on-log Fetch RPC). Prints one JSON object
//! per kind with p50/p99 in microseconds, explicitly labeled sequential smoke.
//! `LATENCY_MODE=open-loop` offers fixed-rate arrivals independently of delivery,
//! retaining all raw outcomes and timing bounds; see docs/open-loop-latency.md.

use std::time::{Duration, Instant};

use bytes::Bytes;
use partitionline::{Consumer, ConsumerConfig, ProduceRecord, Producer, ProducerConfig};

fn env_or(key: &str, default: &str) -> String {
    std::env::var(key).unwrap_or_else(|_| default.into())
}

fn env_parse<T: std::str::FromStr>(key: &str, default: T) -> T {
    std::env::var(key)
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(default)
}

fn percentile_us(sorted: &[u64], p: u32) -> partitionline::Result<u64> {
    if sorted.is_empty() {
        return Err(partitionline::Error::protocol("no latency samples"));
    }
    let n = sorted.len();
    let rank = n
        .saturating_mul(p as usize)
        .div_ceil(100)
        .saturating_sub(1)
        .min(n.saturating_sub(1));
    sorted
        .get(rank)
        .copied()
        .ok_or_else(|| partitionline::Error::protocol("percentile index"))
}

fn print_latency(kind: &str, mut samples: Vec<u64>, extra: &str) -> partitionline::Result<()> {
    if samples.is_empty() {
        return Err(partitionline::Error::protocol(format!(
            "{kind}: no latency samples"
        )));
    }
    samples.sort_unstable();
    let n = samples.len();
    let min_us = samples.first().copied().unwrap_or(0);
    let max_us = samples.last().copied().unwrap_or(0);
    let sum: u128 = samples.iter().map(|v| u128::from(*v)).sum();
    let mean_us = u64::try_from(sum / u128::from(n as u64))
        .map_err(|_| partitionline::Error::protocol("mean overflow"))?;
    let p50_us = percentile_us(&samples, 50)?;
    let p99_us = percentile_us(&samples, 99)?;
    println!(
        "{{\"kind\":\"{kind}\",\"latency_mode\":\"sequential-smoke\",\"qualification\":false,\"coordinated_omission_avoidance\":false,\"samples\":{n},\"p50_us\":{p50_us},\"p99_us\":{p99_us},\"min_us\":{min_us},\"max_us\":{max_us},\"mean_us\":{mean_us}{extra}}}"
    );
    Ok(())
}

async fn produce_ack(
    producer: &Producer,
    topic: &str,
    value: &Bytes,
    n: u64,
) -> partitionline::Result<Vec<u64>> {
    let mut samples = Vec::with_capacity(usize::try_from(n).unwrap_or(0));
    for _ in 0..n {
        let start = Instant::now();
        let _md = producer
            .send(ProduceRecord::to(topic).value(value.clone()))
            .await?;
        samples.push(u64::try_from(start.elapsed().as_micros()).unwrap_or(u64::MAX));
    }
    Ok(samples)
}

async fn fetch_rpc(consumer: &mut Consumer, count: u64) -> partitionline::Result<(Vec<u64>, u64)> {
    let mut samples = Vec::new();
    let mut got = 0u64;
    let mut empty = 0u32;
    while got < count {
        let start = Instant::now();
        let recs = consumer.fetch().await?;
        let elapsed = u64::try_from(start.elapsed().as_micros()).unwrap_or(u64::MAX);
        if recs.is_empty() {
            empty += 1;
            if empty > 600 {
                return Err(partitionline::Error::Timeout);
            }
            continue;
        }
        empty = 0;
        samples.push(elapsed);
        got += recs.len() as u64;
    }
    Ok((samples, got))
}

#[tokio::main]
async fn main() -> partitionline::Result<()> {
    let latency_mode = env_or("LATENCY_MODE", "sequential-smoke");
    if latency_mode != "sequential-smoke" && latency_mode != "open-loop" {
        return Err(partitionline::Error::protocol(
            "LATENCY_MODE must be sequential-smoke or open-loop",
        ));
    }
    let open_loop = latency_mode == "open-loop";
    let bootstrap = env_or("KAFKA_BOOTSTRAP", "127.0.0.1:9092");
    let topic = env_or("KAFKA_TOPIC", "pllat");
    let payload = if open_loop {
        open_loop::setting("PAYLOAD_BYTES", 100usize)?
    } else {
        env_parse("PAYLOAD_BYTES", 100usize)
    };
    let warmup = if open_loop {
        open_loop::setting("WARMUP", 10_000u64)?
    } else {
        env_parse("WARMUP", 1_000u64)
    };
    let count = env_parse("COUNT", 10_000u64);
    let linger_ms = if open_loop {
        open_loop::setting("LINGER_MS", 0u64)?
    } else {
        env_parse("LINGER_MS", 0u64)
    };
    let acks = if open_loop {
        open_loop::setting("ACKS", 1i16)?
    } else {
        env_parse("ACKS", 1i16)
    };
    let max_wait_ms = env_parse("MAX_WAIT_MS", 100i32);
    let max_bytes = env_parse("MAX_BYTES", 4_096i32);
    let min_bytes = env_parse("MIN_BYTES", 1i32);
    let mode = env_or("MODE", if open_loop { "produce" } else { "both" });
    if open_loop && mode != "produce" {
        return Err(partitionline::Error::protocol(
            "open-loop requires MODE=produce",
        ));
    }
    let load = if open_loop {
        Some(open_loop::Config::from_env(count)?)
    } else {
        None
    };
    if open_loop && acks != 1 && acks != -1 {
        return Err(partitionline::Error::protocol(
            "open-loop broker acknowledgment timing requires ACKS=1 or ACKS=-1",
        ));
    }

    let mut pcfg = ProducerConfig::bootstrap([bootstrap.clone()]);
    pcfg.linger = Duration::from_millis(linger_ms);
    pcfg.acks = acks;
    pcfg.batch_records = 1;
    pcfg.batch_bytes = payload.saturating_add(256).max(1);
    pcfg.connections = 1;
    pcfg.max_in_flight = 1;
    if let Some(ref load) = load {
        pcfg.buffer_memory = load.buffer_memory;
        pcfg.max_block = load.max_block;
        pcfg.delivery_timeout = load.delivery_timeout;
        pcfg.request_timeout = load.request_timeout;
    }
    let producer = Producer::new(pcfg).await?;
    let value = Bytes::from(vec![b'x'; payload]);

    if warmup > 0 {
        let _ = produce_ack(&producer, &topic, &value, warmup).await?;
    }

    if let Some(load) = load {
        let report = open_loop::run(&producer, &topic, &value, &load).await?;
        report.print(&load, payload, acks, linger_ms, warmup);
        producer.close().await?;
        if report.failed() {
            return Err(partitionline::Error::protocol(
                "open-loop recorded rejected, timed-out, or unknown outcomes; retain the raw run",
            ));
        }
        return Ok(());
    }

    if mode == "produce" || mode == "both" {
        let samples = produce_ack(&producer, &topic, &value, count).await?;
        print_latency(
            "produce_ack",
            samples,
            &format!(
                ",\"payload_bytes\":{payload},\"acks\":{acks},\"linger_ms\":{linger_ms},\"client\":\"partitionline\""
            ),
        )?;
    }
    producer.close().await?;

    if mode == "fetch" || mode == "both" {
        if mode == "fetch" {
            let producer = Producer::new({
                let mut cfg = ProducerConfig::bootstrap([bootstrap.clone()]);
                cfg.linger = Duration::ZERO;
                cfg.acks = acks;
                cfg.batch_records = 1;
                cfg.connections = 1;
                cfg.max_in_flight = 1;
                cfg
            })
            .await?;
            let _ = produce_ack(&producer, &topic, &value, count).await?;
            producer.close().await?;
        }
        let mut ccfg = ConsumerConfig::bootstrap([bootstrap]);
        ccfg.max_wait_ms = max_wait_ms;
        ccfg.max_bytes = max_bytes;
        ccfg.min_bytes = min_bytes;
        let mut consumer = Consumer::new(ccfg).await?;
        consumer.assign(&topic, 0, 0).await?;
        let (samples, got) = fetch_rpc(&mut consumer, count).await?;
        print_latency(
            "fetch_rpc",
            samples,
            &format!(
                ",\"consumed\":{got},\"max_wait_ms\":{max_wait_ms},\"max_bytes\":{max_bytes},\"min_bytes\":{min_bytes},\"client\":\"partitionline\""
            ),
        )?;
    }
    Ok(())
}

/// Benchmark-only scheduling and timing; no producer API changes.
pub(crate) mod open_loop {
    use std::future::{poll_fn, Future};
    use std::sync::Arc;
    use std::task::Poll;
    use std::time::{Duration, Instant};

    use bytes::Bytes;
    use parking_lot::Mutex;
    use partitionline::{Error, ProduceRecord, Producer};
    use tokio::task::JoinSet;

    pub(crate) const SAMPLE_FLOOR: usize = 10_000;

    #[derive(Clone, Debug)]
    pub(crate) struct Config {
        pub(crate) count: u64,
        pub(crate) rate_per_second: u64,
        pub(crate) max_pending: usize,
        pub(crate) sample_floor: usize,
        pub(crate) buffer_memory: usize,
        pub(crate) max_block: Duration,
        pub(crate) delivery_timeout: Duration,
        pub(crate) request_timeout: Duration,
    }

    pub(crate) fn setting<T: std::str::FromStr>(key: &str, default: T) -> partitionline::Result<T> {
        match std::env::var(key) {
            Ok(value) => value
                .parse()
                .map_err(|_| Error::protocol(format!("invalid {key}"))),
            Err(std::env::VarError::NotPresent) => Ok(default),
            Err(_) => Err(Error::protocol(format!("invalid {key}"))),
        }
    }

    impl Config {
        pub(crate) fn from_env(count: u64) -> partitionline::Result<Self> {
            let out = Self {
                count: setting("COUNT", count)?,
                rate_per_second: setting("RATE_PER_SECOND", 1_000)?,
                max_pending: setting("MAX_PENDING", 1_024)?,
                sample_floor: setting("SAMPLE_FLOOR", SAMPLE_FLOOR)?,
                buffer_memory: setting("BUFFER_MEMORY", 32 * 1024 * 1024)?,
                max_block: Duration::from_millis(setting("MAX_BLOCK_MS", 1_000)?),
                delivery_timeout: Duration::from_millis(setting("DELIVERY_TIMEOUT_MS", 30_000)?),
                request_timeout: Duration::from_millis(setting("REQUEST_TIMEOUT_MS", 30_000)?),
            };
            out.validate()?;
            Ok(out)
        }

        pub(crate) fn validate(&self) -> partitionline::Result<()> {
            let storage_bytes = usize::try_from(self.count)
                .ok()
                .and_then(|count| count.checked_mul(std::mem::size_of::<Sample>()));
            if self.count == 0
                || storage_bytes
                    .is_none_or(|bytes| bytes > usize::try_from(isize::MAX).unwrap_or(usize::MAX))
            {
                return Err(Error::protocol(
                    "COUNT must be positive and fit the sample vector layout",
                ));
            }
            if self.max_pending == 0 || self.sample_floor < SAMPLE_FLOOR {
                return Err(Error::protocol(
                    "MAX_PENDING must be positive; SAMPLE_FLOOR must be at least 10000",
                ));
            }
            if self.max_block.is_zero()
                || self.delivery_timeout.is_zero()
                || self.request_timeout.is_zero()
            {
                return Err(Error::protocol(
                    "open-loop timeout settings must be positive",
                ));
            }
            let schedule = FixedRateSchedule::new(self.rate_per_second)?;
            let _ = schedule.arrival(self.count - 1)?;
            Ok(())
        }
    }

    /// Absolute arrival offsets: a pause never moves the remaining schedule.
    pub(crate) struct FixedRateSchedule {
        rate: u64,
    }

    impl FixedRateSchedule {
        pub(crate) fn new(rate: u64) -> partitionline::Result<Self> {
            if rate == 0 || rate > 1_000_000_000 {
                return Err(Error::protocol("RATE_PER_SECOND must be in 1..=1000000000"));
            }
            Ok(Self { rate })
        }

        pub(crate) fn arrival(&self, id: u64) -> partitionline::Result<Duration> {
            let ns = u128::from(id) * 1_000_000_000 / u128::from(self.rate);
            Ok(Duration::from_nanos(u64::try_from(ns).map_err(|_| {
                Error::protocol("arrival schedule overflow")
            })?))
        }
    }

    #[derive(Debug)]
    pub(crate) struct Sample {
        pub(crate) id: u64,
        pub(crate) intended_ns: u64,
        pub(crate) offered_ns: u64,
        pub(crate) enqueue_lower_ns: Option<u64>,
        pub(crate) enqueue_upper_ns: Option<u64>,
        pub(crate) acknowledged_ns: Option<u64>,
        pub(crate) completed_ns: u64,
        pub(crate) outcome: &'static str,
        pub(crate) error: Option<String>,
        pub(crate) pending_at_arrival: usize,
        pub(crate) buffered_bytes_at_arrival: u64,
    }

    impl Sample {
        pub(crate) fn offered(id: u64, intended_ns: u64, offered_ns: u64) -> Self {
            Self {
                id,
                intended_ns,
                offered_ns,
                enqueue_lower_ns: None,
                enqueue_upper_ns: None,
                acknowledged_ns: None,
                completed_ns: offered_ns,
                outcome: "unknown",
                error: None,
                pending_at_arrival: 0,
                buffered_bytes_at_arrival: 0,
            }
        }

        pub(crate) fn end_to_end_ns(&self) -> Option<u64> {
            self.acknowledged_ns
                .map(|ack| ack.saturating_sub(self.intended_ns))
        }

        pub(crate) fn enqueue_to_ack_upper_ns(&self) -> Option<u64> {
            self.acknowledged_ns
                .zip(self.enqueue_lower_ns)
                .map(|(ack, enqueue)| ack.saturating_sub(enqueue))
        }

        pub(crate) fn observe_enqueue(
            &mut self,
            before: u64,
            after: u64,
            lower: u64,
            upper: u64,
        ) -> partitionline::Result<()> {
            if after == before {
                return Ok(());
            }
            if after != before.saturating_add(1) || self.enqueue_upper_ns.is_some() {
                return Err(Error::protocol(
                    "enqueue counter changed outside the serialized single-record poll",
                ));
            }
            self.enqueue_lower_ns = Some(lower);
            self.enqueue_upper_ns = Some(upper);
            Ok(())
        }

        fn json(&self) -> String {
            format!(
                "{{\"kind\":\"open_loop_sample\",\"id\":{},\"intended_arrival_ns\":{},\"actual_offer_ns\":{},\"actual_enqueue_lower_ns\":{},\"actual_enqueue_upper_ns\":{},\"acknowledgment_observed_ns\":{},\"independent_receive_ns\":null,\"completed_ns\":{},\"schedule_lag_ns\":{},\"enqueue_wait_upper_ns\":{},\"end_to_end_ns\":{},\"enqueue_to_ack_upper_ns\":{},\"pending_offers_at_arrival\":{},\"producer_buffered_bytes_at_arrival\":{},\"accepted\":{},\"outcome\":\"{}\",\"error\":{}}}",
                self.id, self.intended_ns, self.offered_ns,
                optional(self.enqueue_lower_ns), optional(self.enqueue_upper_ns),
                optional(self.acknowledged_ns), self.completed_ns,
                self.offered_ns.saturating_sub(self.intended_ns),
                optional(self.enqueue_upper_ns.map(|enqueue| enqueue.saturating_sub(self.offered_ns))),
                optional(self.end_to_end_ns()), optional(self.enqueue_to_ack_upper_ns()),
                self.pending_at_arrival, self.buffered_bytes_at_arrival,
                self.enqueue_upper_ns.is_some(), self.outcome,
                self.error.as_ref().map(|s| quote(s)).unwrap_or_else(|| "null".into()),
            )
        }
    }

    fn nanos(duration: Duration) -> u64 {
        u64::try_from(duration.as_nanos()).unwrap_or(u64::MAX)
    }

    fn optional(value: Option<u64>) -> String {
        value
            .map(|v| v.to_string())
            .unwrap_or_else(|| "null".into())
    }

    fn quote(value: &str) -> String {
        let mut out = String::from("\"");
        for c in value.chars() {
            match c {
                '"' => out.push_str("\\\""),
                '\\' => out.push_str("\\\\"),
                '\n' => out.push_str("\\n"),
                '\r' => out.push_str("\\r"),
                '\t' => out.push_str("\\t"),
                c if c.is_control() => out.push_str(&format!("\\u{:04x}", u32::from(c))),
                c => out.push(c),
            }
        }
        out.push('"');
        out
    }

    fn terminal(error: &Error, accepted: bool) -> &'static str {
        match error {
            Error::Timeout => "timed_out",
            Error::Broker { .. } | Error::QueueFull | Error::RecordTooLarge { .. } => "rejected",
            _ if !accepted => "rejected",
            _ => "unknown",
        }
    }

    async fn send_sample(
        producer: Producer,
        record: ProduceRecord,
        origin: Instant,
        serialized_polls: Arc<Mutex<()>>,
        mut sample: Sample,
    ) -> Sample {
        let send = producer.send(record);
        tokio::pin!(send);
        // Only these polls enqueue, and they are serialized even on a multi-thread
        // runtime. Worker tasks change ack/error counters, never records_queued.
        let result = poll_fn(|cx| {
            let _guard = serialized_polls.lock();
            let before = producer.metrics().records_queued;
            let lower = nanos(origin.elapsed());
            let polled = send.as_mut().poll(cx);
            let upper = nanos(origin.elapsed());
            let after = producer.metrics().records_queued;
            if let Err(error) = sample.observe_enqueue(before, after, lower, upper) {
                return Poll::Ready(Err(error));
            }
            polled
        })
        .await;
        sample.completed_ns = nanos(origin.elapsed());
        match result {
            Ok(_) => {
                sample.acknowledged_ns = Some(sample.completed_ns);
                sample.outcome = "acknowledged";
            }
            Err(error) => {
                sample.outcome = terminal(&error, sample.enqueue_upper_ns.is_some());
                sample.error = Some(error.to_string());
            }
        }
        sample
    }

    pub(crate) struct Report {
        pub(crate) samples: Vec<Sample>,
        pub(crate) elapsed_ns: u64,
        pub(crate) max_pending: usize,
    }

    impl Report {
        pub(crate) fn failed(&self) -> bool {
            self.samples.iter().any(|s| s.outcome != "acknowledged")
        }

        pub(crate) fn print(
            &self,
            config: &Config,
            payload: usize,
            acks: i16,
            linger_ms: u64,
            warmup: u64,
        ) {
            for sample in &self.samples {
                println!("{}", sample.json());
            }
            let accepted = self
                .samples
                .iter()
                .filter(|s| s.enqueue_upper_ns.is_some())
                .count();
            let acknowledged = self
                .samples
                .iter()
                .filter(|s| s.outcome == "acknowledged")
                .count();
            let rejected = self
                .samples
                .iter()
                .filter(|s| s.outcome == "rejected")
                .count();
            let timed_out = self
                .samples
                .iter()
                .filter(|s| s.outcome == "timed_out")
                .count();
            let unknown = self
                .samples
                .iter()
                .filter(|s| s.outcome == "unknown")
                .count();
            let capacity_rejections = self
                .samples
                .iter()
                .filter(|s| s.error.as_deref() == Some("benchmark pending capacity exhausted"))
                .count();
            let e2e = summarize(
                self.samples.iter().filter_map(Sample::end_to_end_ns),
                config.sample_floor,
            );
            let service = summarize(
                self.samples
                    .iter()
                    .filter_map(Sample::enqueue_to_ack_upper_ns),
                config.sample_floor,
            );
            let lag = summarize(
                self.samples
                    .iter()
                    .map(|s| s.offered_ns.saturating_sub(s.intended_ns)),
                config.sample_floor,
            );
            let queue = summarize(
                self.samples
                    .iter()
                    .filter_map(|s| s.enqueue_upper_ns.map(|t| t.saturating_sub(s.offered_ns))),
                config.sample_floor,
            );
            let resolution = self
                .samples
                .iter()
                .filter_map(|s| {
                    s.enqueue_upper_ns
                        .zip(s.enqueue_lower_ns)
                        .map(|(upper, lower)| upper.saturating_sub(lower))
                })
                .max()
                .unwrap_or(0);
            println!(
                "{{\"kind\":\"open_loop_produce_ack\",\"latency_mode\":\"open-loop\",\"schedule_type\":\"open_loop_fixed_rate\",\"coordinated_omission_avoidance\":true,\"qualification\":false,\"suite_hold\":\"active\",\"run_disposition\":\"{}\",\"unit\":\"microseconds\",\"timestamp_unit\":\"nanoseconds_since_measurement_origin\",\"sample_floor\":{},\"rate_per_second\":{},\"payload_bytes\":{payload},\"acks\":{acks},\"linger_ms\":{linger_ms},\"warmup_records\":{warmup},\"warmup_excluded\":true,\"elapsed_ns\":{},\"max_pending_configured\":{},\"max_pending_observed\":{},\"buffer_memory_bytes\":{},\"max_block_ms\":{},\"delivery_timeout_ms\":{},\"request_timeout_ms\":{},\"enqueue_observation\":\"serialized_send_poll_bounds\",\"max_enqueue_observation_span_ns\":{resolution},\"acknowledgment_observation\":\"send_future_completion\",\"outcomes\":{{\"offered\":{},\"accepted\":{accepted},\"acknowledged\":{acknowledged},\"consumed\":0,\"rejected\":{rejected},\"timed_out\":{timed_out},\"unknown\":{unknown}}},\"capacity_rejections\":{capacity_rejections},\"end_to_end\":{e2e},\"enqueue_to_ack_upper_bound\":{service},\"schedule_lag\":{lag},\"enqueue_wait_upper_bound\":{queue}}}",
                if self.failed() { "failed" } else { "executed" }, config.sample_floor,
                config.rate_per_second, self.elapsed_ns, config.max_pending, self.max_pending,
                config.buffer_memory, config.max_block.as_millis(), config.delivery_timeout.as_millis(),
                config.request_timeout.as_millis(), self.samples.len(),
            );
        }
    }

    /// Retain rejected offers as well as accepted samples; never await an ack
    /// before advancing the absolute arrival schedule.
    pub(crate) async fn run(
        producer: &Producer,
        topic: &str,
        value: &Bytes,
        config: &Config,
    ) -> partitionline::Result<Report> {
        config.validate()?;
        let schedule = FixedRateSchedule::new(config.rate_per_second)?;
        let origin = Instant::now();
        // Validate Instant arithmetic before offering the first record.
        let _ = origin
            .checked_add(schedule.arrival(config.count - 1)?)
            .ok_or_else(|| Error::protocol("arrival Instant overflow"))?;
        let serialized_polls = Arc::new(Mutex::new(()));
        let mut jobs = JoinSet::new();
        let mut samples = Vec::with_capacity(usize::try_from(config.count).unwrap_or(0));
        let mut max_pending = 0;
        for id in 0..config.count {
            let intended = schedule.arrival(id)?;
            let deadline = tokio::time::Instant::from_std(origin + intended);
            loop {
                tokio::select! {
                    biased;
                    _ = tokio::time::sleep_until(deadline) => break,
                    done = jobs.join_next(), if !jobs.is_empty() => {
                        if let Some(done) = done {
                            samples.push(done.map_err(|error| Error::protocol(format!("open-loop task failed: {error}")))?);
                        }
                    }
                }
            }
            // Finished tasks must not cause spurious capacity rejections.
            while let Some(done) = jobs.try_join_next() {
                samples.push(
                    done.map_err(|error| {
                        Error::protocol(format!("open-loop task failed: {error}"))
                    })?,
                );
            }
            let mut sample = Sample::offered(id, nanos(intended), nanos(origin.elapsed()));
            sample.pending_at_arrival = jobs.len();
            sample.buffered_bytes_at_arrival = producer.metrics().bytes_buffered;
            if jobs.len() >= config.max_pending {
                sample.outcome = "rejected";
                sample.error = Some("benchmark pending capacity exhausted".into());
                samples.push(sample);
                continue;
            }
            let _task = jobs.spawn(send_sample(
                producer.clone(),
                ProduceRecord::to(topic).partition(0).value(value.clone()),
                origin,
                Arc::clone(&serialized_polls),
                sample,
            ));
            max_pending = max_pending.max(jobs.len());
        }
        while let Some(done) = jobs.join_next().await {
            samples.push(
                done.map_err(|error| Error::protocol(format!("open-loop task failed: {error}")))?,
            );
        }
        samples.sort_unstable_by_key(|s| s.id);
        Ok(Report {
            samples,
            elapsed_ns: nanos(origin.elapsed()),
            max_pending,
        })
    }

    fn quantile(sorted: &[u64], permille: usize) -> Option<u64> {
        let rank = sorted
            .len()
            .saturating_mul(permille)
            .div_ceil(1_000)
            .saturating_sub(1);
        sorted.get(rank).copied()
    }

    pub(crate) fn summarize(samples: impl Iterator<Item = u64>, floor: usize) -> String {
        let mut sorted: Vec<u64> = samples.map(|ns| ns / 1_000).collect();
        sorted.sort_unstable();
        let n = sorted.len();
        let eligible = n >= floor.max(SAMPLE_FLOOR);
        let percentile = |p| optional(if eligible { quantile(&sorted, p) } else { None });
        let mean = if n == 0 {
            None
        } else {
            let sum: u128 = sorted.iter().map(|v| u128::from(*v)).sum();
            Some(u64::try_from(sum / u128::try_from(n).unwrap_or(1)).unwrap_or(u64::MAX))
        };
        let mad = quantile(&sorted, 500).and_then(|median| {
            let mut deviations: Vec<u64> = sorted.iter().map(|v| v.abs_diff(median)).collect();
            deviations.sort_unstable();
            quantile(&deviations, 500)
        });
        format!(
            "{{\"sample_count\":{n},\"sample_floor_met\":{eligible},\"p50_us\":{},\"p95_us\":{},\"p99_us\":{},\"p99_9_us\":{},\"min_us\":{},\"max_us\":{},\"mean_us\":{},\"mad_us\":{}}}",
            percentile(500), percentile(950), percentile(990), percentile(999),
            optional(sorted.first().copied()), optional(sorted.last().copied()), optional(mean), optional(mad),
        )
    }
}

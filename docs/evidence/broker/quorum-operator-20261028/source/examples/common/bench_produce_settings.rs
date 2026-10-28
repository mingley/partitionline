//! Validated producer benchmark settings and the pinned C peer's record stream.
use bytes::Bytes;
use partitionline::{Compression, Error, ProducerConfig, Result};
use std::time::Duration;

// Known harness binary locators do not configure the example itself.
const SUPPORTED: &[&str] = &[
    "BENCH_PRODUCE_BINARY",
    "BENCH_FETCH_BINARY",
    "KAFKA_BOOTSTRAP",
    "KAFKA_TOPIC",
    "PAYLOAD_BYTES",
    "WARMUP",
    "WARMUP_SECS",
    "MEASURE_SECS",
    "COUNT",
    "LINGER_MS",
    "ACKS",
    "IDEMPOTENT",
    "RECORD_HISTORY",
    "SEED",
    "RECORD_SEED",
    "CONNECTIONS",
    "MAX_IN_FLIGHT",
    "COMPRESSION",
    "BATCH_SIZE",
    "BATCH_BYTES",
    "BATCH_RECORDS",
    "BUFFER_MEMORY",
    "QUEUE_KBYTES",
    "PARTITIONS",
    "PAYLOAD_MODE",
    "KEY_MODE",
    "MAX_REQUEST_SIZE",
    "DELIVERY_TIMEOUT_MS",
    "RUN_TIMEOUT_MS",
    "TLS_CA_PEM",
    "TLS_SERVER_NAME",
    "SASL_MECHANISM",
    "SASL_USERNAME",
    "SASL_PASSWORD",
    "SASL_OAUTH_PRINCIPAL",
];
const PREFIXES: &[&str] = &[
    "COUNT",
    "COMPRESSION",
    "PARTITIONS",
    "SEED",
    "BENCH_",
    "PAYLOAD_",
    "WARMUP",
    "MEASURE_",
    "BATCH_",
    "KEY_",
    "RECORD_",
    "QUEUE_",
    "MAX_IN_FLIGHT",
    "IDEMPOTENT",
    "CONNECTIONS",
    "LINGER_",
    "BUFFER_MEMORY",
    "ACKS",
    "DELIVERY_TIMEOUT",
    "RUN_TIMEOUT",
    "MAX_REQUEST_SIZE",
    "SASL_",
    "TLS_",
];

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum PayloadMode {
    Seeded,
    ConstantX,
    History,
}
impl PayloadMode {
    pub(crate) fn name(self) -> &'static str {
        match self {
            Self::Seeded => "seeded",
            Self::ConstantX => "constant-x",
            Self::History => "history-v1",
        }
    }
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum KeyMode {
    Id,
    None,
    History,
}
impl KeyMode {
    pub(crate) fn name(self) -> &'static str {
        match self {
            Self::Id => "id",
            Self::None => "none",
            Self::History => "history-v1",
        }
    }
}
#[derive(Clone, Debug)]
pub(crate) struct Settings {
    pub(crate) topic: String,
    pub(crate) payload: usize,
    pub(crate) warmup_records: Option<u64>,
    pub(crate) warmup: Duration,
    pub(crate) measure: Duration,
    pub(crate) count: Option<u64>,
    pub(crate) seed: u64,
    pub(crate) payload_mode: PayloadMode,
    pub(crate) key_mode: KeyMode,
    pub(crate) history_path: Option<String>,
    pub(crate) partitions: Option<usize>,
    pub(crate) run_timeout: Duration,
    pub(crate) producer: ProducerConfig,
}

fn error(name: &str) -> Error {
    Error::protocol(format!("invalid benchmark setting {name}"))
}
fn number(get: &impl Fn(&str) -> Option<String>, name: &str) -> Result<Option<u64>> {
    get(name)
        .map(|value| {
            if let Some(hex) = value.strip_prefix("0x") {
                u64::from_str_radix(hex, 16)
            } else {
                value.parse()
            }
            .map_err(|_| error(name))
        })
        .transpose()
}
fn size(value: u64, name: &str) -> Result<usize> {
    usize::try_from(value).map_err(|_| error(name))
}
fn alias(
    get: &impl Fn(&str) -> Option<String>,
    left: &str,
    right: &str,
    default: u64,
) -> Result<u64> {
    let (left_value, right_value) = (number(get, left)?, number(get, right)?);
    if left_value.is_some() && right_value.is_some() && left_value != right_value {
        return Err(Error::protocol(format!(
            "conflicting benchmark settings {left}/{right}"
        )));
    }
    Ok(left_value.or(right_value).unwrap_or(default))
}
fn bounded(
    get: &impl Fn(&str) -> Option<String>,
    name: &str,
    default: u64,
    min: u64,
    max: u64,
) -> Result<u64> {
    let value = number(get, name)?.unwrap_or(default);
    if !(min..=max).contains(&value) {
        return Err(error(name));
    }
    Ok(value)
}
impl Settings {
    pub(crate) fn from_env() -> Result<Self> {
        for (name, value) in std::env::vars_os() {
            if let Some(name) = name.to_str() {
                if PREFIXES.iter().any(|prefix| name.starts_with(prefix))
                    && !SUPPORTED.contains(&name)
                {
                    return Err(Error::protocol(format!("unknown benchmark setting {name}")));
                }
                if SUPPORTED.contains(&name) && value.to_str().is_none() {
                    return Err(error(name));
                }
            }
        }
        Self::parse(|name| std::env::var(name).ok())
    }
    pub(crate) fn parse(get: impl Fn(&str) -> Option<String>) -> Result<Self> {
        let bootstrap = get("KAFKA_BOOTSTRAP").unwrap_or_else(|| "127.0.0.1:9092".into());
        let topic = get("KAFKA_TOPIC").unwrap_or_else(|| "partitionline".into());
        if bootstrap.trim().is_empty() || topic.trim().is_empty() {
            return Err(error("KAFKA_BOOTSTRAP/KAFKA_TOPIC"));
        }
        let payload = size(
            bounded(&get, "PAYLOAD_BYTES", 100, 1, 10_000_000)?,
            "PAYLOAD_BYTES",
        )?;
        let warmup_records = number(&get, "WARMUP")?;
        let warmup = Duration::from_secs(bounded(
            &get,
            "WARMUP_SECS",
            if warmup_records.is_some() { 0 } else { 2 },
            0,
            3600,
        )?);
        let measure = Duration::from_secs(bounded(&get, "MEASURE_SECS", 5, 0, 3600)?);
        let count = number(&get, "COUNT")?;
        if count == Some(0) || (count.is_none() && measure.is_zero()) {
            return Err(error("COUNT/MEASURE_SECS"));
        }
        let history_path = get("RECORD_HISTORY");
        if history_path.as_ref().is_some_and(|path| path.is_empty()) {
            return Err(error("RECORD_HISTORY"));
        }
        if history_path.is_some() && (payload < 24 || count.is_none()) {
            return Err(Error::protocol(
                "RECORD_HISTORY requires COUNT and PAYLOAD_BYTES >= 24",
            ));
        }
        let seed = alias(&get, "RECORD_SEED", "SEED", 0x5eed_0001)?;
        let payload_mode =
            match get("PAYLOAD_MODE")
                .as_deref()
                .unwrap_or(if history_path.is_some() {
                    "history-v1"
                } else {
                    "seeded"
                }) {
                "seeded" => PayloadMode::Seeded,
                "constant-x" => PayloadMode::ConstantX,
                "history-v1" if history_path.is_some() => PayloadMode::History,
                _ => return Err(error("PAYLOAD_MODE")),
            };
        let key_mode = match get("KEY_MODE")
            .as_deref()
            .unwrap_or(if history_path.is_some() {
                "history-v1"
            } else {
                "id"
            }) {
            "id" => KeyMode::Id,
            "none" => KeyMode::None,
            "history-v1" if history_path.is_some() => KeyMode::History,
            _ => return Err(error("KEY_MODE")),
        };
        if history_path.is_some()
            && (payload_mode != PayloadMode::History || key_mode != KeyMode::History)
        {
            return Err(Error::protocol(
                "RECORD_HISTORY requires history-v1 key and payload modes",
            ));
        }
        let idempotent = match get("IDEMPOTENT").as_deref().unwrap_or("0") {
            "0" | "false" => false,
            "1" | "true" => true,
            _ => return Err(error("IDEMPOTENT")),
        };
        let acks: i16 = get("ACKS")
            .unwrap_or_else(|| "1".into())
            .parse()
            .map_err(|_| error("ACKS"))?;
        if ![-1, 0, 1].contains(&acks) || (idempotent && acks != -1) {
            return Err(Error::protocol(
                "ACKS must be -1, 0 or 1; IDEMPOTENT=1 requires ACKS=-1",
            ));
        }
        let mut producer = ProducerConfig::bootstrap([bootstrap.clone()]);
        producer.acks = acks;
        producer.enable_idempotence = idempotent;
        producer.max_in_flight = size(
            bounded(
                &get,
                "MAX_IN_FLIGHT",
                if idempotent { 5 } else { 16 },
                1,
                1024,
            )?,
            "MAX_IN_FLIGHT",
        )?;
        if idempotent && producer.max_in_flight > 5 {
            return Err(Error::protocol("IDEMPOTENT=1 requires MAX_IN_FLIGHT<=5"));
        }
        producer.connections = size(bounded(&get, "CONNECTIONS", 8, 1, 1024)?, "CONNECTIONS")?;
        producer.batch_records = size(
            bounded(&get, "BATCH_RECORDS", 32768, 1, 1_000_000)?,
            "BATCH_RECORDS",
        )?;
        let batch_bytes = alias(&get, "BATCH_SIZE", "BATCH_BYTES", 1_000_000)?;
        if !(1..=64 * 1024 * 1024).contains(&batch_bytes) {
            return Err(error("BATCH_SIZE/BATCH_BYTES"));
        }
        producer.batch_bytes = size(batch_bytes, "BATCH_SIZE/BATCH_BYTES")?;
        producer.max_request_size = size(
            bounded(&get, "MAX_REQUEST_SIZE", 1_048_576, 1, 64 * 1024 * 1024)?,
            "MAX_REQUEST_SIZE",
        )?;
        if producer.batch_bytes > producer.max_request_size {
            return Err(Error::protocol(
                "batch bytes exceed MAX_REQUEST_SIZE; implicit clamping prohibited",
            ));
        }
        let queue_kbytes = number(&get, "QUEUE_KBYTES")?
            .map(|value| value.checked_mul(1024).ok_or_else(|| error("QUEUE_KBYTES")))
            .transpose()?;
        let buffer_memory = number(&get, "BUFFER_MEMORY")?;
        if queue_kbytes.is_some() && buffer_memory.is_some() && queue_kbytes != buffer_memory {
            return Err(error("QUEUE_KBYTES/BUFFER_MEMORY"));
        }
        let buffer = queue_kbytes.or(buffer_memory).unwrap_or(32 * 1024 * 1024);
        if !(1..=1024 * 1024 * 1024).contains(&buffer) {
            return Err(error("BUFFER_MEMORY"));
        }
        producer.buffer_memory = size(buffer, "BUFFER_MEMORY")?;
        producer.linger = Duration::from_millis(bounded(&get, "LINGER_MS", 5, 0, 60_000)?);
        producer.delivery_timeout =
            Duration::from_millis(bounded(&get, "DELIVERY_TIMEOUT_MS", 30_000, 1, 3_600_000)?);
        producer.compression =
            Compression::from_name(&get("COMPRESSION").unwrap_or_else(|| "none".into()))?;
        let partitions = number(&get, "PARTITIONS")?
            .map(|value| {
                if !(1..=10_000).contains(&value) {
                    Err(error("PARTITIONS"))
                } else {
                    size(value, "PARTITIONS")
                }
            })
            .transpose()?;
        let run_timeout =
            Duration::from_millis(bounded(&get, "RUN_TIMEOUT_MS", 120_000, 1, 3_600_000)?);
        Ok(Self {
            topic,
            payload,
            warmup_records,
            warmup,
            measure,
            count,
            seed,
            payload_mode,
            key_mode,
            history_path,
            partitions,
            run_timeout,
            producer,
        })
    }
    pub(crate) fn effective_json(&self) -> String {
        let p = &self.producer;
        format!("{{\"acks\":{},\"linger_ms\":{},\"batch_size_bytes\":{},\"batch_records\":{},\"max_in_flight\":{},\"idempotence\":{},\"connections\":{},\"buffer_memory_bytes\":{},\"max_request_size\":{},\"delivery_timeout_ms\":{},\"compression\":\"{}\",\"payload_bytes\":{},\"payload_mode\":\"{}\",\"key_mode\":\"{}\",\"record_seed\":{},\"warmup_requested_records\":{},\"warmup_min_duration_ms\":{},\"measure_duration_ms\":{},\"requested_records\":{},\"explicit_partitions\":{},\"run_timeout_ms\":{}}}",
            p.acks,p.linger.as_millis(),p.batch_bytes,p.batch_records,p.max_in_flight,p.enable_idempotence,
            p.connections,p.buffer_memory,p.max_request_size,p.delivery_timeout.as_millis(),p.compression.as_str(),
            self.payload,self.payload_mode.name(),self.key_mode.name(),self.seed,
            self.warmup_records.map(|n|n.to_string()).unwrap_or_else(||"null".into()),self.warmup.as_millis(),self.measure.as_millis(),
            self.count.map(|n|n.to_string()).unwrap_or_else(||"null".into()),self.partitions.map(|n|n.to_string()).unwrap_or_else(||"null".into()),self.run_timeout.as_millis())
    }
}
fn mix(mut value: u64) -> u64 {
    value = value.wrapping_add(0x9e37_79b9_7f4a_7c15);
    value = (value ^ (value >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    value = (value ^ (value >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    value ^ (value >> 31)
}
pub(crate) fn seeded_payload(seed: u64, id: u64, size: usize) -> Bytes {
    let mut state = seed ^ id.wrapping_mul(0x9e37_79b9_7f4a_7c15);
    let mut output = Vec::with_capacity(size);
    while output.len() < size {
        state = mix(state);
        let length = (size - output.len()).min(8);
        output.extend_from_slice(state.to_be_bytes().get(..length).unwrap_or(&[]));
    }
    Bytes::from(output)
}
pub(crate) fn id_key(seed: u64, id: u64) -> Bytes {
    let mut key = [0; 16];
    key[..8].copy_from_slice(&id.to_be_bytes());
    key[8..].copy_from_slice(&mix(seed ^ id).to_be_bytes());
    Bytes::copy_from_slice(&key)
}

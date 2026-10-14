//! Effective-configuration equality gate for gateway produce cells.

use serde::{Deserialize, Serialize};

use crate::cells::{AckMode, Cell};

/// TLS shape compared by the gate. Host and certificate paths are not compared.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TlsMode {
    /// No TLS.
    Plain,
    /// Server TLS without a client certificate.
    Tls,
    /// TLS with a client certificate (mTLS).
    Mtls,
}

impl TlsMode {
    /// From a Kafka `security.protocol` plus whether a client cert is configured.
    #[must_use]
    pub fn from_protocol(protocol: &str, client_cert: bool) -> Self {
        match protocol.to_ascii_uppercase().as_str() {
            "SSL" | "SASL_SSL" if client_cert => Self::Mtls,
            "SSL" | "SASL_SSL" => Self::Tls,
            _ => Self::Plain,
        }
    }

    /// Kafka security.protocol for this mode.
    #[must_use]
    pub fn security_protocol(self) -> &'static str {
        match self {
            Self::Plain => "PLAINTEXT",
            Self::Tls | Self::Mtls => "SSL",
        }
    }
}

/// Settings the driver refuses to compare across when they differ.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ComparableSettings {
    /// Codec name (`gzip` or `lz4`).
    pub codec: String,
    /// Advertised compression level (`Compression::default_level` / librdkafka `compression.level`).
    pub level: i32,
    /// `acks` (`-1` / `0` / `1`).
    pub acks: i16,
    /// Max in-flight Produce requests per connection.
    pub max_in_flight: u64,
    /// Linger in milliseconds.
    pub linger_ms: u64,
    /// Produce batch size in bytes.
    pub batch_size_bytes: u64,
    /// TLS shape.
    pub tls: TlsMode,
}

impl ComparableSettings {
    /// Profile defaults for `cell`, with the campaign TLS mode.
    #[must_use]
    pub fn for_cell(cell: Cell, tls: TlsMode) -> Self {
        Self {
            codec: cell.codec.to_string(),
            level: cell.level,
            acks: cell.acks,
            max_in_flight: cell.max_in_flight,
            linger_ms: cell.linger_ms,
            batch_size_bytes: cell.batch_size_bytes,
            tls,
        }
    }

    /// Parse a partitionline or rust-rdkafka effective-config object.
    pub fn from_effective(value: &serde_json::Value) -> Result<Self, String> {
        let codec = string_field(value, &["codec", "compression", "compression.codec"])?;
        let level = int_field(value, &["level", "compression_level", "compression.level"])?;
        let acks = i16::try_from(int_field(value, &["acks", "request.required.acks"])?)
            .map_err(|_| "acks does not fit i16".to_string())?;
        let max_in_flight = u64_field(
            value,
            &["max_in_flight", "max.in.flight.requests.per.connection"],
        )?;
        let linger_ms = u64_field(value, &["linger_ms", "queue.buffering.max.ms"])?;
        let batch_size_bytes = u64_field(value, &["batch_size_bytes", "batch.size"])?;
        let protocol = value
            .get("security_protocol")
            .or_else(|| value.get("security.protocol"))
            .and_then(serde_json::Value::as_str)
            .unwrap_or("PLAINTEXT");
        let client_cert = value
            .get("tls_client_cert")
            .and_then(serde_json::Value::as_bool)
            .unwrap_or(false);
        Ok(Self {
            codec: codec.to_ascii_lowercase(),
            level,
            acks,
            max_in_flight,
            linger_ms,
            batch_size_bytes,
            tls: TlsMode::from_protocol(protocol, client_cert),
        })
    }
}

fn string_field<'a>(value: &'a serde_json::Value, names: &[&str]) -> Result<&'a str, String> {
    for name in names {
        if let Some(text) = value.get(*name).and_then(serde_json::Value::as_str) {
            return Ok(text);
        }
    }
    Err(format!("missing string field {}", names.join("/")))
}

fn int_field(value: &serde_json::Value, names: &[&str]) -> Result<i32, String> {
    for name in names {
        if let Some(n) = value.get(*name).and_then(serde_json::Value::as_i64) {
            return i32::try_from(n).map_err(|_| format!("{name} does not fit i32"));
        }
        if let Some(text) = value.get(*name).and_then(serde_json::Value::as_str) {
            return text
                .parse()
                .map_err(|_| format!("{name} is not an integer"));
        }
    }
    Err(format!("missing integer field {}", names.join("/")))
}

fn u64_field(value: &serde_json::Value, names: &[&str]) -> Result<u64, String> {
    for name in names {
        if let Some(n) = value.get(*name).and_then(serde_json::Value::as_u64) {
            return Ok(n);
        }
        if let Some(n) = value.get(*name).and_then(serde_json::Value::as_i64) {
            return u64::try_from(n).map_err(|_| format!("{name} is negative"));
        }
        if let Some(text) = value.get(*name).and_then(serde_json::Value::as_str) {
            return text
                .parse()
                .map_err(|_| format!("{name} is not an unsigned integer"));
        }
    }
    Err(format!("missing unsigned field {}", names.join("/")))
}

/// Why two runs cannot be compared.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CompareRefusal {
    /// Gated field that differed.
    pub field: &'static str,
    /// Left-hand value.
    pub left: String,
    /// Right-hand value.
    pub right: String,
}

impl std::fmt::Display for CompareRefusal {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "refusing comparison: {} differs ({} vs {})",
            self.field, self.left, self.right
        )
    }
}

/// Refuse when any gated knob differs. Acknowledgment mode is labeled, not gated here.
pub fn refuse_compare(
    left: &ComparableSettings,
    right: &ComparableSettings,
) -> Result<(), CompareRefusal> {
    let checks: [(&'static str, String, String, bool); 7] = [
        (
            "codec",
            left.codec.clone(),
            right.codec.clone(),
            left.codec == right.codec,
        ),
        (
            "level",
            left.level.to_string(),
            right.level.to_string(),
            left.level == right.level,
        ),
        (
            "acks",
            left.acks.to_string(),
            right.acks.to_string(),
            left.acks == right.acks,
        ),
        (
            "max_in_flight",
            left.max_in_flight.to_string(),
            right.max_in_flight.to_string(),
            left.max_in_flight == right.max_in_flight,
        ),
        (
            "linger_ms",
            left.linger_ms.to_string(),
            right.linger_ms.to_string(),
            left.linger_ms == right.linger_ms,
        ),
        (
            "batch_size_bytes",
            left.batch_size_bytes.to_string(),
            right.batch_size_bytes.to_string(),
            left.batch_size_bytes == right.batch_size_bytes,
        ),
        (
            "tls",
            format!("{:?}", left.tls),
            format!("{:?}", right.tls),
            left.tls == right.tls,
        ),
    ];
    for (field, l, r, ok) in checks {
        if !ok {
            return Err(CompareRefusal {
                field,
                left: l,
                right: r,
            });
        }
    }
    Ok(())
}

/// Fetch knobs both clients must match.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct FetchSettings {
    /// `fetch.min.bytes`.
    pub min_bytes: i32,
    /// `fetch.max.wait.ms`.
    pub max_wait_ms: i32,
    /// `fetch.max.bytes`.
    pub fetch_max_bytes: i32,
    /// `max.partition.fetch.bytes`.
    pub max_partition_fetch_bytes: i32,
    /// TLS shape.
    pub tls: TlsMode,
    /// `isolation.level`.
    pub isolation: String,
}

impl FetchSettings {
    /// Profile defaults with the campaign TLS mode.
    #[must_use]
    pub fn for_cell(tls: TlsMode) -> Self {
        Self {
            min_bytes: crate::cells::FETCH_MIN_BYTES,
            max_wait_ms: crate::cells::FETCH_MAX_WAIT_MS,
            fetch_max_bytes: crate::cells::FETCH_MAX_BYTES,
            max_partition_fetch_bytes: crate::cells::FETCH_PARTITION_BYTES,
            tls,
            isolation: "read_uncommitted".into(),
        }
    }

    /// Parse partitionline or rust-rdkafka effective-config.
    pub fn from_effective(value: &serde_json::Value) -> Result<Self, String> {
        let min_bytes = i32_field(value, &["fetch_min_bytes", "min_bytes", "fetch.min.bytes"])?;
        let max_wait_ms = i32_field(
            value,
            &["fetch_max_wait_ms", "max_wait_ms", "fetch.wait.max.ms"],
        )?;
        let fetch_max_bytes =
            i32_field(value, &["fetch_max_bytes", "max_bytes", "fetch.max.bytes"])?;
        let max_partition_fetch_bytes = i32_field(
            value,
            &[
                "max_partition_fetch_bytes",
                "max.partition.fetch.bytes",
                "fetch.message.max.bytes",
            ],
        )?;
        let isolation = string_field(value, &["isolation", "isolation_level", "isolation.level"])?
            .to_ascii_lowercase();
        let protocol = value
            .get("security_protocol")
            .or_else(|| value.get("security.protocol"))
            .and_then(serde_json::Value::as_str)
            .unwrap_or("PLAINTEXT");
        let client_cert = value
            .get("tls_client_cert")
            .and_then(serde_json::Value::as_bool)
            .unwrap_or(false);
        Ok(Self {
            min_bytes,
            max_wait_ms,
            fetch_max_bytes,
            max_partition_fetch_bytes,
            tls: TlsMode::from_protocol(protocol, client_cert),
            isolation,
        })
    }
}

fn i32_field(value: &serde_json::Value, names: &[&str]) -> Result<i32, String> {
    int_field(value, names)
}

/// Refuse when any gated fetch knob differs.
pub fn refuse_fetch_compare(
    left: &FetchSettings,
    right: &FetchSettings,
) -> Result<(), CompareRefusal> {
    let checks: [(&'static str, String, String, bool); 6] = [
        (
            "min_bytes",
            left.min_bytes.to_string(),
            right.min_bytes.to_string(),
            left.min_bytes == right.min_bytes,
        ),
        (
            "max_wait_ms",
            left.max_wait_ms.to_string(),
            right.max_wait_ms.to_string(),
            left.max_wait_ms == right.max_wait_ms,
        ),
        (
            "fetch_max_bytes",
            left.fetch_max_bytes.to_string(),
            right.fetch_max_bytes.to_string(),
            left.fetch_max_bytes == right.fetch_max_bytes,
        ),
        (
            "max_partition_fetch_bytes",
            left.max_partition_fetch_bytes.to_string(),
            right.max_partition_fetch_bytes.to_string(),
            left.max_partition_fetch_bytes == right.max_partition_fetch_bytes,
        ),
        (
            "tls",
            format!("{:?}", left.tls),
            format!("{:?}", right.tls),
            left.tls == right.tls,
        ),
        (
            "isolation",
            left.isolation.clone(),
            right.isolation.clone(),
            left.isolation == right.isolation,
        ),
    ];
    for (field, l, r, ok) in checks {
        if !ok {
            return Err(CompareRefusal {
                field,
                left: l,
                right: r,
            });
        }
    }
    Ok(())
}

/// Fire-and-forget is never compared with per-record completion.
pub fn refuse_ack_modes(left: AckMode, right: AckMode) -> Result<(), CompareRefusal> {
    if left == right {
        Ok(())
    } else {
        Err(CompareRefusal {
            field: "acknowledgment_mode",
            left: left.label().to_string(),
            right: right.label().to_string(),
        })
    }
}

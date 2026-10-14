//! Canonical gateway produce cells from `docs/plan/gateway-adoption.md` section 0.

use serde::{Deserialize, Serialize};

/// How a cell confirms delivery.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AckMode {
    /// `try_send` plus a timed `flush` that confirms every accepted record.
    FireAndForgetTimedFlush,
    /// One completion per record (`Producer::send` / librdkafka delivery callback).
    PerRecordCompletion,
    /// Manual assignment consume; every ID/hash is verified.
    ManualAssignment,
}

impl AckMode {
    /// Stable label written into result artifacts.
    #[must_use]
    pub fn label(self) -> &'static str {
        match self {
            Self::FireAndForgetTimedFlush => "fire-and-forget confirmed by a timed flush",
            Self::PerRecordCompletion => "per-record completion",
            Self::ManualAssignment => "manual assignment from an explicit offset",
        }
    }
}

/// Load shape for a cell.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Load {
    /// KL04-06 open-loop at 25,000 records/s.
    OpenLoop25k,
    /// Unpaced saturation.
    Unpaced,
    /// Consume a backlog written once by the pinned peer.
    SeededBacklog,
}

impl Load {
    /// Stable id suffix.
    #[must_use]
    pub fn id(self) -> &'static str {
        match self {
            Self::OpenLoop25k => "open-loop-25k",
            Self::Unpaced => "unpaced",
            Self::SeededBacklog => "backlog",
        }
    }

    /// Open-loop rate, if any.
    #[must_use]
    pub fn rate_per_second(self) -> Option<u64> {
        match self {
            Self::OpenLoop25k => Some(25_000),
            Self::Unpaced | Self::SeededBacklog => None,
        }
    }
}

/// One registered produce cell.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Cell {
    /// Cell id registered in performance-leadership.md section 4.
    pub id: &'static str,
    /// Codec.
    pub codec: &'static str,
    /// Advertised compression level.
    pub level: i32,
    /// Acknowledgment mode.
    pub ack_mode: AckMode,
    /// `acks`.
    pub acks: i16,
    /// Max in flight.
    pub max_in_flight: u64,
    /// Linger milliseconds.
    pub linger_ms: u64,
    /// Batch bytes.
    pub batch_size_bytes: u64,
    /// Partitions.
    pub partitions: i32,
}

/// gzip fire-and-forget cell.
pub const GZIP: Cell = Cell {
    id: "lb-gateway-produce-gzip",
    codec: "gzip",
    level: -1,
    ack_mode: AckMode::FireAndForgetTimedFlush,
    acks: -1,
    max_in_flight: 1,
    linger_ms: 100,
    batch_size_bytes: 1_048_576,
    partitions: 8,
};

/// lz4 fire-and-forget cell.
pub const LZ4: Cell = Cell {
    id: "lb-gateway-produce-lz4",
    codec: "lz4",
    level: 9,
    ack_mode: AckMode::FireAndForgetTimedFlush,
    acks: -1,
    max_in_flight: 1,
    linger_ms: 100,
    batch_size_bytes: 1_048_576,
    partitions: 8,
};

/// gzip per-record acknowledgment cell.
pub const ACK: Cell = Cell {
    id: "lb-gateway-produce-ack",
    codec: "gzip",
    level: -1,
    ack_mode: AckMode::PerRecordCompletion,
    acks: -1,
    max_in_flight: 1,
    linger_ms: 100,
    batch_size_bytes: 1_048_576,
    partitions: 8,
};

/// gzip fetch cell over a seeded backlog.
pub const FETCH_GZIP: Cell = Cell {
    id: "lb-gateway-fetch-gzip",
    codec: "gzip",
    level: -1,
    ack_mode: AckMode::ManualAssignment,
    acks: -1,
    max_in_flight: 1,
    linger_ms: 100,
    batch_size_bytes: 1_048_576,
    partitions: 8,
};

/// Java / librdkafka `fetch.min.bytes`.
pub const FETCH_MIN_BYTES: i32 = 1;
/// Java / librdkafka `fetch.max.wait.ms`.
pub const FETCH_MAX_WAIT_MS: i32 = 500;
/// Java / librdkafka `fetch.max.bytes` (50 MiB).
pub const FETCH_MAX_BYTES: i32 = 52_428_800;
/// Java / librdkafka `max.partition.fetch.bytes` (1 MiB).
pub const FETCH_PARTITION_BYTES: i32 = 1_048_576;

/// All produce cells built by KL10-02.
pub const CELLS: [Cell; 3] = [GZIP, LZ4, ACK];

/// Fetch cells built by KL10-14.
pub const FETCH_CELLS: [Cell; 1] = [FETCH_GZIP];

/// Both load shapes every cell runs.
pub const LOADS: [Load; 2] = [Load::OpenLoop25k, Load::Unpaced];

/// Look up a registered cell.
pub fn find(id: &str) -> Result<Cell, String> {
    CELLS
        .iter()
        .copied()
        .find(|cell| cell.id == id)
        .ok_or_else(|| format!("unknown gateway produce cell {id}"))
}

/// Look up a registered fetch cell.
pub fn find_fetch(id: &str) -> Result<Cell, String> {
    FETCH_CELLS
        .iter()
        .copied()
        .find(|cell| cell.id == id)
        .ok_or_else(|| format!("unknown gateway fetch cell {id}"))
}

/// Clients the campaign interleaves.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum ClientKind {
    /// This repository's producer.
    Partitionline,
    /// Isolated rust-rdkafka BaseProducer peer (KL04-05).
    RustRdkafka,
}

impl ClientKind {
    /// Result `scenario.peer` value.
    #[must_use]
    pub fn peer_label(self) -> &'static str {
        match self {
            Self::Partitionline => "partitionline",
            Self::RustRdkafka => "peer-adapter",
        }
    }

    /// Human name.
    #[must_use]
    pub fn name(self) -> &'static str {
        match self {
            Self::Partitionline => "partitionline",
            Self::RustRdkafka => "rust-rdkafka",
        }
    }
}

/// Section 4 pairing: A-B-B-A… for `repetitions` pairs.
#[must_use]
pub fn interleave(repetitions: usize) -> Vec<ClientKind> {
    let mut order = Vec::with_capacity(repetitions.saturating_mul(2));
    for index in 0..repetitions {
        if index % 2 == 0 {
            order.push(ClientKind::Partitionline);
            order.push(ClientKind::RustRdkafka);
        } else {
            order.push(ClientKind::RustRdkafka);
            order.push(ClientKind::Partitionline);
        }
    }
    order
}

/// Differences from gateway-adoption.md section 2. Recorded, not gated.
#[must_use]
pub fn section2_differences() -> [&'static str; 5] {
    [
        "Host is this Linux measurement environment, not the 2026-09-30 Apple M4 Pro laptop.",
        "mTLS is preferred when TLS_CLIENT_CERT_PEM is set; PLAINTEXT is accepted and recorded when it is not.",
        "High-watermark deltas are reconciled against acknowledged counts (section 2 did not check them).",
        "gzip uses the repository default backend (zlib-rs unless built with default-features = false), not the dated miniz_oxide-only profile.",
        "Open-loop 25k and unpaced loads share one run_timeout; section 2 used separate 20 s / 15 s walls.",
    ]
}

/// Differences from gateway-adoption.md section 2 consume table. Recorded, not gated.
#[must_use]
pub fn fetch_section2_differences() -> [&'static str; 6] {
    [
        "Host is this Linux measurement environment, not the 2026-09-30 Apple M4 Pro laptop.",
        "mTLS is preferred when TLS_CLIENT_CERT_PEM is set; PLAINTEXT is accepted and recorded when it is not.",
        "Both clients use matched Java/librdkafka fetch knobs (1 byte min, 500 ms wait, 50 MiB fetch.max.bytes, 1 MiB max.partition.fetch.bytes).",
        "gzip uses the repository default backend (zlib-rs unless built with default-features = false), not the dated miniz_oxide-only profile.",
        "The backlog is COUNT records written once by the pinned rust-rdkafka peer; section 2 did not publish the backlog size.",
        "Every consumed ID/hash is verified against the KL04-07 produce history; section 2 did not report that gate.",
    ]
}

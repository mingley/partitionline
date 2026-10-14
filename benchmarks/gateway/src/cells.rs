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
}

impl AckMode {
    /// Stable label written into result artifacts.
    #[must_use]
    pub fn label(self) -> &'static str {
        match self {
            Self::FireAndForgetTimedFlush => "fire-and-forget confirmed by a timed flush",
            Self::PerRecordCompletion => "per-record completion",
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
}

impl Load {
    /// Stable id suffix.
    #[must_use]
    pub fn id(self) -> &'static str {
        match self {
            Self::OpenLoop25k => "open-loop-25k",
            Self::Unpaced => "unpaced",
        }
    }

    /// Open-loop rate, if any.
    #[must_use]
    pub fn rate_per_second(self) -> Option<u64> {
        match self {
            Self::OpenLoop25k => Some(25_000),
            Self::Unpaced => None,
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

/// All produce cells built by KL10-02.
pub const CELLS: [Cell; 3] = [GZIP, LZ4, ACK];

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

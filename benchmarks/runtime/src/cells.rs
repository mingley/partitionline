//! Producer cell definitions (KL09-09): the eight null-broker cells from
//! `docs/plan/performance-leadership.md` section 4, plus the documented
//! defaults for every knob section 4 leaves open.

use std::time::Duration;

use bytes::Bytes;
use partitionline::protocol::records::Header;

use codec::payload;

/// How the measured phase drives the producer.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DriveMode {
    /// Pipelined sends bounded by [`CellDef::max_in_flight_sends`].
    Pipelined,
    /// Sequential send-and-await, one record at a time.
    Sequential,
    /// Groups of [`CellDef::flush_every`] pipelined sends, each followed
    /// by an awaited flush.
    FlushHeavy,
    /// No sends; RSS is sampled while the client idles.
    Idle,
}

/// One topic's share of a cell.
#[derive(Debug, Clone)]
pub struct CellTopic {
    /// Topic name.
    pub name: &'static str,
    /// Broker partitions for this topic.
    pub partitions: i32,
    /// Records addressed to this topic.
    pub records: usize,
    /// Records per partition (explicit round-robin assignment).
    pub records_per_partition: usize,
}

/// A producer measurement cell.
#[derive(Debug, Clone)]
pub struct CellDef {
    /// Cell ID (`nb-produce-bulk`, ...).
    pub id: &'static str,
    /// Topics and their record counts.
    pub topics: Vec<CellTopic>,
    /// Value bytes per record (includes the 8-byte embedded record ID).
    pub value_bytes: usize,
    /// Key bytes per record.
    pub key_bytes: usize,
    /// Record headers per record.
    pub headers_each: usize,
    /// Key and value bytes per header.
    pub header_kv_bytes: usize,
    /// Payload entropy selector (see [`codec::payload`]).
    pub entropy: &'static str,
    /// Idempotent producer.
    pub idempotent: bool,
    /// Produce acks (ignored when idempotent: idempotence forces all).
    pub acks: i16,
    /// How the measured phase drives sends.
    pub mode: DriveMode,
    /// Pipelined-send bound ([`DriveMode::Pipelined`] and flush groups).
    pub max_in_flight_sends: usize,
    /// Records between flushes ([`DriveMode::FlushHeavy`]).
    pub flush_every: usize,
    /// Idle seconds ([`DriveMode::Idle`]).
    pub idle_seconds: u64,
    /// Timeout for the whole measured phase.
    pub timeout: Duration,
    /// Base seed; domains derive per-record seeds from it.
    pub seed: u64,
}

impl CellDef {
    /// Total records addressed by this cell.
    #[must_use]
    pub fn total_records(&self) -> usize {
        self.topics.iter().map(|t| t.records).sum()
    }

    /// Broker partitions (max over topics; the broker advertises one
    /// partition count for every topic).
    #[must_use]
    pub fn broker_partitions(&self) -> i32 {
        self.topics.iter().map(|t| t.partitions).max().unwrap_or(6)
    }
}

const fn topic(name: &'static str, partitions: i32, records: usize) -> CellTopic {
    CellTopic {
        name,
        partitions,
        records,
        records_per_partition: records / partitions as usize,
    }
}

/// The eight section-4 producer cells.
///
/// Documented defaults for knobs section 4 leaves open: acks 1 (all for
/// the idempotent cell), crate-default linger/batching, 100-byte values
/// (1 KiB for mixed-topics), 16-byte keys, 6 partitions (128 for 128p),
/// explicit round-robin partition assignment, 256 pipelined sends.
#[must_use]
pub fn producer_cells() -> Vec<CellDef> {
    vec![
        CellDef {
            id: "nb-produce-bulk",
            topics: vec![topic("nb-bulk", 6, 20_000)],
            value_bytes: 100,
            key_bytes: 16,
            headers_each: 0,
            header_kv_bytes: 8,
            entropy: "random",
            idempotent: false,
            acks: 1,
            mode: DriveMode::Pipelined,
            max_in_flight_sends: 256,
            flush_every: 0,
            idle_seconds: 0,
            timeout: Duration::from_secs(60),
            seed: 0x5EED_0001,
        },
        CellDef {
            id: "nb-produce-idem",
            topics: vec![topic("nb-idem", 6, 20_000)],
            value_bytes: 100,
            key_bytes: 16,
            headers_each: 0,
            header_kv_bytes: 8,
            entropy: "random",
            idempotent: true,
            acks: -1,
            mode: DriveMode::Pipelined,
            max_in_flight_sends: 256,
            flush_every: 0,
            idle_seconds: 0,
            timeout: Duration::from_secs(120),
            seed: 0x5EED_0002,
        },
        CellDef {
            id: "nb-produce-mixed-topics",
            topics: vec![
                topic("nb-mix-0", 6, 4_000),
                topic("nb-mix-1", 6, 4_000),
                topic("nb-mix-2", 6, 4_000),
            ],
            value_bytes: 1024,
            key_bytes: 16,
            headers_each: 0,
            header_kv_bytes: 8,
            entropy: "random",
            idempotent: false,
            acks: 1,
            mode: DriveMode::Pipelined,
            max_in_flight_sends: 256,
            flush_every: 0,
            idle_seconds: 0,
            timeout: Duration::from_secs(300),
            seed: 0x5EED_0003,
        },
        CellDef {
            id: "nb-produce-headers",
            topics: vec![topic("nb-headers", 6, 5_000)],
            value_bytes: 100,
            key_bytes: 16,
            headers_each: 3,
            header_kv_bytes: 8,
            entropy: "random",
            idempotent: false,
            acks: 1,
            mode: DriveMode::Pipelined,
            max_in_flight_sends: 256,
            flush_every: 0,
            idle_seconds: 0,
            timeout: Duration::from_secs(60),
            seed: 0x5EED_0004,
        },
        CellDef {
            id: "nb-produce-128p",
            topics: vec![topic("nb-128p", 128, 256_000)],
            value_bytes: 100,
            key_bytes: 16,
            headers_each: 0,
            header_kv_bytes: 8,
            entropy: "random",
            idempotent: false,
            acks: 1,
            mode: DriveMode::Pipelined,
            max_in_flight_sends: 256,
            flush_every: 0,
            idle_seconds: 0,
            timeout: Duration::from_secs(30),
            seed: 0x5EED_0005,
        },
        CellDef {
            id: "nb-produce-flush-heavy",
            topics: vec![topic("nb-flush", 6, 2_000)],
            value_bytes: 100,
            key_bytes: 16,
            headers_each: 0,
            header_kv_bytes: 8,
            entropy: "random",
            idempotent: false,
            acks: 1,
            mode: DriveMode::FlushHeavy,
            max_in_flight_sends: 256,
            flush_every: 10,
            idle_seconds: 0,
            timeout: Duration::from_secs(60),
            seed: 0x5EED_0006,
        },
        CellDef {
            id: "nb-send-seq",
            topics: vec![topic("nb-seq", 6, 1_000)],
            value_bytes: 100,
            key_bytes: 16,
            headers_each: 0,
            header_kv_bytes: 8,
            entropy: "random",
            idempotent: false,
            acks: 1,
            mode: DriveMode::Sequential,
            max_in_flight_sends: 1,
            flush_every: 0,
            idle_seconds: 0,
            timeout: Duration::from_secs(30),
            seed: 0x5EED_0007,
        },
        CellDef {
            id: "nb-produce-idle-rss",
            topics: vec![topic("nb-idle", 6, 0)],
            value_bytes: 100,
            key_bytes: 16,
            headers_each: 0,
            header_kv_bytes: 8,
            entropy: "random",
            idempotent: false,
            acks: 1,
            mode: DriveMode::Idle,
            max_in_flight_sends: 1,
            flush_every: 0,
            idle_seconds: 10,
            timeout: Duration::from_secs(60),
            seed: 0x5EED_0008,
        },
    ]
}

/// One generated record: global 1-based ID plus its wire bytes.
pub struct GenRecord {
    /// Global 1-based record ID (embedded in the value prefix).
    pub id: u64,
    /// Topic index into [`CellDef::topics`].
    pub topic_idx: usize,
    /// Explicit partition (round-robin).
    pub partition: i32,
    /// Record key bytes.
    pub key: Bytes,
    /// Record value bytes (ID prefix + payload).
    pub value: Bytes,
    /// Record headers.
    pub headers: Vec<Header>,
    /// Key + value + header bytes (throughput accounting).
    pub bytes: usize,
}

/// Deterministically generate every record a cell addresses.
///
/// Layout per topic is contiguous IDs in send order; partitions are
/// explicit round-robin so offset runs stay comparable across runs.
#[must_use]
pub fn generate(cell: &CellDef) -> Vec<GenRecord> {
    let mut out = Vec::with_capacity(cell.total_records());
    let mut id: u64 = 0;
    for (topic_idx, topic) in cell.topics.iter().enumerate() {
        for i in 0..topic.records {
            id += 1;
            let dom = (topic_idx as u64) << 56;
            let key = Bytes::from(payload(cell.seed ^ dom ^ id, cell.entropy, cell.key_bytes));
            let mut value = id.to_be_bytes().to_vec();
            value.extend_from_slice(&payload(
                cell.seed ^ dom ^ id ^ 0x9E37_79B9_7F4A_7C15,
                cell.entropy,
                cell.value_bytes.saturating_sub(8),
            ));
            let mut headers = Vec::with_capacity(cell.headers_each);
            for h in 0..cell.headers_each {
                headers.push(Header::new(
                    format!("h{h}"),
                    Bytes::from(payload(
                        cell.seed ^ dom ^ id ^ ((h as u64) << 32),
                        "text",
                        cell.header_kv_bytes,
                    )),
                ));
            }
            let bytes = key.len()
                + value.len()
                + headers
                    .iter()
                    .map(|h| h.key.len() + h.value.as_ref().map_or(0, Bytes::len))
                    .sum::<usize>();
            out.push(GenRecord {
                id,
                topic_idx,
                partition: (i % topic.partitions.max(1) as usize) as i32,
                key,
                value: Bytes::from(value),
                headers,
                bytes,
            });
        }
    }
    out
}

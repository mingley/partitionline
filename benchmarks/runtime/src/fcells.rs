//! Fetch-cell definitions (KL09-10): the seven null-broker
//! consumer cells from `docs/plan/performance-leadership.md` section 4,
//! plus the documented defaults for every knob section 4 leaves open.
//!
//! The broker serves the KL09-07 synthetic log; every delivered
//! record carries its offset ID and `record_hash` in the value
//! prefix, so the drive verifies each ID/hash pair client-side.

use std::time::Duration;

use partitionline::consumer::FetchedRecord;

use nullbroker::synth::record_hash;

/// A consumer measurement cell.
#[derive(Debug, Clone)]
pub struct FetchCellDef {
    /// Cell ID (`nb-fetch-bulk`, ...).
    pub id: &'static str,
    /// Topic to assign (every broker topic serves the synth log).
    pub topic: &'static str,
    /// Broker partitions (all assigned unless paused).
    pub partitions: i32,
    /// Synth seed (IDs, hashes, filler).
    pub synth_seed: u64,
    /// Synth log size per partition.
    pub synth_records_per_partition: u64,
    /// Synth records per batch.
    pub synth_records_per_batch: u32,
    /// Synth value bytes per record.
    pub synth_payload_bytes: usize,
    /// Every Nth synth batch (1-based) is aborted; 0 disables.
    pub synth_abort_every: u64,
    /// `read_committed` isolation (else `read_uncommitted`).
    pub read_committed: bool,
    /// `max_poll_records` override (`None` = crate default).
    pub max_poll_records: Option<usize>,
    /// Verified records to fetch before stopping.
    pub target_records: u64,
    /// Partitions to pause (must deliver nothing).
    pub paused_partitions: Vec<i32>,
    /// Seek offset applied to partition 0 after assignment
    /// (`None` = fetch from 0).
    pub seek_offset: Option<i64>,
    /// Application sleep after every returned batch.
    pub app_delay_per_batch: Duration,
    /// Broker nodes (1 = single node).
    pub nodes: u16,
    /// Slow node id, if any.
    pub slow_node: Option<i32>,
    /// Slow-node fixed response delay.
    pub slow_delay: Duration,
    /// Timeout for the whole measured phase.
    pub timeout: Duration,
    /// Base seed for repetition seeds.
    pub seed: u64,
}

/// The seven section-4 fetch cells.
///
/// Documented defaults for knobs section 4 leaves open: 20k
/// delivered records per bulk-family cell (10k for 1000p/seek,
/// 2k polls for capped-paused), 500-record synth batches (1000 for
/// seek-in-batch), uncompressed synth log, 5s fetch waits.
#[must_use]
pub fn fetch_cells() -> Vec<FetchCellDef> {
    vec![
        FetchCellDef {
            id: "nb-fetch-bulk",
            topic: "nb-fetch",
            partitions: 6,
            synth_seed: 0xFE7C_0001,
            synth_records_per_partition: 100_000,
            synth_records_per_batch: 500,
            synth_payload_bytes: 100,
            synth_abort_every: 0,
            read_committed: false,
            max_poll_records: None,
            target_records: 20_000,
            paused_partitions: Vec::new(),
            seek_offset: None,
            app_delay_per_batch: Duration::ZERO,
            nodes: 1,
            slow_node: None,
            slow_delay: Duration::ZERO,
            timeout: Duration::from_secs(120),
            seed: 0xFE7C_0001,
        },
        FetchCellDef {
            id: "nb-fetch-1000p",
            topic: "nb-fetch-1k",
            partitions: 1000,
            synth_seed: 0xFE7C_0002,
            synth_records_per_partition: 10,
            synth_records_per_batch: 500,
            synth_payload_bytes: 100,
            synth_abort_every: 0,
            read_committed: false,
            max_poll_records: None,
            target_records: 10_000,
            paused_partitions: Vec::new(),
            seek_offset: None,
            app_delay_per_batch: Duration::ZERO,
            nodes: 1,
            slow_node: None,
            slow_delay: Duration::ZERO,
            timeout: Duration::from_secs(180),
            seed: 0xFE7C_0002,
        },
        FetchCellDef {
            id: "nb-fetch-committed-aborts",
            topic: "nb-fetch-txn",
            partitions: 6,
            synth_seed: 0xFE7C_0003,
            synth_records_per_partition: 50_000,
            synth_records_per_batch: 500,
            synth_payload_bytes: 100,
            synth_abort_every: 5,
            read_committed: true,
            max_poll_records: None,
            target_records: 20_000,
            paused_partitions: Vec::new(),
            seek_offset: None,
            app_delay_per_batch: Duration::ZERO,
            nodes: 1,
            slow_node: None,
            slow_delay: Duration::ZERO,
            timeout: Duration::from_secs(180),
            seed: 0xFE7C_0003,
        },
        FetchCellDef {
            id: "nb-fetch-seek-in-batch",
            topic: "nb-fetch-seek",
            partitions: 1,
            synth_seed: 0xFE7C_0004,
            synth_records_per_partition: 100_000,
            synth_records_per_batch: 1000,
            synth_payload_bytes: 100,
            synth_abort_every: 0,
            read_committed: false,
            max_poll_records: None,
            target_records: 10_000,
            paused_partitions: Vec::new(),
            seek_offset: Some(500),
            app_delay_per_batch: Duration::ZERO,
            nodes: 1,
            slow_node: None,
            slow_delay: Duration::ZERO,
            timeout: Duration::from_secs(120),
            seed: 0xFE7C_0004,
        },
        FetchCellDef {
            id: "nb-fetch-capped-paused",
            topic: "nb-fetch-cap",
            partitions: 6,
            synth_seed: 0xFE7C_0005,
            synth_records_per_partition: 20_000,
            synth_records_per_batch: 500,
            synth_payload_bytes: 100,
            synth_abort_every: 0,
            read_committed: false,
            max_poll_records: Some(1),
            target_records: 2_000,
            paused_partitions: vec![1, 2, 3, 4, 5],
            seek_offset: None,
            app_delay_per_batch: Duration::ZERO,
            nodes: 1,
            slow_node: None,
            slow_delay: Duration::ZERO,
            timeout: Duration::from_secs(180),
            seed: 0xFE7C_0005,
        },
        FetchCellDef {
            id: "nb-fetch-appdelay",
            topic: "nb-fetch-app",
            partitions: 6,
            synth_seed: 0xFE7C_0006,
            synth_records_per_partition: 100_000,
            synth_records_per_batch: 500,
            synth_payload_bytes: 100,
            synth_abort_every: 0,
            read_committed: false,
            max_poll_records: None,
            target_records: 20_000,
            paused_partitions: Vec::new(),
            seek_offset: None,
            app_delay_per_batch: Duration::from_millis(1),
            nodes: 1,
            slow_node: None,
            slow_delay: Duration::ZERO,
            timeout: Duration::from_secs(300),
            seed: 0xFE7C_0006,
        },
        FetchCellDef {
            id: "nb-fetch-multinode",
            topic: "nb-fetch-multi",
            partitions: 6,
            synth_seed: 0xFE7C_0007,
            synth_records_per_partition: 100_000,
            synth_records_per_batch: 500,
            synth_payload_bytes: 100,
            synth_abort_every: 0,
            read_committed: false,
            max_poll_records: None,
            target_records: 20_000,
            paused_partitions: Vec::new(),
            seek_offset: None,
            app_delay_per_batch: Duration::ZERO,
            nodes: 3,
            slow_node: Some(2),
            slow_delay: Duration::from_millis(50),
            timeout: Duration::from_secs(300),
            seed: 0xFE7C_0007,
        },
    ]
}

/// Verify one delivered record against the synth layout: value is
/// `offset:u64be + record_hash:u64be + filler` at exactly
/// `payload_bytes`, key is `partition:i32be + offset:u64be +
/// hash-low32`, and the value ID matches the consumer offset.
pub fn verify_record(seed: u64, payload_bytes: usize, rec: &FetchedRecord) -> Result<(), String> {
    let offset =
        u64::try_from(rec.offset).map_err(|_| format!("negative offset {}", rec.offset))?;
    let value = rec
        .value
        .as_ref()
        .ok_or_else(|| format!("p{} o{offset}: missing value", rec.partition))?;
    if value.len() != payload_bytes {
        return Err(format!(
            "p{} o{offset}: value len {} != {payload_bytes}",
            rec.partition,
            value.len()
        ));
    }
    if payload_bytes < 16 {
        return Err(format!(
            "p{} o{offset}: payload {payload_bytes} < 16",
            rec.partition
        ));
    }
    let id = u64::from_be_bytes(value[0..8].try_into().unwrap_or([0u8; 8]));
    if id != offset {
        return Err(format!(
            "p{} o{offset}: value ID {id} != offset",
            rec.partition
        ));
    }
    let hash = u64::from_be_bytes(value[8..16].try_into().unwrap_or([0u8; 8]));
    let expected = record_hash(seed, rec.partition, offset);
    if hash != expected {
        return Err(format!("p{} o{offset}: hash mismatch", rec.partition));
    }
    let key = rec
        .key
        .as_ref()
        .ok_or_else(|| format!("p{} o{offset}: missing key", rec.partition))?;
    if key.len() != 16 {
        return Err(format!(
            "p{} o{offset}: key len {} != 16",
            rec.partition,
            key.len()
        ));
    }
    let key_part = i32::from_be_bytes(key[0..4].try_into().unwrap_or([0u8; 4]));
    let key_off = u64::from_be_bytes(key[4..12].try_into().unwrap_or([0u8; 8]));
    let key_frag = u32::from_be_bytes(key[12..16].try_into().unwrap_or([0u8; 4]));
    if key_part != rec.partition || key_off != offset || key_frag != expected as u32 {
        return Err(format!("p{} o{offset}: key mismatch", rec.partition));
    }
    Ok(())
}

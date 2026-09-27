//! Measured-phase drivers (KL09-09): how each [`CellDef`] pushes
//! records through the client.
//!
//! Every mode records per-record send latency (send call to metadata)
//! in microseconds. `FlushHeavy` additionally reports total flush
//! time, which is outside per-record latency by definition.

use std::sync::Arc;
use std::time::{Duration, Instant};

use partitionline::producer::{ProduceRecord, Producer};

use crate::cells::{CellDef, DriveMode, GenRecord};

/// Outcome of one measured drive.
#[derive(Debug, Default)]
pub struct DriveOutcome {
    /// Per-record send latency, microseconds, in completion order.
    pub latencies_us: Vec<u64>,
    /// Records acknowledged (metadata received without error).
    pub acked: u64,
    /// Acknowledged records with a valid (non-negative) offset.
    pub offsets_valid: u64,
    /// Send failures, newest last (capped).
    pub errors: Vec<String>,
    /// Key + value + header bytes offered.
    pub bytes_offered: u64,
    /// Total `flush()` time, microseconds ([`DriveMode::FlushHeavy`]).
    pub flush_us_total: u64,
    /// The drive hit the cell timeout.
    pub timed_out: bool,
}

/// Max retained send-error strings per run.
const MAX_ERRORS: usize = 32;

fn to_produce_record(cell: &CellDef, rec: &GenRecord) -> ProduceRecord {
    let mut out = ProduceRecord::to(cell.topics[rec.topic_idx].name);
    out.partition = Some(rec.partition);
    out.key = Some(rec.key.clone());
    out.value = Some(rec.value.clone());
    out.headers = rec.headers.clone();
    out
}

fn record_error(out: &mut DriveOutcome, err: String) {
    if out.errors.len() < MAX_ERRORS {
        out.errors.push(err);
    }
}

fn account_metadata(out: &mut DriveOutcome, meta: &partitionline::producer::RecordMetadata) {
    out.acked += 1;
    if meta.offset >= 0 {
        out.offsets_valid += 1;
    }
}

/// Drive `records` through `producer` per the cell's mode.
pub async fn drive_cell(
    producer: &Arc<Producer>,
    cell: &CellDef,
    records: &[GenRecord],
) -> DriveOutcome {
    let mut out = DriveOutcome {
        bytes_offered: records.iter().map(|r| r.bytes as u64).sum(),
        ..DriveOutcome::default()
    };
    out.latencies_us.reserve(records.len());
    let result = tokio::time::timeout(cell.timeout, async {
        match cell.mode {
            DriveMode::Pipelined => drive_pipelined(producer, cell, records, &mut out).await,
            DriveMode::Sequential => drive_sequential(producer, cell, records, &mut out).await,
            DriveMode::FlushHeavy => drive_flush_heavy(producer, cell, records, &mut out).await,
            DriveMode::Idle => {
                tokio::time::sleep(Duration::from_secs(cell.idle_seconds)).await;
            }
        }
    })
    .await;
    if result.is_err() {
        out.timed_out = true;
    }
    out
}

async fn drive_pipelined(
    producer: &Arc<Producer>,
    cell: &CellDef,
    records: &[GenRecord],
    out: &mut DriveOutcome,
) {
    let bound = cell.max_in_flight_sends.max(1);
    let mut set = tokio::task::JoinSet::new();
    for rec in records {
        let req = to_produce_record(cell, rec);
        while set.len() >= bound {
            join_one(&mut set, out).await;
        }
        let client = Arc::clone(producer);
        set.spawn(async move {
            let start = Instant::now();
            let meta = client.send(req).await;
            (start.elapsed(), meta)
        });
    }
    while !set.is_empty() {
        join_one(&mut set, out).await;
    }
}

async fn drive_sequential(
    producer: &Arc<Producer>,
    cell: &CellDef,
    records: &[GenRecord],
    out: &mut DriveOutcome,
) {
    for rec in records {
        let req = to_produce_record(cell, rec);
        let start = Instant::now();
        match producer.send(req).await {
            Ok(meta) => {
                out.latencies_us
                    .push(start.elapsed().as_micros().min(u128::from(u64::MAX)) as u64);
                account_metadata(out, &meta);
            }
            Err(e) => record_error(out, format!("send: {e}")),
        }
    }
}

async fn drive_flush_heavy(
    producer: &Arc<Producer>,
    cell: &CellDef,
    records: &[GenRecord],
    out: &mut DriveOutcome,
) {
    let group = cell.flush_every.max(1);
    for chunk in records.chunks(group) {
        let mut set = tokio::task::JoinSet::new();
        for rec in chunk {
            let req = to_produce_record(cell, rec);
            let client = Arc::clone(producer);
            set.spawn(async move {
                let start = Instant::now();
                let meta = client.send(req).await;
                (start.elapsed(), meta)
            });
        }
        while !set.is_empty() {
            join_one(&mut set, out).await;
        }
        let start = Instant::now();
        if let Err(e) = producer.flush().await {
            record_error(out, format!("flush: {e}"));
        }
        out.flush_us_total = out
            .flush_us_total
            .saturating_add(start.elapsed().as_micros().min(u128::from(u64::MAX)) as u64);
    }
}

async fn join_one(
    set: &mut tokio::task::JoinSet<(
        Duration,
        Result<partitionline::producer::RecordMetadata, partitionline::Error>,
    )>,
    out: &mut DriveOutcome,
) {
    match set.join_next().await {
        Some(Ok((elapsed, Ok(meta)))) => {
            out.latencies_us
                .push(elapsed.as_micros().min(u128::from(u64::MAX)) as u64);
            account_metadata(out, &meta);
        }
        Some(Ok((_, Err(e)))) => record_error(out, format!("send: {e}")),
        Some(Err(e)) => record_error(out, format!("join: {e}")),
        None => {}
    }
}

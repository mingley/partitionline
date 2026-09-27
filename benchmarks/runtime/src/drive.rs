//! Measured-phase drivers (KL09-09): how each [`CellDef`] pushes
//! records through the client.
//!
//! [`DriveMode::Pipelined`] and [`DriveMode::FlushHeavy`] are
//! `try_send`-driven per section 4: enqueue stamps are taken per
//! record and latencies resolve against the flush that delivers
//! them (the offer-to-ack bound). Only [`DriveMode::Sequential`]
//! observes per-record offsets; the `try_send` modes reconcile
//! against broker-accepted counts instead (see `artifact.rs`).
//!
//! The caller pre-reserves [`DriveOutcome::latencies_us`] and the
//! stamp buffer before the census starts. Records are taken by
//! value and their headers moved (never cloned) into the API type,
//! so the common path adds no harness allocations; a `QueueFull`
//! retry regenerates the one record deterministically (counted in
//! [`DriveOutcome::queue_full_retries`]).

use std::sync::Arc;
use std::time::{Duration, Instant};

use partitionline::producer::{ProduceRecord, Producer};

use crate::cells::{gen_one, CellDef, DriveMode, GenRecord};

/// Outcome of one measured drive.
#[derive(Debug, Default)]
pub struct DriveOutcome {
    /// Per-record latency, microseconds, in record order for
    /// `try_send` modes and completion order for sequential.
    pub latencies_us: Vec<u64>,
    /// Records acknowledged (metadata received, or offered before a
    /// successful final flush for `try_send` modes).
    pub acked: u64,
    /// Acknowledged records with a valid (non-negative) offset.
    /// Only meaningful when [`Self::offsets_observed`].
    pub offsets_valid: u64,
    /// Whether per-record offsets were observed (send-based modes).
    pub offsets_observed: bool,
    /// `QueueFull` retries (each regenerates one record).
    pub queue_full_retries: u64,
    /// Send failures, newest last (capped).
    pub errors: Vec<String>,
    /// Key + value + header bytes offered.
    pub bytes_offered: u64,
    /// Total `flush()` time, microseconds.
    pub flush_us_total: u64,
    /// The drive hit the cell timeout.
    pub timed_out: bool,
}

/// Max retained send-error strings per run.
const MAX_ERRORS: usize = 32;

/// Build the API record, moving headers out of `rec` (no clone).
fn to_produce_record(cell: &CellDef, rec: &mut GenRecord) -> ProduceRecord {
    let mut out = ProduceRecord::to(cell.topics[rec.topic_idx].name);
    out.partition = Some(rec.partition);
    out.key = Some(std::mem::take(&mut rec.key));
    out.value = Some(std::mem::take(&mut rec.value));
    out.headers = std::mem::take(&mut rec.headers);
    out
}

fn record_error(out: &mut DriveOutcome, err: String) {
    if out.errors.len() < MAX_ERRORS {
        out.errors.push(err);
    }
}

fn micros(d: Duration) -> u64 {
    d.as_micros().min(u128::from(u64::MAX)) as u64
}

/// Drive `records` through `producer` per the cell's mode.
///
/// `out.latencies_us` and `stamps` must be pre-reserved for
/// `records.len()` entries; the drive itself allocates nothing in
/// the common path.
pub async fn drive_cell(
    producer: &Arc<Producer>,
    cell: &CellDef,
    records: &mut [GenRecord],
    out: &mut DriveOutcome,
    stamps: &mut Vec<Instant>,
) {
    out.bytes_offered = records.iter().map(|r| r.bytes as u64).sum();
    let result = tokio::time::timeout(cell.timeout, async {
        match cell.mode {
            DriveMode::Pipelined => drive_pipelined(producer, records, cell, out, stamps).await,
            DriveMode::Sequential => drive_sequential(producer, records, cell, out).await,
            DriveMode::FlushHeavy => drive_flush_heavy(producer, records, cell, out, stamps).await,
            DriveMode::Idle => {
                tokio::time::sleep(Duration::from_secs(cell.idle_seconds)).await;
            }
        }
    })
    .await;
    if result.is_err() {
        out.timed_out = true;
    }
}

/// Enqueue one record, flushing on `QueueFull` to make room. The
/// first attempt moves the record's buffers; each retry regenerates
/// the record deterministically (the failed attempt's buffers were
/// dropped by `try_send`). Returns the enqueue stamp on success.
async fn try_send_one(
    producer: &Arc<Producer>,
    cell: &CellDef,
    rec: &mut GenRecord,
    out: &mut DriveOutcome,
) -> Option<Instant> {
    let mut first = true;
    loop {
        let req = if first {
            first = false;
            to_produce_record(cell, rec)
        } else {
            let regen = gen_one(cell, rec.topic_idx, rec.index, rec.id);
            to_produce_record_owned(cell, regen)
        };
        match producer.try_send(req) {
            Ok(()) => return Some(Instant::now()),
            Err(partitionline::Error::QueueFull) => {
                out.queue_full_retries += 1;
                // Yield before flushing: on a single-threaded runtime
                // a `try_send`/`flush` pair that completes
                // synchronously never gives the scheduler a chance to
                // advance metadata, deadlocking at 100% CPU.
                tokio::task::yield_now().await;
                tokio::time::sleep(Duration::from_millis(1)).await;
                let start = Instant::now();
                if let Err(e) = producer.flush().await {
                    record_error(out, format!("flush-on-full: {e}"));
                    return None;
                }
                out.flush_us_total = out.flush_us_total.saturating_add(micros(start.elapsed()));
            }
            Err(e) => {
                record_error(out, format!("try_send: {e}"));
                return None;
            }
        }
    }
}

fn to_produce_record_owned(cell: &CellDef, mut rec: GenRecord) -> ProduceRecord {
    to_produce_record(cell, &mut rec)
}

async fn drive_pipelined(
    producer: &Arc<Producer>,
    records: &mut [GenRecord],
    cell: &CellDef,
    out: &mut DriveOutcome,
    stamps: &mut Vec<Instant>,
) {
    let total = records.len() as u64;
    for rec in records.iter_mut() {
        match try_send_one(producer, cell, rec, out).await {
            Some(stamp) => stamps.push(stamp),
            None => return,
        }
    }
    let start = Instant::now();
    match producer.flush().await {
        Ok(()) => {
            let end = Instant::now();
            out.flush_us_total = out.flush_us_total.saturating_add(micros(end - start));
            out.acked = total;
            out.latencies_us.extend(
                stamps
                    .iter()
                    .map(|s| micros(end.saturating_duration_since(*s))),
            );
        }
        Err(e) => record_error(out, format!("flush: {e}")),
    }
}

async fn drive_sequential(
    producer: &Arc<Producer>,
    records: &mut [GenRecord],
    cell: &CellDef,
    out: &mut DriveOutcome,
) {
    out.offsets_observed = true;
    for rec in records.iter_mut() {
        let req = to_produce_record(cell, rec);
        let start = Instant::now();
        match producer.send(req).await {
            Ok(meta) => {
                out.latencies_us.push(micros(start.elapsed()));
                out.acked += 1;
                if meta.offset >= 0 {
                    out.offsets_valid += 1;
                }
            }
            Err(e) => record_error(out, format!("send: {e}")),
        }
    }
}

async fn drive_flush_heavy(
    producer: &Arc<Producer>,
    records: &mut [GenRecord],
    cell: &CellDef,
    out: &mut DriveOutcome,
    stamps: &mut Vec<Instant>,
) {
    let group = cell.flush_every.max(1);
    for chunk in records.chunks_mut(group) {
        stamps.clear();
        let mut group_ok = true;
        for rec in chunk.iter_mut() {
            match try_send_one(producer, cell, rec, out).await {
                Some(stamp) => stamps.push(stamp),
                None => {
                    group_ok = false;
                    break;
                }
            }
        }
        if !group_ok {
            return;
        }
        let start = Instant::now();
        match producer.flush().await {
            Ok(()) => {
                let end = Instant::now();
                out.flush_us_total = out.flush_us_total.saturating_add(micros(end - start));
                out.acked += chunk.len() as u64;
                out.latencies_us.extend(
                    stamps
                        .iter()
                        .map(|s| micros(end.saturating_duration_since(*s))),
                );
            }
            Err(e) => {
                record_error(out, format!("flush: {e}"));
                return;
            }
        }
    }
}

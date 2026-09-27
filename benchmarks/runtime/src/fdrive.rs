//! Fetch drivers (KL09-10): assign, fetch until the target, verify
//! every delivered ID/hash.
//!
//! Per-record latency is the enclosing `fetch()` call's duration, an
//! honest upper bound on delivery latency (every record in the batch
//! was delivered no later than the call's end). Empty fetch rounds
//! count toward [`FetchOutcome::rounds`] but carry no latency.

use std::collections::BTreeMap;
use std::time::{Duration, Instant};

use partitionline::consumer::{Consumer, ConsumerConfig};
use partitionline::{IsolationLevel, TopicPartition};

use crate::fcells::{verify_record, FetchCellDef};

/// Outcome of one measured fetch drive.
#[derive(Debug, Default)]
pub struct FetchOutcome {
    /// Per-record latency bound, microseconds, in delivery order.
    pub latencies_us: Vec<u64>,
    /// Records verified (ID + hash + key).
    pub verified: u64,
    /// Records failing verification.
    pub mismatched: u64,
    /// `fetch()` rounds executed.
    pub rounds: u64,
    /// Rounds returning zero records.
    pub empty_rounds: u64,
    /// Delivered records per partition.
    pub per_partition: BTreeMap<i32, u64>,
    /// Key + value bytes delivered.
    pub bytes_delivered: u64,
    /// Verification/drive failures, newest last (capped).
    pub errors: Vec<String>,
    /// Paused partitions that delivered records (must stay empty).
    pub paused_delivered: BTreeMap<i32, u64>,
    /// The drive hit the cell timeout.
    pub timed_out: bool,
}

/// Max retained error strings per run.
const MAX_ERRORS: usize = 32;

/// Per-fetch wait: long enough to ride out the slow node, short
/// enough to notice log end promptly.
const FETCH_WAIT: Duration = Duration::from_secs(5);

/// Consecutive empty rounds that mean log end.
const LOG_END_EMPTIES: u64 = 3;

fn record_error(out: &mut FetchOutcome, err: String) {
    if out.errors.len() < MAX_ERRORS {
        out.errors.push(err);
    }
}

fn micros(d: Duration) -> u64 {
    d.as_micros().min(u128::from(u64::MAX)) as u64
}

/// Build the consumer for `cell` over `bootstrap` endpoints.
pub fn consumer_config(cell: &FetchCellDef, bootstrap: &[String]) -> ConsumerConfig {
    let cfg = ConsumerConfig::bootstrap(bootstrap.iter().cloned())
        .client_id(format!("runtime-{}", cell.id))
        .isolation(if cell.read_committed {
            IsolationLevel::ReadCommitted
        } else {
            IsolationLevel::ReadUncommitted
        });
    match cell.max_poll_records {
        Some(n) => cfg.max_poll_records(n),
        None => cfg,
    }
}

/// Drive `cell`: assign, seek/pause as configured, fetch until
/// `target_records` verify. `out.latencies_us` must be pre-reserved.
pub async fn drive_fetch(consumer: &mut Consumer, cell: &FetchCellDef, out: &mut FetchOutcome) {
    let result = tokio::time::timeout(cell.timeout, async {
        let starts: Vec<(TopicPartition, i64)> = (0..cell.partitions)
            .map(|p| (TopicPartition::new(cell.topic, p), 0))
            .collect();
        if let Err(e) = consumer.assign_many(starts).await {
            record_error(out, format!("assign: {e}"));
            return;
        }
        if let Some(offset) = cell.seek_offset {
            if let Err(e) = consumer.seek(cell.topic, 0, offset) {
                record_error(out, format!("seek: {e}"));
                return;
            }
        }
        if !cell.paused_partitions.is_empty() {
            let paused: Vec<TopicPartition> = cell
                .paused_partitions
                .iter()
                .map(|p| TopicPartition::new(cell.topic, *p))
                .collect();
            consumer.pause(paused);
        }
        let mut empties = 0u64;
        while out.verified < cell.target_records {
            let start = Instant::now();
            let batch = match consumer.fetch_timeout(FETCH_WAIT).await {
                Ok(batch) => batch,
                Err(e) => {
                    record_error(out, format!("fetch: {e}"));
                    return;
                }
            };
            let elapsed_us = micros(start.elapsed());
            out.rounds += 1;
            if batch.is_empty() {
                out.empty_rounds += 1;
                empties += 1;
                if empties >= LOG_END_EMPTIES {
                    record_error(
                        out,
                        format!(
                            "log end after {} verified of {}",
                            out.verified, cell.target_records
                        ),
                    );
                    return;
                }
                continue;
            }
            empties = 0;
            for rec in batch.iter() {
                *out.per_partition.entry(rec.partition).or_default() += 1;
                if cell.paused_partitions.contains(&rec.partition) {
                    *out.paused_delivered.entry(rec.partition).or_default() += 1;
                }
                match verify_record(cell.synth_seed, cell.synth_payload_bytes, rec) {
                    Ok(()) => {
                        out.verified += 1;
                        out.latencies_us.push(elapsed_us);
                        out.bytes_delivered += rec.key.as_ref().map_or(0, |k| k.len() as u64)
                            + rec.value.as_ref().map_or(0, |v| v.len() as u64);
                        if out.verified >= cell.target_records {
                            break;
                        }
                    }
                    Err(e) => {
                        out.mismatched += 1;
                        record_error(out, e);
                    }
                }
            }
            if !cell.app_delay_per_batch.is_zero() {
                tokio::time::sleep(cell.app_delay_per_batch).await;
            }
        }
    })
    .await;
    if result.is_err() {
        out.timed_out = true;
    }
}

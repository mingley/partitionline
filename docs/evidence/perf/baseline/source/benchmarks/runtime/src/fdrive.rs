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
    /// Actual synthetic records retained in the five paused queues after prefill.
    pub paused_backlog_records: u64,
    /// Buffered key/value/header bytes immediately after the prefill poll.
    pub prefill_buffered_bytes: usize,
    /// Records returned by the consumer, including the unverified tail past the target.
    pub returned_records: u64,
    /// Exact verified committed history: partition, inclusive start, exclusive end.
    pub committed_history: Vec<(i32, i64, i64)>,
    /// Aborted offsets skipped between adjacent verified committed records.
    pub committed_abort_gap_records: u64,
    /// Aborted offsets unexpectedly delivered by the committed cell.
    pub committed_aborted_deliveries: u64,
    /// Fetch cursor for every assigned partition after the committed response.
    pub committed_partition_cursors: Vec<(i32, i64)>,
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

fn committed_expected_offset(cell: &FetchCellDef, ordinal: u64) -> Result<u64, String> {
    let batch = u64::from(cell.synth_records_per_batch);
    let committed_group = batch.saturating_mul(cell.synth_abort_every.saturating_sub(1));
    if committed_group == 0 {
        return Err("committed history requires a nonempty committed batch group".to_owned());
    }
    Ok(ordinal.saturating_add((ordinal / committed_group).saturating_mul(batch)))
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
    let cfg = match cell.max_poll_records {
        Some(n) => cfg.max_poll_records(n),
        None => cfg,
    };
    let cfg = match cell.buffer_memory {
        Some(n) => cfg.buffer_memory(n),
        None => cfg,
    };
    let cfg = match cell.max_partition_fetch_bytes {
        Some(n) => cfg.max_partition_fetch_bytes(n),
        None => cfg,
    };
    match cell.max_bytes {
        Some(n) => cfg.max_bytes(n),
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
        let pause_after_prefill = cell.id == "nb-fetch-capped-paused";
        if !pause_after_prefill && !cell.paused_partitions.is_empty() {
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
            out.returned_records += batch.len() as u64;
            for rec in batch.iter() {
                let delivered = out.per_partition.entry(rec.partition).or_default();
                let ordinal = *delivered;
                *delivered += 1;
                if cell.id == "nb-fetch-committed-aborts" {
                    let expected = match committed_expected_offset(cell, ordinal) {
                        Ok(expected) => expected,
                        Err(e) => {
                            record_error(out, e);
                            return;
                        }
                    };
                    if u64::try_from(rec.offset).ok() != Some(expected) {
                        let batch_index = u64::try_from(rec.offset)
                            .ok()
                            .map(|offset| offset / u64::from(cell.synth_records_per_batch));
                        if batch_index
                            .is_some_and(|index| (index + 1) % cell.synth_abort_every == 0)
                        {
                            out.committed_aborted_deliveries += 1;
                        }
                        out.mismatched += 1;
                        record_error(
                            out,
                            format!(
                                "committed history p{}: offset {} != expected {expected}",
                                rec.partition, rec.offset
                            ),
                        );
                        return;
                    }
                    let committed_group =
                        u64::from(cell.synth_records_per_batch) * (cell.synth_abort_every - 1);
                    if ordinal > 0 && ordinal % committed_group == 0 {
                        out.committed_abort_gap_records += u64::from(cell.synth_records_per_batch);
                    }
                    match out.committed_history.last_mut() {
                        Some((partition, _, end))
                            if *partition == rec.partition && *end == rec.offset =>
                        {
                            *end = rec.offset + 1;
                        }
                        _ => {
                            out.committed_history
                                .push((rec.partition, rec.offset, rec.offset + 1))
                        }
                    }
                }
                if cell.paused_partitions.contains(&rec.partition) {
                    *out.paused_delivered.entry(rec.partition).or_default() += 1;
                }
                match verify_record(cell.synth_seed, cell.synth_payload_bytes, rec) {
                    Ok(()) => {
                        out.verified += 1;
                        out.latencies_us.push(elapsed_us);
                        out.bytes_delivered += rec.key.as_ref().map_or(0, |k| k.len() as u64)
                            + rec.value.as_ref().map_or(0, |v| v.len() as u64);
                        if out.verified >= cell.target_records
                            && cell.id != "nb-fetch-committed-aborts"
                        {
                            break;
                        }
                    }
                    Err(e) => {
                        out.mismatched += 1;
                        record_error(out, e);
                    }
                }
            }
            if cell.id == "nb-fetch-committed-aborts" {
                let batch_records = u64::from(cell.synth_records_per_batch);
                let group_records = batch_records * cell.synth_abort_every;
                let committed_group = group_records - batch_records;
                for partition in 0..cell.partitions {
                    let cursor = match consumer.fetch_cursor(cell.topic, partition) {
                        Ok(cursor) if cursor >= 0 => cursor,
                        result => {
                            record_error(out, format!("committed history cursor: {result:?}"));
                            return;
                        }
                    };
                    let end = cursor as u64;
                    let expected_count = (end / group_records) * committed_group
                        + (end % group_records).min(committed_group);
                    if out.per_partition.get(&partition).copied().unwrap_or(0) != expected_count {
                        out.mismatched += 1;
                        record_error(
                            out,
                            format!(
                                "incomplete committed history p{partition} at cursor {cursor}: expected {expected_count} verified records"
                            ),
                        );
                        return;
                    }
                    out.committed_partition_cursors.push((partition, cursor));
                }
            }
            if pause_after_prefill && out.rounds == 1 {
                // Fetch all six sparse logs before pausing: the named cell
                // requires 100k client-held paused records, not just a single
                // active partition with the other five excluded from Fetch.
                let mut backlog = 0u64;
                for &partition in &cell.paused_partitions {
                    let held = consumer
                        .fetch_cursor(cell.topic, partition)
                        .and_then(|cursor| {
                            consumer
                                .position(cell.topic, partition)
                                .map(|position| cursor.saturating_sub(position))
                        });
                    match held {
                        Ok(records) if records >= 0 => backlog += records as u64,
                        Ok(_) => {
                            record_error(out, "negative paused prefill backlog".to_owned());
                            return;
                        }
                        Err(e) => {
                            record_error(out, format!("paused prefill position: {e}"));
                            return;
                        }
                    }
                }
                out.paused_backlog_records = backlog;
                out.prefill_buffered_bytes = consumer.buffered_bytes();
                let expected = cell
                    .synth_records_per_partition
                    .saturating_mul(cell.paused_partitions.len() as u64);
                if backlog != expected || out.prefill_buffered_bytes == 0 {
                    record_error(
                        out,
                        format!("paused prefill backlog {backlog}, expected {expected}"),
                    );
                    return;
                }
                consumer.pause(
                    cell.paused_partitions
                        .iter()
                        .map(|p| TopicPartition::new(cell.topic, *p)),
                );
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

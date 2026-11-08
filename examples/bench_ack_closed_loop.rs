//! Closed-loop, one-record-at-a-time acknowledged-produce diagnostic.
//!
//! The raw record receipt file must be independently audited before use.
//! This intentionally does not implement the paired-matrix result format.

use std::error::Error;
use std::fs::OpenOptions;
use std::io::{BufWriter, Write};
use std::sync::Arc;
use std::time::{Duration, Instant};

use partitionline::protocol::records::Records;
use partitionline::{Compression, ProduceRecord, Producer, ProducerConfig};

const SEED: u64 = 0x5eed_0001;
const GOLDEN: u64 = 0x9e37_79b9_7f4a_7c15;

fn mix(mut value: u64) -> u64 {
    value = value.wrapping_add(GOLDEN);
    value = (value ^ (value >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    value = (value ^ (value >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    value ^ (value >> 31)
}

fn record(seed: u64, id: u64, bytes: usize) -> ([u8; 16], Vec<u8>) {
    let mut key = [0; 16];
    for (slot, byte) in key.iter_mut().zip(
        id.to_be_bytes()
            .into_iter()
            .chain(mix(seed ^ id).to_be_bytes()),
    ) {
        *slot = byte;
    }
    let mut payload = Vec::with_capacity(bytes);
    let mut state = seed ^ id.wrapping_mul(GOLDEN);
    while payload.len() < bytes {
        state = mix(state);
        let remaining = bytes - payload.len();
        payload.extend(state.to_be_bytes().into_iter().take(remaining.min(8)));
    }
    (key, payload)
}

fn positive(name: &str, default: u64, maximum: u64) -> Result<u64, Box<dyn Error>> {
    let value = match std::env::var(name) {
        Ok(value) => value.parse::<u64>()?,
        Err(std::env::VarError::NotPresent) => default,
        Err(error) => return Err(error.into()),
    };
    if value == 0 || value > maximum {
        return Err(format!("{name} must be in 1..={maximum}").into());
    }
    Ok(value)
}

async fn phase(
    producer: &Producer,
    topic: &Arc<str>,
    phase: &str,
    count: u64,
    partitions: i32,
    payload_bytes: usize,
    output: &mut BufWriter<std::fs::File>,
) -> Result<(), Box<dyn Error>> {
    let phase_started = Instant::now();
    let phase_budget = Duration::from_secs(120);
    let mut outstanding = 0_u8;
    let mut max_outstanding = 0_u8;
    for id in 0..count {
        if outstanding != 0 {
            return Err("more than one outstanding record".into());
        }
        outstanding += 1;
        max_outstanding = max_outstanding.max(outstanding);
        let remaining = phase_budget
            .checked_sub(phase_started.elapsed())
            .ok_or("phase deadline expired")?;
        let partition = i32::try_from(id % u64::try_from(partitions)?)?;
        let (key, payload) = record(SEED, id, payload_bytes);
        let record = ProduceRecord::to(Arc::clone(topic))
            .partition(partition)
            .key(key.to_vec())
            .value(payload);
        let started = Instant::now();
        let result = tokio::time::timeout(remaining, producer.send(record)).await;
        let metadata = match result {
            Ok(Ok(metadata)) => metadata,
            Ok(Err(error)) => {
                writeln!(output, "failed,{phase},{id},delivery")?;
                output.flush()?;
                return Err(format!("{phase} record {id}: {error}").into());
            }
            Err(_) => {
                writeln!(output, "failed,{phase},{id},unknown-timeout")?;
                output.flush()?;
                return Err(format!("{phase} record {id}: outcome unknown after timeout").into());
            }
        };
        outstanding -= 1;
        let elapsed = started.elapsed().as_nanos();
        if metadata.topic.as_ref() != topic.as_ref()
            || metadata.partition != partition
            || metadata.offset < 0
            || metadata.serialized_key_size != 16
            || metadata.serialized_value_size != i32::try_from(payload_bytes)?
        {
            writeln!(output, "failed,{phase},{id},metadata")?;
            output.flush()?;
            return Err(format!("{phase} record {id}: invalid delivery metadata").into());
        }
        writeln!(
            output,
            "record,{phase},{id},{partition},{},{elapsed}",
            metadata.offset
        )?;
    }
    let elapsed = phase_started.elapsed().as_nanos();
    if outstanding != 0 || max_outstanding != 1 {
        return Err("missing completion or occupancy evidence".into());
    }
    writeln!(
        output,
        "phase_end,{phase},{count},{elapsed},{max_outstanding}"
    )?;
    output.flush()?;
    Ok(())
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn Error>> {
    let bootstrap = std::env::var("KAFKA_BOOTSTRAP")?;
    let topic: Arc<str> = std::env::var("KAFKA_TOPIC")?.into();
    let output_path = std::env::var("ACK_DIAG_OUTPUT")?;
    let warmup = positive("WARMUP", 16, 100_000)?;
    let count = positive("COUNT", 64, 1_000_000)?;
    let partitions = i32::try_from(positive("PARTITIONS", 6, 1_000)?)?;
    let payload_bytes = usize::try_from(positive("PAYLOAD_BYTES", 100, 100_000)?)?;
    let file = OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(output_path)?;
    let mut output = BufWriter::new(file);
    writeln!(
        output,
        "config,{SEED},{warmup},{count},{partitions},{payload_bytes}"
    )?;
    output.flush()?;

    let mut cfg = ProducerConfig::bootstrap([bootstrap]);
    cfg.acks = -1;
    cfg.enable_idempotence = true;
    cfg.connections = 1;
    cfg.max_in_flight = 1;
    cfg.linger = Duration::ZERO;
    cfg.batch_records = 1;
    cfg.batch_bytes = 1_000_000;
    cfg.max_request_size = 1_048_576;
    cfg.buffer_memory = 32 * 1024 * 1024;
    cfg.delivery_timeout = Duration::from_secs(30);
    cfg.request_timeout = Duration::from_secs(30);
    cfg.max_block = Duration::from_secs(30);
    cfg.transactional_id = None;
    cfg.compression = Compression::None;
    let sample_key = [0_u8; 16];
    let sample_value = vec![0_u8; payload_bytes];
    let record_upper = usize::try_from(Records::estimate_size_in_bytes_upper_bound(
        Some(&sample_key),
        Some(&sample_value),
        &[],
    )?)?;
    if record_upper > cfg.max_request_size || cfg.max_request_size >= cfg.buffer_memory {
        return Err("configured buffers cannot contain the record bound".into());
    }
    writeln!(
        output,
        "settings,{},{},{},{},{},{},{},{},{},{},{record_upper}",
        cfg.acks,
        cfg.enable_idempotence,
        cfg.linger.as_millis(),
        cfg.batch_bytes,
        cfg.batch_records,
        cfg.max_in_flight,
        cfg.connections,
        cfg.buffer_memory,
        cfg.delivery_timeout.as_millis(),
        cfg.max_request_size
    )?;
    output.flush()?;
    let producer = Producer::new(cfg).await?;
    phase(
        &producer,
        &topic,
        "warmup",
        warmup,
        partitions,
        payload_bytes,
        &mut output,
    )
    .await?;
    // Every warmup send returned its individual broker acknowledgement before
    // the timed phase begins; flush is not used as a completion barrier.
    phase(
        &producer,
        &topic,
        "measure",
        count,
        partitions,
        payload_bytes,
        &mut output,
    )
    .await?;
    tokio::time::timeout(Duration::from_secs(30), producer.close()).await??;
    writeln!(output, "complete")?;
    output.flush()?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn deterministic_record_matches_the_native_peer() {
        let (key, payload) = record(SEED, 1, 16);
        assert_eq!(
            key,
            [0, 0, 0, 0, 0, 0, 0, 1, 0x1c, 0xc1, 0x52, 0xe4, 0x7d, 0x17, 0x4d, 0x3c]
        );
        assert_eq!(
            payload,
            [
                0x67, 0xf1, 0xd8, 0x67, 0x34, 0x33, 0xdf, 0x28, 0x52, 0x41, 0x67, 0xc4, 0x4d, 0xf7,
                0x11, 0xf8
            ]
        );
    }
}

//! Closed-loop producer comparison fixture; receipts require independent audit.
use partitionline::{Compression, ProduceRecord, Producer, ProducerConfig};
use std::error::Error;
use std::fs::OpenOptions;
use std::io::{BufWriter, Write};
use std::sync::Arc;
use std::time::{Duration, Instant};

const SEED: u64 = 0x5eed_0001;
const GOLDEN: u64 = 0x9e37_79b9_7f4a_7c15;

fn mix(mut value: u64) -> u64 {
    value = value.wrapping_add(GOLDEN);
    value = (value ^ (value >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    value = (value ^ (value >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    value ^ (value >> 31)
}

fn record(id: u64, bytes: usize) -> ([u8; 16], Vec<u8>) {
    let mut key = [0; 16];
    for (slot, byte) in key.iter_mut().zip(
        id.to_be_bytes()
            .into_iter()
            .chain(mix(SEED ^ id).to_be_bytes()),
    ) {
        *slot = byte;
    }
    let mut payload = Vec::with_capacity(bytes);
    let mut state = SEED ^ id.wrapping_mul(GOLDEN);
    while payload.len() < bytes {
        state = mix(state);
        let remaining = bytes - payload.len();
        payload.extend(state.to_be_bytes().into_iter().take(remaining.min(8)));
    }
    (key, payload)
}

#[derive(Debug)]
struct Receipt {
    id: u64,
    offset: i64,
    elapsed_ns: u128,
}

struct Phase {
    name: &'static str,
    expected: u64,
    receipts: Vec<Receipt>,
    elapsed_ns: u128,
}

impl Phase {
    fn new(name: &'static str, expected: u64) -> Self {
        Self {
            name,
            expected,
            receipts: Vec::with_capacity(expected as usize),
            elapsed_ns: 0,
        }
    }
    fn write(&self, out: &mut impl Write) -> std::io::Result<()> {
        for item in &self.receipts {
            writeln!(out, "record,{},{},0,{},{}", self.name, item.id, item.offset, item.elapsed_ns)?;
        }
        writeln!(out, "phase_end,{},{},{},{}", self.name, self.receipts.len(), self.elapsed_ns, if self.receipts.is_empty() { 0 } else { 1 })
    }
}

async fn phase(
    producer: &Producer,
    topic: &Arc<str>,
    start: u64,
    payload_bytes: usize,
    result: &mut Phase,
) -> Result<(), Box<dyn Error>> {
    let phase_started = Instant::now();
    for id in start..start + result.expected {
        let (key, value) = record(id, payload_bytes);
        let item = ProduceRecord::to(Arc::clone(topic))
            .partition(0)
            .key(key.to_vec())
            .value(value);
        let started = Instant::now();
        let outcome = producer.send(item).await;
        let elapsed_ns = started.elapsed().as_nanos();
        let metadata = match outcome {
            Ok(metadata) => metadata,
            Err(error) => {
                result.elapsed_ns = phase_started.elapsed().as_nanos();
                return Err(format!("{} record {} delivery: {}", result.name, id, error).into());
            }
        };
        if metadata.topic.as_ref() != topic.as_ref()
            || metadata.partition != 0
            || metadata.offset != i64::try_from(id)?
            || metadata.serialized_key_size != 16
            || metadata.serialized_value_size != i32::try_from(payload_bytes)?
        {
            result.elapsed_ns = phase_started.elapsed().as_nanos();
            return Err(format!("{} record {} invalid metadata", result.name, id).into());
        }
        result.receipts.push(Receipt {
            id,
            offset: metadata.offset,
            elapsed_ns,
        });
    }
    result.elapsed_ns = phase_started.elapsed().as_nanos();
    Ok(())
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn Error>> {
    let topic: Arc<str> = std::env::var("KAFKA_TOPIC")?.into();
    let bootstrap = std::env::var("KAFKA_BOOTSTRAP")?;
    let output = std::env::var("ACK_TRACE")?;
    let warmup = std::env::var("WARMUP")?.parse::<u64>()?;
    let count = std::env::var("COUNT")?.parse::<u64>()?;
    let payload = std::env::var("PAYLOAD_BYTES")?.parse::<usize>()?;
    if warmup == 0 || count == 0 || warmup > 100_000 || count > 100_000 || payload != 100 {
        return Err("invalid workload".into());
    }
    let mut cfg = ProducerConfig::bootstrap([bootstrap]);
    cfg.client_id = "paired-ack-benchmark".into();
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
    cfg.connect_timeout = Duration::from_secs(10);
    cfg.connections_max_idle = Duration::from_secs(540);
    cfg.compression = Compression::None;
    cfg.transactional_id = None;
    cfg.allow_auto_topic_creation = false;
    let settings = format!(
        "settings,{},{},{},{},{},{},{},{},{},{},{}",
        cfg.acks, cfg.enable_idempotence, cfg.linger.as_millis(), cfg.batch_bytes,
        cfg.batch_records, cfg.max_in_flight, cfg.connections, cfg.buffer_memory,
        cfg.delivery_timeout.as_millis(), cfg.max_request_size, cfg.connections_max_idle.as_millis()
    );
    let mut warm = Phase::new("warmup", warmup);
    let mut timed = Phase::new("measure", count);
    let outcome = async {
        let producer = Producer::new(cfg).await?;
        phase(&producer, &topic, 0, payload, &mut warm).await?;
        phase(&producer, &topic, warmup, payload, &mut timed).await?;
        producer.close().await?;
        Ok::<(), Box<dyn Error>>(())
    }.await;
    let mut file = BufWriter::new(OpenOptions::new().create_new(true).write(true).open(output)?);
    writeln!(file, "config,{SEED},{warmup},{count},1,{payload}")?;
    writeln!(file, "{settings}")?;
    warm.write(&mut file)?;
    timed.write(&mut file)?;
    match &outcome {
        Ok(()) => writeln!(file, "complete")?,
        Err(error) => writeln!(file, "failed,{error}")?,
    }
    file.flush()?;
    outcome
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn shared_record_pattern() {
        let (key, value) = record(1, 16);
        assert_eq!(key, [0, 0, 0, 0, 0, 0, 0, 1, 0x1c, 0xc1, 0x52, 0xe4, 0x7d, 0x17, 0x4d, 0x3c]);
        assert_eq!(value, [0x67, 0xf1, 0xd8, 0x67, 0x34, 0x33, 0xdf, 0x28, 0x52, 0x41, 0x67, 0xc4, 0x4d, 0xf7, 0x11, 0xf8]);
    }
}

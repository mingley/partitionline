//! Local baseline tool. All received key/value bytes and offsets are checked.
use partitionline::{Consumer, ConsumerConfig};
use runtime::measure::{cpu_now, peak_rss_bytes, rss_now, RssSampler};
use std::time::{Duration, Instant};

#[global_allocator]
static ALLOC: codec::CountingAlloc = codec::CountingAlloc;

fn mix(mut value: u64) -> u64 {
    value = value.wrapping_add(0x9e3779b97f4a7c15);
    value = (value ^ (value >> 30)).wrapping_mul(0xbf58476d1ce4e5b9);
    value = (value ^ (value >> 27)).wrapping_mul(0x94d049bb133111eb);
    value ^ (value >> 31)
}

fn verify(seed: u64, id: u64, key: Option<&[u8]>, value: Option<&[u8]>) -> bool {
    let mut expected_key = [0u8; 16];
    expected_key[..8].copy_from_slice(&id.to_be_bytes());
    expected_key[8..].copy_from_slice(&mix(seed ^ id).to_be_bytes());
    let mut expected_value = [0u8; 100];
    let mut state = seed ^ id.wrapping_mul(0x9e3779b97f4a7c15);
    for block in expected_value.chunks_mut(8) {
        state = mix(state);
        block.copy_from_slice(&state.to_be_bytes()[..block.len()]);
    }
    key == Some(expected_key.as_slice()) && value == Some(expected_value.as_slice())
}

fn check_record(
    record: &partitionline::FetchedRecord,
    positions: &mut [u64; 6],
    seed: u64,
    warmup: u64,
) -> Result<(), Box<dyn std::error::Error>> {
    let p = usize::try_from(record.partition())?;
    let next = positions
        .get_mut(p)
        .ok_or("partition outside declared six")?;
    if u64::try_from(record.offset())? != *next || !record.headers().is_empty() {
        return Err("offset gap/duplicate or unexpected headers".into());
    }
    let warm = (warmup + 5 - u64::try_from(p)?) / 6;
    let ordinal = if *next < warm { *next } else { *next - warm };
    let id = u64::try_from(p)? + ordinal * 6;
    if !verify(seed, id, record.key(), record.value()) {
        return Err("received ID, key or payload differs from seeded generator".into());
    }
    *next += 1;
    Ok(())
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<_> = std::env::args().skip(1).collect();
    if args.len() != 5 {
        return Err("usage: baseline-fetch-verifier bootstrap topic warmup count seed".into());
    }
    let warmup: u64 = args[2].parse()?;
    let count: u64 = args[3].parse()?;
    let seed: u64 = args[4].parse()?;
    if warmup < 10_000 || count == 0 || count > 8_000_000 {
        return Err("warmup>=10000 and count in 1..8000000 required".into());
    }
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    let mut config = ConsumerConfig::bootstrap([args[0].clone()]);
    config.max_wait_ms = 100;
    config.max_bytes = 16 * 1024 * 1024;
    config.max_partition_fetch_bytes = 1024 * 1024;
    config.buffer_memory = 32 * 1024 * 1024;
    config.min_bytes = 1;
    let mut consumer = rt.block_on(Consumer::new(config))?;
    rt.block_on(consumer.assign_topic(&args[1], 0))?;
    let assignment = consumer.assignment();
    if assignment.len() != 6 {
        return Err("six partitions required".into());
    }
    let end = rt.block_on(consumer.end_offsets(assignment))?;
    for (tp, offset) in &end {
        let p = u64::try_from(tp.partition())?;
        let expected = (warmup + 5 - p) / 6 + (count + 5 - p) / 6;
        if u64::try_from(*offset)? != expected {
            return Err("independent end fence differs".into());
        }
    }
    let mut positions = [0u64; 6];
    rt.block_on(async {
        let deadline = Instant::now() + Duration::from_secs(60);
        while positions.iter().sum::<u64>() < warmup {
            if Instant::now() >= deadline {
                return Err("warmup deadline".into());
            }
            let batch = consumer.fetch().await?;
            for record in batch.iter() {
                check_record(record, &mut positions, seed, warmup)?;
            }
        }
        Ok::<_, Box<dyn std::error::Error>>(())
    })?;
    let warmup_verified = positions.iter().sum::<u64>();
    // Discard warmup-prefetched records and begin each partition at its measured fence.
    for (p, position) in positions.iter_mut().enumerate() {
        *position = (warmup + 5 - u64::try_from(p)?) / 6;
        consumer.seek(&args[1], i32::try_from(p)?, i64::try_from(*position)?)?;
    }
    let baseline_rss = rss_now();
    let sampler = RssSampler::start(Duration::from_millis(10), 60_000);
    let before_cpu = cpu_now();
    let start = Instant::now();
    let (outcome, allocations, allocated_bytes) = codec::census(|| {
        rt.block_on(async {
            let deadline = start + Duration::from_secs(180);
            let mut received = 0;
            let mut rounds = 0;
            while received < count {
                if Instant::now() >= deadline {
                    return Err("measured fetch deadline".into());
                }
                let batch = consumer.fetch().await?;
                rounds += 1;
                for record in batch.iter() {
                    check_record(record, &mut positions, seed, warmup)?;
                    received += 1;
                    if received > count {
                        return Err("more records than the measured fence".into());
                    }
                }
            }
            Ok::<_, Box<dyn std::error::Error>>((received, rounds))
        })
    });
    let elapsed = start.elapsed().as_secs_f64();
    let cpu = cpu_now().saturating_sub(before_cpu);
    let (peak_rss, average_rss) = sampler.stop();
    rt.block_on(consumer.close())?;
    let (received, rounds) = outcome?;
    for (tp, offset) in end {
        if positions[usize::try_from(tp.partition())?] != u64::try_from(offset)? {
            return Err("final per-partition fence differs".into());
        }
    }
    println!(
        "{}",
        serde_json::json!({"schema_version":1,"status":"verified",
        "scope":"local/unsigned","suite_hold":"active","consumer_closed":true,
        "count":received,"records_verified":received,"warmup_records_verified":warmup_verified,
        "warmup_fence_records":warmup,"elapsed_seconds":elapsed,"fetch_rounds":rounds,
        "records_per_second":received as f64/elapsed,"cpu_ns_per_record":cpu.total_us() as f64*1000.0/received as f64,
        "allocations":allocations,"allocated_bytes":allocated_bytes,"peak_rss_bytes":peak_rss,
        "average_rss_bytes":average_rss,"baseline_rss_bytes":baseline_rss,"process_peak_rss_bytes":peak_rss_bytes(),
        "partitions":6,"max_wait_ms":100,"max_bytes":16777216,"max_partition_bytes":1048576,
        "buffer_memory":33554432,"seed":seed,"isolation":"read_uncommitted",
        "measurement_note":"Single-thread client; inline full-byte verification included. Warmup readback and seeks excluded; pending warmup responses discarded before measurement."})
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::verify;
    #[test]
    fn corrupted_null_short_and_wrong_identity_are_rejected() {
        assert!(!verify(912, 0, None, None));
        assert!(!verify(912, 0, Some(&[0; 15]), Some(&[0; 100])));
        assert!(!verify(912, 1, Some(&[0; 16]), Some(&[0; 99])));
    }
}

//! Timing driver for the production ShareFetch acquisition lookup.
use std::hint::black_box;
use std::path::PathBuf;
use std::time::Instant;

use codec::{census, CountingAlloc};
use partitionline::protocol;
use protocol::share::AcquiredRange;
use serde_json::json;

// Compile the actual production helper, including its behavioral tests.
#[path = "../../../src/share/acquired.rs"]
mod acquired;

#[global_allocator]
static ALLOC: CountingAlloc = CountingAlloc;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut args = std::env::args().skip(1);
    let mut output = None;
    let mut iterations = 10_000_u64;
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--output" => output = Some(PathBuf::from(args.next().ok_or("missing output")?)),
            "--iterations" => iterations = args.next().ok_or("missing iterations")?.parse()?,
            _ => return Err(format!("unknown argument: {arg}").into()),
        }
    }
    if !(1..=1_000_000).contains(&iterations) {
        return Err("iterations must be 1..1000000".into());
    }
    let output = output.ok_or("--output is required")?;
    let ranges: Vec<_> = (0..1_000)
        .map(|i| AcquiredRange {
            first_offset: i * 5,
            last_offset: i * 5 + 2,
            delivery_count: (i % 7 + 1) as i16,
        })
        .collect();
    let offsets: Vec<_> = (0..5_000).collect();
    let mut lookup = acquired::AcquisitionRanges::new(&ranges);
    for &offset in &offsets {
        let expected = ranges
            .iter()
            .find(|r| r.first_offset <= offset && offset <= r.last_offset)
            .map(|r| r.delivery_count);
        if lookup.delivery_count(offset) != expected {
            return Err("production lookup disagrees with the linear offset oracle".into());
        }
    }
    let scan = || {
        let mut lookup = acquired::AcquisitionRanges::new(black_box(&ranges));
        let mut sum = 0_u64;
        for &offset in black_box(&offsets) {
            sum += u64::from(lookup.delivery_count(black_box(offset)).unwrap_or(0) as u16);
        }
        black_box(sum)
    };
    let expected = scan();
    for _ in 0..100 {
        assert_eq!(scan(), expected);
    }
    let (_, allocation_count, allocated_bytes) = census(scan);
    let start = Instant::now();
    let mut checksum = 0_u64;
    for _ in 0..iterations {
        checksum += scan();
    }
    let elapsed_ns = start.elapsed().as_nanos();
    if checksum != expected * iterations || elapsed_ns == 0 {
        return Err("timed lookup checksum or clock failed".into());
    }
    let value = json!({
        "schema_version": 1,
        "cell": "micro-share-ranges",
        "range_count": ranges.len(),
        "lookups_per_iteration": offsets.len(),
        "acquired_offsets_per_iteration": 3000,
        "gap_offsets_per_iteration": 2000,
        "iterations": iterations,
        "excluded_warmup_iterations": 100,
        "elapsed_ns": elapsed_ns as u64,
        "ns_per_record": elapsed_ns as f64 / (iterations as f64 * offsets.len() as f64),
        "checksum": checksum,
        "expected_checksum": expected * iterations,
        "lookup_allocation_count": allocation_count,
        "lookup_allocated_bytes": allocated_bytes,
        "scope": "Local lookup microbenchmark; production helper; fixtures and linear oracle excluded from timing"
    });
    let mut file = std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(output)?;
    serde_json::to_writer_pretty(&mut file, &value)?;
    std::io::Write::write_all(&mut file, b"\n")?;
    file.sync_all()?;
    println!("{}", value);
    Ok(())
}

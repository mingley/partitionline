//! Paired reopen timing for a segmented ordinary log with valid indexes.
use partitionline_broker::{journal, records, segments};
use std::{fs, path::Path, time::Instant};

const COUNT: usize = 16;
const WARMUP: usize = 8;
const SAMPLES: usize = 64;
const BASIC: &[u8] = include_bytes!("../tests/fixtures/records/valid-basic.bin");

fn payload(offset: u64) -> Vec<u8> {
    let mut bytes = BASIC.to_vec();
    bytes[..8].copy_from_slice(&offset.to_be_bytes());
    bytes
}

fn open(path: &Path) -> (segments::Log, segments::Recovery) {
    segments::Log::open(
        path,
        0,
        journal::Limits::new(1024, 8192, 64, 4096).expect("journal limits"),
        records::Limits::default(),
        segments::Limits::new(150, 32, 4, 2, 1024 * 1024, 1024 * 1024, 4096).expect("segment limits"),
    ).expect("reopen")
}

fn verify(log: &mut segments::Log, recovery: &segments::Recovery) {
    assert_eq!(recovery.segments, COUNT);
    assert_eq!(recovery.rebuilt_indexes, 0);
    assert_eq!(recovery.removed_staging_files, 0);
    assert_eq!(recovery.truncated_bytes, 0);
    assert_eq!(recovery.next_offset, COUNT as u64);
    let entries = log.fetch(0, COUNT, 4096).expect("fetch");
    assert_eq!(entries.len(), COUNT);
    for (offset, entry) in entries.iter().enumerate() {
        assert_eq!(entry.first_offset, offset as u64);
        assert_eq!(entry.record_count, 1);
        assert_eq!(entry.payload, payload(offset as u64));
    }
}

fn main() {
    let root = std::path::PathBuf::from(std::env::var_os("BENCH_ROOT").expect("BENCH_ROOT"));
    fs::create_dir(&root).expect("fresh root");
    let path = root.join("log");
    let (mut log, _) = open(&path);
    for offset in 0..COUNT {
        let append = log.append(1, &payload(offset as u64)).expect("append");
        assert_eq!(append.first_offset, offset as u64);
    }
    assert_eq!(log.segment_count(), COUNT);
    drop(log);
    let mut times = Vec::with_capacity(SAMPLES);
    for iteration in 0..(WARMUP + SAMPLES) {
        let started = Instant::now();
        let (mut log, recovery) = open(&path);
        let elapsed = started.elapsed().as_nanos();
        verify(&mut log, &recovery);
        drop(log);
        if iteration >= WARMUP {
            times.push(elapsed);
            println!("sample,{},{}", iteration - WARMUP, elapsed);
        }
    }
    times.sort_unstable();
    println!(
        "summary,segments,{COUNT},sealed,{},warmup,{WARMUP},samples,{SAMPLES},min_ns,{},median_ns,{},p99_ns,{},max_ns,{}",
        COUNT - 1, times[0], times[SAMPLES / 2], times[(SAMPLES * 99 / 100).min(SAMPLES - 1)], times[SAMPLES - 1]
    );
    println!("validated,all_16_offsets_and_payloads,valid_indexes_no_rebuild");
    fs::remove_dir_all(root).expect("cleanup");
}

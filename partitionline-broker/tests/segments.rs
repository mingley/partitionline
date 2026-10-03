//! Real rolled files, checked physical seeks, index recovery and bounded replacement.
use partitionline_broker::{journal, records, segments};
use std::{
    fs,
    io::{Read, Write},
    path::PathBuf,
    sync::atomic::{AtomicU64, Ordering},
};
type Result = std::result::Result<(), Box<dyn std::error::Error>>;
const BASIC: &[u8] = include_bytes!("fixtures/records/valid-basic.bin");
const MULTIPLE: &[u8] = include_bytes!("fixtures/records/valid-multiple-batches.bin");
struct Temp(PathBuf);
impl Temp {
    fn new() -> std::io::Result<Self> {
        static NEXT: AtomicU64 = AtomicU64::new(0);
        let path = std::env::temp_dir().join(format!(
            "partitionline-segments-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        fs::create_dir(&path)?;
        Ok(Self(path))
    }
    fn log(&self) -> PathBuf {
        self.0.join("log")
    }
}
impl Drop for Temp {
    fn drop(&mut self) {
        drop(fs::remove_dir_all(&self.0));
    }
}
fn limits(count: usize) -> std::result::Result<segments::Limits, segments::Error> {
    segments::Limits::new(150, count, 4, 2, 1024 * 1024, 64 * 1024, 4096)
}
fn journal_limits() -> std::result::Result<journal::Limits, journal::Error> {
    journal::Limits::new(1024, 8192, 16, 4096)
}
fn payload(offset: u64, time: i64) -> Vec<u8> {
    let mut b = BASIC.to_vec();
    b[..8].copy_from_slice(&offset.to_be_bytes());
    b[27..35].copy_from_slice(&time.to_be_bytes());
    b[35..43].copy_from_slice(&time.to_be_bytes());
    let crc = crc32c::crc32c(&b[21..]);
    b[17..21].copy_from_slice(&crc.to_be_bytes());
    b
}
fn open(
    path: PathBuf,
    count: usize,
) -> std::result::Result<(segments::Log, segments::Recovery), segments::Error> {
    segments::Log::open(
        path,
        0,
        journal_limits().map_err(segments::Error::Storage)?,
        records::Limits::default(),
        limits(count)?,
    )
}
fn files(path: &std::path::Path, suffix: &str) -> std::io::Result<Vec<PathBuf>> {
    let mut v = Vec::new();
    for e in fs::read_dir(path)? {
        let e = e?;
        if e.file_name().to_string_lossy().ends_with(suffix) {
            v.push(e.path());
        }
    }
    v.sort();
    Ok(v)
}

fn read_file(path: impl AsRef<std::path::Path>) -> std::io::Result<Vec<u8>> {
    let mut bytes = Vec::new();
    fs::File::open(path)?
        .take(1_048_577)
        .read_to_end(&mut bytes)?;
    if bytes.len() > 1_048_576 {
        return Err(std::io::Error::other("bounded test file"));
    }
    Ok(bytes)
}
fn write_file(path: impl AsRef<std::path::Path>, bytes: impl AsRef<[u8]>) -> std::io::Result<()> {
    fs::File::create(path)?.write_all(bytes.as_ref())
}

#[test]
fn rolls_and_physical_offset_timestamp_seeks_survive_restart() -> Result {
    let temp = Temp::new()?;
    let times = [1000, 1007, 1003, 1007, 999, 1010];
    let (mut log, _) = open(temp.log(), 16)?;
    for (offset, time) in times.iter().enumerate() {
        assert_eq!(
            log.append(1, &payload(offset as u64, *time))?.first_offset,
            offset as u64
        );
    }
    assert_eq!(log.next_offset(), 6);
    assert_eq!(log.segment_count(), 6);
    assert_eq!(log.entry_count(), 6);
    assert!(log.retained_index_bytes() <= limits(16)?.max_index_bytes());
    for round in 0..2 {
        for offset in 0..6 {
            let got = log.fetch(offset, 1, 4096)?;
            assert_eq!(got.len(), 1);
            assert_eq!(got[0].payload, payload(offset, times[offset as usize]));
        }
        for wanted in [0, 999, 1000, 1001, 1003, 1007, 1008, 1010, 1011, i64::MAX] {
            let first = times
                .iter()
                .position(|t| *t >= wanted)
                .map_or(6, |n| n as u64);
            let hint = log.timestamp_start(wanted)?;
            assert!(hint <= first);
            let got = log.fetch(hint, 16, 4096)?;
            let actual = got
                .iter()
                .find(|e| i64::from_be_bytes(e.payload[35..43].try_into().unwrap()) >= wanted)
                .map_or(6, |e| e.first_offset);
            assert_eq!(actual, first, "round{round}/time{wanted}");
        }
        if round == 0 {
            drop(log);
            let (reopened, recovery) = open(temp.log(), 16)?;
            assert_eq!(recovery.rebuilt_indexes, 0);
            assert_eq!(recovery.next_offset, 6);
            log = reopened;
        }
    }
    if let Some(out) = std::env::var_os("PARTITIONLINE_SEGMENTS_PROOF_DIR") {
        let out = PathBuf::from(out);
        fs::create_dir_all(&out)?;
        for file in files(&temp.log(), "")? {
            fs::copy(&file, out.join(file.file_name().unwrap()))?;
        }
    }
    Ok(())
}
#[test]
fn atomic_multi_batch_input_and_containing_offsets_are_not_split() -> Result {
    let temp = Temp::new()?;
    let (mut log, _) = open(temp.log(), 8)?;
    // Source batches are unassigned. Log receives Partition's assigned bytes.
    let mut assigned = MULTIPLE.to_vec();
    assigned[74..82].copy_from_slice(&1i64.to_be_bytes());
    log.append(4, &assigned)?;
    log.append(1, &payload(4, 1000))?;
    assert_eq!(log.segment_count(), 2);
    assert_eq!(log.fetch(3, 1, 4096)?[0].payload, assigned);
    assert!(matches!(
        log.fetch(3, 1, 1),
        Err(segments::Error::Storage(
            journal::Error::FetchBudgetExceeded
        ))
    ));
    assert!(!log.is_poisoned());
    drop(log);
    let (mut log, _) = open(temp.log(), 8)?;
    assert_eq!(log.fetch(4, 1, 4096)?[0].first_offset, 4);
    assert_eq!(log.next_offset(), 5);
    Ok(())
}
#[test]
fn missing_and_valid_crc_stale_indexes_are_rejected_and_rebuilt() -> Result {
    let temp = Temp::new()?;
    let (mut log, _) = open(temp.log(), 8)?;
    for n in 0..3 {
        log.append(1, &payload(n, 1000 + n as i64))?;
    }
    drop(log);
    let indexes = files(&temp.log(), ".seek")?;
    assert_eq!(indexes.len(), 2);
    let original = read_file(&indexes[0])?;
    let mut stale = original.clone();
    stale[84..92].copy_from_slice(&100_000i64.to_be_bytes());
    let n = stale.len();
    let crc = crc32c::crc32c(&stale[..n - 4]);
    stale[n - 4..].copy_from_slice(&crc.to_be_bytes());
    write_file(&indexes[0], &stale)?;
    fs::remove_file(&indexes[1])?;
    let (mut log, recovery) = open(temp.log(), 8)?;
    assert_eq!(recovery.rebuilt_indexes, 2);
    assert_eq!(read_file(&indexes[0])?, original);
    assert_eq!(log.fetch(0, 4, 4096)?.len(), 3);
    assert_eq!(log.timestamp_start(1000)?, 0);
    Ok(())
}
#[test]
fn corrupt_or_missing_authoritative_data_fails_closed_without_tail_repair() -> Result {
    for mode in ["crc", "sealed-tail", "missing", "manifest"] {
        let temp = Temp::new()?;
        let (mut log, _) = open(temp.log(), 8)?;
        log.append(1, &payload(0, 1000))?;
        log.append(1, &payload(1, 1001))?;
        drop(log);
        let data = files(&temp.log(), ".journal")?;
        let before = read_file(&data[0])?;
        match mode {
            "crc" => {
                let mut bad = before.clone();
                *bad.last_mut().unwrap() ^= 1;
                write_file(&data[0], bad)?;
            }
            "sealed-tail" => write_file(&data[0], &before[..before.len() - 1])?,
            "missing" => fs::remove_file(&data[0])?,
            _ => {
                let p = temp.log().join("manifest");
                let mut b = read_file(&p)?;
                b[0] ^= 1;
                write_file(p, b)?;
            }
        }
        assert!(open(temp.log(), 8).is_err(), "{mode}");
        if mode == "sealed-tail" {
            assert_eq!(read_file(&data[0])?.len(), before.len() - 1);
        }
    }
    Ok(())
}
#[test]
fn only_active_incomplete_tail_is_repaired_and_ownership_is_exclusive() -> Result {
    let temp = Temp::new()?;
    let (mut log, _) = open(temp.log(), 8)?;
    log.append(1, &payload(0, 1000))?;
    log.append(1, &payload(1, 1001))?;
    assert!(matches!(
        open(temp.log(), 8),
        Err(segments::Error::Storage(journal::Error::AlreadyOpen))
    ));
    drop(log);
    let data = files(&temp.log(), ".journal")?;
    let mut file = fs::OpenOptions::new()
        .append(true)
        .open(data.last().unwrap())?;
    file.write_all(b"PLENT")?;
    drop(file);
    let (mut log, recovery) = open(temp.log(), 8)?;
    assert_eq!(recovery.truncated_bytes, 5);
    assert_eq!(recovery.next_offset, 2);
    assert_eq!(log.fetch(1, 1, 4096)?[0].payload, payload(1, 1001));
    Ok(())
}
#[test]
fn replacement_is_byte_preserving_and_old_generations_are_durably_removed() -> Result {
    let temp = Temp::new()?;
    let (mut log, _) = open(temp.log(), 8)?;
    for n in 0..3 {
        log.append(1, &payload(n, 1000 + n as i64))?;
    }
    let old = files(&temp.log(), ".journal")?[0].clone();
    let bytes = read_file(&old)?;
    log.replace_sealed(0)?;
    assert!(!old.exists());
    let first = files(&temp.log(), ".journal")?[0].clone();
    assert_eq!(read_file(first)?, bytes);
    assert_eq!(log.fetch(0, 4, 4096)?.len(), 3);
    drop(log);
    let (mut log, recovery) = open(temp.log(), 8)?;
    assert_eq!(recovery.rebuilt_indexes, 0);
    assert_eq!(log.fetch(0, 4, 4096)?.len(), 3);
    Ok(())
}

#[test]
fn hidden_scan_exhaustion_cannot_return_a_successful_partial_fetch() -> Result {
    let temp = Temp::new()?;
    let configured = segments::Limits::new(1080, 8, 4, 4, 1024 * 1024, 64 * 1024, 1080)?;
    let (mut log, _) = segments::Log::open(
        temp.log(),
        0,
        journal_limits()?,
        records::Limits::default(),
        configured,
    )?;
    for n in 0..12 {
        log.append(1, &payload(n, 1000 + n as i64))?;
    }
    // Repeated containing-entry seeks include their sparse predecessor scans.
    // Several output entries fit, but cumulative hidden payload work does not.
    assert!(matches!(
        log.fetch(0, 16, 4096),
        Err(segments::Error::ScanBudget)
    ));
    assert!(!log.is_poisoned());
    assert_eq!(log.fetch(11, 1, 4096)?[0].first_offset, 11);
    Ok(())
}

#[test]
fn every_active_entry_prefix_recovers_prior_acks_and_complete_ambiguous_append() -> Result {
    let baseline = Temp::new()?;
    let configured = segments::Limits::new(4096, 8, 2, 1, 1024 * 1024, 64 * 1024, 4096)?;
    let (mut log, _) = segments::Log::open(
        baseline.log(),
        0,
        journal_limits()?,
        records::Limits::default(),
        configured,
    )?;
    for n in 0..3 {
        log.append(1, &payload(n, 1000 + n as i64))?;
    }
    drop(log);
    let tail = Temp::new()?;
    let raw = tail.0.join("entry");
    let (mut journal, _) = journal::Journal::open(&raw, 3, journal_limits()?)?;
    journal.append(1, &payload(3, 1003))?;
    drop(journal);
    let raw_bytes = read_file(&raw)?;
    let entry = &raw_bytes[24..];
    for prefix in 0..=entry.len() {
        let attempt = Temp::new()?;
        fs::create_dir(attempt.log())?;
        for file in files(&baseline.log(), "")? {
            fs::copy(&file, attempt.log().join(file.file_name().unwrap()))?;
        }
        let active = files(&attempt.log(), ".journal")?.pop().unwrap();
        fs::OpenOptions::new()
            .append(true)
            .open(&active)?
            .write_all(&entry[..prefix])?;
        let (mut recovered, recovery) = segments::Log::open(
            attempt.log(),
            0,
            journal_limits()?,
            records::Limits::default(),
            configured,
        )?;
        let complete = prefix == entry.len();
        assert_eq!(
            recovery.next_offset,
            if complete { 4 } else { 3 },
            "prefix{prefix}"
        );
        assert_eq!(
            recovery.truncated_bytes,
            if complete { 0 } else { prefix as u64 }
        );
        for n in 0..recovery.next_offset {
            assert_eq!(
                recovered.fetch(n, 1, 4096)?[0].payload,
                payload(n, 1000 + n as i64)
            );
        }
        let next = recovery.next_offset;
        assert_eq!(
            recovered
                .append(1, &payload(next, 1000 + next as i64))?
                .first_offset,
            next
        );
    }
    Ok(())
}

#[test]
fn every_partial_index_and_valid_crc_stale_hint_is_rebuilt_from_checked_data() -> Result {
    let temp = Temp::new()?;
    let (mut log, _) = open(temp.log(), 8)?;
    log.append(1, &payload(0, 1000))?;
    log.append(1, &payload(1, 1001))?;
    drop(log);
    let index = files(&temp.log(), ".seek")?.remove(0);
    let original = read_file(&index)?;
    for prefix in 0..original.len() {
        write_file(&index, &original[..prefix])?;
        let (mut recovered, recovery) = open(temp.log(), 8)?;
        assert_eq!(recovery.rebuilt_indexes, 1, "prefix{prefix}");
        assert_eq!(read_file(&index)?, original);
        assert_eq!(recovered.fetch(0, 1, 4096)?[0].payload, payload(0, 1000));
    }
    // Descriptor identity/length/count/max-time/fingerprint and checkpoint
    // offset/physical position/prefix time remain untrusted even with valid CRC.
    for position in [8, 16, 24, 32, 40, 48, 56, 64, 68, 76, 84] {
        let mut stale = original.clone();
        stale[position] ^= 1;
        let end = stale.len() - 4;
        let crc = crc32c::crc32c(&stale[..end]);
        stale[end..].copy_from_slice(&crc.to_be_bytes());
        write_file(&index, &stale)?;
        let (mut recovered, recovery) = open(temp.log(), 8)?;
        assert_eq!(recovery.rebuilt_indexes, 1, "field{position}");
        assert_eq!(read_file(&index)?, original);
        assert_eq!(recovered.timestamp_start(1000)?, 0);
        assert_eq!(recovered.fetch(0, 2, 4096)?.len(), 2);
    }
    Ok(())
}
#[test]
fn count_and_seek_work_exhaustion_never_invent_a_successful_miss() -> Result {
    let temp = Temp::new()?;
    let (mut log, _) = open(temp.log(), 2)?;
    log.append(1, &payload(0, 1000))?;
    log.append(1, &payload(1, 1001))?;
    let before = log.disk_bytes()?;
    assert!(matches!(
        log.append(1, &payload(2, 1002)),
        Err(segments::Error::SegmentBudget)
    ));
    assert_eq!(log.next_offset(), 2);
    assert_eq!(log.disk_bytes()?, before);
    assert!(!log.is_poisoned());
    assert_eq!(log.fetch(0, 2, 4096)?.len(), 2);
    assert!(segments::Limits::new(0, 2, 4, 1, 4096, 65536, 4096).is_err());
    assert!(segments::Limits::new(150, 1024, 65536, 1, 4096, 65536, 4096).is_err());
    Ok(())
}

#[test]
fn disk_and_staging_reservations_hold_before_mutation_and_after_restart() -> Result {
    let temp = Temp::new()?;
    let limits = segments::Limits::new(150, 64, 4, 2, 4096, 65536, 4096)?;
    let (mut log, _) = segments::Log::open(
        temp.log(),
        0,
        journal_limits()?,
        records::Limits::default(),
        limits,
    )?;
    let mut committed = 0u64;
    loop {
        let before = log.disk_bytes()?;
        match log.append(1, &payload(committed, 1000 + committed as i64)) {
            Ok(_) => {
                committed += 1;
                assert!(log.disk_bytes()? <= 4096);
            }
            Err(segments::Error::DiskBudget) => {
                assert_eq!(log.disk_bytes()?, before);
                assert_eq!(log.next_offset(), committed);
                assert!(!log.is_poisoned());
                break;
            }
            other => return Err(format!("unexpected disk outcome {other:?}").into()),
        }
        assert!(committed < 64);
    }
    assert!(committed > 1);
    drop(log);
    let (mut log, _) = segments::Log::open(
        temp.log(),
        0,
        journal_limits()?,
        records::Limits::default(),
        limits,
    )?;
    assert_eq!(log.next_offset(), committed);
    for n in 0..committed {
        assert_eq!(
            log.fetch(n, 1, 4096)?[0].payload,
            payload(n, 1000 + n as i64)
        );
    }
    Ok(())
}

#[test]
fn valid_crc_generation_overflow_is_rejected_before_any_publication() -> Result {
    let temp = Temp::new()?;
    let (mut log, _) = open(temp.log(), 8)?;
    log.append(1, &payload(0, 1000))?;
    drop(log);
    let path = temp.log().join("manifest");
    let mut manifest = read_file(&path)?;
    manifest[8..16].copy_from_slice(&u64::MAX.to_be_bytes());
    let n = manifest.len();
    let crc = crc32c::crc32c(&manifest[..n - 4]);
    manifest[n - 4..].copy_from_slice(&crc.to_be_bytes());
    write_file(&path, &manifest)?;
    let (mut log, _) = open(temp.log(), 8)?;
    let before = log.disk_bytes()?;
    assert!(matches!(
        log.append(1, &payload(1, 1001)),
        Err(segments::Error::GenerationOverflow)
    ));
    assert_eq!(log.next_offset(), 1);
    assert_eq!(log.disk_bytes()?, before);
    assert_eq!(read_file(path)?, manifest);
    assert!(!log.is_poisoned());
    Ok(())
}

#[test]
fn sparse_time_checkpoints_keep_first_equal_and_regressing_timestamp_offsets() -> Result {
    let temp = Temp::new()?;
    let limits = segments::Limits::new(1000, 8, 4, 2, 1024 * 1024, 65536, 4096)?;
    let times = [1000, 1007, 1003, 1015, 999, 1010];
    let (mut log, _) = segments::Log::open(
        temp.log(),
        0,
        journal_limits()?,
        records::Limits::default(),
        limits,
    )?;
    for (n, time) in times.iter().enumerate() {
        log.append(1, &payload(n as u64, *time))?;
    }
    assert_eq!(log.segment_count(), 2);
    for round in 0..2 {
        assert_eq!(log.timestamp_start(1007)?, 0);
        assert_eq!(log.timestamp_start(1010)?, 2);
        for wanted in [999, 1000, 1003, 1007, 1008, 1010, 1015, 1016] {
            let expected = times
                .iter()
                .position(|t| *t >= wanted)
                .map_or(6, |n| n as u64);
            let hint = log.timestamp_start(wanted)?;
            let entries = log.fetch(hint, 16, 4096)?;
            let actual = entries
                .iter()
                .find(|e| i64::from_be_bytes(e.payload[35..43].try_into().unwrap()) >= wanted)
                .map_or(6, |e| e.first_offset);
            assert_eq!(actual, expected, "round{round}/wanted{wanted}");
        }
        if round == 0 {
            drop(log);
            log = segments::Log::open(
                temp.log(),
                0,
                journal_limits()?,
                records::Limits::default(),
                limits,
            )?
            .0;
        }
    }
    Ok(())
}

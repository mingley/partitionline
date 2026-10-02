//! Real-file durability, restart, torn-tail, integrity and budget cases.

use partitionline_broker::journal::{Corruption, Entry, Error, Journal, Limits};
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

const FILE_HEADER: usize = 24;
const ENTRY_HEADER: usize = 32;

struct Temp {
    directory: PathBuf,
}
impl Temp {
    fn new() -> std::io::Result<Self> {
        static NEXT: AtomicU64 = AtomicU64::new(0);
        let root = std::env::var_os("PL_JOURNAL_TEST_TMP")
            .map(PathBuf::from)
            .unwrap_or_else(std::env::temp_dir);
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();
        let directory = root.join(format!(
            "partitionline-journal-{}-{nonce}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        fs::create_dir(&directory)?;
        Ok(Self { directory })
    }
    fn path(&self, name: &str) -> PathBuf {
        self.directory.join(name)
    }
}
impl Drop for Temp {
    fn drop(&mut self) {
        drop(fs::remove_dir_all(&self.directory));
    }
}
fn read(path: &Path) -> std::io::Result<Vec<u8>> {
    let mut bytes = Vec::new();
    File::open(path)?.read_to_end(&mut bytes)?;
    Ok(bytes)
}
fn write(path: &Path, bytes: &[u8]) -> std::io::Result<()> {
    let mut file = OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .open(path)?;
    file.write_all(bytes)?;
    file.sync_all()
}
fn committed(path: &Path) -> Result<Vec<u8>, Error> {
    let (mut journal, _) = Journal::open(path, 7, Limits::default())?;
    journal.append(3, b"first-payload")?;
    journal.append(2, b"second-payload")?;
    drop(journal);
    Ok(read(path)?)
}
fn refresh_header_crc(header: &mut [u8]) {
    let checksum = crc32c::crc32c(&header[..28]);
    header[28..32].copy_from_slice(&checksum.to_be_bytes());
}

#[test]
fn durable_restart_preserves_explicit_record_counts_and_logical_ranges() {
    let temp = Temp::new().unwrap();
    let path = temp.path("journal");
    let (mut journal, opened) = Journal::open(&path, 7, Limits::default()).unwrap();
    assert!(opened.initialized);
    assert_eq!(opened.original_file_bytes, 0);
    assert_eq!(opened.recovered_file_bytes, FILE_HEADER as u64);
    let first = journal.append(3, b"first-payload").unwrap();
    let second = journal.append(2, b"second-payload").unwrap();
    assert_eq!((first.first_offset, first.next_offset), (7, 10));
    assert_eq!((second.first_offset, second.next_offset), (10, 12));
    assert_eq!(journal.entry_count(), 2);
    assert_eq!(journal.file_bytes(), fs::metadata(&path).unwrap().len());
    drop(journal);
    let (mut journal, recovered) = Journal::open(&path, 7, Limits::default()).unwrap();
    assert!(!recovered.initialized);
    assert_eq!(recovered.truncated_bytes, 0);
    assert_eq!(recovered.recovered_entries, 2);
    assert_eq!(recovered.next_offset, 12);
    assert_eq!(journal.base_offset(), 7);
    assert_eq!(journal.next_offset(), 12);
    assert_eq!(
        journal.fetch(8, 4, 4096).unwrap(),
        vec![
            Entry {
                first_offset: 7,
                record_count: 3,
                payload: b"first-payload".to_vec()
            },
            Entry {
                first_offset: 10,
                record_count: 2,
                payload: b"second-payload".to_vec()
            },
        ]
    );
    assert_eq!(journal.fetch(10, 1, 4096).unwrap()[0].first_offset, 10);
    assert!(journal.fetch(12, 1, 4096).unwrap().is_empty());
    assert!(journal.fetch(u64::MAX, 1, 4096).unwrap().is_empty());
    assert!(matches!(
        journal.fetch(6, 1, 4096),
        Err(Error::OffsetBeforeBase)
    ));
    assert_eq!(
        journal.append(1, b"after-restart").unwrap().first_offset,
        12
    );
}

#[test]
fn process_exit_without_destructors_recovers_durable_and_incomplete_entries() {
    const CHILD_PATH: &str = "PL_JOURNAL_CRASH_CHILD_PATH";
    const CHILD_MODE: &str = "PL_JOURNAL_CRASH_CHILD_MODE";
    if let Some(path) = std::env::var_os(CHILD_PATH) {
        let mode = std::env::var(CHILD_MODE).unwrap();
        let path = PathBuf::from(path);
        let (mut journal, _) = Journal::open(&path, 7, Limits::default()).unwrap();
        journal.append(3, b"first-payload").unwrap();
        if mode == "durable" {
            journal.append(2, b"second-payload").unwrap();
        } else {
            assert_eq!(mode, "incomplete");
            let mut file = OpenOptions::new().append(true).open(&path).unwrap();
            file.write_all(b"PLENT").unwrap();
        }
        // End this explicitly invoked child without running Journal/Temp Drop.
        // This is process-death recovery; it does not emulate power loss.
        std::process::exit(0);
    }
    let temp = Temp::new().unwrap();
    for (mode, count, next, truncated) in [("durable", 2, 12, 0), ("incomplete", 1, 10, 5)] {
        let path = temp.path(mode);
        let child = std::process::Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "process_exit_without_destructors_recovers_durable_and_incomplete_entries",
            ])
            .env(CHILD_PATH, &path)
            .env(CHILD_MODE, mode)
            .output()
            .unwrap();
        assert!(
            child.status.success(),
            "child failed: {}",
            String::from_utf8_lossy(&child.stderr)
        );
        let (mut journal, recovery) = Journal::open(&path, 7, Limits::default()).unwrap();
        assert_eq!(recovery.recovered_entries, count);
        assert_eq!(recovery.next_offset, next);
        assert_eq!(recovery.truncated_bytes, truncated);
        assert_eq!(
            journal.fetch(7, 2, 4096).unwrap()[0].payload,
            b"first-payload"
        );
    }
}

#[test]
fn every_incomplete_tail_prefix_repairs_to_last_complete_entry() {
    let temp = Temp::new().unwrap();
    let complete = committed(&temp.path("source")).unwrap();
    let first_end = FILE_HEADER + ENTRY_HEADER + b"first-payload".len();
    for cut in 1..complete.len() - first_end {
        let path = temp.path(&format!("cut-{cut}"));
        write(&path, &complete[..first_end + cut]).unwrap();
        let (mut journal, recovery) = Journal::open(&path, 7, Limits::default()).unwrap();
        assert_eq!(recovery.truncated_bytes, cut as u64);
        assert_eq!(recovery.recovered_entries, 1);
        assert_eq!(recovery.next_offset, 10);
        assert_eq!(read(&path).unwrap(), complete[..first_end]);
        assert_eq!(journal.append(2, b"replacement").unwrap().first_offset, 10);
        drop(journal);
        let (_, again) = Journal::open(&path, 7, Limits::default()).unwrap();
        assert_eq!(again.truncated_bytes, 0);
        assert_eq!(again.next_offset, 12);
    }
}

#[test]
fn complete_payload_and_header_corruption_never_truncate() {
    let temp = Temp::new().unwrap();
    let complete = committed(&temp.path("source")).unwrap();
    let first_end = FILE_HEADER + ENTRY_HEADER + b"first-payload".len();
    for (position, expected) in [
        (FILE_HEADER, Corruption::EntryHeader),
        (FILE_HEADER + 8, Corruption::EntryHeader),
        (FILE_HEADER + ENTRY_HEADER, Corruption::PayloadChecksum),
        (first_end + ENTRY_HEADER, Corruption::PayloadChecksum),
    ] {
        let path = temp.path(&format!("damage-{position}"));
        let mut damaged = complete.clone();
        damaged[position] ^= 0x40;
        write(&path, &damaged).unwrap();
        assert!(
            matches!(Journal::open(&path, 7, Limits::default()), Err(Error::Corrupt { kind, .. }) if kind == expected)
        );
        assert_eq!(read(&path).unwrap(), damaged);
    }
}

#[test]
fn protected_invalid_lengths_counts_and_offset_gaps_fail_closed() {
    let temp = Temp::new().unwrap();
    let complete = committed(&temp.path("source")).unwrap();
    for kind in [
        "zero-length",
        "large-length",
        "zero-count",
        "offset-gap",
        "offset-overflow",
    ] {
        let path = temp.path(kind);
        let mut damaged = complete.clone();
        let header = &mut damaged[FILE_HEADER..FILE_HEADER + ENTRY_HEADER];
        match kind {
            "zero-length" => header[8..12].copy_from_slice(&0u32.to_be_bytes()),
            "large-length" => header[8..12].copy_from_slice(&u32::MAX.to_be_bytes()),
            "zero-count" => header[20..24].copy_from_slice(&0u32.to_be_bytes()),
            "offset-gap" => header[12..20].copy_from_slice(&8u64.to_be_bytes()),
            "offset-overflow" => header[12..20].copy_from_slice(&u64::MAX.to_be_bytes()),
            _ => unreachable!(),
        }
        refresh_header_crc(header);
        write(&path, &damaged).unwrap();
        let error = Journal::open(&path, 7, Limits::default()).err().unwrap();
        if kind == "large-length" {
            assert!(matches!(error, Error::EntryTooLarge));
        } else {
            assert!(matches!(error, Error::Corrupt { .. }));
        }
        assert_eq!(read(&path).unwrap(), damaged);
    }
}

#[test]
fn interior_missing_payload_with_later_entry_is_not_a_torn_final_entry() {
    let temp = Temp::new().unwrap();
    let path = temp.path("journal");
    let (mut journal, _) = Journal::open(&path, 0, Limits::default()).unwrap();
    journal.append(3, &[b'a'; 200]).unwrap();
    journal.append(2, b"later").unwrap();
    drop(journal);
    let original = read(&path).unwrap();
    let mut damaged = original[..FILE_HEADER + ENTRY_HEADER + 1].to_vec();
    damaged.extend_from_slice(&original[FILE_HEADER + ENTRY_HEADER + 200..]);
    write(&path, &damaged).unwrap();
    assert!(matches!(
        Journal::open(&path, 0, Limits::default()),
        Err(Error::Corrupt {
            kind: Corruption::InteriorTail,
            ..
        })
    ));
    assert_eq!(read(&path).unwrap(), damaged);
}

#[test]
fn file_and_index_budgets_hold_on_append_and_recovery() {
    let temp = Temp::new().unwrap();
    for (name, limits, index_limit) in [
        ("file-limit", Limits::new(10, 57, 10, 1024).unwrap(), false),
        ("index-limit", Limits::new(10, 4096, 1, 1024).unwrap(), true),
    ] {
        let path = temp.path(name);
        let (mut journal, _) = Journal::open(&path, 0, limits).unwrap();
        journal.append(1, b"x").unwrap();
        let bytes = read(&path).unwrap();
        let error = journal.append(1, b"y").unwrap_err();
        assert!(if index_limit {
            matches!(error, Error::IndexBudgetExceeded)
        } else {
            matches!(error, Error::FileBudgetExceeded)
        });
        assert!(!journal.is_poisoned());
        assert_eq!(journal.next_offset(), 1);
        assert_eq!(read(&path).unwrap(), bytes);
    }
    let path = temp.path("recovery-limits");
    let complete = committed(&path).unwrap();
    assert!(matches!(
        Journal::open(&path, 7, Limits::new(1024, 57, 10, 1024).unwrap()),
        Err(Error::FileBudgetExceeded)
    ));
    assert!(matches!(
        Journal::open(&path, 7, Limits::new(1024, 4096, 1, 1024).unwrap()),
        Err(Error::IndexBudgetExceeded)
    ));
    assert_eq!(read(&path).unwrap(), complete);
}

#[test]
fn fetch_output_charges_entry_storage_and_never_returns_partial_failure() {
    let temp = Temp::new().unwrap();
    let path = temp.path("journal");
    committed(&path).unwrap();
    let (mut journal, _) = Journal::open(&path, 7, Limits::default()).unwrap();
    let first_charge = std::mem::size_of::<Entry>() + b"first-payload".len();
    assert!(matches!(
        journal.fetch(7, 2, first_charge - 1),
        Err(Error::FetchBudgetExceeded)
    ));
    assert_eq!(journal.fetch(7, 2, first_charge).unwrap().len(), 1);
    for (entries, bytes) in [(0, 1024), (1, 0), (65_537, 1024), (1, 16 * 1024 * 1024 + 1)] {
        assert!(matches!(
            journal.fetch(7, entries, bytes),
            Err(Error::InvalidFetchLimits)
        ));
    }
    assert!(!journal.is_poisoned());
    let mut external = OpenOptions::new().write(true).open(&path).unwrap();
    external
        .seek(SeekFrom::Start(
            (FILE_HEADER + ENTRY_HEADER + b"first-payload".len() + ENTRY_HEADER) as u64,
        ))
        .unwrap();
    external.write_all(b"!").unwrap();
    external.sync_all().unwrap();
    assert!(matches!(
        journal.fetch(7, 2, 4096),
        Err(Error::Corrupt {
            kind: Corruption::PayloadChecksum,
            ..
        })
    ));
    assert!(journal.is_poisoned());
    assert!(matches!(
        journal.append(1, b"blocked"),
        Err(Error::Poisoned)
    ));
}

#[test]
fn invalid_append_and_offset_overflow_leave_committed_state_unchanged() {
    let temp = Temp::new().unwrap();
    let path = temp.path("journal");
    let limits = Limits::new(4, 4096, 10, 1024).unwrap();
    let (mut journal, _) = Journal::open(&path, u64::MAX - 1, limits).unwrap();
    let original = read(&path).unwrap();
    for (count, payload) in [(0, b"x".as_slice()), (1, b""), (1, b"large"), (2, b"x")] {
        assert!(journal.append(count, payload).is_err());
        assert_eq!(journal.next_offset(), u64::MAX - 1);
        assert!(!journal.is_poisoned());
        assert_eq!(read(&path).unwrap(), original);
    }
    assert_eq!(journal.append(1, b"x").unwrap().next_offset, u64::MAX);
    assert!(matches!(
        journal.append(1, b"x"),
        Err(Error::OffsetOverflow)
    ));
    drop(journal);
    let (_, recovered) = Journal::open(&path, u64::MAX - 1, limits).unwrap();
    assert_eq!(recovered.next_offset, u64::MAX);
}

#[test]
fn file_header_identity_and_partial_prefix_damage_fail_closed() {
    let temp = Temp::new().unwrap();
    let path = temp.path("journal");
    let complete = committed(&path).unwrap();
    assert!(matches!(
        Journal::open(&path, 8, Limits::default()),
        Err(Error::BaseOffsetMismatch)
    ));
    for bytes in [
        complete[..10].to_vec(),
        {
            let mut bytes = complete.clone();
            bytes[0] ^= 1;
            bytes
        },
        {
            let mut bytes = complete[..FILE_HEADER].to_vec();
            bytes.extend_from_slice(b"garbage");
            bytes
        },
    ] {
        write(&path, &bytes).unwrap();
        assert!(matches!(
            Journal::open(&path, 7, Limits::default()),
            Err(Error::Corrupt { .. })
        ));
        assert_eq!(read(&path).unwrap(), bytes);
    }
}

#[test]
fn simultaneous_new_file_open_rejects_a_second_live_handle() {
    let temp = Temp::new().unwrap();
    let path = Arc::new(temp.path("journal"));
    let start = Arc::new(std::sync::Barrier::new(2));
    let attempted = Arc::new(std::sync::Barrier::new(2));
    let mut threads = Vec::new();
    for _ in 0..2 {
        let path = path.clone();
        let start = start.clone();
        let attempted = attempted.clone();
        threads.push(std::thread::spawn(move || {
            start.wait();
            let opened = Journal::open(path.as_ref(), 0, Limits::default());
            attempted.wait(); // successful owner lives until both attempts finish.
            match opened {
                Ok((mut journal, _)) => {
                    journal.append(1, b"one owner").unwrap();
                    true
                }
                Err(Error::AlreadyOpen) => false,
                Err(error) => panic!("unexpected ownership failure: {error}"),
            }
        }));
    }
    let successes = threads
        .into_iter()
        .map(|thread| usize::from(thread.join().unwrap()))
        .sum::<usize>();
    assert_eq!(successes, 1);
    let (_, recovered) = Journal::open(path.as_ref(), 0, Limits::default()).unwrap();
    assert_eq!(recovered.recovered_entries, 1);
}

#[cfg(unix)]
#[test]
fn live_inode_aliases_are_rejected_and_guard_releases_after_drop() {
    let temp = Temp::new().unwrap();
    let path = temp.path("journal");
    let (journal, _) = Journal::open(&path, 0, Limits::default()).unwrap();
    let hard = temp.path("hard-link");
    let symbolic = temp.path("symbolic-link");
    fs::hard_link(&path, &hard).unwrap();
    std::os::unix::fs::symlink(&path, &symbolic).unwrap();
    for alias in [&path, &hard, &symbolic] {
        assert!(matches!(
            Journal::open(alias, 0, Limits::default()),
            Err(Error::AlreadyOpen)
        ));
    }
    drop(journal);
    let (mut journal, _) = Journal::open(&hard, 0, Limits::default()).unwrap();
    journal.append(1, b"alias after release").unwrap();
    drop(journal);
    assert_eq!(
        Journal::open(&path, 0, Limits::default())
            .unwrap()
            .1
            .next_offset,
        1
    );
}

#[test]
fn external_file_growth_poisons_append_without_offset_advance() {
    let temp = Temp::new().unwrap();
    let path = temp.path("journal");
    let (mut journal, _) = Journal::open(&path, 0, Limits::default()).unwrap();
    journal.append(2, b"committed").unwrap();
    let mut external = OpenOptions::new().append(true).open(&path).unwrap();
    external.write_all(&b"PLENTRY1"[..3]).unwrap();
    external.sync_all().unwrap();
    assert!(matches!(
        journal.append(1, b"blocked"),
        Err(Error::ChangedFile)
    ));
    assert!(journal.is_poisoned());
    assert_eq!(journal.next_offset(), 2);
    drop(journal);
    let (_, recovered) = Journal::open(&path, 0, Limits::default()).unwrap();
    assert_eq!(recovered.truncated_bytes, 3);
    assert_eq!(recovered.next_offset, 2);
}

#[test]
fn failed_open_releases_ownership_and_positive_limits_are_validated() {
    let temp = Temp::new().unwrap();
    let path = temp.path("journal");
    committed(&path).unwrap();
    assert!(Journal::open(&path, 8, Limits::default()).is_err());
    Journal::open(&path, 7, Limits::default()).unwrap();
    let charge = std::mem::size_of::<Entry>() + 1;
    for (entry, file, index, fetch) in [
        (0, 57, 1, charge),
        (1, 56, 1, charge),
        (1, 57, 0, charge),
        (1, 57, 1, 0),
        (64 * 1024 * 1024 + 1, 57, 1, charge),
        (1, (1u64 << 40) + 1, 1, charge),
        (1, 57, 1_000_001, charge),
        (1, 57, 1, 128 * 1024 * 1024 + 1),
    ] {
        assert!(matches!(
            Limits::new(entry, file, index, fetch),
            Err(Error::InvalidLimits)
        ));
    }
    let limits = Limits::new(1, 57, 1, charge).unwrap();
    assert_eq!(limits.max_entry_bytes(), 1);
    assert_eq!(limits.max_file_bytes(), 57);
    assert_eq!(limits.max_index_entries(), 1);
    assert_eq!(limits.max_fetch_bytes(), charge);
}

#[test]
fn actual_file_matches_documented_checksums_and_can_be_retained_for_oracle() {
    let temp = Temp::new().unwrap();
    let path = temp.path("sample");
    let bytes = committed(&path).unwrap();
    assert_eq!(&bytes[..8], b"PLJRNL01");
    assert_eq!(u64::from_be_bytes(bytes[8..16].try_into().unwrap()), 7);
    assert_eq!(
        u32::from_be_bytes(bytes[20..24].try_into().unwrap()),
        crc32c::crc32c(&bytes[..20])
    );
    let header = &bytes[FILE_HEADER..FILE_HEADER + ENTRY_HEADER];
    assert_eq!(&header[..8], b"PLENTRY1");
    assert_eq!(
        u32::from_be_bytes(header[8..12].try_into().unwrap()),
        b"first-payload".len() as u32
    );
    assert_eq!(u64::from_be_bytes(header[12..20].try_into().unwrap()), 7);
    assert_eq!(u32::from_be_bytes(header[20..24].try_into().unwrap()), 3);
    assert_eq!(
        u32::from_be_bytes(header[24..28].try_into().unwrap()),
        crc32c::crc32c(b"first-payload")
    );
    assert_eq!(
        u32::from_be_bytes(header[28..32].try_into().unwrap()),
        crc32c::crc32c(&header[..28])
    );
    if let Some(output) = std::env::var_os("PL_JOURNAL_ARTIFACT_OUTPUT") {
        let output = PathBuf::from(output);
        fs::create_dir_all(&output).unwrap();
        write(&output.join("sample.bin"), &bytes).unwrap();
    }
}

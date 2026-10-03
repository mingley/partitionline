//! Actual-file admission, assigned batch boundaries, restart and corruption.

use partitionline_broker::{journal, partition, records};
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

const BASIC: &[u8] = include_bytes!("fixtures/records/valid-basic.bin");
const MULTIPLE: &[u8] = include_bytes!("fixtures/records/valid-multiple-batches.bin");
type Result = std::result::Result<(), Box<dyn std::error::Error>>;
struct Temp(PathBuf);
impl Temp {
    fn new() -> std::io::Result<Self> {
        static NEXT: AtomicU64 = AtomicU64::new(0);
        let path = std::env::temp_dir().join(format!(
            "partitionline-partition-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        fs::create_dir(&path)?;
        Ok(Self(path))
    }
    fn path(&self, name: &str) -> PathBuf {
        self.0.join(name)
    }
}
impl Drop for Temp {
    fn drop(&mut self) {
        drop(fs::remove_dir_all(&self.0));
    }
}
fn open(path: PathBuf, base: i64) -> std::result::Result<partition::Partition, partition::Error> {
    partition::Partition::open(
        path,
        base,
        journal::Limits::default(),
        records::Limits::default(),
    )
    .map(|(p, _)| p)
}

fn read_file(path: impl AsRef<Path>) -> std::io::Result<Vec<u8>> {
    let mut bytes = Vec::new();
    File::open(path)?.take(1_048_577).read_to_end(&mut bytes)?;
    if bytes.len() > 1_048_576 {
        return Err(std::io::Error::other("test input exceeds bound"));
    }
    Ok(bytes)
}
fn write_file(path: impl AsRef<Path>, bytes: &[u8]) -> std::io::Result<()> {
    File::create(path)?.write_all(bytes)
}

#[test]
fn assigned_batches_preserve_all_other_bytes_and_survive_restart() -> Result {
    let temp = Temp::new()?;
    let path = temp.path("ordinary");
    let mut log = open(path.clone(), 7)?;
    let appended = log.append(MULTIPLE)?;
    assert_eq!(
        appended,
        partition::Append {
            base_offset: 7,
            next_offset: 11,
            record_count: 4,
            batch_count: 2,
        }
    );
    assert_eq!(log.append(BASIC)?.base_offset, 11);
    assert_eq!(log.next_offset(), 12);
    assert_eq!(log.entry_count(), 2);
    let entries = log.fetch(8, 1, 4096)?;
    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0].first_offset, 7);
    let checked = records::validate(&entries[0].payload, records::Limits::default())?;
    let source = records::validate(MULTIPLE, records::Limits::default())?;
    for (index, (assigned, original)) in checked.batches().zip(source.batches()).enumerate() {
        let assigned = assigned?;
        let original = original?;
        assert_eq!(assigned.base_offset, [7, 8][index]);
        assert_eq!(&assigned.bytes[8..], &original.bytes[8..]);
    }
    drop(log);
    let mut reopened = open(path, 7)?;
    assert_eq!(reopened.next_offset(), 12);
    assert_eq!(reopened.fetch(8, 1, 4096)?, entries);
    assert_eq!(reopened.fetch(11, 1, 4096)?[0].first_offset, 11);
    assert!(reopened.fetch(12, 1, 4096)?.is_empty());
    assert!(reopened.fetch(13, 1, 4096)?.is_empty());
    if let Ok(out) = std::env::var("PL_PARTITION_PROOF_DIR") {
        let out = PathBuf::from(out);
        fs::create_dir(&out)?;
        write_file(out.join("assigned-multiple.bin"), &entries[0].payload)?;
        write_file(
            out.join("assigned-basic.bin"),
            &reopened.fetch(11, 1, 4096)?[0].payload,
        )?;
        fs::copy(temp.path("ordinary"), out.join("partition.journal"))?;
    }
    Ok(())
}

#[test]
fn invalid_later_batch_and_unsupported_features_never_append_a_prefix() -> Result {
    let temp = Temp::new()?;
    let mut log = open(temp.path("admission"), 0)?;
    let mut bad = MULTIPLE.to_vec();
    *bad.last_mut().unwrap() ^= 1;
    assert!(matches!(
        log.append(&bad),
        Err(partition::Error::Records(_))
    ));
    assert_eq!(log.entry_count(), 0);
    for name in [
        "feature-idempotent",
        "feature-transactional",
        "feature-gzip",
    ] {
        let fixture = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("tests/fixtures/records")
            .join(format!("{name}.bin"));
        assert!(matches!(
            log.append(&read_file(fixture)?),
            Err(partition::Error::Records(_))
        ));
        assert_eq!(log.entry_count(), 0);
    }
    assert!(log.append(&[]).is_err());
    assert!(!log.is_poisoned());
    assert_eq!(log.append(BASIC)?.base_offset, 0);
    Ok(())
}

#[test]
fn signed_offset_end_bound_is_checked_before_any_write() -> Result {
    let temp = Temp::new()?;
    let path = temp.path("offsets");
    let mut log = open(path.clone(), i64::MAX - 4)?;
    assert_eq!(log.append(MULTIPLE)?.next_offset, i64::MAX);
    let before = read_file(&path)?;
    assert!(matches!(
        log.append(BASIC),
        Err(partition::Error::OffsetOverflow)
    ));
    assert_eq!(read_file(&path)?, before);
    assert_eq!(log.entry_count(), 1);
    drop(log);
    assert_eq!(open(path, i64::MAX - 4)?.next_offset(), i64::MAX);
    assert!(matches!(
        open(temp.path("negative"), -1),
        Err(partition::Error::InvalidLimits)
    ));
    Ok(())
}

#[test]
fn configured_entry_index_output_and_disk_budgets_prevent_growth() -> Result {
    let temp = Temp::new()?;
    let charge = std::mem::size_of::<journal::Entry>() + BASIC.len();
    let limits = journal::Limits::new(BASIC.len(), 4096, 1, charge)?;
    let (mut log, _) =
        partition::Partition::open(temp.path("limits"), 0, limits, records::Limits::default())?;
    assert!(matches!(
        log.append(MULTIPLE),
        Err(partition::Error::InputTooLarge)
    ));
    assert_eq!(log.entry_count(), 0);
    log.append(BASIC)?;
    assert!(log.append(BASIC).is_err());
    assert!(!log.is_poisoned());
    assert!(log.fetch(0, 1, charge - 1).is_err());
    assert_eq!(log.fetch(0, 1, charge)?.len(), 1);
    assert!(log.fetch(0, 0, charge).is_err());
    assert!(log.fetch(-1, 1, charge).is_err());
    let too_small = journal::Limits::new(BASIC.len(), 4096, 1, charge - 1)?;
    assert!(matches!(
        partition::Partition::open(
            temp.path("bad-output"),
            0,
            too_small,
            records::Limits::default()
        ),
        Err(partition::Error::InvalidLimits)
    ));
    let one_file = journal::Limits::new(BASIC.len(), (24 + 32 + BASIC.len()) as u64, 2, charge)?;
    let (mut log, _) =
        partition::Partition::open(temp.path("disk"), 0, one_file, records::Limits::default())?;
    log.append(BASIC)?;
    assert!(log.append(BASIC).is_err());
    assert_eq!(log.next_offset(), 1);
    assert!(!log.is_poisoned());
    Ok(())
}

#[test]
fn outer_checksummed_forged_counts_offsets_crc_and_batch_gaps_fail_recovery() -> Result {
    let temp = Temp::new()?;
    for case in 0..4 {
        let path = temp.path(&format!("forged-{case}"));
        let (mut raw, _) = journal::Journal::open(&path, 0, journal::Limits::default())?;
        let mut payload = if case == 3 {
            MULTIPLE.to_vec()
        } else {
            BASIC.to_vec()
        };
        let count = match case {
            0 => 2,
            1 => {
                payload[..8].copy_from_slice(&1_i64.to_be_bytes());
                1
            }
            2 => {
                *payload.last_mut().unwrap() ^= 1;
                1
            }
            _ => 4,
        };
        raw.append(count, &payload)?;
        drop(raw);
        assert!(
            matches!(open(path, 0), Err(partition::Error::CorruptPayload)),
            "case {case}"
        );
    }
    Ok(())
}

#[test]
fn every_incomplete_final_entry_prefix_repairs_to_the_previous_whole_input() -> Result {
    let temp = Temp::new()?;
    let path = temp.path("whole");
    let mut log = open(path.clone(), 0)?;
    log.append(MULTIPLE)?;
    let prefix = read_file(&path)?;
    log.append(BASIC)?;
    drop(log);
    let full = read_file(&path)?;
    let tail = &full[prefix.len()..];
    for cut in 0..tail.len() {
        let path = temp.path(&format!("cut-{cut}"));
        write_file(&path, &prefix)?;
        OpenOptions::new()
            .append(true)
            .open(&path)?
            .write_all(&tail[..cut])?;
        let mut recovered = open(path.clone(), 0)?;
        assert_eq!(recovered.next_offset(), 4, "cut {cut}");
        assert_eq!(recovered.entry_count(), 1);
        assert_eq!(fs::metadata(path)?.len(), prefix.len() as u64);
        assert_eq!(recovered.fetch(3, 1, 4096)?.len(), 1);
    }
    Ok(())
}

#[test]
fn external_payload_change_poisoned_live_handle_returns_no_partial_output() -> Result {
    let temp = Temp::new()?;
    let path = temp.path("changed");
    let mut log = open(path.clone(), 0)?;
    log.append(BASIC)?;
    log.append(BASIC)?;
    let mut bytes = read_file(&path)?;
    *bytes.last_mut().unwrap() ^= 1;
    write_file(path, &bytes)?;
    assert!(log.fetch(0, 2, 4096).is_err());
    assert!(log.is_poisoned());
    assert!(matches!(log.append(BASIC), Err(partition::Error::Poisoned)));
    assert!(matches!(
        log.fetch(0, 1, 4096),
        Err(partition::Error::Poisoned)
    ));
    Ok(())
}

#[test]
fn rolling_backend_assigns_atomic_multi_batch_ranges_and_replacement_survives_restart() -> Result {
    let temp = Temp::new()?;
    let path = temp.path("rolling");
    let segments = partitionline_broker::segments::Limits::new(
        150,
        16,
        4,
        2,
        8 * 1024 * 1024,
        65536,
        2 * 1024 * 1024,
    )?;
    let (mut part, _) = partition::Partition::open_segmented(
        &path,
        7,
        journal::Limits::default(),
        records::Limits::default(),
        segments,
    )?;
    assert_eq!(part.append(MULTIPLE)?.next_offset, 11);
    assert_eq!(part.append(BASIC)?.base_offset, 11);
    assert_eq!(part.segment_count(), 2);
    let before = part.fetch(8, 1, 4096)?;
    assert_eq!(before[0].first_offset, 7);
    let checked = records::validate(&before[0].payload, records::Limits::default())?;
    assert_eq!(
        checked
            .batches()
            .map(|b| b.unwrap().base_offset)
            .collect::<Vec<_>>(),
        vec![7, 8]
    );
    part.replace_sealed(7)?;
    assert_eq!(part.fetch(8, 1, 4096)?, before);
    drop(part);
    let (mut part, recovery) = partition::Partition::open_segmented(
        &path,
        7,
        journal::Limits::default(),
        records::Limits::default(),
        segments,
    )?;
    assert_eq!(recovery.rebuilt_indexes, 0);
    assert_eq!(part.next_offset(), 12);
    assert_eq!(part.fetch(8, 1, 4096)?, before);
    assert_eq!(part.append(BASIC)?.base_offset, 12);
    Ok(())
}

#[test]
fn retention_requires_rolling_and_keeps_atomic_payload_below_logical_floor() -> Result {
    use partitionline_broker::segments::{DeletionGuard, Limits};
    let temp = Temp::new()?;
    let mut flat = open(temp.path("flat"), 7)?;
    flat.append(BASIC)?;
    let before = read_file(temp.path("flat"))?;
    assert_eq!(flat.log_start_offset(), 7);
    assert!(matches!(
        flat.delete_records(8, DeletionGuard::new(8, 8)?),
        Err(partition::Error::RetentionUnsupported)
    ));
    assert_eq!(read_file(temp.path("flat"))?, before);
    let limits = Limits::new(150, 8, 4, 2, 1024 * 1024, 65536, 4096)?;
    let source = journal::Limits::new(1024, 8192, 16, 4096)?;
    let path = temp.path("rolling-retention");
    let (mut part, _) =
        partition::Partition::open_segmented(&path, 7, source, records::Limits::default(), limits)?;
    part.append(MULTIPLE)?;
    part.append(BASIC)?;
    let containing = part.fetch(9, 1, 4096)?;
    part.delete_records(9, DeletionGuard::new(12, 12)?)?;
    assert_eq!(part.log_start_offset(), 9);
    assert_eq!(part.next_offset(), 12);
    assert!(matches!(
        part.fetch(8, 1, 4096),
        Err(partition::Error::Storage(journal::Error::OffsetBeforeBase))
    ));
    assert_eq!(part.fetch(9, 1, 4096)?, containing);
    drop(part);
    let (mut part, recovery) =
        partition::Partition::open_segmented(&path, 7, source, records::Limits::default(), limits)?;
    assert_eq!(recovery.log_start_offset, 9);
    assert_eq!(part.fetch(10, 1, 4096)?, containing);
    assert_eq!(part.append(BASIC)?.base_offset, 12);
    Ok(())
}

#[test]
fn partition_retention_preserves_protected_records_and_synchronized_end() -> Result {
    use partitionline_broker::segments::{
        DeletionGuard, Error as SegmentError, Limits, RetentionPolicy,
    };
    let temp = Temp::new()?;
    let limits = Limits::new(150, 8, 4, 2, 1024 * 1024, 65536, 4096)?;
    let source = journal::Limits::new(1024, 8192, 16, 4096)?;
    let path = temp.path("safe-retention");
    let (mut part, _) =
        partition::Partition::open_segmented(&path, 0, source, records::Limits::default(), limits)?;
    for _ in 0..5 {
        part.append(BASIC)?;
    }
    assert!(matches!(
        part.delete_records(4, DeletionGuard::new(5, 3)?),
        Err(partition::Error::Segments(SegmentError::ProtectedRecords))
    ));
    assert!(matches!(
        part.delete_records(5, DeletionGuard::new(4, 5)?),
        Err(partition::Error::Segments(SegmentError::OffsetOutOfRange))
    ));
    assert_eq!(part.log_start_offset(), 0);
    let policy = RetentionPolicy::new(None, Some(0), 8)?;
    assert_eq!(
        part.apply_retention(2000, policy, DeletionGuard::new(4, 3)?)?
            .log_start_offset,
        3
    );
    assert_eq!(part.next_offset(), 5);
    assert_eq!(part.fetch(3, 2, 4096)?.len(), 2);
    drop(part);
    let (mut part, recovery) =
        partition::Partition::open_segmented(&path, 0, source, records::Limits::default(), limits)?;
    assert_eq!(recovery.log_start_offset, 3);
    assert_eq!(part.fetch(3, 2, 4096)?.len(), 2);
    Ok(())
}

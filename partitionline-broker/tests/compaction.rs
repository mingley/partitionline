//! Actual-file ordinary cleaner replay against independently executed Apache.
use partitionline_broker::{compaction, journal, partition, records, segments};
use std::{
    fs,
    io::Read,
    path::{Path, PathBuf},
    sync::atomic::{AtomicU64, Ordering},
};
type Result = std::result::Result<(), Box<dyn std::error::Error>>;
struct Temp(PathBuf);
impl Temp {
    fn new() -> std::io::Result<Self> {
        static N: AtomicU64 = AtomicU64::new(0);
        let p = std::env::temp_dir().join(format!(
            "partitionline-compaction-{}-{}",
            std::process::id(),
            N.fetch_add(1, Ordering::Relaxed)
        ));
        fs::create_dir(&p)?;
        Ok(Self(p))
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
fn fixture(release: &str, name: &str) -> std::io::Result<Vec<u8>> {
    let p = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures/records-compacted")
        .join(release)
        .join(format!("{name}.bin"));
    if fs::metadata(&p)?.len() > 4096 {
        return Err(std::io::Error::other("fixture bound"));
    }
    read_file(&p)
}
fn read_file(path: &Path) -> std::io::Result<Vec<u8>> {
    let mut bytes = Vec::new();
    fs::File::open(path)?
        .take(1_048_577)
        .read_to_end(&mut bytes)?;
    if bytes.len() > 1_048_576 {
        return Err(std::io::Error::other("test file bound"));
    }
    Ok(bytes)
}
fn open(path: &Path, roll: u64) -> std::result::Result<partition::Partition, partition::Error> {
    partition::Partition::open_segmented(
        path,
        0,
        journal::Limits::new(1024, 8192, 32, 4096)?,
        records::Limits::default(),
        segments::Limits::new(roll, 16, 16, 2, 1024 * 1024, 128 * 1024, 4096)?,
    )
    .map(|(p, _)| p)
}
fn policy() -> std::result::Result<compaction::Policy, compaction::Error> {
    compaction::Policy::new(1000, compaction::Limits::default())
}
fn guard(end: i64) -> std::result::Result<segments::DeletionGuard, segments::Error> {
    segments::DeletionGuard::new(end, end)
}
fn snapshot(path: &Path) -> std::io::Result<Vec<(String, Vec<u8>)>> {
    let mut out = Vec::new();
    for item in fs::read_dir(path)? {
        let item = item?;
        out.push((
            item.file_name().to_string_lossy().into_owned(),
            read_file(&item.path())?,
        ));
    }
    out.sort();
    Ok(out)
}

// These are actual selected files, not a reconstruction from the test verdict.
// An opt-in proof run owns a fresh output directory; normal tests write nothing.
#[allow(clippy::too_many_arguments)]
fn capture_corpus(
    path: &Path,
    release: &str,
    scenario: &str,
    clock: i64,
    phase: &str,
    expected_fixture: &str,
    active_fixture: &str,
    log: &partition::Partition,
    outcome: &compaction::Outcome,
) -> std::io::Result<()> {
    let Some(root) = std::env::var_os("PARTITIONLINE_COMPACTION_CORPUS_DIR") else {
        return Ok(());
    };
    let target = PathBuf::from(root)
        .join(release)
        .join(scenario)
        .join(clock.to_string())
        .join(phase);
    fs::create_dir_all(
        target
            .parent()
            .ok_or_else(|| std::io::Error::other("corpus parent"))?,
    )?;
    fs::create_dir(&target)?;
    let mut files = Vec::new();
    for item in fs::read_dir(path)? {
        let item = item?;
        if files.len() == 64 || !item.file_type()?.is_file() {
            return Err(std::io::Error::other("corpus file/count bound"));
        }
        files.push(item.path());
    }
    files.sort();
    let mut bytes = 0usize;
    for file in files {
        let payload = read_file(&file)?;
        bytes = bytes
            .checked_add(payload.len())
            .filter(|n| *n <= 4 * 1_048_576)
            .ok_or_else(|| std::io::Error::other("corpus aggregate bound"))?;
        let name = file
            .file_name()
            .ok_or_else(|| std::io::Error::other("corpus file name"))?;
        let mut output = fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(target.join(name))?;
        std::io::Write::write_all(&mut output, &payload)?;
        output.sync_all()?;
    }
    let mut receipt = fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(target.join("case.json"))?;
    std::io::Write::write_all(
        &mut receipt,
        format!(
            "{{\"schema\":1,\"release\":\"{release}\",\"scenario\":\"{scenario}\",\"clock_ms\":{clock},\"phase\":\"{phase}\",\"expected_fixture\":\"{expected_fixture}\",\"active_fixture\":\"{active_fixture}\",\"logical_floor\":{},\"logical_end\":{},\"outcome\":{{\"start_offset\":{},\"end_offset\":{},\"scanned_records\":{},\"retained_records\":{},\"rewritten_segments\":{},\"bytes_before\":{},\"bytes_after\":{}}}}}\n",
            log.log_start_offset(), log.next_offset(), outcome.start_offset,
            outcome.end_offset, outcome.scanned_records, outcome.retained_records,
            outcome.rewritten_segments, outcome.bytes_before, outcome.bytes_after,
        )
        .as_bytes(),
    )?;
    receipt.sync_all()
}

#[test]
fn genuine_apache_cleaner_horizons_exact_bytes_offsets_and_restart() -> Result {
    for release in ["4.1.2", "4.2.1", "4.3.1"] {
        let temp = Temp::new()?;
        let path = temp.log();
        let mut log = open(&path, 300)?;
        log.append(&fixture(release, "cleaner-mixed-input")?)?;
        let active = fixture(release, "cleaner-mixed-active-protected")?;
        log.append(&active)?;
        assert_eq!(log.next_offset(), 10);
        let outcome = log.compact(2000, policy()?, guard(10)?)?;
        assert_eq!(
            (
                outcome.start_offset,
                outcome.end_offset,
                outcome.scanned_records,
                outcome.retained_records
            ),
            (0, 8, 8, 4)
        );
        assert_eq!(outcome.rewritten_segments, 1);
        assert_eq!(
            log.fetch(0, 1, 4096)?[0].payload,
            fixture(release, "cleaner-first-horizon-sparse")?
        );
        assert_eq!(log.fetch(8, 1, 4096)?[0].payload, active);
        assert_eq!((log.log_start_offset(), log.next_offset()), (0, 10));
        capture_corpus(
            &path,
            release,
            "mixed",
            2000,
            "selected",
            "cleaner-first-horizon-sparse",
            "cleaner-mixed-active-protected",
            &log,
            &outcome,
        )?;
        drop(log);
        let mut log = open(&path, 300)?;
        capture_corpus(
            &path,
            release,
            "mixed",
            2000,
            "reopened",
            "cleaner-first-horizon-sparse",
            "cleaner-mixed-active-protected",
            &log,
            &outcome,
        )?;
        for (clock, name, count) in [
            (2999, "cleaner-before-horizon-sparse", 4),
            (3000, "cleaner-equal-horizon-sparse", 2),
            (3001, "cleaner-after-horizon-sparse", 2),
        ] {
            let out = log.compact(clock, policy()?, guard(10)?)?;
            assert_eq!(out.retained_records, count);
            for offset in 0..8 {
                assert_eq!(
                    log.fetch(offset, 1, 4096)?[0].payload,
                    fixture(release, name)?
                );
            }
            assert_eq!(log.fetch(8, 1, 4096)?[0].payload, active);
            capture_corpus(
                &path,
                release,
                "mixed",
                clock,
                "selected",
                name,
                "cleaner-mixed-active-protected",
                &log,
                &out,
            )?;
            drop(log);
            log = open(&path, 300)?;
            capture_corpus(
                &path,
                release,
                "mixed",
                clock,
                "reopened",
                name,
                "cleaner-mixed-active-protected",
                &log,
                &out,
            )?;
        }
        let first = log.fetch(0, 1, 4096)?;
        log.replace_sealed(0)?;
        assert_eq!(log.fetch(0, 1, 4096)?, first);
        drop(log);
        let mut log = open(&path, 300)?;
        assert_eq!(log.fetch(0, 1, 4096)?, first);
        log.delete_records(3, guard(10)?)?;
        assert_eq!(log.log_start_offset(), 3);
        assert!(log.fetch(2, 1, 4096).is_err());
        assert_eq!(log.fetch(3, 1, 4096)?[0].payload, first[0].payload);
        drop(log);
        let mut log = open(&path, 300)?;
        assert_eq!(log.log_start_offset(), 3);
        assert_eq!(log.fetch(8, 1, 4096)?[0].payload, active);
    }
    Ok(())
}

#[test]
fn genuine_empty_last_batch_and_deleted_intermediate_batch_preserve_logical_span() -> Result {
    for release in ["4.1.2", "4.2.1", "4.3.1"] {
        for (input, active, first, last, end) in [
            (
                "cleaner-allremoved-input",
                "cleaner-allremoved-active-protected",
                "cleaner-intermediate-empty-dropped",
                "cleaner-last-empty-61",
                4,
            ),
            (
                "cleaner-nullonly-input",
                "cleaner-nullonly-active-protected",
                "cleaner-nullonly-empty-61",
                "cleaner-nullonly-empty-61",
                3,
            ),
        ] {
            let temp = Temp::new()?;
            let path = temp.log();
            let mut log = open(&path, 57)?;
            let scenario = if end == 4 { "allremoved" } else { "nullonly" };
            let active_name = active;
            let input = fixture(release, input)?;
            let active = fixture(release, active_name)?;
            let span = records::validate(&input, records::Limits::default())?.record_count() as u32;
            log.append(&input)?;
            log.append(&active)?;
            for (clock, expected) in [(2000, first), (3000, last)] {
                let outcome = log.compact(clock, policy()?, guard(end)?)?;
                let entry = &log.fetch(0, 1, 4096)?[0];
                assert_eq!(entry.record_count, span);
                assert_eq!(entry.payload, fixture(release, expected)?);
                assert_eq!(log.next_offset(), end);
                capture_corpus(
                    &path,
                    release,
                    scenario,
                    clock,
                    "selected",
                    expected,
                    active_name,
                    &log,
                    &outcome,
                )?;
                drop(log);
                log = open(&path, 57)?;
                capture_corpus(
                    &path,
                    release,
                    scenario,
                    clock,
                    "reopened",
                    expected,
                    active_name,
                    &log,
                    &outcome,
                )?;
            }
            log.append(include_bytes!("fixtures/records/valid-basic.bin"))?;
            assert_eq!(log.next_offset(), end + 1);
        }
    }
    Ok(())
}

#[test]
fn independently_protected_sealed_and_active_suffixes_never_enter_key_map() -> Result {
    for release in ["4.1.2", "4.2.1", "4.3.1"] {
        let temp = Temp::new()?;
        let path = temp.log();
        let mut log = open(&path, 57)?;
        log.append(&fixture(release, "cleaner-protected-sealed-input")?)?;
        let sealed = fixture(release, "cleaner-protected-sealed-suffix")?;
        let active = fixture(release, "cleaner-protected-active-suffix")?;
        log.append(&sealed)?;
        log.append(&active)?;
        log.compact(2000, policy()?, segments::DeletionGuard::new(12, 8)?)?;
        assert_eq!(
            log.fetch(0, 1, 4096)?[0].payload,
            fixture(release, "cleaner-protected-sealed-first-sparse")?
        );
        assert_eq!(log.fetch(8, 1, 4096)?[0].payload, sealed);
        assert_eq!(log.fetch(10, 1, 4096)?[0].payload, active);
        drop(log);
        let mut log = open(&path, 57)?;
        assert_eq!(log.fetch(8, 1, 4096)?[0].payload, sealed);
        assert_eq!(log.fetch(10, 1, 4096)?[0].payload, active);
    }
    Ok(())
}

#[test]
fn every_round_budget_and_invalid_clock_fails_before_any_file_change() -> Result {
    let temp = Temp::new()?;
    let path = temp.log();
    let mut log = open(&path, 57)?;
    log.append(&fixture("4.3.1", "cleaner-mixed-input")?)?;
    log.append(&fixture("4.3.1", "cleaner-mixed-active-protected")?)?;
    let before = snapshot(&path)?;
    for (limits, expected) in [
        (
            [
                1,
                65536,
                262144,
                65536,
                4 * 1024 * 1024,
                1000000,
                64,
                64 * 1024 * 1024,
            ],
            compaction::Budget::ScanBytes,
        ),
        (
            [
                128 * 1024 * 1024,
                1,
                262144,
                65536,
                4 * 1024 * 1024,
                1000000,
                64,
                64 * 1024 * 1024,
            ],
            compaction::Budget::ScanEntries,
        ),
        (
            [
                128 * 1024 * 1024,
                65536,
                1,
                65536,
                4 * 1024 * 1024,
                1000000,
                64,
                64 * 1024 * 1024,
            ],
            compaction::Budget::Records,
        ),
        (
            [
                128 * 1024 * 1024,
                65536,
                262144,
                1,
                4 * 1024 * 1024,
                1000000,
                64,
                64 * 1024 * 1024,
            ],
            compaction::Budget::Keys,
        ),
        (
            [
                128 * 1024 * 1024,
                65536,
                262144,
                65536,
                1,
                1000000,
                64,
                64 * 1024 * 1024,
            ],
            compaction::Budget::KeyBytes,
        ),
        (
            [
                128 * 1024 * 1024,
                65536,
                262144,
                65536,
                4 * 1024 * 1024,
                1,
                64,
                64 * 1024 * 1024,
            ],
            compaction::Budget::Probes,
        ),
        (
            [
                128 * 1024 * 1024,
                65536,
                262144,
                65536,
                4 * 1024 * 1024,
                1000000,
                64,
                65536,
            ],
            compaction::Budget::Scratch,
        ),
    ] {
        let configured = compaction::Limits::new(
            limits[0] as u64,
            limits[1],
            limits[2],
            limits[3],
            limits[4],
            limits[5],
            limits[6],
            limits[7],
        )?;
        let result = log.compact(2000, compaction::Policy::new(1000, configured)?, guard(10)?);
        assert!(
            matches!(result,Err(partition::Error::Segments(segments::Error::Compaction(compaction::Error::BudgetExceeded(b)))) if b==expected),
            "{expected:?}: {result:?}"
        );
        assert_eq!(snapshot(&path)?, before);
        assert!(!log.is_poisoned());
    }
    for clock in [-1, i64::MAX] {
        assert!(log.compact(clock, policy()?, guard(10)?).is_err());
        assert_eq!(snapshot(&path)?, before);
    }
    Ok(())
}

#[test]
fn monolithic_storage_and_strict_sparse_admission_remain_unchanged() -> Result {
    let temp = Temp::new()?;
    let (mut flat, _) = partition::Partition::open(
        temp.0.join("flat"),
        0,
        journal::Limits::default(),
        records::Limits::default(),
    )?;
    flat.append(include_bytes!("fixtures/records/valid-basic.bin"))?;
    assert!(matches!(
        flat.compact(2000, policy()?, guard(1)?),
        Err(partition::Error::CompactionUnsupported)
    ));
    assert_eq!(flat.next_offset(), 1);
    let mut rolled = open(&temp.log(), 57)?;
    for name in [
        "cleaner-first-horizon-sparse",
        "cleaner-last-empty-61",
        "filter-delete-empty-output",
    ] {
        assert!(rolled.append(&fixture("4.3.1", name)?).is_err());
        assert_eq!(rolled.next_offset(), 0);
    }
    Ok(())
}

#[test]
fn zero_horizon_window_never_resurrects_old_values_and_segment_cap_is_atomic() -> Result {
    let temp = Temp::new()?;
    let path = temp.log();
    let mut log = open(&path, 57)?;
    log.append(&fixture("4.3.1", "cleaner-mixed-input")?)?;
    log.append(&fixture("4.3.1", "cleaner-protected-sealed-suffix")?)?;
    log.append(&fixture("4.3.1", "cleaner-protected-active-suffix")?)?;
    let before = snapshot(&path)?;
    let caps = compaction::Limits::new(
        128 * 1024 * 1024,
        65536,
        262144,
        65536,
        4 * 1024 * 1024,
        1000000,
        1,
        64 * 1024 * 1024,
    )?;
    assert!(matches!(
        log.compact(2000, compaction::Policy::new(0, caps)?, guard(12)?),
        Err(partition::Error::Segments(segments::Error::Compaction(
            compaction::Error::BudgetExceeded(compaction::Budget::Segments)
        )))
    ));
    assert_eq!(snapshot(&path)?, before);
    assert!(!log.is_poisoned());
    let zero = compaction::Policy::new(0, compaction::Limits::default())?;
    let protected = segments::DeletionGuard::new(12, 8)?;
    assert_eq!(log.compact(2000, zero, protected)?.retained_records, 4);
    assert_eq!(log.compact(2000, zero, protected)?.retained_records, 2);
    let payload = &log.fetch(0, 1, 4096)?[0].payload;
    let checked = records::validate_read(payload, records::Limits::default())?;
    let mut offsets = Vec::new();
    for batch in checked.batches() {
        for record in batch?.records() {
            offsets.push(record?.offset);
        }
    }
    assert_eq!(offsets, [2, 5]);
    assert_eq!((log.log_start_offset(), log.next_offset()), (0, 12));
    Ok(())
}

#[test]
fn horizon_varint_growth_exceeds_entry_ceiling_before_creating_a_generation() -> Result {
    let temp = Temp::new()?;
    let path = temp.log();
    let all = fixture("4.3.1", "cleaner-allremoved-input")?;
    let checked = records::validate(&all, records::Limits::default())?;
    let tombstone = checked.batches().last().ok_or("last batch")??.bytes;
    let cap = tombstone.len();
    let (mut log, _) = partition::Partition::open_segmented(
        &path,
        0,
        journal::Limits::new(cap, 8192, 32, 4096)?,
        records::Limits::default(),
        segments::Limits::new(57, 16, 16, 2, 1024 * 1024, 128 * 1024, 4096)?,
    )?;
    log.append(tombstone)?;
    log.append(include_bytes!("fixtures/records/valid-basic.bin"))?;
    let before = snapshot(&path)?;
    assert!(matches!(
        log.compact(1_i64 << 50, policy()?, guard(2)?),
        Err(partition::Error::Segments(segments::Error::Compaction(
            compaction::Error::BudgetExceeded(compaction::Budget::OutputBytes)
        )))
    ));
    assert_eq!(snapshot(&path)?, before);
    assert!(!log.is_poisoned());
    assert_eq!(log.next_offset(), 2);
    Ok(())
}

#[test]
fn corrupted_sealed_source_poison_preserves_selected_manifest_and_confirmed_offsets() -> Result {
    let temp = Temp::new()?;
    let path = temp.log();
    let mut log = open(&path, 57)?;
    log.append(&fixture("4.3.1", "cleaner-mixed-input")?)?;
    log.append(&fixture("4.3.1", "cleaner-mixed-active-protected")?)?;
    let manifest = read_file(&path.join("manifest"))?;
    let file = path.join("0000000000000000-0000000000000000.journal");
    let mut bytes = read_file(&file)?;
    *bytes.last_mut().ok_or("payload")? ^= 1;
    use std::io::Write;
    fs::File::create(&file)?.write_all(&bytes)?;
    assert!(log.compact(2000, policy()?, guard(10)?).is_err());
    assert!(log.is_poisoned());
    assert_eq!((log.log_start_offset(), log.next_offset()), (0, 10));
    assert_eq!(read_file(&path.join("manifest"))?, manifest);
    assert!(log.fetch(0, 1, 4096).is_err());
    assert!(log
        .append(include_bytes!("fixtures/records/valid-basic.bin"))
        .is_err());
    drop(log);
    assert!(open(&path, 57).is_err());
    Ok(())
}

#[test]
fn carried_empty_batch_maximum_allows_guarded_age_reclamation_without_timestamp_records() -> Result
{
    let temp = Temp::new()?;
    let path = temp.log();
    let mut log = open(&path, 57)?;
    log.append(&fixture("4.3.1", "cleaner-allremoved-input")?)?;
    log.append(&fixture("4.3.1", "cleaner-allremoved-active-protected")?)?;
    log.compact(2000, policy()?, guard(4)?)?;
    log.compact(3000, policy()?, guard(4)?)?;
    let entry = &log.fetch(0, 1, 4096)?[0];
    let read = records::validate_read(&entry.payload, records::Limits::default())?;
    assert_eq!(read.record_count(), 0);
    assert_eq!(
        read.batches().next().ok_or("empty header")??.max_timestamp,
        1002
    );
    let sweep = segments::RetentionPolicy::new(Some(1000), None, 1)?;
    let protected = segments::DeletionGuard::new(4, 3)?;
    // Equality still retains the segment; strict age removes it one tick later.
    assert_eq!(
        log.apply_retention(2002, sweep, protected)?.reclaimed_files,
        0
    );
    assert_eq!(
        log.apply_retention(2003, sweep, protected)?
            .log_start_offset,
        3
    );
    assert_eq!((log.log_start_offset(), log.next_offset()), (3, 4));
    assert_eq!(
        log.fetch(3, 1, 4096)?[0].payload,
        fixture("4.3.1", "cleaner-allremoved-active-protected")?
    );
    drop(log);
    let mut log = open(&path, 57)?;
    assert_eq!((log.log_start_offset(), log.next_offset()), (3, 4));
    assert!(log.fetch(2, 1, 4096).is_err());
    Ok(())
}

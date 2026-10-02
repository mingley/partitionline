//! Topic history, malformed metadata, budgets, crash and journal-recovery cases.

use partitionline_broker::catalog::{Catalog, Corruption, Error, Limits, TopicId};
use partitionline_broker::journal::{self, Journal};
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

struct Temp(PathBuf);
impl Temp {
    #[allow(
        clippy::unwrap_used,
        reason = "Fixed synchronous test fixture setup; failure must fail the test."
    )]
    fn new() -> Self {
        static NEXT: AtomicU64 = AtomicU64::new(0);
        let root = std::env::var_os("PL_CATALOG_TEST_TMP")
            .map(PathBuf::from)
            .unwrap_or_else(std::env::temp_dir);
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();
        let path = root.join(format!(
            "partitionline-catalog-{}-{nonce}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        fs::create_dir(&path).unwrap();
        Self(path)
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
#[allow(
    clippy::unwrap_used,
    reason = "Fixed synchronous test fixture setup; failure must fail the test."
)]
fn id(value: u128) -> TopicId {
    TopicId::new(value.to_be_bytes()).unwrap()
}
#[allow(
    clippy::unwrap_used,
    reason = "Fixed synchronous test fixture setup; failure must fail the test."
)]
fn bounds(
    topics: usize,
    identities: usize,
    per_topic: u32,
    partitions: u64,
    operations: usize,
    bytes: u64,
    file: u64,
) -> Limits {
    Limits::new(
        topics,
        identities,
        per_topic,
        partitions,
        operations,
        bytes,
        journal::Limits::new(1024, file, 1024, 4096).unwrap(),
    )
    .unwrap()
}
#[allow(
    clippy::unwrap_used,
    reason = "Fixed synchronous test fixture setup; failure must fail the test."
)]
fn persisted(path: &Path, entries: &[(u32, Vec<u8>)]) {
    let (mut journal, _) = Journal::open(path, 0, journal::Limits::default()).unwrap();
    for (count, entry) in entries {
        journal.append(*count, entry).unwrap();
    }
}
fn read(path: &Path) -> std::io::Result<Vec<u8>> {
    let mut bytes = Vec::new();
    File::open(path)?.read_to_end(&mut bytes)?;
    Ok(bytes)
}
fn operation(create: bool, identity: u128, name: &str, partitions: u32) -> Vec<u8> {
    let mut payload = Vec::from(&b"PLTCAT01"[..]);
    payload.extend_from_slice(&[if create { 1 } else { 2 }, 0, 0, 0]);
    payload.extend_from_slice(&identity.to_be_bytes());
    payload.extend_from_slice(&partitions.to_be_bytes());
    payload.extend_from_slice(&(name.len() as u16).to_be_bytes());
    payload.extend_from_slice(name.as_bytes());
    payload
}
#[allow(
    clippy::unwrap_used,
    reason = "Fixed synchronous test fixture setup; failure must fail the test."
)]
fn write(path: &Path, bytes: &[u8]) {
    let mut file = OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .open(path)
        .unwrap();
    file.write_all(bytes).unwrap();
    file.sync_all().unwrap();
}

#[test]
fn atomic_create_delete_tombstone_and_name_recreation_survive_restart() {
    let temp = Temp::new();
    let path = temp.path("catalog");
    let (mut catalog, initial) = Catalog::open(&path, Limits::default()).unwrap();
    assert!(initial.initialized);
    let first = catalog.create("alpha", id(2), 3).unwrap();
    assert_eq!(
        (first.first_offset, first.next_offset, first.record_count),
        (0, 1, 1)
    );
    catalog.create("beta", id(3), 2).unwrap();
    catalog.delete(id(2)).unwrap();
    assert!(catalog.by_name("alpha").is_none());
    assert!(catalog.by_id(id(2)).is_none());
    assert!(catalog.is_tombstoned(id(2)));
    assert!(matches!(
        catalog.create("other", id(2), 1),
        Err(Error::DuplicateIdentity)
    ));
    catalog.create("alpha", id(4), 6).unwrap();
    assert_eq!(
        (
            catalog.topic_count(),
            catalog.identity_count(),
            catalog.total_partitions(),
            catalog.operation_count()
        ),
        (2, 3, 8, 4)
    );
    assert_eq!(catalog.journal_bytes(), fs::metadata(&path).unwrap().len());
    let expected_bytes = catalog.replay_bytes();
    if let Some(output) = std::env::var_os("PL_CATALOG_FORMAT_OUTPUT") {
        fs::copy(&path, output).unwrap();
    }
    drop(catalog);
    let (catalog, recovered) = Catalog::open(&path, Limits::default()).unwrap();
    assert_eq!(
        (recovered.recovered_entries, recovered.truncated_bytes),
        (4, 0)
    );
    assert_eq!(
        (
            catalog.topic_count(),
            catalog.identity_count(),
            catalog.total_partitions(),
            catalog.operation_count()
        ),
        (2, 3, 8, 4)
    );
    assert_eq!(catalog.replay_bytes(), expected_bytes);
    assert_eq!(catalog.by_name("alpha").unwrap().id(), id(4));
    assert_eq!(catalog.by_id(id(3)).unwrap().partition_count(), 2);
    assert_eq!(
        catalog
            .topics()
            .map(|topic| topic.name())
            .collect::<Vec<_>>(),
        ["beta", "alpha"]
    );
    assert!(catalog.is_tombstoned(id(2)));
}

#[test]
fn independent_apache_name_vectors_match_and_reserved_ids_are_rejected() {
    let vectors = include_str!("../../docs/evidence/broker/KL11-59/topic-name-cases.tsv");
    let mut tested = 0;
    for line in vectors.lines().filter(|line| !line.starts_with('#')) {
        let fields: Vec<_> = line.split('\t').collect();
        let bytes: Vec<_> = fields[0]
            .as_bytes()
            .chunks_exact(2)
            .map(|pair| {
                let hex = std::str::from_utf8(pair).unwrap();
                u8::from_str_radix(hex, 16).unwrap()
            })
            .collect();
        let name = std::str::from_utf8(&bytes).unwrap();
        let valid = partitionline_broker::catalog::validate_topic_name(name).is_ok();
        assert_eq!(valid.to_string(), fields[1], "name hex {}", fields[0]);
        tested += 1;
    }
    assert_eq!(tested, 29);
    assert!(matches!(
        TopicId::new(0u128.to_be_bytes()),
        Err(Error::ReservedIdentity)
    ));
    assert!(matches!(
        TopicId::new(1u128.to_be_bytes()),
        Err(Error::ReservedIdentity)
    ));
    assert_eq!(
        TopicId::new(u128::MAX.to_be_bytes()).unwrap().bytes(),
        [255; 16]
    );
}

#[test]
fn invalid_names_partitions_duplicate_names_ids_and_collisions_do_not_mutate() {
    let temp = Temp::new();
    let path = temp.path("catalog");
    let (mut catalog, _) = Catalog::open(&path, Limits::default()).unwrap();
    catalog.create("topic.name", id(2), 3).unwrap();
    let original = read(&path).unwrap();
    for invalid in ["", ".", "..", "../escape", "a/b", "a\\b", "é", "a\0b"] {
        assert!(matches!(
            catalog.create(invalid, id(3), 1),
            Err(Error::InvalidName)
        ));
    }
    assert!(matches!(
        catalog.create("__cluster_metadata", id(3), 1),
        Err(Error::ReservedName)
    ));
    assert!(matches!(
        catalog.create("other", id(3), 0),
        Err(Error::InvalidPartitionCount)
    ));
    assert!(matches!(
        catalog.create("other", id(3), u32::MAX),
        Err(Error::InvalidPartitionCount)
    ));
    assert!(matches!(
        catalog.create("topic.name", id(3), 1),
        Err(Error::DuplicateName)
    ));
    assert!(matches!(
        catalog.create("other", id(2), 1),
        Err(Error::DuplicateIdentity)
    ));
    assert!(matches!(
        catalog.create("topic_name", id(3), 1),
        Err(Error::NameCollision)
    ));
    assert!(matches!(catalog.delete(id(9)), Err(Error::UnknownIdentity)));
    assert_eq!(read(&path).unwrap(), original);
    assert_eq!(
        (
            catalog.topic_count(),
            catalog.identity_count(),
            catalog.total_partitions(),
            catalog.operation_count()
        ),
        (1, 1, 3, 1)
    );
    assert!(!catalog.is_poisoned());
    catalog.create("TOPIC.name", id(3), 1).unwrap();
    catalog.create("__consumer_offsets", id(4), 1).unwrap();
    catalog.create("part..name", id(5), 1).unwrap();
    assert!(!temp.path("escape").exists());
}

#[test]
fn deleted_collision_name_is_released_but_identity_is_not() {
    let temp = Temp::new();
    let (mut catalog, _) = Catalog::open(temp.path("catalog"), Limits::default()).unwrap();
    catalog.create("topic.name", id(2), 3).unwrap();
    catalog.delete(id(2)).unwrap();
    assert!(matches!(catalog.delete(id(2)), Err(Error::UnknownIdentity)));
    assert!(matches!(
        catalog.create("topic_name", id(2), 1),
        Err(Error::DuplicateIdentity)
    ));
    catalog.create("topic_name", id(3), 1).unwrap();
    assert_eq!(
        (
            catalog.topic_count(),
            catalog.identity_count(),
            catalog.total_partitions()
        ),
        (1, 2, 1)
    );
}

#[test]
fn topic_identity_partition_operation_and_replay_budgets_hold_before_append() {
    let temp = Temp::new();
    let cases = [
        ("topics", bounds(1, 3, 3, 9, 9, 1024, 4096)),
        ("identities", bounds(1, 1, 3, 9, 9, 1024, 4096)),
        ("per-topic", bounds(3, 3, 1, 9, 9, 1024, 4096)),
        ("partitions", bounds(3, 3, 3, 1, 9, 1024, 4096)),
        ("operations", bounds(3, 3, 3, 9, 1, 1024, 4096)),
        ("replay", bounds(3, 3, 3, 9, 9, 35, 4096)),
    ];
    for (name, limits) in cases {
        let path = temp.path(name);
        let (mut catalog, _) = Catalog::open(&path, limits).unwrap();
        catalog.create("a", id(2), 1).unwrap();
        if name == "identities" {
            catalog.delete(id(2)).unwrap();
        }
        let original = read(&path).unwrap();
        let error = catalog
            .create("b", id(3), if name == "per-topic" { 2 } else { 1 })
            .unwrap_err();
        assert!(matches!(
            (name, error),
            ("topics", Error::TopicBudgetExceeded)
                | ("identities", Error::IdentityBudgetExceeded)
                | ("per-topic" | "partitions", Error::PartitionBudgetExceeded)
                | ("operations", Error::OperationBudgetExceeded)
                | ("replay", Error::ReplayBudgetExceeded)
        ));
        assert_eq!(read(&path).unwrap(), original, "budget {name}");
        assert!(!catalog.is_poisoned());
        drop(catalog);
        Catalog::open(&path, limits).unwrap();
    }
}

#[test]
fn deletes_count_against_operation_replay_and_journal_budgets() {
    let temp = Temp::new();
    for (name, limits) in [
        ("operations", bounds(3, 3, 3, 9, 1, 1024, 4096)),
        ("replay", bounds(3, 3, 3, 9, 9, 35, 4096)),
        ("file", bounds(3, 3, 3, 9, 9, 1024, 91)),
    ] {
        let path = temp.path(name);
        let (mut catalog, _) = Catalog::open(&path, limits).unwrap();
        catalog.create("a", id(2), 1).unwrap();
        let original = read(&path).unwrap();
        let error = catalog.delete(id(2)).unwrap_err();
        assert!(matches!(
            (name, error),
            ("operations", Error::OperationBudgetExceeded)
                | ("replay", Error::ReplayBudgetExceeded)
                | ("file", Error::Journal(journal::Error::FileBudgetExceeded))
        ));
        assert!(catalog.by_id(id(2)).is_some());
        assert!(!catalog.is_tombstoned(id(2)));
        assert_eq!(read(&path).unwrap(), original);
        assert!(!catalog.is_poisoned());
    }
}

#[test]
fn stricter_restart_budgets_fail_without_rewriting_complete_history() {
    let temp = Temp::new();
    let path = temp.path("catalog");
    let (mut catalog, _) = Catalog::open(&path, Limits::default()).unwrap();
    catalog.create("a", id(2), 2).unwrap();
    catalog.create("b", id(3), 2).unwrap();
    drop(catalog);
    let original = read(&path).unwrap();
    for limits in [
        bounds(1, 2, 3, 9, 9, 1024, 4096),
        bounds(2, 2, 1, 9, 9, 1024, 4096),
        bounds(2, 2, 3, 3, 9, 1024, 4096),
        bounds(2, 2, 3, 9, 9, 35, 4096),
        bounds(2, 2, 3, 9, 1, 1024, 4096),
        bounds(2, 2, 3, 9, 9, 1024, 91),
    ] {
        assert!(Catalog::open(&path, limits).is_err());
        assert_eq!(read(&path).unwrap(), original);
    }
    let (catalog, _) = Catalog::open(&path, Limits::default()).unwrap();
    assert_eq!((catalog.topic_count(), catalog.total_partitions()), (2, 4));
}

#[test]
fn every_torn_tombstone_tail_prefix_recovers_whole_previous_catalog() {
    let temp = Temp::new();
    let source = temp.path("source");
    let (mut catalog, _) = Catalog::open(&source, Limits::default()).unwrap();
    catalog.create("alpha", id(2), 3).unwrap();
    let first_end = catalog.journal_bytes() as usize;
    catalog.delete(id(2)).unwrap();
    drop(catalog);
    let complete = read(&source).unwrap();
    for cut in 1..complete.len() - first_end {
        let path = temp.path(&format!("tail-{cut}"));
        write(&path, &complete[..first_end + cut]);
        let (mut catalog, recovery) = Catalog::open(&path, Limits::default()).unwrap();
        assert_eq!(recovery.truncated_bytes, cut as u64);
        assert_eq!(catalog.by_name("alpha").unwrap().id(), id(2));
        assert!(!catalog.is_tombstoned(id(2)));
        assert_eq!(
            (catalog.total_partitions(), catalog.operation_count()),
            (3, 1)
        );
        assert_eq!(read(&path).unwrap(), complete[..first_end]);
        catalog.delete(id(2)).unwrap();
        drop(catalog);
        let (catalog, recovery) = Catalog::open(&path, Limits::default()).unwrap();
        assert_eq!(recovery.truncated_bytes, 0);
        assert_eq!(catalog.topic_count(), 0);
        assert!(catalog.is_tombstoned(id(2)));
    }
}

#[test]
fn complete_journal_corruption_and_interior_byte_loss_fail_closed() {
    let temp = Temp::new();
    let source = temp.path("source");
    let (mut catalog, _) = Catalog::open(&source, Limits::default()).unwrap();
    catalog.create(&"a".repeat(249), id(2), 3).unwrap();
    catalog.create("later", id(3), 1).unwrap();
    drop(catalog);
    let original = read(&source).unwrap();
    let mut changed = original.clone();
    changed[24 + 32 + 34] ^= 1;
    let path = temp.path("checksum");
    write(&path, &changed);
    assert!(matches!(
        Catalog::open(&path, Limits::default()),
        Err(Error::Journal(journal::Error::Corrupt {
            kind: journal::Corruption::PayloadChecksum,
            ..
        }))
    ));
    assert_eq!(read(&path).unwrap(), changed);
    let mut interior = original[..24 + 32 + 1].to_vec();
    interior.extend_from_slice(&original[24 + 32 + 283..]);
    let path = temp.path("interior");
    write(&path, &interior);
    assert!(matches!(
        Catalog::open(&path, Limits::default()),
        Err(Error::Journal(journal::Error::Corrupt {
            kind: journal::Corruption::InteriorTail,
            ..
        }))
    ));
    assert_eq!(read(&path).unwrap(), interior);
}

#[test]
fn checksummed_malformed_operations_and_conflicting_histories_fail_closed() {
    let temp = Temp::new();
    let valid = operation(true, 2, "alpha", 1);
    let mut cases = vec![
        (vec![(2, valid.clone())], Corruption::RecordCount),
        (
            vec![(1, operation(true, 0, "alpha", 1))],
            Corruption::Identity,
        ),
        (
            vec![(1, operation(true, 1, "alpha", 1))],
            Corruption::Identity,
        ),
        (
            vec![(1, operation(true, 2, "../alpha", 1))],
            Corruption::Name,
        ),
        (
            vec![(1, operation(true, 2, "__cluster_metadata", 1))],
            Corruption::Name,
        ),
        (
            vec![(1, operation(true, 2, "alpha", 0))],
            Corruption::Partitions,
        ),
        (
            vec![(1, operation(true, 2, "alpha", u32::MAX))],
            Corruption::Partitions,
        ),
        (
            vec![(1, valid.clone()), (1, operation(true, 3, "alpha", 1))],
            Corruption::DuplicateName,
        ),
        (
            vec![(1, valid.clone()), (1, operation(true, 2, "beta", 1))],
            Corruption::DuplicateIdentity,
        ),
        (
            vec![
                (1, operation(true, 2, "a.b", 1)),
                (1, operation(true, 3, "a_b", 1)),
            ],
            Corruption::NameCollision,
        ),
        (
            vec![(1, operation(false, 2, "", 0))],
            Corruption::UnknownIdentity,
        ),
        (
            vec![
                (1, valid.clone()),
                (1, operation(false, 2, "", 0)),
                (1, operation(false, 2, "", 0)),
            ],
            Corruption::UnknownIdentity,
        ),
        (
            vec![
                (1, valid.clone()),
                (1, operation(false, 2, "", 0)),
                (1, operation(true, 2, "alpha", 1)),
            ],
            Corruption::DuplicateIdentity,
        ),
        (vec![(1, operation(false, 2, "x", 0))], Corruption::Format),
        (vec![(1, operation(false, 2, "", 1))], Corruption::Format),
    ];
    for field in [0, 8, 9, 32, 34] {
        let mut changed = valid.clone();
        changed[field] = if field == 34 { 255 } else { 9 };
        cases.push((vec![(1, changed)], Corruption::Format));
    }
    cases.push((vec![(1, valid[..33].to_vec())], Corruption::Format));
    for (index, (entries, expected)) in cases.into_iter().enumerate() {
        let path = temp.path(&format!("case-{index}"));
        persisted(&path, &entries);
        let original = read(&path).unwrap();
        assert!(
            matches!(Catalog::open(&path, Limits::default()), Err(Error::Corrupt { kind, .. }) if kind == expected),
            "case {index}"
        );
        assert_eq!(read(&path).unwrap(), original);
    }
}

#[test]
fn oversized_persisted_entry_and_duplicate_owner_are_rejected() {
    let temp = Temp::new();
    let path = temp.path("large");
    persisted(&path, &[(1, vec![0; 284])]);
    let original = read(&path).unwrap();
    assert!(matches!(
        Catalog::open(&path, Limits::default()),
        Err(Error::Journal(journal::Error::EntryTooLarge))
    ));
    assert_eq!(read(&path).unwrap(), original);
    let path = temp.path("owned");
    let (catalog, _) = Catalog::open(&path, Limits::default()).unwrap();
    assert!(matches!(
        Catalog::open(&path, Limits::default()),
        Err(Error::Journal(journal::Error::AlreadyOpen))
    ));
    assert!(matches!(
        Journal::open(&path, 0, journal::Limits::default()),
        Err(journal::Error::AlreadyOpen)
    ));
    drop(catalog);
    Catalog::open(&path, Limits::default()).unwrap();
}

#[test]
fn changed_file_poisoning_keeps_confirmed_state_and_recovery_repairs_tail() {
    let temp = Temp::new();
    let path = temp.path("catalog");
    let (mut catalog, _) = Catalog::open(&path, Limits::default()).unwrap();
    catalog.create("alpha", id(2), 3).unwrap();
    OpenOptions::new()
        .append(true)
        .open(&path)
        .unwrap()
        .write_all(b"PLENT")
        .unwrap();
    assert!(matches!(
        catalog.create("beta", id(3), 1),
        Err(Error::Journal(journal::Error::ChangedFile))
    ));
    assert!(catalog.is_poisoned());
    assert!(catalog.by_name("beta").is_none());
    assert_eq!(
        (
            catalog.topic_count(),
            catalog.identity_count(),
            catalog.total_partitions(),
            catalog.operation_count()
        ),
        (1, 1, 3, 1)
    );
    assert!(matches!(catalog.delete(id(2)), Err(Error::Poisoned)));
    drop(catalog);
    let (mut catalog, recovery) = Catalog::open(&path, Limits::default()).unwrap();
    assert_eq!(recovery.truncated_bytes, 5);
    assert!(catalog.by_name("alpha").is_some());
    catalog.create("beta", id(3), 1).unwrap();
}

#[test]
fn process_exit_without_destructors_preserves_durable_history_and_reports_tails() {
    const CHILD_PATH: &str = "PL_CATALOG_CRASH_CHILD_PATH";
    const CHILD_MODE: &str = "PL_CATALOG_CRASH_CHILD_MODE";
    if let Some(path) = std::env::var_os(CHILD_PATH) {
        let mode = std::env::var(CHILD_MODE).unwrap();
        let (mut catalog, _) = Catalog::open(&path, Limits::default()).unwrap();
        catalog.create("alpha", id(2), 3).unwrap();
        catalog.delete(id(2)).unwrap();
        catalog.create("beta", id(3), 2).unwrap();
        if mode == "tail" {
            OpenOptions::new()
                .append(true)
                .open(&path)
                .unwrap()
                .write_all(b"PLENT")
                .unwrap();
        }
        std::process::exit(0);
    }
    let temp = Temp::new();
    for mode in ["durable", "tail"] {
        let path = temp.path(mode);
        let child = std::process::Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "process_exit_without_destructors_preserves_durable_history_and_reports_tails",
            ])
            .env(CHILD_PATH, &path)
            .env(CHILD_MODE, mode)
            .output()
            .unwrap();
        assert!(
            child.status.success(),
            "{}",
            String::from_utf8_lossy(&child.stderr)
        );
        let (catalog, recovery) = Catalog::open(&path, Limits::default()).unwrap();
        assert_eq!(recovery.truncated_bytes, if mode == "tail" { 5 } else { 0 });
        assert_eq!(
            (
                catalog.topic_count(),
                catalog.identity_count(),
                catalog.total_partitions(),
                catalog.operation_count()
            ),
            (1, 2, 2, 3)
        );
        assert!(catalog.is_tombstoned(id(2)));
        assert_eq!(catalog.by_name("beta").unwrap().id(), id(3));
    }
}

#[test]
fn invalid_limits_and_tightened_journal_bounds_are_explicit() {
    let journal = journal::Limits::default();
    for values in [
        (0, 1, 1, 1, 1, 35),
        (2, 1, 1, 1, 1, 35),
        (1, 1, 0, 1, 1, 35),
        (1, 1, u32::MAX, 1, 1, 35),
        (1, 1, 1, 0, 1, 35),
        (1, 1, 1, 1, 0, 35),
        (1, 1, 1, 1, 1, 34),
    ] {
        assert!(
            Limits::new(values.0, values.1, values.2, values.3, values.4, values.5, journal)
                .is_err()
        );
    }
    assert!(Limits::new(
        1,
        1,
        1,
        1,
        1,
        35,
        journal::Limits::new(282, 4096, 1, 4096).unwrap()
    )
    .is_err());
    assert!(Limits::new(
        1,
        1,
        1,
        1,
        1,
        35,
        journal::Limits::new(283, 4096, 1, 41).unwrap()
    )
    .is_err());
    let limits = bounds(2, 3, 4, 5, 6, 2048, 4096);
    assert_eq!(
        (
            limits.max_live_topics(),
            limits.max_identities(),
            limits.max_partitions_per_topic(),
            limits.max_total_partitions(),
            limits.max_operations(),
            limits.max_replay_bytes()
        ),
        (2, 3, 4, 5, 6, 2048)
    );
    assert_eq!(limits.journal_limits().max_entry_bytes(), 283);
    assert_eq!(limits.journal_limits().max_index_entries(), 6);
    assert!(limits.journal_limits().max_fetch_bytes() <= 323);
}

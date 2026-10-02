//! Apache-fixture admission, bounded parsing and journal composition checks.

use std::fs::{self, File};
use std::io::Read;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};

use partitionline_broker::journal::{Journal, Limits as JournalLimits};
use partitionline_broker::records::{validate, Budget, Error, Invalid, Limits, Unsupported};

const EXPECTATIONS: &str = include_str!("fixtures/records/expectations.tsv");
const BASIC: &[u8] = include_bytes!("fixtures/records/valid-basic.bin");
const RICH: &[u8] = include_bytes!("fixtures/records/valid-nulls-empty.bin");
const MULTIPLE: &[u8] = include_bytes!("fixtures/records/valid-multiple-batches.bin");
const DEFAULT: [usize; 8] = [
    16 * 1024 * 1024,
    1024 * 1024,
    1024,
    65536,
    1024 * 1024,
    1024 * 1024,
    1024,
    65536,
];

fn limits(v: [usize; 8]) -> Result<Limits, Error> {
    Limits::new(v[0], v[1], v[2], v[3], v[4], v[5], v[6], v[7])
}
fn fixture(id: &str) -> std::io::Result<Vec<u8>> {
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures/records")
        .join(format!("{id}.bin"));
    let file = File::open(path)?;
    if file.metadata()?.len() > 4096 {
        return Err(std::io::Error::other("fixture exceeds test input bound"));
    }
    let mut bytes = Vec::new();
    file.take(4097).read_to_end(&mut bytes)?;
    if bytes.len() > 4096 {
        return Err(std::io::Error::other("fixture grew beyond bound"));
    }
    Ok(bytes)
}
fn classification(
    result: Result<partitionline_broker::records::Validated<'_>, Error>,
) -> &'static str {
    match result {
        Ok(_) => "accepted",
        Err(Error::BudgetExceeded { budget, .. }) => match budget {
            Budget::InputBytes => "inputbudget",
            Budget::BatchBytes => "batchbudget",
            Budget::Batches => "batchesbudget",
            Budget::Records => "recordbudget",
            Budget::RecordBytes => "recordbytesbudget",
            Budget::FieldBytes => "fieldbudget",
            Budget::HeadersPerRecord => "headerbudget",
            Budget::Headers => "headersbudget",
        },
        Err(Error::Unsupported { feature, .. }) => match feature {
            Unsupported::LegacyMagic(0) => "legacy0",
            Unsupported::LegacyMagic(1) => "legacy1",
            Unsupported::Compression(1) => "compression1",
            Unsupported::Compression(2) => "compression2",
            Unsupported::Compression(3) => "compression3",
            Unsupported::Compression(4) => "compression4",
            Unsupported::Idempotent => "idempotent",
            Unsupported::Transactional => "transactional",
            Unsupported::Control => "control",
            Unsupported::DeleteHorizon => "deletehorizon",
            Unsupported::LogAppendTime => "logappend",
            _ => "unexpectedfeature",
        },
        Err(Error::Invalid { kind, .. }) => match kind {
            Invalid::Empty => "empty",
            Invalid::Truncated => "truncated",
            Invalid::Length => "length",
            Invalid::Magic => "magic",
            Invalid::Checksum => "checksum",
            Invalid::Attributes => "attributes",
            Invalid::Metadata => "metadata",
            Invalid::Offset => "offset",
            Invalid::Timestamp => "timestamp",
            Invalid::VarintOverflow => "varintoverflow",
            Invalid::NonCanonicalVarint => "noncanonical",
            Invalid::TrailingBytes => "trailing",
            Invalid::RecordAttributes => "recordattributes",
            Invalid::HeaderKey => "headerkey",
        },
        Err(Error::InvalidLimits) => "invalidlimits",
    }
}
fn crc(bytes: &mut [u8]) {
    let value = crc32c::crc32c(&bytes[21..]);
    bytes[17..21].copy_from_slice(&value.to_be_bytes());
}
fn minimal(body: &[u8], base_time: i64, max_time: i64) -> Vec<u8> {
    let mut bytes = BASIC[..61].to_vec();
    bytes[27..35].copy_from_slice(&base_time.to_be_bytes());
    bytes[35..43].copy_from_slice(&max_time.to_be_bytes());
    bytes.push((body.len() as u8) << 1);
    bytes.extend_from_slice(body);
    let length = (bytes.len() - 12) as i32;
    bytes[8..12].copy_from_slice(&length.to_be_bytes());
    crc(&mut bytes);
    bytes
}

#[test]
fn apache_generated_valid_features_and_hostile_fixtures_have_typed_outcomes() {
    let mut cases = 0;
    let mut accepted = 0;
    for row in EXPECTATIONS.lines() {
        let (id, expected) = row.split_once('\t').unwrap();
        let bytes = fixture(id).unwrap();
        assert_eq!(
            classification(validate(&bytes, Limits::default())),
            expected,
            "{id}"
        );
        cases += 1;
        accepted += usize::from(expected == "accepted");
    }
    assert_eq!(cases, 58);
    assert_eq!(accepted, 6);
}

#[test]
fn borrowed_projection_preserves_input_and_independent_batch_bases() {
    let checked = validate(MULTIPLE, Limits::default()).unwrap();
    assert_eq!(checked.as_bytes().as_ptr(), MULTIPLE.as_ptr());
    assert_eq!(checked.as_bytes().len(), MULTIPLE.len());
    assert_eq!(checked.batch_count(), 2);
    assert_eq!(checked.record_count(), 4);
    assert_eq!(checked.header_count(), 5);
    let mut batches = checked.batches();
    let first = batches.next().unwrap().unwrap();
    assert_eq!(first.bytes.as_ptr(), MULTIPLE.as_ptr());
    assert_eq!(first.bytes, BASIC);
    assert_eq!(first.base_offset, 0);
    assert_eq!(first.next_offset, 1);
    assert_eq!(first.record_count, 1);
    assert_eq!(first.max_timestamp, 1000);
    let second = batches.next().unwrap().unwrap();
    assert_eq!(second.bytes, RICH);
    assert_eq!(second.base_offset, 0);
    assert_eq!(second.next_offset, 3);
    assert_eq!(second.max_timestamp, 1007);
    assert!(batches.next().is_none());
    assert!(batches.next().is_none());
    assert!(std::mem::size_of_val(&checked) <= 5 * std::mem::size_of::<usize>());
    assert!(std::mem::size_of_val(&batches) <= 6 * std::mem::size_of::<usize>());
}

#[test]
fn every_truncation_and_incomplete_trailing_prefix_is_rejected() {
    for cut in 0..BASIC.len() {
        assert!(
            validate(&BASIC[..cut], Limits::default()).is_err(),
            "cut={cut}"
        );
    }
    for trailing in 1..12 {
        let mut bytes = BASIC.to_vec();
        bytes.resize(bytes.len() + trailing, 0);
        assert!(matches!(
            validate(&bytes, Limits::default()),
            Err(Error::Invalid {
                kind: Invalid::Truncated,
                ..
            })
        ));
    }
}

#[test]
fn each_protected_bit_flip_fails_checksum_before_record_parsing() {
    for at in 21..BASIC.len() {
        for bit in 0..8 {
            let mut bytes = BASIC.to_vec();
            bytes[at] ^= 1 << bit;
            assert_eq!(
                validate(&bytes, Limits::default()).unwrap_err(),
                Error::Invalid {
                    at: 17,
                    kind: Invalid::Checksum
                }
            );
        }
    }
}

#[test]
fn damaged_unsupported_batches_still_fail_checksum_first() {
    for id in [
        "feature-gzip",
        "feature-snappy",
        "feature-lz4",
        "feature-zstd",
        "feature-transactional",
        "feature-control",
        "feature-idempotent",
    ] {
        let mut bytes = fixture(id).unwrap();
        bytes[21] ^= 8;
        assert_eq!(
            validate(&bytes, Limits::default()).unwrap_err(),
            Error::Invalid {
                at: 17,
                kind: Invalid::Checksum
            }
        );
    }
}

#[test]
fn every_independent_byte_and_work_budget_is_checked() {
    for (index, maximum, bytes, expected) in [
        (0, BASIC.len() - 1, BASIC, Budget::InputBytes),
        (1, BASIC.len() - 1, BASIC, Budget::BatchBytes),
        (2, 1, MULTIPLE, Budget::Batches),
        (3, 1, RICH, Budget::Records),
        (4, 1, BASIC, Budget::RecordBytes),
        (5, 1, RICH, Budget::FieldBytes),
        (6, 1, RICH, Budget::HeadersPerRecord),
        (7, 1, MULTIPLE, Budget::Headers),
    ] {
        let mut values = DEFAULT;
        values[index] = maximum;
        let configured = limits(values).unwrap();
        assert!(
            matches!(validate(bytes, configured), Err(Error::BudgetExceeded { budget, .. }) if budget == expected),
            "budget={expected:?}"
        );
    }
}

#[test]
fn limits_are_positive_and_have_checked_hard_maxima() {
    for index in 0..8 {
        for value in [0, usize::MAX] {
            let mut values = DEFAULT;
            values[index] = value;
            assert_eq!(limits(values), Err(Error::InvalidLimits));
        }
    }
    assert!(limits([
        64 * 1024 * 1024,
        64 * 1024 * 1024,
        1_000_000,
        1_000_000,
        64 * 1024 * 1024,
        64 * 1024 * 1024,
        1_000_000,
        1_000_000
    ])
    .is_ok());
}

#[test]
fn exact_byte_and_count_limits_admit_the_complete_batch() {
    let checked = validate(
        BASIC,
        limits([BASIC.len(), BASIC.len(), 1, 1, BASIC.len() - 61, 1, 1, 1]).unwrap(),
    )
    .unwrap();
    assert_eq!(checked.record_count(), 1);
    assert_eq!(checked.header_count(), 1);
    assert_eq!(checked.as_bytes(), BASIC);
}

#[test]
fn signed_timestamp_varlong_boundaries_are_checked_without_wrapping() {
    let mut min_delta = vec![0];
    min_delta.extend_from_slice(&[255, 255, 255, 255, 255, 255, 255, 255, 255, 1]);
    min_delta.extend_from_slice(&[0, 1, 1, 0]);
    let bytes = minimal(&min_delta, i64::MAX, -1);
    assert_eq!(
        validate(&bytes, Limits::default()).unwrap().record_count(),
        1
    );
    let mut max_delta = vec![0];
    max_delta.extend_from_slice(&[254, 255, 255, 255, 255, 255, 255, 255, 255, 1]);
    max_delta.extend_from_slice(&[0, 1, 1, 0]);
    let bytes = minimal(&max_delta, i64::MIN, -1);
    assert!(validate(&bytes, Limits::default()).is_ok());
    let bytes = minimal(&[0, 1, 0, 1, 1, 0], i64::MIN, i64::MAX);
    assert!(matches!(
        validate(&bytes, Limits::default()),
        Err(Error::Invalid {
            kind: Invalid::Timestamp,
            ..
        })
    ));
}

#[test]
fn zero_header_records_are_accepted_and_header_keys_values_remain_nullable_correctly() {
    let bytes = minimal(&[0, 0, 0, 1, 1, 0], 0, 0);
    let checked = validate(&bytes, Limits::default()).unwrap();
    assert_eq!(checked.record_count(), 1);
    assert_eq!(checked.header_count(), 0);
    assert_eq!(validate(RICH, Limits::default()).unwrap().header_count(), 4);
    for id in [
        "bad-header-key-null",
        "bad-header-value-negative",
        "bad-header-negative-count",
    ] {
        assert!(matches!(
            validate(&fixture(id).unwrap(), Limits::default()),
            Err(Error::Invalid {
                kind: Invalid::Length,
                ..
            })
        ));
    }
}

#[test]
fn variable_fields_cannot_read_beyond_the_enclosing_record() {
    for body in [
        &[0, 128, 128, 128, 128, 128][..],
        &[0, 0, 0, 1, 1, 128][..],
        &[0, 0, 0, 2, 255, 1][..],
    ] {
        let bytes = minimal(body, 0, 0);
        assert!(validate(&bytes, Limits::default()).is_err());
    }
}

#[test]
fn rejected_admission_leaves_journal_state_and_validated_restart_bytes_intact() {
    static NONCE: AtomicU64 = AtomicU64::new(0);
    let temp_root = std::env::var_os("PL_RECORDS_TEST_TMP")
        .map(PathBuf::from)
        .unwrap_or_else(std::env::temp_dir);
    fs::create_dir_all(&temp_root).unwrap();
    let time = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let directory = temp_root.join(format!(
        "partitionline-records-{}-{time}-{}",
        std::process::id(),
        NONCE.fetch_add(1, Ordering::Relaxed)
    ));
    fs::create_dir(&directory).unwrap();
    let path = directory.join("trusted-journal");
    let (mut journal, _) = Journal::open(&path, 0, JournalLimits::default()).unwrap();
    let checked = validate(MULTIPLE, Limits::default()).unwrap();
    journal
        .append(
            u32::try_from(checked.record_count()).unwrap(),
            checked.as_bytes(),
        )
        .unwrap();
    for id in [
        "bad-crc",
        "bad-key-length-bomb",
        "bad-offset-overflow",
        "feature-gzip",
    ] {
        assert!(validate(&fixture(id).unwrap(), Limits::default()).is_err());
        assert_eq!(journal.entry_count(), 1);
        assert_eq!(journal.next_offset(), 4);
    }
    drop(journal);
    let (mut journal, recovery) = Journal::open(&path, 0, JournalLimits::default()).unwrap();
    assert_eq!(recovery.next_offset, 4);
    assert_eq!(recovery.truncated_bytes, 0);
    let entries = journal.fetch(0, 1, 4096).unwrap();
    assert_eq!(entries[0].payload, MULTIPLE);
    assert_eq!(
        validate(&entries[0].payload, Limits::default())
            .unwrap()
            .record_count(),
        4
    );
    drop(journal);
    fs::remove_dir_all(directory).unwrap();
}

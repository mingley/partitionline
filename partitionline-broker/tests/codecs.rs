//! Actual Apache/JNI histories, malformed streams and resource-budget checks.
#![cfg(feature = "codecs")]

use std::fs::File;
use std::io::Read;
use std::path::PathBuf;

use partitionline_broker::codecs::{normalize, Budget, Error, Limits};
use partitionline_broker::records;

const PLAIN: &[u8] = include_bytes!("fixtures/codecs/plain.bin");
const MULTIPLE: &[u8] = include_bytes!("fixtures/codecs/multiple.bin");
const MULTIPLE_PLAIN: &[u8] = include_bytes!("fixtures/codecs/multiple-plain.bin");
const TABLE: &str = include_str!("fixtures/codecs/expectations.tsv");
const DEFAULT: [usize; 9] = [
    16 * 1024 * 1024,
    1024 * 1024,
    1024,
    65536,
    1024 * 1024,
    16 * 1024 * 1024,
    4 * 1024 * 1024,
    32 * 1024 * 1024,
    65536,
];

fn limits(v: [usize; 9]) -> Result<Limits, Error> {
    Limits::new(v[0], v[1], v[2], v[3], v[4], v[5], v[6], v[7], v[8])
}
fn fixture(id: &str) -> std::io::Result<Vec<u8>> {
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures/codecs")
        .join(format!("{id}.bin"));
    let file = File::open(path)?;
    if file.metadata()?.len() > 65536 {
        return Err(std::io::Error::other("fixture exceeds input limit"));
    }
    let mut bytes = Vec::new();
    file.take(65537).read_to_end(&mut bytes)?;
    if bytes.len() > 65536 {
        return Err(std::io::Error::other("fixture grew beyond input limit"));
    }
    Ok(bytes)
}
fn checked(input: &[u8]) -> Result<partitionline_broker::codecs::Normalized<'_>, Error> {
    normalize(input, Limits::default(), records::Limits::default())
}
fn crc(bytes: &mut [u8]) {
    let crc = crc32c::crc32c(&bytes[21..]);
    bytes[17..21].copy_from_slice(&crc.to_be_bytes());
}
fn body(id: &str, payload: &[u8]) -> std::io::Result<Vec<u8>> {
    let mut b = fixture(id)?[..61].to_vec();
    b.extend_from_slice(payload);
    let length = i32::try_from(b.len() - 12).map_err(std::io::Error::other)?;
    b[8..12].copy_from_slice(&length.to_be_bytes());
    crc(&mut b);
    Ok(b)
}

#[test]
fn all_actual_apache_fixtures_have_the_declared_admission_outcome() {
    let mut total = 0;
    let mut accepted = 0;
    for row in TABLE.lines() {
        let (id, expected) = row.split_once('\t').unwrap();
        let b = fixture(id).unwrap();
        let result = checked(&b);
        assert_eq!(result.is_ok(), expected == "accepted", "{id}: {result:?}");
        accepted += usize::from(result.is_ok());
        total += 1;
    }
    assert_eq!((total, accepted), (44, 16));
}

#[test]
fn four_codecs_raw_snappy_and_concatenated_streams_preserve_exact_history_bytes() {
    for id in [
        "gzip",
        "snappy",
        "lz4",
        "zstd",
        "snappy-raw",
        "gzip-concatenated",
        "lz4-concatenated",
        "zstd-concatenated",
    ] {
        let original = fixture(id).unwrap();
        let result = checked(&original).unwrap();
        assert!(!result.is_borrowed(), "{id}");
        assert_eq!(result.as_bytes(), PLAIN, "{id}");
        assert_eq!(
            (
                result.batch_count(),
                result.record_count(),
                result.header_count()
            ),
            (1, 3, 4)
        );
        assert_eq!(&result.as_bytes()[..8], &original[..8]);
        assert_eq!(&result.as_bytes()[12..17], &original[12..17]);
        assert_eq!(&result.as_bytes()[23..61], &original[23..61]);
    }
    let result = checked(MULTIPLE).unwrap();
    assert_eq!(result.as_bytes(), MULTIPLE_PLAIN);
    assert_eq!(
        (
            result.batch_count(),
            result.record_count(),
            result.header_count()
        ),
        (5, 15, 20)
    );
}

#[test]
fn uncompressed_fast_path_borrows_and_still_validates_every_record() {
    let mut v = DEFAULT;
    v[7] = 1; // no codec/output workspace needed for a borrowed input
    let result = normalize(PLAIN, limits(v).unwrap(), records::Limits::default()).unwrap();
    assert!(result.is_borrowed());
    assert_eq!(result.as_bytes().as_ptr(), PLAIN.as_ptr());
    assert!(checked(&fixture("gzip-bad-record").unwrap()).is_err());
    let mut invalid = PLAIN.to_vec();
    invalid[61] = 0;
    crc(&mut invalid);
    assert!(checked(&invalid).is_err());
}

#[test]
fn every_encoded_truncation_and_trailing_incomplete_batch_is_rejected() {
    for id in ["gzip", "snappy", "lz4", "zstd"] {
        let b = fixture(id).unwrap();
        for at in 0..b.len() {
            assert!(checked(&b[..at]).is_err(), "{id} truncation {at}");
        }
        for at in 1..61 {
            let mut trailing = b.clone();
            trailing.extend_from_slice(&PLAIN[..at]);
            assert!(checked(&trailing).is_err(), "{id} trailing {at}");
        }
    }
}

#[test]
fn encoded_crc_and_count_checks_precede_bad_codec_streams() {
    for id in ["gzip", "snappy", "lz4", "zstd"] {
        let mut b = fixture(id).unwrap();
        b[61] ^= 255;
        assert!(matches!(
            checked(&b),
            Err(Error::Records(records::Error::Invalid {
                kind: records::Invalid::Checksum,
                ..
            }))
        ));
        b[57..61].copy_from_slice(&i32::MAX.to_be_bytes());
        crc(&mut b);
        assert_eq!(
            checked(&b).unwrap_err(),
            Error::BudgetExceeded(Budget::Records)
        );
    }
}

#[test]
fn each_normalization_budget_is_independent_and_positive() {
    let gzip = fixture("gzip").unwrap();
    for (index, bound, input, expected) in [
        (0, gzip.len() - 1, gzip.as_slice(), Budget::EncodedBytes),
        (1, gzip.len() - 1, gzip.as_slice(), Budget::BatchBytes),
        (2, 1, MULTIPLE, Budget::Batches),
        (3, 2, gzip.as_slice(), Budget::Records),
        (
            4,
            PLAIN.len() - 1,
            gzip.as_slice(),
            Budget::DecodedBatchBytes,
        ),
        (5, 60, gzip.as_slice(), Budget::NormalizedBytes),
        (6, 32767, gzip.as_slice(), Budget::WindowBytes),
        (7, DEFAULT[4], gzip.as_slice(), Budget::WorkspaceBytes),
        (
            8,
            1,
            fixture("gzip-concatenated").unwrap().as_slice(),
            Budget::Units,
        ),
    ] {
        let mut v = DEFAULT;
        v[index] = bound;
        assert_eq!(
            normalize(input, limits(v).unwrap(), records::Limits::default()).unwrap_err(),
            Error::BudgetExceeded(expected),
            "index {index}"
        );
    }
    for index in 0..9 {
        for value in [0, usize::MAX] {
            let mut v = DEFAULT;
            v[index] = value;
            assert_eq!(limits(v), Err(Error::InvalidLimits));
        }
    }
}

#[test]
fn exact_decoded_bounds_succeed_and_one_byte_less_rejects_all_codecs() {
    for id in ["gzip", "snappy", "lz4", "zstd"] {
        let b = fixture(id).unwrap();
        let mut v = DEFAULT;
        v[4] = PLAIN.len();
        v[5] = PLAIN.len();
        assert_eq!(
            normalize(&b, limits(v).unwrap(), records::Limits::default())
                .unwrap()
                .as_bytes(),
            PLAIN,
            "{id}"
        );
        v[4] -= 1;
        assert!(
            normalize(&b, limits(v).unwrap(), records::Limits::default()).is_err(),
            "{id}"
        );
    }
    let mut v = DEFAULT;
    v[5] = MULTIPLE_PLAIN.len() - 1;
    assert!(normalize(MULTIPLE, limits(v).unwrap(), records::Limits::default()).is_err());
}

#[test]
fn independent_expanding_streams_exercise_compressed_blocks_and_output_caps() {
    let plain = fixture("expanded-plain").unwrap();
    for id in ["gzip", "snappy", "lz4", "zstd"] {
        let b = fixture(&format!("{id}-expanded")).unwrap();
        assert!(b.len() < plain.len() / 4);
        assert_eq!(checked(&b).unwrap().as_bytes(), plain, "{id}");
        let mut v = DEFAULT;
        v[4] = plain.len() - 1;
        assert_eq!(
            normalize(&b, limits(v).unwrap(), records::Limits::default()).unwrap_err(),
            Error::BudgetExceeded(Budget::DecodedBatchBytes),
            "{id}"
        );
        v[4] = plain.len();
        v[5] = plain.len();
        assert_eq!(
            normalize(&b, limits(v).unwrap(), records::Limits::default())
                .unwrap()
                .as_bytes(),
            plain,
            "{id}"
        );
    }
}

#[test]
fn codec_units_are_aggregate_across_preflighted_frames_and_decoded_gzip_members() {
    let mut input = fixture("gzip-concatenated").unwrap();
    input.extend_from_slice(&fixture("zstd").unwrap());
    let mut v = DEFAULT;
    // JNI emits a data block and a final empty block: three Zstd units plus two gzip members.
    v[8] = 4;
    assert_eq!(
        normalize(&input, limits(v).unwrap(), records::Limits::default()).unwrap_err(),
        Error::BudgetExceeded(Budget::Units)
    );
    v[8] = 5;
    assert_eq!(
        normalize(&input, limits(v).unwrap(), records::Limits::default())
            .unwrap()
            .record_count(),
        6
    );
}

#[test]
fn decoded_records_keep_existing_field_header_and_semantic_rejections() {
    let headers = records::Limits::new(4096, 4096, 10, 100, 4096, 4096, 1, 100).unwrap();
    for id in ["gzip", "snappy", "lz4", "zstd"] {
        assert!(matches!(
            normalize(&fixture(id).unwrap(), Limits::default(), headers),
            Err(Error::Records(records::Error::BudgetExceeded {
                budget: records::Budget::HeadersPerRecord,
                ..
            }))
        ));
        for suffix in ["idempotent", "transactional", "bad-record"] {
            assert!(checked(&fixture(&format!("{id}-{suffix}")).unwrap()).is_err());
        }
    }
    // A malformed later batch cannot leave an admitted earlier batch visible.
    let mut input = fixture("gzip").unwrap();
    input.extend_from_slice(&fixture("zstd-bad-record").unwrap());
    assert!(checked(&input).is_err());
    assert_eq!(
        checked(&fixture("gzip").unwrap()).unwrap().as_bytes(),
        PLAIN
    );
}

#[test]
fn dictionary_skippable_legacy_reserved_and_optional_header_forms_are_rejected() {
    let mut gzip = fixture("gzip").unwrap();
    gzip[64] = 8;
    crc(&mut gzip);
    assert_eq!(checked(&gzip).unwrap_err(), Error::MalformedCodec);
    for id in ["lz4", "zstd"] {
        let skip = body(id, &[0x50, 0x2a, 0x4d, 0x18, 0, 0, 0, 0]).unwrap();
        assert_eq!(checked(&skip).unwrap_err(), Error::MalformedCodec);
    }
    let mut lz4 = fixture("lz4").unwrap();
    lz4[65] |= 1;
    crc(&mut lz4);
    assert_eq!(checked(&lz4).unwrap_err(), Error::MalformedCodec);
    let legacy = body("lz4", &[2, 33, 76, 24, 0, 0, 0, 0]).unwrap();
    assert_eq!(checked(&legacy).unwrap_err(), Error::MalformedCodec);
    let mut zstd = fixture("zstd").unwrap();
    zstd[65] |= 0x10;
    crc(&mut zstd);
    assert_eq!(checked(&zstd).unwrap_err(), Error::MalformedCodec);
    let dict = body("zstd", &[0x28, 0xb5, 0x2f, 0xfd, 0x21, 1, 0, 1, 0, 0]).unwrap();
    assert_eq!(checked(&dict).unwrap_err(), Error::MalformedCodec);
}

#[test]
fn advertised_window_and_content_size_bombs_fail_without_decoding() {
    let huge = body("zstd", &[0x28, 0xb5, 0x2f, 0xfd, 0, 0xf8, 1, 0, 0]).unwrap();
    assert_eq!(
        checked(&huge).unwrap_err(),
        Error::BudgetExceeded(Budget::WindowBytes)
    );
    let size = body(
        "zstd",
        &[
            0x28, 0xb5, 0x2f, 0xfd, 0xe0, 255, 255, 255, 255, 255, 255, 255, 127, 1, 0, 0,
        ],
    )
    .unwrap();
    assert_eq!(
        checked(&size).unwrap_err(),
        Error::BudgetExceeded(Budget::WindowBytes)
    );
    let mut lz4 = fixture("lz4").unwrap();
    lz4[66] = 0x70;
    crc(&mut lz4);
    let mut v = DEFAULT;
    v[6] = 65536;
    assert_eq!(
        normalize(&lz4, limits(v).unwrap(), records::Limits::default()).unwrap_err(),
        Error::BudgetExceeded(Budget::WindowBytes)
    );
    // Snappy raw block declares an enormous output without an enormous input.
    let bomb = body("snappy", &[255, 255, 255, 255, 7]).unwrap();
    assert_eq!(
        checked(&bomb).unwrap_err(),
        Error::BudgetExceeded(Budget::DecodedBatchBytes)
    );
}

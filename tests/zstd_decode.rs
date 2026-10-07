//! Independent Apache and librdkafka zstd record batches.
use partitionline::protocol::records::{decode_record_batches_with_limit, Compression};
use partitionline::Error;
use std::path::{Path, PathBuf};

fn fixtures() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/zstd-decode")
}
#[expect(
    clippy::disallowed_methods,
    clippy::expect_used,
    reason = "finite fixture I/O in synchronous tests"
)]
fn read(name: &str) -> Vec<u8> {
    std::fs::read(fixtures().join(name)).expect("read independently generated fixture")
}
#[expect(
    clippy::expect_used,
    reason = "finite fixture enumeration in synchronous tests"
)]
fn names(extension: &str) -> Vec<String> {
    let mut names = std::fs::read_dir(fixtures())
        .expect("fixture directory")
        .map(|x| x.expect("fixture entry").path())
        .filter(|p| p.extension().is_some_and(|e| e == extension))
        .map(|p| {
            p.file_name()
                .expect("fixture filename")
                .to_string_lossy()
                .into_owned()
        })
        .collect::<Vec<_>>();
    names.sort();
    names
}

#[cfg(not(feature = "zstd"))]
#[test]
fn disabled_feature_returns_typed_unsupported_for_real_peer_batches() {
    assert!(matches!(
        Compression::from_name("zstd"),
        Err(Error::Unsupported(_))
    ));
    assert!(Compression::from_id(4).is_none());
    for name in names("batch").into_iter().chain(names("native")) {
        if name == "java-none.batch" {
            continue;
        }
        assert!(
            matches!(
                decode_record_batches_with_limit(&mut &read(&name)[..], 1024 * 1024),
                Err(Error::Unsupported(_))
            ),
            "{name}"
        );
    }
}

#[cfg(feature = "zstd")]
#[test]
fn java_and_native_frames_preserve_records_headers_nulls_and_timestamps() -> Result<(), Error> {
    let plain = read("java-none.batch");
    let decoded_bytes = plain.len() - 61;
    let expected = decode_record_batches_with_limit(&mut &plain[..], decoded_bytes)?;
    assert_eq!(Compression::from_name("zstd")?, Compression::Zstd);
    assert_eq!(Compression::Zstd.id(), 4);
    for name in names("batch") {
        if name == "java-none.batch" {
            continue;
        }
        let bytes = read(&name);
        let mut actual = decode_record_batches_with_limit(&mut &bytes[..], decoded_bytes)?;
        for batch in &mut actual {
            batch.attributes &= !7;
        }
        assert_eq!(actual, expected, "{name}");
        assert!(
            decode_record_batches_with_limit(&mut &bytes[..], decoded_bytes - 1).is_err(),
            "{name}: exact cap minus one"
        );
        assert!(
            decode_record_batches_with_limit(&mut &bytes[..], 0).is_err(),
            "{name}: zero cap"
        );
    }
    Ok(())
}

#[cfg(feature = "zstd")]
#[test]
fn corrupt_and_over_limit_frames_fail_before_records_are_returned() {
    for name in names("bad") {
        let bytes = read(&name);
        assert!(
            decode_record_batches_with_limit(&mut &bytes[..], usize::MAX).is_err(),
            "{name}"
        );
    }
}

#[cfg(feature = "zstd")]
#[test]
fn actual_librdkafka_batches_contain_all_128_independently_verified_records() -> Result<(), Error> {
    fn mix(mut value: u64) -> u64 {
        value = value.wrapping_add(0x9e3779b97f4a7c15);
        value = (value ^ (value >> 30)).wrapping_mul(0xbf58476d1ce4e5b9);
        value = (value ^ (value >> 27)).wrapping_mul(0x94d049bb133111eb);
        value ^ (value >> 31)
    }
    let mut ids = std::collections::BTreeSet::new();
    for name in names("native") {
        for batch in decode_record_batches_with_limit(&mut &read(&name)[..], 1024 * 1024)? {
            assert_eq!(batch.attributes & 7, 4);
            for record in batch.records {
                let key = record
                    .key
                    .ok_or_else(|| Error::protocol("missing C producer key"))?;
                let id = u64::from_be_bytes(
                    key.get(..8)
                        .ok_or_else(|| Error::protocol("short C key"))?
                        .try_into()
                        .map_err(|_| Error::protocol("short C key"))?,
                );
                assert!(
                    id < 128 && ids.insert(id),
                    "out-of-range or duplicate C record ID"
                );
                let value = record
                    .value
                    .ok_or_else(|| Error::protocol("missing C value"))?;
                assert_eq!(value.len(), 100);
                assert_eq!(key.len(), 16);
                assert_eq!(key.get(8..), Some(&mix(1592590337 ^ id).to_be_bytes()[..]));
                let mut state = 1592590337 ^ id.wrapping_mul(0x9e3779b97f4a7c15);
                let mut expected = Vec::new();
                while expected.len() < 100 {
                    state = mix(state);
                    expected.extend_from_slice(&state.to_be_bytes());
                }
                expected.truncate(100);
                assert_eq!(&value[..], &expected);
            }
        }
    }
    assert_eq!(ids.len(), 128);
    Ok(())
}

#[cfg(feature = "zstd")]
#[test]
fn truncated_inner_frames_are_errors_with_valid_outer_crc() {
    let bytes = read("java-zstd.batch");
    let ends = (0..32)
        .chain((32..bytes.len() - 61).step_by(1021))
        .chain([bytes.len() - 62]);
    for end in ends {
        let mut truncated = bytes.get(..61 + end).unwrap_or_default().to_vec();
        if let Some(length) = truncated.get_mut(8..12) {
            length.copy_from_slice(&i32::try_from(49 + end).unwrap_or_default().to_be_bytes());
        }
        let crc = crc32c::crc32c(truncated.get(21..).unwrap_or_default());
        if let Some(field) = truncated.get_mut(17..21) {
            field.copy_from_slice(&crc.to_be_bytes());
        }
        assert!(
            decode_record_batches_with_limit(&mut &truncated[..], 1024 * 1024).is_err(),
            "truncated frame {end}"
        );
    }
}

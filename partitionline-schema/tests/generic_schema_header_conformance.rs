//! Independent Java vectors for the generic five-byte header contract.
use partitionline_schema::{decode, encode, DecodeError};
use std::path::{Path, PathBuf};

// These synchronous tests run outside an async runtime.
#[allow(clippy::disallowed_methods)]
fn fixture(name: &str) -> Vec<u8> {
    std::fs::read(
        Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../tests/fixtures/generic_schema_header")
            .join(format!("{name}.bin")),
    )
    .unwrap()
}
#[allow(clippy::disallowed_methods)]
fn emit(name: &str, frame: &[u8]) {
    if let Some(directory) = std::env::var_os("PL_GENERIC_HEADER_OUTPUT") {
        let directory = PathBuf::from(directory);
        std::fs::create_dir_all(&directory).unwrap();
        std::fs::write(directory.join(format!("{name}.bin")), frame).unwrap();
    }
}

#[test]
fn independently_encoded_boundaries_and_payloads_match_actual_helper_output() {
    let vectors: &[(&str, u32, &[u8])] = &[
        ("empty-zero", 0, &[]),
        ("empty-max", u32::MAX, &[]),
        ("one", 1, &[0]),
        ("byte-order", 0x01020304, &[0, 1, 2, 3, 4, 255]),
        ("signed-max", 0x7fffffff, b"ABC"),
        ("unsigned-high", 0x80000000, &[255, 0, 128]),
        ("max-payload", u32::MAX, &[0, 0, 0, 0, 0, 10, 13]),
    ];
    for &(name, id, payload) in vectors {
        let peer = fixture(name);
        let decoded = decode(&peer).unwrap();
        assert_eq!(decoded.schema_id, id);
        assert_eq!(decoded.payload, payload);
        assert!(
            std::ptr::eq(decoded.payload.as_ptr(), peer[5..].as_ptr()),
            "decode borrows the input payload"
        );
        let actual = encode(id, payload);
        assert_eq!(actual.len(), payload.len() + 5);
        assert_eq!(actual, peer, "{name}");
        emit(name, &actual);
    }
}

#[test]
fn independent_truncation_and_bad_magic_dispositions_have_the_same_precedence() {
    for length in 0..5 {
        let name = format!("truncated-{length}");
        assert_eq!(
            decode(&fixture(&name)),
            Err(DecodeError::Truncated { got: length })
        );
        let mut actual = encode(0, &[]);
        actual[0] = 255;
        actual.truncate(length);
        assert_eq!(actual, fixture(&name));
        emit(&name, &actual);
    }
    for magic in [1, 255] {
        let name = format!("bad-magic-{magic}");
        assert_eq!(
            decode(&fixture(&name)),
            Err(DecodeError::BadMagic { got: magic })
        );
        let mut actual = encode(0x01020304, &[]);
        actual[0] = magic;
        assert_eq!(actual, fixture(&name));
        emit(&name, &actual);
    }
}

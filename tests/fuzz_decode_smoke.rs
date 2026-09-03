//! Adversarial decode smoke (WP-2.1 / WP-2.2).
//!
//! Feeds structured and pseudo-random byte strings into hot decode paths.
//! Decoders must return `Ok` or `Err` — never panic. This is not libFuzzer;
//! CI runs it as a normal test. cargo-fuzz targets can wrap the same helpers
//! later.

#![allow(
    clippy::cast_possible_wrap,
    reason = "fuzz seeds intentionally reinterpret bits as signed integers"
)]

use partitionline::protocol::api::{decode_metadata_response, decode_produce_response};
use partitionline::protocol::buf::{
    get_compact_string, get_varint, get_varlong, put_compact_string, put_varint, put_varlong,
};
use partitionline::protocol::fetch::decode_fetch_response;
use partitionline::protocol::header::{decode_request_header, decode_response_header};
use partitionline::protocol::records::decode_record_batches;

/// Deterministic xorshift for reproducible corpus expansion.
fn xorshift(state: &mut u64) -> u64 {
    let mut x = *state;
    x ^= x << 13;
    x ^= x >> 7;
    x ^= x << 17;
    *state = x;
    x
}

fn blob(seed: u64, len: usize) -> Vec<u8> {
    let mut state = seed | 1;
    let mut out = Vec::with_capacity(len);
    while out.len() < len {
        let n = xorshift(&mut state);
        out.extend_from_slice(&n.to_le_bytes());
    }
    out.truncate(len);
    out
}

fn assert_no_panic_fetch(bytes: &[u8], version: i16) {
    let mut cur = bytes;
    drop(decode_fetch_response(&mut cur, version));
}

fn assert_no_panic_produce(bytes: &[u8], version: i16) {
    let mut cur = bytes;
    drop(decode_produce_response(&mut cur, version));
}

fn assert_no_panic_metadata(bytes: &[u8], version: i16) {
    let mut cur = bytes;
    drop(decode_metadata_response(&mut cur, version));
}

#[test]
fn empty_and_short_buffers_do_not_panic() {
    for version in [4_i16, 7, 11, 12, 16, 17] {
        assert_no_panic_fetch(&[], version);
        assert_no_panic_fetch(&[0, 0, 0], version);
    }
    for version in [3_i16, 8, 9, 12] {
        assert_no_panic_produce(&[], version);
        assert_no_panic_produce(&[0xff; 7], version);
    }
    for version in [1_i16, 8, 10, 12, 13] {
        assert_no_panic_metadata(&[], version);
        assert_no_panic_metadata(&[1, 2, 3, 4], version);
    }
    let mut empty = &[][..];
    drop(decode_request_header(&mut empty));
    let mut empty = &[][..];
    drop(decode_response_header(&mut empty, 1, 12));
    let mut empty = &[][..];
    drop(decode_record_batches(&mut empty));
}

#[test]
fn random_blobs_do_not_panic_hot_decoders() {
    for seed in 1_u64..64 {
        for len in [1, 8, 32, 128, 512, 2048] {
            let data = blob(seed.wrapping_mul(len as u64), len);
            for version in [4_i16, 11, 12, 16, 17] {
                assert_no_panic_fetch(&data, version);
            }
            for version in [3_i16, 9, 12] {
                assert_no_panic_produce(&data, version);
            }
            for version in [1_i16, 9, 12, 13] {
                assert_no_panic_metadata(&data, version);
            }
            let mut cur = data.as_slice();
            drop(decode_record_batches(&mut cur));
            let mut cur = data.as_slice();
            drop(decode_request_header(&mut cur));
            let mut cur = data.as_slice();
            drop(decode_response_header(&mut cur, 1, 12));
        }
    }
}

#[test]
fn varint_roundtrip_property() {
    let mut state = 0xC0FFEE_u64;
    for _ in 0..200 {
        let v = i32::from_ne_bytes(xorshift(&mut state).to_ne_bytes()[..4].try_into().unwrap());
        let mut buf = bytes::BytesMut::new();
        put_varint(&mut buf, v);
        let mut cur = buf.as_ref();
        assert_eq!(get_varint(&mut cur).expect("get_varint"), v);
        assert!(cur.is_empty());
    }
}

#[test]
fn varlong_roundtrip_property() {
    let mut state = 0xBADC0DE_u64;
    for _ in 0..200 {
        let v = i64::from_ne_bytes(xorshift(&mut state).to_ne_bytes());
        let mut buf = bytes::BytesMut::new();
        put_varlong(&mut buf, v);
        let mut cur = buf.as_ref();
        assert_eq!(get_varlong(&mut cur).expect("get_varlong"), v);
        assert!(cur.is_empty());
    }
}

#[test]
fn compact_string_roundtrip_property() {
    let cases = [
        None,
        Some(""),
        Some("a"),
        Some("partitionline"),
        Some("unicode-ok"),
    ];
    for s in cases {
        let mut buf = bytes::BytesMut::new();
        put_compact_string(&mut buf, s).expect("put");
        let mut cur = buf.as_ref();
        assert_eq!(get_compact_string(&mut cur).expect("get").as_deref(), s);
    }
}

#[test]
fn truncated_varint_is_error_not_panic() {
    // Continuation bit set, no following byte.
    let mut cur = &[0x80_u8][..];
    assert!(get_varint(&mut cur).is_err());
    let mut cur = &[0xff_u8, 0xff, 0xff, 0xff, 0xff][..];
    // Five continuation-style bytes may be illegal or short; must not panic.
    drop(get_varint(&mut cur));
}

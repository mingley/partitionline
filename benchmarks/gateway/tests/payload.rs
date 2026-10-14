use gateway::json1k::{header, value, HEADER_KEY, SEED, VALUE_BYTES};

#[test]
fn json1k_is_exact_kib_and_matches_codec() {
    let first = value(SEED, 0);
    assert_eq!(first.len(), VALUE_BYTES);
    assert!(first.starts_with(b"{"));
    assert_eq!(first, codec::json1k::value(codec::json1k::SEED, 0));
    assert_eq!(value(SEED, 7), codec::json1k::value(codec::json1k::SEED, 7));
    assert_ne!(value(SEED, 0), value(SEED, 1));
}

#[test]
fn header_is_eight_bytes() {
    assert_eq!(HEADER_KEY, "gw");
    assert_eq!(header(SEED, 0).len(), 8);
    assert_ne!(header(SEED, 0), header(SEED, 1));
}

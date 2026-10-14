//! Seeded 1 KiB JSON payloads, byte-identical with `benchmarks/codec` KL10-03.
//! This file is path-included by the isolated rust-rdkafka peer so both
//! clients emit the same values without sharing a Cargo graph.

/// KL10-03 payload seed.
pub const SEED: u64 = 0xC0DEC;
/// Exact ASCII JSON document size.
pub const VALUE_BYTES: usize = 1024;
/// Header key for the profile's single 8-byte header.
pub const HEADER_KEY: &str = "gw";

const WORDS: &[&str] = &[
    "the",
    "quick",
    "brown",
    "fox",
    "jumps",
    "over",
    "lazy",
    "dogs",
    "while",
    "kafka",
    "streams",
    "flow",
    "through",
    "partitions",
    "keyed",
    "by",
    "order",
    "id",
    "with",
    "exactly",
    "once",
    "semantics",
    "and",
    "compacted",
    "topics",
    "retaining",
    "the",
    "latest",
    "value",
    "per",
    "key",
];

/// Splitmix64 stream (matches the codec harness).
pub fn splitmix64(state: &mut u64) -> u64 {
    *state = state.wrapping_add(0x9E37_79B9_7F4A_7C15);
    let mut z = *state;
    z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    z ^ (z >> 31)
}

fn payload(seed: u64, entropy: &str, n: usize) -> Vec<u8> {
    let mut out = Vec::with_capacity(n);
    if entropy == "random" {
        let mut state = seed;
        while out.len() < n {
            out.extend_from_slice(&splitmix64(&mut state).to_be_bytes());
        }
        out.truncate(n);
        return out;
    }
    let mut state = seed;
    while out.len() < n {
        let word = WORDS[(splitmix64(&mut state) as usize) % WORDS.len()];
        out.extend_from_slice(word.as_bytes());
        out.push(b' ');
    }
    out.truncate(n);
    out
}

/// Valid JSON, exactly 1024 ASCII bytes, with seeded IDs, metrics and text.
pub fn value(seed: u64, index: usize) -> Vec<u8> {
    let mut state = seed ^ index as u64;
    let mut document = serde_json::json!({
        "id": format!("{:016x}", splitmix64(&mut state)),
        "tenant": format!("tenant-{}", splitmix64(&mut state) % 64),
        "sequence": index,
        "kind": "gateway-event",
        "timestamp": 1_700_000_000_000u64 + index as u64,
        "enabled": splitmix64(&mut state) & 1 == 0,
        "attributes": {"region": "eu", "source": "gateway"},
        "samples": [splitmix64(&mut state) % 1000, splitmix64(&mut state) % 1000],
        "message": "",
    });
    let overhead = serde_json::to_vec(&document).unwrap().len();
    let text = String::from_utf8(payload(state, "text", VALUE_BYTES - overhead)).unwrap();
    document["message"] = serde_json::Value::String(text);
    let value = serde_json::to_vec(&document).unwrap();
    assert_eq!(value.len(), VALUE_BYTES);
    value
}

/// Eight-byte header value derived from the same seed stream.
pub fn header(seed: u64, index: u64) -> [u8; 8] {
    let mut state = seed ^ index;
    splitmix64(&mut state).to_be_bytes()
}

//! Support for the KL04-09 wire-codec microbenchmarks: deterministic
//! fixtures, record builders, allocation census and the slow-transform
//! observability check.

use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

use bytes::{Bytes, BytesMut};
use partitionline::protocol::records::{
    decode_record_batch, encode_record_batch, Compression, Header, Record, RecordBatch,
};
use sha2::Digest;

pub mod json1k;

/// Zstd record-batch benchmark shapes and correctness preflight.
#[cfg(feature = "zstd")]
pub mod zstd;

/// Allocation baselines are recorded on x86_64, the CI gate host. zlib-rs
/// keeps a 64-byte PCLMULQDQ CRC fold accumulator in each stream state only on
/// x86_64, so other targets allocate this much less per zlib-rs stream.
pub const ZLIB_RS_STATE_BYTES_BELOW_X86_64: u64 =
    if cfg!(all(feature = "zlib-rs", not(target_arch = "x86_64"))) {
        64
    } else {
        0
    };

/// Splitmix64 stream (matches the repo's other deterministic harnesses).
pub fn splitmix64(state: &mut u64) -> u64 {
    *state = state.wrapping_add(0x9E37_79B9_7F4A_7C15);
    let mut z = *state;
    z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    z ^ (z >> 31)
}

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

/// Deterministic payload bytes: `random` is incompressible, `text` is a
/// compressible word stream. Both derive from `seed` only.
pub fn payload(seed: u64, entropy: &str, n: usize) -> Vec<u8> {
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

/// Build `count` deterministic records matching a fixture shape.
pub fn build_records(
    seed: u64,
    count: usize,
    key_bytes: usize,
    payload_bytes: usize,
    entropy: &str,
    header_count: usize,
) -> Vec<Record> {
    let mut records = Vec::with_capacity(count);
    for i in 0..count as u64 {
        let key = if key_bytes == 0 {
            None
        } else {
            Some(Bytes::from(payload(
                seed ^ i.wrapping_mul(0x9E37_79B9_7F4A_7C15),
                entropy,
                key_bytes,
            )))
        };
        let dom = 0xD1B5_4A32_243F_6A88u64;
        let value = Bytes::from(payload(seed ^ i ^ dom, entropy, payload_bytes));
        let mut headers = Vec::with_capacity(header_count);
        for h in 0..header_count {
            headers.push(Header::new(
                format!("h{h}"),
                Bytes::from(payload(seed ^ i ^ (h as u64), "text", 8)),
            ));
        }
        records.push(Record {
            offset: i as i64,
            timestamp: 1_700_000_000_000 + i as i64,
            key,
            value: Some(value),
            headers,
        });
    }
    records
}

/// One checked-in independent fixture: raw v2 bytes plus manifest facts.
#[derive(Debug, Clone)]
pub struct Fixture {
    pub name: String,
    pub bytes: Vec<u8>,
    pub records: usize,
    pub payload_bytes: usize,
    pub first_key_sha256: String,
    pub first_value_sha256: String,
    pub last_key_sha256: String,
    pub last_value_sha256: String,
}

/// Load every fixture named by `dir/manifest.json`.
pub fn load_fixtures(dir: &std::path::Path) -> Result<Vec<Fixture>, String> {
    let manifest_raw = std::fs::read_to_string(dir.join("manifest.json"))
        .map_err(|e| format!("read manifest.json: {e}"))?;
    let manifest: serde_json::Value =
        serde_json::from_str(&manifest_raw).map_err(|e| format!("parse manifest.json: {e}"))?;
    let entries = manifest
        .get("fixtures")
        .and_then(serde_json::Value::as_object)
        .ok_or_else(|| "manifest.json lacks fixtures object".to_owned())?;
    let mut names: Vec<&String> = entries.keys().collect();
    names.sort();
    let mut out = Vec::new();
    for name in names {
        let entry = &entries[name.as_str()];
        let get = |key: &str| {
            entry
                .get(key)
                .ok_or_else(|| format!("fixture {name} lacks {key}"))
        };
        let file = get("file")?
            .as_str()
            .ok_or_else(|| format!("fixture {name} file not a string"))?;
        let bytes = std::fs::read(dir.join(file)).map_err(|e| format!("read {file}: {e}"))?;
        let digest: [u8; 32] = sha2::Sha256::digest(&bytes).into();
        let actual = hex(&digest);
        let expected = get("sha256")?
            .as_str()
            .ok_or_else(|| format!("fixture {name} sha256 not a string"))?;
        if actual != expected {
            return Err(format!(
                "fixture {name} sha256 mismatch: {actual} != {expected}"
            ));
        }
        let usize_field = |key: &str| {
            get(key)?
                .as_u64()
                .ok_or_else(|| format!("fixture {name} {key} not a number"))
                .map(|v| v as usize)
        };
        let str_field = |key: &str| {
            get(key)?
                .as_str()
                .ok_or_else(|| format!("fixture {name} {key} not a string"))
                .map(str::to_owned)
        };
        out.push(Fixture {
            name: name.clone(),
            bytes,
            records: usize_field("records")?,
            payload_bytes: usize_field("payload_bytes")?,
            first_key_sha256: str_field("first_key_sha256")?,
            first_value_sha256: str_field("first_value_sha256")?,
            last_key_sha256: str_field("last_key_sha256")?,
            last_value_sha256: str_field("last_value_sha256")?,
        });
    }
    Ok(out)
}

fn hex(bytes: &[u8]) -> String {
    let mut out = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        out.push_str(&format!("{b:02x}"));
    }
    out
}

/// Decode a fixture, verifying manifest facts; returns the batch.
pub fn decode_fixture(fixture: &Fixture) -> Result<RecordBatch, String> {
    let mut buf = Bytes::from(fixture.bytes.clone());
    let batch =
        decode_record_batch(&mut buf).map_err(|e| format!("{} decode: {e}", fixture.name))?;
    if batch.records().len() != fixture.records {
        return Err(format!(
            "{} record count: {} != {}",
            fixture.name,
            batch.records().len(),
            fixture.records
        ));
    }
    let first = &batch.records()[0];
    let last = &batch.records()[fixture.records - 1];
    let key_hex = |key: &Option<Bytes>| {
        key.as_ref()
            .map(|k| hex(&sha2::Sha256::digest(k)))
            .unwrap_or_else(|| "null".to_owned())
    };
    let val_hex = |val: &Option<Bytes>| {
        val.as_ref()
            .map(|v| hex(&sha2::Sha256::digest(v)))
            .unwrap_or_else(|| "null".to_owned())
    };
    for (label, actual, expected) in [
        ("first key", key_hex(&first.key), &fixture.first_key_sha256),
        (
            "first value",
            val_hex(&first.value),
            &fixture.first_value_sha256,
        ),
        ("last key", key_hex(&last.key), &fixture.last_key_sha256),
        (
            "last value",
            val_hex(&last.value),
            &fixture.last_value_sha256,
        ),
    ] {
        if &actual != expected {
            return Err(format!("{} {label} sha mismatch", fixture.name));
        }
    }
    Ok(batch)
}

/// Re-encode a batch; the preflight requires byte-identical output for
/// fixtures (no silent normalization).
pub fn encode_batch(batch: &RecordBatch) -> Result<Vec<u8>, String> {
    let mut buf = BytesMut::new();
    encode_record_batch(&mut buf, batch).map_err(|e| format!("encode: {e}"))?;
    Ok(buf.to_vec())
}

/// Build a batch with `compression` from `records` (builder API used by
/// the producer path).
pub fn compressed_batch(records: Vec<Record>, compression: Compression) -> RecordBatch {
    RecordBatch::from_records(records).with_compression(compression)
}

/// CRC32C body of a v2 batch: everything after baseOffset(8) +
/// batchLength(4) + partitionLeaderEpoch(4) + magic(1) + crc(4).
pub fn crc_body(batch_bytes: &[u8]) -> &[u8] {
    &batch_bytes[21..]
}

/// Fast fixture transformation: decode once.
pub fn transform_fast(batch_bytes: &[u8]) -> Result<Vec<Record>, String> {
    let mut buf = Bytes::from(batch_bytes.to_vec());
    decode_record_batch(&mut buf)
        .map(|batch| batch.records().to_vec())
        .map_err(|e| format!("fast decode: {e}"))
}

/// Deliberately slower transformation with identical semantic output:
/// decode, rebuild the batch from scratch, re-encode, decode again.
pub fn transform_slow(batch_bytes: &[u8]) -> Result<Vec<Record>, String> {
    let mut buf = Bytes::from(batch_bytes.to_vec());
    let batch = decode_record_batch(&mut buf).map_err(|e| format!("slow decode 1: {e}"))?;
    let rebuilt = RecordBatch::from_records(batch.records().to_vec());
    let mut out = BytesMut::new();
    encode_record_batch(&mut out, &rebuilt).map_err(|e| format!("slow encode: {e}"))?;
    let mut back = out.freeze();
    decode_record_batch(&mut back)
        .map(|batch| batch.records().to_vec())
        .map_err(|e| format!("slow decode 2: {e}"))
}

/// Allocation census counts allocations (not frees) while enabled.
/// Single-threaded use: other threads' allocations would pollute counts.
pub struct CountingAlloc;

static CENSUS_ENABLED: AtomicBool = AtomicBool::new(false);
static CENSUS_ALLOCS: AtomicU64 = AtomicU64::new(0);
static CENSUS_BYTES: AtomicU64 = AtomicU64::new(0);

unsafe impl GlobalAlloc for CountingAlloc {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        if CENSUS_ENABLED.load(Ordering::Relaxed) {
            CENSUS_ALLOCS.fetch_add(1, Ordering::Relaxed);
            CENSUS_BYTES.fetch_add(layout.size() as u64, Ordering::Relaxed);
        }
        unsafe { System.alloc(layout) }
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        unsafe { System.dealloc(ptr, layout) }
    }
}

/// Run `f` with the census enabled; returns `(value, allocs, bytes)`.
pub fn census<R>(f: impl FnOnce() -> R) -> (R, u64, u64) {
    CENSUS_ALLOCS.store(0, Ordering::Relaxed);
    CENSUS_BYTES.store(0, Ordering::Relaxed);
    CENSUS_ENABLED.store(true, Ordering::Relaxed);
    let value = f();
    CENSUS_ENABLED.store(false, Ordering::Relaxed);
    (
        value,
        CENSUS_ALLOCS.load(Ordering::Relaxed),
        CENSUS_BYTES.load(Ordering::Relaxed),
    )
}

use std::env;
use std::hint::black_box;
use std::time::Instant;

use bytes::{Bytes, BytesMut};
use partitionline::protocol::records::{
    decode_record_batch, encode_record_batch, Compression, RecordBatch,
};
use serde_json::json;
use sha2::{Digest, Sha256};

struct Case {
    name: &'static str,
    records: usize,
    key_bytes: usize,
    value_bytes: usize,
    entropy: &'static str,
    headers: usize,
    compression: Compression,
    iterations: usize,
}

fn decode(wire: &Bytes) -> RecordBatch {
    let mut cursor = wire.clone();
    let batch = decode_record_batch(&mut cursor).expect("decode fixed fixture");
    assert!(cursor.is_empty());
    batch
}

fn time(case: &Case) {
    let expected = RecordBatch::from_records(codec::build_records(
        0xc0dec,
        case.records,
        case.key_bytes,
        case.value_bytes,
        case.entropy,
        case.headers,
    ))
    .with_compression(case.compression);

    let mut encoded = BytesMut::new();
    encode_record_batch(&mut encoded, &expected).expect("encode fixed fixture");
    let wire = encoded.freeze();
    assert_eq!(decode(&wire), expected);

    let digest = format!("{:x}", Sha256::digest(&wire));
    for _ in 0..(case.iterations / 8).max(10) {
        black_box(decode(black_box(&wire)));
    }
    let start = Instant::now();
    for _ in 0..case.iterations {
        black_box(decode(black_box(&wire)));
    }
    let elapsed_ns = u64::try_from(start.elapsed().as_nanos()).expect("bounded time");
    println!(
        "{}",
        json!({
            "case": case.name,
            "records_per_batch": case.records,
            "iterations": case.iterations,
            "wire_bytes_per_batch": wire.len(),
            "wire_sha256": digest,
            "elapsed_ns": elapsed_ns,
        })
    );
}

fn main() {
    let rotation: usize = env::args()
        .nth(1)
        .expect("case rotation")
        .parse()
        .expect("valid rotation");
    let standard = Case {
        name: "lz4-100-text",
        records: 500,
        key_bytes: 16,
        value_bytes: 100,
        entropy: "text",
        headers: 0,
        compression: Compression::Lz4,
        iterations: 2400,
    };
    let cases = [
        Case {
            name: "lz4-small",
            records: 32,
            key_bytes: 8,
            value_bytes: 24,
            iterations: 20000,
            ..standard
        },
        Case {
            name: "lz4-1024-h2-text",
            value_bytes: 1024,
            headers: 2,
            iterations: 350,
            ..standard
        },
        Case {
            name: "lz4-9000-random",
            records: 64,
            value_bytes: 9000,
            entropy: "random",
            iterations: 350,
            ..standard
        },
        Case {
            name: "lz4-64k-random",
            records: 32,
            value_bytes: 65536,
            entropy: "random",
            iterations: 100,
            ..standard
        },
        Case {
            name: "gzip-100",
            compression: Compression::Gzip,
            iterations: 1000,
            ..standard
        },
        Case {
            name: "none-100",
            compression: Compression::None,
            ..standard
        },
        Case {
            name: "snappy-100",
            compression: Compression::Snappy,
            ..standard
        },
        #[cfg(feature = "zstd")]
        Case {
            name: "zstd-100",
            compression: Compression::Zstd,
            iterations: 1000,
            ..standard
        },
        standard,
    ];
    for case in cases
        .iter()
        .cycle()
        .skip(rotation % cases.len())
        .take(cases.len())
    {
        time(case);
    }
}

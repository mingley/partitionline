use std::env;
use std::hint::black_box;
use std::time::Instant;

use bytes::{Bytes, BytesMut};
use partitionline::protocol::records::{
    decode_record_batch, write_record_batch_scratch, BatchHeader, CompressScratch, Compression,
    EncodeRecord, RecordBatch,
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

fn encode(
    output: &mut BytesMut,
    scratch: &mut CompressScratch,
    header: &BatchHeader,
    batch: &RecordBatch,
) {
    output.clear();
    write_record_batch_scratch(
        output,
        header,
        batch.records().iter().map(EncodeRecord::from_record),
        scratch,
    )
    .expect("encode deterministic batch");
    black_box(output.as_ref());
}

fn time(case: &Case) {
    let batch = RecordBatch::from_records(codec::build_records(
        0xb3ce_19e7,
        case.records,
        case.key_bytes,
        case.value_bytes,
        case.entropy,
        case.headers,
    ))
    .with_compression(case.compression);
    let header = BatchHeader {
        base_offset: batch.base_offset,
        partition_leader_epoch: batch.partition_leader_epoch,
        attributes: batch.attributes,
        base_timestamp: batch.base_timestamp,
        max_timestamp: batch.max_timestamp,
        producer_id: batch.producer_id,
        producer_epoch: batch.producer_epoch,
        base_sequence: batch.base_sequence,
        count: i32::try_from(case.records).expect("bounded batch"),
    };
    let mut output = BytesMut::new();
    let mut scratch = CompressScratch::new();
    encode(&mut output, &mut scratch, &header, &batch);
    let digest = format!("{:x}", Sha256::digest(&output));
    let mut wire = Bytes::copy_from_slice(&output);
    let decoded = decode_record_batch(&mut wire).expect("decode deterministic batch");
    assert_eq!(decoded, batch);
    assert!(wire.is_empty());

    for _ in 0..(case.iterations / 8).max(10) {
        encode(&mut output, &mut scratch, &header, &batch);
    }
    let start = Instant::now();
    for _ in 0..case.iterations {
        encode(
            black_box(&mut output),
            black_box(&mut scratch),
            black_box(&header),
            black_box(&batch),
        );
    }
    let elapsed_ns = u64::try_from(start.elapsed().as_nanos()).expect("bounded time");
    assert_eq!(digest, format!("{:x}", Sha256::digest(&output)));
    println!(
        "{}",
        json!({
            "case": case.name,
            "records_per_batch": case.records,
            "iterations": case.iterations,
            "wire_bytes_per_batch": output.len(),
            "wire_sha256": digest,
            "elapsed_ns": elapsed_ns,
        })
    );
}

fn main() {
    let rotation: usize = env::args().nth(1).expect("case rotation").parse().expect("valid rotation");
    let standard = Case {
        name: "none-100",
        records: 500,
        key_bytes: 16,
        value_bytes: 100,
        entropy: "random",
        headers: 0,
        compression: Compression::None,
        iterations: 5000,
    };
    let text = Case {
        name: "none-1024-h2",
        value_bytes: 1024,
        entropy: "text",
        headers: 2,
        iterations: 1000,
        ..standard
    };
    let cases = [
        Case {
            name: "none-tiny",
            records: 32,
            key_bytes: 8,
            value_bytes: 24,
            iterations: 70000,
            ..standard
        },
        Case {
            name: "none-large",
            records: 64,
            value_bytes: 9000,
            iterations: 1000,
            ..standard
        },
        Case {
            name: "gzip-100",
            entropy: "text",
            compression: Compression::Gzip,
            iterations: 180,
            ..standard
        },
        Case {
            name: "snappy-100",
            entropy: "text",
            compression: Compression::Snappy,
            iterations: 2000,
            ..standard
        },
        Case {
            name: "lz4-1024-h2",
            compression: Compression::Lz4,
            iterations: 180,
            ..text
        },
        #[cfg(feature = "zstd")]
        Case {
            name: "zstd-1024-h2",
            compression: Compression::Zstd,
            iterations: 180,
            ..text
        },
        standard,
        text,
    ];
    for case in cases.iter().cycle().skip(rotation % cases.len()).take(cases.len()) {
        time(case);
    }
}

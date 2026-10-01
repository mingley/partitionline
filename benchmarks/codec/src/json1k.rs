//! KL10-03 seeded 1 KiB JSON payloads and backend-only comparison inputs.
//! Batch sizes name value bytes; Kafka record metadata adds wire overhead.
use std::io::{Read, Write};

use bytes::{Bytes, BytesMut};
use partitionline::protocol::records::{encode_record_batch, Compression, Record, RecordBatch};

use crate::{payload, splitmix64};

pub const SEED: u64 = 0xC0DEC;
pub const VALUE_BYTES: usize = 1024;
pub const COUNTS: [usize; 2] = [16, 256];
pub const CODECS: [(&str, Compression); 3] = [
    ("gzip", Compression::Gzip),
    ("lz4", Compression::Lz4),
    ("snappy", Compression::Snappy),
];

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

pub fn batch(count: usize, compression: Compression) -> RecordBatch {
    let records = (0..count)
        .map(|i| Record {
            offset: i as i64,
            timestamp: 1_700_000_000_000 + i as i64,
            key: Some(Bytes::from(payload(SEED ^ i as u64, "random", 16))),
            value: Some(Bytes::from(value(SEED, i))),
            headers: Vec::new(),
        })
        .collect();
    RecordBatch::from_records(records).with_compression(compression)
}

pub fn wire(batch: &RecordBatch) -> Bytes {
    let mut output = BytesMut::new();
    encode_record_batch(&mut output, batch).unwrap();
    output.freeze()
}

/// Exact uncompressed Kafka record section; identical bytes enter each backend.
pub fn section(count: usize) -> Bytes {
    let wire = wire(&batch(count, Compression::None));
    wire.slice(RecordBatch::RECORD_BATCH_OVERHEAD as usize..)
}

/// One backend invocation over the prebuilt section, without Kafka record
/// serialization, CRC or batch header. Gzip/LZ4 match the client settings;
/// raw Snappy omits the 20-byte Xerial wrapper. Output allocation is included.
pub fn raw_compress(compression: Compression, input: &[u8]) -> Vec<u8> {
    match compression {
        Compression::Gzip => {
            let mut encoder =
                flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
            encoder.write_all(input).unwrap();
            encoder.finish().unwrap()
        }
        Compression::Lz4 => {
            use lz4_flex::frame::{BlockMode, BlockSize, FrameEncoder, FrameInfo};
            let info = FrameInfo::new()
                .block_mode(BlockMode::Independent)
                .block_size(BlockSize::Max64KB)
                .block_checksums(false)
                .content_checksum(false)
                .content_size(Some(input.len() as u64));
            let mut encoder = FrameEncoder::with_frame_info(info, Vec::new());
            encoder.write_all(input).unwrap();
            encoder.finish().unwrap()
        }
        Compression::Snappy => snap::raw::Encoder::new().compress_vec(input).unwrap(),
        Compression::None => input.to_vec(),
    }
}

/// Backend-only decode of these fixed, trusted fixtures. Unlike the public
/// client path, excludes Kafka parsing and bounded expansion enforcement.
pub fn raw_decompress(compression: Compression, input: &[u8]) -> Vec<u8> {
    let mut output = Vec::new();
    match compression {
        Compression::Gzip => {
            flate2::read::GzDecoder::new(input)
                .read_to_end(&mut output)
                .unwrap();
        }
        Compression::Lz4 => {
            lz4_flex::frame::FrameDecoder::new(input)
                .read_to_end(&mut output)
                .unwrap();
        }
        Compression::Snappy => return snap::raw::Decoder::new().decompress_vec(input).unwrap(),
        Compression::None => output.extend_from_slice(input),
    }
    output
}

/// Allocation and ratio census. Caller must install CountingAlloc and call
/// serially. Input construction/inspection stays outside each measured closure.
pub fn census_report() -> serde_json::Value {
    use crate::census;
    use partitionline::protocol::records::decode_record_batch;
    use sha2::{Digest, Sha256};
    use std::hint::black_box;
    let mut rows = Vec::new();
    for count in COUNTS {
        let input = section(count);
        let hash = format!("{:x}", Sha256::digest(&input));
        for (name, compression) in CODECS {
            let batch = batch(count, compression);
            let (encoded, ca, cb) = census(|| {
                let mut output = BytesMut::new();
                encode_record_batch(&mut output, black_box(&batch)).unwrap();
                black_box(output)
            });
            let compressed_bytes = encoded.len() - RecordBatch::RECORD_BATCH_OVERHEAD as usize;
            let mut encoded_input = encoded.freeze();
            let (decoded, da, db) =
                census(|| decode_record_batch(black_box(&mut encoded_input)).unwrap());
            assert_eq!(decoded.records(), batch.records());
            assert!(encoded_input.is_empty());
            let (packed, ra, rb) = census(|| raw_compress(compression, black_box(&input)));
            let (unpacked, rda, rdb) = census(|| raw_decompress(compression, black_box(&packed)));
            assert_eq!(unpacked, input);
            // Snappy raw framing differs by exactly the Xerial header/chunk prefix.
            assert_eq!(
                compressed_bytes,
                packed.len()
                    + if compression == Compression::Snappy {
                        20
                    } else {
                        0
                    }
            );
            for (operation, allocations, allocated_bytes, output_bytes) in [
                ("micro-compress", ca, cb, compressed_bytes),
                ("micro-decompress", da, db, compressed_bytes),
                ("raw-compress", ra, rb, packed.len()),
                ("raw-decompress", rda, rdb, packed.len()),
            ] {
                rows.push(serde_json::json!({
                    "cell": format!("{operation}:{name}:json1k/{count}k"),
                    "records": count, "value_bytes": count * VALUE_BYTES,
                    "section_bytes": input.len(), "section_sha256": hash,
                    "compressed_bytes": output_bytes,
                    "compressed_to_section_ratio": output_bytes as f64 / input.len() as f64,
                    "allocations": allocations, "allocated_bytes": allocated_bytes,
                }));
            }
        }
    }
    serde_json::json!({"seed": SEED, "value_bytes_per_record": VALUE_BYTES, "cells": rows})
}

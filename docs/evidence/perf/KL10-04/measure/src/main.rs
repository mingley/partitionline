//! Time the KL10-04 hypotheses without editing `records.rs` or `producer.rs`.
//!
//! Reconstructs gateway json1k records, the 1 MiB / linger packing bound, and
//! the same `GzEncoder::write_all` + `finish` sequence `gzip_compress_into`
//! uses. Compares that to Kafka encode, reused `CompressScratch`, 8 KiB
//! writes, and per-record writes.

use std::hint::black_box;
use std::io::Write;
use std::path::PathBuf;
use std::time::{Duration, Instant};

use bytes::{Bytes, BytesMut};
use partitionline::protocol::records::{
    encode_record_batch, write_record_batch_scratch, BatchHeader, CompressScratch, Compression,
    EncodeRecord, Header, Record, RecordBatch,
};
use serde::Serialize;

#[path = "../../../../../../benchmarks/gateway/src/json1k.rs"]
mod json1k;

const WARMUP: usize = 8;
const ITERS: usize = 40;
const BATCH_BYTES: usize = 1_048_576;
const BATCH_RECORDS: usize = 10_000;
const OVERHEAD: usize = RecordBatch::RECORD_BATCH_OVERHEAD as usize;

#[derive(Serialize)]
struct Timing {
    name: String,
    records: usize,
    section_bytes: usize,
    output_bytes: usize,
    median_ns: u64,
    p10_ns: u64,
    p90_ns: u64,
    ns_per_record: f64,
    us_per_record: f64,
}

#[derive(Serialize)]
struct Packing {
    records_at_byte_cap: usize,
    estimated_bytes: u64,
    encoded_none_bytes: usize,
    section_bytes: usize,
    gzip_wire_bytes: usize,
    ratio: f64,
}

#[derive(Serialize)]
struct Report {
    backend: String,
    rustc: String,
    packing: Packing,
    timings: Vec<Timing>,
    reading: Vec<String>,
}

fn main() {
    if let Err(error) = run() {
        eprintln!("kl10-04-measure: {error}");
        std::process::exit(2);
    }
}

fn run() -> Result<(), String> {
    let records = build_records(2_000);
    let packing = packing(&records)?;
    let n = packing.records_at_byte_cap;
    let batch = records[..n].to_vec();
    let section = uncompressed_section(&batch)?;
    let gzip_batch = RecordBatch::from_records(batch.clone()).with_compression(Compression::Gzip);
    let none_batch = RecordBatch::from_records(batch.clone());
    let header = gzip_header(&batch)?;
    let per_record_sections = batch
        .iter()
        .map(|record| uncompressed_section(std::slice::from_ref(record)))
        .collect::<Result<Vec<_>, _>>()?;
    let mut scratch = CompressScratch::with_caps(BATCH_BYTES, BATCH_BYTES);
    let mut scratch_buf = BytesMut::with_capacity(BATCH_BYTES);
    let mut timings = Vec::new();

    timings.push(time("encode_record_batch_gzip", n, section.len(), || {
        let mut buf = BytesMut::new();
        encode_record_batch(&mut buf, &gzip_batch).map_err(|e| e.to_string())?;
        Ok(buf.len())
    })?);

    timings.push(time(
        "write_record_batch_scratch_reused",
        n,
        section.len(),
        || {
            scratch_buf.clear();
            write_record_batch_scratch(
                &mut scratch_buf,
                &header,
                batch.iter().map(EncodeRecord::from_record),
                &mut scratch,
            )
            .map_err(|e| e.to_string())?;
            Ok(scratch_buf.len())
        },
    )?);

    timings.push(time("encode_record_batch_none", n, section.len(), || {
        let mut buf = BytesMut::new();
        encode_record_batch(&mut buf, &none_batch).map_err(|e| e.to_string())?;
        Ok(buf.len())
    })?);

    timings.push(time(
        "gzip_write_all_new_encoder",
        n,
        section.len(),
        || Ok(gzip_write_all(&section)?.len()),
    )?);

    timings.push(time("gzip_write_8kib_chunks", n, section.len(), || {
        Ok(gzip_chunks(&section, 8192)?.len())
    })?);

    timings.push(time("gzip_write_per_record", n, section.len(), || {
        Ok(gzip_prebuilt_records(&per_record_sections)?.len())
    })?);

    timings.push(time("gzip_encoder_new_empty", n, 0, || {
        let encoder = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
        let out = encoder.finish().map_err(|e| e.to_string())?;
        Ok(out.len())
    })?);

    timings.push(time("gzip_write_all_level_6", n, section.len(), || {
        Ok(gzip_level(&section, flate2::Compression::new(6))?.len())
    })?);

    let report = Report {
        backend: backend_name(),
        rustc: rustc_version(),
        packing,
        timings,
        reading: readings(),
    };
    let json = serde_json::to_string_pretty(&report).map_err(|e| e.to_string())?;
    println!("{json}");
    if let Some(path) = std::env::args().nth(1) {
        let path = PathBuf::from(path);
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent).map_err(|e| e.to_string())?;
        }
        std::fs::write(path, json.as_bytes()).map_err(|e| e.to_string())?;
    }
    Ok(())
}

fn packing(records: &[Record]) -> Result<Packing, String> {
    let mut estimated = OVERHEAD as u64;
    let mut count = 0usize;
    for record in records.iter().take(BATCH_RECORDS) {
        let add = u64::try_from(
            record
                .record_size_upper_bound()
                .map_err(|e| e.to_string())?,
        )
        .map_err(|e| e.to_string())?;
        if count > 0 && estimated.saturating_add(add) > BATCH_BYTES as u64 {
            break;
        }
        estimated = estimated.saturating_add(add);
        count += 1;
        if estimated >= BATCH_BYTES as u64 {
            break;
        }
    }
    count = count.max(1);
    let slice = &records[..count];
    let none = RecordBatch::from_records(slice.to_vec());
    let mut raw = BytesMut::new();
    encode_record_batch(&mut raw, &none).map_err(|e| e.to_string())?;
    let gzip = RecordBatch::from_records(slice.to_vec()).with_compression(Compression::Gzip);
    let mut packed = BytesMut::new();
    encode_record_batch(&mut packed, &gzip).map_err(|e| e.to_string())?;
    let section = raw.len().saturating_sub(OVERHEAD);
    Ok(Packing {
        records_at_byte_cap: count,
        estimated_bytes: estimated,
        encoded_none_bytes: raw.len(),
        section_bytes: section,
        gzip_wire_bytes: packed.len(),
        ratio: packed.len().saturating_sub(OVERHEAD) as f64 / section.max(1) as f64,
    })
}

fn build_records(count: usize) -> Vec<Record> {
    (0..count)
        .map(|index| {
            let value = json1k::value(json1k::SEED, index);
            let mut key = [0u8; 16];
            key[..8].copy_from_slice(&(index as u64).to_be_bytes());
            let mut state = json1k::SEED ^ index as u64;
            key[8..].copy_from_slice(&json1k::splitmix64(&mut state).to_be_bytes());
            Record {
                offset: index as i64,
                timestamp: 1_700_000_000_000 + index as i64,
                key: Some(Bytes::copy_from_slice(&key)),
                value: Some(Bytes::from(value)),
                headers: vec![Header::new(
                    json1k::HEADER_KEY,
                    Bytes::copy_from_slice(&json1k::header(json1k::SEED, index as u64)),
                )],
            }
        })
        .collect()
}

fn uncompressed_section(records: &[Record]) -> Result<Vec<u8>, String> {
    let none = RecordBatch::from_records(records.to_vec());
    let mut buf = BytesMut::new();
    encode_record_batch(&mut buf, &none).map_err(|e| e.to_string())?;
    Ok(buf.split_off(OVERHEAD.min(buf.len())).to_vec())
}

fn gzip_header(records: &[Record]) -> Result<BatchHeader, String> {
    let batch = RecordBatch::from_records(records.to_vec()).with_compression(Compression::Gzip);
    Ok(BatchHeader {
        base_offset: batch.base_offset,
        partition_leader_epoch: batch.partition_leader_epoch,
        attributes: batch.attributes,
        base_timestamp: batch.base_timestamp,
        max_timestamp: batch.max_timestamp,
        producer_id: batch.producer_id,
        producer_epoch: batch.producer_epoch,
        base_sequence: batch.base_sequence,
        count: i32::try_from(records.len()).map_err(|e| e.to_string())?,
    })
}

fn gzip_write_all(src: &[u8]) -> Result<Vec<u8>, String> {
    let out = Vec::with_capacity(src.len() / 2);
    let mut encoder = flate2::write::GzEncoder::new(out, flate2::Compression::default());
    encoder.write_all(src).map_err(|e| e.to_string())?;
    encoder.finish().map_err(|e| e.to_string())
}

fn gzip_level(src: &[u8], level: flate2::Compression) -> Result<Vec<u8>, String> {
    let mut encoder = flate2::write::GzEncoder::new(Vec::new(), level);
    encoder.write_all(src).map_err(|e| e.to_string())?;
    encoder.finish().map_err(|e| e.to_string())
}

fn gzip_chunks(src: &[u8], chunk: usize) -> Result<Vec<u8>, String> {
    let mut encoder = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
    for piece in src.chunks(chunk) {
        encoder.write_all(piece).map_err(|e| e.to_string())?;
    }
    encoder.finish().map_err(|e| e.to_string())
}

fn gzip_prebuilt_records(sections: &[Vec<u8>]) -> Result<Vec<u8>, String> {
    let mut encoder = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
    for one in sections {
        encoder.write_all(one).map_err(|e| e.to_string())?;
    }
    encoder.finish().map_err(|e| e.to_string())
}

fn time(
    name: &str,
    records: usize,
    section_bytes: usize,
    mut body: impl FnMut() -> Result<usize, String>,
) -> Result<Timing, String> {
    for _ in 0..WARMUP {
        let _ = black_box(body()?);
    }
    let mut samples = Vec::with_capacity(ITERS);
    let mut output_bytes = 0usize;
    for _ in 0..ITERS {
        let start = Instant::now();
        output_bytes = black_box(body()?);
        samples.push(start.elapsed());
    }
    samples.sort();
    let median = samples[samples.len() / 2];
    let p10 = samples[samples.len() / 10];
    let p90 = samples[samples.len() * 9 / 10];
    let ns = nanos(median);
    Ok(Timing {
        name: name.into(),
        records,
        section_bytes,
        output_bytes,
        median_ns: ns,
        p10_ns: nanos(p10),
        p90_ns: nanos(p90),
        ns_per_record: ns as f64 / records.max(1) as f64,
        us_per_record: ns as f64 / records.max(1) as f64 / 1e3,
    })
}

fn nanos(duration: Duration) -> u64 {
    u64::try_from(duration.as_nanos()).unwrap_or(u64::MAX)
}

fn backend_name() -> String {
    if cfg!(feature = "zlib-rs") {
        "zlib-rs".into()
    } else {
        "miniz_oxide".into()
    }
}

fn rustc_version() -> String {
    std::process::Command::new("rustc")
        .arg("+stable")
        .arg("--version")
        .output()
        .ok()
        .and_then(|o| String::from_utf8(o.stdout).ok())
        .map(|s| s.trim().to_string())
        .unwrap_or_else(|| "unknown".into())
}

fn readings() -> Vec<String> {
    vec![
        "gzip_compress_into already write_all-s the full encoded section, then finish; it is not a per-record deflate.".into(),
        "write_record_batch allocates a fresh CompressScratch; the produce worker reuses one via write_record_batch_scratch.".into(),
        "Gateway packing is linger 100 ms or 1 MiB estimated record bytes, max in flight 1, eight explicit partitions.".into(),
        "A gap >=5% of the live cell (~48 us/record wall CPU on KL10-02 gzip open-loop) is required before a child card.".into(),
    ]
}

//! KL09-05 instruction-count benches (iai-callgrind): the tracked set
//! for the deterministic regression gate. Fixture bytes are built in
//! `setup` fns (excluded from collection); only the library call under
//! test is measured. Shapes mirror the criterion suite (seed 0xC0DEC).

use std::hint::black_box;

use bytes::{Bytes, BytesMut};
use iai_callgrind::{library_benchmark, library_benchmark_group, main, LibraryBenchmarkConfig};
use partitionline::protocol::api::{
    encode_produce_request, ProducePartitionData, ProduceTopicData,
};
use partitionline::protocol::records::{
    decode_record_batch, encode_record_batch, Compression, RecordBatch,
};

use codec::{build_records, compressed_batch, crc_body};

const SEED: u64 = 0xC0DEC;

fn batch_500() -> RecordBatch {
    RecordBatch::from_records(build_records(SEED, 500, 16, 100, "random", 0))
}

fn batch_bytes(batch: &RecordBatch) -> Vec<u8> {
    let mut buf = BytesMut::new();
    encode_record_batch(&mut buf, batch).unwrap();
    buf.to_vec()
}

#[library_benchmark]
#[bench::f03(setup = batch_500)]
fn iai_encode(batch: RecordBatch) {
    let mut buf = BytesMut::new();
    encode_record_batch(&mut buf, black_box(&batch)).unwrap();
    black_box(buf);
}

fn decode_input() -> Bytes {
    Bytes::from(batch_bytes(&batch_500()))
}

#[library_benchmark]
#[bench::f03(setup = decode_input)]
fn iai_decode(mut buf: Bytes) {
    black_box(decode_record_batch(black_box(&mut buf)).unwrap());
}

fn crc_input() -> Vec<u8> {
    crc_body(&batch_bytes(&batch_500())).to_vec()
}

#[library_benchmark]
#[bench::f03(setup = crc_input)]
fn iai_crc(body: Vec<u8>) {
    black_box(crc32c::crc32c(black_box(&body)));
}

fn gzip_batch() -> RecordBatch {
    compressed_batch(
        build_records(SEED, 500, 16, 100, "text", 0),
        Compression::Gzip,
    )
}

#[library_benchmark]
#[bench::text(setup = gzip_batch)]
fn iai_compress_gzip(batch: RecordBatch) {
    let mut buf = BytesMut::new();
    encode_record_batch(&mut buf, black_box(&batch)).unwrap();
    black_box(buf);
}

fn snappy_batch() -> RecordBatch {
    compressed_batch(
        build_records(SEED, 500, 16, 100, "text", 0),
        Compression::Snappy,
    )
}

#[library_benchmark]
#[bench::text(setup = snappy_batch)]
fn iai_compress_snappy(batch: RecordBatch) {
    let mut buf = BytesMut::new();
    encode_record_batch(&mut buf, black_box(&batch)).unwrap();
    black_box(buf);
}

fn lz4_batch() -> RecordBatch {
    compressed_batch(
        build_records(SEED, 500, 16, 100, "text", 0),
        Compression::Lz4,
    )
}

#[library_benchmark]
#[bench::text(setup = lz4_batch)]
fn iai_compress_lz4(batch: RecordBatch) {
    let mut buf = BytesMut::new();
    encode_record_batch(&mut buf, black_box(&batch)).unwrap();
    black_box(buf);
}

fn lz4_bytes() -> Bytes {
    Bytes::from(batch_bytes(&lz4_batch()))
}

#[library_benchmark]
#[bench::text(setup = lz4_bytes)]
fn iai_decompress_lz4(mut buf: Bytes) {
    black_box(decode_record_batch(black_box(&mut buf)).unwrap());
}

fn request_topics() -> Vec<ProduceTopicData> {
    let batch = RecordBatch::from_records(build_records(SEED, 100, 16, 100, "random", 0));
    vec![ProduceTopicData {
        topic: "t".to_owned(),
        partitions: vec![ProducePartitionData {
            index: 0,
            records: batch,
        }],
    }]
}

#[library_benchmark]
#[bench::v9(setup = request_topics)]
fn iai_request(topics: Vec<ProduceTopicData>) {
    let mut buf = BytesMut::new();
    encode_produce_request(black_box(&mut buf), 9, None, 1, 30_000, black_box(&topics)).unwrap();
    black_box(buf);
}

library_benchmark_group!(
    name = tracked;
    benchmarks =
        iai_encode, iai_decode, iai_crc,
        iai_compress_gzip, iai_compress_snappy, iai_compress_lz4,
        iai_decompress_lz4, iai_request
);

main!(
    config = LibraryBenchmarkConfig::default();
    library_benchmark_groups = tracked
);

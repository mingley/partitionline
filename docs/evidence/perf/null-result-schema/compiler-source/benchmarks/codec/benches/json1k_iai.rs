//! KL10-03 instruction census. Setup is excluded; separate from the historical
//! tracked regression group so its existing instruction baselines stay intact.
use bytes::{Bytes, BytesMut};
use codec::json1k::{batch, raw_compress, raw_decompress, section, wire};
use iai_callgrind::{library_benchmark, library_benchmark_group, main, LibraryBenchmarkConfig};
use partitionline::protocol::records::{
    decode_record_batch, encode_record_batch, Compression, RecordBatch,
};
use std::hint::black_box;

fn setup_batch(count: usize, compression: Compression) -> RecordBatch {
    batch(count, compression)
}
fn setup_wire(count: usize, compression: Compression) -> Bytes {
    wire(&batch(count, compression))
}
fn setup_raw(count: usize, compression: Compression) -> (Compression, Bytes) {
    (compression, section(count))
}
fn setup_packed(count: usize, compression: Compression) -> (Compression, Vec<u8>) {
    (compression, raw_compress(compression, &section(count)))
}

#[library_benchmark]
#[bench::gzip_16k(args = (16, Compression::Gzip), setup = setup_batch)]
#[bench::gzip_256k(args = (256, Compression::Gzip), setup = setup_batch)]
#[bench::lz4_16k(args = (16, Compression::Lz4), setup = setup_batch)]
#[bench::lz4_256k(args = (256, Compression::Lz4), setup = setup_batch)]
#[bench::snappy_16k(args = (16, Compression::Snappy), setup = setup_batch)]
#[bench::snappy_256k(args = (256, Compression::Snappy), setup = setup_batch)]
fn json1k_compress(batch: RecordBatch) {
    let mut output = BytesMut::new();
    encode_record_batch(&mut output, black_box(&batch)).unwrap();
    black_box(output);
}

#[library_benchmark]
#[bench::gzip_16k(args = (16, Compression::Gzip), setup = setup_wire)]
#[bench::gzip_256k(args = (256, Compression::Gzip), setup = setup_wire)]
#[bench::lz4_16k(args = (16, Compression::Lz4), setup = setup_wire)]
#[bench::lz4_256k(args = (256, Compression::Lz4), setup = setup_wire)]
#[bench::snappy_16k(args = (16, Compression::Snappy), setup = setup_wire)]
#[bench::snappy_256k(args = (256, Compression::Snappy), setup = setup_wire)]
fn json1k_decompress(mut input: Bytes) {
    black_box(decode_record_batch(black_box(&mut input)).unwrap());
}

#[library_benchmark]
#[bench::gzip_16k(args = (16, Compression::Gzip), setup = setup_raw)]
#[bench::gzip_256k(args = (256, Compression::Gzip), setup = setup_raw)]
#[bench::lz4_16k(args = (16, Compression::Lz4), setup = setup_raw)]
#[bench::lz4_256k(args = (256, Compression::Lz4), setup = setup_raw)]
#[bench::snappy_16k(args = (16, Compression::Snappy), setup = setup_raw)]
#[bench::snappy_256k(args = (256, Compression::Snappy), setup = setup_raw)]
fn json1k_raw_compress(input: (Compression, Bytes)) {
    black_box(raw_compress(input.0, black_box(&input.1)));
}

#[library_benchmark]
#[bench::gzip_16k(args = (16, Compression::Gzip), setup = setup_packed)]
#[bench::gzip_256k(args = (256, Compression::Gzip), setup = setup_packed)]
#[bench::lz4_16k(args = (16, Compression::Lz4), setup = setup_packed)]
#[bench::lz4_256k(args = (256, Compression::Lz4), setup = setup_packed)]
#[bench::snappy_16k(args = (16, Compression::Snappy), setup = setup_packed)]
#[bench::snappy_256k(args = (256, Compression::Snappy), setup = setup_packed)]
fn json1k_raw_decompress(input: (Compression, Vec<u8>)) {
    black_box(raw_decompress(input.0, black_box(&input.1)));
}

library_benchmark_group!(name = json1k; benchmarks = json1k_compress, json1k_decompress, json1k_raw_compress, json1k_raw_decompress);
main!(config = LibraryBenchmarkConfig::default(); library_benchmark_groups = json1k);

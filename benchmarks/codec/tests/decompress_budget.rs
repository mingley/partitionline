//! KL09-34 decompression census; input/record inspection is outside collection.
use std::hint::black_box;

use bytes::{Bytes, BytesMut};
use codec::{build_records, census, compressed_batch, CountingAlloc};
use partitionline::protocol::records::{decode_record_batch, encode_record_batch, Compression};

#[global_allocator]
static ALLOC: CountingAlloc = CountingAlloc;

#[test]
fn decompression_census_preserves_every_record() {
    for (name, compression) in [
        ("gzip", Compression::Gzip),
        ("snappy", Compression::Snappy),
        ("lz4", Compression::Lz4),
    ] {
        let records = build_records(0xC0DEC, 500, 16, 100, "text", 0);
        let batch = compressed_batch(records, compression);
        let mut encoded = BytesMut::new();
        encode_record_batch(&mut encoded, &batch).unwrap();
        let mut input = Bytes::from(encoded.to_vec());
        let (decoded, allocations, bytes) =
            census(|| black_box(decode_record_batch(black_box(&mut input)).unwrap()));
        assert_eq!(decoded.count(), 500);
        assert_eq!(decoded.records(), batch.records());
        assert!(input.is_empty());
        println!(
            "{}",
            serde_json::json!({
                "cell": format!("micro-decompress/{name}/text"),
                "allocations": allocations,
                "bytes": bytes,
                "records": decoded.count(),
            })
        );
    }
}

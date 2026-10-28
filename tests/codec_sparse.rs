//! Independent Kafka Java sparse and wrapped-sparse record batches.
use bytes::BytesMut;
use partitionline::protocol::records::{
    decode_record_batch, encode_record_batch_with_offsets, Compression, RecordBatch,
};
use partitionline::Error;
use std::path::Path;

#[test]
#[expect(
    clippy::disallowed_methods,
    reason = "finite test artifact output for the independent Java verifier"
)]
fn java_sparse_batches_preserve_offsets_and_header_delta() -> Result<(), Error> {
    let cases: [(&str, &[u8], [i64; 2]); 2] = [
        (
            "sparse",
            include_bytes!("fixtures/codec-sparse/java-sparse.batch"),
            [10, 12],
        ),
        (
            "wrapped-sparse",
            include_bytes!("fixtures/codec-sparse/java-wrapped-sparse.batch"),
            [i64::MAX, i64::MIN + 1],
        ),
    ];
    let codecs = [
        Compression::None,
        Compression::Gzip,
        Compression::Snappy,
        Compression::Lz4,
        #[cfg(feature = "zstd")]
        Compression::Zstd,
    ];
    for (name, input, offsets) in cases {
        let delta = RecordBatch::encoded_last_offset_delta(input)?;
        assert_eq!(delta, 4);
        let original = decode_record_batch(&mut &*input)?;
        assert_eq!(
            original
                .records
                .iter()
                .map(|record| record.offset)
                .collect::<Vec<_>>(),
            offsets
        );
        for codec in codecs {
            let mut encoded = BytesMut::new();
            encode_record_batch_with_offsets(
                &mut encoded,
                &original.clone().with_compression(codec),
                delta,
            )?;
            assert_eq!(RecordBatch::encoded_last_offset_delta(&encoded)?, delta);
            let roundtrip = decode_record_batch(&mut encoded.clone())?;
            assert_eq!(roundtrip.records, original.records);
            assert_eq!(roundtrip.base_offset, original.base_offset);
            assert_eq!(roundtrip.base_timestamp, original.base_timestamp);
            assert_eq!(roundtrip.max_timestamp, original.max_timestamp);
            if let Some(dir) = std::env::var_os("PL_SPARSE_CODEC_OUTPUT") {
                std::fs::create_dir_all(&dir).expect("create sparse peer output");
                std::fs::write(
                    Path::new(&dir).join(format!("rust-{name}-{}.batch", codec.as_str())),
                    &encoded,
                )
                .expect("write sparse peer output");
            }
        }
        let mut prefix = BytesMut::from(&b"prefix"[..]);
        assert!(encode_record_batch_with_offsets(&mut prefix, &original, 1).is_err());
        assert_eq!(prefix.as_ref(), b"prefix");
    }
    Ok(())
}

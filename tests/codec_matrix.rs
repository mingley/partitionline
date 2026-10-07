//! Independent codec fixtures and opt-in native cross-client matrix.
use bytes::BytesMut;
use partitionline::protocol::records::{
    decode_record_batches_with_limit, encode_record_batch, Compression,
};
use partitionline::Error;
use std::path::{Path, PathBuf};
fn directory() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/codec-matrix")
}
#[expect(
    clippy::disallowed_methods,
    clippy::expect_used,
    reason = "finite synchronous independent fixture I/O"
)]
fn read(name: &str) -> Vec<u8> {
    std::fs::read(directory().join(name)).expect("read independent codec fixture")
}
#[expect(
    clippy::disallowed_methods,
    clippy::expect_used,
    reason = "finite synchronous peer output"
)]
fn emit(name: &str, bytes: &[u8]) {
    if let Some(dir) = std::env::var_os("PL_CODEC_MATRIX_OUTPUT") {
        std::fs::create_dir_all(&dir).expect("create peer output");
        std::fs::write(Path::new(&dir).join(name), bytes).expect("write Rust frame");
    }
}
#[test]
fn independent_java_codec_batches_roundtrip_all_record_fields() -> Result<(), Error> {
    let plain = read("java-none.batch");
    let expected = decode_record_batches_with_limit(&mut &plain[..], 1024 * 1024)?;
    let codecs = [
        Compression::None,
        Compression::Gzip,
        Compression::Snappy,
        Compression::Lz4,
        #[cfg(feature = "zstd")]
        Compression::Zstd,
    ];
    for codec in codecs {
        let bytes = read(&format!("java-{}.batch", codec.as_str()));
        let mut actual = decode_record_batches_with_limit(&mut &bytes[..], 1024 * 1024)?;
        for batch in &mut actual {
            batch.attributes &= !7;
        }
        assert_eq!(actual, expected);
        let mut output = BytesMut::new();
        for batch in &expected {
            encode_record_batch(&mut output, &batch.clone().with_compression(codec))?;
        }
        emit(&format!("rust-{}.batch", codec.as_str()), &output);
    }
    let raw = read("native-raw-snappy.batch");
    let mut actual = decode_record_batches_with_limit(&mut &raw[..], 1024 * 1024)?;
    for batch in &mut actual {
        batch.attributes &= !7;
    }
    assert_eq!(actual, expected);
    Ok(())
}
#[test]
fn signed_offset_and_timestamp_delta_boundary_matches_java_without_panicking() -> Result<(), Error>
{
    let input = read("java-signed-boundary.batch");
    let decoded = decode_record_batches_with_limit(&mut &input[..], 1024 * 1024)?;
    let codecs = [
        Compression::None,
        Compression::Gzip,
        Compression::Snappy,
        Compression::Lz4,
        #[cfg(feature = "zstd")]
        Compression::Zstd,
    ];
    for codec in codecs {
        let mut output = BytesMut::new();
        for batch in &decoded {
            encode_record_batch(&mut output, &batch.clone().with_compression(codec))?;
        }
        emit(&format!("rust-signed-{}.batch", codec.as_str()), &output);
    }
    for batch in decoded {
        for (index, r) in batch.records.iter().enumerate() {
            let delta = i64::try_from(index).map_err(|_| Error::protocol("test index overflow"))?;
            assert_eq!(r.offset, i64::MAX.wrapping_add(delta));
            assert_eq!(r.timestamp, i64::MAX.wrapping_add(delta));
        }
    }
    Ok(())
}

#[tokio::test]
#[ignore = "requires explicit owned native broker topic and independent SDK peers"]
async fn native_codec_matrix_peer() -> Result<(), Error> {
    use bytes::Bytes;
    use partitionline::{
        Consumer, ConsumerConfig, Header, ProduceRecord, Producer, ProducerConfig,
    };
    use std::time::{Duration, Instant};
    let env = |name| {
        std::env::var(name).map_err(|_| Error::protocol(format!("required matrix setting {name}")))
    };
    let bootstrap = env("PL_CODEC_BOOTSTRAP")?;
    let topic = env("PL_CODEC_TOPIC")?;
    let mode = env("PL_CODEC_MODE")?;
    let codec = Compression::from_name(&env("PL_CODEC")?)?;
    let timestamp: i64 = env("PL_CODEC_TIMESTAMP")?
        .parse()
        .map_err(|_| Error::protocol("matrix timestamp invalid"))?;
    fn entropy(size: usize) -> Vec<u8> {
        let mut state = 0x5eed0001u64;
        let mut out = Vec::new();
        while out.len() < size {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            out.extend_from_slice(&state.to_le_bytes());
        }
        out.truncate(size);
        out
    }
    fn key(id: usize) -> Option<Bytes> {
        match id {
            1 => Some(Bytes::new()),
            2 => Some(Bytes::from_static(b"key")),
            5 => Some(Bytes::from_static(b"headers")),
            _ => None,
        }
    }
    fn value(id: usize) -> Option<Bytes> {
        match id {
            0 => None,
            1 => Some(Bytes::new()),
            2 => Some(Bytes::from_static("世界".as_bytes())),
            3 => Some(Bytes::from(entropy(65536))),
            4 => Some(Bytes::from(vec![b'x'; 200000])),
            5 => Some(Bytes::from_static(&[0, 1, 2])),
            _ => Some(Bytes::from(entropy(131071 + id - 6))),
        }
    }
    fn headers(id: usize) -> Result<Vec<Header>, Error> {
        Ok(vec![
            Header::new(
                "id",
                Bytes::copy_from_slice(
                    &u64::try_from(id)
                        .map_err(|_| Error::protocol("test ID overflow"))?
                        .to_be_bytes(),
                ),
            ),
            Header::new("a", Bytes::from_static(&[0, 255])),
            Header {
                key: "nullable".into(),
                value: None,
            },
        ])
    }
    if mode == "produce" {
        let producer = Producer::new(
            ProducerConfig::bootstrap([bootstrap])
                .compression(codec)
                .linger(Duration::ZERO),
        )
        .await?;
        for id in 0..9 {
            let delta = i64::try_from(id).map_err(|_| Error::protocol("test ID overflow"))?;
            let mut record = ProduceRecord::to(topic.clone()).partition(0);
            record.key = key(id);
            record.value = value(id);
            record.timestamp = Some(timestamp + delta);
            record.headers = headers(id)?;
            let metadata = producer.send(record).await?;
            assert_eq!(metadata.offset, delta);
        }
        producer.flush().await?;
        producer.close().await?;
    } else if mode == "consume" {
        let mut consumer = Consumer::new(ConsumerConfig::bootstrap([bootstrap])).await?;
        consumer.assign(topic, 0, 0).await?;
        let mut count = 0usize;
        let deadline = Instant::now() + Duration::from_secs(30);
        while count < 9 && Instant::now() < deadline {
            for record in consumer.fetch_timeout(Duration::from_secs(5)).await? {
                let delta =
                    i64::try_from(count).map_err(|_| Error::protocol("test ID overflow"))?;
                assert_eq!(record.offset, delta);
                assert_eq!(record.timestamp, timestamp + delta);
                assert_eq!(record.key, key(count));
                assert_eq!(record.value, value(count));
                assert_eq!(record.headers, headers(count)?);
                count += 1;
            }
        }
        assert_eq!(count, 9);
        consumer.close().await?;
    } else {
        return Err(Error::protocol("matrix mode invalid"));
    }
    Ok(())
}

#[test]
#[ignore = "requires all segments from the completed owned native codec matrix"]
#[expect(
    clippy::disallowed_methods,
    reason = "finite synchronous native artifact audit, fail on missing inputs"
)]
fn native_stored_codec_batches_match_independent_record_fields() -> Result<(), Error> {
    let directory = std::env::var_os("PL_CODEC_NATIVE_ARTIFACTS")
        .map(PathBuf::from)
        .ok_or_else(|| Error::protocol("required native matrix artifact directory"))?;
    let cells = std::fs::read_to_string(directory.join("stored-cells.tsv"))
        .expect("native stored cell ledger");
    let cells: Vec<_> = cells.lines().collect();
    let expected_count: usize = std::env::var("PL_CODEC_STORED_CELL_COUNT")
        .map_err(|_| Error::protocol("required native stored cell count"))?
        .parse()
        .map_err(|_| Error::protocol("invalid native stored cell count"))?;
    assert!(matches!(expected_count, 60 | 90 | 150));
    assert_eq!(cells.len(), expected_count);
    let plain = read("java-none.batch");
    let expected = decode_record_batches_with_limit(&mut &plain[..], 1024 * 1024)?;
    assert_eq!(expected.len(), 1);
    for cell in cells {
        let fields: Vec<_> = cell.split('\t').collect();
        assert_eq!(fields.len(), 4);
        let broker = fields[0];
        assert!(matches!(
            broker,
            "3.9.1" | "4.1.0" | "4.1.2" | "4.2.1" | "4.3.1"
        ));
        let topic = fields[1];
        assert!(
            topic.len() < 100
                && topic
                    .bytes()
                    .all(|b| b.is_ascii_alphanumeric() || b == b'-')
        );
        let codec = Compression::from_name(fields[2])?;
        let timestamp: i64 = fields[3].parse().expect("cell timestamp");
        let files = std::fs::read_dir(directory.join(broker).join("batches").join(topic))
            .expect("native topic segments");
        let mut files: Vec<_> = files
            .map(|entry| entry.expect("native segment entry").path())
            .filter(|path| path.extension().is_some_and(|extension| extension == "log"))
            .collect();
        files.sort();
        assert!(!files.is_empty());
        let mut records = Vec::new();
        for file in files {
            assert!(std::fs::metadata(&file).expect("segment size").len() <= 16 * 1024 * 1024);
            let bytes = std::fs::read(file).expect("native segment bytes");
            for batch in decode_record_batches_with_limit(&mut &bytes[..], 1024 * 1024)? {
                assert_eq!(batch.compression_type()?, codec);
                records.extend(batch.records);
            }
        }
        assert_eq!(records.len(), 9);
        for (id, (actual, expected)) in records.iter().zip(&expected[0].records).enumerate() {
            let delta = i64::try_from(id).map_err(|_| Error::protocol("test ID overflow"))?;
            assert_eq!(actual.offset, delta);
            assert_eq!(actual.timestamp, timestamp + delta);
            assert_eq!(actual.key, expected.key);
            assert_eq!(actual.value, expected.value);
            assert_eq!(actual.headers, expected.headers);
        }
    }
    Ok(())
}

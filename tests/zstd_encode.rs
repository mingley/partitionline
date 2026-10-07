//! Optional zstd encoding checked by independent Java and native peers.
#![cfg(feature = "zstd")]
mod common;
use bytes::{Bytes, BytesMut};
use partitionline::protocol::records::{
    decode_record_batches_with_limit, encode_record_batch, write_record_batch_scratch, BatchHeader,
    CompressScratch, Compression, EncodeRecord, RecordBatch, ZstdLevel,
};
use partitionline::{Error, ProduceRecord, Producer, ProducerConfig};
use std::{path::Path, time::Duration};

#[expect(
    clippy::disallowed_methods,
    clippy::expect_used,
    reason = "finite synchronous peer fixture I/O"
)]
fn fixture() -> Vec<u8> {
    std::fs::read(
        Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/zstd-decode/java-none.batch"),
    )
    .expect("Apache fixture")
}
#[expect(
    clippy::disallowed_methods,
    clippy::expect_used,
    reason = "finite synchronous independent peer output"
)]
fn emit(name: &str, encoded: &[u8], expected: &[u8]) {
    if let Some(path) = std::env::var_os("PL_ZSTD_ENCODE_OUTPUT") {
        let dir = Path::new(&path);
        std::fs::create_dir_all(dir).expect("create peer output");
        std::fs::write(dir.join(format!("{name}.batch")), encoded)
            .expect("write actual Rust frame");
        std::fs::write(dir.join(format!("{name}.expected")), expected)
            .expect("write reference batch");
    }
}
fn header(batch: &RecordBatch) -> Result<BatchHeader, Error> {
    Ok(BatchHeader {
        base_offset: batch.base_offset,
        partition_leader_epoch: batch.partition_leader_epoch,
        attributes: (batch.attributes & !7) | 4,
        base_timestamp: batch.base_timestamp,
        max_timestamp: batch.max_timestamp,
        producer_id: batch.producer_id,
        producer_epoch: batch.producer_epoch,
        base_sequence: batch.base_sequence,
        count: i32::try_from(batch.records.len())
            .map_err(|_| Error::protocol("test record count overflow"))?,
    })
}
#[test]
fn each_explicit_level_emits_independently_decodable_record_batches() -> Result<(), Error> {
    let reference = fixture();
    let batches = decode_record_batches_with_limit(&mut &reference[..], 1024 * 1024)?;
    let mut scratch = CompressScratch::with_caps(1024 * 1024, 1024 * 1024);
    for level in 1..=19 {
        scratch.set_zstd_level(ZstdLevel::new(level)?);
        let mut out = BytesMut::new();
        for batch in &batches {
            write_record_batch_scratch(
                &mut out,
                &header(batch)?,
                batch.records.iter().map(EncodeRecord::from_record),
                &mut scratch,
            )?;
        }
        let mut roundtrip = decode_record_batches_with_limit(&mut &out[..], 1024 * 1024)?;
        for batch in &mut roundtrip {
            batch.attributes &= !7;
        }
        assert_eq!(roundtrip, batches);
        emit(&format!("level-{level:02}"), &out, &reference);
    }
    assert_eq!(Compression::Zstd.min_level()?, 1);
    assert_eq!(Compression::Zstd.max_level()?, 19);
    assert_eq!(Compression::Zstd.default_level()?, 3);
    for level in [i32::MIN, -7, 0, 20, 22, i32::MAX] {
        assert!(ZstdLevel::new(level).is_err());
    }
    Ok(())
}
#[test]
fn reused_context_survives_empty_small_and_block_boundary_batches() -> Result<(), Error> {
    use partitionline::protocol::records::{Header, Record};
    let mut scratch = CompressScratch::new();
    for (i, size) in [0, 1, 127, 128, 65535, 131071, 131072, 131073, 262145]
        .into_iter()
        .enumerate()
    {
        let mut expected = RecordBatch::from_records(vec![Record {
            offset: 0,
            timestamp: 0,
            key: None,
            value: Some(Bytes::from(vec![b'x'; size])),
            headers: vec![Header::new("boundary", Bytes::from_static(b"v"))],
        }]);
        let mut plain = BytesMut::new();
        encode_record_batch(&mut plain, &expected)?;
        expected.attributes = (expected.attributes & !7) | 4;
        let mut out = BytesMut::new();
        write_record_batch_scratch(
            &mut out,
            &header(&expected)?,
            expected.records.iter().map(EncodeRecord::from_record),
            &mut scratch,
        )?;
        assert_eq!(
            decode_record_batches_with_limit(&mut &out[..], 1024 * 1024)?,
            vec![expected]
        );
        emit(&format!("boundary-{i:02}"), &out, &plain);
    }
    let empty = RecordBatch::from_records(Vec::new()).with_compression(Compression::Zstd);
    let mut out = BytesMut::new();
    encode_record_batch(&mut out, &empty)?;
    assert_eq!(
        decode_record_batches_with_limit(&mut &out[..], 0)?,
        vec![empty]
    );
    Ok(())
}
#[test]
fn encoder_errors_preserve_output_and_allow_scratch_reuse() -> Result<(), Error> {
    let reference = fixture();
    let batch = decode_record_batches_with_limit(&mut &reference[..], 1024 * 1024)?.remove(0);
    let mut scratch = CompressScratch::new();
    let mut out = BytesMut::from(&b"prefix"[..]);
    let mut bad = header(&batch)?;
    bad.count += 1;
    assert!(write_record_batch_scratch(
        &mut out,
        &bad,
        batch.records.iter().map(EncodeRecord::from_record),
        &mut scratch
    )
    .is_err());
    assert_eq!(&out[..], b"prefix");
    let huge = vec![b'x'; 64 * 1024 * 1024];
    let bad = BatchHeader {
        attributes: 4,
        count: 1,
        ..BatchHeader::default()
    };
    assert!(write_record_batch_scratch(
        &mut out,
        &bad,
        std::iter::once(EncodeRecord {
            timestamp: 0,
            key: None,
            value: Some(&huge),
            headers: &[]
        }),
        &mut scratch
    )
    .is_err());
    assert_eq!(&out[..], b"prefix");
    write_record_batch_scratch(
        &mut out,
        &header(&batch)?,
        batch.records.iter().map(EncodeRecord::from_record),
        &mut scratch,
    )?;
    assert_eq!(
        decode_record_batches_with_limit(&mut out.get(6..).unwrap_or_default(), 1024 * 1024)?.len(),
        1
    );
    Ok(())
}
#[tokio::test]
async fn unsupported_produce_version_releases_reservations_without_application_send(
) -> Result<(), Error> {
    let mock = common::Mock::start().await;
    mock.set_api_max(partitionline::protocol::api_keys::PRODUCE, 6);
    let cfg = ProducerConfig::bootstrap([mock.addr.clone()])
        .compression(Compression::Zstd)
        .zstd_level(ZstdLevel::new(1)?)
        .linger(Duration::ZERO);
    let producer = Producer::new(cfg).await?;
    assert!(matches!(
        producer
            .send(ProduceRecord::to("t").value(Bytes::from_static(b"v")))
            .await,
        Err(Error::Unsupported(_))
    ));
    assert_eq!(producer.metrics().bytes_buffered, 0);
    assert_eq!(mock.last_produce_version(), None);
    drop(producer.close().await);
    Ok(())
}
#[tokio::test]
async fn backend_input_limit_releases_buffer_memory_and_sequence_reservations() -> Result<(), Error>
{
    let mock = common::Mock::start().await;
    let cfg = ProducerConfig::bootstrap([mock.addr.clone()])
        .compression(Compression::Zstd)
        .zstd_level(ZstdLevel::new(1)?)
        .linger(Duration::ZERO)
        .idempotent(true)
        .max_request_size(0)
        .buffer_memory(128 * 1024 * 1024);
    let producer = Producer::new(cfg).await?;
    let result = producer
        .send(ProduceRecord::to("t").value(Bytes::from(vec![b'x'; 64 * 1024 * 1024])))
        .await;
    assert!(
        matches!(result,Err(Error::Protocol(ref message)) if message.contains("zstd record section exceeds 64 MiB")),
        "{result:?}"
    );
    assert_eq!(producer.metrics().records_queued, 1);
    assert_eq!(producer.metrics().bytes_buffered, 0);
    assert_eq!(mock.last_produce_version(), None);
    drop(producer.close().await);
    Ok(())
}

#[tokio::test]
#[ignore = "requires explicit fresh native Kafka topic and independent readers"]
async fn native_zstd_producer_levels() -> Result<(), Error> {
    let bootstrap = std::env::var("PL_ZSTD_NATIVE_BOOTSTRAP")
        .map_err(|_| Error::protocol("native bootstrap required"))?;
    let topic = std::env::var("PL_ZSTD_NATIVE_TOPIC")
        .map_err(|_| Error::protocol("native fresh topic required"))?;
    fn mix(mut value: u64) -> u64 {
        value = value.wrapping_add(0x9e3779b97f4a7c15);
        value = (value ^ (value >> 30)).wrapping_mul(0xbf58476d1ce4e5b9);
        value = (value ^ (value >> 27)).wrapping_mul(0x94d049bb133111eb);
        value ^ (value >> 31)
    }
    let mut id = 0u64;
    for (level, count) in [(1, 43), (3, 43), (19, 42)] {
        let cfg = ProducerConfig::bootstrap([bootstrap.clone()])
            .compression(Compression::Zstd)
            .zstd_level(ZstdLevel::new(level)?)
            .linger(Duration::ZERO);
        let producer = Producer::new(cfg).await?;
        for _ in 0..count {
            let mut key = id.to_be_bytes().to_vec();
            key.extend_from_slice(&mix(1592590337 ^ id).to_be_bytes());
            let mut state = 1592590337 ^ id.wrapping_mul(0x9e3779b97f4a7c15);
            let mut value = Vec::new();
            while value.len() < 100 {
                state = mix(state);
                value.extend_from_slice(&state.to_be_bytes());
            }
            value.truncate(100);
            let metadata = producer
                .send(
                    ProduceRecord::to(topic.clone())
                        .partition(0)
                        .key(Bytes::from(key))
                        .value(Bytes::from(value)),
                )
                .await?;
            assert_eq!(
                metadata.offset,
                i64::try_from(id).map_err(|_| Error::protocol("native ID overflow"))?
            );
            id += 1;
        }
        producer.flush().await?;
        producer.close().await?;
    }
    assert_eq!(id, 128);
    Ok(())
}

#[tokio::test]
async fn compression_expansion_obeys_configured_batch_limit_before_send() -> Result<(), Error> {
    let mock = common::Mock::start().await;
    let mut value = Vec::new();
    let mut state = 0x5eed0001u64;
    for _ in 0..16384 {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        value.extend_from_slice(&state.to_le_bytes());
    }
    let upper = usize::try_from(RecordBatch::estimate_batch_size_upper_bound(
        None,
        Some(&value),
        &[],
    )?)
    .map_err(|_| Error::protocol("test upper-bound overflow"))?;
    let cfg = ProducerConfig::bootstrap([mock.addr.clone()])
        .compression(Compression::Zstd)
        .zstd_level(ZstdLevel::new(1)?)
        .linger(Duration::ZERO)
        .max_request_size(upper);
    let producer = Producer::new(cfg).await?;
    let result = producer
        .send(ProduceRecord::to("t").value(Bytes::from(value)))
        .await;
    assert!(
        matches!(result,Err(Error::RecordTooLarge{size,max,config}) if size>max && max==u64::try_from(upper).unwrap_or_default() && config==Error::MAX_REQUEST_SIZE_CONFIG),
        "{result:?}"
    );
    assert_eq!(producer.metrics().records_queued, 1);
    assert_eq!(producer.metrics().bytes_buffered, 0);
    assert_eq!(mock.last_produce_version(), None);
    drop(producer.close().await);
    Ok(())
}

#[test]
fn raw_encoded_batch_limit_is_exact_and_transactional() -> Result<(), Error> {
    let reference = fixture();
    let batch = decode_record_batches_with_limit(&mut &reference[..], 1024 * 1024)?.remove(0);
    let mut scratch = CompressScratch::new();
    let mut good = BytesMut::new();
    write_record_batch_scratch(
        &mut good,
        &header(&batch)?,
        batch.records.iter().map(EncodeRecord::from_record),
        &mut scratch,
    )?;
    scratch.set_zstd_max_batch_bytes(good.len());
    let mut exact = BytesMut::new();
    write_record_batch_scratch(
        &mut exact,
        &header(&batch)?,
        batch.records.iter().map(EncodeRecord::from_record),
        &mut scratch,
    )?;
    assert_eq!(exact, good);
    scratch.set_zstd_max_batch_bytes(good.len() - 1);
    let mut out = BytesMut::from(&b"prefix"[..]);
    assert!(matches!(
        write_record_batch_scratch(
            &mut out,
            &header(&batch)?,
            batch.records.iter().map(EncodeRecord::from_record),
            &mut scratch
        ),
        Err(Error::RecordTooLarge { .. })
    ));
    assert_eq!(&out[..], b"prefix");
    Ok(())
}

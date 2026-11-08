//! Qualification-only all-success Produce timing with independent raw-journal readback.
#![allow(
    clippy::unwrap_used,
    clippy::indexing_slicing,
    reason = "bounded qualification fixture"
)]

#[path = "produce_dense_partitions.rs"]
mod dense_fixture;

use dense_fixture::{benchmark_indexes, TestResult};
use partitionline_broker::{
    catalog::{Catalog, TopicId},
    metadata::{self, Router},
    produce,
};
use std::{fs, time::Instant};

fn crc32c(bytes: &[u8]) -> u32 {
    let mut crc = !0_u32;
    for &byte in bytes {
        crc ^= u32::from(byte);
        for _ in 0..8 {
            let mask = 0_u32.wrapping_sub(crc & 1);
            crc = (crc >> 1) ^ (0x82f6_3b78 & mask);
        }
    }
    !crc
}

fn batch_for(partition: i32, iteration: usize) -> TestResult<Vec<u8>> {
    let mut batch = include_bytes!("fixtures/records/valid-basic.bin").to_vec();
    assert_eq!(batch.len(), 74);
    assert_eq!(&batch[61..], &[0x18, 0, 0, 0, 2, b'k', 2, b'v', 2, 2, b'h', 2, b'x']);
    assert_eq!(crc32c(&batch[21..]), u32::from_be_bytes(batch[17..21].try_into()?));
    batch[66] = u8::try_from(partition)?;
    batch[68] = u8::try_from(iteration)?;
    let checksum = crc32c(&batch[21..]);
    batch[17..21].copy_from_slice(&checksum.to_be_bytes());
    Ok(batch)
}

fn request(indexes: &[i32], iteration: usize) -> TestResult<Vec<u8>> {
    let mut out = Vec::new();
    out.extend_from_slice(&0_i16.to_be_bytes());
    out.extend_from_slice(&3_i16.to_be_bytes());
    out.extend_from_slice(&77_i32.to_be_bytes());
    out.extend_from_slice(&(-1_i16).to_be_bytes());
    out.extend_from_slice(&(-1_i16).to_be_bytes());
    out.extend_from_slice(&1_i16.to_be_bytes());
    out.extend_from_slice(&60_000_i32.to_be_bytes());
    out.extend_from_slice(&1_i32.to_be_bytes());
    out.extend_from_slice(&5_i16.to_be_bytes());
    out.extend_from_slice(b"alpha");
    out.extend_from_slice(&i32::try_from(indexes.len())?.to_be_bytes());
    for &index in indexes {
        let batch = batch_for(index, iteration)?;
        out.extend_from_slice(&index.to_be_bytes());
        out.extend_from_slice(&i32::try_from(batch.len())?.to_be_bytes());
        out.extend_from_slice(&batch);
    }
    Ok(out)
}

fn expected_success(indexes: &[i32], offset: usize) -> TestResult<Vec<u8>> {
    let mut out = Vec::new();
    out.extend_from_slice(&77_i32.to_be_bytes());
    out.extend_from_slice(&1_i32.to_be_bytes());
    out.extend_from_slice(&5_i16.to_be_bytes());
    out.extend_from_slice(b"alpha");
    out.extend_from_slice(&i32::try_from(indexes.len())?.to_be_bytes());
    for &index in indexes {
        out.extend_from_slice(&index.to_be_bytes());
        out.extend_from_slice(&0_i16.to_be_bytes());
        out.extend_from_slice(&i64::try_from(offset)?.to_be_bytes());
        out.extend_from_slice(&(-1_i64).to_be_bytes());
    }
    out.extend_from_slice(&0_i32.to_be_bytes());
    Ok(out)
}

fn verify_raw_journal(path: &std::path::Path, partition: i32, iterations: usize) -> TestResult<usize> {
    let bytes = fs::read(path)?;
    let len = 74_usize;
    let stride = 32_usize + len;
    assert_eq!(bytes.len(), 24 + (iterations + 1) * stride);
    assert_eq!(&bytes[..8], b"PLJRNL01");
    assert_eq!(u64::from_be_bytes(bytes[8..16].try_into()?), 0);
    assert_eq!(&bytes[16..20], &[0, 0, 0, 0]);
    assert_eq!(u32::from_be_bytes(bytes[20..24].try_into()?), crc32c(&bytes[..20]));
    for offset in 0..=iterations {
        let start = 24 + offset * stride;
        let header = &bytes[start..start + 32];
        let payload = &bytes[start + 32..start + stride];
        assert_eq!(&header[..8], b"PLENTRY1");
        assert_eq!(u32::from_be_bytes(header[8..12].try_into()?), u32::try_from(len)?);
        assert_eq!(u64::from_be_bytes(header[12..20].try_into()?), u64::try_from(offset)?);
        assert_eq!(u32::from_be_bytes(header[20..24].try_into()?), 1);
        assert_eq!(u32::from_be_bytes(header[24..28].try_into()?), crc32c(payload));
        assert_eq!(u32::from_be_bytes(header[28..32].try_into()?), crc32c(&header[..28]));
        let mut expected = batch_for(partition, offset)?;
        expected[..8].copy_from_slice(&i64::try_from(offset)?.to_be_bytes());
        expected[12..16].copy_from_slice(&0_i32.to_be_bytes());
        assert_eq!(payload, expected);
    }
    Ok(bytes.len())
}

#[tokio::test(flavor = "current_thread")]
#[ignore = "paired all-valid Produce timing; qualification workflow only"]
async fn produce_dense_success_all_timing() -> TestResult {
    assert_eq!(crc32c(b"123456789"), 0xe306_9283);
    let count: usize = std::env::var("PL_DENSE_COUNT")?.parse()?;
    let iterations: usize = std::env::var("PL_DENSE_ITERATIONS")?.parse()?;
    let shape = std::env::var("PL_DENSE_SHAPE")?;
    assert!([17, 32].contains(&count));
    assert!((1..=64).contains(&iterations));
    let indexes = benchmark_indexes(count, &shape)?;
    let mut sorted = indexes.clone();
    sorted.sort_unstable();
    assert_eq!(sorted, (0..i32::try_from(count)?).collect::<Vec<_>>());

    let mut inputs = Vec::with_capacity(iterations + 1);
    let mut outputs = Vec::with_capacity(iterations + 1);
    for iteration in 0..=iterations {
        inputs.push(request(&indexes, iteration)?);
        outputs.push(expected_success(&indexes, iteration)?);
    }
    let temp = std::env::temp_dir();
    let expected_temp = std::path::PathBuf::from(std::env::var("PL_DENSE_EXPECT_TEMP")?);
    assert_eq!(fs::canonicalize(&temp)?, fs::canonicalize(expected_temp)?);
    let setup = Instant::now();
    let root = temp.join(format!(
        "pl-dense-success-{}-{}",
        std::process::id(),
        std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH)?.as_nanos()
    ));
    fs::create_dir(&root)?;
    let common = metadata::Config::new(0, "127.0.0.1".into(), 9092, "dense".into());
    let (mut catalog, _) = Catalog::open(root.join("catalog"), common.catalog_limits)?;
    let mut raw_id = [0; 16];
    raw_id[15] = 2;
    catalog.create("alpha", TopicId::new(raw_id)?, u32::try_from(count)?)?;
    assert_eq!(catalog.by_name("alpha").map(|topic| topic.partition_count()), Some(u32::try_from(count)?));
    drop(catalog);

    let config = produce::Config::new(root.join("data"));
    assert!(config.segment_limits.is_none());
    assert!(config.max_stores >= count);
    assert!(config.max_partitions >= count);
    assert_eq!(config.max_sync_concurrency, 4);
    let sync_concurrency = config.max_sync_concurrency;
    let (router, _) = Router::open_with_store(root.join("catalog"), common, config).await?;
    assert_eq!(router.dispatch(inputs[0].clone()).await?.ok_or("missing warmup")?, outputs[0]);
    let setup_ns = setup.elapsed().as_nanos();
    let start = Instant::now();
    for iteration in 1..=iterations {
        let response = router.dispatch(inputs[iteration].clone()).await?.ok_or("missing response")?;
        assert_eq!(response, outputs[iteration]);
    }
    let elapsed_ns = start.elapsed().as_nanos();
    let shutdown = Instant::now();
    router.shutdown().await?;
    drop(router);
    let shutdown_ns = shutdown.elapsed().as_nanos();
    let verify = Instant::now();
    let mut read_bytes = 0usize;
    for index in 0..i32::try_from(count)? {
        let path = root.join(format!("data/00000000000000000000000000000002-{index}.journal"));
        read_bytes += verify_raw_journal(&path, index, iterations)?;
    }
    let verify_ns = verify.elapsed().as_nanos();
    let cleanup = Instant::now();
    fs::remove_dir_all(root)?;
    let cleanup_ns = cleanup.elapsed().as_nanos();
    println!("DENSE_SUCCESS shape={shape} count={count} iterations={iterations} elapsed_ns={elapsed_ns} setup_ns={setup_ns} shutdown_ns={shutdown_ns} verify_ns={verify_ns} cleanup_ns={cleanup_ns} verified_records={} verified_partitions={count} read_bytes={read_bytes} sync_concurrency={sync_concurrency} temp={}", count * (iterations + 1), temp.display());
    Ok(())
}

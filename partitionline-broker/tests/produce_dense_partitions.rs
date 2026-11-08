//! Public Produce duplicate-partition controls and a scoped router benchmark.
#![allow(
    clippy::unwrap_used,
    clippy::indexing_slicing,
    reason = "bounded independent test fixtures"
)]

use partitionline_broker::{
    catalog::{Catalog, TopicId},
    metadata::{self, Router},
    partition, produce,
};
use std::{
    collections::{BTreeMap, BTreeSet},
    error::Error,
    fs,
    path::PathBuf,
    sync::atomic::{AtomicU64, Ordering},
    time::{Instant, SystemTime, UNIX_EPOCH},
};

static NEXT: AtomicU64 = AtomicU64::new(0);
pub(crate) type TestResult<T = ()> = Result<T, Box<dyn Error>>;

pub(crate) fn request(
    indexes: &[i32],
    topic: &str,
    acks: i16,
    timeout: i32,
    transaction: Option<&str>,
    record_batch: Option<&[u8]>,
) -> TestResult<Vec<u8>> {
    let mut out = Vec::new();
    out.extend_from_slice(&0_i16.to_be_bytes());
    out.extend_from_slice(&3_i16.to_be_bytes());
    out.extend_from_slice(&77_i32.to_be_bytes());
    out.extend_from_slice(&(-1_i16).to_be_bytes());
    if let Some(transaction) = transaction {
        out.extend_from_slice(&i16::try_from(transaction.len())?.to_be_bytes());
        out.extend_from_slice(transaction.as_bytes());
    } else {
        out.extend_from_slice(&(-1_i16).to_be_bytes());
    }
    out.extend_from_slice(&acks.to_be_bytes());
    out.extend_from_slice(&timeout.to_be_bytes());
    out.extend_from_slice(&1_i32.to_be_bytes());
    out.extend_from_slice(&i16::try_from(topic.len())?.to_be_bytes());
    out.extend_from_slice(topic.as_bytes());
    out.extend_from_slice(&i32::try_from(indexes.len())?.to_be_bytes());
    for index in indexes {
        out.extend_from_slice(&index.to_be_bytes());
        if let Some(batch) = record_batch {
            out.extend_from_slice(&i32::try_from(batch.len())?.to_be_bytes());
            out.extend_from_slice(batch);
        } else {
            out.extend_from_slice(&(-1_i32).to_be_bytes());
        }
    }
    Ok(out)
}

// Independent wire oracle for one Produce v3 topic. The sorted map supplies
// counts; the set keeps only each partition's first response in request order.
pub(crate) fn expected(
    indexes: &[i32],
    topic: &str,
    acks: i16,
    timeout: i32,
    transaction: Option<&str>,
    has_batch: bool,
    base_offsets: [i64; 2],
) -> TestResult<Vec<u8>> {
    let mut counts = BTreeMap::new();
    for index in indexes {
        *counts.entry(*index).or_insert(0usize) += 1;
    }
    let mut seen = BTreeSet::new();
    let mut entries = Vec::new();
    for index in indexes {
        if !seen.insert(*index) {
            continue;
        }
        let repeated = counts.get(index).copied().unwrap_or(0) > 1;
        let error = if !(-1..=1).contains(&acks) {
            21_i16
        } else if timeout < 0 {
            42
        } else if transaction.is_some() {
            43
        } else if repeated {
            42
        } else if topic != "alpha" || *index < 0 || *index >= 2 {
            3
        } else if !has_batch {
            87
        } else {
            0
        };
        entries.push((*index, error));
    }
    let mut out = Vec::new();
    out.extend_from_slice(&77_i32.to_be_bytes());
    out.extend_from_slice(&1_i32.to_be_bytes());
    out.extend_from_slice(&i16::try_from(topic.len())?.to_be_bytes());
    out.extend_from_slice(topic.as_bytes());
    out.extend_from_slice(&i32::try_from(entries.len())?.to_be_bytes());
    for (index, error) in entries {
        out.extend_from_slice(&index.to_be_bytes());
        out.extend_from_slice(&error.to_be_bytes());
        let base = if error == 0 {
            let position = usize::try_from(index)?;
            *base_offsets.get(position).ok_or("unexpected successful partition")?
        } else {
            -1
        };
        out.extend_from_slice(&base.to_be_bytes());
        out.extend_from_slice(&(-1_i64).to_be_bytes());
    }
    out.extend_from_slice(&0_i32.to_be_bytes());
    Ok(out)
}

pub(crate) async fn router(label: &str) -> TestResult<(PathBuf, Router)> {
    let nanos = SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos();
    let root = std::env::temp_dir().join(format!(
        "pl-produce-dense-{}-{nanos}-{}-{label}",
        std::process::id(),
        NEXT.fetch_add(1, Ordering::Relaxed)
    ));
    fs::create_dir(&root)?;
    let common = metadata::Config::new(0, "127.0.0.1".into(), 9092, "dense".into());
    let (mut catalog, _) = Catalog::open(root.join("catalog"), common.catalog_limits)?;
    let mut raw_id = [0; 16];
    raw_id[15] = 2;
    catalog.create("alpha", TopicId::new(raw_id)?, 2)?;
    drop(catalog);
    let mut config = produce::Config::new(root.join("data"));
    config.max_partitions = 4096;
    let (router, _) = Router::open_with_store(root.join("catalog"), common, config).await?;
    Ok((root, router))
}

#[allow(clippy::too_many_arguments, reason = "independent wire fixture options")]
async fn assert_response(
    router: &Router,
    indexes: &[i32],
    topic: &str,
    acks: i16,
    timeout: i32,
    transaction: Option<&str>,
    batch: Option<&[u8]>,
    base_offsets: [i64; 2],
) -> TestResult {
    let bytes = request(indexes, topic, acks, timeout, transaction, batch)?;
    let want = expected(indexes, topic, acks, timeout, transaction, batch.is_some(), base_offsets)?;
    let got = router.dispatch(bytes).await?.ok_or("missing response")?;
    assert_eq!(got, want);
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn produce_dense_public_precedence_order_and_readback() -> TestResult {
    let (root, router) = router("controls").await?;
    for size in [16usize, 17, 32, 256] {
        let mut values: Vec<i32> = (0..size).rev().map(|value| value as i32).collect();
        assert_response(&router, &values, "omega", 1, 60_000, None, None, [0, 0]).await?;
        values[size - 1] = values[0];
        assert_response(&router, &values, "omega", 1, 60_000, None, None, [0, 0]).await?;
        values[size - 1] = -1;
        assert_response(&router, &values, "omega", 1, 60_000, None, None, [0, 0]).await?;
        values[size - 1] = i32::MAX;
        assert_response(&router, &values, "omega", 1, 60_000, None, None, [0, 0]).await?;
    }
    let mut duplicate: Vec<i32> = (0..32).rev().collect();
    duplicate[31] = duplicate[0];
    assert_response(&router, &duplicate, "alpha", 2, 60_000, None, None, [0, 0]).await?;
    assert_response(&router, &duplicate, "alpha", 1, -1, Some("tx"), None, [0, 0]).await?;
    assert_response(&router, &duplicate, "alpha", 1, 60_000, Some("tx"), None, [0, 0]).await?;
    let no_ack = request(&duplicate, "alpha", 0, 60_000, None, None)?;
    assert!(matches!(
        router.dispatch(no_ack).await,
        Err(metadata::Error::Produce(produce::Error::AcksZeroError))
    ));
    let batch = include_bytes!("fixtures/records/valid-basic.bin");
    let indexes: Vec<i32> = (0..32).rev().collect();
    let valid = request(&indexes, "alpha", 1, 60_000, None, Some(batch))?;
    assert!(router.dispatch(valid[..valid.len() - 1].to_vec()).await.is_err());
    assert_response(&router, &indexes, "alpha", 1, 60_000, None, Some(batch), [0, 0]).await?;
    let mut duplicate_valid = indexes.clone();
    duplicate_valid.push(0);
    assert_response(&router, &duplicate_valid, "alpha", 1, 60_000, None, Some(batch), [1, 1]).await?;
    assert_response(&router, &duplicate, "alpha", 2, 60_000, None, Some(batch), [0, 0]).await?;
    assert_response(&router, &indexes, "alpha", 1, 60_000, None, Some(batch), [1, 2]).await?;
    router.shutdown().await?;
    drop(router);

    let config = produce::Config::new(root.join("data"));
    for index in [0, 1] {
        let path = root.join(format!("data/00000000000000000000000000000002-{index}.journal"));
        let (mut log, _) =
            partition::Partition::open(path, 0, config.journal_limits, config.record_limits)?;
        let entries = log.fetch(0, 4, 1 << 20)?;
        assert_eq!(entries.len(), if index == 0 { 2 } else { 3 });
        for (offset, entry) in entries.iter().enumerate() {
            assert_eq!(entry.first_offset, offset as u64);
            assert_eq!(entry.record_count, 1);
            let mut want = batch.to_vec();
            want[..8].copy_from_slice(&(offset as i64).to_be_bytes());
            want[12..16].copy_from_slice(&0_i32.to_be_bytes());
            assert_eq!(entry.payload, want);
        }
    }
    fs::remove_dir_all(root)?;
    Ok(())
}

pub(crate) fn benchmark_indexes(count: usize, shape: &str) -> TestResult<Vec<i32>> {
    let mut indexes: Vec<i32> = (0..count).map(|value| value as i32).collect();
    match shape {
        "ascending" => {}
        "reversed" => indexes.reverse(),
        "rotate" => indexes.rotate_left(count / 3),
        "duplicates" => {
            for (position, value) in indexes.iter_mut().enumerate() {
                *value = (position % 7) as i32;
            }
            indexes.reverse();
        }
        "sparse_last" => {
            indexes.reverse();
            if let Some(last) = indexes.last_mut() {
                *last = i32::MAX;
            }
        }
        "negative_last" => {
            indexes.reverse();
            if let Some(last) = indexes.last_mut() {
                *last = -1;
            }
        }
        _ => return Err("unknown benchmark shape".into()),
    }
    Ok(indexes)
}

#[tokio::test(flavor = "current_thread")]
#[ignore = "paired router timing; run only by the qualification workflow"]
async fn produce_dense_router_timing() -> TestResult {
    let count: usize = std::env::var("PL_DENSE_COUNT")?.parse()?;
    let iterations: usize = std::env::var("PL_DENSE_ITERATIONS")?.parse()?;
    let shape = std::env::var("PL_DENSE_SHAPE")?;
    assert!((1..=4096).contains(&count));
    assert!(iterations > 0);
    let indexes = benchmark_indexes(count, &shape)?;
    let input = request(&indexes, "omega", 1, 60_000, None, None)?;
    let want = expected(&indexes, "omega", 1, 60_000, None, false, [0, 0])?;
    let (root, router) = router("timing").await?;
    let warm = router.dispatch(input.clone()).await?.ok_or("missing warmup")?;
    assert_eq!(warm, want);
    let start = Instant::now();
    for _ in 0..iterations {
        let response = router.dispatch(input.clone()).await?.ok_or("missing response")?;
        assert_eq!(response, want);
    }
    let elapsed_ns = start.elapsed().as_nanos();
    router.shutdown().await?;
    fs::remove_dir_all(root)?;
    println!(
        "DENSE_ROUTER shape={shape} count={count} iterations={iterations} elapsed_ns={elapsed_ns}"
    );
    Ok(())
}

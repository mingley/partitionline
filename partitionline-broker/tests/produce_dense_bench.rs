//! Scoped Produce router timing with actual staged writes and readback.
#![allow(
    clippy::unwrap_used,
    clippy::indexing_slicing,
    reason = "bounded qualification fixture"
)]

#[path = "produce_dense_partitions.rs"]
mod dense_fixture;

use dense_fixture::{benchmark_indexes, expected, request, router, TestResult};
use partitionline_broker::{partition, produce};
use std::{fs, time::Instant};

#[tokio::test(flavor = "current_thread")]
#[ignore = "paired durable router timing; run only by the qualification workflow"]
async fn produce_dense_durable_timing() -> TestResult {
    let count: usize = std::env::var("PL_DENSE_COUNT")?.parse()?;
    let iterations: usize = std::env::var("PL_DENSE_ITERATIONS")?.parse()?;
    let shape = std::env::var("PL_DENSE_SHAPE")?;
    assert!((17..=1024).contains(&count));
    assert!((1..=100).contains(&iterations));
    let indexes = benchmark_indexes(count, &shape)?;
    let batch = include_bytes!("fixtures/records/valid-basic.bin");
    let input = request(&indexes, "alpha", 1, 60_000, None, Some(batch))?;
    let warm = expected(&indexes, "alpha", 1, 60_000, None, true, [0, 0])?;
    let mut outcomes = Vec::with_capacity(iterations);
    for iteration in 0..iterations {
        let base = i64::try_from(iteration + 1)?;
        outcomes.push(expected(&indexes, "alpha", 1, 60_000, None, true, [base, base])?);
    }
    let (root, router) = router("durable-timing").await?;
    assert_eq!(router.dispatch(input.clone()).await?.ok_or("missing warmup")?, warm);
    let start = Instant::now();
    for want in &outcomes {
        let response = router.dispatch(input.clone()).await?.ok_or("missing response")?;
        assert_eq!(&response, want);
    }
    let elapsed_ns = start.elapsed().as_nanos();
    router.shutdown().await?;
    drop(router);

    let config = produce::Config::new(root.join("data"));
    for index in [0, 1] {
        let path = root.join(format!("data/00000000000000000000000000000002-{index}.journal"));
        let (mut log, _) =
            partition::Partition::open(path, 0, config.journal_limits, config.record_limits)?;
        let entries = log.fetch(0, iterations + 2, 1 << 20)?;
        assert_eq!(entries.len(), iterations + 1);
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
    println!(
        "DENSE_DURABLE shape={shape} count={count} iterations={iterations} verified_records={} elapsed_ns={elapsed_ns}",
        2 * (iterations + 1)
    );
    Ok(())
}

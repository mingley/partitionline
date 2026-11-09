//! Qualification workload for ordinary journal-backed Kafka Fetch v6.
use partitionline_broker::{
    catalog::{Catalog, TopicId}, fetch, metadata::{self, Router}, partition, produce,
};
use std::{error::Error, fs, hint::black_box, path::Path, time::Instant};

const ENTRIES: usize = 128;
fn common() -> metadata::Config {
    metadata::Config::new(0, "127.0.0.1".into(), 19095, "bench".into())
}
fn vint(mut n: u32, out: &mut Vec<u8>) {
    loop {
        let byte = (n & 0x7f) as u8;
        n >>= 7;
        out.push(byte | if n != 0 { 0x80 } else { 0 });
        if n == 0 { return; }
    }
}
fn batch(value_len: usize) -> Vec<u8> {
    let mut record = vec![0, 0, 0, 1]; // attributes, timestamp, offset, null key
    vint((value_len as u32) << 1, &mut record);
    record.extend(std::iter::repeat_n(b'x', value_len));
    record.push(0); // no headers
    let mut b = Vec::new();
    b.extend_from_slice(&0i64.to_be_bytes());
    b.extend_from_slice(&0i32.to_be_bytes());
    b.extend_from_slice(&0i32.to_be_bytes());
    b.push(2);
    b.extend_from_slice(&0u32.to_be_bytes());
    b.extend_from_slice(&0i16.to_be_bytes());
    b.extend_from_slice(&0i32.to_be_bytes());
    b.extend_from_slice(&0i64.to_be_bytes());
    b.extend_from_slice(&0i64.to_be_bytes());
    b.extend_from_slice(&(-1i64).to_be_bytes());
    b.extend_from_slice(&(-1i16).to_be_bytes());
    b.extend_from_slice(&(-1i32).to_be_bytes());
    b.extend_from_slice(&1i32.to_be_bytes());
    vint((record.len() as u32) << 1, &mut b);
    b.extend_from_slice(&record);
    let length = (b.len() - 12) as i32;
    b[8..12].copy_from_slice(&length.to_be_bytes());
    let crc = crc32c::crc32c(&b[21..]);
    b[17..21].copy_from_slice(&crc.to_be_bytes());
    b
}
fn seed(root: &Path, input: &[u8]) -> Result<(), Box<dyn Error>> {
    fs::create_dir_all(root)?;
    let (mut catalog, _) = Catalog::open(root.join("catalog.journal"), common().catalog_limits)?;
    let mut id = [0; 16]; id[15] = 2;
    catalog.create("alpha", TopicId::new(id)?, 1)?;
    drop(catalog);
    fs::create_dir(root.join("partitions"))?;
    let cfg = produce::Config::new(root.join("partitions"));
    let (mut part, _) = partition::Partition::open(
        root.join("partitions/00000000000000000000000000000002-0.journal"),
        0, cfg.journal_limits, cfg.record_limits,
    )?;
    for _ in 0..ENTRIES { part.append(input)?; }
    Ok(())
}
fn string(out: &mut Vec<u8>, s: &str) {
    out.extend_from_slice(&(s.len() as i16).to_be_bytes());
    out.extend_from_slice(s.as_bytes());
}
fn request(nbytes: usize) -> Vec<u8> {
    let mut b = Vec::new();
    b.extend_from_slice(&1i16.to_be_bytes()); // Fetch
    b.extend_from_slice(&6i16.to_be_bytes());
    b.extend_from_slice(&77i32.to_be_bytes());
    b.extend_from_slice(&(-1i16).to_be_bytes());
    for x in [-1, 0, 0, nbytes as i32] { b.extend_from_slice(&x.to_be_bytes()); }
    b.push(0); // read uncommitted
    b.extend_from_slice(&1i32.to_be_bytes());
    string(&mut b, "alpha");
    b.extend_from_slice(&1i32.to_be_bytes());
    b.extend_from_slice(&0i32.to_be_bytes());
    b.extend_from_slice(&0i64.to_be_bytes());
    b.extend_from_slice(&0i64.to_be_bytes());
    b.extend_from_slice(&(nbytes as i32).to_be_bytes());
    b
}
fn expected(input: &[u8], count: usize) -> Vec<u8> {
    let mut b = Vec::new();
    b.extend_from_slice(&77i32.to_be_bytes());
    b.extend_from_slice(&0i32.to_be_bytes());
    b.extend_from_slice(&1i32.to_be_bytes());
    string(&mut b, "alpha");
    b.extend_from_slice(&1i32.to_be_bytes());
    b.extend_from_slice(&0i32.to_be_bytes());
    b.extend_from_slice(&0i16.to_be_bytes());
    b.extend_from_slice(&(ENTRIES as i64).to_be_bytes()); // high watermark
    b.extend_from_slice(&(ENTRIES as i64).to_be_bytes()); // last stable offset
    b.extend_from_slice(&0i64.to_be_bytes()); // log start
    b.extend_from_slice(&(-1i32).to_be_bytes()); // aborted transactions
    b.extend_from_slice(&((input.len() * count) as i32).to_be_bytes());
    for offset in 0..count {
        let mut item = input.to_vec();
        item[..8].copy_from_slice(&(offset as i64).to_be_bytes());
        b.extend_from_slice(&item);
    }
    b
}

#[tokio::test]
#[ignore = "qualification workload; expects PL_FETCH_BENCH_DIR"]
async fn successful_fetch_bench() -> Result<(), Box<dyn Error>> {
    let directory = std::path::PathBuf::from(std::env::var_os("PL_FETCH_BENCH_DIR").ok_or("PL_FETCH_BENCH_DIR")?);
    let seeding = std::env::var_os("PL_FETCH_BENCH_SEED").is_some();
    let n: usize = std::env::var("PL_FETCH_BENCH_ITERS").unwrap_or("80".into()).parse()?;
    for value_size in [8usize, 4096, 16384] {
        let input = batch(value_size);
        let root = directory.join(value_size.to_string());
        if seeding { seed(&root, &input)?; }
        let router = Router::open_with_read_store(
            root.join("catalog.journal"), common(), produce::Config::new(root.join("partitions")),
            fetch::Limits::default(),
        ).await?.0;
        for count in [1usize, 16, 128] {
            let request = request(input.len() * count);
            let expected = expected(&input, count);
            assert_eq!(router.respond(request.clone()).await?, expected);
            for _ in 0..8 { black_box(router.respond(request.clone()).await?); }
            let start = Instant::now();
            for _ in 0..n {
                let result = router.respond(request.clone()).await?;
                assert_eq!(result.len(), expected.len());
                black_box(result);
            }
            let ns = start.elapsed().as_nanos();
            assert_eq!(router.respond(request).await?, expected);
            println!("PL_FETCH_SAMPLE value_bytes={value_size} entries={count} iterations={n} ns={ns} response_bytes={}", expected.len());
        }
        router.shutdown().await?;
    }
    Ok(())
}

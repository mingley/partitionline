//! Paired Router benchmark. See .github/workflows/router-paired-perf.yml.
use partitionline_broker::{
    catalog::{Catalog, TopicId}, fetch,
    metadata::{self, Router}, produce,
};
use std::{
    fs,
    path::Path,
    sync::Arc,
    time::Instant,
};
use tokio::sync::Barrier;

const WORKERS: usize = 8;
const WARMUP: usize = 25;
const PRODUCE_SAMPLES: usize = 300;
const FETCH_SAMPLES: usize = 500;
const BATCH: &[u8] = include_bytes!("../tests/fixtures/fetch/4.3.1/log-batch-3.bin");

fn header(key: i16, version: i16) -> Vec<u8> {
    let mut b = Vec::new();
    b.extend_from_slice(&key.to_be_bytes());
    b.extend_from_slice(&version.to_be_bytes());
    b.extend_from_slice(&77i32.to_be_bytes());
    b.extend_from_slice(&(-1i16).to_be_bytes());
    b
}
fn string(b: &mut Vec<u8>, s: &str) {
    b.extend_from_slice(&(s.len() as i16).to_be_bytes());
    b.extend_from_slice(s.as_bytes());
}
fn produce_request() -> Vec<u8> {
    let mut b = header(0, 3);
    b.extend_from_slice(&(-1i16).to_be_bytes()); // transactional id: null
    b.extend_from_slice(&1i16.to_be_bytes()); // acks=1; local durable sync
    b.extend_from_slice(&60000i32.to_be_bytes());
    b.extend_from_slice(&1i32.to_be_bytes());
    string(&mut b, "alpha");
    b.extend_from_slice(&1i32.to_be_bytes());
    b.extend_from_slice(&0i32.to_be_bytes());
    b.extend_from_slice(&(BATCH.len() as i32).to_be_bytes());
    b.extend_from_slice(BATCH);
    b
}
fn fetch_request(max: i32) -> Vec<u8> {
    let mut b = header(1, 6);
    for x in [-1, 0, 0, max] { b.extend_from_slice(&x.to_be_bytes()); }
    b.push(0); // read uncommitted
    b.extend_from_slice(&1i32.to_be_bytes());
    string(&mut b, "alpha");
    b.extend_from_slice(&1i32.to_be_bytes());
    b.extend_from_slice(&0i32.to_be_bytes());
    b.extend_from_slice(&0i64.to_be_bytes()); // fetch offset
    b.extend_from_slice(&0i64.to_be_bytes()); // log start
    b.extend_from_slice(&max.to_be_bytes());
    b
}
fn i16_at(b: &[u8], p: usize) -> i16 { i16::from_be_bytes(b[p..p + 2].try_into().unwrap()) }
fn i32_at(b: &[u8], p: usize) -> i32 { i32::from_be_bytes(b[p..p + 4].try_into().unwrap()) }
fn i64_at(b: &[u8], p: usize) -> i64 { i64::from_be_bytes(b[p..p + 8].try_into().unwrap()) }
fn check_ack(b: &[u8]) -> i64 {
    assert!(b.len() >= 33);
    assert_eq!(i32_at(b, 0), 77);
    assert_eq!(i32_at(b, 4), 1);
    assert_eq!(i16_at(b, 8), 5);
    assert_eq!(&b[10..15], b"alpha");
    assert_eq!(i32_at(b, 15), 1);
    assert_eq!(i32_at(b, 19), 0);
    assert_eq!(i16_at(b, 23), 0, "Produce partition error");
    let offset = i64_at(b, 25);
    assert!(offset >= 0);
    offset
}
fn check_fetch<'a>(b: &'a [u8], watermark: i64) -> &'a [u8] {
    assert!(b.len() >= 61);
    assert_eq!(i32_at(b, 0), 77);
    assert_eq!(i32_at(b, 8), 1);
    assert_eq!(i16_at(b, 12), 5);
    assert_eq!(&b[14..19], b"alpha");
    assert_eq!(i32_at(b, 19), 1);
    assert_eq!(i32_at(b, 23), 0);
    assert_eq!(i16_at(b, 27), 0, "Fetch partition error");
    assert_eq!(i64_at(b, 29), watermark, "high watermark");
    assert_eq!(i64_at(b, 45), 0, "log start offset");
    let n = i32_at(b, 57);
    assert!(n >= 0);
    assert_eq!(b.len(), 61 + n as usize);
    &b[61..]
}
fn expected_batch(offset: i64) -> Vec<u8> {
    let mut b = BATCH.to_vec();
    b[..8].copy_from_slice(&offset.to_be_bytes());
    b[12..16].copy_from_slice(&0i32.to_be_bytes());
    b
}
fn expected_history(count: usize) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(count * BATCH.len());
    for n in 0..count { bytes.extend_from_slice(&expected_batch(n as i64)); }
    bytes
}
fn hex(b: &[u8]) -> String {
    b.iter().map(|x| format!("{x:02x}")).collect()
}
fn stats(kind: &str, elapsed: u128, samples: &[(usize, usize, u128, i64)]) {
    let mut values: Vec<u128> = samples.iter().map(|x| x.2).collect();
    values.sort_unstable();
    let p = |pct: usize| values[((values.len() * pct + 99) / 100).saturating_sub(1)];
    println!("summary,{kind},{},{elapsed},{:.3},{},{},{},{}",
        values.len(), values.len() as f64 * 1_000_000_000.0 / elapsed as f64,
        p(50), p(95), p(99), values[values.len() - 1]);
    for &(worker, seq, ns, offset) in samples {
        println!("sample,{kind},{worker},{seq},{ns},{offset}");
    }
}
async fn produce_phase(router: Arc<Router>, count: usize) -> (u128, Vec<(usize, usize, u128, i64)>) {
    let gate = Arc::new(Barrier::new(WORKERS + 1));
    let request = Arc::new(produce_request());
    let mut tasks = Vec::new();
    for worker in 0..WORKERS {
        let (router, gate, request) = (router.clone(), gate.clone(), request.clone());
        tasks.push(tokio::spawn(async move {
            let mut out = Vec::with_capacity(count);
            gate.wait().await;
            for seq in 0..count {
                let input = request.as_ref().clone();
                let start = Instant::now();
                let response = router.respond(input).await.expect("Produce dispatch");
                let ns = start.elapsed().as_nanos();
                out.push((worker, seq, ns, check_ack(&response)));
            }
            out
        }));
    }
    let start = Instant::now();
    gate.wait().await;
    let mut samples = Vec::with_capacity(WORKERS * count);
    for task in tasks { samples.extend(task.await.expect("Produce worker")); }
    (start.elapsed().as_nanos(), samples)
}
async fn fetch_phase(router: Arc<Router>, watermark: i64) -> (u128, Vec<(usize, usize, u128, i64)>) {
    let gate = Arc::new(Barrier::new(WORKERS + 1));
    let request = Arc::new(fetch_request(1));
    let expected = Arc::new(expected_batch(0));
    let mut tasks = Vec::new();
    for worker in 0..WORKERS {
        let (router, gate, request, expected) = (router.clone(), gate.clone(), request.clone(), expected.clone());
        tasks.push(tokio::spawn(async move {
            let mut out = Vec::with_capacity(FETCH_SAMPLES);
            gate.wait().await;
            for seq in 0..FETCH_SAMPLES {
                let input = request.as_ref().clone();
                let start = Instant::now();
                let response = router.respond(input).await.expect("Fetch dispatch");
                let ns = start.elapsed().as_nanos();
                assert_eq!(check_fetch(&response, watermark), expected.as_slice(), "Fetch bytes");
                out.push((worker, seq, ns, 0));
            }
            out
        }));
    }
    let start = Instant::now();
    gate.wait().await;
    let mut samples = Vec::with_capacity(WORKERS * FETCH_SAMPLES);
    for task in tasks { samples.extend(task.await.expect("Fetch worker")); }
    (start.elapsed().as_nanos(), samples)
}
async fn open(root: &Path, config: metadata::Config) -> Router {
    Router::open_with_read_store(
        root.join("catalog.journal"), config,
        produce::Config::new(root.join("partitions")), fetch::Limits::default(),
    ).await.expect("open Router").0
}
#[tokio::main(flavor = "multi_thread", worker_threads = 2)]
async fn main() {
    assert_eq!(BATCH.len(), 78);
    assert_eq!(i32_at(BATCH, 57), 1);
    let root = std::path::PathBuf::from(std::env::var_os("BENCH_ROOT").expect("BENCH_ROOT"));
    fs::create_dir(&root).expect("fresh benchmark root");
    let config = metadata::Config::new(0, "127.0.0.1".into(), 19095, "paired-router-benchmark".into());
    {
        let (mut catalog, _) = Catalog::open(root.join("catalog.journal"), config.catalog_limits).expect("catalog");
        let mut id = [0u8; 16];
        id[15] = 2;
        catalog.create("alpha", TopicId::new(id).expect("topic id"), 1).expect("create topic");
    }
    println!("info,workers,{WORKERS},warmup_per_worker,{WARMUP},produce_per_worker,{PRODUCE_SAMPLES},fetch_per_worker,{FETCH_SAMPLES}");
    println!("info,produce_request_hex,{}", hex(&produce_request()));
    println!("info,fetch_request_hex,{}", hex(&fetch_request(1)));
    let router = Arc::new(open(&root, config.clone()).await);
    let (_, warm) = produce_phase(router.clone(), WARMUP).await;
    let (produce_elapsed, produce_samples) = produce_phase(router.clone(), PRODUCE_SAMPLES).await;
    let count = WORKERS * (WARMUP + PRODUCE_SAMPLES);
    let mut offsets: Vec<i64> = warm.iter().chain(produce_samples.iter()).map(|x| x.3).collect();
    offsets.sort_unstable();
    assert_eq!(offsets, (0..count as i64).collect::<Vec<_>>(), "all acks contiguous and unique");
    let expected = expected_history(count);
    let whole = router.respond(fetch_request(1024 * 1024)).await.expect("full history Fetch");
    assert_eq!(check_fetch(&whole, count as i64), expected.as_slice(), "complete fetched history");
    let (fetch_elapsed, fetch_samples) = fetch_phase(router.clone(), count as i64).await;
    router.shutdown().await.expect("shutdown");
    drop(router);
    let reopened = open(&root, config).await;
    let recovered = reopened.respond(fetch_request(1024 * 1024)).await.expect("recovered history Fetch");
    assert_eq!(check_fetch(&recovered, count as i64), expected.as_slice(), "recovered fetched history");
    reopened.shutdown().await.expect("reopened shutdown");
    drop(reopened);
    fs::remove_dir_all(&root).expect("remove benchmark root");
    println!("validated,produce_acks,{count},unique_offsets,0..{},fetch_watermark,{count},reopened_history_bytes,{}", count - 1, expected.len());
    println!("columns,summary,kind,count,elapsed_ns,requests_per_second,p50_ns,p95_ns,p99_ns,max_ns");
    println!("columns,sample,kind,worker,sequence,latency_ns,offset");
    stats("produce", produce_elapsed, &produce_samples);
    stats("fetch", fetch_elapsed, &fetch_samples);
}

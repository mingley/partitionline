//! Public Router poll timing witness; not a throughput benchmark.
use partitionline_broker::{
    fetch, metadata::{self, Router}, produce, protocol,
    transport::{Handler, Peer},
};
use std::{fs, future::Future, path::PathBuf, task::Poll, time::Instant};

const MAXIMUM: usize = 64 * 1024 * 1024;

fn request() -> Vec<u8> {
    let mut b = Vec::with_capacity(32);
    b.extend_from_slice(&1i16.to_be_bytes());
    b.extend_from_slice(&6i16.to_be_bytes());
    b.extend_from_slice(&77i32.to_be_bytes());
    b.extend_from_slice(&(-1i16).to_be_bytes());
    b.extend_from_slice(&(-1i32).to_be_bytes());
    b.extend_from_slice(&0i32.to_be_bytes());
    b.extend_from_slice(&0i32.to_be_bytes());
    b.extend_from_slice(&0i32.to_be_bytes());
    b.push(0);
    b.extend_from_slice(&0i32.to_be_bytes());
    assert_eq!(b.len(), 31);
    b
}
async fn open(root: &PathBuf) -> Router {
    fs::create_dir(root).expect("fresh root");
    let mut config = metadata::Config::new(0, "127.0.0.1".into(), 19095, "poll-witness".into());
    config.max_queued_requests = 1;
    config.protocol_limits = protocol::Limits::new(MAXIMUM, 0).expect("limits");
    config.max_response_bytes = 64;
    let reads = fetch::Limits::default().with_retained_bytes(MAXIMUM + 64).expect("retained");
    Router::open_with_read_store(root.join("catalog"), config, produce::Config::new(root.join("parts")), reads)
        .await.expect("router").0
}
async fn spare_case(root: PathBuf, peer: &Peer, size: usize, trial: usize) {
    let router = open(&root).await;
    let mut spare = Vec::with_capacity(size);
    spare.resize(size, 0x5a);
    assert!(spare.capacity() >= size);
    let mut pending = Box::pin(router.exchange_request(peer, request(), spare));
    let mut elapsed = 0;
    let mut completed = None;
    std::future::poll_fn(|cx| {
        let started = Instant::now();
        let result = pending.as_mut().poll(cx);
        elapsed = started.elapsed().as_nanos();
        if let Poll::Ready(result) = result { completed = Some(result); }
        Poll::Ready(())
    }).await;
    println!("spare_poll,{size},{trial},{elapsed},{}", completed.is_some());
    drop(pending);
    drop(completed);
    router.shutdown().await.expect("shutdown");
    drop(router);
    fs::remove_dir_all(root).expect("cleanup");
}
async fn request_case(root: PathBuf, peer: &Peer, arm: &str, trial: usize) {
    let router = open(&root).await;
    let mut input = Vec::with_capacity(MAXIMUM + 1);
    input.extend_from_slice(&request());
    input.resize(MAXIMUM, 0xa5);
    assert_eq!(input.len(), MAXIMUM);
    assert!(input.capacity() > MAXIMUM);
    // The payload has a valid Fetch key/version prefix but an intentionally
    // padded body. Only pre-enqueue normalization is under measurement.
    let mut pending = Box::pin(router.exchange_request(peer, input, Vec::new()));
    let mut times = Vec::new();
    let mut completed = None;
    let count = if arm == "candidate" { 256 } else { 1 };
    for poll in 0..count {
        let mut elapsed = 0;
        let mut ready = false;
        std::future::poll_fn(|cx| {
            let started = Instant::now();
            let result = pending.as_mut().poll(cx);
            elapsed = started.elapsed().as_nanos();
            if let Poll::Ready(result) = result {
                completed = Some(result);
                ready = true;
            }
            Poll::Ready(())
        }).await;
        println!("request_poll,{trial},{poll},{elapsed},{ready}");
        times.push(elapsed);
        if ready { break; }
        tokio::task::yield_now().await;
    }
    if arm == "candidate" { assert_eq!(times.len(), 256); }
    let first = times[0];
    let last = times[times.len()-1];
    times.sort_unstable();
    let n = times.len();
    println!("request_summary,{trial},polls,{n},first,{first},last,{last},median,{},p99,{},max,{}", times[n/2], times[(n*99/100).min(n-1)], times[n-1]);
    drop(pending);
    drop(completed);
    router.shutdown().await.expect("shutdown");
    drop(router);
    fs::remove_dir_all(root).expect("cleanup");
}
#[tokio::main(flavor = "current_thread")]
async fn main() {
    let arm = std::env::var("ARM").expect("ARM");
    assert!(arm == "baseline" || arm == "candidate");
    let root = PathBuf::from(std::env::var_os("BENCH_ROOT").expect("BENCH_ROOT"));
    fs::create_dir(&root).expect("root");
    let peer = Peer::__qualification_peer();
    for size in [64 * 1024 * 1024, 256 * 1024 * 1024] {
        for trial in 0..3 { spare_case(root.join(format!("spare-{size}-{trial}")), &peer, size, trial).await; }
    }
    for trial in 0..3 { request_case(root.join(format!("request-{trial}")), &peer, &arm, trial).await; }
    fs::remove_dir_all(root).expect("root cleanup");
    println!("validated,poll_witness");
}

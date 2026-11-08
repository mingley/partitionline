//! Isolated, paired Router capacity benchmark. Read the accompanying workflow.
use partitionline_broker::{
    fetch, metadata::{self, Router}, produce, protocol,
    transport::{Handler, Peer},
};
use std::{fs, hint::black_box, path::PathBuf, time::Instant};

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
fn verify(response: &[u8]) {
    assert_eq!(response.len(), 12);
    assert_eq!(&response[..4], &77i32.to_be_bytes());
    assert_eq!(&response[4..], &[0; 8]);
}
async fn exchange(router: &Router, peer: &Peer, arm: &str) {
    const REUSED: usize = 5000;
    const OVERSIZED: usize = 1000;
    let expected = request();
    let mut input = request();
    let mut spare = Vec::with_capacity(64);
    let start = Instant::now();
    for _ in 0..REUSED {
        let output = router.exchange_request(peer, input, spare).await.expect("in-budget");
        assert_eq!(output.request, expected);
        let response = output.response.expect("in-budget response");
        verify(&response);
        input = output.request;
        spare = response;
    }
    let elapsed = start.elapsed().as_nanos();
    println!("exchange,in_budget,{REUSED},{elapsed},{:.3}", elapsed as f64 / REUSED as f64);
    drop(input);
    drop(spare);

    // Initial Vec allocation and initialization happen outside the timer.
    let inputs: Vec<_> = (0..OVERSIZED).map(|_| {
        let mut input = Vec::with_capacity(8192);
        input.extend_from_slice(&expected);
        (input, Vec::with_capacity(8192))
    }).collect();
    let start = Instant::now();
    for (input, spare) in inputs {
        let output = router.exchange_request(peer, input, spare).await.expect("oversized");
        assert_eq!(output.request, expected);
        let response = output.response.expect("oversized response");
        verify(&response);
        if arm == "candidate" {
            assert!(output.request.capacity() <= 4096);
            assert!(response.capacity() <= 64);
        } else {
            assert!(output.request.capacity() >= 8192);
            assert!(response.capacity() >= 8192);
        }
    }
    let elapsed = start.elapsed().as_nanos();
    println!("exchange,oversized_31b_8192_capacity,{OVERSIZED},{elapsed},{:.3}", elapsed as f64 / OVERSIZED as f64);
}
fn copy_control(arm: &str, length: usize, maximum: usize, count: usize) {
    let mut times = Vec::with_capacity(count);
    for sequence in 0..count {
        // Allocate and fault in the source before timing. The measured call
        // includes candidate reservation, copy, source drop and clock overhead.
        let mut bytes = Vec::with_capacity(maximum + 1);
        bytes.resize(length, 0xa5);
        assert!(bytes.capacity() > maximum);
        let start = Instant::now();
        let out = metadata::__qualification_copy(black_box(bytes), maximum).expect("copy");
        let elapsed = start.elapsed().as_nanos();
        assert_eq!(out.len(), length);
        assert!(out.iter().all(|v| *v == 0xa5));
        if arm == "candidate" { assert!(out.capacity() <= maximum); }
        else { assert!(out.capacity() > maximum); }
        black_box(&out);
        times.push(elapsed);
        println!("copy_sample,{length},{maximum},{sequence},{elapsed},{}", out.capacity());
    }
    times.sort_unstable();
    println!("copy_summary,{length},{maximum},{count},{},{},{}", times[0], times[count / 2], times[count - 1]);
}
#[tokio::main(flavor = "current_thread")]
async fn main() {
    let arm = std::env::var("ARM").expect("ARM");
    assert!(arm == "baseline" || arm == "candidate");
    let root = PathBuf::from(std::env::var_os("BENCH_ROOT").expect("BENCH_ROOT"));
    fs::create_dir(&root).expect("fresh root");
    let mut config = metadata::Config::new(0, "127.0.0.1".into(), 19095, "capacity-bench".into());
    config.max_queued_requests = 1;
    config.protocol_limits = protocol::Limits::new(4096, 0).expect("limits");
    config.max_response_bytes = 64;
    let limits = fetch::Limits::default().with_retained_bytes(4160).expect("retained");
    let (router, _) = Router::open_with_read_store(
        root.join("catalog"), config, produce::Config::new(root.join("parts")), limits,
    ).await.expect("router");
    let peer = Peer::__qualification_peer();
    println!("fixture,request_len,31,request_max,4096,response_max,64,oversized_capacity,8192");
    exchange(&router, &peer, &arm).await;
    router.shutdown().await.expect("shutdown");
    drop(router);
    fs::remove_dir_all(root).expect("cleanup");
    for (len, max, count) in [
        (31, 4096, 1000),
        (4096, 4096, 1000),
        (1_048_576, 1_048_576, 32),
        (8_388_608, 8_388_608, 12),
        (67_108_864, 67_108_864, 5),
    ] { copy_control(&arm, len, max, count); }
    println!("validated,all_responses_and_copy_content");
}

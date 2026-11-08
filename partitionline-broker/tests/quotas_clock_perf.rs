//! Matched local timing for the quota clock policy.
#![allow(clippy::unwrap_used)]

use partitionline_broker::quotas::{Kind, Limits, Window};
use std::hint::black_box;
use std::time::Instant;

#[test]
fn quota_clock_timing() {
    let shape = std::env::var("QUOTA_SHAPE").unwrap();
    let limits = Limits::new(1_000_000_000, 1_000_000_000, 1_000_000_000, 1, 2_048, 1_000_000, 1_000).unwrap();
    let mut window = Window::new(limits, 0);
    if shape == "rates" {
        for index in 0..1_024 {
            let id = format!("client-{index:04}");
            let item = window.admit(Some(&id), Kind::Produce, 1, 0).unwrap();
            window.complete(item.work_id).unwrap();
        }
    }
    let start = Instant::now();
    let mut checksum = 0_u64;
    match shape.as_str() {
        "client" | "replication" => {
            let kind = if shape == "client" { Kind::Produce } else { Kind::Replication };
            let client = if shape == "client" { Some("hot-client") } else { None };
            for now in 0..300_000_u64 {
                let item = window.admit(client, kind, 1, now).unwrap();
                checksum ^= item.work_id ^ item.throttle_ms;
                checksum ^= window.complete(item.work_id).unwrap();
            }
        }
        "connect" => {
            for now in 0..300_000_u64 {
                window.connect_at("hot-client", now).unwrap();
                window.disconnect("hot-client").unwrap();
                checksum ^= window.connections() as u64;
            }
        }
        "rates" => {
            for now in 1..=500_u64 {
                window.set_rates_at(1_000_000_000, 1_000_000_000, 1_000_000_000, now).unwrap();
                checksum ^= now;
            }
        }
        _ => panic!("unknown shape"),
    }
    let elapsed_ns = start.elapsed().as_nanos();
    let _checksum = black_box(checksum);
    println!("QUOTA_BENCH shape={shape} elapsed_ns={elapsed_ns} checksum={checksum}");
}

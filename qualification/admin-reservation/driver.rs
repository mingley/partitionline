use std::alloc::{GlobalAlloc, Layout, System};
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::hint::black_box;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::Instant;

use bytes::{Buf, BufMut, BytesMut};
use partitionline::protocol::{admin::*, buf};
use partitionline::Result;
use tokio::runtime::{Builder, Runtime};

struct Instrumented;
static ACTIVE: AtomicBool = AtomicBool::new(false);
static CALLS: AtomicUsize = AtomicUsize::new(0);
static TOTAL: AtomicUsize = AtomicUsize::new(0);
static LARGEST: AtomicUsize = AtomicUsize::new(0);
static CURRENT: AtomicUsize = AtomicUsize::new(0);
static PEAK: AtomicUsize = AtomicUsize::new(0);
static OVERFLOW: AtomicBool = AtomicBool::new(false);
const SLOTS: usize = 262_144;
static KEYS: [AtomicUsize; SLOTS] = [const { AtomicUsize::new(0) }; SLOTS];
static SIZES: [AtomicUsize; SLOTS] = [const { AtomicUsize::new(0) }; SLOTS];

// This single-threaded probe tracks only allocations started during a decode.
// The fixed table avoids allocating from inside the global allocator.
fn reset_tracking() {
    for key in &KEYS { key.store(0, Ordering::Relaxed); }
    CALLS.store(0, Ordering::Relaxed);
    TOTAL.store(0, Ordering::Relaxed);
    LARGEST.store(0, Ordering::Relaxed);
    CURRENT.store(0, Ordering::Relaxed);
    PEAK.store(0, Ordering::Relaxed);
    OVERFLOW.store(false, Ordering::Relaxed);
}
fn record_request(size: usize) {
    CALLS.fetch_add(1, Ordering::Relaxed);
    TOTAL.fetch_add(size, Ordering::Relaxed);
    LARGEST.fetch_max(size, Ordering::Relaxed);
}
fn slot(key: usize) -> usize {
    (key >> 4).wrapping_mul(0x9e3779b97f4a7c15usize) & (SLOTS - 1)
}
fn insert(ptr: *mut u8, size: usize) {
    if ptr.is_null() { return; }
    let key = ptr as usize;
    let start = slot(key);
    for probe in 0..SLOTS {
        let index = (start + probe) & (SLOTS - 1);
        let existing = KEYS[index].load(Ordering::Relaxed);
        if existing <= 1 {
            SIZES[index].store(size, Ordering::Relaxed);
            KEYS[index].store(key, Ordering::Relaxed);
            let live = CURRENT.fetch_add(size, Ordering::Relaxed) + size;
            PEAK.fetch_max(live, Ordering::Relaxed);
            return;
        }
    }
    OVERFLOW.store(true, Ordering::Relaxed);
}
fn remove(ptr: *mut u8) {
    let key = ptr as usize;
    let start = slot(key);
    for probe in 0..SLOTS {
        let index = (start + probe) & (SLOTS - 1);
        let existing = KEYS[index].load(Ordering::Relaxed);
        if existing == 0 { return; }
        if existing == key {
            let size = SIZES[index].load(Ordering::Relaxed);
            KEYS[index].store(1, Ordering::Relaxed);
            CURRENT.fetch_sub(size, Ordering::Relaxed);
            return;
        }
    }
}

unsafe impl GlobalAlloc for Instrumented {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let ptr = unsafe { System.alloc(layout) };
        if ACTIVE.load(Ordering::Relaxed) {
            record_request(layout.size());
            insert(ptr, layout.size());
        }
        ptr
    }
    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        if ACTIVE.load(Ordering::Relaxed) { remove(ptr); }
        unsafe { System.dealloc(ptr, layout) }
    }
    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, size: usize) -> *mut u8 {
        let active = ACTIVE.load(Ordering::Relaxed);
        if active { remove(ptr); }
        let next = unsafe { System.realloc(ptr, layout, size) };
        if active {
            record_request(size);
            if next.is_null() { insert(ptr, layout.size()); } else { insert(next, size); }
        }
        next
    }
}
#[global_allocator]
static ALLOC: Instrumented = Instrumented;

fn base_case(label: &str) -> &str {
    label.split(':').next().unwrap()
}

fn sync_decode<B: Buf>(label: &str, cur: &mut B, version: i16) -> Result<usize> {
    match base_case(label) {
        "create" => decode_create_topics_response(cur, version).map(|x| x.0.len()),
        "delete" => decode_delete_topics_response(cur, version).map(|x| x.0.len()),
        "partitions" => decode_create_partitions_response(cur, version).map(|x| x.0.len()),
        "configs" => decode_describe_configs_response(cur, version).map(|x| x.0.len()),
        "alter" => decode_alter_configs_resource_results(cur, version).map(|x| x.0.len()),
        "incremental" => decode_incremental_alter_configs_resource_results(cur, version).map(|x| x.0.len()),
        "records" => decode_delete_records_topics_response(cur, version).map(|x| x.0.len()),
        _ => panic!("unknown case"),
    }
}

async fn cooperative_decode<B: Buf>(label: &str, cur: &mut B, version: i16) -> Result<usize> {
    match base_case(label) {
        "create" => decode_create_topics_response_cooperative(cur, version).await.map(|x| x.0.len()),
        "delete" => decode_delete_topics_response_cooperative(cur, version).await.map(|x| x.0.len()),
        "partitions" => decode_create_partitions_response_cooperative(cur, version).await.map(|x| x.0.len()),
        "configs" => decode_describe_configs_response_cooperative(cur, version).await.map(|x| x.0.len()),
        "alter" => decode_alter_configs_resource_results_cooperative(cur, version).await.map(|x| x.0.len()),
        "incremental" => decode_incremental_alter_configs_resource_results_cooperative(cur, version).await.map(|x| x.0.len()),
        "records" => decode_delete_records_topics_response_cooperative(cur, version).await.map(|x| x.0.len()),
        _ => panic!("unknown case"),
    }
}

fn sync_render<B: Buf>(label: &str, cur: &mut B, version: i16) -> String {
    match base_case(label) {
        "create" => format!("{:?}", decode_create_topics_response(cur, version)),
        "delete" => format!("{:?}", decode_delete_topics_response(cur, version)),
        "partitions" => format!("{:?}", decode_create_partitions_response(cur, version)),
        "configs" => format!("{:?}", decode_describe_configs_response(cur, version)),
        "alter" => format!("{:?}", decode_alter_configs_resource_results(cur, version)),
        "incremental" => format!("{:?}", decode_incremental_alter_configs_resource_results(cur, version)),
        "records" => format!("{:?}", decode_delete_records_topics_response(cur, version)),
        _ => panic!("unknown case"),
    }
}

async fn cooperative_render<B: Buf>(label: &str, cur: &mut B, version: i16) -> String {
    match base_case(label) {
        "create" => format!("{:?}", decode_create_topics_response_cooperative(cur, version).await),
        "delete" => format!("{:?}", decode_delete_topics_response_cooperative(cur, version).await),
        "partitions" => format!("{:?}", decode_create_partitions_response_cooperative(cur, version).await),
        "configs" => format!("{:?}", decode_describe_configs_response_cooperative(cur, version).await),
        "alter" => format!("{:?}", decode_alter_configs_resource_results_cooperative(cur, version).await),
        "incremental" => format!("{:?}", decode_incremental_alter_configs_resource_results_cooperative(cur, version).await),
        "records" => format!("{:?}", decode_delete_records_topics_response_cooperative(cur, version).await),
        _ => panic!("unknown case"),
    }
}

fn make_valid(label: &str, version: i16, count: usize) -> BytesMut {
    let mut bytes = BytesMut::new();
    match base_case(label) {
        "create" | "delete" | "partitions" => {
            let items: Vec<_> = (0..count).map(|i| TopicResult {
                name: format!("topic-{i}"),
                error_code: 0,
                error_message: None,
                topic_id: [7; 16],
                num_partitions: 3,
                replication_factor: 1,
                configs: Vec::new(),
            }).collect();
            match base_case(label) {
                "create" => encode_create_topics_response(&mut bytes, version, &items).unwrap(),
                "delete" => encode_delete_topics_response(&mut bytes, version, &items).unwrap(),
                _ => encode_create_partitions_response(&mut bytes, version, &items).unwrap(),
            }
        }
        "configs" => {
            let entries: Vec<_> = (0..count).map(|i| ConfigEntry {
                name: format!("config-{i}"),
                value: Some("value".into()),
                read_only: false,
                source: 1,
                is_sensitive: false,
                synonyms: vec![ConfigSynonym { name: "source".into(), value: None, source: 1 }],
                config_type: 2,
                documentation: Some("doc".into()),
            }).collect();
            let items = [DescribeConfigsResult {
                error_code: 0,
                error_message: None,
                resource_type: 2,
                name: "resource".into(),
                entries,
            }];
            encode_describe_configs_response(&mut bytes, version, &items).unwrap();
        }
        "alter" | "incremental" => {
            let items: Vec<_> = (0..count).map(|i| AlterConfigsResourceResult {
                error_code: 0,
                error_message: None,
                resource_type: 2,
                name: format!("resource-{i}"),
            }).collect();
            if base_case(label) == "alter" {
                encode_alter_configs_resource_results(&mut bytes, version, &items).unwrap();
            } else {
                encode_incremental_alter_configs_resource_results(&mut bytes, version, &items).unwrap();
            }
        }
        "records" => {
            let partitions = (0..count).map(|i| DeletedRecordsPartition {
                partition: i32::try_from(i).unwrap(),
                low_watermark: 1,
                error_code: 0,
            }).collect();
            let topics = [DeletedRecordsTopic { topic: "topic".into(), partitions }];
            encode_delete_records_topics_response(&mut bytes, version, &topics).unwrap();
        }
        _ => panic!("unknown case"),
    }
    bytes
}

fn malformed(label: &str, count: usize) -> (BytesMut, i16) {
    let mut b = BytesMut::new();
    b.put_i32(0); // throttle, present in all these flexible versions
    match label {
        "create:topics" => {
            buf::put_array_len(&mut b, true, Some(count)).unwrap();
            b.extend_from_slice(&[2, 0xff]);
        }
        "create:configs" => {
            buf::put_array_len(&mut b, true, Some(1)).unwrap();
            buf::put_compact_string(&mut b, Some("topic")).unwrap();
            b.extend_from_slice(&[0; 16]); // topic ID, v7
            b.put_i16(0);
            buf::put_compact_string(&mut b, None).unwrap();
            b.put_i32(1);
            b.put_i16(1);
            buf::put_array_len(&mut b, true, Some(count)).unwrap();
            b.extend_from_slice(&[2, 0xff]);
        }
        "delete:topics" | "partitions:topics" | "records:topics" => {
            buf::put_array_len(&mut b, true, Some(count)).unwrap();
            b.extend_from_slice(&[2, 0xff]);
        }
        "alter:resources" | "incremental:resources" | "configs:resources" => {
            buf::put_array_len(&mut b, true, Some(count)).unwrap();
            b.put_i16(0);
            b.extend_from_slice(&[2, 0xff]);
        }
        "configs:entries" | "configs:synonyms" => {
            buf::put_array_len(&mut b, true, Some(1)).unwrap();
            b.put_i16(0);
            buf::put_compact_string(&mut b, None).unwrap();
            b.put_i8(2);
            buf::put_compact_string(&mut b, Some("topic")).unwrap();
            if label == "configs:synonyms" {
                buf::put_array_len(&mut b, true, Some(1)).unwrap();
                buf::put_compact_string(&mut b, Some("key")).unwrap();
                buf::put_compact_string(&mut b, None).unwrap();
                b.put_u8(0); // read-only
                b.put_i8(1); // source
                b.put_u8(0); // sensitive
            }
            buf::put_array_len(&mut b, true, Some(count)).unwrap();
            b.extend_from_slice(&[2, 0xff]);
        }
        "records:partitions" => {
            buf::put_array_len(&mut b, true, Some(1)).unwrap();
            buf::put_compact_string(&mut b, Some("topic")).unwrap();
            buf::put_array_len(&mut b, true, Some(count)).unwrap();
            b.extend_from_slice(&[0; 14]); // partition, watermark, error
            b.extend_from_slice(&[0x80; 5]); // invalid tag-count varint
        }
        _ => panic!("unknown witness"),
    }
    b.resize(b.len() + count, 0);
    let version = match base_case(label) {
        "create" => 7,
        "delete" => 6,
        "partitions" => 3,
        "configs" => 4,
        "alter" => 2,
        "incremental" => 1,
        "records" => 2,
        _ => unreachable!(),
    };
    (b, version)
}

fn witness(rt: &Runtime) {
    for label in [
        "create:topics", "create:configs", "delete:topics",
        "partitions:topics", "configs:resources", "configs:entries",
        "configs:synonyms", "alter:resources", "incremental:resources",
        "records:topics", "records:partitions",
    ] {
        let (body, version) = malformed(label, 100_000);
        for cooperative in [false, true] {
            let mut cur = &body[..];
            reset_tracking();
            ACTIVE.store(true, Ordering::SeqCst);
            let result = if cooperative {
                rt.block_on(cooperative_decode(label, &mut cur, version))
            } else {
                sync_decode(label, &mut cur, version)
            };
            ACTIVE.store(false, Ordering::SeqCst);
            let error = result.unwrap_err();
            println!(
                "{}|{}|{}|{}|{}|{}|{:?}",
                label,
                if cooperative { "cooperative" } else { "sync" },
                CALLS.load(Ordering::SeqCst),
                TOTAL.load(Ordering::SeqCst),
                LARGEST.load(Ordering::SeqCst),
                cur.remaining(),
                error
            );
        }
    }
}

fn corpus(rt: &Runtime) {
    let cases: &[(&str, &[i16])] = &[
        ("create", &[0, 4, 5, 7]),
        ("delete", &[0, 3, 4, 6]),
        ("partitions", &[0, 1, 2, 3]),
        ("configs", &[0, 1, 2, 3, 4]),
        ("alter", &[0, 1, 2]),
        ("incremental", &[0, 1]),
        ("records", &[0, 1, 2]),
    ];
    for &(label, versions) in cases {
        for &version in versions {
            let original = make_valid(label, version, 3);
            let mut digest = DefaultHasher::new();
            let mut cases = 0;
            for end in 0..=original.len() {
                let value = &original[..end];
                let mut one = value;
                let sync = sync_render(label, &mut one, version);
                sync.hash(&mut digest);
                one.remaining().hash(&mut digest);
                let midpoint = value.len() / 2;
                let mut two = value[..midpoint].chain(&value[midpoint..]);
                let cooperative = rt.block_on(cooperative_render(label, &mut two, version));
                cooperative.hash(&mut digest);
                two.remaining().hash(&mut digest);
                cases += 1;
            }
            for index in 0..original.len().min(128) {
                let mut altered = original.clone();
                altered[index] ^= 0xff;
                let mut cur = &altered[..];
                sync_render(label, &mut cur, version).hash(&mut digest);
                cur.remaining().hash(&mut digest);
                cases += 1;
            }
            println!("{label}|{version}|{cases}|{:016x}", digest.finish());
        }
    }
}

fn bench(rt: &Runtime) {
    for (label, version, count, repetitions, expected) in [
        ("create", 7, 10, 30000, 10),
        ("create", 7, 320, 1200, 320),
        ("create", 7, 20000, 20, 20000),
        ("configs", 4, 10, 30000, 1),
        ("configs", 4, 320, 1200, 1),
        ("configs", 4, 10000, 40, 1),
    ] {
        let body = make_valid(label, version, count);
        let mut total = 0usize;
        // Warm the code and allocator before timing.
        for _ in 0..8 {
            let mut cur = &body[..];
            assert_eq!(rt.block_on(cooperative_decode(label, &mut cur, version)).unwrap(), expected);
            assert!(cur.is_empty());
        }
        let start = Instant::now();
        for _ in 0..repetitions {
            let mut cur = black_box(&body[..]);
            total = total.wrapping_add(rt.block_on(cooperative_decode(label, &mut cur, version)).unwrap());
            assert!(cur.is_empty());
        }
        let elapsed = start.elapsed().as_nanos();
        assert_eq!(total, repetitions * expected);
        println!("{label}|{count}|{repetitions}|{elapsed}|{total}");
    }
}

fn valid_allocations(rt: &Runtime) {
    for (label, version, count, expected) in [
        ("create", 7, 10, 10),
        ("create", 7, 320, 320),
        ("create", 7, 20000, 20000),
        ("delete", 6, 10, 10),
        ("delete", 6, 320, 320),
        ("delete", 6, 20000, 20000),
        ("configs", 4, 10, 1),
        ("configs", 4, 320, 1),
        ("configs", 4, 10000, 1),
        ("records", 2, 10, 1),
        ("records", 2, 320, 1),
        ("records", 2, 20000, 1),
    ] {
        let body = make_valid(label, version, count);
        for cooperative in [false, true] {
            let mut cur = &body[..];
            reset_tracking();
            ACTIVE.store(true, Ordering::SeqCst);
            let result = if cooperative {
                rt.block_on(cooperative_decode(label, &mut cur, version))
            } else {
                sync_decode(label, &mut cur, version)
            };
            ACTIVE.store(false, Ordering::SeqCst);
            let value = result.unwrap();
            assert!(!OVERFLOW.load(Ordering::SeqCst), "allocation tracker overflow");
            assert_eq!(value, expected);
            assert!(cur.is_empty());
            println!(
                "{}|{}|{}|{}|{}|{}|{}|{}|{}",
                label, count,
                if cooperative { "cooperative" } else { "sync" },
                CALLS.load(Ordering::SeqCst),
                TOTAL.load(Ordering::SeqCst),
                LARGEST.load(Ordering::SeqCst),
                PEAK.load(Ordering::SeqCst),
                cur.remaining(), value
            );
        }
    }
}

fn main() {
    let rt = Builder::new_current_thread().enable_all().build().unwrap();
    match std::env::args().nth(1).as_deref() {
        Some("witness") => witness(&rt),
        Some("corpus") => corpus(&rt),
        Some("bench") => bench(&rt),
        Some("valid-allocs") => valid_allocations(&rt),
        _ => panic!("expected witness, corpus or bench"),
    }
}

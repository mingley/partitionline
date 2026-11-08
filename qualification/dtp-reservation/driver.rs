use std::alloc::{GlobalAlloc, Layout, System};
use std::hint::black_box;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::Instant;
use bytes::{Buf, BufMut, BytesMut};
use partitionline::protocol::{admin::*, buf};
use tokio::runtime::Builder;

struct Measured;
static ACTIVE: AtomicBool = AtomicBool::new(false);
static LARGEST: AtomicUsize = AtomicUsize::new(0);
static CALLS: AtomicUsize = AtomicUsize::new(0);
fn record(n: usize) {
    if ACTIVE.load(Ordering::Relaxed) {
        LARGEST.fetch_max(n, Ordering::Relaxed);
        CALLS.fetch_add(1, Ordering::Relaxed);
    }
}
unsafe impl GlobalAlloc for Measured {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        record(layout.size());
        unsafe { System.alloc(layout) }
    }
    unsafe fn dealloc(&self, p: *mut u8, layout: Layout) {
        unsafe { System.dealloc(p, layout) }
    }
    unsafe fn realloc(&self, p: *mut u8, layout: Layout, n: usize) -> *mut u8 {
        record(n);
        unsafe { System.realloc(p, layout, n) }
    }
}
#[global_allocator] static ALLOC: Measured = Measured;

fn malformed(variant: usize) -> BytesMut {
    let mut b = BytesMut::new();
    b.put_i32(0);
    buf::put_array_len(&mut b, true, Some(if variant==0 {100_000} else {1})).unwrap();
    b.put_i16(0);
    if variant==0 {
        buf::put_unsigned_varint(&mut b, u32::MAX);
    } else {
        buf::put_compact_string(&mut b, Some("orders")).unwrap();
        b.extend_from_slice(&[0; 16]);
        b.put_u8(0);
        buf::put_array_len(&mut b, true, Some(if variant==1 {100_000} else {1})).unwrap();
        b.put_i16(0);
        b.put_i32(0); b.put_i32(1); b.put_i32(0);
        if variant==1 { buf::put_unsigned_varint(&mut b, u32::MAX); }
        else { buf::put_array_len(&mut b, true, Some(100_000)).unwrap(); }
    }
    b.resize(b.len() + 100_000, 0);
    b
}
fn valid(topics: usize, parts: usize, replicas: usize) -> BytesMut {
    let mut result = Vec::new();
    for i in 0..topics {
        let mut t = DescribedTopicPartitions::new(format!("topic-{i}"),0);
        t.partitions = (0..parts).map(|p| {
            let mut part=DescribedTopicPartition::new(0);
            part.partition_index=i32::try_from(p).unwrap();
            part.replica_nodes=(0..i32::try_from(replicas).unwrap()).collect();
            part
        }).collect();
        result.push(t);
    }
    let response=DescribeTopicPartitionsResponse::new(result);
    let mut b=BytesMut::new();
    encode_describe_topic_partitions_response(&mut b,&response).unwrap();
    b
}
fn witness() {
    let rt=Builder::new_current_thread().enable_all().build().unwrap();
    for variant in 0..3 {
        let input=malformed(variant);
        for cooperative in [false,true] {
            let mut cur=&input[..];
            LARGEST.store(0,Ordering::Relaxed);
            CALLS.store(0,Ordering::Relaxed);
            ACTIVE.store(true,Ordering::Relaxed);
            let got=if cooperative {
                rt.block_on(decode_describe_topic_partitions_response_cooperative(&mut cur))
            } else { decode_describe_topic_partitions_response(&mut cur) };
            ACTIVE.store(false,Ordering::Relaxed);
            assert!(got.is_err());
            println!("{variant}|{cooperative}|{}|{}|{}|{:?}",CALLS.load(Ordering::Relaxed),LARGEST.load(Ordering::Relaxed),cur.remaining(),got);
        }
    }
}
fn corpus() {
    let rt=Builder::new_current_thread().enable_all().build().unwrap();
    let wire=valid(2,2,3);
    let mut cases:Vec<Vec<u8>>=(0..=wire.len()).map(|n| wire[..n].to_vec()).collect();
    for at in 0..wire.len().min(100) {
        for value in [0u8,1,127,255] {
            let mut copy=wire.to_vec();
            copy[at]=value;
            cases.push(copy);
        }
    }
    for (i,wire) in cases.iter().enumerate() {
        let mut a=wire.as_slice();
        let x=decode_describe_topic_partitions_response(&mut a);
        let mut b=wire.as_slice();
        let y=rt.block_on(decode_describe_topic_partitions_response_cooperative(&mut b));
        assert_eq!(x,y,"case {i}");
        assert_eq!(a.remaining(),b.remaining(),"case {i}");
        println!("{i}|{}|{:?}",a.remaining(),x);
    }
}
fn bench() {
    for (name, topics, parts, replicas, repeats) in [
        ("small",1,2,3,4000),
        ("topics",10000,0,0,40),
        ("parts",1,10000,0,20),
        ("replicas",1,1,20000,100)
    ] {
        let input=valid(topics,parts,replicas);
        let started=Instant::now();
        for _ in 0..repeats {
            let mut cur=black_box(&input[..]);
            let got=decode_describe_topic_partitions_response(&mut cur).unwrap();
            assert!(cur.is_empty());
            black_box(got);
        }
        println!("{name}|{repeats}|{}",started.elapsed().as_nanos());
    }
}
fn main() {
    match std::env::args().nth(1).as_deref() {
        Some("witness")=>witness(),
        Some("corpus")=>corpus(),
        Some("bench")=>bench(),
        _=>panic!("mode required"),
    }
}

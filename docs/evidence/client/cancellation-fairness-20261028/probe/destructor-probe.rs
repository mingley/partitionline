//! Standalone destructor microprobe, not the Kafka client's cancellation path.
use std::{hint::black_box, time::Instant};

struct Position { topic: String, partition: i32, offset: i64 }

fn make_positions(n: usize) -> Vec<Position> {
    (0..n).map(|i| Position { topic: "orders".to_owned(), partition: i as i32, offset: 1 }).collect()
}

fn setup(n: usize) -> (std::vec::IntoIter<Position>, Vec<Position>) {
    let mut iter = make_positions(n).into_iter();
    let mut done = Vec::with_capacity(n / 10);
    for item in iter.by_ref().take(n / 10) { done.push(item); }
    assert_eq!(iter.len() + done.len(), n);
    assert_eq!(done.first().map(|x| (x.topic.as_str(), x.partition, x.offset)), if n >= 10 { Some(("orders", 0, 1)) } else { None });
    (iter, done)
}

fn unchunked(n: usize) {
    let (iter, done) = setup(n);
    let remaining = iter.len();
    let complete = done.len();
    let start = Instant::now();
    drop(black_box(iter));
    let iterator_drop_ns = start.elapsed().as_nanos();
    let start = Instant::now();
    drop(black_box(done));
    let output_drop_ns = start.elapsed().as_nanos();
    println!("{{\"mode\":\"unchunked\",\"records\":{n},\"remaining\":{remaining},\"partial_output\":{complete},\"iterator_drop_ns\":{iterator_drop_ns},\"output_drop_ns\":{output_drop_ns}}}");
}

fn chunked(n: usize, k: usize) {
    let (mut iter, mut done) = setup(n);
    let mut chunks = Vec::new();
    let mut freed = 0usize;
    let start_total = Instant::now();
    loop {
        let mut count = 0;
        let started = Instant::now();
        while count < k {
            match iter.next() {
                Some(item) => { drop(black_box(item)); count += 1; },
                None => break,
            }
        }
        if count == 0 { break; }
        freed += count;
        chunks.push(started.elapsed().as_nanos());
        std::thread::yield_now();
    }
    let started = Instant::now();
    drop(black_box(iter));
    let iterator_buffer_ns = started.elapsed().as_nanos();
    while !done.is_empty() {
        let started = Instant::now();
        let mut count = 0;
        while count < k {
            match done.pop() {
                Some(item) => { drop(black_box(item)); count += 1; },
                None => break,
            }
        }
        freed += count;
        chunks.push(started.elapsed().as_nanos());
        std::thread::yield_now();
    }
    let started = Instant::now();
    drop(black_box(done));
    let output_buffer_ns = started.elapsed().as_nanos();
    assert_eq!(freed, n);
    let total_ns = start_total.elapsed().as_nanos();
    chunks.sort_unstable();
    let max_ns = chunks.last().copied().unwrap_or(0);
    let p99_ns = if chunks.is_empty() { 0 } else { chunks[(chunks.len() - 1) * 99 / 100] };
    println!("{{\"mode\":\"chunked\",\"records\":{n},\"chunk_items\":{k},\"freed\":{freed},\"chunks\":{},\"max_chunk_ns\":{max_ns},\"p99_chunk_ns\":{p99_ns},\"iterator_buffer_ns\":{iterator_buffer_ns},\"output_buffer_ns\":{output_buffer_ns},\"total_ns\":{total_ns}}}", chunks.len());
}

fn borrowed(n: usize) {
    let values = make_positions(n);
    let slice = black_box(values.as_slice());
    let started = Instant::now();
    let _ = black_box(slice);
    let borrowed_reference_check_ns = started.elapsed().as_nanos();
    let started = Instant::now();
    drop(black_box(values));
    let owner_drop_ns = started.elapsed().as_nanos();
    println!("{{\"mode\":\"borrowed\",\"records\":{n},\"borrowed_reference_check_ns\":{borrowed_reference_check_ns},\"owner_drop_ns\":{owner_drop_ns}}}");
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    assert_eq!(args.len(), 4, "usage: probe RECORDS CHUNK ROUND");
    let n: usize = args[1].parse().expect("records");
    let k: usize = args[2].parse().expect("chunk");
    let round: usize = args[3].parse().expect("round");
    assert!((1..=2_000_000).contains(&n) && (1..=65_536).contains(&k));
    // Alternate order to avoid assigning allocator state systematically to one case.
    if round.is_multiple_of(2) { unchunked(n); chunked(n, k); borrowed(n); }
    else { chunked(n, k); unchunked(n); borrowed(n); }
}

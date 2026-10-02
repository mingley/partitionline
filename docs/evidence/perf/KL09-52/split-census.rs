use bytes::BytesMut;
use codec::{census, CountingAlloc};
use std::hint::black_box;
#[global_allocator]
static ALLOC: CountingAlloc = CountingAlloc;
fn main() {
    for size in [32, 128, 512, 4096, 16384, 65536] {
        let input = vec![42; size];
        let mut split = BytesMut::with_capacity(16 * 1024);
        let (_, baseline, baseline_bytes) = census(|| {
            for _ in 0..10_000 {
                split.clear();
                split.extend_from_slice(&input);
                let payload = split.split();
                black_box(&payload);
            }
        });
        let mut reuse = BytesMut::with_capacity(16 * 1024);
        let (_, candidate, candidate_bytes) = census(|| {
            for _ in 0..10_000 {
                reuse.clear();
                reuse.extend_from_slice(&input);
                let payload = std::mem::take(&mut reuse);
                black_box(&payload);
                reuse = payload;
            }
        });
        println!("size={size} requests=10000 split_allocations={baseline} split_bytes={baseline_bytes} reuse_allocations={candidate} reuse_bytes={candidate_bytes}");
    }
}

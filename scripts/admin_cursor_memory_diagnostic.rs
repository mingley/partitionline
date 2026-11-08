use std::alloc::{GlobalAlloc, Layout, System};
use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicUsize, Ordering};

struct Measured;
static LIVE: AtomicUsize = AtomicUsize::new(0);
static PEAK: AtomicUsize = AtomicUsize::new(0);

fn add(bytes: usize) {
    let value = LIVE.fetch_add(bytes, Ordering::SeqCst) + bytes;
    PEAK.fetch_max(value, Ordering::SeqCst);
}

unsafe impl GlobalAlloc for Measured {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let ptr = unsafe { System.alloc(layout) };
        if !ptr.is_null() { add(layout.size()); }
        ptr
    }
    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        let ptr = unsafe { System.alloc_zeroed(layout) };
        if !ptr.is_null() { add(layout.size()); }
        ptr
    }
    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        LIVE.fetch_sub(layout.size(), Ordering::SeqCst);
        unsafe { System.dealloc(ptr, layout) };
    }
    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        let result = unsafe { System.realloc(ptr, layout, new_size) };
        if !result.is_null() {
            if new_size >= layout.size() { add(new_size - layout.size()); }
            else { LIVE.fetch_sub(layout.size() - new_size, Ordering::SeqCst); }
        }
        result
    }
}
#[global_allocator]
static ALLOC: Measured = Measured;

fn run(n: i32, distinct: bool) {
    let start = LIVE.load(Ordering::SeqCst);
    PEAK.store(start, Ordering::SeqCst);
    let mut seen: HashMap<String, HashSet<i32>> = HashMap::new();
    for i in 0..n {
        let next = if distinct { format!("topic-{i}") } else { "orders".to_owned() };
        let _already_seen = seen.get(next.as_str()).is_some_and(|s| s.contains(&i));
        let _inserted = seen.entry(next).or_default().insert(i);
    }
    let retained = LIVE.load(Ordering::SeqCst) - start;
    let peak = PEAK.load(Ordering::SeqCst) - start;
    let hash_slots: usize = seen.values().map(HashSet::capacity).sum();
    let string_capacity: usize = seen.keys().map(String::capacity).sum();
    println!("distinct={distinct} count={n} topics={} map_capacity={} set_capacity={} key_capacity={} retained_requested_bytes={retained} peak_requested_bytes={peak}",
        seen.len(), seen.capacity(), hash_slots, string_capacity);
    std::hint::black_box(&seen);
}
fn main() {
    println!("usize={} String={} HashSet_i32={}", std::mem::size_of::<usize>(), std::mem::size_of::<String>(), std::mem::size_of::<HashSet<i32>>());
    for n in [100, 10_000, 100_000] {
        run(n, false);
        run(n, true);
    }
}

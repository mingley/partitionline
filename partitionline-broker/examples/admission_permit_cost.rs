//! Paired microbenchmark for uncontended borrowed and owned semaphore permits.

use std::{hint::black_box, sync::Arc, time::Instant};
use tokio::sync::Semaphore;

const ITERATIONS: usize = 2_000_000;
const ROUNDS: usize = 21;

fn borrowed(sem: &Arc<Semaphore>, iterations: usize) -> u128 {
    let start = Instant::now();
    for _ in 0..iterations {
        let permit = match black_box(sem.as_ref()).try_acquire() {
            Ok(permit) => permit,
            Err(_) => std::process::abort(),
        };
        let _seen = black_box(&permit);
        drop(permit);
    }
    start.elapsed().as_nanos()
}

fn owned(sem: &Arc<Semaphore>, iterations: usize) -> u128 {
    let start = Instant::now();
    for _ in 0..iterations {
        let permit = match Arc::clone(black_box(sem)).try_acquire_owned() {
            Ok(permit) => permit,
            Err(_) => std::process::abort(),
        };
        let _seen = black_box(&permit);
        drop(permit);
    }
    start.elapsed().as_nanos()
}

fn arc_only(sem: &Arc<Semaphore>, iterations: usize) -> u128 {
    let start = Instant::now();
    for _ in 0..iterations {
        let copy = Arc::clone(black_box(sem));
        let _seen = black_box(&copy);
        drop(copy);
    }
    start.elapsed().as_nanos()
}

fn main() {
    let sem = Arc::new(Semaphore::new(1));
    let _ = borrowed(&sem, 100_000);
    let _ = owned(&sem, 100_000);
    let _ = arc_only(&sem, 100_000);
    println!("round,case,elapsed_ns,operations");
    for round in 0..ROUNDS {
        let (borrowed_ns, owned_ns, arc_ns) = match round % 3 {
            0 => (
                borrowed(&sem, ITERATIONS),
                owned(&sem, ITERATIONS),
                arc_only(&sem, ITERATIONS),
            ),
            1 => {
                let owned_ns = owned(&sem, ITERATIONS);
                let arc_ns = arc_only(&sem, ITERATIONS);
                let borrowed_ns = borrowed(&sem, ITERATIONS);
                (borrowed_ns, owned_ns, arc_ns)
            }
            _ => {
                let arc_ns = arc_only(&sem, ITERATIONS);
                let borrowed_ns = borrowed(&sem, ITERATIONS);
                let owned_ns = owned(&sem, ITERATIONS);
                (borrowed_ns, owned_ns, arc_ns)
            }
        };
        println!("{round},borrowed,{borrowed_ns},{ITERATIONS}");
        println!("{round},owned,{owned_ns},{ITERATIONS}");
        println!("{round},arc_only,{arc_ns},{ITERATIONS}");
    }
    if sem.available_permits() != 1 || Arc::strong_count(&sem) != 1 {
        std::process::abort();
    }
}

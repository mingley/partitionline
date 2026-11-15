//! Standalone std HashSet duplicate-insert diagnostic.
//! The one-place queue below is a model, not a Kafka client or Tokio channel.

use std::collections::hash_map::RandomState;
use std::collections::HashSet;
use std::hash::{BuildHasher, BuildHasherDefault, Hasher};

const CHUNK: usize = 8192;
type Key = (i32, usize);

// A valid deterministic hasher for these keys. Ignoring the slot is allowed:
// equal keys still hash equally, and collisions are resolved by Eq.
#[derive(Default)]
struct LeaderHasher(u64);

impl Hasher for LeaderHasher {
    fn finish(&self) -> u64 {
        self.0
    }

    fn write(&mut self, bytes: &[u8]) {
        for byte in bytes {
            self.0 = self.0.wrapping_mul(257).wrapping_add(u64::from(*byte));
        }
    }

    fn write_i32(&mut self, value: i32) {
        self.0 = u64::try_from(value).expect("positive fixture node");
    }

    fn write_usize(&mut self, _value: usize) {}
}

#[derive(Debug)]
struct Observation {
    capacity: usize,
    order: Vec<Key>,
}

fn observe<S: BuildHasher>(builder: S, leaders: &[i32], repeats: usize) -> Observation {
    let mut slots = HashSet::with_hasher(builder);
    for leader in leaders {
        for _ in 0..repeats {
            let _ = slots.insert((*leader, 0usize));
        }
    }
    let capacity = slots.capacity();
    let order = slots.into_iter().collect();
    Observation { capacity, order }
}

// Two duplicates per distinct chunk suffice to find the capacity boundary.
// Re-run a selected example with exactly 8,192 inserts per chunk below.
fn find_example<S: BuildHasher + Clone>(builder: &S) -> Option<[i32; 3]> {
    for a in 1..=64 {
        for b in (a + 1)..=64 {
            for c in (b + 1)..=64 {
                let leaders = [a, b, c];
                let original = observe(builder.clone(), &leaders, 2);
                let representative = observe(builder.clone(), &leaders, 1);
                if original.order.first() != representative.order.first() {
                    return Some(leaders);
                }
            }
        }
    }
    None
}

fn run_example<S: BuildHasher + Clone>(label: &str, builder: S, leaders: [i32; 3]) {
    let original = observe(builder.clone(), &leaders, CHUNK);
    let representative = observe(builder, &leaders, 1);
    let mut left = original.order.clone();
    let mut right = representative.order.clone();
    left.sort_unstable();
    right.sort_unstable();
    assert_eq!(left, right);
    assert_eq!(left, leaders.map(|node| (node, 0)).to_vec());
    assert_ne!(original.capacity, representative.capacity);
    assert_ne!(original.order.first(), representative.order.first());
    println!("{label}: leaders={leaders:?}; inserts_per_chunk={CHUNK} versus 1");
    println!("{label}: original={original:?}; representative={representative:?}");
    println!(
        "{label}: model_one_free_queue_slot original={:?}; representative={:?}",
        original.order.first(),
        representative.order.first()
    );
}

fn main() {
    let fixed = BuildHasherDefault::<LeaderHasher>::default();
    let control = observe(fixed.clone(), &[1], 8191);
    let singleton = observe(fixed, &[1], 1);
    assert_eq!(control.capacity, singleton.capacity);
    assert_eq!(control.order, singleton.order);
    println!("one_chunk_control: original={control:?}; representative={singleton:?}");

    let fixed = BuildHasherDefault::<LeaderHasher>::default();
    let leaders = find_example(&fixed).expect("fixed-hasher search found no example");
    run_example("fixed", fixed, leaders);

    // RandomState is the standard HashSet builder. Clone a single instance so
    // both sets use the same keys; the search result is evidence for this run.
    let random = RandomState::new();
    if let Some(leaders) = find_example(&random) {
        let hashes: Vec<_> = leaders.iter().map(|node| random.hash_one((*node, 0usize))).collect();
        println!("random_state: selected_hashes={hashes:?}");
        run_example("random_state", random, leaders);
    } else {
        println!("random_state: no example found in bounded search");
    }
}

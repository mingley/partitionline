// Inserted only into separate diagnostic builds. Never used for timings.
static PL_FULL_DRAIN_CENSUS: [std::sync::atomic::AtomicU64; 16] =
    [const { std::sync::atomic::AtomicU64::new(0) }; 16];

fn pl_full_drain_observe(spare: &Vec<Pending>, pending: &Vec<Pending>, count: usize) {
    use std::sync::atomic::Ordering::Relaxed;
    let add = |index: usize, value: u64| {
        let _previous = PL_FULL_DRAIN_CENSUS[index].fetch_add(value, Relaxed);
    };
    add(0, 1);
    if count == 0 {
        add(1, 1);
    }
    if count != 0 {
        add(2, 1);
    }
    if count == pending.len() && count != 0 {
        add(3, 1);
        if spare.capacity() == pending.capacity() {
            add(4, 1);
            add(5, u64::try_from(count).unwrap_or(u64::MAX));
            let bin = match count {
                1 => 10,
                2..=8 => 11,
                9..=64 => 12,
                65..=512 => 13,
                513..=4096 => 14,
                _ => 15,
            };
            add(bin, 1);
        } else {
            add(6, 1);
        }
    } else if count != 0 {
        add(7, 1);
    }
    add(8, u64::try_from(count).unwrap_or(u64::MAX));
    if spare.capacity() == pending.capacity() {
        add(9, 1);
    }
}

/// Resets diagnostic counters after the warmup has flushed.
#[doc(hidden)]
pub fn __pl_full_drain_census_reset() {
    for counter in &PL_FULL_DRAIN_CENSUS {
        counter.store(0, std::sync::atomic::Ordering::Relaxed);
    }
}

/// Returns non-timing diagnostic counts from the producer drain path.
#[doc(hidden)]
#[must_use]
pub fn __pl_full_drain_census_snapshot() -> [u64; 16] {
    std::array::from_fn(|index| {
        PL_FULL_DRAIN_CENSUS[index].load(std::sync::atomic::Ordering::Relaxed)
    })
}

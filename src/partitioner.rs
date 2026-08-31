//! Record partitioning: Kafka murmur2 and a pluggable [`Partitioner`].

use std::fmt;
use std::sync::atomic::{AtomicI32, Ordering};
use std::sync::Arc;

/// Maps a produce record to a partition index.
///
/// Called only when [`crate::ProduceRecord::partition`] is `None`. The returned
/// value is clamped into `0..num_partitions` by the producer.
pub trait Partitioner: Send + Sync + 'static {
    /// Choose a partition in `0..num_partitions`.
    ///
    /// `key` is `None` when the record has no key.
    fn partition(&self, topic: &str, key: Option<&[u8]>, num_partitions: i32) -> i32;
}

/// Java `DefaultPartitioner`: murmur2 when there is a key, round-robin if not.
#[derive(Debug, Default)]
pub struct DefaultPartitioner {
    rr: AtomicI32,
}

impl DefaultPartitioner {
    /// Start round-robin at partition 0.
    #[must_use]
    pub const fn new() -> Self {
        Self {
            rr: AtomicI32::new(0),
        }
    }
}

impl Partitioner for DefaultPartitioner {
    fn partition(&self, _topic: &str, key: Option<&[u8]>, num_partitions: i32) -> i32 {
        if num_partitions <= 0 {
            return 0;
        }
        match key {
            Some(k) => partition_for_key(k, num_partitions),
            None => to_positive(self.rr.fetch_add(1, Ordering::Relaxed)) % num_partitions,
        }
    }
}

/// [`Arc`] wrapper so [`crate::ProducerConfig`] can stay `Clone` + `Debug`.
#[derive(Clone)]
pub struct PartitionerBox(Arc<dyn Partitioner>);

impl PartitionerBox {
    /// Wrap any [`Partitioner`].
    pub fn new(p: impl Partitioner) -> Self {
        Self(Arc::new(p))
    }

    pub(crate) fn arc(&self) -> Arc<dyn Partitioner> {
        Arc::clone(&self.0)
    }
}

impl Default for PartitionerBox {
    fn default() -> Self {
        Self::new(DefaultPartitioner::new())
    }
}

impl fmt::Debug for PartitionerBox {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("Partitioner")
    }
}

/// Kafka-compatible Murmur2 (seed 0x9747b28c), matching
/// `org.apache.kafka.common.utils.Utils.murmur2`.
#[must_use]
pub fn murmur2(data: &[u8]) -> i32 {
    const M: u32 = 0x5bd1e995;
    const R: u32 = 24;
    const SEED: u32 = 0x9747b28c;
    let len = u32::try_from(data.len()).unwrap_or(u32::MAX);
    let mut h = SEED ^ len;
    let (chunks, rest) = data.split_at(data.len() / 4 * 4);
    for chunk in chunks.chunks_exact(4) {
        let &[a, b, c, d] = chunk else {
            continue;
        };
        let mut k =
            u32::from(a) | (u32::from(b) << 8) | (u32::from(c) << 16) | (u32::from(d) << 24);
        k = k.wrapping_mul(M);
        k ^= k >> R;
        k = k.wrapping_mul(M);
        h = h.wrapping_mul(M);
        h ^= k;
    }
    match *rest {
        [a, b, c] => {
            h ^= u32::from(c) << 16;
            h ^= u32::from(b) << 8;
            h ^= u32::from(a);
            h = h.wrapping_mul(M);
        }
        [a, b] => {
            h ^= u32::from(b) << 8;
            h ^= u32::from(a);
            h = h.wrapping_mul(M);
        }
        [a] => {
            h ^= u32::from(a);
            h = h.wrapping_mul(M);
        }
        _ => {}
    }
    h ^= h >> 13;
    h = h.wrapping_mul(M);
    h ^= h >> 15;
    i32::from_ne_bytes(h.to_ne_bytes())
}

/// Java `Utils.toPositive` (`number & 0x7fffffff`).
///
/// Used so a murmur2 hash can be a partition index. This is not
/// [`abs`]: negative inputs keep the low 31 bits rather than the
/// magnitude.
#[must_use]
pub fn to_positive(n: i32) -> i32 {
    n & 0x7fff_ffff
}

/// Java `Utils.abs`. [`i32::MIN`] is `0` (unlike [`i32::abs`]).
#[must_use]
pub fn abs(n: i32) -> i32 {
    n.checked_abs().unwrap_or(0)
}

/// Java `DefaultPartitioner` for a keyed record: `murmur2(key) % num_partitions`.
#[must_use]
pub fn partition_for_key(key: &[u8], num_partitions: i32) -> i32 {
    if num_partitions <= 0 {
        return 0;
    }
    to_positive(murmur2(key)) % num_partitions
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn murmur2_matches_java_utils() {
        // Empty-string vector is the widely copied Java Utils.murmur2 result.
        assert_eq!(murmur2(b""), 275_646_681);
        assert_eq!(murmur2(b"kafka"), -798_503_068);
        assert_eq!(partition_for_key(b"key", 1), 0);
        assert!(partition_for_key(b"key", 16) >= 0);
        assert!(partition_for_key(b"key", 16) < 16);
        assert_eq!(partition_for_key(b"key", 0), 0);
        assert_eq!(to_positive(-1), i32::MAX);
        assert_eq!(to_positive(1), 1);
        assert_eq!(to_positive(i32::MIN), 0);
    }

    #[test]
    fn abs_matches_java_utils() {
        assert_eq!(abs(i32::MIN), 0);
        assert_eq!(abs(-10), 10);
        assert_eq!(abs(10), 10);
        assert_eq!(abs(0), 0);
        assert_eq!(abs(-1), 1);
    }

    #[test]
    fn default_partitioner_keys_match_murmur2() {
        let p = DefaultPartitioner::new();
        assert_eq!(
            p.partition("t", Some(b"key"), 16),
            partition_for_key(b"key", 16)
        );
        let a = p.partition("t", None, 3);
        let b = p.partition("t", None, 3);
        assert_ne!(a, b);
        assert!((0..3).contains(&a));
        assert!((0..3).contains(&b));
    }
}

//! Kafka-compatible partitioners. Hash is Java `Utils.murmur2` of the key.

/// Kafka murmur2 (seed `0x9747b28c`), then `hash & 0x7fff_ffff`.
pub fn murmur2(data: &[u8]) -> i32 {
    const SEED: i32 = 0x9747_b28c_u32 as i32;
    const M: i32 = 0x5bd1_e995_u32 as i32;
    const R: u32 = 24;

    let length = data.len() as i32;
    let mut h = SEED ^ length;
    let length4 = (data.len() / 4) as i32;

    for i in 0..length4 {
        let i4 = (i * 4) as usize;
        let mut k = (data[i4] as i32) & 0xff
            | ((data[i4 + 1] as i32) & 0xff) << 8
            | ((data[i4 + 2] as i32) & 0xff) << 16
            | ((data[i4 + 3] as i32) & 0xff) << 24;
        k = k.wrapping_mul(M);
        k ^= ((k as u32) >> R) as i32;
        k = k.wrapping_mul(M);
        h = h.wrapping_mul(M);
        h ^= k;
    }

    let left = data.len() % 4;
    if left != 0 {
        if left >= 3 {
            h ^= ((data[data.len() - 3] as i32) & 0xff) << 16;
        }
        if left >= 2 {
            h ^= ((data[data.len() - 2] as i32) & 0xff) << 8;
        }
        if left >= 1 {
            h ^= (data[data.len() - 1] as i32) & 0xff;
        }
        h = h.wrapping_mul(M);
    }

    h ^= ((h as u32) >> 13) as i32;
    h = h.wrapping_mul(M);
    h ^= ((h as u32) >> 15) as i32;
    h
}

/// `toPositive(murmur2(key)) % num_partitions` (Java DefaultPartitioner).
pub fn hash_partition(key: &[u8], num_partitions: i32) -> i32 {
    debug_assert!(num_partitions > 0);
    (murmur2(key) & 0x7fff_ffff) % num_partitions
}

/// Sticky assignment for null keys: keep `current` until `rotate` is true.
#[derive(Debug, Default)]
pub struct Sticky {
    current: Option<i32>,
}

impl Sticky {
    /// Choose a partition for a null key.
    pub fn pick(&mut self, num_partitions: i32, rotate: bool) -> i32 {
        if num_partitions <= 0 {
            return 0;
        }
        if rotate || self.current.is_none() {
            let next = match self.current {
                Some(c) => (c + 1) % num_partitions,
                None => 0,
            };
            self.current = Some(next);
        }
        self.current.unwrap_or(0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn murmur2_stable_and_kafka_empty() {
        // Java Utils.murmur2(new byte[0]) with seed 0x9747b28c.
        assert_eq!(murmur2(b""), 275646681);
        assert_eq!(murmur2(b"hello"), murmur2(b"hello"));
        assert_ne!(murmur2(b"hello"), murmur2(b"world"));
    }

    #[test]
    fn hash_partition_in_range() {
        let n = 6;
        for key in [b"" as &[u8], b"k", b"partitionline"] {
            let p = hash_partition(key, n);
            assert!((0..n).contains(&p), "{p}");
        }
    }

    #[test]
    fn sticky_holds_then_rotates() {
        let mut s = Sticky::default();
        let a = s.pick(6, false);
        let b = s.pick(6, false);
        assert_eq!(a, b);
        let c = s.pick(6, true);
        assert_ne!(c, a);
        assert_eq!(c, (a + 1) % 6);
    }
}

/// Kafka-compatible Murmur2 (seed 0x9747b28c), matching
/// `org.apache.kafka.common.utils.Utils.murmur2`.
pub fn murmur2(data: &[u8]) -> i32 {
    const M: u32 = 0x5bd1e995;
    const R: u32 = 24;
    const SEED: u32 = 0x9747b28c;
    let mut h = SEED ^ data.len() as u32;
    let chunks = data.len() / 4;
    for i in 0..chunks {
        let i4 = i * 4;
        let mut k = u32::from(data[i4])
            | (u32::from(data[i4 + 1]) << 8)
            | (u32::from(data[i4 + 2]) << 16)
            | (u32::from(data[i4 + 3]) << 24);
        k = k.wrapping_mul(M);
        k ^= k >> R;
        k = k.wrapping_mul(M);
        h = h.wrapping_mul(M);
        h ^= k;
    }
    let rest = &data[chunks * 4..];
    match rest.len() {
        3 => {
            h ^= u32::from(rest[2]) << 16;
            h ^= u32::from(rest[1]) << 8;
            h ^= u32::from(rest[0]);
            h = h.wrapping_mul(M);
        }
        2 => {
            h ^= u32::from(rest[1]) << 8;
            h ^= u32::from(rest[0]);
            h = h.wrapping_mul(M);
        }
        1 => {
            h ^= u32::from(rest[0]);
            h = h.wrapping_mul(M);
        }
        _ => {}
    }
    h ^= h >> 13;
    h = h.wrapping_mul(M);
    h ^= h >> 15;
    h as i32
}

pub fn to_positive(n: i32) -> i32 {
    n & 0x7fff_ffff
}

pub fn partition_for_key(key: &[u8], num_partitions: i32) -> i32 {
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
    }
}

//! Byte-compatible with the independent C and franz-go comparison peers.

pub fn mix(mut value: u64) -> u64 {
    value = value.wrapping_add(0x9e3779b97f4a7c15);
    value = (value ^ (value >> 30)).wrapping_mul(0xbf58476d1ce4e5b9);
    value = (value ^ (value >> 27)).wrapping_mul(0x94d049bb133111eb);
    value ^ (value >> 31)
}

pub fn key(seed: u64, index: u64) -> [u8; 16] {
    let mut key = [0; 16];
    key[..8].copy_from_slice(&index.to_be_bytes());
    key[8..].copy_from_slice(&mix(seed ^ index).to_be_bytes());
    key
}

pub fn value(seed: u64, index: u64, bytes: usize, seeded: bool) -> Vec<u8> {
    if !seeded {
        return vec![b'x'; bytes];
    }
    let mut result = vec![0; bytes];
    let mut state = seed ^ index.wrapping_mul(0x9e3779b97f4a7c15);
    for chunk in result.chunks_mut(8) {
        state = mix(state);
        chunk.copy_from_slice(&state.to_be_bytes()[..chunk.len()]);
    }
    result
}

pub fn hex(bytes: &[u8]) -> String {
    const DIGITS: &[u8; 16] = b"0123456789abcdef";
    let mut output = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        output.push(DIGITS[(byte >> 4) as usize] as char);
        output.push(DIGITS[(byte & 15) as usize] as char);
    }
    output
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn published_splitmix_vectors_and_partial_chunks() {
        assert_eq!(mix(0), 0xe220a8397b1dcdaf);
        assert_eq!(mix(1), 0x910a2dec89025cc1);
        assert_eq!(hex(&key(0, 1)), "0000000000000001910a2dec89025cc1");
        assert_eq!(hex(&value(0, 0, 8, true)), "e220a8397b1dcdaf");
        assert_eq!(value(0, 0, 3, true), [0xe2, 0x20, 0xa8]);
        assert!(value(1, 1, 0, true).is_empty());
        assert_eq!(value(5, 9, 7, false), b"xxxxxxx");
    }
}

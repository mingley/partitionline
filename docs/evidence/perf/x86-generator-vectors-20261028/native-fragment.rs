fn mix(mut value: u64) -> u64 {
    value = value.wrapping_add(0x9e37_79b9_7f4a_7c15);
    value = (value ^ (value >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    value = (value ^ (value >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    value ^ (value >> 31)
}
pub(crate) fn seeded_payload(seed: u64, id: u64, size: usize) -> Bytes {
    let mut state = seed ^ id.wrapping_mul(0x9e37_79b9_7f4a_7c15);
    let mut output = Vec::with_capacity(size);
    while output.len() < size {
        state = mix(state);
        let length = (size - output.len()).min(8);
        output.extend_from_slice(state.to_be_bytes().get(..length).unwrap_or(&[]));
    }
    Bytes::from(output)
}
pub(crate) fn id_key(seed: u64, id: u64) -> Bytes {
    let mut key = [0; 16];
    key[..8].copy_from_slice(&id.to_be_bytes());
    key[8..].copy_from_slice(&mix(seed ^ id).to_be_bytes());
    Bytes::copy_from_slice(&key)
}

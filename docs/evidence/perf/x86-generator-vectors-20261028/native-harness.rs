use std::io::{self,BufRead};
struct Bytes(Vec<u8>);
impl Bytes {fn from(v:Vec<u8>)->Self{Self(v)} fn copy_from_slice(v:&[u8])->Self{Self(v.to_vec())}}
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

fn main(){for line in io::stdin().lock().lines(){let s=line.unwrap();let v:Vec<u64>=s.split_whitespace().map(|x|x.parse().unwrap()).collect();let k=id_key(v[0],v[1]);let p=seeded_payload(v[0],v[1],v[2] as usize);for b in k.0{print!("{b:02x}")}print!(" ");for b in p.0{print!("{b:02x}")}println!();}}

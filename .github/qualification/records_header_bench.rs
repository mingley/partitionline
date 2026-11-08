//! Isolated record validation benchmark fixture.
use partitionline_broker::records::{self, Limits};
use std::hint::black_box;
use std::time::Instant;

fn vint(out: &mut Vec<u8>, n: i32) {
    let mut value = ((n as u32) << 1) ^ ((n >> 31) as u32);
    while value >= 128 {
        out.push((value as u8) | 128);
        value >>= 7;
    }
    out.push(value as u8);
}
fn vlong(out: &mut Vec<u8>, n: i64) {
    let mut value = ((n as u64) << 1) ^ ((n >> 63) as u64);
    while value >= 128 {
        out.push((value as u8) | 128);
        value >>= 7;
    }
    out.push(value as u8);
}
fn field(out: &mut Vec<u8>, data: Option<&[u8]>) {
    if let Some(data) = data {
        vint(out, i32::try_from(data.len()).unwrap());
        out.extend_from_slice(data);
    } else {
        vint(out, -1);
    }
}
fn batch(count: usize, headers: usize, late: bool, payload: usize) -> (Vec<u8>, usize) {
    let mut body = Vec::new();
    let mut encoded = Vec::new();
    let value = vec![b'x'; payload];
    let mut header_count = 0;
    for n in 0..count {
        body.clear();
        body.push(0);
        vlong(&mut body, n as i64);
        vint(&mut body, n as i32);
        field(&mut body, Some(b"key-1234"));
        field(&mut body, Some(&value));
        let current = if late && n + 1 != count { 0 } else { headers };
        vint(&mut body, current as i32);
        for h in 0..current {
            let key: &[u8] = if h & 1 == 0 { b"header-key" } else { "caf\u{e9}".as_bytes() };
            vint(&mut body, key.len() as i32);
            body.extend_from_slice(key);
            field(&mut body, if h & 1 == 0 { Some(b"header-value") } else { None });
        }
        header_count += current;
        vint(&mut encoded, body.len() as i32);
        encoded.extend_from_slice(&body);
    }
    let mut out = vec![0u8; 61];
    let size = (61 + encoded.len() - 12) as i32;
    out[8..12].copy_from_slice(&size.to_be_bytes());
    out[16] = 2;
    out[23..27].copy_from_slice(&((count - 1) as i32).to_be_bytes());
    out[27..35].copy_from_slice(&1_000i64.to_be_bytes());
    out[35..43].copy_from_slice(&(1_000i64 + count as i64 - 1).to_be_bytes());
    out[43..51].copy_from_slice(&(-1i64).to_be_bytes());
    out[51..53].copy_from_slice(&(-1i16).to_be_bytes());
    out[53..57].copy_from_slice(&(-1i32).to_be_bytes());
    out[57..61].copy_from_slice(&(count as i32).to_be_bytes());
    out.extend_from_slice(&encoded);
    let crc = crc32c::crc32c(&out[21..]);
    out[17..21].copy_from_slice(&crc.to_be_bytes());
    (out, header_count)
}
fn main() {
    let args: Vec<String> = std::env::args().collect();
    let mode = &args[1];
    let (count, headers, late, payload, iters) = match mode.as_str() {
        "one_plain" => (1, 0, false, 32, 50_000),
        "many_plain" => (1024, 0, false, 32, 180),
        "many_header" => (1024, 1, false, 32, 180),
        "many_four" => (1024, 4, false, 32, 100),
        "late_header" => (1024, 1, true, 32, 180),
        "large_header" => (256, 1, false, 1024, 180),
        other => panic!("unknown mode {other}"),
    };
    let (data, expected_headers) = batch(count, headers, late, payload);
    let limits = Limits::default();
    let checked = records::validate(&data, limits).unwrap();
    assert_eq!(checked.as_bytes(), data);
    assert_eq!(checked.batch_count(), 1);
    assert_eq!(checked.record_count(), count);
    assert_eq!(checked.header_count(), expected_headers);
    let summary = records::validate_read(&data, limits).unwrap();
    assert_eq!(summary.record_count(), count);
    assert_eq!(summary.header_count(), expected_headers);
    let batch = checked.batches().next().unwrap().unwrap();
    let mut seen = 0usize;
    let expected_value = vec![b'x'; payload];
    for item in batch.records() {
        let record = item.unwrap();
        assert_eq!(record.offset, seen as i64);
        assert_eq!(record.timestamp, 1_000 + seen as i64);
        assert_eq!(record.key, Some(&b"key-1234"[..]));
        assert_eq!(record.value, Some(&expected_value[..]));
        seen += 1;
    }
    assert_eq!(seen, count);
    let mut damaged = data.clone();
    damaged[21] ^= 0x80;
    assert!(matches!(
        records::validate(&damaged, limits),
        Err(records::Error::Invalid { kind: records::Invalid::Checksum, .. })
    ));
    eprintln!("case={mode} bytes={} crc={:08x} records={count} headers={expected_headers} iterations={iters}", data.len(), crc32c::crc32c(&data));
    let work = || {
        let mut checksum = 0usize;
        for _ in 0..iters {
            let result = records::validate(black_box(&data), limits).unwrap();
            checksum = checksum.wrapping_add(result.record_count()).wrapping_add(result.header_count());
            black_box(result.as_bytes());
        }
        assert_eq!(checksum, iters * (count + expected_headers));
    };
    work();
    for sample in 0..7 {
        let start = Instant::now();
        work();
        let ns = start.elapsed().as_nanos();
        println!("{mode} {sample} {iters} {ns}");
    }
}

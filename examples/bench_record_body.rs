//! Whole-batch producer wire-encoding experiment. Not a public benchmark claim.
use std::error::Error;
use std::fs::File;
use std::io::{BufWriter, Write};
use std::time::Instant;
use bytes::{Bytes, BytesMut};
use partitionline::protocol::records::{
    decode_record_batch, write_record_batch_scratch, write_uncompressed_batch_cooperative,
    BatchHeader, CompressScratch,
    EncodeRecord, Header, Record,
};

#[derive(Clone, Copy)]
struct Shape {
    name: &'static str,
    count: usize,
    value: usize,
    headers: usize,
    codec: i16,
    key: u8,
    mixed: bool,
}

const SHAPES: &[Shape] = &[
    Shape { name: "one_empty", count: 1, value: 0, headers: 0, codec: 0, key: 0, mixed: false },
    Shape { name: "one_100", count: 1, value: 100, headers: 0, codec: 0, key: 2, mixed: false },
    Shape { name: "f01_8_plain", count: 8, value: 100, headers: 0, codec: 0, key: 2, mixed: false },
    Shape { name: "f02_8_headers", count: 8, value: 100, headers: 3, codec: 0, key: 2, mixed: false },
    Shape { name: "f03_500_plain", count: 500, value: 100, headers: 0, codec: 0, key: 2, mixed: false },
    Shape { name: "f04_32_headers", count: 32, value: 1024, headers: 2, codec: 0, key: 2, mixed: false },
    Shape { name: "many_null_key", count: 512, value: 100, headers: 0, codec: 0, key: 0, mixed: false },
    Shape { name: "many_empty_key", count: 512, value: 100, headers: 0, codec: 0, key: 1, mixed: false },
    Shape { name: "header_heavy", count: 128, value: 100, headers: 16, codec: 0, key: 2, mixed: false },
    Shape { name: "boundaries", count: 128, value: 128, headers: 1, codec: 0, key: 2, mixed: true },
    Shape { name: "large_values", count: 16, value: 65_536, headers: 0, codec: 0, key: 2, mixed: false },
    Shape { name: "gzip_128", count: 128, value: 100, headers: 0, codec: 1, key: 2, mixed: false },
    Shape { name: "snappy_128", count: 128, value: 100, headers: 0, codec: 2, key: 2, mixed: false },
    Shape { name: "lz4_128", count: 128, value: 100, headers: 0, codec: 3, key: 2, mixed: false },
    Shape { name: "async_f01_8", count: 8, value: 100, headers: 0, codec: 0, key: 2, mixed: false },
    Shape { name: "async_f03_500", count: 500, value: 100, headers: 0, codec: 0, key: 2, mixed: false },
    Shape { name: "async_headers", count: 128, value: 100, headers: 16, codec: 0, key: 2, mixed: false },
    Shape { name: "async_large", count: 16, value: 65_536, headers: 0, codec: 0, key: 2, mixed: false },
];

fn mix(mut n: u64) -> u64 {
    n = n.wrapping_add(0x9e37_79b9_7f4a_7c15);
    n = (n ^ (n >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    n = (n ^ (n >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    n ^ (n >> 31)
}

fn payload(index: usize, length: usize) -> Bytes {
    let mut bytes = Vec::with_capacity(length);
    let mut state = (index as u64) ^ 0x54fa_33e1;
    while bytes.len() < length {
        state = mix(state);
        let n = (length - bytes.len()).min(8);
        bytes.extend_from_slice(&state.to_le_bytes()[..n]);
    }
    Bytes::from(bytes)
}

fn make_records(shape: Shape) -> Vec<Record> {
    let lengths = [0, 1, 7, 63, 64, 127, 128, 129, 1024];
    (0..shape.count).map(|i| {
        let len = if shape.mixed { lengths[i % lengths.len()] } else { shape.value };
        let key = match shape.key {
            0 => None,
            1 => Some(Bytes::new()),
            _ => Some(payload(i + 1000, 16)),
        };
        let value = if shape.mixed && i % 4 == 0 { None } else { Some(payload(i, len)) };
        let headers = (0..shape.headers).map(|h| {
            let key = format!("h{h}-é-{}", i % 5);
            let value = if (h + i) % 3 == 0 { None }
                        else { Some(payload(i * 31 + h, (h * 17 + i) % 140)) };
            Header { key, value }
        }).collect();
        Record { offset: i as i64, timestamp: (i % 17) as i64, key, value, headers }
    }).collect()
}

fn encode(
    records: &[Record],
    shape: Shape,
    output: &mut BytesMut,
    scratch: &mut CompressScratch,
) -> Result<(), Box<dyn Error>> {
    output.clear();
    let header = BatchHeader {
        attributes: shape.codec,
        count: i32::try_from(records.len())?,
        max_timestamp: 16,
        ..BatchHeader::default()
    };
    write_record_batch_scratch(output, &header, records.iter().map(EncodeRecord::from_record), scratch)?;
    Ok(())
}

async fn encode_cooperative(
    records: &[Record],
    shape: Shape,
    output: &mut BytesMut,
    scratch: &mut CompressScratch,
) -> Result<(), Box<dyn Error>> {
    output.clear();
    let header = BatchHeader {
        attributes: shape.codec,
        count: i32::try_from(records.len())?,
        max_timestamp: 16,
        ..BatchHeader::default()
    };
    write_uncompressed_batch_cooperative(
        output, &header, records.iter().map(EncodeRecord::from_record), scratch,
    ).await?;
    Ok(())
}

fn checked_one(
    output: &mut BufWriter<File>,
    shape: Shape,
    scratch: &mut CompressScratch,
    runtime: &tokio::runtime::Runtime,
) -> Result<(), Box<dyn Error>> {
    let records = make_records(shape);
    let mut encoded = BytesMut::new();
    encode(&records, shape, &mut encoded, scratch)?;
    if shape.codec == 0 {
        let mut async_output = BytesMut::new();
        let mut async_scratch = CompressScratch::new();
        runtime.block_on(encode_cooperative(&records, shape, &mut async_output, &mut async_scratch))?;
        if async_output != encoded { return Err("cooperative batch mismatch".into()); }
    }
    let mut input = encoded.clone().freeze();
    let decoded = decode_record_batch(&mut input)?;
    if decoded.records() != records.as_slice() || !input.is_empty() {
        return Err(format!("roundtrip mismatch {}", shape.name).into());
    }
    output.write_all(&(u64::try_from(encoded.len())?).to_le_bytes())?;
    output.write_all(&encoded)?;
    Ok(())
}

fn check_error(
    output: &mut BufWriter<File>,
    header: BatchHeader,
    records: &[Record],
    scratch: &mut CompressScratch,
    runtime: &tokio::runtime::Runtime,
) -> Result<(), Box<dyn Error>> {
    let sentinel = b"before";
    let mut dst = BytesMut::from(sentinel.as_slice());
    let result = write_record_batch_scratch(
        &mut dst, &header, records.iter().map(EncodeRecord::from_record), scratch
    );
    let err = match result { Ok(()) => return Err("expected encoder failure".into()), Err(err) => err };
    if dst.as_ref() != sentinel { return Err("failed encode changed caller buffer".into()); }
    let message = err.to_string();
    if header.attributes == 0 {
        let mut cooperative = BytesMut::from(sentinel.as_slice());
        let mut cooperative_scratch = CompressScratch::new();
        let result = runtime.block_on(write_uncompressed_batch_cooperative(
            &mut cooperative, &header, records.iter().map(EncodeRecord::from_record),
            &mut cooperative_scratch,
        ));
        let cooperative_error = match result {
            Ok(()) => return Err("expected cooperative encoder failure".into()),
            Err(error) => error.to_string(),
        };
        if cooperative_error != message || cooperative.as_ref() != sentinel {
            return Err("cooperative error or rollback differs".into());
        }
    }
    output.write_all(&(u64::try_from(message.len())?).to_le_bytes())?;
    output.write_all(message.as_bytes())?;
    output.write_all(&dst)?;
    Ok(())
}

fn corpus(path: &str) -> Result<(), Box<dyn Error>> {
    let mut out = BufWriter::new(File::create(path)?);
    let mut scratch = CompressScratch::new();
    let runtime = tokio::runtime::Builder::new_current_thread().enable_all().build()?;
    let mut cases = 0usize;
    for codec in 0..=3 {
        for count in [1, 7, 64] {
            for value in [0, 1, 63, 64, 127, 128, 1024] {
                for headers in [0, 1, 3, 9] {
                    for key in [0, 1, 2] {
                        checked_one(&mut out, Shape {
                            name: "corpus", count, value, headers, codec, key, mixed: false,
                        }, &mut scratch, &runtime)?;
                        cases += 1;
                    }
                }
            }
        }
    }
    for codec in 0..=3 {
        checked_one(&mut out, Shape {
            name: "mixed", count: 128, value: 128, headers: 3, codec, key: 2, mixed: true,
        }, &mut scratch, &runtime)?;
        cases += 1;
    }
    let shape = SHAPES[1];
    let mut records = make_records(shape);
    let header = BatchHeader { count: 2, ..BatchHeader::default() };
    check_error(&mut out, header, &records, &mut scratch, &runtime)?;
    records.push(records[0].clone());
    records[1].offset = 1;
    let header = BatchHeader { base_offset: i64::MAX, count: 2, ..BatchHeader::default() };
    check_error(&mut out, header, &records, &mut scratch, &runtime)?;
    records[0].timestamp = i64::MAX;
    let header = BatchHeader { base_timestamp: i64::MIN, count: 2, ..BatchHeader::default() };
    check_error(&mut out, header, &records, &mut scratch, &runtime)?;
    out.flush()?;
    eprintln!("corpus_cases={cases}, error_cases=3");
    Ok(())
}

fn run(shape: Shape, iterations: u64) -> Result<u128, Box<dyn Error>> {
    let records = make_records(shape);
    let mut output = BytesMut::new();
    let mut scratch = CompressScratch::new();
    if shape.name.starts_with("async_") {
        let runtime = tokio::runtime::Builder::new_current_thread().enable_all().build()?;
        return runtime.block_on(async {
            for _ in 0..iterations.min(256) {
                encode_cooperative(&records, shape, &mut output, &mut scratch).await?;
                let _observed = std::hint::black_box(&output);
            }
            let start = Instant::now();
            for _ in 0..iterations {
                encode_cooperative(std::hint::black_box(&records), shape, &mut output, &mut scratch).await?;
                let _observed = std::hint::black_box(&output);
            }
            Ok(start.elapsed().as_nanos())
        });
    }
    for _ in 0..iterations.min(256) {
        encode(&records, shape, &mut output, &mut scratch)?;
        let _observed = std::hint::black_box(&output);
    }
    let start = Instant::now();
    for _ in 0..iterations {
        encode(std::hint::black_box(&records), shape, &mut output, &mut scratch)?;
        let _observed = std::hint::black_box(&output);
    }
    Ok(start.elapsed().as_nanos())
}

fn main() -> Result<(), Box<dyn Error>> {
    let mut args = std::env::args().skip(1);
    let mode = args.next().ok_or("missing mode")?;
    if mode == "corpus" {
        return corpus(&args.next().ok_or("missing output path")?);
    }
    let name = args.next().ok_or("missing shape")?;
    let shape = *SHAPES.iter().find(|s| s.name == name).ok_or("unknown shape")?;
    match mode.as_str() {
        "calibrate" => {
            let mut iters = 8u64;
            loop {
                let ns = run(shape, iters)?;
                if ns >= 120_000_000 || iters >= 16_777_216 {
                    println!("{iters}");
                    return Ok(());
                }
                iters *= 2;
            }
        }
        "bench" => {
            let iters = args.next().ok_or("missing iterations")?.parse::<u64>()?;
            if iters == 0 || iters > 16_777_216 { return Err("invalid iteration count".into()); }
            println!("{name},{iters},{}", run(shape, iters)?);
            Ok(())
        }
        _ => Err("unknown mode".into()),
    }
}

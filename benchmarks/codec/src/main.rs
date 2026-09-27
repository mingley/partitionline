//! codec-preflight (KL04-09): fixture correctness preflight, allocation
//! census, slow-transform observability check and reproducible baseline
//! emission. Usage:
//!
//!   codec-preflight preflight [--fixtures DIR]
//!   codec-preflight census [--fixtures DIR]
//!   codec-preflight slowcheck [--fixtures DIR]
//!   codec-preflight baseline [--fixtures DIR] [--json PATH]

use std::hint::black_box;
use std::path::{Path, PathBuf};
use std::time::Instant;

use bytes::{Bytes, BytesMut};
use partitionline::protocol::records::{decode_record_batch, encode_record_batch, Compression};

use codec::{
    build_records, census, compressed_batch, crc_body, decode_fixture, encode_batch, load_fixtures,
    transform_fast, transform_slow, CountingAlloc, Fixture,
};

#[global_allocator]
static ALLOC: CountingAlloc = CountingAlloc;

fn fixture_dir(args: &[String]) -> PathBuf {
    if let Some(pos) = args.iter().position(|a| a == "--fixtures") {
        return PathBuf::from(args.get(pos + 1).cloned().unwrap_or_default());
    }
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("fixtures")
}

fn fail(msg: String) -> ! {
    eprintln!("codec-preflight: {msg}");
    std::process::exit(1);
}

/// Correctness preflight: decode every independent fixture, verify
/// manifest facts, require byte-identical re-encode, and round-trip
/// every compression codec in-process.
fn preflight(dir: &Path) -> Vec<Fixture> {
    let fixtures = load_fixtures(dir).unwrap_or_else(|e| fail(e));
    for fixture in &fixtures {
        let batch = decode_fixture(fixture).unwrap_or_else(|e| fail(e));
        let back = encode_batch(&batch).unwrap_or_else(|e| fail(e));
        if back != fixture.bytes {
            fail(format!(
                "{}: re-encode differs from fixture bytes",
                fixture.name
            ));
        }
        println!(
            "preflight {}: {} records, {} bytes, decode+re-encode identical",
            fixture.name,
            fixture.records,
            fixture.bytes.len()
        );
    }
    for (name, codec) in [
        ("gzip", Compression::Gzip),
        ("snappy", Compression::Snappy),
        ("lz4", Compression::Lz4),
    ] {
        for entropy in ["random", "text"] {
            let records = build_records(0xC0DEC, 500, 16, 100, entropy, 0);
            let batch = compressed_batch(records, codec);
            let mut buf = BytesMut::new();
            encode_record_batch(&mut buf, &batch).unwrap_or_else(|e| fail(e.to_string()));
            let wire = buf.freeze().to_vec();
            let mut back = Bytes::from(wire);
            let decoded = decode_record_batch(&mut back).unwrap_or_else(|e| fail(e.to_string()));
            if decoded.records().len() != 500 {
                fail(format!("{name}/{entropy}: lost records in round-trip"));
            }
            if decoded.records() != batch.records() {
                fail(format!("{name}/{entropy}: records differ after round-trip"));
            }
        }
        println!("preflight {name}: 500-record random+text round-trips identical");
    }
    println!("preflight: PASS");
    fixtures
}

/// Allocation census: deterministic alloc counts per op (single thread).
fn census_cmd(fixtures: &[Fixture]) -> Vec<(String, u64, u64)> {
    let mut rows = Vec::new();
    for fixture in fixtures {
        let batch = decode_fixture(fixture).unwrap_or_else(|e| fail(e));
        let (_, allocs, bytes) = census(|| {
            let mut buf = BytesMut::new();
            encode_record_batch(black_box(&mut buf), black_box(&batch)).unwrap();
            black_box(buf)
        });
        rows.push((format!("encode/{}", fixture.name), allocs, bytes));
        let (_, allocs, bytes) = census(|| {
            let mut buf = Bytes::from(fixture.bytes.clone());
            black_box(decode_record_batch(&mut buf).unwrap())
        });
        rows.push((format!("decode/{}", fixture.name), allocs, bytes));
    }
    for (name, codec) in [
        ("gzip", Compression::Gzip),
        ("snappy", Compression::Snappy),
        ("lz4", Compression::Lz4),
    ] {
        let records = build_records(0xC0DEC, 500, 16, 100, "text", 0);
        let batch = compressed_batch(records, codec);
        let (_, allocs, bytes) = census(|| {
            let mut buf = BytesMut::new();
            encode_record_batch(black_box(&mut buf), black_box(&batch)).unwrap();
            black_box(buf)
        });
        rows.push((format!("compress/{name}/text"), allocs, bytes));
        let mut wire = BytesMut::new();
        encode_record_batch(&mut wire, &batch).unwrap();
        let wire = wire.freeze().to_vec();
        let (_, allocs, bytes) = census(|| {
            let mut buf = Bytes::from(wire.clone());
            black_box(decode_record_batch(&mut buf).unwrap())
        });
        rows.push((format!("decompress/{name}/text"), allocs, bytes));
    }
    for (op, allocs, bytes) in &rows {
        println!("census {op}: allocs={allocs} bytes={bytes}");
    }
    rows
}

/// Slow-transform check: the deliberately slower transformation must be
/// observably slower with identical semantic output. Interleaved rounds
/// cancel host drift; the bar (1.5x) sits far below the ~3x mechanism.
fn slowcheck(fixtures: &[Fixture]) -> f64 {
    let f03 = fixtures
        .iter()
        .find(|f| f.name == "f03")
        .unwrap_or_else(|| fail("f03 fixture missing".to_owned()));
    let fast_records = transform_fast(&f03.bytes).unwrap_or_else(|e| fail(e));
    let slow_records = transform_slow(&f03.bytes).unwrap_or_else(|e| fail(e));
    if fast_records != slow_records {
        fail("slow transform output differs semantically".to_owned());
    }
    let rounds = 10u32;
    let iters = 20u32;
    let mut fast_total = std::time::Duration::ZERO;
    let mut slow_total = std::time::Duration::ZERO;
    for _ in 0..rounds {
        let start = Instant::now();
        for _ in 0..iters {
            black_box(transform_fast(black_box(&f03.bytes)).unwrap());
        }
        fast_total += start.elapsed();
        let start = Instant::now();
        for _ in 0..iters {
            black_box(transform_slow(black_box(&f03.bytes)).unwrap());
        }
        slow_total += start.elapsed();
    }
    let ratio = slow_total.as_secs_f64() / fast_total.as_secs_f64().max(1e-9);
    println!(
        "slowcheck: fast={:.3}ms slow={:.3}ms ratio={ratio:.2}x ({} rounds x {} iters, f03)",
        fast_total.as_secs_f64() * 1000.0,
        slow_total.as_secs_f64() * 1000.0,
        rounds,
        iters
    );
    if ratio < 1.5 {
        fail(format!(
            "slow transform not observable (ratio {ratio:.2}x < 1.5x)"
        ));
    }
    // Touch crc_body so dead-code lints stay quiet in bins that skip it.
    let _ = crc_body(&f03.bytes).len();
    ratio
}

fn git_sha() -> String {
    std::process::Command::new("git")
        .args(["rev-parse", "--short", "HEAD"])
        .output()
        .ok()
        .and_then(|o| String::from_utf8(o.stdout).ok())
        .map(|s| s.trim().to_owned())
        .unwrap_or_else(|| "unknown".to_owned())
}

fn baseline(dir: &Path, json_path: Option<&str>) {
    let fixtures = preflight(dir);
    let census_rows = census_cmd(&fixtures);
    let ratio = slowcheck(&fixtures);
    let doc = serde_json::json!({
        "tool": "codec-preflight",
        "git_sha": git_sha(),
        "rustc": std::env::var("RUSTC_VERSION").unwrap_or_else(|_| "unknown".to_owned()),
        "os": std::env::consts::OS,
        "arch": std::env::consts::ARCH,
        "fixtures": fixtures.iter().map(|f| serde_json::json!({
            "name": f.name, "bytes": f.bytes.len(), "records": f.records,
        })).collect::<Vec<_>>(),
        "census": census_rows.iter().map(|(op, allocs, bytes)| serde_json::json!({
            "op": op, "allocs": allocs, "bytes": bytes,
        })).collect::<Vec<_>>(),
        "slowcheck_ratio": ratio,
    });
    let text = serde_json::to_string_pretty(&doc).unwrap();
    if let Some(path) = json_path {
        std::fs::write(path, format!("{text}\n")).unwrap_or_else(|e| fail(e.to_string()));
        println!("baseline written to {path}");
    } else {
        println!("{text}");
    }
}

fn usage() -> ! {
    eprintln!("usage: codec-preflight <preflight|census|slowcheck|baseline> [--fixtures DIR] [--json PATH]");
    std::process::exit(2);
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let cmd = args.get(1).cloned().unwrap_or_default();
    let dir = fixture_dir(&args);
    match cmd.as_str() {
        "preflight" => {
            preflight(&dir);
        }
        "census" => {
            let fixtures = load_fixtures(&dir).unwrap_or_else(|e| fail(e));
            census_cmd(&fixtures);
        }
        "slowcheck" => {
            let fixtures = load_fixtures(&dir).unwrap_or_else(|e| fail(e));
            slowcheck(&fixtures);
        }
        "baseline" => {
            let json_path = args
                .iter()
                .position(|a| a == "--json")
                .and_then(|pos| args.get(pos + 1).map(String::as_str));
            baseline(&dir, json_path);
        }
        _ => usage(),
    }
}

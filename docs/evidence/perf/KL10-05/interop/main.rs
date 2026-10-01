//! KL10-05 gzip interop: export Rust-compressed record sections; import foreign
//! gzip sections by splicing them into a v2 batch and decoding with the client.
use bytes::Bytes;
use codec::json1k;
use partitionline::protocol::records::{decode_record_batch, Compression, RecordBatch};
use std::{fs, path::Path};

const COUNTS: [usize; 3] = [1, 16, 256];
const BACKEND: &str = if cfg!(feature = "zlib-rs") { "zlibrs" } else { "miniz" };

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let dir = Path::new(&args[2]);
    fs::create_dir_all(dir).unwrap();
    let hdr = RecordBatch::RECORD_BATCH_OVERHEAD as usize;
    match args[1].as_str() {
        "export" => {
            for n in COUNTS {
                fs::write(dir.join(format!("section-{n}.bin")), json1k::section(n)).unwrap();
                let wire = json1k::wire(&json1k::batch(n, Compression::Gzip));
                fs::write(dir.join(format!("rust-{BACKEND}-{n}.gz")), &wire[hdr..]).unwrap();
            }
            println!("exported with {BACKEND}");
        }
        "import" => {
            let mut fails = 0;
            let mut entries: Vec<_> = fs::read_dir(dir).unwrap().map(|e| e.unwrap().path()).collect();
            entries.sort();
            for p in entries.iter().filter(|p| p.extension().is_some_and(|e| e == "gz")) {
                let name = p.file_stem().unwrap().to_string_lossy().to_string();
                let n: usize = name.rsplit('-').next().unwrap().parse().unwrap();
                let foreign = fs::read(p).unwrap();
                let template = json1k::wire(&json1k::batch(n, Compression::Gzip));
                let mut b = template[..hdr].to_vec();
                b.extend_from_slice(&foreign);
                let batch_len = (b.len() - 12) as i32;
                b[8..12].copy_from_slice(&batch_len.to_be_bytes());
                let crc = crc32c::crc32c(&b[21..]);
                b[17..21].copy_from_slice(&crc.to_be_bytes());
                let mut input = Bytes::from(b);
                let want = json1k::batch(n, Compression::Gzip);
                match decode_record_batch(&mut input) {
                    Ok(got) if got.records() == want.records() && input.is_empty() => {
                        println!("{BACKEND} decodes {name}: ok ({} records)", got.count())
                    }
                    Ok(_) => { fails += 1; println!("{BACKEND} decodes {name}: RECORD MISMATCH") }
                    Err(e) => { fails += 1; println!("{BACKEND} decodes {name}: ERROR {e}") }
                }
            }
            std::process::exit(if fails == 0 { 0 } else { 1 });
        }
        m => panic!("unknown mode {m}"),
    }
}

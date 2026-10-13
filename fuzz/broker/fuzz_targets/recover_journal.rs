#![no_main]

use libfuzzer_sys::fuzz_target;
use partitionline_broker::journal::{Journal, Limits};
use std::io::Write;

fuzz_target!(|data: &[u8]| {
    let bytes = data.get(..data.len().min(64 * 1024)).unwrap_or(data);
    let path = std::env::temp_dir().join(format!("plb-fuzz-journal-{}", std::process::id()));
    let Ok(mut file) = std::fs::File::create(&path) else {
        return;
    };
    if file.write_all(bytes).is_err() {
        let _ = std::fs::remove_file(&path);
        return;
    }
    drop(file);
    if let Ok(limits) = Limits::new(1024, 1 << 20, 1024, 8192) {
        let _ = Journal::open(&path, 0, limits);
    }
    let _ = std::fs::remove_file(&path);
});

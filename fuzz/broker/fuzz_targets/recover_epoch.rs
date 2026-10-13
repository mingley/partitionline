#![no_main]

use libfuzzer_sys::fuzz_target;
use partitionline_broker::journal::Limits as JournalLimits;
use partitionline_broker::recovery::{EpochLog, Limits};
use std::io::Write;

fuzz_target!(|data: &[u8]| {
    let bytes = data.get(..data.len().min(64 * 1024)).unwrap_or(data);
    let path = std::env::temp_dir().join(format!("plb-fuzz-epoch-{}", std::process::id()));
    let Ok(mut file) = std::fs::File::create(&path) else {
        return;
    };
    if file.write_all(bytes).is_err() {
        let _ = std::fs::remove_file(&path);
        return;
    }
    drop(file);
    if let Ok(journal) = JournalLimits::new(1024, 1 << 20, 1024, 8192) {
        if let Ok(limits) = Limits::new(64, journal) {
            let _ = EpochLog::open(&path, limits);
        }
    }
    let _ = std::fs::remove_file(&path);
});

#![no_main]

use libfuzzer_sys::fuzz_target;
use partitionline_broker::journal::Limits as JournalLimits;
use partitionline_broker::recovery::{EpochLog, Limits};

mod recovery_fixture;

fuzz_target!(|data: &[u8]| {
    recovery_fixture::with_file(data, "epoch", |path| {
        let journal = JournalLimits::new(1024, 1 << 20, 1024, 8192)
            .unwrap_or_else(|error| recovery_fixture::failed("journal limits", error));
        let limits = Limits::new(64, journal)
            .unwrap_or_else(|error| recovery_fixture::failed("epoch limits", error));
        let _ = EpochLog::open(path, limits);
    });
});

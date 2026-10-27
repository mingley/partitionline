#![no_main]

use libfuzzer_sys::fuzz_target;
use partitionline_broker::journal::{Journal, Limits};

mod recovery_fixture;

fuzz_target!(|data: &[u8]| {
    recovery_fixture::with_file(data, "journal", |path| {
        let limits = Limits::new(1024, 1 << 20, 1024, 8192)
            .unwrap_or_else(|error| recovery_fixture::failed("journal limits", error));
        let _ = Journal::open(path, 0, limits);
    });
});

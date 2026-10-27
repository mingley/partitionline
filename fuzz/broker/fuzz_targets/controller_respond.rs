#![no_main]

use libfuzzer_sys::fuzz_target;
use partitionline_broker::raft::protocol::{Config, Controller};
use std::cell::RefCell;

thread_local! {
    // libFuzzer invokes this synchronous target on one thread per process.
    static CONTROLLER: RefCell<Controller> = RefCell::new({
        let path = std::env::temp_dir().join(format!("plb-fuzz-controller-{}", std::process::id()));
        let config = Config::new(1, vec![1, 2], "fuzz".to_owned()).unwrap_or_else(|_| {
            std::process::abort();
        });
        Controller::open(&path, config, 0).unwrap_or_else(|_| {
            std::process::abort();
        })
    });
}

fuzz_target!(|data: &[u8]| {
    let bytes = data.get(..data.len().min(8 * 1024)).unwrap_or(data);
    CONTROLLER.with(|controller| {
        let _ = controller.borrow_mut().respond(bytes, 1);
    });
});

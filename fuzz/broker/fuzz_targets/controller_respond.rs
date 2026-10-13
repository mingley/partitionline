#![no_main]

use libfuzzer_sys::fuzz_target;
use partitionline_broker::raft::protocol::{Config, Controller};
use std::sync::{Mutex, OnceLock};

fn controller() -> &'static Mutex<Controller> {
    static CELL: OnceLock<Mutex<Controller>> = OnceLock::new();
    CELL.get_or_init(|| {
        let path = std::env::temp_dir().join(format!("plb-fuzz-controller-{}", std::process::id()));
        let config = Config::new(1, vec![1, 2], "fuzz".to_owned()).unwrap_or_else(|_| {
            std::process::abort();
        });
        let controller = Controller::open(&path, config, 0).unwrap_or_else(|_| {
            std::process::abort();
        });
        Mutex::new(controller)
    })
}

fuzz_target!(|data: &[u8]| {
    let bytes = data.get(..data.len().min(8 * 1024)).unwrap_or(data);
    let Ok(mut controller) = controller().lock() else {
        return;
    };
    let _ = controller.respond(bytes, 1);
});

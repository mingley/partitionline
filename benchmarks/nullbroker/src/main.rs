//! `nullbroker` binary: serve until the deadline, then write the artifact.

use std::path::PathBuf;
use std::time::Duration;

use nullbroker::Config;

fn usage() -> ! {
    eprintln!(
        "usage: nullbroker [--bind ADDR] [--partitions N] [--seconds S] [--artifact PATH]\n\
         defaults: --bind 127.0.0.1:19092 --partitions 6 --seconds 30 \\\n\
         --artifact nullbroker-ceiling.json"
    );
    std::process::exit(2);
}

fn main() {
    let mut config = Config::default();
    let mut args = std::env::args().skip(1);
    while let Some(arg) = args.next() {
        let value = args.next().unwrap_or_else(|| usage());
        match arg.as_str() {
            "--bind" => config.bind = value,
            "--partitions" => {
                config.partitions = value.parse().unwrap_or_else(|_| usage());
            }
            "--seconds" => {
                let secs: u64 = value.parse().unwrap_or_else(|_| usage());
                config.serve_for = Duration::from_secs(secs);
            }
            "--artifact" => config.artifact = PathBuf::from(value),
            _ => usage(),
        }
    }
    if config.partitions <= 0 {
        usage();
    }
    eprintln!(
        "nullbroker: bind={} partitions={} seconds={} artifact={}",
        config.bind,
        config.partitions,
        config.serve_for.as_secs(),
        config.artifact.display()
    );
    match nullbroker::NullBroker::run(&config) {
        Ok(report) => {
            println!(
                "label=client-ceiling accepted_records={} accepted_wire_bytes={} \
                 produce_requests={} validation_failures={}",
                report.accepted_records,
                report.accepted_wire_bytes,
                report.produce_requests,
                report.failures.total()
            );
        }
        Err(e) => {
            eprintln!("nullbroker: {e}");
            std::process::exit(1);
        }
    }
}

//! `nullbroker` binary: serve until the deadline, then write the artifact.

use std::path::PathBuf;
use std::time::Duration;

use nullbroker::Config;

fn usage() -> ! {
    eprintln!(
        "usage: nullbroker [--bind ADDR] [--partitions N] [--seconds S] [--artifact PATH]\n\
         [--fetch-seed S] [--fetch-records N] [--fetch-batch-records N]\n\
         [--fetch-payload-bytes N] [--fetch-headers N] [--fetch-codec none|gzip|snappy|lz4]\n\
         [--fetch-abort-every N]\n\
         defaults: --bind 127.0.0.1:19092 --partitions 6 --seconds 30 \\\n\
         --artifact nullbroker-ceiling.json --fetch-seed 1593835521 \\\n\
         --fetch-records 10000000 --fetch-batch-records 500 \\\n\
         --fetch-payload-bytes 100 --fetch-headers 0 --fetch-codec none \\\n\
         --fetch-abort-every 0"
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
            "--fetch-seed" => {
                config.synth.seed = value.parse().unwrap_or_else(|_| usage());
            }
            "--fetch-records" => {
                config.synth.records_per_partition = value.parse().unwrap_or_else(|_| usage());
            }
            "--fetch-batch-records" => {
                config.synth.records_per_batch = value.parse().unwrap_or_else(|_| usage());
            }
            "--fetch-payload-bytes" => {
                config.synth.payload_bytes = value.parse().unwrap_or_else(|_| usage());
            }
            "--fetch-headers" => {
                config.synth.header_count = value.parse().unwrap_or_else(|_| usage());
            }
            "--fetch-codec" => {
                config.synth.codec = match value.as_str() {
                    "none" => 0,
                    "gzip" => 1,
                    "snappy" => 2,
                    "lz4" => 3,
                    _ => usage(),
                };
            }
            "--fetch-abort-every" => {
                config.synth.abort_every = value.parse().unwrap_or_else(|_| usage());
            }
            _ => usage(),
        }
    }
    if config.partitions <= 0
        || config.synth.records_per_batch == 0
        || config.synth.payload_bytes < 16
    {
        usage();
    }
    eprintln!(
        "nullbroker: bind={} partitions={} seconds={} artifact={} fetch={:?}",
        config.bind,
        config.partitions,
        config.serve_for.as_secs(),
        config.artifact.display(),
        config.synth,
    );
    match nullbroker::NullBroker::run(&config) {
        Ok(report) => {
            println!(
                "label=client-ceiling accepted_records={} accepted_wire_bytes={} \
                 produce_requests={} validation_failures={} fetch_requests={} \
                 fetched_records={} fetched_wire_bytes={}",
                report.accepted_records,
                report.accepted_wire_bytes,
                report.produce_requests,
                report.failures.total(),
                report.fetch_requests,
                report.fetched_records,
                report.fetched_wire_bytes,
            );
        }
        Err(e) => {
            eprintln!("nullbroker: {e}");
            std::process::exit(1);
        }
    }
}

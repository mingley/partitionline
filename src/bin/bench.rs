//! Comparison harness entry point. See BENCH.md.
//!
//! This binary prints a table. Until a local Kafka is up and both librdkafka C
//! and rdkafka 0.39.0 have been run on the same host, it refuses to invent
//! numbers.

fn main() {
    eprintln!("partitionline bench: no published same-hardware results yet.");
    eprintln!("See BENCH.md. Will not print a fake comparison table.");
    std::process::exit(2);
}

# partitionline

Pure-Rust Apache Kafka client. Zero C / FFI / librdkafka. Not a wrapper.

This is a **performance bet**, not a claim that Rust lacks a Kafka client. The
category is crowded. The production Rust client is already
[`rdkafka` 0.39.0](https://crates.io/crates/rdkafka): **34,548,428 downloads**
and **292 reverse dependencies** on crates.io (checked 2026-08-22). That crate
is a librdkafka FFI binding — the same shape as the C++, Python, and Confluent
clients, not a missing-capability hole versus the Java client.

Those rdkafka 0.39.0 numbers are the **baseline we have to beat**, not a reason
this project should not exist. Partitionline is done only when it is faster than
librdkafka on published, honest, same-hardware benches for **throughput and
latency**. Slow-but-safe is not v1.

Other Rust-native clients already exist (`samsa`, `krafka`, `kacrab`). We are a
sixth client. The bet is the hot path, not empty-category marketing.

## What we reuse

Wire types come from [`kafka-protocol` 0.18.0](https://crates.io/crates/kafka-protocol)
(generated from Apache Kafka 4.1.0 schemas). We do not rewrite `ApiKey`, request
bodies, tagged fields, or record-batch magic v2. See [PROTOCOL.md](PROTOCOL.md).

## Status

Early. Produce / Fetch / Metadata / classic consumer groups / admin are the v1
slice. Gaps (SASL, TLS-on-by-default, KIP-848, …) are listed in PROTOCOL.md
instead of being silently skipped.

## How to run

```bash
# library + unit tests (no broker)
cargo test

# local Kafka (KRaft)
docker compose up -d
cargo run --example produce_fetch -- --bootstrap localhost:9092
```

## How to bench

See [BENCH.md](BENCH.md). Comparison is against **librdkafka C** (its own
examples / `rdkafka-sys`) on the same machine, same broker, same topic shape.
`rdkafka` 0.39.0 is the named Rust-side baseline we also have to beat. No
warmup-only numbers. Scripts and raw results will be published. If we cannot
beat C yet, the table will say so with numbers.

```bash
./scripts/bench.sh            # prints the comparison table
```

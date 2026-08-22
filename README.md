# partitionline

Pure-Rust Apache Kafka client. Zero C / FFI / librdkafka. Not a wrapper.

**Not faster than librdkafka.** The first published Lab A produce window
(60 s warmup + 180 s × 3, acks=all, linger=50, 1 KiB) is a **loss**: **46%**
of librdkafka 2.15.0 C (434k vs 949k rec/s). See
[results/lab-a.md](results/lab-a.md). That is the default table. It is not a
win.

This is a **performance bet**, not a claim that Rust has no Kafka client. The
category is crowded. The production Rust client is already
[`rdkafka` 0.39.0](https://crates.io/crates/rdkafka) — crates.io **2026-08-22**:
**34,548,428 downloads**, **292 reverse dependencies**. It depends on
[`rdkafka-sys` 4.10.0+2.12.1](https://crates.io/crates/rdkafka-sys) (vendored
**librdkafka 2.12.1**). That FFI shape is the same as the C++, Python, and
Confluent clients, not a missing-capability hole versus Java.

Those rdkafka 0.39.0 numbers are the **README baseline we have to beat**, not a
reason this project should not exist. The **speed bar** is librdkafka itself
(C `rdkafka_performance`, pin **2.15.0**). Beating the Rust wrapper is not a
win. Partitionline is not done until it is faster on published honest
same-hardware benches for **throughput and latency**. Slow-but-safe is not v1.

Other Rust-native clients already exist (`samsa`, `krafka`, `kacrab`,
`kafkit-client`). We are another client. The bet is the hot path.

## What we reuse

[`kafka-protocol` 0.18.0](https://crates.io/crates/kafka-protocol) with
`default-features = false, features = ["client"]`. We do not rewrite wire
types. See [PROTOCOL.md](PROTOCOL.md) and
[API-AND-PROTOCOL.md](API-AND-PROTOCOL.md).

## Status

Producer first (magic v2, Produce v9–v13, linger/batch, hash/sticky,
InitProducerId). Classic consumer groups and admin next. PLAINTEXT until after
the produce bench. Gaps (TLS/SASL, KIP-848, transactions/EOS, …) are named in
API-AND-PROTOCOL.md.

## How to run

```bash
cargo test
docker compose up -d
cargo run --example produce_fetch -- localhost:9092
```

## How to bench

See [BENCH.md](BENCH.md) (published Lab A: Kafka 4.3.1, linger=50, acks=all,
60 s warmup + 180 s × 3, C librdkafka **2.15.0**). The latest same-window
table is [results/lab-a-0f25201.md](results/lab-a-0f25201.md) (pin
`0f25201`). Earlier windows stay at [results/lab-a.md](results/lab-a.md),
[results/lab-a-pipeline.md](results/lab-a-pipeline.md),
[results/lab-a-083ae1f.md](results/lab-a-083ae1f.md), and
[results/lab-a-cf77216.md](results/lab-a-cf77216.md). Fetch/e2e
unmeasured. Not done. No warmup-only numbers. 10 min × 3 was the original
ask and is not this published window.

```bash
./scripts/bench.sh
```

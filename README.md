# partitionline

Pure-Rust Apache Kafka client. Zero C / FFI / librdkafka. Not a wrapper.

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

See [BENCH.md](BENCH.md) (Lab A: Kafka 4.3.1, linger=50, acks=1, 60 s warmup +
10 min × 3, C librdkafka **2.15.0**). No warmup-only numbers. Scripts and raw
HDR/CSV will be published. A loss is published as a loss.

```bash
./scripts/bench.sh
```

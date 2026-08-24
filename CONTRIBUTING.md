# Contributing

Dual-licensed MIT OR Apache-2.0. A contribution is under that same dual
license unless you state otherwise.

```
cargo fmt
cargo clippy --all-targets --all-features -- -D warnings
cargo test
```

No `rdkafka`, `rdkafka-sys`, `zstd-sys`, `lz4-sys`, or other C Kafka codec
in default features. `unsafe` is forbidden in this crate.

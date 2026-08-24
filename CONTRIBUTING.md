# Contributing

This project is MIT OR Apache-2.0. Patches are under the same licenses unless you say otherwise.

```
cargo fmt
cargo clippy --all-targets --all-features -- -D warnings
cargo test
```

Please do not add librdkafka, or C compression libraries, as default dependencies. This crate forbids `unsafe`.

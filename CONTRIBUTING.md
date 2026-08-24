# Contributing

This project is MIT OR Apache-2.0. Patches are under the same licenses unless you say otherwise.

```
cargo fmt
cargo clippy --all-targets --all-features -- -D warnings
cargo test
```

Please do not add librdkafka, or C compression libraries, as default dependencies. This crate forbids `unsafe`.

MSRV is 1.85. `rcgen` pulls `time`; keep `time` at **0.3.41** in `Cargo.lock` (`0.3.55` needs rustc 1.88).

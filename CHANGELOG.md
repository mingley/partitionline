# Changelog

All notable changes to this project are documented here.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to the 0.x policy in [`docs/RELEASE.md`](docs/RELEASE.md).

## [Unreleased]

### Added

- Civilization adoption plan (`docs/CIVILIZATION.md`) and supporting release,
  API stability, security, and operator docs.
- Real-broker CI smoke workflow and `scripts/ci-broker-smoke.sh`.
- GitHub issue / PR templates for stewardship.
- Adversarial decode smoke tests (`tests/fuzz_decode_smoke.rs`).
- Lab A produce harness (`scripts/lab-a-produce.sh`).
- Optional `tracing` feature (produce/fetch/poll/rejoin/txn spans).
- libFuzzer targets under `fuzz/` and `scripts/ci-fuzz-smoke.sh`.
- Relative produce-ack latency CI gate (`scripts/ci-latency-gate.sh`,
  `docs/latency-baseline.json`).
- Adoption survey issue template, zstd spike doc, README feature matrix.
- `examples/metrics.rs` Prometheus text mode (`FORMAT=prom`).

### Security

- Reject array / tagged-field lengths that exceed remaining buffer bytes so
  malformed broker frames cannot force multi-gigabyte `Vec` allocations.

## [0.1.0] - 2026-09-03

Initial public baseline (git / pre-crates.io).

### Added

- Pure-Rust Kafka client: produce, fetch, classic groups, cooperative-sticky,
  KIP-848 consumer groups, KIP-932 share groups, transactions / EOS, and
  Kafka 3.x/4.x admin APIs.
- TLS via rustls (no OpenSSL); SASL PLAIN, SCRAM-SHA-256/512, OAUTHBEARER,
  OIDC client credentials.
- Compression: gzip, snappy, lz4 (zstd and Kerberos remain out of default
  features; see `docs/gaps.md`).
- Java-shaped builders and rustdoc naming matching Java client calls.
- Examples: produce, consume, group, txn, eos, admin, tls, sasl, share,
  benches, interceptors, and related operator samples.
- Mock protocol / surface tests; design, gaps, and benchmark documentation.

### Notes

- Not a drop-in for `rd_kafka_*` or rust-rdkafka types.
- Defaults that differ from Java: `auto.offset.reset=Earliest`,
  `allow.auto.create.topics=false`, shorter `delivery.timeout.ms` /
  `max.block.ms` (see README).

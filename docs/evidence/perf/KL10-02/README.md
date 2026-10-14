# KL10-02 gateway produce cells

Harness-only revision. The library is unchanged. Suite HOLD remains active.
No performance claim.

## Cells

Registered in `docs/plan/performance-leadership.md` section 4 and
`benchmarks/scenarios.json` (`gateway_produce`):

- `lb-gateway-produce-gzip` — fire-and-forget + timed flush
- `lb-gateway-produce-lz4` — same, lz4 level 9
- `lb-gateway-produce-ack` — per-record `Producer::send` / librdkafka callback

Each cell × `{open-loop-25k, unpaced}` × `{partitionline, rust-rdkafka}` × 5
A-B-B-A interleaved pairs (60 result files per campaign).

## Config gate

The driver refuses to compare runs whose codec, level, acks, max in flight,
linger, batch size or TLS mode differ. Fire-and-forget is never compared with
per-record completion. Throughput is `acknowledged / elapsed`; enqueue rates
are not used. CPU is process rusage (`getrusage` / `wait4`).

## Live blocker

This environment has no Kafka distribution, no Docker, and no pinned
librdkafka 2.15.0 install (`KAFKA_BOOTSTRAP` unset, `GATEWAY_RUST_PEER_BIN`
unset). The campaign therefore writes schema-valid `not_run` results. The card
stays `in_progress` until a harness-only commit is measured with ≥5 interleaved
repetitions on a real broker against the rust-rdkafka peer.

## Section 2 differences (recorded, not gated)

See `gateway::cells::section2_differences`: Linux host rather than the
2026-09-30 Apple M4 Pro laptop; PLAINTEXT accepted when mTLS env is absent;
high watermarks are reconciled; gzip uses the repository default backend;
open-loop and unpaced share one `run_timeout`.

## Validation recorded here

- `cargo +stable test --locked --manifest-path benchmarks/gateway/Cargo.toml` (14 passed)
- `python3 scripts/benchmark-report.py` on the sample `not_run` result
- `cargo tree -p partitionline --edges normal` contains no librdkafka

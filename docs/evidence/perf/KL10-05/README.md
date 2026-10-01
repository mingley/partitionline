# KL10-05 gzip backend measurements

Parent `65d3e89a` (miniz_oxide, flate2 without `runtime_detection`) against
the KL10-06 candidate, as three arms:

| Arm | Build |
|---|---|
| P | parent tree, default features |
| M | candidate, `--no-default-features` (miniz_oxide + `runtime_detection`) |
| Z | candidate, default features (zlib-rs + `runtime_detection`) |

## Files

- `measurements.json`: criterion medians with paired deltas and bootstrap
  CIs (arm64 macOS and x86_64 Linux), iai-callgrind instructions (aarch64
  and x86_64 Linux) and the json1k gzip allocation census (both targets).
- `interop.txt`: 60 decode checks. The JDK's `java.util.zip` and zlib 1.2.12
  with librdkafka's `deflateInit2` settings decode Rust sections from both
  backends. Both backends decode Java and zlib sections at levels default/1/9
  and each other's sections. Sources are in `interop/`. The harness was a
  standalone crate with path dependencies on `benchmarks/codec`
  (`default-features = false`) and the client, a `zlib-rs` feature forwarding
  to both, plus `bytes` and `crc32c`.
- `measure.sh` / `analyze.py`: the A/B driver and summarizer used on both
  hosts.

## Commands

```bash
# per arm; M adds --no-default-features
cargo run --release --locked --manifest-path benchmarks/codec/Cargo.toml --bin json1k-census
cargo bench --locked --manifest-path benchmarks/codec/Cargo.toml --bench iai -- --allow-aslr --output-format=json
cargo bench --locked --manifest-path benchmarks/codec/Cargo.toml --bench json1k_iai -- --allow-aslr --output-format=json
cargo bench --locked --manifest-path benchmarks/codec/Cargo.toml --bench codec --no-run
<codec bench binary> --bench gzip --measurement-time 1 --warm-up-time 0.5 --sample-size 20 --nresamples 10000 --noplot
# five interleaved repetitions: P M Z, then Z M P, ...
REPS=5 bash measure.sh <parent tree> <candidate tree> <out> census iai criterion
python3 analyze.py <out>/crit P,M,Z
```

x86_64 ran the same `measure.sh` on a GitHub-hosted runner from a temporary
branch, run [36866219590](https://github.com/mingley/partitionline/actions/runs/36866219590)
(artifact `kl10-05-x86_64`, 30-day retention). The branch was deleted after
the run.

## Limits

- Local and unsigned; not a Kafka throughput or peer comparison claim.
- arm64 wall clock ran on a shared laptop (1-minute load average 6.6–16.6).
  Interleaving and paired deltas absorb drift. x86_64 ran on a quiet 4-vCPU
  hosted runner with a newer rustc (1.99.0).
- aarch64 instructions came from a Debian container with valgrind 3.19.0. The
  committed `perf-baseline.json` used 3.24.0, so absolute Ir differ by up to
  about 1%; arms were compared within one run.
- The gateway cells (`lb-gateway-produce-gzip`, `lb-gateway-fetch-gzip`) do not
  exist yet (KL10-02, KL10-14). KL10-12 measures them on the new default.

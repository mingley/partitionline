# KL10-14 gateway fetch gzip cell

Harness-only. The `partitionline` library is unchanged. Suite HOLD remains
active. **No performance claim.**

## What ran

Native Apache Kafka 4.1.0 (KRaft, PLAINTEXT `127.0.0.1:9092`, RF=1, 8
partitions, topic `pl-gateway-fetch`) against this tree and the isolated
rust-rdkafka 0.39.0 peer dynamically linked to pinned librdkafka 2.15.0
(`9a94e11452cdeb0a844db44ee5dd01ccbe17d3ab`, SHA256
`a4a43bb95a0ddd16951e81e734deb8e92b3333fd60c91e81a3fe8dfc7aa92ca3`).

The rust-rdkafka peer wrote the gzip json1k backlog once (`COUNT=25000`,
`acks=-1`, linger 100 ms, 1 MiB batches). Both clients then reused those
start offsets unchanged. Every consumed ID/hash was checked against that
KL04-07 produce history (`peer.attempts.jsonl` SHA256
`a2d5975789713a280972d35a42913212d8a8153dfc46c57ffa0ae7538eb62ce5`).

Matched fetch knobs: `fetch.min.bytes=1`, `fetch.wait.max.ms=500`,
`fetch.max.bytes=52428800`, `max.partition.fetch.bytes=1048576`,
`isolation=read_uncommitted`. Five A-B-B-A interleaved repetitions.
Throughput is verified consumed / elapsed. CPU is process rusage
(partitionline: per-run `getrusage` delta; rust-rdkafka: `wait4` of the
child).

10 result files in `results/` all `executed`, high-watermark matched
(25000), histories verified, and each file passed
`python3 scripts/benchmark-report.py`. Medians and bootstrap 95% CIs are
in `summary.json`.

## Limitation versus section 2

`COUNT=25000` is a short seeded backlog (~0.13–0.22 s consume), not an
unpublished dated consume wall. Host is a 4-core Linux Xeon VM, not the
2026-09-30 Apple M4 Pro laptop. Security is PLAINTEXT. gzip uses the
repository default zlib-rs backend. Fetch sizes are the Java/librdkafka
defaults, so the dated partitionline 1,426 MB peak RSS is not reproduced
(median ~52 MB on both clients).

## Config gate

The driver refuses to compare runs whose `fetch.min.bytes`,
`fetch.max.wait.ms`, `fetch.max.bytes`, `max.partition.fetch.bytes`, TLS
mode or isolation differ.

`cargo tree -p partitionline --edges normal` contains no librdkafka.
The rust-rdkafka binary stays in the workspace-excluded peer crate.

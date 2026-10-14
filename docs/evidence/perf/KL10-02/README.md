# KL10-02 gateway produce cells

Harness-only. The `partitionline` library is unchanged. Suite HOLD remains
active. **No performance claim.**

## What ran

Native Apache Kafka 4.1.0 (KRaft, PLAINTEXT `127.0.0.1:9092`, RF=1, 8
partitions) against this tree and the isolated rust-rdkafka 0.39.0 peer
dynamically linked to pinned librdkafka 2.15.0
(`9a94e11452cdeb0a844db44ee5dd01ccbe17d3ab`, SHA256
`a4a43bb95a0ddd16951e81e734deb8e92b3333fd60c91e81a3fe8dfc7aa92ca3`).

Profile knobs: json1k (1024-byte seeded JSON + 8-byte `gw` header),
`acks=-1`, max in flight 1, linger 100 ms, 1 MiB batches. gzip level -1
or lz4 level 9. Five A-B-B-A interleaved repetitions. `COUNT=25000`
(`WARMUP=0`). Throughput is acknowledged/elapsed. CPU is process rusage
(partitionline: per-run `getrusage` delta; rust-rdkafka: `wait4` of the
child).

60 result files in `results/` all `executed`, high-watermark matched,
KL04-07 histories counted, and each file passed
`python3 scripts/benchmark-report.py`. Medians and bootstrap 95% CIs are
in `summary.json`.

## Limitation versus section 2

`COUNT=25000` is a 1.0 s open-loop at 25k records/s, not the dated 20 s /
15 s walls. Host is a 4-core Linux Xeon VM, not the 2026-09-30 Apple M4
Pro laptop. Security is PLAINTEXT (mTLS preferred; not configured).
gzip uses the repository default zlib-rs backend. The rust-rdkafka peer
is the KL04-05 `BaseProducer` (delivery callbacks), not
`FutureProducer::send_result`. High watermarks were reconciled.

## Config gate

The driver refuses to compare runs whose codec, level, acks, max in
flight, linger, batch size or TLS mode differ. Fire-and-forget is never
compared with per-record completion.

`cargo tree -p partitionline --edges normal` contains no librdkafka.
The rust-rdkafka binary stays in the workspace-excluded peer crate.

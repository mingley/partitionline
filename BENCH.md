# Bench plan

Partitionline is not done until it is faster than **librdkafka** on honest
same-hardware benches for **throughput and latency**. This file is the contract
for those benches. No warmup-only numbers. No “looks fast on my laptop”
writeups without the table.

## What we compare

Two opponents, same hardware, same broker, same topic, same payload, same
config knobs:

| Opponent | What it is | Why it is here |
|---|---|---|
| **librdkafka C** | The C library and its own examples (`rdkafka_performance` / equivalent `rdkafka-sys` C path) | The bar. Language wrappers are not a substitute. |
| **`rdkafka` 0.39.0** | The production Rust crate (FFI to that same C library). crates.io 2026-08-22: **34,548,428** downloads, **292** reverse deps | Named Rust-side baseline we also have to beat. FFI overhead is real; beating only the wrapper and losing to C is not victory. |

`samsa`, `krafka`, and `kacrab` are peers in a crowded category. They are not
the pass/fail bar. Optional later columns, never a replacement for the C
column.

## Hardware and software pins

Publish these with every result file. Do not mix pins across rows of a table.

| Pin | v1 default | Notes |
|---|---|---|
| Machine | one host, isolated run | Same CPU, same NUMA, same disk. Record `lscpu`, mem, `uname -a`. |
| Kafka | **Apache Kafka 3.8.1** (KRaft, `apache/kafka:3.8.1`) | One broker unless a later suite says otherwise. |
| Topic | `bench` · **6 partitions** · **RF=1** · `min.insync.replicas=1` | Created fresh per run. No compacted topics. |
| linger | **5 ms** | Same on every client. |
| acks | **1** and **all** (two rows each) | Do not hide acks=0 as a throughput win. |
| batch.size | **1 MiB** | librdkafka default-shaped, not Java’s 16 KiB. |
| compression | **none** and **lz4** | lz4 is the compressed row. zstd is a later row once both sides use a real encoder. |
| Payload | **100 B, 1 KiB, 10 KiB** | Fixed-size, incompressible (or published compressible) bodies. Say which. |
| Message count | enough that steady state is **>10 s** after warmup | See procedure. |
| Clients | partitionline (this repo) · librdkafka C examples · rdkafka 0.39.0 | Same bootstrap, same topic. |

## Metrics (required columns)

Every published table includes all of these. Missing a column means the run is
incomplete.

1. **Producer throughput** — records/s and MiB/s, acks=1 and acks=all.
2. **Produce latency** — p50 / p99 / **p999**, measured at the client when the
   produce response (or delivery report) is received. Not “time to queue.”
3. **Consumer fetch throughput** — records/s and MiB/s on a pre-filled topic
   (produce completed first). Isolation `read_uncommitted`.
4. **End-to-end produce-to-consume latency** — p50 / p99 / p999 from produce
   send timestamp (in payload or header) to consumer decode of that record.

Also record: CPU % of client, CPU % of broker, error count, dropped/timeout
count. A faster client that times out is not a win.

## Procedure (not warmup-only)

1. Start Kafka from `docker-compose.yml`. Wait until metadata is reachable.
2. Create the topic. Delete leftover consumer groups.
3. **Warmup (discarded):** 5 seconds or 200k records, whichever is first.
   Warmup numbers are **not** published as results.
4. **Measured window:** at least 10 seconds of steady state, or 2 million
   records, whichever is later. One window per (payload × acks × compression)
   cell.
5. Repeat the measured window **3 times**. Publish mean and the raw three
   runs. Do not pick the best run.
6. Flush / close clients between cells. Recreate the topic when leftover data
   would change fetch results.

## Scripts and raw results

| Path | Role |
|---|---|
| `scripts/bench.sh` | Drive the matrix, print the comparison table to stdout. |
| `scripts/bench-librdkafka.sh` | Build/run the C librdkafka performance example against the pinned broker. |
| `scripts/bench-rdkafka.sh` | Run the `rdkafka` 0.39.0 Rust-side baseline. |
| `scripts/bench-partitionline.sh` | Run this crate’s harness (`cargo run --release -p partitionline --bin bench`). |
| `results/` | Raw JSON/CSV per run (gitignored until a published snapshot is committed). |

A published snapshot is a directory under `results/published/<date>-<host>/`
containing `machine.txt`, `pins.toml`, `raw/*.json`, and `table.md`. Without
those files, do not claim a win.

## Comparison table (shape)

```
payload   acks  codec   client          prod rec/s   prod MiB/s   p50   p99   p999   fetch rec/s   e2e p99
100B      1     none    librdkafka C    …            …            …     …     …      …             …
100B      1     none    rdkafka 0.39.0  …            …            …     …     …      …             …
100B      1     none    partitionline   …            …            …     …     …      …             …
…
```

Fill this only from measured windows. If partitionline loses a cell, say so
in the table. Do not drop the losing row.

## What this is not

- Not a microbench of encode-only without a broker (useful as a diagnostic,
  not as the bar).
- Not a comparison against a slow wrapper configuration (`acks=0`, linger=0,
  tiny batches) while we use linger=5 and 1 MiB batches.
- Not “we beat rdkafka because FFI is slow” without the C column.

# Bench plan

This is a **performance bet**, not a claim that Rust has no Kafka client. The
category is crowded. The README-facing baseline is
[`rdkafka` 0.39.0](https://crates.io/crates/rdkafka) (crates.io **2026-08-22**:
**34,548,428** downloads, **292** reverse dependencies). That crate depends on
[`rdkafka-sys` 4.10.0+2.12.1](https://crates.io/crates/rdkafka-sys), which
vendors **librdkafka 2.12.1**.

The **speed bar** is librdkafka itself, via its C
[`rdkafka_performance`](https://github.com/confluentinc/librdkafka) example,
pinned to **2.15.0**. Beating the Rust FFI wrapper and losing to C is **not** a
win.

Cite: [rdkafka on crates.io](https://crates.io/crates/rdkafka),
[confluentinc/librdkafka](https://github.com/confluentinc/librdkafka),
[Apache Kafka downloads](https://kafka.apache.org/community/downloads/).

## Must-beat metrics

A published suite that cannot fill every must-beat column is incomplete.

| Metric | How it is measured |
|---|---|
| **Producer throughput** | records/s **and** MiB/s |
| **Produce latency** | p50 / p99 / **p999**, at produce response / delivery report — not time-to-queue |
| **Consumer fetch throughput** | records/s **and** MiB/s on a pre-filled topic |
| **End-to-end produce-to-consume latency** | p50 / p99 from send timestamp in the payload to consumer decode |

Admin (create/delete topic, list offsets) is **informational**. It does not
pass or fail the bar.

## Lab A — first published suite that can fail the bar

One machine. One broker **on that machine** (not a broker in another container
or host). Same linger / batch / compression / acks on both clients.

| Pin | Lab A |
|---|---|
| Broker | **Apache Kafka 4.3.1** ([community downloads](https://kafka.apache.org/community/downloads/)) |
| Topic | `bench` · **6 partitions** · **RF=1** |
| Primary payload | **1 KiB** incompressible |
| Extra columns | **100 B** and **10 KiB** — columns, not substitutes for the 1 KiB row |
| linger.ms | **50** |
| acks | **1** |
| compression | **none** |
| idempotent | **true** |
| C opponent | librdkafka **2.15.0** `rdkafka_performance` |
| Rust-side baseline | rdkafka **0.39.0** (librdkafka **2.12.1** via rdkafka-sys). Reported. Not the win condition. |
| Warmup | **60 s** — discarded |
| Measured | **10 minutes × 3 reps** |
| Results | scripts + raw **HDR / CSV**. If we lose, we publish the loss. |

No warmup-only numbers. No linger=0 vs librdkafka default-batch bait. No
separate-container brokers.

## Procedure

1. Record `lscpu`, memory, `uname -a` into `machine.txt`.
2. Start Kafka **4.3.1** on the same host. Wait until metadata answers.
3. Create `bench` (6 partitions, RF=1). Drop leftover groups.
4. **Warmup 60 s** on both clients. Discard those numbers.
5. **Measured window:** 10 minutes. One window per payload column (1 KiB is
   the bar; 100 B and 10 KiB are extra columns).
6. Repeat the measured window **3 times**. Publish all three raw runs and the
   mean. Do not pick the best.
7. Fetch throughput: produce first (completed), then consume from earliest.
8. E2E: timestamp in the payload; consumer records decode time.

## Scripts and raw results

| Path | Role |
|---|---|
| `scripts/bench.sh` | Drive Lab A, print the comparison table |
| `scripts/bench-librdkafka.sh` | Build/run **librdkafka 2.15.0** `rdkafka_performance` |
| `scripts/bench-rdkafka.sh` | rdkafka **0.39.0** Rust-side baseline (not the win) |
| `scripts/bench-partitionline.sh` | This crate, `--release` |
| `results/published/<date>-<host>/` | `machine.txt`, `pins.toml`, `raw/*.hdr`, `raw/*.csv`, `table.md` |

Without those files, do not claim a win. A loss is published as a loss.

## Comparison table (shape)

```
payload  acks  linger  idem  client                 rec/s   MiB/s   p50   p99   p999  fetch rec/s  e2e p50  e2e p99
1KiB     1     50      true  librdkafka 2.15.0 C    …       …       …     …     …     …            …        …
1KiB     1     50      true  rdkafka 0.39.0         …       …       …     …     …     …            …        …
1KiB     1     50      true  partitionline          …       …       …     …     …     …            …        …
100B     1     50      true  …                      …       …       …     …     …     …            …        …
10KiB    1     50      true  …                      …       …       …     …     …     …            …        …
```

Fill from measured 10-minute windows only.

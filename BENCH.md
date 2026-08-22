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
| acks | **all** |
| compression | **none** |
| idempotent | **true** |
| C opponent | librdkafka **2.15.0** `rdkafka_performance` |
| Rust-side baseline | rdkafka **0.39.0** (librdkafka **2.12.1** via rdkafka-sys). Not run in this published window. Not the win condition. |
| Warmup | **60 s** — discarded |
| Measured | **180 s × 3 reps** |
| Results | scripts + raw **CSV / logs**. If we lose, we publish the loss. This file’s comparison table is the `4727da4` window. |

**10 min × 3 was the original Lab A ask and is not this published window.** Do not compare these 180 s numbers to a 10-minute run.

No warmup-only numbers. No linger=0 vs librdkafka default-batch bait. No
separate-container brokers.

## Procedure

1. Record `lscpu`, memory, `uname -a` into `machine.txt`.
2. Start Kafka **4.3.1** on the same host. Wait until metadata answers.
3. Create `bench` (6 partitions, RF=1). Drop leftover groups.
4. **Warmup 60 s** on both clients. Discard those numbers.
5. **Measured window:** 180 s. One window per payload column (1 KiB is
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
| `scripts/bench-partitionline.sh` | This crate, `--release` |
| `results/published/<date>-<host>/` | `machine.txt`, `pins.toml`, `raw/*.hdr`, `raw/*.csv`, `table.md` |

rdkafka **0.39.0** (Rust FFI baseline) was **not** run in the published
window and has no script here. It is not the win condition.

Without those files, do not claim a win. A loss is published as a loss.

## Comparison table (Lab A produce, 2026-08-22, `4727da4`)

Pin `4727da4c8311880319ecd3ae15d7027bca1272c3` (single-buffer `encode_request`). Produce-only. Window is **60 s warmup + 180 s × 3**. Not a 10-minute run. The 10 s smoke is not this table. Fetch and e2e were not measured. rdkafka 0.39.0 was not run. Not done.

acks=**all**. Latency is produce-ack / delivery-report, **microseconds**. Means of three reps.

| payload | acks | linger | idem | client | rec/s | MiB/s | p50 µs | p99 µs | p999 µs |
|---|---|---|---|---|---:|---:|---:|---:|---:|
| 1 KiB | all | 50 | true | librdkafka 2.15.0 C | 922100 | 900.492 | 99891 | 256687 | 331989 |
| 1 KiB | all | 50 | true | partitionline | 1003075 | 979.565 | 98724 | 160221 | 187797 |

**Win on rec/s, MiB/s, and p50.** partitionline mean rec/s is **109%** of C (1003075 vs 922100). Mean p50 is **1.2%** better (98724 vs 99891 µs). p99 and p999 are better. Not a drop toward the 434k loss.

Locked knobs on both clients: acks=all, linger=50, compression=none, idempotent=true, batch.size=1000000. Raw + three reps: [results/lab-a-4727da4.md](results/lab-a-4727da4.md), [results/published/2026-08-22-cursor-4727da4/](results/published/2026-08-22-cursor-4727da4/).

Earlier same-window tables (do not overwrite): [results/lab-a.md](results/lab-a.md) (first published loss), [results/lab-a-pipeline.md](results/lab-a-pipeline.md) (pipeline re-run), [results/lab-a-083ae1f.md](results/lab-a-083ae1f.md) (transport-only retry pin), [results/lab-a-cf77216.md](results/lab-a-cf77216.md) (acks before next encode), [results/lab-a-0f25201.md](results/lab-a-0f25201.md) (wait-task oneshots).

100 B / 10 KiB columns, fetch rec/s, and e2e p50/p99 are empty. A suite that cannot fill every must-beat column is incomplete.

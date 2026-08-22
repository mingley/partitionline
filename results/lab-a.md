# Lab A produce — 2026-08-22 `cursor` VM

**We lost.** This is a produce-only window. Fetch throughput and e2e latency were not measured. No 100 B / 10 KiB columns.

## Hardware

| | |
|---|---|
| Host | `cursor` (KVM), 2026-08-22 |
| CPU | Intel Xeon, **4 cores**, 1 thread/core |
| Memory | 16 GiB |
| Kernel | Linux 6.12.94+ x86_64 |
| Broker | **Apache Kafka 4.3.1** host process (not Docker), `localhost:9092` |
| Topic | `bench`, **6 partitions**, **RF=1** |

`machine.txt` and `pins.toml`: `results/published/2026-08-22-cursor/`.

## Window

**60 s warmup (discarded) + 180 s × 3 measured.** Not 10 min × 3 — labeled as such. All three reps published; means are arithmetic means of the three, not the best.

## Knobs (same on both clients)

| Knob | Value |
|---|---|
| payload | **1024 B** incompressible |
| linger.ms | **50** (librdkafka example binary defaults to 1000; we set 50) |
| compression | **none** |
| idempotent | **true** |
| batch.size | **1000000** |
| queue.buffering.max.messages / inflight | **100000** |
| acks | **all** |

Lab A’s written pin was acks=1 + idempotent=true. **librdkafka 2.15.0 refuses that pair** (`acks` must be `all` when `enable.idempotence` is true). Both clients used **acks=all** so the knobs match. We did not set linger=0 on C. We did not give partitionline a larger batch.

## Flags

**librdkafka 2.15.0** `examples/rdkafka_performance`:

```
rdkafka_performance -P -t bench -b localhost:9092 -s 1024 -l -a all -z none \
  -X linger.ms=50 -X enable.idempotence=true -X batch.size=1000000 \
  -X queue.buffering.max.messages=100000 -A <rep>.lat
```

SIGINT after 180 s; summary after the queue drained. rec/s from that summary. MiB/s = delivered bytes / seconds / 1024². p50/p99/p999 from `-A` samples (produce → delivery report, microseconds).

**partitionline** `target/release/bench` (`df00ea7` harness):

```
bench --bootstrap localhost:9092 --topic bench --size 1024 \
  --seconds 180 --warmup 60 --linger-ms 50 --inflight 100000
```

`Producer::enqueue` + wait on a 100000-deep delivery queue. Latency is enqueue → Produce ack, not time-to-queue.

## Produce table

Latency columns are **microseconds**.

| payload | acks | linger | idem | client | rec/s | MiB/s | p50 µs | p99 µs | p999 µs |
|---|---|---|---|---|---:|---:|---:|---:|---:|
| 1 KiB | all | 50 | true | librdkafka 2.15.0 C **mean** | 949398 | 927.683 | 97446 | 251577 | 327705 |
| 1 KiB | all | 50 | true | partitionline **mean** | 434481 | 424.298 | 237110 | 449096 | 469742 |
| 1 KiB | all | 50 | true | librdkafka C rep1 | 938901 | 917.436 | 97996 | 269723 | 381306 |
| 1 KiB | all | 50 | true | librdkafka C rep2 | 955278 | 933.417 | 96125 | 244225 | 299560 |
| 1 KiB | all | 50 | true | librdkafka C rep3 | 954016 | 932.196 | 98216 | 240783 | 302249 |
| 1 KiB | all | 50 | true | partitionline rep1 | 435703 | 425.491 | 235737 | 447520 | 470174 |
| 1 KiB | all | 50 | true | partitionline rep2 | 433188 | 423.035 | 236334 | 450800 | 471644 |
| 1 KiB | all | 50 | true | partitionline rep3 | 434553 | 424.368 | 239260 | 448967 | 467407 |

## Outcome

**Loss.** Mean produce throughput is **46%** of librdkafka C (434k vs 949k rec/s). Mean p50 ack latency is **2.4×** worse (237 ms vs 97 ms). p99 and p999 are also worse.

rdkafka 0.39.0 (wrapper) was **not** run. Beating the wrapper is not the bar.

Fetch rec/s and e2e p50/p99: **not measured** in this suite.

Raw: `results/published/2026-08-22-cursor/raw/` (CSV + librdkafka logs + `*.pct`). Per-message `.lat` dumps are local-only (too large to commit).

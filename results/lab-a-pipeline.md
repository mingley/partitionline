# Lab A produce — 2026-08-22 `cursor` VM (pipeline re-run)

**Win on rec/s and MiB/s.** Same locked window as the published loss. Fetch throughput and e2e latency were not measured. No 100 B / 10 KiB columns.

The original loss stays at [lab-a.md](lab-a.md) and [published/2026-08-22-cursor/](published/2026-08-22-cursor/). This file does not replace it.

## Hardware

Same host as the published loss.

| | |
|---|---|
| Host | `cursor` (KVM), 2026-08-22 |
| CPU | Intel Xeon, **4 cores**, 1 thread/core |
| Memory | 16 GiB |
| Kernel | Linux 6.12.94+ x86_64 |
| Broker | **Apache Kafka 4.3.1** host process (not Docker), `localhost:9092` |
| Topic | `bench`, **6 partitions**, **RF=1** |

`machine.txt` and `pins.toml`: `results/published/2026-08-22-cursor-pipeline/`. Harness commit `98f077f`.

## Window

**60 s warmup (discarded) + 180 s × 3 measured.** Not a 10-minute run. The 10 s smoke after the write-order fix is not this table. All three reps published; means are arithmetic means of the three, not the best.

## Knobs (same on both clients)

| Knob | Value |
|---|---|
| payload | **1024 B** incompressible |
| linger.ms | **50** |
| compression | **none** |
| idempotent | **true** |
| batch.size | **1000000** |
| queue.buffering.max.messages / inflight | **100000** |
| acks | **all** |

## Flags

Same as the published loss.

**librdkafka 2.15.0** `examples/rdkafka_performance`:

```
rdkafka_performance -P -t bench -b localhost:9092 -s 1024 -l -a all -z none \
  -X linger.ms=50 -X enable.idempotence=true -X batch.size=1000000 \
  -X queue.buffering.max.messages=100000 -A <rep>.lat
```

SIGINT after 180 s. rec/s from the binary summary. MiB/s = delivered bytes / seconds / 1024². p50/p99/p999 from `-A` samples.

**partitionline** `target/release/bench` (`98f077f` + pipeline commits):

```
bench --bootstrap localhost:9092 --topic bench --size 1024 \
  --seconds 180 --warmup 60 --linger-ms 50 --inflight 100000
```

`Producer::enqueue` + wait on a 100000-deep delivery queue. Latency is enqueue → Produce ack.

## Produce table

Latency columns are **microseconds**.

| payload | acks | linger | idem | client | rec/s | MiB/s | p50 µs | p99 µs | p999 µs |
|---|---|---|---|---|---:|---:|---:|---:|---:|
| 1 KiB | all | 50 | true | librdkafka 2.15.0 C **mean** | 944287 | 922.159 | 99141 | 245744 | 303591 |
| 1 KiB | all | 50 | true | partitionline **mean** | 989840 | 966.640 | 99881 | 157503 | 185944 |
| 1 KiB | all | 50 | true | librdkafka C rep1 | 951445 | 929.150 | 98641 | 244076 | 306733 |
| 1 KiB | all | 50 | true | librdkafka C rep2 | 947434 | 925.232 | 97607 | 243319 | 299072 |
| 1 KiB | all | 50 | true | librdkafka C rep3 | 933983 | 912.095 | 101174 | 249838 | 304969 |
| 1 KiB | all | 50 | true | partitionline rep1 | 1014546 | 990.768 | 98499 | 152441 | 173393 |
| 1 KiB | all | 50 | true | partitionline rep2 | 978997 | 956.052 | 100545 | 158408 | 202826 |
| 1 KiB | all | 50 | true | partitionline rep3 | 975977 | 953.102 | 100598 | 161660 | 181613 |

## Outcome

**Win on rec/s and MiB/s.** Mean produce throughput is **105%** of librdkafka C (989840 vs 944287). Mean p50 is **0.7%** worse (99881 vs 99141 µs). p99 and p999 are better (157503 vs 245744, 185944 vs 303591).

What changed vs the published loss: the produce actor keeps receiving while up to five Produce requests are in flight per connection; `base_sequence` is assigned when the batch is written; writes stay in sequence order.

rdkafka 0.39.0 (wrapper) was **not** run. Beating the wrapper is not the bar.

Fetch rec/s and e2e p50/p99: **not measured** in this suite.

Raw: `results/published/2026-08-22-cursor-pipeline/raw/` (CSV + librdkafka logs + `*.pct`). Per-message `.lat` dumps are local-only (too large to commit).

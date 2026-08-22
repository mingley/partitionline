# Lab A produce — 2026-08-22 `cursor` VM (`083ae1f`)

**Win on rec/s and MiB/s.** Same locked window as the published loss. Fetch throughput and e2e latency were not measured. No 100 B / 10 KiB columns. Not done.

The original loss stays at [lab-a.md](lab-a.md) and [published/2026-08-22-cursor/](published/2026-08-22-cursor/). The earlier pipeline table stays at [lab-a-pipeline.md](lab-a-pipeline.md) and [published/2026-08-22-cursor-pipeline/](published/2026-08-22-cursor-pipeline/). This file does not replace them.

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

`machine.txt` and `pins.toml`: `results/published/2026-08-22-cursor-083ae1f/`. Pin `083ae1fad48507d2beab7ba458c4dde46f769550`.

## Window

**60 s warmup (discarded) + 180 s × 3 measured.** Not a 10-minute run. The 10 s smoke after the write-order fix is not this table. All three reps published; means are arithmetic means of the three, not the best.

Started `2026-08-22T16:59:41Z`, finished `2026-08-22T17:22:00Z` (`BENCH_EXIT:0`).

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

**partitionline** `target/release/bench` (`083ae1fad48507d2beab7ba458c4dde46f769550`):

```
bench --bootstrap localhost:9092 --topic bench --size 1024 \
  --seconds 180 --warmup 60 --linger-ms 50 --inflight 100000
```

`Producer::enqueue` + wait on a 100000-deep delivery queue. Latency is enqueue → Produce ack. Produce retries only on `wait()` transport failure; a ProduceResponse is final.

## Produce table

Latency columns are **microseconds**.

| payload | acks | linger | idem | client | rec/s | MiB/s | p50 µs | p99 µs | p999 µs |
|---|---|---|---|---|---:|---:|---:|---:|---:|
| 1 KiB | all | 50 | true | librdkafka 2.15.0 C **mean** | 957736 | 935.292 | 96136 | 242107 | 304156 |
| 1 KiB | all | 50 | true | partitionline **mean** | 1006625 | 983.033 | 98948 | 156977 | 184353 |
| 1 KiB | all | 50 | true | librdkafka C rep1 | 975327 | 952.473 | 93573 | 234134 | 286869 |
| 1 KiB | all | 50 | true | librdkafka C rep2 | 931503 | 909.674 | 100697 | 253143 | 315489 |
| 1 KiB | all | 50 | true | librdkafka C rep3 | 966378 | 943.730 | 94137 | 239044 | 310110 |
| 1 KiB | all | 50 | true | partitionline rep1 | 1034985 | 1010.728 | 97206 | 152941 | 178008 |
| 1 KiB | all | 50 | true | partitionline rep2 | 994527 | 971.218 | 99772 | 158934 | 191164 |
| 1 KiB | all | 50 | true | partitionline rep3 | 990364 | 967.152 | 99866 | 159055 | 183888 |

## Outcome

**Win on rec/s and MiB/s.** Mean produce throughput is **105%** of librdkafka C (1006625 vs 957736). Mean p50 is **2.9%** worse (98948 vs 96136 µs). p99 and p999 are better (156977 vs 242107, 184353 vs 304156).

Pin: transport-only Produce retries (`083ae1f`). A ProduceResponse is final (46 on retry still success; 45 / NotLeader / InvalidProducerEpoch fail the batch). Fetch/e2e stay unmeasured. Not done.

rdkafka 0.39.0 (wrapper) was **not** run. Beating the wrapper is not the bar.

Raw: `results/published/2026-08-22-cursor-083ae1f/raw/` (CSV + librdkafka logs + `*.pct`). Per-message `.lat` dumps are local-only (too large to commit).

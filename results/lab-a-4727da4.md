# Lab A produce — 2026-08-22 `cursor` VM (`4727da4`)

**Win on rec/s, MiB/s, and p50.** Same locked window as the published loss. Fetch throughput and e2e latency were not measured. No 100 B / 10 KiB columns. Not done.

Prior tables stay where they are and were not edited: [lab-a.md](lab-a.md), [lab-a-pipeline.md](lab-a-pipeline.md), [lab-a-083ae1f.md](lab-a-083ae1f.md), [lab-a-cf77216.md](lab-a-cf77216.md), [lab-a-0f25201.md](lab-a-0f25201.md), and their `published/` dirs.

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

`machine.txt` and `pins.toml`: `results/published/2026-08-22-cursor-4727da4/`. Pin `4727da4c8311880319ecd3ae15d7027bca1272c3`.

## Window

**60 s warmup (discarded) + 180 s × 3 measured.** Not a 10-minute run. The 10 s smoke after the single-buffer encode pin is not this table. All three reps published; means are arithmetic means of the three, not the best.

Started `2026-08-22T18:36:25Z`, finished `2026-08-22T18:58:43Z` (`BENCH_EXIT:0`).

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

SIGINT after 180 s. rec/s from the binary summary. MiB/s = recs / seconds / 1024. p50/p99/p999 from `-A` samples.

**partitionline** `target/release/bench` (`4727da4c8311880319ecd3ae15d7027bca1272c3`):

```
bench --bootstrap localhost:9092 --topic bench --size 1024 \
  --seconds 180 --warmup 60 --linger-ms 50 --inflight 100000
```

`Producer::enqueue` + wait on a 100000-deep delivery queue. Latency is enqueue → Produce ack. Request frames are encoded in one buffer (no second memcpy of the Produce body). Pipeline cap 5 and transport-only retries unchanged.

## Produce table

Latency columns are **microseconds**.

| payload | acks | linger | idem | client | rec/s | MiB/s | p50 µs | p99 µs | p999 µs |
|---|---|---|---|---|---:|---:|---:|---:|---:|
| 1 KiB | all | 50 | true | librdkafka 2.15.0 C **mean** | 922100 | 900.492 | 99891 | 256687 | 331989 |
| 1 KiB | all | 50 | true | partitionline **mean** | 1003075 | 979.565 | 98724 | 160221 | 187797 |
| 1 KiB | all | 50 | true | librdkafka C rep1 | 946912 | 924.722 | 97772 | 244125 | 314699 |
| 1 KiB | all | 50 | true | librdkafka C rep2 | 921122 | 899.535 | 99516 | 256905 | 331600 |
| 1 KiB | all | 50 | true | librdkafka C rep3 | 898267 | 877.217 | 102386 | 269032 | 349667 |
| 1 KiB | all | 50 | true | partitionline rep1 | 1019814 | 995.912 | 97947 | 157070 | 183549 |
| 1 KiB | all | 50 | true | partitionline rep2 | 997646 | 974.263 | 98955 | 160477 | 192020 |
| 1 KiB | all | 50 | true | partitionline rep3 | 991764 | 968.520 | 99269 | 163117 | 187822 |

## Outcome

**Win on rec/s, MiB/s, and p50.** Mean produce throughput is **109%** of librdkafka C (1003075 vs 922100). Mean p50 is **1.2%** better (98724 vs 99891 µs). p99 and p999 are better (160221 vs 256687, 187797 vs 331989). This is not a drop toward the 434k published loss.

C's mean rec/s in this window (922100) is lower than the `0f25201` C mean (938461). partitionline's mean rec/s is 1003075 vs 997401 on `0f25201`. Relative 109% is not a claim that C is unchanged.

Pin: encode Kafka request frames in one buffer (`4727da4`). Fetch/e2e stay unmeasured. Not done.

rdkafka 0.39.0 (wrapper) was **not** run. Beating the wrapper is not the bar.

Raw: `results/published/2026-08-22-cursor-4727da4/raw/` (CSV + librdkafka logs + `*.pct`). Per-message `.lat` dumps are local-only (too large to commit).

# Lab A produce — 2026-08-22 `cursor` VM (`cf77216`)

**Win on rec/s and MiB/s.** Same locked window as the published loss. Fetch throughput and e2e latency were not measured. No 100 B / 10 KiB columns. Not done.

Prior tables stay where they are and were not edited: [lab-a.md](lab-a.md), [lab-a-pipeline.md](lab-a-pipeline.md), [lab-a-083ae1f.md](lab-a-083ae1f.md), and their `published/` dirs.

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

`machine.txt` and `pins.toml`: `results/published/2026-08-22-cursor-cf77216/`. Pin `cf77216ef44aaa732be722322ebdf85721d8deb0`.

## Window

**60 s warmup (discarded) + 180 s × 3 measured.** Not a 10-minute run. The 10 s smoke after the latency pin is not this table. All three reps published; means are arithmetic means of the three, not the best.

Started `2026-08-22T17:32:04Z`, finished `2026-08-22T17:54:21Z` (`BENCH_EXIT:0`).

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

**partitionline** `target/release/bench` (`cf77216ef44aaa732be722322ebdf85721d8deb0`):

```
bench --bootstrap localhost:9092 --topic bench --size 1024 \
  --seconds 180 --warmup 60 --linger-ms 50 --inflight 100000
```

`Producer::enqueue` + wait on a 100000-deep delivery queue. Latency is enqueue → Produce ack. Completions are drained between submits so an ack is not held behind the next batch encode.

## Produce table

Latency columns are **microseconds**.

| payload | acks | linger | idem | client | rec/s | MiB/s | p50 µs | p99 µs | p999 µs |
|---|---|---|---|---|---:|---:|---:|---:|---:|
| 1 KiB | all | 50 | true | librdkafka 2.15.0 C **mean** | 912091 | 890.719 | 101244 | 258856 | 332334 |
| 1 KiB | all | 50 | true | partitionline **mean** | 961110 | 938.584 | 101414 | 161180 | 182777 |
| 1 KiB | all | 50 | true | librdkafka C rep1 | 926805 | 905.088 | 100671 | 253094 | 336933 |
| 1 KiB | all | 50 | true | librdkafka C rep2 | 909414 | 888.105 | 100026 | 256887 | 325376 |
| 1 KiB | all | 50 | true | librdkafka C rep3 | 900055 | 878.964 | 103036 | 266586 | 334694 |
| 1 KiB | all | 50 | true | partitionline rep1 | 958945 | 936.470 | 101681 | 160724 | 182965 |
| 1 KiB | all | 50 | true | partitionline rep2 | 956754 | 934.330 | 101566 | 160872 | 181707 |
| 1 KiB | all | 50 | true | partitionline rep3 | 967630 | 944.951 | 100996 | 161943 | 183659 |

## Outcome

**Win on rec/s and MiB/s.** Mean produce throughput is **105%** of librdkafka C (961110 vs 912091). Mean p50 is **0.2%** worse (101414 vs 101244 µs). p99 and p999 are better (161180 vs 258856, 182777 vs 332334).

Pin: deliver Produce acks before encoding the next in-flight batch (`cf77216`). Transport-only retries unchanged (`083ae1f`). Fetch/e2e stay unmeasured. Not done.

rdkafka 0.39.0 (wrapper) was **not** run. Beating the wrapper is not the bar.

Raw: `results/published/2026-08-22-cursor-cf77216/raw/` (CSV + librdkafka logs + `*.pct`). Per-message `.lat` dumps are local-only (too large to commit).

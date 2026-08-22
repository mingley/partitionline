# Lab A produce — 2026-08-22 `cursor` VM (pipeline re-run)

Window: **60 s warmup (discarded) + 180 s × 3**. Same knobs as the published loss. Not a 10-minute run. The 10 s smoke is not this table.

| payload | acks | linger | idem | client | rec/s | MiB/s | p50 µs | p99 µs | p999 µs |
|---|---|---|---|---|---:|---:|---:|---:|---:|
| 1 KiB | all | 50 | true | librdkafka 2.15.0 C **mean** | 944287 | 922.159 | 99141 | 245744 | 303591 |
| 1 KiB | all | 50 | true | partitionline **mean** | 989840 | 966.640 | 99881 | 157503 | 185944 |

**Win on rec/s and MiB/s.** Mean produce throughput is **105%** of librdkafka C (989840 vs 944287). Mean p50 is **0.7%** worse (99881 vs 99141 µs). p99 and p999 are better.

Previous loss (do not edit): [results/lab-a.md](../../lab-a.md), [2026-08-22-cursor](../2026-08-22-cursor/).

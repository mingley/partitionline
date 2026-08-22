# Lab A produce — 2026-08-22 `cursor` VM (`083ae1f`)

Window: **60 s warmup (discarded) + 180 s × 3**. Same knobs as the published loss. Pin `083ae1fad48507d2beab7ba458c4dde46f769550` (transport-only retries). Not a 10-minute run. The 10 s smoke is not this table.

| payload | acks | linger | idem | client | rec/s | MiB/s | p50 µs | p99 µs | p999 µs |
|---|---|---|---|---|---:|---:|---:|---:|---:|
| 1 KiB | all | 50 | true | librdkafka 2.15.0 C **mean** | 957736 | 935.292 | 96136 | 242107 | 304156 |
| 1 KiB | all | 50 | true | partitionline **mean** | 1006625 | 983.033 | 98948 | 156977 | 184353 |

**Win on rec/s and MiB/s.** Mean produce throughput is **105%** of librdkafka C (1006625 vs 957736). Mean p50 is **2.9%** worse (98948 vs 96136 µs). p99 and p999 are better.

Previous windows (do not edit): [results/lab-a.md](../../lab-a.md), [2026-08-22-cursor](../2026-08-22-cursor/), [results/lab-a-pipeline.md](../../lab-a-pipeline.md), [2026-08-22-cursor-pipeline](../2026-08-22-cursor-pipeline/).

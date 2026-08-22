# Lab A produce — 2026-08-22 `cursor` VM (`0f25201`)

Window: **60 s warmup (discarded) + 180 s × 3**. Same knobs as the published loss. Pin `0f25201d24e6b71b3b62ae1de1ed873039d6fb05` (oneshots in the wait task). Not a 10-minute run. The 10 s smoke is not this table.

| payload | acks | linger | idem | client | rec/s | MiB/s | p50 µs | p99 µs | p999 µs |
|---|---|---|---|---|---:|---:|---:|---:|---:|
| 1 KiB | all | 50 | true | librdkafka 2.15.0 C **mean** | 938461 | 916.469 | 98466 | 249909 | 319086 |
| 1 KiB | all | 50 | true | partitionline **mean** | 997401 | 974.024 | 98869 | 160644 | 191408 |

**Win on rec/s and MiB/s.** Mean produce throughput is **106%** of librdkafka C (997401 vs 938461). Mean p50 is **0.4%** worse (98869 vs 98466 µs). p99 and p999 are better. Not a drop toward the 434k loss.

Previous windows (do not edit): [results/lab-a.md](../../lab-a.md), [2026-08-22-cursor](../2026-08-22-cursor/), [results/lab-a-pipeline.md](../../lab-a-pipeline.md), [2026-08-22-cursor-pipeline](../2026-08-22-cursor-pipeline/), [results/lab-a-083ae1f.md](../../lab-a-083ae1f.md), [2026-08-22-cursor-083ae1f](../2026-08-22-cursor-083ae1f/), [results/lab-a-cf77216.md](../../lab-a-cf77216.md), [2026-08-22-cursor-cf77216](../2026-08-22-cursor-cf77216/).

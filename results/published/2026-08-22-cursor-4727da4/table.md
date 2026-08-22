# Lab A produce — 2026-08-22 `cursor` VM (`4727da4`)

Window: **60 s warmup (discarded) + 180 s × 3**. Same knobs as the published loss. Pin `4727da4c8311880319ecd3ae15d7027bca1272c3` (single-buffer `encode_request`). Not a 10-minute run. The 10 s smoke is not this table.

| payload | acks | linger | idem | client | rec/s | MiB/s | p50 µs | p99 µs | p999 µs |
|---|---|---|---|---|---:|---:|---:|---:|---:|
| 1 KiB | all | 50 | true | librdkafka 2.15.0 C **mean** | 922100 | 900.492 | 99891 | 256687 | 331989 |
| 1 KiB | all | 50 | true | partitionline **mean** | 1003075 | 979.565 | 98724 | 160221 | 187797 |

**Win on rec/s, MiB/s, and p50.** Mean produce throughput is **109%** of librdkafka C (1003075 vs 922100). Mean p50 is **1.2%** better (98724 vs 99891 µs). p99 and p999 are better. Not a drop toward the 434k loss.

Previous windows (do not edit): [results/lab-a.md](../../lab-a.md), [2026-08-22-cursor](../2026-08-22-cursor/), [results/lab-a-pipeline.md](../../lab-a-pipeline.md), [2026-08-22-cursor-pipeline](../2026-08-22-cursor-pipeline/), [results/lab-a-083ae1f.md](../../lab-a-083ae1f.md), [2026-08-22-cursor-083ae1f](../2026-08-22-cursor-083ae1f/), [results/lab-a-cf77216.md](../../lab-a-cf77216.md), [2026-08-22-cursor-cf77216](../2026-08-22-cursor-cf77216/), [results/lab-a-0f25201.md](../../lab-a-0f25201.md), [2026-08-22-cursor-0f25201](../2026-08-22-cursor-0f25201/).

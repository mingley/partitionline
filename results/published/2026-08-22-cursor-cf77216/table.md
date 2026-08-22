# Lab A produce — 2026-08-22 `cursor` VM (`cf77216`)

Window: **60 s warmup (discarded) + 180 s × 3**. Same knobs as the published loss. Pin `cf77216ef44aaa732be722322ebdf85721d8deb0` (deliver acks before next encode). Not a 10-minute run. The 10 s smoke is not this table.

| payload | acks | linger | idem | client | rec/s | MiB/s | p50 µs | p99 µs | p999 µs |
|---|---|---|---|---|---:|---:|---:|---:|---:|
| 1 KiB | all | 50 | true | librdkafka 2.15.0 C **mean** | 912091 | 890.719 | 101244 | 258856 | 332334 |
| 1 KiB | all | 50 | true | partitionline **mean** | 961110 | 938.584 | 101414 | 161180 | 182777 |

**Win on rec/s and MiB/s.** Mean produce throughput is **105%** of librdkafka C (961110 vs 912091). Mean p50 is **0.2%** worse (101414 vs 101244 µs). p99 and p999 are better.

Previous windows (do not edit): [results/lab-a.md](../../lab-a.md), [2026-08-22-cursor](../2026-08-22-cursor/), [results/lab-a-pipeline.md](../../lab-a-pipeline.md), [2026-08-22-cursor-pipeline](../2026-08-22-cursor-pipeline/), [results/lab-a-083ae1f.md](../../lab-a-083ae1f.md), [2026-08-22-cursor-083ae1f](../2026-08-22-cursor-083ae1f/).

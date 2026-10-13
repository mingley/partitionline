# KL09-46 accounting and delivered-position cost

Parent `2cd461d2c3d335f40a1492f4b645f86a363dba07`. No production change.

The named cells call `Consumer::fetch`, not `ConsumerGroup::poll`. `fetch_records` walks each returned record twice (`fetched_bytes`, then `accumulate_fetch_topics`). Group `positions()` is not on this path.

Release runtime `bac765108182c9d307faa555707c83f7a061bab0cba31151b236e8f1421c9c68`, one repetition each:

| Cell | Rounds | Fetched | Consumed | Client CPU |
|---|---:|---:|---:|---:|
| `nb-fetch-bulk` | 1 | 135000 | 20000 | 59.498 ms/round |
| `nb-fetch-capped-paused` | 2000 | 120000 | 2000 | 26.037 µs/poll |

`acct_bench` times the same two-pass accounting over 135000 records with a 100-byte value. Two runs saved 181 µs and 357 µs versus one fused pass: 0.30% and 0.60% of the bulk round. The whole two-pass accounting block was 2.13–2.37 ms (3.6–4.0% of the round); the pass that must remain is most of that. Capped accounting for 2000 one-record polls was 75 µs, 0.14% of cell CPU. A six-partition `positions()` clone scan was 0.5 µs, and these cells do not call it.

Fusing the walks does not remove the per-round topic-metric `HashMap`. Bulk allocations stay 135451 either way. The redundant pass is below 1% of both named cells, so the card is `rejected-no-gain`.

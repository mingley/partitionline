# KL10-17 retained fetch batches

`nb-fetch-gzip-overfetch` on this host (Intel Xeon, 4 cores, Ubuntu 24.04, rustc 1.99.0). Each run consumes 60,000 verified records. The parent is `028dedca` (framed batches kept only up to `buffer_memory`, so most of a response was fetched again). The candidate keeps framed bytes up to `max(buffer_memory, max_bytes)`.

| Arm | Repetitions | Fetched records | Consumed | Ratio | Fetch requests |
|---|---:|---:|---:|---:|---:|
| Parent | 3 | 773,000 | 60,000 | 12.883 | 15 |
| Candidate | 3 | 103,500 | 60,000 | 1.725 | 2 |

Every repetition of an arm produced the same counts, so the fetched/consumed change is -86.6% with no spread. Client CPU per record fell from 5,812 ns on the parent run to 1,379–1,687 ns on the candidate runs. Allocations per record stayed at about 1.04. Record verification reported zero mismatches.

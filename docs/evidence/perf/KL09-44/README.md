# KL09-44 fast leaders return without the slow leader

Parent `f52161d9bfe8ee421d833e9115bacde0cf612134`. Candidate runtime `65f65b86306cbaf1aadc47b5d3560b59bd66bf96d0169bab78cad885312cba51`.

A multi-leader fetch returns after the first response plus a 10 ms collection window. A slower leader stays in flight and is applied on a later fetch. One leader still waits for its own response. Seek, assignment changes, and close abort the parked round.

Five interleaved `nb-fetch-multinode` runs, parent then candidate:

| Pair | Parent fast rec/s | Candidate fast rec/s | Parent p99 µs | Candidate p99 µs |
|---|---:|---:|---:|---:|
| 0 | 54554 | 68704 | 311997 | 238724 |
| 1 | 59152 | 69176 | 284313 | 238617 |
| 2 | 58487 | 61554 | 287006 | 271641 |
| 3 | 58220 | 66612 | 292376 | 251400 |
| 4 | 55818 | 69239 | 306751 | 240247 |

Fast-node records/s paired mean +17.3%, 95% CI [+7.0%, +27.6%]. Delivery p99 paired mean −16.1%, CI [−25.0%, −7.2%]. Allocations 289865 → 266783 (−8.0%, identical on every run). Peak RSS and CPU-ns/record also fell. The broker still counted 399000 fetched records; the client returned before decoding the slow leader.

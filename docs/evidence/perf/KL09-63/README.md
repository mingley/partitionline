# KL09-63 allocator comparison

Benchmark-only features on `benchmarks/runtime`. The `partitionline` crate does not depend on jemalloc or mimalloc.

Five interleaved `nb-fetch-bulk` rounds, system census allocator then jemalloc 0.6.1 then mimalloc 0.1.52. Same `nb-serve`. Intel Xeon family 6 model 207, 4 cores, Linux 6.12.94+, rustc 1.99.0, release profile, frequency unpinned.

| Allocator | Records/s median | CPU-ns/record median | Peak RSS median | Allocations |
| --- | ---: | ---: | ---: | ---: |
| system census | 117643 | 2593 | 64716800 | 135451 |
| jemalloc | 118548 | 2479 | 72753152 | census not installed |
| mimalloc | 125803 | 2177 | 75325440 | census not installed |

Paired versus system, five pairs, t critical 2.776:

- jemalloc records/s mean +3.4%, 95% CI −3.0% to +9.9%
- jemalloc CPU-ns/record mean −5.7%, CI −15.1% to +3.7%
- jemalloc RSS mean +12.3%, CI +11.7% to +12.9%
- mimalloc records/s mean +8.3%, CI +4.5% to +12.1%
- mimalloc CPU-ns/record mean −16.6%, CI −32.8% to −0.4%
- mimalloc RSS mean +16.2%, CI +15.5% to +16.8%

`scripts/benchmark-report.py` accepted the three committed rep0 artifacts. KL09-62 throughput intervals for LTO, native CPU, and PGO included zero, so the recipe does not promote those profiles.

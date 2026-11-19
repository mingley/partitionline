# x86_64 codec instruction counts

Rust 1.100.0 and Valgrind 3.26.0 produced the same Callgrind `Ir` counts
for all ten codec cases on two GitHub-hosted x86_64 runners. The baseline
records the source commit `713c8a3d861a843bc1e6e229eecf53ce5632b3bf`
and the original +1% tolerance.
[Source run](https://github.com/mingley/partitionline/actions/runs/44414668464/job/135191877353)
[Repeat measurement](https://github.com/mingley/partitionline/actions/runs/44412321206/job/135183609203)

The extractor self-test checks that ambiguous results, duplicate case
names and invalid counters are rejected. The gate self-test measures the
clean tree, injects an extra allocation per record into the batch encoder,
and measures again after restoring the source.

| Gate step | `iai_encode/f03` instructions | Result |
| --- | ---: | --- |
| Clean | 779,897 | All ten cases passed |
| Injected allocation | 826,910 | Failed at +6.03% |
| Restored | 779,897 | All ten cases passed |

The self-test requires a measured threshold failure in the target case.
An unrelated build or measurement error fails the self-test.
[Qualification run](https://github.com/mingley/partitionline/actions/runs/44609695835/job/135862682418)

These counts cover the local codec harness. Matched Kafka throughput and
latency comparisons have their own [benchmark contract](../../../benchmark-contract.md).

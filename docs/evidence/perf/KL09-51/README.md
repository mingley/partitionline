# Share acquisition lookup (KL09-51)

The ShareFetch lookup now reuses a range cursor for ordered offsets. It
advances at most four ranges before binary search and uses a full search
when offsets move backward. Range validation, gaps and delivery counts
are unchanged.

The benchmark was committed first at `de7c68d3`. Its production helper
already used binary search; this comparison does not substitute an older
linear-scan baseline. The cursor was committed separately at `56525ec1`.
The final qualified source is `b829dc62`.

Five baseline runs preceded the change. Two cohorts of seven interleaved
A/B pairs compare the same 1,000 short ranges and 5,000 offsets, including
2,000 gaps. Each run excludes 100 warmup iterations and times 10,000
iterations. The final median fell from 11.89 to 3.98 ns per lookup
(66.5%). The paired time-ratio 95% CI is [0.319, 0.347], from 20,000
resamples of complete pairs with seed 951. Both lookup allocation censuses
are zero. These results describe the lookup microbenchmark.

The 103 selected share tests and 330 full-surface tests passed. The same
helper's two tests also pass in the benchmark crate. Strict core and
benchmark Clippy and formatting passed. Repeated, reverse, shuffled,
empty and integer-limit cases check counts against a linear oracle.
Sixteen changed captured results were rejected by the result validator.

A clean-checkout Clippy run found narrowing casts in the new tests and
a missing preexisting zstd fixture. The corrected tests and 29 existing
manifest-matched fixtures are checked in. The fixture suites passed
20 tests; their existing native-only ignored case is recorded separately.
The failed lint log remains in the archive.

`capture/capture.py` records guarded source/ELF inputs, parent-bound
processes, successful waits, raw results and whole-child resources.
`capture/analyze.py` replays the actual results and paired bootstrap.
`capture/validate-result.py` checks fixture facts, clocks, checksums and
allocation counts. `source/` retains both helper/benchmark source trees.
Commands and process receipts retain their original absolute workspace
paths. `SHA256SUMS` covers every other archive file.

All measurements are local and unsigned. Whole-child CPU/RSS includes
setup, oracle and warmup; it is not a measured-phase guardrail. No complete
ShareFetch throughput improvement, production qualification or leadership
result is inferred. No allocation budget was raised.

# DescribeTopicPartitions row checkpoint

The aggregate Admin call currently converts every named topic row in a page
before entering its cooperative merge. A [baseline probe](../admin-dtp-row-poll-20261108/README.md)
measured a long poll for large pages. An isolated candidate yields after
8,192 completed row conversions, carrying the count across pages. It also
yields if the 8,192nd row is the last row of a page.

The candidate has not been applied. It introduces a point at which
cancellation or the existing shared deadline can win before later rows, cursor
checks or page merging. A checkpoint bounds completed row conversions between
its own yields; it does not bound name hashing, one row's partition and
replica clones, allocation, cleanup, synchronous destruction, or the total
time of a poll. `yield_now` does not promise another task will run.

## Correctness controls

The standalone candidate uses production blob
`23cb280eee5e91a64a9a6f58347f707524163d07` and permanent test blob
`5451103503eabc359f5404849851ad11f5388cac`, in
[commit d7603b6](https://github.com/mingley/partitionline/commit/d7603b690dc3f5ae6cfb4135633dc2eb11901171).
[Run 42481729183](https://github.com/mingley/partitionline/actions/runs/42481729183)
passed 4/4 focused row controls and 32/32 Admin pagination tests in each of
the default and all-feature profiles. It also passed stable Rust formatting
and strict all-target, all-feature Clippy. The row tests manually poll the
actual helper at 8,191, 8,192 and 8,193 rows, check the final-row boundary and
a cross-page 8,191-plus-one budget, and cover partition-heavy, error and
unnamed rows. A helper test supplies an already expired deadline when checking
unnamed-row cleanup.

### Public checkpoint observation and correction

The first public probe used only a marker immediately before the new await.
Its standalone positive tests passed in
[run 42481828052](https://github.com/mingley/partitionline/actions/runs/42481828052).
But the corresponding combined-source probe
[run 42482306075](https://github.com/mingley/partitionline/actions/runs/42482306075)
failed its negative gate in both profiles: after the row yield was replaced
with an immediately ready future, the cancellation test still passed.
A later pending operation could satisfy the one-marker test. The original
positive results therefore do not by themselves establish cancellation at the
row yield.

The corrected probe marks both before and after the await. It accepts a pending
poll only while the before marker is set and the after marker is not. In
[standalone run 42482710384](https://github.com/mingley/partitionline/actions/runs/42482710384),
both default and all-feature profiles passed 2/2 public controls. Each profile's
no-yield mutation then failed 0/1 with
`row checkpoint resumed before observation: 2`, as intended.

For each positive control, a current-thread mock peer supplied 4,096 rows on
each of two pages, with a third cursor available. The test drove the public
call to the marked cross-page checkpoint after the second response, then
either dropped it or stopped polling until its original three-second deadline
had expired. The deadline case returned `Timeout`; neither returned a partial
list. The peer then verified a raw request on the same socket, with no third
pagination request. The timeout uses real time and the observer adds
test-only work; these controls do not establish a hard deadline or bounded
destructor cost. The observer source blob is
`f4ac80ad0ca9686fe1f86871efb373c465b5ef92`, test blob
`3856ea977ab2af578a025e497807a57227596a59`, from
[commit 88962c6](https://github.com/mingley/partitionline/commit/88962c6002f7fa539b9de885bdd64d79d2dff2f8).
It is separate from the production candidate.

A snapshot combined with a separate requested-name validation draft also
passed its [exact-source gate 42482784330](https://github.com/mingley/partitionline/actions/runs/42482784330):
4/4 row controls, 29/29 requested-name controls and 61/61 pagination tests in
each profile, including a `Send` assertion on the public future; formatting
and strict Clippy passed. Its
[corrected observer run 42482670005](https://github.com/mingley/partitionline/actions/runs/42482670005)
passed the two checkpoint controls and a mismatch-scan cancellation control in
each profile. The no-yield mutation failed with the before/after-marker
assertion in both profiles. This snapshot uses production blob
`95d95498f07faccba3f1f95b2cab0a20f1d9eaf5` and test blob
`fc16bda39bac493941389dc4d9bda51542f1ff41`; it is not a final integration
candidate because the requested-name validation is being revised.

In the combined draft the conversion, mismatch-tail scan and cleanup have
separate counters. For example, 8,191 converted rows followed by an
unrequested row, an 8,191-row named tail and cleanup of 8,191 converted rows
can visit roughly 24,573 rows before any of those counters reaches 8,192. The
new checkpoint does not bound that whole phase.

## Paired public mock timing

[Run 42481897914](https://github.com/mingley/partitionline/actions/runs/42481897914)
passed 260 calls using the same instrumented public test fixture and one
runner. It alternated baseline/candidate order, built in separate target
directories, checked distinct binary hashes, and verified every returned
name, topic error, empty partition list and omitted authorization flag.
[All samples](standalone-paired.csv) retain their execution order.

The table gives the median of the longest individual public-future poll
starting after the peer completed its response write. There are 21 pairs for
1 and 32 rows, 5 pairs for 8,191 through 100,000, and 3 pairs for 500,000.
Each pair used separate processes.

| Rows | Row kind | Baseline median | Checkpoint median |
| ---: | --- | ---: | ---: |
| 1 | empty | 0.020 ms | 0.022 ms |
| 1 | error | 0.019 ms | 0.022 ms |
| 32 | empty | 0.039 ms | 0.043 ms |
| 32 | error | 0.037 ms | 0.040 ms |
| 8,191 | empty | 4.181 ms | 4.234 ms |
| 8,192 | empty | 2.749 ms | 2.206 ms |
| 8,192 | error | 2.784 ms | 2.230 ms |
| 8,193 | empty | 2.869 ms | 2.316 ms |
| 100,000 | empty | 8.822 ms | 8.764 ms |
| 100,000 | error | 8.825 ms | 8.835 ms |
| 500,000 | empty | 62.600 ms | 65.535 ms |
| 500,000 | error | 63.422 ms | 64.115 ms |

The longest observed poll at 500,000 rows remained about 62–66 ms in both
arms. This measurement includes public-call work such as reads, decoding,
conversion, merging and destruction, and does not identify which one caused
the maximum. Total mocked-call medians at 32 rows were 215.2→205.9 µs for
empty rows and 184.9→182.3 µs for errors; they include peer work and varied
between samples. These are not Kafka throughput or production latency
measurements. No general performance improvement is established.

The baseline used Admin source blob
`4d4212057984e47201b2bb1fe383ac2ae5a55562` and the checkpoint used
`23cb280eee5e91a64a9a6f58347f707524163d07`. Both used public fixture blob
`6944285477b86a8cad7b2e65ef3b55dcebd64f95` and the same lockfile. On
GitHub-hosted Ubuntu 24.04, the machine reported AMD EPYC 7763, stable Rust
1.99.1 (`58cf8f9c3`), LLVM 23.1.1 and `RUSTFLAGS=unset`; `cpu.max` was
unavailable. Release used thin LTO and one codegen unit. Binary SHA-256s were
`fb00b56fb4f960b94fa1f4f4ade7a94cbb6c1f6f86046528c92b0e38639d725d`
and `4faf0eb3e226863d1410e5294ead85445fed811b36b7c8f6ee50df9902704543`;
the binaries were not retained. The run artifact contains the build and raw
output for 30 days.

The name-validation-only versus combined-snapshot comparison is a separate
run. Any revised requested-name implementation must be merged and qualified
again against its own exact source.

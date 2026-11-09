# Indexed Fetch vectored-read experiment

Status: **not adopted**. The experiment reduced elapsed time for several
multi-entry Fetch workloads, but Windows file-backed testing failed before a
Fetch and strict formatting and lint checks remain incomplete. The candidate
remains on an isolated branch.

## Change under test

The candidate tries to read an indexed journal entry's header and payload in
one vectored operation when the payload is at most 8 KiB. It uses the existing
exclusive mutable file cursor, position tracking, per-entry cancellation point,
header checks, payload CRC, destination rollback and poisoning. Larger
payloads use the original header and payload reads. There is no unsafe code.

The small-payload path initializes the destination bytes before reading them;
the original `read_to_end` path did not zero-fill them first. One vectored I/O
error can be reported before a corrupt header is identified, whereas separate
header and payload reads can encounter those events in a different order. The
cached index, not the on-disk header, bounds the speculative reservation. If
that reservation fails, the candidate returns to the header-first path before
reporting allocation failure. These differences are part of the experiment.

## Results

Both runs used stable Rust 1.99.1 and GitHub-hosted x86-64 Linux machines,
with four reported CPUs and each benchmark process pinned to CPUs 0 and 1.
Each value below is the median of 12 paired changes in mean elapsed time per
request; positive means less time with the candidate. The range includes every
pair, including outliers. It is not a confidence interval or an individual
request latency range.

| Value bytes | Entries per Fetch | EPYC 9V45 median | 9V45 range | EPYC 7763 median | 7763 range |
| ---: | ---: | ---: | ---: | ---: | ---: |
| 8 | 1 | 1.90% | -10.92% to 3.20% | 2.44% | 0.46% to 3.60% |
| 8 | 16 | 20.50% | 18.86% to 22.30% | 16.80% | 15.80% to 17.80% |
| 8 | 128 | 34.07% | -9.08% to 36.21% | 29.19% | 28.27% to 30.59% |
| 4,096 | 1 | 0.83% | -191.20% to 5.29% | 3.01% | 1.72% to 5.04% |
| 4,096 | 16 | 13.79% | 12.25% to 16.63% | 11.18% | 9.65% to 16.38% |
| 4,096 | 128 | 19.06% | -92.76% to 24.96% | 16.41% | 15.33% to 16.79% |
| 16,384 | 1 | -0.45% | -83.30% to 1.83% | -0.23% | -1.26% to 5.02% |
| 16,384 | 16 | 0.31% | -1.59% to 2.80% | -0.17% | -6.87% to 4.26% |
| 16,384 | 128 | 0.02% | -8.30% to 3.04% | -0.20% | -1.32% to 1.91% |

The 9V45 run had a candidate process with large adverse observations in
several workloads, including pair 11 for both 128-entry workloads. Its cause is
unknown and no samples were removed. The 16,384-byte values use the original large-payload read strategy; their ranges cross zero. These measurements do not establish
a global ranking or performance against another Kafka implementation.

Raw pairs are in [EPYC 9V45 samples](samples-9v45.tsv) and
[EPYC 7763 samples](samples-7763.tsv). The
[9V45 run](https://github.com/mingley/partitionline/actions/runs/42665748248)
and [7763 run](https://github.com/mingley/partitionline/actions/runs/42666434903)
include environment information and complete job logs.

## Workload and controls

The [workload source](fetch_vectored_bench.rs) builds an ordinary Kafka
Fetch v6 request and uses the broker's real Router with one journal-backed
partition. It seeds 128 distinct, synchronized entries, each containing one
valid record batch, before timing. A timed request reads from offset zero;
the 1-, 16- and 128-entry cases cap the response at the corresponding whole
batch boundary. The sizes in the table are record value sizes, not complete
batch sizes.

Each process checks the complete response bytes against independently
constructed expected bytes before and after each timed case, warms the case
with eight requests, and then times 1,000, 300 or 100 sequential requests
for the 1-, 16- or 128-entry case, respectively. Each timed response also has
its length checked. The request clone and Router response are inside the timed
interval; seeding, warmup and byte comparisons are outside it. Baseline and
candidate use the same seeded files, built with separate target directories and
alternating order in every pair. The second runner used the same source and
harness.

The files are warm in the operating system cache. This test does not measure
TCP or network throughput, concurrent producers or clients, cold storage,
tail latency, memory consumption, crash recovery, or performance against a
different Kafka implementation.

On Linux, the four focused indexed-read controls passed, including partial
vectored reads, interrupted reads, I/O-error ordering, destination rollback,
and an actual file payload CRC failure. The Apache read goldens, capped
first-batch control and transport deadline cancellation control also passed.
The candidate's formatting preview showed four rustfmt changes; strict
formatting and all-feature lint checks were not completed.

On Windows, three in-memory indexed-read controls passed, but the real-file
control failed with OS error 5, PermissionDenied. A test-only diagnostic in the
temporary checkout showed that scratch-directory creation succeeded and
`Journal::open` returned the error before append or opening the second writer.
The underlying system call was not identified. No permissions were changed.
The Windows file-backed behavior remains unqualified.

## Source and failed runs

The baseline is commit
[`133d241`](https://github.com/mingley/partitionline/tree/133d2413217cee4130bca2267e4e36b7225ff86f),
with `journal.rs` Git blob `88e0794ff175bb7760d32d9be7383bea89be87b2` and
SHA-256 `480d55ee2a8b3f4a7924c5b5d18f30b814d59e36a69d8cb563ce6bb6b5b7d6c2`.
The candidate source and its unit tests are in isolated commit
[`daa941e`](https://github.com/mingley/partitionline/tree/daa941ef1d85d8840534035532482dbfbce91eed),
`journal.rs` Git blob `d9f85041b4c7768d21bbbd557a52a68e6b9c79dd`,
SHA-256 `fbe30f7909fe8728acba9cf9bd58f16c9b842002328b366bca49bc66721a8a43`.
The second run used commit
[`b721f15`](https://github.com/mingley/partitionline/tree/b721f15bbe955961941f0facbf732d4251c94a0d),
which retains the same candidate source and workload and adds only temporary
Windows diagnostic instrumentation. The workload Git blob is
`410223c74db582180e6ba1dc166845a8fcae6e04`, SHA-256
`f4965510959194c320892087803ee12f8f246d49f3a1e1191ae6bdcb1626bb63`;
the broker lockfile Git blob is `19652acd96b929402b1e4a9394a33d12a7e69449`,
SHA-256 `42eaddf993ab4dfbe99e5af9d7c1a7cd858776a214eedbd6112de6a146933a2c`.

Earlier runs produced no performance samples:

- [42663450445](https://github.com/mingley/partitionline/actions/runs/42663450445)
  and [42663512649](https://github.com/mingley/partitionline/actions/runs/42663512649)
  passed the focused Linux controls but lacked the baseline commit in the
  shallow checkout.
- [42664165991](https://github.com/mingley/partitionline/actions/runs/42664165991)
  passed the focused Linux controls, then nested the baseline checkout inside
  the wrong Cargo workspace. Its Windows checkout also failed on long paths.
- [42665101017](https://github.com/mingley/partitionline/actions/runs/42665101017)
  omitted the root source target from the sparse checkout, so Cargo could not
  parse the root manifest on either platform.
- [42665509147](https://github.com/mingley/partitionline/actions/runs/42665509147)
  included the root source but omitted root examples named by that manifest;
  Cargo again stopped before tests on both platforms.

The two measured runs copied both broker checkouts outside the root workspace.
The Windows failure above is separate from these harness failures.

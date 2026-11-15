# Admin cursor-cycle qualification

This note covers the bounded cursor history added to the
`DescribeTopicPartitions` aggregate. It is evidence for this change, not a
claim that the client is production ready or faster than other Kafka clients.

## Sources

- Baseline commit: `dfe179d7066e2c1afecb88df5ba76d2920308c46`.
  `src/admin.rs`: `8c2f94353e04c2925b1bfaf8478285190dd317d7`;
  `src/admin/pagination_tests.rs`: `a69d5cceff56a7d1c52f321b5766d695423871aa`.
- Candidate commit: `0f6a5d259627b37accdad851a187c2f04c678d58`.
  `src/admin.rs`: `e6c9899f3c645023782a9a33531665e3ef4b741c`;
  `src/admin/pagination_tests.rs`: `d497709daffaa1cae3e6cd328aa0848a41c91567`.
- Both use `Cargo.toml` blob `4a90b08ab3fb886676231310b1f7132e04e2cfbb`
  and `Cargo.lock` blob `96e364d34121fe90183f87f4f49fd92b883bc447`.
  The candidate commit contains no qualification workflows.
- Hosted runs used stable Rust 1.99.1 on x86_64. Each run linked below records
  the source pins and compiler used.

## Behavior and limits

The aggregate remembers the 16 most recent cursor tuples and one cycle
checkpoint. It borrows names from the requested inputs and does not retain
broker-supplied cursor strings in that history. Repeats found in this state
return `Error::Protocol`; `Error::is_retriable` returns false. Older,
nonperiodic repeats outside that state may pass. Detecting a longer periodic
cycle can take more pages, and the deadline may expire first.

Pages need not form a snapshot. A repeat can therefore be transient: a later
page might have progressed. In the matched scripted test, the baseline reads
A → B → A → C → end and succeeds, whereas the candidate returns
`Error::Protocol` after the third page. Raw page requests can continue from
the repeated cursor and preserve the broker response. The permanent
`dtp_transient_repeat_is_rejected_but_raw_pages_can_progress` test also
checks that continuation on the same connection. This rejection is a client
consistency policy, not proof that every repeat is an infinite loop.

The history has a fixed number of entries; it does not bound the requested
names, result, decoder, or total request memory. Its lookup performs up to 17
synchronous name comparisons per page. Existing tests cover a deadline and
cancellation while a later page is in flight, including cancellation after
the recent window fills. They do not establish a hard poll-time bound for
very long names during synchronous comparison.

## Focused checks

[Run 42664674123](https://github.com/mingley/partitionline/actions/runs/42664674123)
checked the exact candidate blobs: formatting, all-target/all-feature Clippy
with warnings denied, warning-denied Rustdoc, and 83 pagination tests in each
of the default and all-features profiles passed. These tests include cursor
periods beyond the recent window, unique cursors, name lookup parity,
deadlines, cancellation, socket reuse, and raw page behavior.

[Run 42663358078](https://github.com/mingley/partitionline/actions/runs/42663358078)
passed a smoke test against digest-pinned Apache Kafka 4.1.2 in both feature
profiles. For a seven-partition topic, raw and aggregate descriptions matched
over four pages with both authorization settings. The production source
blob was the same; this smoke used the prior pagination-test blob
`6c47b3cb7cb2f1d99700263a099dfcb84f6a2724`, before the permanent transient
test was added.

The transient baseline/candidate script passed in
[run 42663358059](https://github.com/mingley/partitionline/actions/runs/42663358059).
The separate `admin_response_reservation` integration tests passed in both
profiles in [run 42664255407](https://github.com/mingley/partitionline/actions/runs/42664255407).

## Broad Admin suites

The focused results do not make the broad Admin suite green. In
[run 42666552963](https://github.com/mingley/partitionline/actions/runs/42666552963),
baseline and candidate were built in separate target directories on the same
runner. Source and binary hashes were recorded, a candidate-only test checked
which binary was used, and each binary hash was checked before and after its
suite. All four suites failed:

| Feature profile | Arm | Passed | Failed | Failed 80 ms assertions |
| --- | --- | ---: | ---: | --- |
| Default | Baseline | 677 | 2 | Grouping named results: 82.72 ms; indexing coordinators: 83.19 ms |
| Default | Candidate | 688 | 2 | Grouping named results: 83.07 ms; indexing coordinators: 87.80 ms |
| All features | Baseline | 678 | 1 | Indexing coordinators: 83.31 ms |
| All features | Candidate | 689 | 1 | Indexing coordinators: 85.20 ms |

These are observed wall times on one hosted runner, not a determination of
why the assertions failed. The candidate has additional pagination tests,
so the suite counts differ.

An earlier shared-target comparison in
[run 42664255407](https://github.com/mingley/partitionline/actions/runs/42664255407)
is invalid for broad-suite arm attribution: Cargo reused the first arm's
test binary for the second arm. The separately run integration jobs in that
workflow are not affected. In
[run 42662878406](https://github.com/mingley/partitionline/actions/runs/42662878406),
strict checks passed but the behavior jobs stopped before testing because
the workflow inspected a parent commit in a shallow checkout. The corrected
focused runs above supersede that behavior gate.

## Release loopback measurements

[Run 42663358059](https://github.com/mingley/partitionline/actions/runs/42663358059)
built the two production source blobs in separate release target directories,
using the same benchmark fixture
`f6f531aed278e3df5c216cd8404830bddcd64161` in both arms. The binaries had
different SHA-256 hashes: baseline
`688fc9032509c56eb5ee5e2272ed13a2ffa06b754e03669863bd6e8499b20337`;
candidate `7dbadec89cf03cd3fb2ec57a9fa001f2fefa486d11afdd00be63bf0be24664e7`.
The benchmark used the same production source blob as the final candidate;
its replaced test fixture was not the final pagination-test blob.

With a one-name slice iterator, the measured aggregate future grew from
3,272 to 3,704 bytes, an increase of 432 bytes. The raw future remained
2,904 bytes. These are layouts for this Rust release build and iterator
type, not fixed sizes for every target or caller.

On one four-vCPU AMD EPYC 9V45 hosted runner, the diagnostic pinned both
arms to one CPU and ran 12 alternating pairs per shape. The table reports
candidate elapsed time divided by baseline elapsed time; lower is faster.
Ranges are observed pairs, not confidence intervals.

| Loopback shape | Median ratio | Observed range |
| --- | ---: | ---: |
| One page | 1.0039 | 0.8018–1.0107 |
| Two pages | 1.0046 | 0.9934–1.2060 |
| 16 pages, short name | 1.0026 | 0.9882–1.0249 |
| 16 pages, 248-byte names | 1.0077 | 0.9915–1.0163 |
| 32 names, three pages | 1.0020 | 0.9381–1.2265 |

These loopback results have outliers and include mock server work. They do
not measure broker throughput, tail latency, or other Kafka implementations.

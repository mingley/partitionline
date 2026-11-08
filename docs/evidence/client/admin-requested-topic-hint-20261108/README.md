# Requested topic validation with cooperative row conversion

Before this change, aggregate `describe_topics` could return a topic or next
cursor that was not requested. The [initial candidate][prior] rejected these
responses but added substantial overhead to the 32- and 256-name error-row
fixtures. The applied change uses an ordered-name fast path; qualification
results are recorded below.

Before an index is built, the candidate compares a returned name to the
current and next requested names. It compares the strings, so the hint
cannot accept an unrelated name. A different order, an omitted name, or
duplicates may cause a fallback. For more than eight inputs, the first
mismatch builds one randomized `HashSet<&str>` from caller names; smaller
lists use direct membership checks. Index construction yields after each
8,192 inputs, and later checks use the index. This does not assume Kafka or
another peer returns names in the request order. The empty aggregate remains a no-op, Metadata fallback
keeps its existing projection, and the raw one-page API is unchanged.

For a response page, an immediately repeated cursor is checked first.
Subject to a timeout, any null name wins over an unrequested named row;
that row wins over an unrequested next cursor; those checks precede
same-page topic-ID conflict detection. An unrequested name or cursor
returns a fixed-text client-side `Error::Protocol`, not a broker error.
Checking cursor membership is a client consistency policy; the protocol
schema does not promise this behavior for every peer.

The [candidate][candidate] is based on main
`464d33d5afe668d46274972cd3baa4e870159bf8`, which contains the accepted
cooperative topic-row checkpoint. The candidate preserves that checkpoint:
the counter carries across pages and yields after 8,192 successfully
converted rows. Deferred name indexing can yield before the current row is
counted. The candidate source Git blob is
`3125c234259c87d1bc4a89d0cd0734d124529bea` (SHA-256
`ebeaea8620424abe1f05278ae1af7a97c902a4e20bc705a1a4dbcaf0dfec4984`)
and its tests blob is `f18e87db6a6eab6d33a05288fe1d39817b9c2388`
(SHA-256
`8343a51719ba69207d431aae29f41f1d231be0667517c0341cc51a91c99d1e8c`).
The main baseline source blob is
`23cb280eee5e91a64a9a6f58347f707524163d07`.

## Checks

The [combined gate][gate] has passed rustfmt, strict all-target/all-feature
Clippy, all-feature Rustdoc with warnings denied, and nine client doctests.
Kafka 4.1.2 smoke tests passed in each feature profile: the broker CLI
confirmed topics with four and two partitions, and the aggregate fixture
checked the topics, duplicate requests and seven missing-name errors.
The smoke uses image
`apache/kafka:4.1.2@sha256:5cc2a2fd93fa2687b44015eee04fb2c3edd9e526bd64bf8bec5ff1e268772e0e`
and fixture Git blob `5163fe81c4cbfd2462ffa86ba406851ff4a92fec`.
It does not reproduce unsolicited or null response rows.

The final candidate's 66 focused pagination controls passed in each
profile, including a compile-time `Send` assertion for the public future
when passed a slice iterator of `&str`. Arbitrary non-`Send` iterators
are outside that assertion. Both full Admin suites **failed** on 80 ms
wall-time assertions:

| Profile | Passed / failed | Failed test (measured time) |
| --- | --- | --- |
| Default | 670 / 3 | `decoding_many_described_group_members_does_not_hold_the_caller` (83.491501 ms); `grouping_many_named_results_does_not_hold_the_caller` (84.293590 ms); `indexing_many_coordinators_does_not_hold_the_caller` (127.861731 ms) |
| All features | 672 / 1 | `indexing_many_coordinators_does_not_hold_the_caller` (110.190508 ms) |

All 20 independently restored mutations produced their expected test
failure in each profile. The exact-suite failures are not passing results,
and their causes have not been established.

A separate [instrumented row-checkpoint control][observer] passed in both
profiles: two public cross-page cancellation/timeout checks and one
mismatch-scan cancellation check passed in each. Removing the row yield
made the targeted test fail with
`row checkpoint resumed before observation: 2` in each profile, as
intended. The observer-only source
blob `c6042c72b8067f9c49f2dca6f31f0e907aaabc98` adds test-only
before/after markers at the row yield; its tests blob is
`8038f78dd5672ea7db82cb0f3b73d4f98df9691b`. The markers add work at the
test checkpoint. These current-thread mock checks exercise that
suspension and socket behavior; they are not an exact-source run or a
wall-clock bound.

The [earlier hint-only gate][hint-gate] passed 60 focused tests in each
profile, strict checks and six restored mutants per profile. The earlier
[combined draft][draft-gate] passed 66 focused tests in each profile and
strict checks using the candidate production blob but an earlier test blob
`a715d649c26bc99d5407d9a1fc29e9229aa0a471`. These do not replace the
final-source gate. A [preceding unformatted draft][draft-fmt] failed
rustfmt on three test-line wraps; a hint draft also failed rustfmt before
its corrected check. An isolated branch update was rejected as
non-fast-forward and then updated with a lease. The [four-arm prototype
timing][prototype-perf] is separate from the combined-source benchmark.

## Restored mutation controls

The [candidate gate][gate] restores the source before each mutation, then
requires Cargo exit 101 and the named test to fail. Both feature profiles
completed all 20 cases below. The original source and test SHA-256 values
matched again after each job. These are negative controls, not passing test
runs on mutated code.

| Mutation | Expected failing test |
| --- | --- |
| `row_guard` | `dtp_requested_names_reject_unsolicited_success` |
| `current_hint` | `dtp_requested_names_reject_unsolicited_success` |
| `next_hint` | `dtp_requested_names_hint_next_rejects_unrequested_name` |
| `indexed_fail_open` | `dtp_requested_names_existing_index_rejects_later_unsolicited_topic` |
| `cap_duplicates` | `dtp_requested_names_deferred_index_has_capped_initial_capacity` |
| `cursor_guard` | `dtp_requested_names_reject_unsolicited_cursor` |
| `null_priority` | `dtp_requested_names_null_precedes_unsolicited_row_and_cursor` |
| `null_after_checkpoint` | `dtp_requested_names_null_after_row_checkpoint_precedes_unrequested` |
| `repeated` | `dtp_requested_names_repeated_cursor_precedes_unsolicited_rows` |
| `index_population` | `dtp_requested_names_large_input_accepts_members_and_duplicates` |
| `index_yield` | `dtp_requested_names_index_yields_after_progress` |
| `index_cancel_page` | `dtp_requested_names_cancel_deferred_index_after_partial_page_keeps_socket_usable` |
| `index_cancel_cursor` | `dtp_requested_names_cancel_cursor_deferred_index_keeps_socket_usable` |
| `scan_yield` | `dtp_requested_names_scan_yields_after_progress` |
| `scan_cancel` | `dtp_requested_names_cancel_during_scan_keeps_socket_usable` |
| `cleanup` | `dtp_requested_names_rejection_cleans_and_checks_deadline` |
| `cursor_id` | `dtp_requested_names_cursor_precedes_same_page_id_conflict` |
| `row_cursor` | `dtp_requested_names_unsolicited_row_precedes_unsolicited_cursor` |
| `row_yield` | `dtp_conversion_yields_on_the_exact_row_boundary` |
| `row_budget_reset` | `dtp_conversion_carries_the_row_budget_across_pages` |

The no-index-yield mutation after a prior page failed with
`deferred index never paused at its checkpoint`; the cursor variant failed
with `deferred index checkpoint completed without an observable pause`.
The row-yield mutation failed with
`row conversion checkpoint did not suspend at 8,192 rows`; the row-budget
mutation failed with `row conversion checkpoint did not carry across pages`.
These messages distinguish the intended suspension checks from unrelated
pending work.

## Matched Admin suites

The [matched suite comparison][broad] ran both source versions on a
shared AMD EPYC 9V45 x86_64 runner with Rust 1.99.1 and LLVM 23.1.1.
It used the same compatibility test fixture in both arms, Git blob
`33d917cba15f1c0c5ed5646c277b0890a725938c` (SHA-256
`f11ee5fa7fa095e684e1ba0f7971b4fa2ea37c5ad5618251b93756fa3fbd2827`).
This fixture does not contain the new name-validation or row-helper
controls; those ran separately at the candidate test pin above. The
matched workflow **failed**. In execution order:

| Order | Profile and arm | Passed / failed | Failed test (measured time) |
| --- | --- | --- | --- |
| 1 | Default baseline | 633 / 2 | `grouping_many_named_results_does_not_hold_the_caller` (100.990434 ms); `indexing_many_coordinators_does_not_hold_the_caller` (81.886451 ms) |
| 2 | Default candidate | 632 / 3 | `decoding_many_consumer_group_members_does_not_hold_the_caller` (99.864319 ms); `grouping_many_named_results_does_not_hold_the_caller` (96.591971 ms); `indexing_many_coordinators_does_not_hold_the_caller` (85.627277 ms) |
| 3 | All features candidate | 632 / 3 | `flattening_many_list_reassignments_does_not_hold_the_caller` (86.812287 ms); `grouping_many_named_results_does_not_hold_the_caller` (89.779487 ms); `indexing_many_coordinators_does_not_hold_the_caller` (82.633298 ms) |
| 4 | All features baseline | 631 / 4 | `decoding_many_listed_groups_does_not_hold_the_caller` (85.454642 ms); `flattening_many_list_reassignments_does_not_hold_the_caller` (91.471043 ms); `grouping_many_named_results_does_not_hold_the_caller` (82.687971 ms); `indexing_many_coordinators_does_not_hold_the_caller` (93.198846 ms) |

The same runner saw wall-time failures in both source arms. This does not
establish their cause or change the exact candidate-suite failures.

## Combined-source timing

The [matched release comparison][perf] used one pinned CPU on a shared
AMD EPYC 7763 runner, Rust 1.99.1 and LLVM 23.1.1. It compared the exact
candidate source blob `3125c234259c87d1bc4a89d0cd0734d124529bea` against
main's row-checkpoint source `23cb280eee5e91a64a9a6f58347f707524163d07`.
Both arms used the same benchmark fixture Git blob
`dae07e157e9b6687cca044b366d6aa4f5b673ab0` (SHA-256
`4ca812ef0d29cd3df23de0126ffb844384ef063d3ec205907f99dceb12be9883`).
The baseline and candidate release test binaries had SHA-256
`9ff1df7c503db0bca2b5a83d68d87abeb21a15361b6fd0af58741681650f6e8f`
and `b667177f5f2cb72fbb623e7a018b6d49f5d1988b4b8a1ad0a33e2d927d4b4884`.
The [CSV](combined-timings.csv) preserves all 336 observations, with 12
alternating pairs per shape.

| Shape | Baseline / candidate median (ms) | Median paired ratio | Paired ratio range | Candidate slower |
| --- | --- | --- | --- | --- |
| One-name, one successful page | 123.223 / 124.010 | 1.0107 | 0.9887–1.0183 | 9/12 |
| One-name, 16 successful pages | 122.538 / 123.506 | 1.0053 | 0.9980–1.0180 | 9/12 |
| 4 ordered error rows | 63.185 / 63.568 | 0.9990 | 0.9507–1.0168 | 5/12 |
| 32 ordered error rows | 44.306 / 44.510 | 1.0044 | 0.9756–1.0234 | 9/12 |
| 256 ordered error rows | 28.244 / 28.560 | 1.0112 | 1.0011–1.0173 | 12/12 |
| 32 reversed error rows | 44.304 / 46.486 | 1.0510 | 1.0303–1.0706 | 12/12 |
| 256 reversed error rows | 28.166 / 33.403 | 1.1874 | 1.1686–1.2143 | 12/12 |
| 32 names, three successful pages | 30.861 / 31.359 | 1.0137 | 1.0069–1.0215 | 12/12 |
| 32 lexical error rows | 44.375 / 46.760 | 1.0557 | 1.0381–1.1913 | 12/12 |
| 256 lexical error rows | 28.294 / 33.715 | 1.1900 | 1.1700–1.2088 | 12/12 |
| 32 ordered successful rows | 52.956 / 53.726 | 1.0116 | 0.9850–1.0217 | 10/12 |
| 256 ordered successful rows | 40.811 / 41.766 | 1.0201 | 0.9863–1.0356 | 11/12 |
| 32 lexical successful rows | 53.793 / 55.799 | 1.0376 | 1.0175–1.0475 | 12/12 |
| 256 lexical successful rows | 40.862 / 45.628 | 1.1207 | 1.1070–1.1322 | 12/12 |

A ratio above one means the candidate was slower for that pair. The ratio
is the median of paired ratios, not the ratio of the two median elapsed
times. One-page, 16-page, four-name, 32-name, 256-name and three-page
shapes use 4,000, 250, 2,000, 1,000, 200 and 250 reads per sample
respectively. Successful rows have one partition; error rows carry
`UNKNOWN_TOPIC_OR_PARTITION`. Ordered and reversed response rows follow
or reverse the numeric input order; lexical rows use lexicographic response
order for numerically ordered input. The latter is relevant to Kafka-style
sorting but does not model every broker or request.

The candidate's paired median was about 19% slower for 256 lexical error
rows and 12% slower for 256 lexical successful rows on this runner; both
were slower in all 12 pairs. These loopback fixtures include mock-peer work
and are not a comparison with another Kafka client. They do not establish a
global performance ranking or a bound on all workloads.

## Prototype timing

The [four-arm comparison][prototype-perf] used one pinned CPU on a shared
AMD EPYC 9V74 runner with Rust 1.99.1 and LLVM 23.1.1. It compared baseline
source `4d4212057984e47201b2bb1fe383ac2ae5a55562`, initial eager validation
`eb5a967b89e94a8a3d1a7598919accdfde96f20f`, capped eager validation
`31373ec93f6e22365383b58dc458b4f3ffc0bf19`, and the pre-merge hint
prototype `ee7ebc1c097780d3c39bd49a81c89a1af254d65a`. All arms used the
same benchmark fixture `5d7b8522b58257ffd6fcedacef93e5be6d71b3c5`
(SHA-256 `e6521ed66d8da77c580d0fcba3ee5224a84f3720933aa7778ce7bfb30a96eac8`).
The [CSV](prototype-timings.csv) retains all 576 elapsed-time samples in
measurement order. There were 12 alternating pairs for every shape and
variant. Each cell is median paired candidate/baseline ratio, range, and
number of slower pairs:

| Shape | Initial | Capped | Hint prototype |
| --- | --- | --- | --- |
| Single successful page | 1.0049 (0.9997–1.0081; 11/12) | 1.0006 (0.9896–1.0095; 7/12) | 1.0033 (0.9954–1.0140; 9/12) |
| 16 successful pages | 1.0018 (0.9952–1.0108; 7/12) | 1.0031 (0.9974–1.0115; 8/12) | 1.0018 (0.9883–1.0060; 9/12) |
| 4 ordered error rows | 1.0039 (0.9587–1.0114; 10/12) | 1.0006 (0.9953–1.0090; 7/12) | 1.0033 (0.9962–1.0117; 9/12) |
| 32 ordered error rows | 1.0821 (1.0766–1.0863; 12/12) | 1.0547 (1.0462–1.0592; 12/12) | 1.0059 (1.0004–1.0125; 12/12) |
| 256 ordered error rows | 1.1817 (1.1737–1.2009; 12/12) | 1.1702 (1.1572–1.1813; 12/12) | 1.0199 (1.0114–1.0566; 12/12) |
| 32 reversed error rows | 1.0912 (1.0847–1.0961; 12/12) | 1.0519 (1.0416–1.0617; 12/12) | 1.0401 (1.0327–1.0508; 12/12) |
| 256 reversed error rows | 1.1767 (1.1665–1.1866; 12/12) | 1.1671 (1.1524–1.1836; 12/12) | 1.1854 (1.1216–1.1916; 12/12) |
| 32 names, three successful pages | 1.0387 (1.0334–1.0551; 12/12) | 1.0252 (0.9712–1.0316; 11/12) | 1.0095 (0.9978–1.0218; 11/12) |

The single-page case ran 4,000 reads; the 16-page case ran 250 reads; the
4-, 32- and 256-name cases ran 2,000, 1,000 and 200 reads respectively;
the three-page case ran 250 reads. Error-row shapes contain
`UNKNOWN_TOPIC_OR_PARTITION`; the page shapes include successful
partitions. The hint reduced cost on the ordered fixtures, but the reversed
256-name case remained materially slower on this host. These are
loopback fixtures, not comparisons with other Kafka clients. They do not
qualify the merged source; [its matched timing][perf] is reported separately
above.

## Separate sorted-input experiment

The [sorted-input experiment][sorted-perf] compared the combined candidate
source `3125c234259c87d1bc4a89d0cd0734d124529bea` with prototype source
`ed27320d26193080eb38d9bf863d4c5737a7e9ca`. On the first hint miss for
more than eight inputs, the prototype scans for nondecreasing request names,
yielding every 8,192 comparisons. It uses binary search for sorted names
and the existing randomized set for unsorted names. Duplicates are allowed.

This prototype is **not applied or qualified for integration**. A smoke
run passed eight generated fixture shapes, but it has not completed
permanent cancellation, deadline, mutation, live Kafka, broad-suite or
exact-source documentation checks. The source scan's yield is not a
wall-time bound. Results do not qualify the candidate above.

The shared AMD EPYC 7763 runner used one pinned CPU, Rust 1.99.1 and LLVM
23.1.1. Both arms used benchmark fixture Git blob
`807700b69d6023338bc0ce18e54b0fe19180eda5` (SHA-256
`c7c72a250ddca1e858b06530ccb019450e03007bb4fb613493f645de497989a1`).
Baseline and prototype release test binaries had SHA-256
`4039c07895d0b6653f75bf8e2b884d6327bcf5eeaf76a88dffe475ea21a1603b`
and `8fd95c866dfb941cfae457aba78ac4b99fa28e807667afc25234fb0f439b7234`.
The [CSV](sorted-prototype-timings.csv) contains all 240 observations;
there are 12 alternating pairs per shape.

| Shape | Hint / sorted prototype median (ms) | Median paired ratio | Paired ratio range | Prototype slower |
| --- | --- | --- | --- | --- |
| One-name, one successful page | 123.302 / 123.243 | 0.9929 | 0.9787–1.0154 | 3/12 |
| One-name, 16 successful pages | 122.277 / 121.856 | 0.9975 | 0.9649–1.0406 | 4/12 |
| 32 sorted names, reversed rows | 46.858 / 46.319 | 0.9888 | 0.9465–0.9969 | 0/12 |
| 256 sorted names, reversed rows | 33.155 / 32.825 | 0.9882 | 0.9434–1.0021 | 1/12 |
| 256 sorted names, ordered rows | 28.775 / 28.771 | 0.9991 | 0.9905–1.1158 | 5/12 |
| 256 sorted names, duplicates, reversed rows | 40.679 / 39.382 | 0.9650 | 0.9617–0.9889 | 0/12 |
| 256 sorted long names, reversed rows | 27.912 / 25.515 | 0.9120 | 0.9033–0.9181 | 0/12 |
| 256 unsorted names, reversed rows | 33.272 / 33.005 | 0.9945 | 0.9448–1.0096 | 2/12 |
| 256 unsorted names, ordered rows | 28.474 / 28.194 | 0.9982 | 0.9867–1.0611 | 5/12 |
| 256 unsorted long names, reversed rows | 27.786 / 27.376 | 0.9865 | 0.9687–1.0086 | 2/12 |

Ratios are prototype divided by the hint candidate. The 32-name cases run
1,000 reads; 256-name cases run 200, except long-name cases run 80.
Single- and 16-page controls run 4,000 and 250 reads. Sorted names use
zero-padded numeric suffixes; unsorted names use unpadded numeric suffixes.
The duplicate fixture sends each of 256 sorted names twice and receives
one error row per distinct name. Long names share a prefix and are about
200 bytes. These synthetic loopback results show a modest gain on short
sorted names and a larger gain on that long-name fixture; they do not
remove the separate numeric-input/lexical-response regression above or
establish a global ranking.

## Limits

The initial fallback reservation is based on at most 32 input entries even
when there are fewer distinct names; the table's actual capacity depends on
the standard-library implementation. Subsequent growth depends on distinct
caller names. The set borrows caller strings and never stores peer names.
This is a bounded initial reservation, not an absolute memory bound: input
collection, response decoding, result accumulation and the fallback table
still consume memory.

The row checkpoint, index build and unsolicited-row remainder scan have
separate counters. They do not bound the work of hashing long names, table
growth, allocation, nested partition cloning or synchronous destruction in
one poll. Cleanup uses per-vector and per-topic chunks, not a global
partition-drop or wall-time bound. The decoded response is still owned
while validation and cleanup run.

For tests, before/after thread-local counters are updated only at each
8,192-input index yield. The page and cursor cancellation controls wait
until the former exceeds the latter, then cancel and reuse the same
socket. They verify that suspension point, not an elapsed-time guarantee.
The temporary row observer and its overhead are reported separately.

[prior]: ../admin-requested-topic-validation-20261108/README.md
[candidate]: https://github.com/mingley/partitionline/commit/542602573ceb8712360cbab6b8381e856129b481
[gate]: https://github.com/mingley/partitionline/actions/runs/42485196572
[observer]: https://github.com/mingley/partitionline/actions/runs/42485346758
[hint-gate]: https://github.com/mingley/partitionline/actions/runs/42484132205
[draft-gate]: https://github.com/mingley/partitionline/actions/runs/42484745286
[draft-fmt]: https://github.com/mingley/partitionline/actions/runs/42484469639
[prototype-perf]: https://github.com/mingley/partitionline/actions/runs/42483323372
[sorted-perf]: https://github.com/mingley/partitionline/actions/runs/42485820704
[broad]: https://github.com/mingley/partitionline/actions/runs/42485301649
[perf]: https://github.com/mingley/partitionline/actions/runs/42484971478

# Requested topic names in paginated Admin responses

This note tracks an isolated candidate for the name-based
`describe_topics` aggregate. It is separate from the [earlier topic
identity change][prior]. A faster revision is under investigation after the initial timing
comparison.

Kafka 4.1.0's [request handler][handler] builds a set from a nonempty
explicit topic list, deduplicates names, and emits success and topic-error
rows from that set. Its [metadata cache][cache] produces next cursors from
the filtered requested-topic iterator. The [request schema][request]
describes the list as the topics to fetch; it does not separately state a
cross-implementation cursor-membership rule. The candidate would enforce requested-name membership for response rows
and next cursors as a client consistency policy, returning a fixed-text
client-side `Error::Protocol` rather than fabricating a broker error. The
raw single-page API would not enforce this policy.

An immediately repeated cursor is checked first. Subject to timeout, any
null name in a page takes precedence over an unrequested named row; the
unrequested row takes precedence over an unrequested next cursor, and
those checks precede page ID-conflict detection. A null name remains the
existing `Unsupported` result. The scan after the first unsolicited row
continues only to find a possible null name, with cooperative yields.

Empty aggregate input remains a no-op. An empty raw request still asks the
broker for all topics. Duplicate DTP inputs remain on the wire and the
aggregate merges repeated named results. The Metadata fallback remains
separate: it projects requested names in request order, preserves duplicate
requests, and ignores unrelated Metadata response rows. An explicitly
requested empty string is distinct from a null response name.

## Candidate and baseline

The [candidate][candidate] is based on main
`536d5e938aa2ad123471fe6446b0ba406dabe8cd`. Its source Git blob is
`eb5a967b89e94a8a3d1a7598919accdfde96f20f` (SHA-256
`b1e05bb2d56ce1df2a0ecb4ebb1e4947fc561a974b113d6c7e4fed9096da056b`)
and its test blob is `492a3393a4626d96aa7a45e69b876294df64996a`
(SHA-256
`31d12d04e8bab072a5b6b9e6d3cfd056c31a0c886f0d8a63e2ceb6672a536dd2`).

The [baseline controls][baseline] reproduced four failures in each profile:
the old aggregate returned `Ok` for an unsolicited success, an
unsolicited topic-error row, an unsolicited row after a valid partial page,
and an unrequested empty name. Each negative test run exited 101; the
workflow expected those failures. Three positive controls passed in each
profile: duplicate DTP inputs with a requested error, a raw empty request,
and Metadata v4 fallback with duplicate requests and an extra response
topic. A [separate cursor baseline][cursor-baseline] did not produce the
expected client-side error for an unsolicited next cursor in either profile;
the raw cursor control passed.

The [first baseline attempt][baseline-setup] used Metadata v1 and failed its
positive fallback setup because this client cannot represent
`allowAutoTopicCreation=false` below v4. That attempt is not a passing
fallback control. The [first candidate draft][draft1] passed 49 pagination
controls in each profile but strict Clippy rejected an ignored
`oneshot::send` result in a test. The corrected [second draft][draft2]
passed 53 pagination controls in both profiles and strict Clippy, including
cancellation after scan progress and same-socket reuse. The final test blob
adds a negative control for the >8-name index path.

## Initial candidate-source checks

The [candidate gate][final] used workflow commit
`a5226e76d15709312624da964e11b596d21e917b` with the source and test blobs
above. The 54 pagination controls passed in each profile; rustfmt, strict
all-target/all-feature Clippy, all-feature client Rustdoc with
`RUSTDOCFLAGS="-D warnings"`, and nine client doctests passed.

The Kafka 4.1.2 live smoke passed two tests in each profile, using image
`apache/kafka:4.1.2@sha256:5cc2a2fd93fa2687b44015eee04fb2c3edd9e526bd64bf8bec5ff1e268772e0e`
and fixture blob `5163fe81c4cbfd2462ffa86ba406851ff4a92fec`. The
broker's command-line tool independently confirmed the two created
topics' four and two partitions. The test covered pagination and ten
requested entries comprising both topics, a duplicate, and seven missing
names. This normal-broker smoke does not reproduce the scripted unsolicited
or null responses.

Both candidate-source full Admin suites **failed**, with four 80 ms wall-time
assertions in each profile. These tests exercise other methods; their causes are not established, and
the full suites did not pass.

| Profile | Passed / failed | Failed test (measured time) |
| --- | --- | --- |
| Default | 657 / 4 | `decoding_many_consumer_group_members_does_not_hold_the_caller` (98.622396 ms); `decoding_many_share_group_members_does_not_hold_the_caller` (103.339267 ms); `grouping_many_replica_dirs_does_not_hold_the_caller` (105.723794 ms); `indexing_many_coordinators_does_not_hold_the_caller` (121.705861 ms) |
| All features | 657 / 4 | `decoding_many_delegation_tokens_does_not_hold_the_caller` (80.368156 ms); `grouping_many_named_results_does_not_hold_the_caller` (95.433503 ms); `indexing_many_coordinators_does_not_hold_the_caller` (186.831326 ms); `indexing_many_listed_offsets_does_not_hold_the_caller` (85.166865 ms) |

## Matched full-suite comparison

The [matched comparison][broad] ran four suites on one shared x86_64 AMD
EPYC 9V45 runner, in the order shown below, with Rust 1.99.1. The workflow
returned **failure**. Baseline source blob was
`4d4212057984e47201b2bb1fe383ac2ae5a55562` and candidate source was
`eb5a967b89e94a8a3d1a7598919accdfde96f20f`. Both arms used the identical
compatibility test fixture `33d917cba15f1c0c5ed5646c277b0890a725938c`
(SHA-256 `f11ee5fa7fa095e684e1ba0f7971b4fa2ea37c5ad5618251b93756fa3fbd2827`);
it updates the old empty-name control to request an empty string and does
not include the new permanent tests. The manifests and lockfile were
identical as recorded in the run.

| Order | Profile and arm | Passed / failed | Failed test (measured time) |
| --- | --- | --- | --- |
| 1 | Default baseline | 633 / 2 | `flattening_many_list_reassignments_does_not_hold_the_caller` (84.143988 ms); `grouping_many_named_results_does_not_hold_the_caller` (81.229789 ms) |
| 2 | Default candidate | 633 / 2 | `grouping_many_named_results_does_not_hold_the_caller` (80.073845 ms); `indexing_many_coordinators_does_not_hold_the_caller` (88.175662 ms) |
| 3 | All features candidate | 635 / 0 | None |
| 4 | All features baseline | 632 / 3 | `decoding_many_share_group_members_does_not_hold_the_caller` (92.625747 ms); `grouping_many_named_results_does_not_hold_the_caller` (93.780793 ms); `indexing_many_coordinators_does_not_hold_the_caller` (144.736546 ms) |

The matching fixture shows wall-time failures in both arms on the same
runner; it does not establish their cause or turn the separately failed
exact-test-blob suites into passes. An [earlier matched-control
attempt][broad-setup] failed before tests: fixture comparison reported a
difference at byte 37789, line 1050. The corrected run copied the same
fixture to both arms.

## Matched release timing

The initial [paired comparison][perf] used a shared x86_64 AMD EPYC 7763,
one pinned CPU, Rust 1.99.1 and LLVM 23.1.1. Each of five shapes had one
warmup and 12 alternating baseline/candidate pairs. Both arms used the
identical benchmark test fixture Git blob
`ff54c6adee06ac202cc80da58535a6bba1803b74` (SHA-256
`95760d3b42c942bb3a1f7f41e59086586a8e9893b83d2d969410ce6ef397e3fd`).
Production blobs are the baseline and candidate recorded above. Release
test binaries had SHA-256
`187ce61c8d5ed3a26b7129db6779ba7406484f3e07dab2c20f4f794afcd78bf6`
and `7f53f21c3a915e1bb98b15f068156826d4bbbf6615d1b23f5d81f8433729ae4c`.
The [CSV](paired-timings.csv) preserves all 120 elapsed-time samples in
measurement order.

| Shape | Baseline / candidate median time (ms) | Median paired ratio | Paired ratio range | Candidate slower |
| --- | --- | --- | --- | --- |
| 4,000 one-name, one-page successful reads | 124.146 / 125.400 | 1.0089 | 1.0024–1.3838 | 12 / 12 |
| 250 one-name, 16-page successful reads | 122.748 / 124.246 | 1.0112 | 0.9953–1.0272 | 11 / 12 |
| 2,000 four-name, one-page error-row reads | 64.000 / 64.267 | 1.0123 | 0.9717–1.0294 | 8 / 12 |
| 1,000 32-name, one-page error-row reads | 44.340 / 48.362 | 1.0909 | 1.0788–1.1091 | 12 / 12 |
| 200 256-name, one-page error-row reads | 27.843 / 34.078 | 1.2221 | 1.1946–1.2312 | 12 / 12 |

A ratio above one is slower for the candidate. The 32- and 256-name
regressions are material for these particular loopback fixtures; this is
not a comparison with another Kafka client or a claim about all workloads.
The first two shapes include partitions; the remaining shapes return only
`UNKNOWN_TOPIC_OR_PARTITION` rows. A lower-cost revision is being
investigated and must be measured and qualified separately.

A standalone probe on the same target measured a 16-byte `&str`. Inserting
1, 9, 32, 256 and 1,024 unique names into an initially empty
`HashSet<&str>` gave final capacities 3, 14, 56, 448 and 1,792, with 1, 3,
5, 8 and 10 observed capacity-growth events respectively. For 1,024
identical inputs it observed one unique value, capacity 3, and one growth.
This is a capacity probe, not an allocator-byte measurement; the candidate
uses the set only when the input has more than eight entries.

The [candidate gate][final] also ran 12 separately restored negative
mutants in each profile. Every targeted test failed as expected with Cargo
exit 101, and the original source and test SHA-256 values were verified
again after each job. The workflow as a whole still failed because of the
full Admin suites reported above.

| Mutation | Expected failing control |
| --- | --- |
| Remove row guard | `dtp_requested_names_reject_unsolicited_success` |
| Let the large-input index accept any name | `dtp_requested_names_large_input_rejects_unsolicited_topic` |
| Remove cursor membership guard | `dtp_requested_names_reject_unsolicited_cursor` |
| Disable null-name priority | `dtp_requested_names_null_precedes_unsolicited_row_and_cursor` |
| Remove immediate-repeat check | `dtp_requested_names_repeated_cursor_precedes_unsolicited_rows` |
| Remove index-construction yield | `dtp_requested_names_index_yields_after_progress` |
| Remove remainder-scan yield | `dtp_requested_names_scan_yields_after_progress` |
| Replace cooperative accumulated-output cleanup with a synchronous clear | `dtp_requested_names_rejection_cleans_and_checks_deadline` |
| Do not populate the requested-name index | `dtp_requested_names_large_input_accepts_members_and_duplicates` |
| Remove scan yield for cancellation test | `dtp_requested_names_cancel_during_scan_keeps_socket_usable` |
| Remove cursor guard for cursor/ID precedence | `dtp_requested_names_cursor_precedes_same_page_id_conflict` |
| Remove row guard for row/cursor precedence | `dtp_requested_names_unsolicited_row_precedes_unsolicited_cursor` |

These controls exercise the named mutations and fixtures; they do not prove
all possible broker behavior or a wall-clock bound.

## Resource and compatibility limits

In the initial candidate, for up to eight input names, membership uses
direct checks. Larger input lists build a `HashSet<&str>` of borrowed,
distinct caller-supplied names;
it does not clone the names or add peer-supplied response names. Its
additional table memory grows with distinct caller names and table capacity,
not with the number of unsolicited response names. Index construction and
the remainder scan yield after 8,192 items, but hashing long names, table
growth, existing decoding and dropping allocations can exceed any fixed
wall-time target.

The existing loop that converts accepted topic rows has no per-topic yield
for long runs of empty or error rows. The response still owns its decoded
data during rejection and cleanup, and cancellation can synchronously drop
remaining state. Cleanup has per-vector and per-topic counters, not a global
partition-drop bound. The caller's initial input collection and decoded
response/output sizes are not globally bounded by this change.

A `#[cfg(test)]` thread-local observer counts each row in the unsolicited
remainder scan for the cancellation/reuse control. It adds work in test
builds on that error path and is absent from non-test builds. The normal
release timing fixture does not enter that scan. The separate cooperative
row-checkpoint candidate edits the same conversion loop; combining these
changes requires fresh combined-source review and qualification.

[prior]: ../admin-topic-identity-null-20261108/README.md
[handler]: https://github.com/apache/kafka/blob/4.1.0/core/src/main/java/kafka/server/handlers/DescribeTopicPartitionsRequestHandler.java
[cache]: https://github.com/apache/kafka/blob/4.1.0/core/src/main/scala/kafka/server/metadata/KRaftMetadataCache.scala
[request]: https://github.com/apache/kafka/blob/4.1.0/clients/src/main/resources/common/message/DescribeTopicPartitionsRequest.json
[candidate]: https://github.com/mingley/partitionline/commit/0092a3931387528f712bf06f6075b4b5c1a94d4d
[baseline]: https://github.com/mingley/partitionline/actions/runs/42480748853
[baseline-setup]: https://github.com/mingley/partitionline/actions/runs/42480269036
[cursor-baseline]: https://github.com/mingley/partitionline/actions/runs/42481066995
[draft1]: https://github.com/mingley/partitionline/actions/runs/42480807664
[draft2]: https://github.com/mingley/partitionline/actions/runs/42481350778
[final]: https://github.com/mingley/partitionline/actions/runs/42481853719
[broad-setup]: https://github.com/mingley/partitionline/actions/runs/42481915686
[broad]: https://github.com/mingley/partitionline/actions/runs/42481957154
[perf]: https://github.com/mingley/partitionline/actions/runs/42481597370

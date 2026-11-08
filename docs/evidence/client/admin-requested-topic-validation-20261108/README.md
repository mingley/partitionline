# Requested topic names in paginated Admin responses

This note tracks an isolated candidate for the name-based
`describe_topics` aggregate. It is separate from the [earlier topic
identity change][prior], and its final checks are still running.

Kafka 4.1.0's [request handler][handler] builds a set from a nonempty
explicit topic list, deduplicates names, and emits success and topic-error
rows from that set. Its [metadata cache][cache] produces next cursors from
the filtered requested-topic iterator. The [request schema][request]
describes the list as the topics to fetch; it does not separately state a
cross-implementation cursor-membership rule. The aggregate therefore
enforces requested-name membership for response rows and next cursors as a
client consistency policy, returning a fixed-text client-side
`Error::Protocol` rather than fabricating a broker error. The raw
single-page API does not enforce this policy.

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

The [final exact-source gate][final] passed all-feature client Rustdoc with
`RUSTDOCFLAGS="-D warnings"` and all nine client doctests. Its focused and
full Admin suites, strict Clippy, live Kafka smoke and restored negative
controls are still running. The [matched whole-suite control][broad] and
[paired release timing][perf] are also running. An [earlier matched-control
attempt][broad-setup] was setup-only: its baseline and candidate fixture
bytes differed before the corrected run copied the same fixture to both
arms. Remaining results must be recorded before this note is complete.

## Resource and compatibility limits

For up to eight input names, membership uses direct checks. Larger input
lists build a `HashSet<&str>` of borrowed, distinct caller-supplied names;
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

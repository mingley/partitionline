# Paginated topic errors and identity: earlier candidate

This note records the earlier candidate and its checks. A later revision is
being prepared to handle nullable topic names; none of these runs qualify
that changed code.

This candidate changes the `describe_topics` path that aggregates
DescribeTopicPartitions pages. A per-topic error now replaces an earlier
partial success and keeps the error's fields with no partitions. An earlier
error remains the result if a later page succeeds. Topics retain their
first-seen order.

Until a topic error is retained, the index remembers the first nonzero
ID from successful pages and compares subsequent nonzero IDs. A change fails
the whole aggregate call with a client-side `Error::Protocol`, including
when a valid topic deletion and recreation occurs. Global errors, including
an immediately repeated cursor or timeout, can take precedence over a
page's topic error. This error is not classified
as retriable by `Error::is_retriable`; a caller may retry the whole read.
Error-page IDs are not compared. If the first successful page has a zero ID,
the returned successful description keeps zero even if a later page has a
known ID; the earlier page's identity cannot be established. Pages need not
be a consistent snapshot. The raw one-page API is unchanged.

Kafka's [4.1.0 handler][handler] takes the current cursor, filters the
requested topics, and returns topic errors for unauthorized descriptions.
Its [metadata cache][cache] reads a current image per request and reports
unknown topics and topic IDs. The [response schema][response] carries
per-topic errors and IDs. These sources describe the pinned implementation;
they do not establish a cross-page snapshot or general broker ordering rule.
No new non-adjacent cursor-cycle rejection was added.

## Controls and earlier failures

The [baseline run][baseline] used source Git blob
`540592a38a92ebeaada716b51d3b0f735b72b8be` and scripted
pagination tests (blob `c502fbfd7371cb28522c79d092a64e58cc658e3f`).
Both default and all-feature profiles reproduced all four failures:
a later deletion error (code 3) or authorization error (code 29) was
returned as success (code 0); pages with IDs `[7; 16]` and `[8; 16]`
were returned together under ID `[7; 16]`; and a topic that first
reported an authorization error retained a partition from a later success.
The three Cargo invocations per profile each exited 101; the job succeeded
only because those failures were expected.

The [first draft][draft1] failed to compile in both profiles and in Clippy:
the local index still had the old `HashMap<String, usize>` type after
the helper's index type changed. The [next draft][draft2] passed its 19
pagination controls and two existing merge controls in both profiles, but
strict Clippy rejected two manual no-op test wakers. A [draft Kafka 4.1.2
smoke][draft-live] passed four raw pages and aggregation in both profiles,
with matching nonzero IDs and CLI-confirmed partition indexes 0–3.

The [first main-overlay qualification][failed-final] passed its live Kafka
job but failed both focused profiles and strict Clippy because its observing
test waker ignored `AtomicUsize::fetch_add`'s return under
`-D unused-results`. Its mutation jobs stopped at the first compile
failure and did not qualify any mutant. The latest test blob binds that
return value.

## Earlier candidate and qualification

The earlier clean candidate is [commit `ea54757`][candidate], based on main
`d2575d27ca53af47a627ca48f27a12bba34945bd`. The production source
Git blob is `d66f9c4a3ac9cb1094713ab147ab783ae81ced31`
(SHA-256 `e959c24b36cadb25b223a786e5f18dcf8f9615e27b04b7eb28dcd91fbc98a3aa`);
the permanent test blob is `746dc1724698adcc53dd4f026a73ebc5b6d4f93a`.
The permanent test SHA-256 is
`46b288f3cf9337699b6adf58466fcb1e0deaa686b523f24633e3ece2a6d97a71`.
In the [final qualification run][final], formatting and strict
all-target/all-feature Clippy passed. The pagination controls passed 20/20
in each feature profile. A fresh digest-pinned Kafka 4.1.2 broker passed the
live smoke in both profiles: the CLI confirmed partitions 0–3, and four raw
pages and the aggregate had the same nonzero topic ID and expected
partitions. This is one normal local broker history; metadata changes and
authorization errors were scripted in the loopback controls.

The full default Admin suite failed: 624 passed and three tests exceeded
their 80 ms thresholds—delegation-token decoding (91.759289 ms), named
result grouping (97.628215 ms), and coordinator indexing (111.333483 ms).
The all-feature suite had 626 passed and one failure: coordinator indexing
at 160.219672 ms. These are retained failures, not a green broad suite.
Fourteen negative controls passed across default and all-feature profiles:
each profile independently restored the original source before mutating
later-error propagation, first-error retention, known-ID comparison,
known-ID tracking after an unknown page, cleanup yielding, error cleanup,
and conflict cleanup. Each intended permanent test failed under its
mutation, and both jobs verified the original source hash after restoration.
The [first matched whole-suite baseline control][broad] stopped after its
first baseline suite failed: GitHub's Bash wrapper retained `-e`, so the
harness exited before printing that suite's status and before any other arm
ran. Its [captured baseline log][broad-artifact] was recovered: 615 passed,
and delegation-token decoding (80.197017 ms) and named-result grouping
(124.29199 ms) failed their 80 ms thresholds.

The [corrected four-arm control][broad2] ran both whole Admin suites on one
shared AMD EPYC 9V74 x86_64 runner, with the original pagination test
fixture in both arms and the earlier candidate's production source. The
default test harness settings were the same. All four arms exited 101:

- Default baseline: 614 passed, 3 failed. Reassignment-result flattening
  took 90.309441 ms, named-result grouping 97.145835 ms, and coordinator
  indexing 104.467155 ms against their 80 ms thresholds.
- Default candidate: 616 passed, 1 failed. Coordinator indexing took
  117.736497 ms.
- All-feature candidate: 615 passed, 2 failed. Delegation-token decoding
  took 82.966014 ms and coordinator indexing 106.761231 ms.
- All-feature baseline: 614 passed, 3 failed. Delegation-token decoding took
  92.037223 ms, named-result grouping 94.303584 ms, and coordinator
  indexing 106.311004 ms.

This is the execution order. The shared host and matching failures do not
establish their cause. The run used baseline source SHA-256
`929aaafb35e3ef1f376de7debd090367960ec22428cccf79a4bd806c50992d79`,
candidate source SHA-256
`e959c24b36cadb25b223a786e5f18dcf8f9615e27b04b7eb28dcd91fbc98a3aa`,
and the same pagination fixture SHA-256
`cb19e0db9291dd8a1daf9eff49ea5bd039a6d8f8d72cf04af3c1cca0798e5114`
in both arms. The candidate fixture here does not contain the new controls;
those passed separately as described above.

## Limits

The earlier candidate converts a null response topic name to an empty
string before indexing it. Different unnamed topics can therefore be
conflated or produce a client-side identity error. The response schema
allows null names, and the revised candidate is addressing this separately.

The existing conversion loop does not yield between empty or error topic
rows. A very large page of such rows can occupy one poll beyond the
deadline; this source-derived limit has not been measured here and was not
changed by the earlier candidate.

Discarded partitions are released in batches of at most 8,192 with
cooperative yields between batches, including the previously accumulated result on an
identity conflict. Manual-poll controls check progress and wakeups; the
observing waker adds work only to those test polls. This bounds a batch's
item count, not its wall time, nested allocations, allocator work or process
memory. A timeout or cancellation can still synchronously drop remaining
state, including the raw decoded response and other accumulated values.
The index retains an additional 16-byte topic ID alongside each topic slot
on the tested x86_64 target, before hash-table capacity and allocator
overhead; result and index growth remain dependent on the responses.
Longer cursor cycles retain the existing timeout behavior and can grow the
aggregate until timeout. A non-adjacent cursor repetition that later
terminates can still append duplicate partitions.

The [paired release-mode loopback comparison][perf] used a shared x86_64
AMD EPYC 7763 runner pinned to CPU 0, Rust 1.99.1 and LLVM 23.1.1.
Each arm was warmed up, then measured in 12 alternating pairs for 4,000
one-page reads and 250 sixteen-page reads, with 4,000 page exchanges in
each shape. The candidate used the final production blob and an isolated
benchmark test fixture (Git blob `f262df289a1473d3b9975633cbaddbd85a2672a1`,
SHA-256 `6debb31af2d9ed693e3b14390fbb512de75d2a8baad0f1e35d777d356527ff7f`).
All [48 elapsed-time samples](paired-timings.csv) are retained.

For the one-page shape, median elapsed time was 123.75 ms baseline and
123.59 ms candidate; the median paired candidate/baseline ratio was 0.99789
(range 0.97572–1.00702), with the candidate slower in 6 of 12 pairs. For
16-page reads, medians were 122.63 ms and 121.09 ms; the paired ratio was
0.98845 (range 0.88400–1.03038), with the candidate slower in 3 of 12.
One baseline 16-page sample was 137.17 ms. These noisy local exchanges do
not establish performance against other Kafka implementations, large topic
lists, other machines, or production workloads.

The baseline source SHA-256 was
`929aaafb35e3ef1f376de7debd090367960ec22428cccf79a4bd806c50992d79`;
its binary SHA-256 was
`2e620a9bb063189720b0c12cd77b0289bb4334f77fd588ff335231f0313b3907`.
The candidate binary SHA-256 was
`36d6b1aeca7f52a0159cba4e8176c332665dbf08a1ee84deec38e6ded6bbba3b`.

[handler]: https://github.com/apache/kafka/blob/4.1.0/core/src/main/java/kafka/server/handlers/DescribeTopicPartitionsRequestHandler.java
[cache]: https://github.com/apache/kafka/blob/4.1.0/core/src/main/scala/kafka/server/metadata/KRaftMetadataCache.scala
[response]: https://github.com/apache/kafka/blob/4.1.0/clients/src/main/resources/common/message/DescribeTopicPartitionsResponse.json
[baseline]: https://github.com/mingley/partitionline/actions/runs/42474445429
[draft1]: https://github.com/mingley/partitionline/actions/runs/42474732955
[draft2]: https://github.com/mingley/partitionline/actions/runs/42474908779
[draft-live]: https://github.com/mingley/partitionline/actions/runs/42474939466
[failed-final]: https://github.com/mingley/partitionline/actions/runs/42475491527
[candidate]: https://github.com/mingley/partitionline/commit/ea54757826cd18d824059d2bc87470151c80dcef
[final]: https://github.com/mingley/partitionline/actions/runs/42475657022
[perf]: https://github.com/mingley/partitionline/actions/runs/42475135691
[broad]: https://github.com/mingley/partitionline/actions/runs/42475935325
[broad-artifact]: https://github.com/mingley/partitionline/actions/runs/42475935325/artifacts/13615189307
[broad2]: https://github.com/mingley/partitionline/actions/runs/42476592121

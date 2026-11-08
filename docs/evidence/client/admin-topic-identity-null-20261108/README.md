# Paginated topic descriptions with nullable names

This note tracks the revised candidate for aggregated
DescribeTopicPartitions responses. It is not yet applied to main. The
[earlier candidate and its measurements][old] use different source and
test blobs; they do not qualify the null-name guard described here.

The candidate keeps the first reported per-topic error and clears any
partial partitions. Until an error is retained, successful pages with
different known topic IDs fail the aggregate call with client-side
`Error::Protocol`; a valid deletion and recreation can cause this.
`Error::is_retriable()` is false for that outcome, although a caller may
retry the whole read. A zero ID cannot establish page identity, and a
successful description keeps its first page's ID.

A null wire topic name cannot be safely associated with a requested name.
The aggregate returns `Error::Unsupported` instead of merging unnamed
entries; the raw `describe_topic_partitions_timeout` method preserves
the response page, nullable names and IDs. An explicit empty string is
distinct from null on the wire and continues through normal aggregation.
Returned topic descriptions follow their first appearance in broker
responses, which can differ from request order.

Global errors can prevent any descriptions from being returned. An
immediately repeated cursor is checked before a page's topic errors or null
names. Within a page, a null name is detected before that page's ID
conflicts are checked. A timeout can take precedence during cleanup. Non-null
response names, including empty strings, are not validated against the
requested set.

## Candidate and checks

The [reviewed candidate][candidate] is based on main
`c4819e83e6f0f5a8a4e1f9dd0f0bbdcad81b862c`. Its production source Git
blob is `4d4212057984e47201b2bb1fe383ac2ae5a55562` (SHA-256
`25fac8f3acbb31f08417d374cff8156cef34d66ca08eb002b08bd6d9e32eb98f`)
and its permanent test blob is `18780c30036bef5fbd693777b265e89a1dcd91e5`
(SHA-256
`e0527fc15e8f84e2d3bb23f27724ebb0995503f82910bdd1f587ef2fddb68afc`).

A [preceding draft][draft] passed 27 pagination controls and two existing
merge controls in each profile, plus strict all-target/all-feature Clippy.
The final candidate adds explicit same-page null-before-ID precedence
documentation and a scripted control, so the draft results use different
source and test blobs.

In the [final qualification][final], formatting and strict
all-target/all-feature Clippy passed, as did 28 pagination controls in
each profile. Both full Admin suites failed with 633 passed and two
failures each. In the default profile, named-result grouping took
137.547952 ms and coordinator indexing 169.259935 ms. In the all-feature
profile, created-topic decoding took 83.091523 ms and coordinator indexing
114.643677 ms. Each failed its 80 ms threshold.

A fresh digest-pinned Kafka 4.1.2 broker passed the live smoke in both
profiles. The CLI confirmed partitions 0–3; four raw pages and the
aggregate agreed on their nonzero topic ID and partition indexes. This is
one normal local broker history, while the null-name and precedence cases
use scripted responses.

The exact-source [all-feature client Rustdoc and doctests][rustdoc] passed
with `RUSTDOCFLAGS="-D warnings"`; all nine doctests passed.

Both profiles passed 14 independently restored negative controls each.
The controls cover later and retained topic errors, changed and unknown IDs,
cooperative partition and topic cleanup, null-name handling before and after
partial results, null-before-ID precedence, retained errors followed by a
different ID, and repeated-cursor precedence over errors and null names.
Each intended test failed under its mutation, and each job verified the
original source and test hashes after restoration. These controls do not
change the failed full-suite results above.

## Matched whole-suite baseline

The [matched comparison][broad] ran both complete Admin suites on one shared
AMD EPYC 9V45 x86_64 runner. The baseline and candidate used the same
original pagination fixture and default harness settings; the new permanent
controls were checked separately. In execution order:

- Default baseline: 614 passed, 3 failed. List-reassignment flattening took
  81.995848 ms, named-result grouping 86.528503 ms, and coordinator indexing
  132.443488 ms against their 80 ms thresholds.
- Default candidate: 617 passed, none failed.
- All-feature candidate: 617 passed, none failed.
- All-feature baseline: 615 passed, 2 failed. Named-result grouping took
  87.028318 ms and coordinator indexing 82.004267 ms.

Both baseline arms exited 101, both candidate arms exited 0, and the job
status was failure. This single matched run does not explain the wall-time
failures or override the failures of the final full suites above. The
baseline source SHA-256 was
`929aaafb35e3ef1f376de7debd090367960ec22428cccf79a4bd806c50992d79`,
the candidate source was the final hash above, and both arms used pagination
fixture SHA-256
`cb19e0db9291dd8a1daf9eff49ea5bd039a6d8f8d72cf04af3c1cca0798e5114`.

## Final paired release timing

The [final-source comparison][perf] ran on a shared x86_64 AMD EPYC 7763
runner pinned to CPU 0, with Rust 1.99.1 and LLVM 23.1.1. After warmup,
it measured 12 alternating pairs of 4,000 one-page reads and 250
sixteen-page reads, each shape involving 4,000 page exchanges. The
candidate production SHA-256 was the final source hash above. Both arms
used benchmark fixture Git blob
`f262df289a1473d3b9975633cbaddbd85a2672a1` (SHA-256
`6debb31af2d9ed693e3b14390fbb512de75d2a8baad0f1e35d777d356527ff7f`).
All [48 samples](paired-timings.csv) are retained.

For one-page reads, median elapsed time was 120.91 ms baseline and
121.42 ms candidate. The median paired candidate/baseline ratio was
1.00507 (range 0.80783–1.01235), with the candidate slower in 9 of 12
pairs. For sixteen-page reads, the medians were 119.54 ms and 120.02 ms;
the paired ratio was 1.00484 (range 0.98541–1.01858), with the candidate
slower in 9 of 12. One baseline one-page sample was 151.27 ms. These
results show a small slowdown signal in this noisy local fixture; they do
not establish performance against other Kafka implementations, larger
topic lists, other machines, or production workloads.

The baseline source SHA-256 was
`929aaafb35e3ef1f376de7debd090367960ec22428cccf79a4bd806c50992d79`;
the baseline binary SHA-256 was
`2e620a9bb063189720b0c12cd77b0289bb4334f77fd588ff335231f0313b3907`
and the candidate binary SHA-256 was
`a67e4c33ae4f76e6523e6d2b4b50405b18d0f40a7f819e7fd29f46357e99ba1b`.

## Intermediate timing

An [intermediate nullable-source comparison][intermediate-perf] used a
shared AMD EPYC 7763 x86_64 runner pinned to CPU 0, Rust 1.99.1 and LLVM
23.1.1. It tested 12 alternating pairs of 4,000 one-page reads and 250
sixteen-page reads after warmup. The intermediate production blob was
`0e7091bc9fbc60b51fefdab7d4ef99b3914141fe` (SHA-256
`f785c320bf501a7d25e4db7b87a5d472ec09e74bc8a9321b1aecf366642a817f`);
it predates the final Rustdoc and test correction. Its benchmark fixture
blob was `f262df289a1473d3b9975633cbaddbd85a2672a1`. The
[48 intermediate samples](intermediate-paired-timings.csv) are retained.

The paired candidate/baseline median was 0.99622 for one-page reads
(range 0.68748–1.04290; candidate slower in 5 of 12 pairs) and 0.98716
for sixteen-page reads (range 0.89933–1.00313; slower in 3 of 12 pairs).
The one-page baseline included a 178.76 ms sample. These local results do
not qualify the final source or establish a comparison with other Kafka
implementations.

## Limits

The conversion loop processes topic rows before merging a page. It has no
per-topic yield for a long run of empty or error rows; this pre-existing
limit is source-derived and unmeasured. Null-name cleanup releases at most
8,192 partition descriptions between yields and yields after 8,192 topic
rows. Manual-poll controls check progress and wakeups using a counting
waker that adds work only to those tests; they do not measure production
wall time.

During null-name cleanup the call still owns the decoded response and
other state. Timeout or cancellation can synchronously drop remaining data,
and a chunk bounds an item count rather than wall time, allocator work or
total memory. On the tested x86_64 target the index value grows from 8 to
24 bytes per slot, before hash-table capacity and allocator overhead.
Neither this change nor the cursor deadline provides a global memory bound.

Longer cursor cycles retain the existing timeout behavior. A non-adjacent
repetition that later terminates can still append duplicate partitions.
Non-null response names are not checked against requested names, and pages
need not represent a single metadata snapshot. These remain separate
follow-ups.

[old]: ../admin-pagination-identity-20261108/README.md
[candidate]: https://github.com/mingley/partitionline/commit/f56f686719ec6a6387986a1a541d5c435ef59156
[draft]: https://github.com/mingley/partitionline/actions/runs/42477540206
[final]: https://github.com/mingley/partitionline/actions/runs/42478040397
[broad]: https://github.com/mingley/partitionline/actions/runs/42478078989
[perf]: https://github.com/mingley/partitionline/actions/runs/42477945484
[intermediate-perf]: https://github.com/mingley/partitionline/actions/runs/42477689277
[rustdoc]: https://github.com/mingley/partitionline/actions/runs/42479481083

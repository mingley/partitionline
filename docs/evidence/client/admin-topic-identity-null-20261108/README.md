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
all-target/all-feature Clippy passed, as did the 28 default-profile
pagination controls. The full default Admin suite failed: 633 passed and
two exceeded their 80 ms thresholds, named-result grouping at
137.547952 ms and coordinator indexing at 169.259935 ms.

A fresh digest-pinned Kafka 4.1.2 broker passed the live smoke in both
profiles. The CLI confirmed partitions 0–3; four raw pages and the
aggregate agreed on their nonzero topic ID and partition indexes. This is
one normal local broker history, while the null-name and precedence cases
use scripted responses.

The all-feature Admin suite, independently restored negative mutations,
[same-host whole-suite comparison][broad] and [paired release-mode
timing][perf] are still running. The broad suite is not green, and the
remaining results must be recorded before this note is complete.

## Limits

The conversion loop processes topic rows before merging a page. It has no
per-topic yield for a long run of empty or error rows; this pre-existing
limit is source-derived and unmeasured. Null-name cleanup yields while
releasing accumulated partition descriptions, but it still owns the decoded
response and other state. Timeout or cancellation can synchronously drop
remaining data, and a chunk bounds an item count rather than wall time,
allocator work or total memory. Neither this change nor the cursor deadline
provides a global memory bound.

Longer cursor cycles retain the existing timeout behavior. A non-adjacent
repetition that later terminates can still append duplicate partitions.
Non-null response names are not checked against requested names, and pages
need not represent a single metadata snapshot. These remain separate
follow-ups.

[old]: ../admin-topic-identity-20261108/README.md
[candidate]: https://github.com/mingley/partitionline/commit/f56f686719ec6a6387986a1a541d5c435ef59156
[draft]: https://github.com/mingley/partitionline/actions/runs/42477540206
[final]: https://github.com/mingley/partitionline/actions/runs/42478040397
[broad]: https://github.com/mingley/partitionline/actions/runs/42478078989
[perf]: https://github.com/mingley/partitionline/actions/runs/42477945484

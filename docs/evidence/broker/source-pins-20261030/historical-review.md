# Broker inventory source binding review, 2026-10-30

Main `02ce806d7ea4970897d51604e71e6a1fa069ca4c` has eleven stale local
source hashes. The retained API inventory job stops at `metadata_source`;
the remaining ten mismatches become visible when every bound path is hashed.
`stale-source-pins.json` records the exact historical source revision matching
each old digest, the current digest and every intervening source commit.
The corresponding before/current diffs are retained beside it.

`source-pins-main-02ce.patch` updates only those eleven digest strings. An
independent JSON comparison verifies that all paths, fixture maps, advertised
API ranges, implementation gates and `qualification: not_run` are identical.
The checker and its runtime report validation are unchanged.

The metadata binding predates the response/request buffer reuse, grouped
Produce integration, individual topic name/UUID resolution, bounded duplicate
tracking, dot/underscore collision checks, bracket-free IPv6 advertisement and
explicit metadata-only quorum endpoint constructor. The ordinary router still
uses the same four API ranges from `protocol::IMPLEMENTED_API_VERSIONS`.
The quorum constructor remains an explicit alternative with fixed endpoint
membership and no attached Produce/Fetch storage; this refresh does not
qualify it as native KRaft or replicated partition data.

The data bindings cover changes to direct record projection, validated CRC
reuse, staged and grouped local fsync, bounded sync concurrency, slot indexes,
duplicate detection, response reuse and Fetch boundary/timestamp handling.
The Produce profile still advertises versions3..13, rejects transactions and
idempotence, and has RF1 local-fsync acknowledgement semantics. Read/write
adds only Fetch4..6 and ListOffsets1..3; retention adds DeleteRecords0..2.
The newer data paths are not a broader support range or production claim.

The Produce integration-test delta adds checks that persisted base offsets
advance by each stored record count. The Fetch test delta uses an admission
watch instead of racing for the sole permit and adds the earliest/latest
ListOffsets case. Retention's source delta reuses the response allocation.
Controller/election drift includes unadvertised modern parsers, defensive
request rejection and journal/voter-image reuse. Independent static review by
`/root/protocol_final` found no blocker in the binding scope: the controller
still advertises only API18v0..4 and keys52/53/54v0, refuses other keys/versions
and dynamic membership contexts before mutation, and leaves modern inspectors
unadvertised. Election keeps append-before-state-publication ordering while
reusing encodings. Neither the patch nor this review grants qualification.

The exact-main broker CI job `124240488981` checked out `02ce806d7` and passed
both default and all-feature all-target suites, strict Clippy and Rustdoc.
Its retained log contains successful executions, under both feature profiles,
of the independent protocol, metadata/admin, Produce, Fetch, DeleteRecords
and fixed-controller golden tests. The CI log SHA-256 is recorded in
`source-pins-main-02ce-manifest.json`. This is evidence from the named CI
checkout; it is not a newly generated, locally attached compiled report.

The static inventory verifier passed on the isolated prepared refresh.
Its receipt records stable input bytes, no OOM changes and a minimum of
1,860,657,152 free bytes above the1.5GiB guard. The first Python negative-control
attempt found an omitted historical wire-report fixture in the isolated view;
its failure log is retained. The next attempt included that exact unchanged
fixture but was stopped when root paused Python load to protect timing tests.
The full55-test suite remains unqualified pending an explicitly granted
compile interval. No Rust build or test was started for this audit.
Existing `--handler-report`, `--metadata-handler-report`, data/read-write,
retention and controller report checks are preserved. Their current source
hash label limitation remains explicit in the checker: real execution
provenance requires the immutable-source command records. A source digest
refresh alone cannot establish fresh runtime behavior.

The separate broker candidate changes four of these sources again. After
its fixes are applied and qualified, its four final digests must replace
the current-main ones; the present patch must not be described as binding
those future bytes.

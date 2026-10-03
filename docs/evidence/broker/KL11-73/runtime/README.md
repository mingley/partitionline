# KL11-73 fixed-membership durable replication

The compiled runtime is pinned to remote Git source
`db002076bb5a19ed4a84cf8a60bce949c1d9cd84`. It adds an operation WAL,
bounded leader/follower content exchange, strict-majority durable commit, and
one joined storage/election actor for the configured fixed voters. Exchanges
are caller-driven typed messages. Kafka replication wire support, automatic
peer networking, dynamic membership, snapshots, and production qualification
remain separate work. The existing version 0 controller advertisement is unchanged.

`final-db002076/results.json` records four complete broker test lanes: stable
and Rust 1.85, each with default and all features. Each default lane passes 173
tests; each all-feature lane passes 250. All lanes have zero failures or ignored
tests, and pass strict all-target Clippy, Rustdoc, and doctest compilation.
There are no doctest cases. The final format check passes. The exact commands,
exit codes, and log hashes are in `final-db002076/commands.json`.

Each lane retains new three/five-voter process histories with 73/121 events and
10/16 pairs of copied real metadata/election journals. A child exits without
running destructors; its parent reopens the actual files. Histories include
minority isolation, quorum expiry, lost and stale responses, catch-up, a new
leader/barrier, and recovery. The five-voter history repairs two uncommitted
suffixes; the three-voter history elects the longer-log holder and commits its
accepted old prefix through a new-term barrier. These are finite executions,
not exhaustive scheduling or physical power-loss testing.

The coordinator's separately owned
`../oracle/history/final-db002076/validation.json` checks raw Journal framing,
CRC32C, operation and election replay, causal majority receipts, and agreement
of committed byte prefixes. It accepts all eight histories, totaling 776
events and 104 journal pairs, and rejects 76 deliberate proof counterexamples.
The retained `partial-final-operation` controls are structurally incomplete
writes, despite an overly broad generic mutation flag in the original checker
output; they are not complete checksum-valid semantic operations.

The independent Apache receipt at
`../oracle/apache/final-f0d4e5d/immutable-validation.json` executes 330 component
assertions and detects three deliberately wrong expectations against official
Kafka 4.1.2, 4.2.1, and 4.3.1 libraries. Its scope is actual LeaderState/FollowerState
and local log components. It does not run Partitionline replication or an
Apache network quorum. Bare LeaderState can advance beyond local durable end
with invalid caller-supplied offsets, and the local log component's high
watermark resets on reopen; the local causal and durable-WAL checks therefore
remain separate obligations.

`mutants/final-db002076/results.json` retains a four-test public-Node baseline
and four predeclared, compiled safety mutations. The baseline passes; each
mutation fails its named durability scenario with exit 101, rather than a
compiler failure. Removing the exact sent target admits a forged ACK inside
the leader's durable tail; weakening distinct majority commits two of five
positions; removing both current-term runtime guards commits an old-term
partial prefix; removing the follower matched-end cap commits an unverified
suffix. Patches, mutated sources, probe source, test logs, and actual journal
files are retained. Main and the immutable runtime archive are unchanged.

`final-db002076/capture-summary.json` retains the suite counts and compiled
protocol/Metadata/Produce/Fetch/controller reports. The 201 controller golden
outcomes and 18 real loopback TCP exchanges per lane preserve the prior wire
contract. All 222 retained controller payload/frame files are byte-identical
across the four lanes. Optional external Kafka live-port helpers were unset;
their successful conditional tests do not establish an external session.

`final-source/source-integrity-before.json` identifies every 20,137 Git blob,
file mode, SHA256, Git tree, and archive digest. The after-QA and after-mutation
receipts verify that all tracked bytes remain unchanged. The full archive
excludes shared uncommitted snapshot, data-storage, transport, and client work.
`source-freeze.json` is the preserved pre-push receipt; its pending status is
historical, and the final receipts bind the actual pushed source.

All development attempts remain under `development/`, including the first
incorrect recovery/torn-tail setups, stale-request fixture failures, strict
lint failures, and failing-first durable replay cases. The corrected runtime
rejects follower commits authorizing newer entry terms, unowned legacy log
summaries, and regressing commit authority. The initial inventory failure is
also retained: the registry still referenced the old controller source hashes
at the runtime source pin. Its coordinator-owned hash normalization and final
gate receipt are recorded separately in the final card evidence.

The normalized registry is pinned to
`1e357b041dcb307dbc270bff369d51e7bb525f25`. Its first combined gate also failed:
the controller reporter embeds the registry labels, and the original runtime
archive correctly retained the historical labels. The raw original reports
were preserved. `controller-labels-final-1e357b04/validation.json` records
rerunning the ten affected controller tests in all four lanes: 40 tests pass,
only the three reflected labels change, and all 222 payload/frame bytes per
lane remain identical. The final combined gate consumes these fresh reports
plus the original protocol/data reports. All archived broker files at the
gate source are byte-identical to the tested runtime. The separately retained
KL11-68 data/read API18 exchanges remain frozen supplementary inputs.

Replication defaults bound a record to 1MiB, a chunk/fetch to 2MiB (4MiB maximum),
live content to 4096 entries/64MiB, and the operation WAL to 65,536 entries/256MiB.
Membership is at most 64 fixed voters, with one outstanding request per peer.
The actor defaults to 16 slots; canceled inputs, completed unconsumed receipts,
and one in-flight envelope are conservatively charged by `(2*slots+1)` against
512MiB. Journal indexing/read scratch and at most 1MiB startup-tail positions
are bounded; startup-tail positions are released before readiness. Returned
consumed records belong to the caller. Admission overload is explicit, and
ambiguous persistence poisons the owner until verified reopen. Storage paths
and typed peer identities are trusted configuration, not authentication.

Dynamic membership is explicitly rejected. KL11-74 and snapshot KL11-15 must
qualify separately; aggregate KL11-14 remains open. No new Kafka API is
advertised, and no production or performance claim follows from this card.

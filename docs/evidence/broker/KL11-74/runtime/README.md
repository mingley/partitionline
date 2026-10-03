# KL11-74 corrected developmental runtime handoff

The configuration-origin recovery fix is ready for a source checkpoint. This
directory contains focused development proof, not final qualification of a
pushed Git source. `source-freeze.json` binds the exact source/test overlay,
executed commands, and staging paths against Git base
`d21c5bdfaff0fb37d248793c27ce5a5b1a06a3b4`. The coordinator owns commits,
`raft/mod.rs`, final immutable validation, and the independent raw/causal oracle.

Historical election references now require an operation that actually changed
the canonical voter configuration from its predecessor, using genesis at
predecessor zero. Bounded journal fetch, canonical size checks, and exact
identity checks remain in place. An equal-view Data or Commit operation cannot
redefine the configuration's source ordinal. The capture helper bounds each of
its four text-file reads to 32 MiB plus a rejection sentinel byte.

The actual old implementation accepted a checksum-valid historical source
forgery and recovered a ready owner with the expected final configuration. Its
intended rejection assertion failed. The source, executed binary, original and
mutated election journals, metadata journal, and result are retained in
`development/ordinal-counterexample-validation.json`. The corrected test rejects
both an equal-view Commit decoy and an equal-view Data decoy, including when a
later genuine configuration change restores the final context.

Both stable and Rust 1.85 passed the complete 26-case `raft_membership` and
11-case `raft_protocol` suites: 74 passes in total, zero failed, ignored, or
filtered. Each suite includes the five-voter denied/first-grant campaign
regression, configuration preflight before durable mutation, both configuration
source counterexamples, fixed-v0/dynamic-owner boundary, and existing loopback
transport tests. Each toolchain also passed focused strict Clippy for the
library and membership, replication, election, snapshot, and controller tests,
plus formatting checks for the ten source/test files. These are six actual
commands, recorded with source maps before and after. The formatting check
excludes the coordinator's module export file to avoid recursive formatting.

Each complete test run retains fresh three- and five-voter process histories:
132/201 events and 28/42 paired raw content/election journal checkpoints.
Each history uses an actual child exit code 88, carries clock 16 to parent 17,
and recovers owners before electing with the new set. Addition requires the
new-set majority; committed leader removal excludes the removed leader's
match and revokes its authority. The history helper is an ordinary no-op test
when invoked without the child environment; the main history test invokes it
twice as a real child process per toolchain.

The complete stable and Rust 1.85 capture seals are
`development/corrected-complete-{stable,msrv}-tests.capture-seal.json`. They bind
all 410 capture files per lane, including the counterexample journals and
fixed-controller outputs. Each lane also retains 186 direct response payloads
from 201 authentic Apache controller cases and 18 actual TCP response payloads
with their 18 frames. The four executed membership/controller test binaries
are retained as gzip files, with raw and compressed hashes, original modes,
and verified lossless decompression.

Earlier failures and successful proof remain unchanged. These include the
first helper lint failure, the original successful three-/five-voter history,
the old candidate-correlation panic, scaffold compile failures, and the
disk-floor cancellation before a semantic outcome. The earlier
`corrected-origin-stable-tests` receipt passed 35 tests with two local transport
cases filtered under an ambiguous restriction. Its receipt remains accurate;
the two complete 37-case runs supersede it for this handoff's case counts.

The runtime remains caller-driven typed exchange with a known leader directory
and configuration. A laggard rejects an unknown newly added leader before
persistent changes. Trusted discovery and autonomous transport remain
KL11-76/70; this change adds no modern KRaft wire or default advertisement.
Snapshots retain the bounded canonical prefix and opaque records; there is no
physical compaction or application fold. The independent Apache component
receipt at `../oracle/apache/final-3aedadf6/validation.json` remains a separate
component proof, including the explicit stronger local committed-removal
fence. Final immutable default/all-feature broker validation and independent
checks of these corrected raw histories are still required.

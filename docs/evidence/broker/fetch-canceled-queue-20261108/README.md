# Canceled Fetch queue witness

The [earlier default run](https://github.com/mingley/partitionline/actions/runs/42442982874)
reported `QueueFull` from a fresh Fetch after transport shutdown. The error
did not identify whether the data permit or the actor queue was full, so its
exact cause remains unproven.

A [gated witness](https://github.com/mingley/partitionline/actions/runs/42444172518)
parks the storage actor with a queued Fetch, cancels the caller and verifies
that the data permit is free while the actor queue is still full. A fresh
Fetch returns `QueueFull` in that state. The test then releases the actor,
uses a FIFO metadata request to drain the queue and successfully fetches
again. A counterexample that expects immediate success fails, and a
forgotten-permit mutation is detected. Both feature profiles passed ten
repetitions of the witness. This reproduces a possible queue state, not the
exact uninstrumented failure.

The fixture, tested as [candidate fc3e11e](https://github.com/mingley/partitionline/commit/fc3e11e64585dd0727e31e9f8592f9b7ba8e5f5a),
adds that queue barrier before the fresh Fetch. The barrier does not acquire
a data permit; the fresh Fetch still checks that the permit was released. The
fixture and witness are applied on main.

The [candidate qualification](https://github.com/mingley/partitionline/actions/runs/42444695386)
passed the focused witnesses, all 887 all-feature tests, formatting,
all-target Clippy, Rustdoc, doctests, the API inventory and all 55 inventory
controls. The full default suite stopped in the broker library after 451
passes: `metadata_quorum::tests::new_leader_repairs_uncommitted_metadata_without_creating_a_ghost_topic`
returned `Journal(AlreadyOpen)`. Later default targets did not run. The
[retained logs](https://github.com/mingley/partitionline/actions/runs/42444695386/artifacts/13603770395)
record the failure. The artifact was not independently inspected.

The candidate tests have SHA-256
`16cfa1814083515420f3e7f705200fcfd19e2c5c40c1282054707132d7f561c9`
for `partitionline-broker/src/metadata.rs` and
`f33dd42b768d8bdd1268bf5cdd419205a7f2e7fea65446e630a6581c32990aed`
for `partitionline-broker/tests/fetch.rs`.

A canceled caller can release its permit while the storage actor still owns
its request and work buffers. This witness addresses queue occupancy; it does
not establish a process-wide memory bound or resolve that ownership overlap.

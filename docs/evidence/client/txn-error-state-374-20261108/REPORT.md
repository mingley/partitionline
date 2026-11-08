# Transaction error-state candidate

The transaction error-state candidate remains on a qualification branch. It is based on main commit [`374dcf45`](https://github.com/mingley/partitionline/commit/374dcf45db20ee728b00c0b0188e77ca1f47fa9c). The source tested by [run 42444340333](https://github.com/mingley/partitionline/actions/runs/42444340333) is commit [`bb1f0ee2`](https://github.com/mingley/partitionline/commit/bb1f0ee2bf7faa9c96605015f19db26195742581):

| File | SHA-256 |
| --- | --- |
| `src/producer.rs` | `3d65fcceecbb43c1a974f7f6b7bd58c8a310930f6bcced9052a00e88700eb37f` |
| `src/producer/txn_error_state_controls.rs` | `4215a4f116ab80588ebea39e78b89917acbb73bb64acd991c6cd3ecb7d4c6e45` |

The candidate records error severity and an abort-claim ticket in one atomic. Pending states make abortable and fatal claims visible before the cause mutex becomes available; a fatal cause takes precedence over an earlier abortable cause. An old abort setter cannot publish after its claim was cleared and replaced. While a cause is still pending, a reader receives a generic protocol error instead of waiting for the specific cause; after publication it receives the retained cause. Ticket exhaustion fails closed instead of wrapping.

## Completed qualification

Run 42444340333 passed on GitHub-hosted Ubuntu 24.04 x86_64 with Rust 1.99.1. In each of the default and all-feature profiles, it ran 13 candidate state controls and nine existing transaction coordinator and lock controls. The state controls cover fatal versus abortable precedence, readers while the cause lock is held, a reader rechecking after fatal publication, competing fatal writers, clear versus setters, a cleared stalled abort followed by another abort, and last-ticket and exhaustion boundaries.

The run also applied test-only scheduling hooks to the baseline and required nine controls to expose the old behavior in each profile. Baseline and candidate used separate clean Cargo target directories. Stable formatting and `cargo +stable clippy --locked --all-targets --all-features -- -D warnings` passed. [Logs](https://github.com/mingley/partitionline/actions/runs/42444340333/artifacts/13603395594) retain the individual results.

Earlier [run 42443810287](https://github.com/mingley/partitionline/actions/runs/42443810287) passed the state and coordinator controls in both profiles but failed overall: strict all-target Clippy rejected two identity operations in test-only ticket helpers. The candidate's helpers were corrected before run 42444340333. An earlier [run 42440453807](https://github.com/mingley/partitionline/actions/runs/42440453807) stopped at formatting before those controls ran.

The broader integration selection in [run 42444608489](https://github.com/mingley/partitionline/actions/runs/42444608489), at commit [`33e775b4`](https://github.com/mingley/partitionline/commit/33e775b49195d4a22c9e9abb5a3293bf14ebe615), failed. The state and coordinator controls passed in both profiles. Thirteen default-profile integration cases passed, then `commit_reuses_a_produce_handshake_for_the_moved_coordinator` observed three ApiVersions requests where it expected two before coordinator movement. The remaining integration cases, all-feature integration selection, and that run's Clippy step did not run. [Logs](https://github.com/mingley/partitionline/actions/runs/42444608489/artifacts/13602773411) retain the failure.

[Run 42445260031](https://github.com/mingley/partitionline/actions/runs/42445260031) tested the same candidate source at commit [`ab7c3cd7`](https://github.com/mingley/partitionline/commit/ab7c3cd73e20d296462177fae5517f7b69bc993a). It completed the state and coordinator controls in both profiles and passed strict all-target, all-feature Clippy. Eighteen of 21 selected integration tests passed in each profile. These three handshake-count tests failed in each profile:

- `commit_reuses_a_produce_handshake_for_the_moved_coordinator`: three ApiVersions requests observed, two expected.
- `epoch_bump_reuses_a_produce_handshake`: three observed, two expected.
- `send_offsets_reuses_bootstrap_api_versions`: two observed, one expected.

The first test failed three times in each profile on the exact unmodified `374dcf4` baseline and three times in each profile on the candidate. The job retained a failing gate for the integration selection. [Logs](https://github.com/mingley/partitionline/actions/runs/42445260031/artifacts/13603467513) include the comparisons and individual results.

[Run 42446145535](https://github.com/mingley/partitionline/actions/runs/42446145535) compared the other two original tests, without altering their assertions. Each failed three times on the baseline and three times on the candidate in both profiles. A separate test-only phase trace showed two ApiVersions v4 requests to node 1 by the end of producer construction in all three cases. Transaction begin added none; in the two-node cases the first send added one request to node 2, and the subsequent failed send in the epoch-bump case added none. Results matched across baseline, candidate and both profiles.

The [source](https://github.com/mingley/partitionline/blob/374dcf45db20ee728b00c0b0188e77ca1f47fa9c/src/producer.rs#L4460) deliberately negotiates new sockets even when cached versions exist, because a broker can change at the same address. The test expectations conflict with that policy. The phase trace reports broker and version, not socket identity; it does not prove behavior across broker replacement. The original failures remain recorded and the diagnostic job failed its test gate. [Logs](https://github.com/mingley/partitionline/actions/runs/42446145535/artifacts/13603009480) contain the original repetitions and traced runs.

## Limits

The ticket distinguishes state claims, not transaction generations. A late operation can first report an old transaction's error after another transaction begins; operations already admitted before a claim can also continue. These lifecycle and admission races remain open. The controls use deliberate test scheduling and do not prove all interleavings. This qualification does not cover the separate AddPartitions response-coverage candidate, retry-timeout repair, cancellation ownership, a live-broker failure campaign, performance, or production readiness.

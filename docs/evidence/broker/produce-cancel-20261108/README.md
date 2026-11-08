# Produce cancellation and staged journal cleanup

The broker now removes a canceled request's unsynced monolithic journal
entries when cancellation is observed between partitions. Previously, this
cleanup ran only for grouped requests. An ungrouped request could leave a
staged entry behind, causing a later fetch or append to fail.

The [hosted qualification](https://github.com/mingley/partitionline/actions/runs/42438380998)
ran on x86_64 with stable Rust 1.99.1. The tested branch commit was
[`ee32c2b2288857eb2789ee36d18e374b917e2db9`](https://github.com/mingley/partitionline/commit/ee32c2b2288857eb2789ee36d18e374b917e2db9),
based on `3f45fe6255e72abc6b0c4488bc13117779c50787`. The tested
`partitionline-broker/src/produce.rs` SHA-256 was
`326b7f8a7b465a8ab23fa431d39a68542bd469402fd927f0f567de95603c8bd4`;
the broker inventory was updated to match it.

| Check | Result |
| --- | --- |
| Original grouped-only cleanup, with the new ungrouped test | Failed at the expected `UNGROUPED_CANCEL_LEFT_STAGED` assertion, after a journal entry had been staged. |
| Candidate cancellation controls | Three passed in each of the default and all-feature profiles. They cover an ungrouped rollback, preservation of an earlier grouped request, and a durable rolling-storage prefix. |
| Broker all-target tests | 737 passed with default features and 885 with all features; no failures or ignored tests. |
| Format and strict all-target/all-feature Clippy | Passed. |
| Independent Apache Produce fixture test and inventory | Passed. The inventory recorded 708 Produce golden cases and accepted the generated handler report; all 55 inventory controls passed. |

The full suites include the new tests. The fixture report does not contain a
source hash; the workflow checked the source hash and inventory pin before
running the test. The job logs were inspected. The uploaded artifact was not
independently inspected.

Two earlier attempts stopped because their sparse checkouts omitted Cargo
examples or a broker fixture. Another full-checkout attempt was canceled
during baseline compilation when the checkout was changed. The linked final
run used a full checkout and completed.

This change does not make a multi-partition Produce request atomic. Rolling
storage can have a durable prefix, and cancellation after the final activity
check can still race with commit. Reopening the journal in a test does not
establish power-loss behavior: truncation is not explicitly synced here.
Truncation-failure poisoning and real transport disconnect or shutdown races
were not injected in these controls. No performance comparison or general
production qualification follows from this run.

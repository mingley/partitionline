# Producer transaction coordinator lock deadlines

A transaction request now includes two coordinator lock waits in its request
budget: the initial wait and the wait to snapshot the coordinator before
rediscovery. The first network request gets a fresh remaining duration after
the lock is acquired and the coordinator's API version is checked.

The [hosted qualification](https://github.com/mingley/partitionline/actions/runs/42439305027)
ran on x86_64 with stable Rust 1.99.1. Its candidate was
[`792712da310de2b870d38dd4f5ce1c05496ce504`](https://github.com/mingley/partitionline/commit/792712da310de2b870d38dd4f5ce1c05496ce504),
based on `3f45fe6255e72abc6b0c4488bc13117779c50787`. The tested
`src/producer.rs` SHA-256 was
`f4a842a85958e0fa90fe3fec458424770ccf389e8a56748999e031e6714b5ed1`.
The workflow verified the base source and the two production changes before
running the controls.

| Check | Result |
| --- | --- |
| Original lock waits | Both held-lock witnesses failed at their expected deadline assertions in default and all-feature builds. |
| Candidate lock waits | Both witnesses returned timeout while the respective lock remained held in each feature profile. |
| Queued cancellation | On both baseline and candidate, canceling the first of two waiting callers left the coordinator usable by the second. |
| Selected candidate controls | Six tests passed in each feature profile, including the three new tests and three existing coordinator/discovery controls. |
| Formatting and strict all-feature library Clippy | Passed. |

The job logs were inspected; the uploaded artifact was not independently
inspected. An earlier [attempt](https://github.com/mingley/partitionline/actions/runs/42438847395)
passed its baseline witnesses and five default candidate controls, then
failed because an exact test filter matched zero tests. The final run used
the correct module and required one selected test per invocation.

At the tested revision, this was not an end-to-end transaction deadline.
Response decoding, parts of discovery, coordinator retirement and the final
replacement lock could still exceed the request budget. Cancellation during
replacement could leave the shared coordinator empty. The later
[replacement fix](../txn-replacement-20261108/README.md) addresses those last
two waits and preserves the slot during retirement. Concurrent rediscoveries,
epoch recovery, transaction error-state races, real Kafka failure recovery
and performance remain open. Tokio scheduling also affects when a ready
timeout is observed.

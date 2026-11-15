# CreateTopics failure diagnostics

The process-test helper now reports the contacted node, the count of earlier
error 41 responses and the decoded CreateTopics v3 error message when a
Create returns an unexpected error. On transport failure it reports the node
and count and marks the wire message unavailable. Request construction,
leader discovery, retry rules and deadlines are unchanged.

[Run 42456784864](https://github.com/mingley/partitionline/actions/runs/42456784864)
qualified the exact test source on the combined broker tree based on
`6f6d8e9bc4f111fc5e4230eb5cec261b052fd154`. On x86_64 with stable Rust
1.99.1, the selected synthetic v3 parser test passed in both default and
all-feature configurations. It covers a message, null, empty, missing,
negative-length, truncated and non-UTF-8 fields. A temporary mutation that
discarded the decoded message made the test fail in both profiles. Formatting
and strict all-target, all-feature Clippy passed.

The earlier [run 42456612273](https://github.com/mingley/partitionline/actions/runs/42456612273)
passed both parser controls and Clippy but failed the formatting gate. Its
failure is retained. Neither run executed the live process test or exercised
the helper's 41 retry, contacted-node logging or transport failure against a
socket. The wire message can narrow an error's origin; it does not identify
the internal token or establish whether a mutation committed. The
[main CI Create error 56](../main-ci-e02-20261108/README.md) remains unresolved.

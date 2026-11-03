# Typed Raft attempt draft

This [source-only draft](candidate.patch.gz) distinguishes a rejection before
the requested append from an indeterminate outcome. It does not add retries or
explain the earlier intermittent Raft failures. The [design](DESIGN.md) records
the proposed contract and controls.

The [independent review](independent-review.json) found that the draft must be
rebased onto the current membership test, resolve public-method compatibility,
replace disallowed file reads and add public Handle timeout and cancellation
controls after an append starts. It also identified a separate inherited limit
on retained proposal-buffer capacity. No Rust compilation, native tests or
performance qualification was performed. Do not adopt this draft as qualified.

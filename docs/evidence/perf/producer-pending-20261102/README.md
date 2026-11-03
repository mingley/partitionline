# Producer pending queue: source candidate

The candidate is an isolated delta against the v8 client candidate, not
`main`. It adds two conditional shortcuts:

- Without sticky partitioning, if the cached minimum is valid and still in the
  future, no queued record can be expired. The worker skips the expiry scan.
- If queued deadlines are nondecreasing, removing the earliest prefix leaves
  the next queue element as the minimum. Nonmonotonic deadlines use the
  existing cooperative suffix scan.

The actor marks its cached queue statistics invalid before receiver accounting
and before sticky/expiry split operations can suspend. Interrupted accounting
cannot use the expiry shortcut. Draining and expiry purge rebuild invalid stats;
readiness refuses to fire until they are valid. An interrupted split retains
v8's existing Worker-drop restoration behavior.
The model and tests do not establish safe arbitrary resumption of a canceled
split on a live Worker.

The record container remains a `Vec`; prefix removal still shifts its suffix
and can take quadratic work. Sticky expiry and nonmonotonic deadline cases can
still scan the queue repeatedly. Tracking order adds work on admission and
recomputation, and its throughput effect has not been measured.

The patch preserves all existing v8 control identities and adds nine proposed
Worker controls. Test-only atomics count expiry predicate checks and records
visited in completed deadline suffix rescans. The ordered repeated-drain
control expects zero of each for 64 one-record drains; the unordered control
retains the fallback scan. The existing grouping cancellation test now polls
three times instead of five because the two skipped expiry scans no longer
yield. Native execution is required to verify that phase alignment.

The latest-stable Rustfmt check and exact forward/reverse patch reconstruction
pass. A separate Python abstraction model passed 30,000 randomized operations,
including 4,326 simulated canceled appends. That model does not execute Rust.
An [independent static review](independent-review.json) found no new blocker
in the inspected paths and recorded remaining cancellation-test gaps.
Compilation, Clippy, native tests and benchmark measurement are not complete.
The patch is not applied to the client candidate or main source.
No fastest or production-readiness claim follows.

Files: `v1.patch.gz`, `v1-receipt.json`, `model.py`, and `model-result.json`.

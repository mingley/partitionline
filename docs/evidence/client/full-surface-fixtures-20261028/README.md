# Full-surface fixture checks, 2026-10-28

A scratch candidate corrects three fixture assumptions in `full_surface`: an
idle group uses a session timeout longer than its heartbeat interval; an empty
retained log reports a high watermark equal to its log start; and changed Admin
API capabilities apply after a new socket negotiates them. The quorum test
keeps its original dropped-response and deadline checks, and verifies that the
old socket sent DescribeQuorum v2 before the replacement sent v0. The voter
tests keep the lost-response, error, and no-send assertions. Each controlled
replacement accepts one new socket and records its successful ApiVersions
response; this is not a general concurrent per-socket wire trace.

The first candidate compiled and its seven selected tests passed, but its
complete default-threaded `full_surface` run reported 333 passed and 5 failed.
Four failures also appeared in the earlier clean matrix: the Admin and Fetch
reconnect-backoff cases and two idempotent lost-ack cases. The fifth,
`kip848_acks_assignment_only_after_apply`, was a newly observed heartbeat
failure in this run. Strict all-feature target Clippy failed on three new
helper lints. Its unmodified logs and failed lint output are retained under
`v1/`.

The revised candidate removed those helper lint findings without changing the
assertions. On the fresh retained executable, all seven selected tests passed;
the complete `full_surface` run reported 334 passed and the same four baseline
failures, with no ignored cases. The extra KIP-848 heartbeat failure did not
recur in this run; the earlier failure remains evidence and its cause is not
established. Strict all-feature Clippy for the `full_surface` target passed.
Neither full-target run passed, and no full-client or production qualification
is claimed.

## Inputs and limits

Both runs used stable Rust 1.99.1 on the shared x86_64 workspace, in a sparse
detached worktree based on `63aa045be` with the fixture changes uncommitted.
The test executables were freshly built as confirmed by Cargo JSON artifacts,
then copied and SHA-verified; the hashes and local paths are in each
`executable.json`. The copied executables remain in the work directory rather
than this archive. Selected tests ran individually with one harness thread;
the full target retained Cargo's default test threading.

The before/after input manifests match for each run and cover the visible
sparse worktree files selected by the recorded runner, excluding docs, `.git`,
and build output. They do not pin ignored files, external dependencies, kernel
or all environment state. The source patches, exact runner, command results,
logs, and toolchain version are included. No OOM kill was recorded. Logs and
other raw inputs are deterministically compressed without changing their
reconstructed bytes. The original clean default matrix is documented in
[`clean-default-validation-20261028`](../clean-default-validation-20261028/README.md).

Verify from the repository root:

```sh
python3 benchmarks/runtime/verify-evidence-archive.py docs/evidence/client/full-surface-fixtures-20261028
```

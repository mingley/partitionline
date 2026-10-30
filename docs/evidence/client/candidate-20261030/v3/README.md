# Client candidate qualification

This is a prepared client candidate, with source hashes, exact patches and a CI runner. Rust compilation and runtime checks have not run for the combined source. The patches are held here for review; they do not change the client shipped from `src/`.

The candidate combines streamed Admin response decoding, bounded cooperative aggregation, socket retirement and dial ownership, JoinGroup retry deadlines, producer drain cancellation ownership, and small retry-chunk scheduling. It also preserves the existing mock timing assertions and adds wire traces and a standard 2MiB group-stack child test.

`spec.json` selects 13 baseline cases before three final fixes: producer drain ownership, transient JoinGroup handling, and the last small retry-chunk yield. Failures are predictions until the actual logs establish their causes. The baseline already contains the other proposed changes; it is not an unchanged historical release. The stack case is recorded without an expected result.

After the fixed delta, CI runs formatting and strict all-target/all-feature Clippy. It then runs each of ten chosen binaries in default and all-feature configurations. Every previously observed failure stays in scope. The 108 required library identities include six newly observed failures on `fe65ac783` for which this candidate does not claim a fix; 14 integration identities are also required. Full chosen-binary results remain required even when focused controls pass. This does not run every integration binary in the repository.

The base Rust source is from `fe65ac783`. Reviewed ancillary Python and JSON changes from the same push are pinned separately in the manifests. The source scope is sparse and excludes `docs/` and CI-only workflow/driver additions. Each command checks its isolated source before and after execution and retains its executable hash, log, resource observations, and owned-process cleanup receipt. Patches are gzip archives with both compressed and original-byte hashes. Source preparation verified baseline and fixed reconstruction without invoking Rust.

`preparation-controls-01.json` records 17 client guard controls and six shared command-ownership controls. These use synthetic Python inputs and small real Python processes. They do not qualify client behavior. The runtime reserve remains 1.5GiB; builds use one Cargo job. Unavailable cgroup OOM counters are reported as unknown. The ordinary runtime stack is 8MiB, with a separate explicit standard 2MiB child control. Passing that mock alone would not establish live Kafka stack safety.

Producer accepted-record ownership during later grouping/encoding/requeue, issue and replay ordering, global memory bounds, synchronous output cleanup, external broker compatibility, cluster durability and comparative performance remain open. Production readiness, a release and fastest-performance claims are not qualified by this preparation.
